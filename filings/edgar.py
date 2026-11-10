"""Land filing events from EDGAR full-text search: 8-K items and late-filing notices.

    python -m filings.edgar 2009-01-01 2026-11-08      # all three event types in the range
    python -m filings.edgar text                       # then read what each Item 4.02 says
    python -m filings.edgar coverage                   # check the 4.02 list against EDGAR's company index
    python -m filings.edgar csv                        # write data/item_4_02.csv

    item_4_02   8-K Item 4.02 "Non-Reliance on Previously Issued Financial Statements": the company (or its
                auditor) says statements it already filed can no longer be relied on. The SEC's own
                restatement disclosure, and the model's main label.
    item_4_01   8-K Item 4.01 "Changes in Registrant's Certifying Accountant": an auditor change. Used only
                as a point-in-time feature (an auditor change before a 10-K).
    nt_10k      Form NT 10-K (Rule 12b-25): the company says its 10-K will be late. Feature only.

EDGAR full-text search (efts.sec.gov/LATEST/search-index) returns every document in a filing, each with
the filing's header metadata, including the 8-K item numbers the filer declared. It has no filter on
items, so for 8-Ks it is searched for text that any Item 4.02 / 4.01 filing contains (the item number,
or its heading), and only filings whose *declared* items include the item are kept; the text query
is a net, the header is the test. A search returns at most 10,000 documents, so each month is
searched on its own and split in half again whenever it would hit that cap.

Output: lake/landing/events/<kind>.parquet, one row per (filing, CIK). The SEC asks for a contact in
the User-Agent and at most 10 requests a second; this stays near 3.
"""
from __future__ import annotations

import os
import re
import sys
import time
import urllib.parse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
EVENTS = ROOT / "lake" / "landing" / "events"
FTS = "https://efts.sec.gov/LATEST/search-index"
CAP, PAGE = 10_000, 100
PAUSE = 0.3  # seconds between requests: ~3 a second, under the SEC's limit of 10

# kind: (form filter, text queries, the item the filing must declare, or None for a form-only search)
KINDS: dict[str, tuple[str, list[str | None], str | None]] = {
    "item_4_02": ("8-K", ['"4.02"', "non-reliance", '"previously issued financial statements"'], "4.02"),
    "item_4_01": ("8-K", ['"Item 4.01"', '"certifying accountant"'], "4.01"),
    "nt_10k": ("NT 10-K", [None], None),
}


def session() -> requests.Session:
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua:
        sys.exit("set SEC_USER_AGENT='Your Name your@email' (the SEC requires a contact)")
    s = requests.Session()
    s.headers["User-Agent"] = ua
    return s


def get(s: requests.Session, params: dict) -> dict:
    for attempt in range(6):
        time.sleep(PAUSE * (1 + 4 * attempt))
        try:
            r = s.get(FTS + "?" + urllib.parse.urlencode(params), timeout=60)
        except requests.RequestException:
            continue
        if r.status_code == 200:
            try:
                d = r.json()
            except ValueError:
                continue
            if "hits" in d:  # an overloaded search can answer 200 with an error body
                return d
            continue
        if r.status_code not in (429, 500, 502, 503, 504):
            r.raise_for_status()
    raise RuntimeError(f"full-text search kept failing for {params}")


def search(s: requests.Session, form: str, q: str | None, start: date, end: date) -> list[dict]:
    """Every document matching (q, form) filed in [start, end], splitting the range under the 10,000 cap."""
    params = {"forms": form, "dateRange": "custom", "startdt": start.isoformat(), "enddt": end.isoformat()}
    if q:
        params["q"] = q
    first = get(s, params)
    total = first["hits"]["total"]
    if total["value"] >= CAP or total.get("relation") == "gte":
        if start == end:
            raise RuntimeError(f"{form} {q} on {start}: over {CAP} documents in one day")
        mid = start + (end - start) / 2
        return search(s, form, q, start, mid) + search(s, form, q, mid + timedelta(days=1), end)
    hits = first["hits"]["hits"]
    while len(hits) < total["value"]:
        page = get(s, params | {"from": len(hits)})["hits"]["hits"]
        if not page:
            break
        hits += page
    return hits


def months(start: date, end: date):
    a = start
    while a <= end:
        b = (a.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        yield a, min(b, end)
        a = b + timedelta(days=1)


def rows(hits: list[dict], item: str | None) -> list[dict]:
    """One row per (filing, CIK) whose declared items include `item`; the main document's name if seen."""
    out: dict[tuple[str, int], dict] = {}
    for h in hits:
        src = h["_source"]
        items = src.get("items") or []
        if item and item not in items:
            continue
        main = h["_id"].split(":", 1)[1] if src.get("file_type", "").upper().startswith(src["form"].upper()) else None
        for cik in src.get("ciks") or []:
            key = (src["adsh"], int(cik))
            r = out.setdefault(key, {"adsh": src["adsh"], "cik": int(cik), "filed": src["file_date"], "form": src["form"],
                                     "items": ",".join(items), "sic": (src.get("sics") or [None])[0],
                                     "period_ending": src.get("period_ending"), "main_doc": None})
            r["main_doc"] = r["main_doc"] or main
    return list(out.values())


def fetch(kind: str, start: date, end: date, s: requests.Session | None = None) -> pd.DataFrame:
    form, queries, item = KINDS[kind]
    s = s or session()
    found: dict[tuple[str, int], dict] = {}
    for a, b in months(start, end):
        n = 0
        for q in queries:
            for r in rows(search(s, form, q, a, b), item):
                prev = found.get((r["adsh"], r["cik"]))
                if prev is None:
                    found[(r["adsh"], r["cik"])] = r
                    n += 1
                elif prev["main_doc"] is None:
                    prev["main_doc"] = r["main_doc"]
        print(f"{kind} {a:%Y-%m}: {n} filings", flush=True)
    df = pd.DataFrame(list(found.values()), columns=["adsh", "cik", "filed", "form", "items", "sic", "period_ending", "main_doc"])
    df["filed"] = pd.to_datetime(df.filed)
    return df.sort_values(["filed", "adsh", "cik"]).reset_index(drop=True)


MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),?\s+(20\d\d)\b")
FISCAL_RE = re.compile(r"\bfiscal(?:\s+year)?\s+(?:ended\s+)?(20\d\d)\b", re.I)
SECTION_RE = re.compile(r"item\s*4\.02(.*?)(?=item\s*[5-9]\.\d\d|signatures?\b|$)", re.I | re.S)


def plain(html: str) -> str:
    text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", html)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&#160;", " ").replace("&#xa0;", " ").replace("&amp;", "&")
    text = re.sub(r"&#8217;|&rsquo;", "'", text)
    return re.sub(r"\s+", " ", text)


def read_nonreliance(text: str) -> dict:
    """What an Item 4.02 says: the period-end dates and fiscal years it names, and whether it is about warrants
    or redeemable shares (the 2021 SPAC accounting wave)."""
    sections = SECTION_RE.findall(text)
    body = max(sections, key=len) if sections and len(max(sections, key=len)) > 200 else text
    months = {m: i for i, m in enumerate(MONTHS.split("|"), 1)}
    dates = sorted({f"{y}-{months[m]:02d}-{int(d):02d}" for m, d, y in DATE_RE.findall(body) if 1 <= int(d) <= 31})
    low = body.lower()
    return {"dates_named": ",".join(dates), "fiscal_years_named": ",".join(sorted(set(FISCAL_RE.findall(body)))),
            "mentions_warrants": "warrant" in low,
            "mentions_redeemable_shares": "temporary equity" in low or ("class a" in low and "redemption" in low),
            "section_found": body is not text}


def nonreliance_text(path: Path, out: Path, s: requests.Session | None = None) -> pd.DataFrame:
    """Read the main document of every Item 4.02 filing outside blank-check companies (SIC 6770). Only the
    fields read_nonreliance extracts are kept, not the documents. Resumable: rows already in `out` are skipped."""
    s = s or session()
    ev = pd.read_parquet(path)
    ev = ev[(ev.sic != "6770") & ev.main_doc.notna()].drop_duplicates("adsh")
    done = pd.read_parquet(out) if out.exists() else pd.DataFrame(columns=["adsh"])
    todo = ev[~ev.adsh.isin(done.adsh)]
    got = []
    for i, r in enumerate(todo.itertuples(), 1):
        url = f"https://www.sec.gov/Archives/edgar/data/{r.cik}/{r.adsh.replace('-', '')}/{r.main_doc}"
        for attempt in range(5):
            time.sleep(PAUSE * (1 + 4 * attempt))
            try:
                resp = s.get(url, timeout=60)
            except requests.RequestException:
                continue
            if resp.status_code == 200 or resp.status_code == 404:
                break
        ok = resp.status_code == 200
        got.append({"adsh": r.adsh, "fetched": ok, **(read_nonreliance(plain(resp.text)) if ok else {})})
        if i % 200 == 0 or i == len(todo):
            done = pd.concat([done, pd.DataFrame(got)], ignore_index=True)
            done.to_parquet(out, index=False)
            got = []
            print(f"item 4.02 text: {i:,}/{len(todo):,}", flush=True)
    return done


def submissions_items(s: requests.Session, cik: int) -> pd.DataFrame:
    """Every filing EDGAR lists for a company (data.sec.gov submissions JSON), with its declared 8-K items."""
    def get_json(url):
        for attempt in range(8):
            time.sleep(PAUSE * (1 + 4 * attempt ** 2))
            try:
                r = s.get(url, timeout=60)
            except requests.RequestException:
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
        raise RuntimeError(url)

    base = "https://data.sec.gov/submissions/"
    d = get_json(f"{base}CIK{cik:010d}.json")
    if d is None:
        return pd.DataFrame(columns=["adsh", "filed", "form", "items"])
    pages = [d["filings"]["recent"]] + [get_json(base + f["name"]) for f in d["filings"].get("files", [])
                                        if f.get("filingTo", "9999") >= "2012-01-01"]
    df = pd.concat([pd.DataFrame({"adsh": p["accessionNumber"], "filed": p["filingDate"], "form": p["form"],
                                  "items": p.get("items", [""] * len(p["form"]))}) for p in pages if p])
    return df


def check_coverage(n: int = 400, seed: int = 0) -> dict:
    """Recall and precision of the full-text-search 4.02 list against EDGAR's own per-company filing index, on a
    random sample of companies that filed a 10-K. Writes reports/item_4_02_coverage.json."""
    import glob
    import json

    s = session()
    sub = pd.concat([pd.read_parquet(f, columns=["cik", "form"]) for f in glob.glob(str(ROOT / "lake/landing/20*/sub.parquet"))])
    ciks = pd.Series(sorted(set(sub[sub.form == "10-K"].cik))).sample(n, random_state=seed).tolist()
    ours = pd.read_parquet(EVENTS / "item_4_02.parquet")
    ours["filed"] = pd.to_datetime(ours.filed)
    lo, hi = pd.Timestamp("2012-01-01"), ours.filed.max()
    ours = ours[ours.cik.isin(ciks) & ours.filed.between(lo, hi)]
    cache = EVENTS / "coverage_sample.parquet"  # resumable: companies already checked are kept here
    seen = pd.read_parquet(cache) if cache.exists() else pd.DataFrame(columns=["cik", "adsh", "filed"])
    new = []
    for i, c in enumerate([c for c in ciks if c not in set(seen.cik)], 1):
        d = submissions_items(s, int(c))
        d = d[d.form.isin(["8-K", "8-K/A"]) & d["items"].fillna("").str.contains("4.02", regex=False)]
        new += [{"cik": int(c), "adsh": a, "filed": f} for a, f in zip(d.adsh, d.filed)] or [{"cik": int(c), "adsh": None, "filed": None}]
        if i % 25 == 0:
            seen = pd.concat([seen, pd.DataFrame(new)], ignore_index=True)
            seen.to_parquet(cache, index=False)
            new = []
    seen = pd.concat([seen, pd.DataFrame(new)], ignore_index=True)
    seen.to_parquet(cache, index=False)
    seen = seen[seen.cik.isin(ciks) & seen.adsh.notna()]
    theirs = [(a, int(c)) for a, c, f in zip(seen.adsh, seen.cik, pd.to_datetime(seen.filed)) if lo <= f <= hi]
    theirs, mine = set(theirs), set(zip(ours.adsh, ours.cik))
    out = {"companies_sampled": n, "range": [str(lo.date()), str(hi.date())],
           "item_4_02_filings_in_edgar_index": len(theirs), "found_by_search": len(theirs & mine),
           "found_by_search_not_in_index": len(mine - theirs), "missed": sorted(a for a, _ in theirs - mine),
           "recall": round(len(theirs & mine) / max(1, len(theirs)), 4)}
    (ROOT / "reports").mkdir(exist_ok=True)
    (ROOT / "reports" / "item_4_02_coverage.json").write_text(json.dumps(out, indent=2) + "\n")
    return out


def export_csv(path: Path = ROOT / "data" / "item_4_02.csv") -> pd.DataFrame:
    """The Item 4.02 list as a small CSV in the repo (public-domain SEC data), so the label can be audited."""
    ev = pd.read_parquet(EVENTS / "item_4_02.parquet")
    text = EVENTS / "item_4_02_text.parquet"
    if text.exists():
        ev = ev.merge(pd.read_parquet(text)[["adsh", "mentions_warrants", "mentions_redeemable_shares"]], "left", "adsh")
    cols = ["filed", "adsh", "cik", "form", "sic", "mentions_warrants", "mentions_redeemable_shares"]
    ev = ev.reindex(columns=cols).sort_values(["filed", "adsh", "cik"])
    path.parent.mkdir(exist_ok=True)
    ev.to_csv(path, index=False)
    return ev


def main() -> None:
    if sys.argv[1] == "csv":
        print(f"{len(export_csv()):,} rows")
        return
    if sys.argv[1] == "coverage":
        print(check_coverage())
        return
    if sys.argv[1] == "text":
        nonreliance_text(EVENTS / "item_4_02.parquet", EVENTS / "item_4_02_text.parquet")
        return
    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today()
    kinds = sys.argv[3].split(",") if len(sys.argv) > 3 else list(KINDS)
    EVENTS.mkdir(parents=True, exist_ok=True)
    s = session()
    for kind in kinds:
        t = time.time()
        for y in range(start.year, end.year + 1):  # saved a year at a time, so a failure loses one year at most
            a, b = max(start, date(y, 1, 1)), min(end, date(y, 12, 31))
            df = save(kind, a, b, fetch(kind, a, b, s))
        print(f"{kind}: {df.adsh.nunique():,} filings, {len(df):,} filing-CIK rows ({time.time() - t:.0f}s)", flush=True)


def save(kind: str, start: date, end: date, df: pd.DataFrame) -> pd.DataFrame:
    """Write the events for [start, end] into the kind's file, replacing what was there for that range only."""
    out = EVENTS / f"{kind}.parquet"
    if out.exists():
        old = pd.read_parquet(out)
        old["filed"] = pd.to_datetime(old.filed)
        old = old[(old.filed < pd.Timestamp(start)) | (old.filed > pd.Timestamp(end))]
        df = pd.concat([old, df], ignore_index=True).sort_values(["filed", "adsh", "cik"]).reset_index(drop=True)
    df["filed"] = pd.to_datetime(df.filed).dt.date  # a plain date: Spark can't read pandas' nanosecond timestamps
    df.to_parquet(out, index=False)
    return df


if __name__ == "__main__":
    main()
