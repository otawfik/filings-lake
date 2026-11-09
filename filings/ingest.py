"""Land SEC Financial Statement Data Sets, one quarter at a time, as Parquet.

    python -m filings.ingest 2012q1 2026q3     # every quarter in the range
    python -m filings.ingest 2024q4

Each quarterly zip (~120 MB, num.txt ~530 MB unzipped) is streamed straight out of the archive, so
nothing large is ever extracted. It keeps the facts that can matter downstream: 10-K and 10-Q filings
(and their amendments), whole-company values (no segment or co-registrant breakdowns), USD or
per-share, for the tags in `concepts.py`. Then the zip is deleted. On Databricks the same files land
whole in a Volume and Auto Loader reads them; this is the laptop-sized equivalent.

The SEC requires a contact in the User-Agent (fair-access policy): set SEC_USER_AGENT="Name email".
"""
from __future__ import annotations

import io
import os
import sys
import time
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from .concepts import TAGS

ROOT = Path(__file__).resolve().parent.parent
RAW, LAND = ROOT / "raw", ROOT / "lake" / "landing"
URL = "https://www.sec.gov/files/dera/data/financial-statement-data-sets/{q}.zip"
FORMS = {"10-K", "10-K/A", "10-Q", "10-Q/A", "10-KT", "10-KT/A"}
SUB_COLS = ["adsh", "cik", "name", "sic", "countryba", "afs", "fye", "form", "period", "fy", "fp", "filed", "prevrpt"]


def quarters(a: str, b: str) -> list[str]:
    ya, qa, yb, qb = int(a[:4]), int(a[-1]), int(b[:4]), int(b[-1])
    return [f"{y}q{q}" for y in range(ya, yb + 1) for q in range(1, 5) if (ya, qa) <= (y, q) <= (yb, qb)]


def download(q: str) -> Path:
    ua = os.environ.get("SEC_USER_AGENT")
    if not ua:
        sys.exit("set SEC_USER_AGENT='Your Name your@email' (the SEC requires a contact)")
    RAW.mkdir(parents=True, exist_ok=True)
    path = RAW / f"{q}.zip"
    with requests.get(URL.format(q=q), headers={"User-Agent": ua}, stream=True, timeout=120) as r:
        r.raise_for_status()
        if "zip" not in r.headers.get("content-type", "") and "octet" not in r.headers.get("content-type", ""):
            raise RuntimeError(f"{q}: SEC returned {r.headers.get('content-type')} (rate limited?)")
        with path.open("wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    return path


def read_tsv(z: zipfile.ZipFile, name: str, columns: list[str] | None = None) -> pa.Table:
    with z.open(name) as f:
        return pacsv.read_csv(
            io.BufferedReader(f, 1 << 22),
            read_options=pacsv.ReadOptions(block_size=1 << 25),
            parse_options=pacsv.ParseOptions(delimiter="\t", quote_char=False, invalid_row_handler=lambda _: "skip"),
            convert_options=pacsv.ConvertOptions(include_columns=columns, strings_can_be_null=True,
                                                 column_types={"cik": pa.int64(), "value": pa.float64(), "ddate": pa.string(),
                                                               "period": pa.string(), "filed": pa.string(), "sic": pa.string(),
                                                               "fy": pa.string(), "fye": pa.string(), "adsh": pa.string()}),
        )


def land(q: str, path: Path) -> dict:
    out = LAND / q
    out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path) as z:
        sub = read_tsv(z, "sub.txt", SUB_COLS)
        sub = sub.filter(pc.is_in(sub["form"], pa.array(sorted(FORMS))))
        with z.open("num.txt") as f:
            header = f.readline().decode().strip().split("\t")
        cols = [c for c in ("adsh", "tag", "version", "ddate", "qtrs", "uom", "segments", "coreg", "value") if c in header]
        num = read_tsv(z, "num.txt", cols)
    keep = pc.and_(pc.is_in(num["tag"], pa.array(sorted(TAGS))), pc.is_in(num["adsh"], sub["adsh"]))
    keep = pc.and_(keep, pc.is_in(num["uom"], pa.array(["USD", "USD/shares"])))
    keep = pc.and_(keep, pc.starts_with(num["version"], "us-gaap"))
    for col in ("segments", "coreg"):  # whole-company values only
        if col in num.column_names:
            keep = pc.and_(keep, pc.fill_null(pc.equal(num[col], ""), True))
    keep = pc.and_(keep, pc.invert(pc.is_null(num["value"])))
    num = num.filter(keep).select(["adsh", "tag", "ddate", "qtrs", "uom", "value"])
    pq.write_table(num, out / "num.parquet", compression="zstd")
    pq.write_table(sub, out / "sub.parquet", compression="zstd")
    return {"quarter": q, "filings": sub.num_rows, "facts": num.num_rows}


def main() -> None:
    qs = quarters(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else sys.argv[1])
    for q in qs:
        if (LAND / q / "num.parquet").exists():
            continue
        t = time.time()
        try:
            path = download(q)
        except requests.HTTPError as e:
            print(f"{q}: not available ({e.response.status_code})", flush=True)
            continue
        try:
            stats = land(q, path)
        finally:
            path.unlink(missing_ok=True)
        print(f"{stats['quarter']}: {stats['filings']:,} filings, {stats['facts']:,} facts ({time.time() - t:.0f}s)", flush=True)
        time.sleep(1)  # well under the SEC's 10 requests/second


if __name__ == "__main__":
    main()
