"""Measured facts about how often filed numbers change, and which warning signs preceded a non-reliance filing.

    python -m filings.findings       # writes reports/findings.json and prints the tables

Everything here is a count over a stated denominator. The universe is the original 10-Ks in gold.annual
outside blank-check companies, restricted to 10-Ks whose 24 months of follow-up have passed, so that every
row has had the same chance to be revised or to be followed by an Item 4.02.

- Revision rate by line item, fiscal year and filer status: the share of figures a 10-K reported for its own
  fiscal year that a later filing changed by more than 1% and $100K under the same tag within 24 months
  (sign flips excluded). "Changed" includes retrospective accounting standards, which is why the rate
  jumps in some years; see ADOPTIONS in model.py.
- How long after the 10-K a change lands.
- Lift of each point-in-time warning sign over the Item 4.02 base rate: precision among flagged 10-Ks
  divided by the base rate, with the recall (share of all 4.02 filers the sign flagged).
- The published Beneish M-score at its conventional -1.78 cut-off, against the Item 4.02 and revision labels.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from .concepts import CONCEPTS
from .model import (BENEISH, BLANK_CHECK, HORIZON_DAYS, LABEL_CONCEPTS, features, load, nonreliance_labels,
                    revision_labels)

REPORT = Path(__file__).resolve().parent.parent / "reports" / "findings.json"
M_CUTOFF = -1.78
REL, ABS = 0.01, 100_000


def annual_revisions(annual: pd.DataFrame, rev: pd.DataFrame) -> pd.DataFrame:
    """Every revision of a figure a 10-K in `annual` reported for its own fiscal year, with days since that 10-K.
    One row per (10-K, concept): the first revision. Sign flips excluded."""
    qtrs = {c: (4 if kind == "duration" else 0) for c, (kind, _) in CONCEPTS.items()}
    r = rev[(rev.qtrs == rev.concept.map(qtrs)) & ~np.isclose(rev.value, -rev.prev_value)].copy()
    r["period"] = pd.to_datetime(r.ddate)
    r["rev_filed"] = pd.to_datetime(r.filed)
    a = annual[["adsh", "cik", "period", "filed"]].copy()
    a["period"], a["filed"] = pd.to_datetime(a.period), pd.to_datetime(a.filed)
    j = a.merge(r[["cik", "period", "concept", "rev_filed", "value", "prev_value"]], on=["cik", "period"])
    j = j[j.rev_filed > j.filed]
    j["days"] = (j.rev_filed - j.filed).dt.days
    j["rel_change"] = ((j.value - j.prev_value).abs() / j.prev_value.abs().clip(lower=1)).round(4)
    return j.sort_values("days").drop_duplicates(["adsh", "concept"])


def rate(num: int, den: int) -> float | None:
    return round(num / den, 4) if den else None


def revision_tables(annual: pd.DataFrame, first_rev: pd.DataFrame) -> dict:
    within = first_rev[first_rev.days <= HORIZON_DAYS]
    hit = {c: set(g.adsh) for c, g in within.groupby("concept")}
    by_item = {}
    for c in CONCEPTS:
        reported = annual[annual[c].notna()] if c in annual else annual.iloc[0:0]
        n = len(reported)
        k = reported.adsh.isin(hit.get(c, set())).sum()
        w = within[within.concept == c]
        by_item[c] = {"figures_reported": int(n), "changed_within_24m": int(k), "rate": rate(k, n),
                      "median_days_to_change": int(w.days.median()) if len(w) else None,
                      "median_relative_change": round(float(w.rel_change.median()), 4) if len(w) else None}
    by_item = dict(sorted(by_item.items(), key=lambda kv: -(kv[1]["rate"] or 0)))

    label = set().union(*(hit.get(c, set()) for c in LABEL_CONCEPTS))
    a = annual.assign(any_of_four=annual.adsh.isin(label).astype(int), fiscal_year=pd.to_datetime(annual.period).dt.year)
    core = a[a[list(LABEL_CONCEPTS)].notna().any(axis=1)]  # 10-Ks that reported at least one of the four
    by_year = {int(y): {"ten_ks": int(len(g)), "changed": int(g.any_of_four.sum()), "rate": rate(g.any_of_four.sum(), len(g))}
               for y, g in core.groupby("fiscal_year") if len(g) >= 500}
    by_afs = {str(s): {"ten_ks": int(len(g)), "changed": int(g.any_of_four.sum()), "rate": rate(g.any_of_four.sum(), len(g))}
              for s, g in core.groupby(core.afs.fillna("unknown")) if len(g) >= 500}

    f = first_rev[first_rev.concept.isin(LABEL_CONCEPTS)]
    q = f.days.quantile([.1, .25, .5, .75, .9])
    buckets = {"0-90 days": (0, 90), "91-300 days": (91, 300), "301-450 days (next 10-K)": (301, 450),
               "451-730 days": (451, 730), "over 730 days": (731, 10**6)}
    timing = {"first_changes_counted": int(len(f)), "line_items": sorted(LABEL_CONCEPTS),
              "days_quantiles": {f"p{int(k * 100)}": int(v) for k, v in q.items()},
              "share_by_window": {k: rate(int(f.days.between(lo, hi).sum()), len(f)) for k, (lo, hi) in buckets.items()}}
    return {"universe": {"ten_ks": int(len(annual)), "with_any_of_the_four_items": int(len(core)),
                         "changed_any_of_the_four_within_24m": int(core.any_of_four.sum()),
                         "rate": rate(core.any_of_four.sum(), len(core))},
            "by_line_item": by_item, "any_of_four_by_fiscal_year": by_year, "any_of_four_by_filer_status": by_afs,
            "days_from_10k_to_first_change": timing}


def lift_table(flags: dict[str, pd.Series], y: np.ndarray) -> dict:
    base = float(y.mean())
    out = {"base_rate": round(base, 4), "ten_ks": int(len(y)), "positives": int(y.sum()), "signals": {}}
    for name, flag in flags.items():
        f = flag.fillna(False).astype(bool).values
        n, k = int(f.sum()), int(y[f].sum())
        out["signals"][name] = {"flagged": n, "positives_among_flagged": k, "precision": rate(k, n),
                                "lift": round(k / n / base, 2) if n else None, "recall": rate(k, int(y.sum())),
                                "share_flagged": rate(n, len(y))}
    out["signals"] = dict(sorted(out["signals"].items(), key=lambda kv: -(kv[1]["lift"] or 0)))
    return out


def signals(x: pd.DataFrame, a: pd.DataFrame) -> dict[str, pd.Series]:
    def top_decile(s):
        return s >= s.quantile(0.9)

    return {
        "late filer: 10-K more than 90 days after year end": x.filing_lag_days > 90,
        "very late filer: more than 180 days": x.filing_lag_days > 180,
        "NT 10-K late notice filed for this 10-K": x.late_notice > 0,
        "any earlier revision of its figures": x.prior_revisions > 0,
        "an earlier Item 4.02 (ever)": x.prior_nonreliance > 0,
        "an Item 4.02 in the prior 24 months": x.nonreliance_24m > 0,
        "auditor change (Item 4.01) in the prior 24 months": x.auditor_changes_24m > 0,
        "10-K/A or 10-Q/A in the prior 24 months": x.amendments_24m > 0,
        "first 10-K in the data": x.prior_10ks == 0,
        "formerly a blank-check company (SPAC)": x.was_blank_check > 0,
        "net loss": x.loss > 0,
        "negative equity": a.equity < 0,
        "smaller reporting company or non-accelerated filer": (a.afs.isin(["4-NON", "5-SML"])),
        "large accelerated filer": a.afs == "1-LAF",
        "smallest decile by total assets": x.log_assets <= x.log_assets.quantile(0.1),
        "largest decile by total assets": x.log_assets >= x.log_assets.quantile(0.9),
        "top-decile accruals (net income minus operating cash flow, over assets)": top_decile(x.tata),
        "top-decile receivables growth (over assets)": top_decile(x.ch_receivables),
        "top-decile asset growth": top_decile(x.asset_growth),
        "top-decile soft assets": top_decile(x.soft_assets),
        "three or more core line items missing": x.missing_items >= 3,
        f"Beneish M-score above {M_CUTOFF}": x.m_score > M_CUTOFF,
    }


def beneish_cutoff(x: pd.DataFrame, labels: dict[str, np.ndarray]) -> dict:
    comp = x[BENEISH].notna().all(axis=1).values
    flag = (x.m_score > M_CUTOFF).values & comp
    out = {"cutoff": M_CUTOFF, "ten_ks_with_all_eight_ratios": int(comp.sum()), "flagged": int(flag.sum()),
           "share_flagged": rate(int(flag.sum()), int(comp.sum()))}
    for name, y in labels.items():
        yc, base = y[comp], float(y[comp].mean())
        k = int(y[flag].sum())
        out[name] = {"base_rate_among_computable": round(base, 4), "positives_among_flagged": k,
                     "precision": rate(k, int(flag.sum())), "lift": round(k / flag.sum() / base, 2),
                     "recall": rate(k, int(yc.sum())), "positives_among_computable": int(yc.sum())}
    return out


def main() -> None:
    annual, rev, events, filings = load()
    annual = annual[annual.total_assets.notna() & (annual.total_assets > 0) & (annual.sic != BLANK_CHECK)]
    end = pd.to_datetime(events.filed).max()
    filed = pd.to_datetime(annual.filed)
    annual = annual[filed + pd.Timedelta(days=HORIZON_DAYS) <= end].reset_index(drop=True)
    filed = pd.to_datetime(annual.filed)

    first_rev = annual_revisions(annual, rev)
    x = features(annual, rev, events, filings)
    y402 = nonreliance_labels(annual, events).values
    yrev = revision_labels(annual, rev).values
    report = {
        "universe": {"original_10_ks": int(len(annual)), "filed_from": str(filed.min().date()), "filed_to": str(filed.max().date()),
                     "note": "original 10-Ks outside SIC 6770 with total assets, filed at least 24 months before the "
                             f"last event date ({end.date()})"},
        "revisions": revision_tables(annual, first_rev),
        "item_4_02_signals": lift_table(signals(x, annual), y402),
        "beneish_at_cutoff": beneish_cutoff(x, {"item_4_02_within_24m": y402, "revision_within_24m": yrev}),
    }
    REPORT.parent.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
