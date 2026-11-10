"""Which annual reports will a company later say can't be relied on? A point-in-time model against Beneish.

    python -m filings.model          # writes reports/model.json and prints the comparison

Two targets, both per company-year (one original 10-K):

- **nonreliance** (main): the company files an 8-K under Item 4.02 "Non-Reliance on Previously Issued
  Financial Statements" in the 24 months after this 10-K's filing date (the day after, through day 730).
  An Item 4.02 says which statements can no longer be relied on; nearly always the latest 10-K and the
  10-Qs since, sometimes two or three years. Counting any 4.02 in the 24 months after a 10-K is filed is a
  firm-level "will this company restate what it has filed" label; `nonreliance_validation` in the
  report measures how often the 4.02 text actually names this 10-K's year-end. Only original 8-Ks count:
  an 8-K/A usually amends a 4.02 filed earlier, which may predate the 10-K.
- **revision** (second, noisier): a later filing changes that year's net income, operating cash flow,
  total assets or equity by more than 1% and $100K under the same tag within 24 months. Exact sign flips
  are XBRL tagging fixes and don't count; nor do revisions in the years and line items where a new
  accounting standard was applied retrospectively by most companies (ADOPTIONS).

Blank-check companies (SIC 6770, SPACs) are left out. In 2021 almost every SPAC filed an Item 4.02 after
the SEC staff statement on warrants (April) and on redeemable Class A shares (November): one accounting
interpretation hitting a whole category, not something a company's numbers predict.

Features are only what was known on the 10-K's filing date: its own figures and prior-year comparatives,
how late it was filed, filer status, the company's earlier 4.02s, auditor changes, late-filing notices,
amendments and revisions (all strictly before the filing date), and how long it has been filing.

Split by filing date so that no label event is shared between train and test: train on 10-Ks filed up to
2018 (their 24-month windows close by the end of 2020), test on 10-Ks filed 2021-2024 whose 24 months have
passed. Model choices were made on a validation split (train up to 2015, validate on 2018), not on test.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

REPORT = Path(__file__).resolve().parent.parent / "reports" / "model.json"
LABEL_CONCEPTS = {"net_income": 4, "operating_cash_flow": 4, "total_assets": 0, "equity": 0}
HORIZON_DAYS = 730
BLANK_CHECK = "6770"
BENEISH = ["dsri", "gmi", "aqi", "sgi", "depi", "sgai", "tata", "lvgi"]
TRAIN_LAST = pd.Timestamp("2018-12-31")
TEST_FIRST, TEST_LAST = pd.Timestamp("2021-01-01"), pd.Timestamp("2024-12-31")
VALID = (pd.Timestamp("2015-12-31"), pd.Timestamp("2018-01-01"), pd.Timestamp("2018-12-31"))  # train ≤, valid in [ , ]

# Retrospective standards that changed a line item for most companies at once. A revision to that line
# item, for a fiscal year ending in the window, is the standard and not a correction. Windows are where
# the revision rate of that line item jumps in this data (README); ASC 842 (leases, 2019) shows no jump,
# because most companies took the option not to restate comparatives.
ADOPTIONS = [
    ("total_assets", "2014-07-01", "2015-12-31", "ASU 2015-03/2015-17: debt issuance costs, deferred taxes"),
    ("operating_cash_flow", "2015-07-01", "2017-12-31", "ASU 2016-09/2016-15/2016-18: cash-flow classification"),
    ("net_income", "2017-01-01", "2018-06-30", "ASC 606 revenue, full retrospective adopters"),
    ("equity", "2017-01-01", "2018-06-30", "ASC 606 revenue, full retrospective adopters"),
]
HISTORY = ["prior_revisions", "prior_nonreliance", "nonreliance_24m", "amendments_24m"]


def beneish(a: pd.DataFrame) -> pd.DataFrame:
    """The eight Beneish (1999) ratios and the M-score. Missing securities / long-term debt count as 0."""
    d = pd.DataFrame(index=a.index)

    def g(c):
        return a[c] if c in a else pd.Series(np.nan, index=a.index)

    sec, sec_p = g("securities").fillna(0), g("securities_prev").fillna(0)
    ltd, ltd_p = g("long_term_debt").fillna(0), g("long_term_debt_prev").fillna(0)
    gp = g("gross_profit").fillna(g("revenue") - g("cost_of_revenue"))
    gp_p = g("gross_profit_prev").fillna(g("revenue_prev") - g("cost_of_revenue_prev"))
    rev, rev_p = g("revenue"), g("revenue_prev")
    d["dsri"] = (g("receivables") / rev) / (g("receivables_prev") / rev_p)
    d["gmi"] = (gp_p / rev_p) / (gp / rev)
    d["aqi"] = (1 - (g("current_assets") + g("ppe") + sec) / g("total_assets")) / (
        1 - (g("current_assets_prev") + g("ppe_prev") + sec_p) / g("total_assets_prev"))
    d["sgi"] = rev / rev_p
    dep, dep_p = g("depreciation"), g("depreciation_prev")
    d["depi"] = (dep_p / (dep_p + g("ppe_prev"))) / (dep / (dep + g("ppe")))
    d["sgai"] = (g("sga") / rev) / (g("sga_prev") / rev_p)
    d["lvgi"] = ((g("current_liabilities") + ltd) / g("total_assets")) / (
        (g("current_liabilities_prev") + ltd_p) / g("total_assets_prev"))
    d["tata"] = (g("net_income") - g("operating_cash_flow")) / g("total_assets")
    d = d.replace([np.inf, -np.inf], np.nan)
    d["m_score"] = (-4.84 + 0.920 * d.dsri + 0.528 * d.gmi + 0.404 * d.aqi + 0.892 * d.sgi + 0.115 * d.depi
                    - 0.172 * d.sgai + 4.679 * d.tata - 0.327 * d.lvgi)
    return d


# --- labels -------------------------------------------------------------------------------------------

def count_between(cik: pd.Series, at: pd.Series, ev_cik: pd.Series, ev_date: pd.Series,
                  start_days: int | None, end_days: int) -> np.ndarray:
    """For each (cik, at), how many events of that cik fall in [at + start_days, at + end_days).
    start_days=None means since the beginning of the data."""
    by = {c: np.sort(g.values.astype("datetime64[D]"))
          for c, g in pd.to_datetime(pd.Series(ev_date.values)).groupby(ev_cik.values)}
    empty = np.array([], "datetime64[D]")
    at = pd.to_datetime(at).values.astype("datetime64[D]")
    out = np.zeros(len(at), dtype=int)
    for i, (c, t) in enumerate(zip(cik.values, at)):
        d = by.get(c, empty)
        lo = 0 if start_days is None else np.searchsorted(d, t + np.timedelta64(start_days, "D"), "left")
        out[i] = np.searchsorted(d, t + np.timedelta64(end_days, "D"), "left") - lo
    return out


def warrant_wave(events: pd.DataFrame) -> pd.Series:
    """Item 4.02s whose text is about warrants or redeemable shares: the SPAC-era accounting questions."""
    flag = pd.Series(False, index=events.index)
    for c in ("mentions_warrants", "mentions_redeemable_shares"):
        if c in events:
            flag |= events[c].fillna(False).astype(bool)
    return flag


def nonreliance_labels(annual: pd.DataFrame, events: pd.DataFrame, exclude_wave: bool = False) -> pd.Series:
    """1 if an original Item 4.02 8-K for the company is filed from the day after the 10-K to day 730.
    exclude_wave ignores 4.02s about warrants or redeemable shares."""
    ev = events[(events.kind == "item_4_02") & (events.form == "8-K")]
    if exclude_wave:
        ev = ev[~warrant_wave(ev)]
    n = count_between(annual.cik, annual.filed, ev.cik, ev.filed, 1, HORIZON_DAYS + 1)
    return pd.Series((n > 0).astype(int), index=annual.index)


def adoption_effect(revisions: pd.DataFrame) -> pd.Series:
    """True for a revision of a line item in a fiscal year a retrospective standard changed for nearly everyone."""
    d = pd.to_datetime(revisions.ddate)
    hit = pd.Series(False, index=revisions.index)
    for concept, lo, hi, _ in ADOPTIONS:
        hit |= (revisions.concept == concept) & d.between(pd.Timestamp(lo), pd.Timestamp(hi))
    return hit


def revision_labels(annual: pd.DataFrame, revisions: pd.DataFrame, exclude_adoptions: bool = True) -> pd.Series:
    r = revisions[revisions.concept.isin(LABEL_CONCEPTS)
                  & (revisions.qtrs == revisions.concept.map(LABEL_CONCEPTS))
                  & ~np.isclose(revisions.value, -revisions.prev_value)
                  & (revisions.days_after_first <= HORIZON_DAYS)]
    if exclude_adoptions:
        r = r[~adoption_effect(r)]
    hit = set(zip(r.cik, pd.to_datetime(r.ddate)))
    return pd.Series([(c, p) in hit for c, p in zip(annual.cik, pd.to_datetime(annual.period))], index=annual.index).astype(int)


def names_period(annual: pd.DataFrame, events: pd.DataFrame) -> pd.Series:
    """1 if a later original Item 4.02 (within 24 months) names this 10-K's year-end date or fiscal year in its
    text. NaN where no 4.02 text was read. Used to check what the 24-month window catches."""
    ev = events[(events.kind == "item_4_02") & (events.form == "8-K") & events.dates_named.notna()]
    by = {c: g for c, g in ev.groupby("cik")}
    out = []
    for c, filed, period, fy in zip(annual.cik, pd.to_datetime(annual.filed), pd.to_datetime(annual.period), annual.fy):
        g = by.get(c)
        if g is None:
            out.append(0)
            continue
        g = g[(pd.to_datetime(g.filed) > filed) & (pd.to_datetime(g.filed) <= filed + pd.Timedelta(days=HORIZON_DAYS))]
        day = period.strftime("%Y-%m-%d")
        out.append(int(any(day in (d or "").split(",") or str(fy) in (y or "").split(",")
                           for d, y in zip(g.dates_named, g.fiscal_years_named))))
    return pd.Series(out, index=annual.index)


# --- features -----------------------------------------------------------------------------------------

def prior_revisions(annual: pd.DataFrame, revisions: pd.DataFrame) -> pd.Series:
    """Revisions the company had filed strictly before this 10-K: known on the day, so fair to use."""
    r = revisions[~np.isclose(revisions.value, -revisions.prev_value)]
    return pd.Series(count_between(annual.cik, annual.filed, r.cik, r.filed, None, 0), index=annual.index)


def event_features(a: pd.DataFrame, events: pd.DataFrame, filings: pd.DataFrame) -> pd.DataFrame:
    """Counts of things that happened strictly before the 10-K's filing date."""
    x = pd.DataFrame(index=a.index)
    k = events.kind
    nr, au, nt = events[k == "item_4_02"], events[k == "item_4_01"], events[k == "nt_10k"]
    nr = nr[nr.form == "8-K"]
    x["prior_nonreliance"] = count_between(a.cik, a.filed, nr.cik, nr.filed, None, 0)
    x["nonreliance_24m"] = count_between(a.cik, a.filed, nr.cik, nr.filed, -HORIZON_DAYS, 0)
    x["auditor_changes_24m"] = count_between(a.cik, a.filed, au.cik, au.filed, -HORIZON_DAYS, 0)
    # a late-filing notice for this 10-K: filed after the year ended and before the 10-K
    x["late_notice"] = (count_between(a.cik, a.filed, nt.cik, nt.filed, None, 0)
                        - count_between(a.cik, a.period, nt.cik, nt.filed, None, 1))
    x["late_notices_3y"] = count_between(a.cik, a.period, nt.cik, nt.filed, -3 * 365, 1)
    amend = filings[filings.form.str.endswith("/A")]
    x["amendments_24m"] = count_between(a.cik, a.filed, amend.cik, amend.filed, -HORIZON_DAYS, 0)
    tenk = filings[filings.form.isin(["10-K", "10-KT"])]
    x["prior_10ks"] = count_between(a.cik, a.filed, tenk.cik, tenk.filed, None, 0)
    spac = filings[filings.sic == BLANK_CHECK]
    x["was_blank_check"] = (count_between(a.cik, a.filed, spac.cik, spac.filed, None, 0) > 0).astype(float)
    return x


def features(a: pd.DataFrame, revisions: pd.DataFrame, events: pd.DataFrame | None = None,
             filings: pd.DataFrame | None = None) -> pd.DataFrame:
    x = beneish(a)

    def ratio(n, d):
        return (n / d).replace([np.inf, -np.inf], np.nan)

    avg_assets = (a.total_assets + a.total_assets_prev.fillna(a.total_assets)) / 2
    x["log_assets"] = np.log10(a.total_assets.clip(lower=1))
    x["loss"] = (a.net_income < 0).astype(float).where(a.net_income.notna())
    x["revenue_growth"] = x.sgi - 1
    x["equity_to_assets"] = ratio(a.equity, a.total_assets)
    x["cash_to_assets"] = ratio(a.cash, a.total_assets)
    # Dechow et al. (2011) misstatement predictors that this data can compute
    x["soft_assets"] = ratio(a.total_assets - a.ppe.fillna(0) - a.cash.fillna(0), a.total_assets)
    x["ch_receivables"] = ratio(a.receivables - a.receivables_prev, avg_assets)
    x["ch_inventory"] = ratio(a.inventory - a.inventory_prev, avg_assets)
    x["ch_roa"] = ratio(a.net_income, a.total_assets) - ratio(a.net_income_prev, a.total_assets_prev)
    x["asset_growth"] = ratio(a.total_assets, a.total_assets_prev) - 1
    x["filing_lag_days"] = (pd.to_datetime(a.filed) - pd.to_datetime(a.period)).dt.days
    x["prior_revisions"] = prior_revisions(a, revisions)
    x["missing_items"] = a.filter(regex="^(revenue|net_income|total_assets|receivables|ppe|sga|depreciation)$").isna().sum(axis=1)
    for status in ("1-LAF", "2-ACC", "3-SRA", "4-NON", "5-SML"):
        x[f"afs_{status[2:]}"] = (a.afs == status).astype(float)
    x["sic_division"] = pd.to_numeric(a.sic, errors="coerce") // 1000
    if events is not None and filings is not None:
        x = x.join(event_features(a, events, filings))
    return x


# --- evaluation ---------------------------------------------------------------------------------------

def precision_at(y: np.ndarray, score: np.ndarray, frac: float) -> float:
    k = max(1, int(len(y) * frac))
    top = np.argsort(-np.nan_to_num(score, nan=-np.inf), kind="stable")[:k]
    return float(y[top].mean())


def evaluate(y: np.ndarray, s: np.ndarray) -> dict:
    return {"auc": round(roc_auc_score(y, s), 4), "avg_precision": round(average_precision_score(y, s), 4),
            "precision_top5pct": round(precision_at(y, s, 0.05), 4), "precision_top10pct": round(precision_at(y, s, 0.10), 4)}


def auc_gap_ci(y: np.ndarray, a: np.ndarray, b: np.ndarray, groups: np.ndarray, reps: int = 300, seed: int = 0) -> dict:
    """AUC(a) - AUC(b) with a 95% interval from resampling whole companies, since a company's years are not independent."""
    rng = np.random.default_rng(seed)
    idx = list(pd.Series(np.arange(len(y))).groupby(groups).apply(lambda g: g.values))
    gaps = []
    for _ in range(reps):
        pick = np.concatenate([idx[i] for i in rng.integers(0, len(idx), len(idx))])
        if 0 < y[pick].sum() < len(pick):
            gaps.append(roc_auc_score(y[pick], a[pick]) - roc_auc_score(y[pick], b[pick]))
    return {"auc_gap": round(float(roc_auc_score(y, a) - roc_auc_score(y, b)), 4),
            "ci95": [round(float(np.percentile(gaps, 2.5)), 4), round(float(np.percentile(gaps, 97.5)), 4)]}


def gbm(**kw) -> HistGradientBoostingClassifier:
    p = dict(max_iter=400, learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0, class_weight="balanced",
             random_state=0) | kw
    return HistGradientBoostingClassifier(**p)


# candidate models, compared on the validation split only
CANDIDATES = {
    "gbm_original_features": lambda cols: (gbm(), [c for c in cols if c in ORIGINAL]),
    "gbm_all_features": lambda cols: (gbm(), cols),
    "gbm_all_features_regularised": lambda cols: (gbm(max_iter=300, learning_rate=0.03, max_leaf_nodes=15,
                                                      min_samples_leaf=200, l2_regularization=5.0), cols),
    "logistic_all_features": lambda cols: (make_pipeline(StandardScaler(), LogisticRegression(
        max_iter=3000, C=0.1, class_weight="balanced")), cols),
}
ORIGINAL = BENEISH + ["m_score", "log_assets", "loss", "revenue_growth", "equity_to_assets", "cash_to_assets",
                      "filing_lag_days", "prior_revisions", "missing_items", "afs_LAF", "afs_ACC", "afs_SRA", "afs_NON",
                      "afs_SML", "sic_division"]


def prepared(model, x: pd.DataFrame, train_mask: np.ndarray) -> pd.DataFrame:
    """Logistic regression needs no NaNs and bounded inputs: clip and fill with training medians."""
    if isinstance(model, HistGradientBoostingClassifier):
        return x
    lo, hi = x[train_mask].quantile(0.01), x[train_mask].quantile(0.99)
    return x.clip(lo, hi, axis=1).fillna(x[train_mask].median()).fillna(0)


def fit_predict(name: str, x: pd.DataFrame, y: np.ndarray, train: np.ndarray, test: np.ndarray):
    model, cols = CANDIDATES[name](list(x.columns))
    xx = prepared(model, x[cols], train)
    model.fit(xx[train], y[train])
    return model, cols, model.predict_proba(xx[test])[:, 1]


def compare(x: pd.DataFrame, y: np.ndarray, train: np.ndarray, test: np.ndarray, comp: np.ndarray, best: str,
            groups: np.ndarray) -> dict:
    """The chosen model against Beneish (published and refitted), on all test rows and where M is computable."""
    model, cols, p = fit_predict(best, x, y, train, test)
    m = x.m_score.values
    clip = x[BENEISH].clip(-10, 10).fillna(x[BENEISH][train].median())
    lr = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced")).fit(clip[train], y[train])
    p_lr = lr.predict_proba(clip[test])[:, 1]
    m_test = np.where(np.isnan(m[test]), np.nanmedian(m[train]), m[test])
    yt, ct = y[test], comp[test]
    no_hist = x.drop(columns=[c for c in HISTORY if c in x])
    _, _, p_nh = fit_predict(best, no_hist, y, train, test)
    out = {
        "rows": {"train": int(train.sum()), "test": int(test.sum()), "positives_train": int(y[train].sum()),
                 "positives_test": int(yt.sum()), "positive_rate_train": round(float(y[train].mean()), 4),
                 "positive_rate_test": round(float(yt.mean()), 4), "m_score_computable_test": round(float(ct.mean()), 4)},
        "test_all": {"model": evaluate(yt, p), "model_without_history": evaluate(yt, p_nh),
                     "beneish_m_score": evaluate(yt, m_test), "beneish_ratios_refit": evaluate(yt, p_lr),
                     "base_rate": round(float(yt.mean()), 4)},
        "test_m_score_computable": {"model": evaluate(yt[ct], p[ct]), "model_without_history": evaluate(yt[ct], p_nh[ct]),
                                    "beneish_m_score": evaluate(yt[ct], m[test][ct]),
                                    "beneish_ratios_refit": evaluate(yt[ct], p_lr[ct]),
                                    "base_rate": round(float(yt[ct].mean()), 4), "rows": int(ct.sum()),
                                    "positives": int(yt[ct].sum())},
    }
    g = groups[test]
    out["model_minus_m_score_auc"] = {"all_test_rows": auc_gap_ci(yt, p, m_test, g),
                                      "m_score_computable": auc_gap_ci(yt[ct], p[ct], m[test][ct], g[ct])}
    xt = prepared(model, x[cols], train)[test]
    imp = permutation_importance(model, xt, yt, scoring="roc_auc", n_repeats=5, random_state=0)
    out["importance_auc_drop"] = {c: round(float(v), 4) for c, v in
                                  sorted(zip(cols, imp.importances_mean), key=lambda kv: -kv[1])[:12]}
    return out


def load() -> tuple[pd.DataFrame, ...]:
    from .pipeline import read, spark_session

    spark = spark_session()
    annual = read(spark, "gold.annual").toPandas()
    rev = read(spark, "gold.revisions").select("cik", "concept", "ddate", "qtrs", "filed", "value", "prev_value",
                                               "days_after_first").toPandas()
    events = read(spark, "silver.events").toPandas()
    filings = read(spark, "gold.filings").select("cik", "sic", "form", "filed").toPandas()
    return annual, rev, events, filings


def main() -> None:
    t0 = time.time()
    annual, rev, events, filings = load()
    t_load = time.time() - t0
    annual = annual[annual.total_assets.notna() & (annual.total_assets > 0)]
    n_spac = int((annual.sic == BLANK_CHECK).sum())
    annual = annual[annual.sic != BLANK_CHECK].reset_index(drop=True)
    events_end = pd.to_datetime(events.filed).max()
    filed = pd.to_datetime(annual.filed)
    matured = filed + pd.Timedelta(days=HORIZON_DAYS) <= events_end

    x = features(annual, rev, events, filings)
    comp = x[BENEISH].notna().all(axis=1).values
    targets = {"nonreliance": nonreliance_labels(annual, events).values,
               "nonreliance_ex_warrants": nonreliance_labels(annual, events, exclude_wave=True).values,
               "nonreliance_names_this_year": names_period(annual, events).values,
               "revision": revision_labels(annual, rev).values}
    y_rev_all = revision_labels(annual, rev, exclude_adoptions=False).values

    train = (filed <= TRAIN_LAST).values
    test = (filed.between(TEST_FIRST, TEST_LAST) & matured).values
    v_train = (filed <= VALID[0]).values
    v_test = filed.between(VALID[1], VALID[2]).values

    report: dict = {"data": {
        "company_years": int(len(annual)), "blank_check_company_years_left_out": n_spac,
        "events_through": str(events_end.date()), "test_filed_through": str(filed[test].max().date()),
        "item_4_02_original_8ks": int(((events.kind == "item_4_02") & (events.form == "8-K")).sum()),
    }}
    yr = filed.dt.year
    for name, y in targets.items():
        report["data"][f"{name}_rate_by_filing_year"] = {int(k): round(float(v), 4) for k, v in
                                                          pd.Series(y).groupby(yr.values).mean().items()}
    report["data"]["revision_positives_removed_as_standard_adoption"] = int((y_rev_all & (1 - targets["revision"])).sum())

    for name, y in targets.items():
        valid = {}
        for cand in CANDIDATES:
            _, _, p = fit_predict(cand, x, y, v_train, v_test)
            valid[cand] = evaluate(y[v_test], p)
        best = max(valid, key=lambda k: valid[k]["avg_precision"])
        report[name] = {"validation": {"train_rows": int(v_train.sum()), "valid_rows": int(v_test.sum()),
                                       "valid_base_rate": round(float(y[v_test].mean()), 4), **valid, "chosen": best},
                        **compare(x, y, train, test, comp, best, annual.cik.values)}

    # what the 24-month window catches: does the 4.02 text name this 10-K's year, and how soon does it come?
    ev402 = events[(events.kind == "item_4_02") & (events.form == "8-K")]
    within = lambda lo, hi: count_between(annual.cik, annual.filed, ev402.cik, ev402.filed, lo, hi) > 0
    pos, named = targets["nonreliance"] == 1, targets["nonreliance_names_this_year"] == 1
    first12, later = within(1, 366), within(366, HORIZON_DAYS + 1) & ~within(1, 366)
    wave = ev402[warrant_wave(ev402)]
    wave_label = count_between(annual.cik, annual.filed, wave.cik, wave.filed, 1, HORIZON_DAYS + 1) > 0
    report["nonreliance_validation"] = {
        "item_4_02_texts_read": int(events[(events.kind == "item_4_02") & events.dates_named.notna()].adsh.nunique()),
        "positives_whose_4_02_names_this_year": round(float(named[pos].mean()), 4),
        "of_those_with_4_02_within_12_months": round(float(named[first12].mean()), 4),
        "of_those_with_4_02_only_in_months_13_to_24": round(float(named[later].mean()), 4),
        "share_of_positives_with_4_02_within_12_months": round(float(first12[pos].mean()), 4),
        "test_positives_with_a_warrant_or_redeemable_share_4_02": round(float(wave_label[test & pos].mean()), 4),
    }
    report["timing_seconds"] = {"load_from_lake": round(t_load), "total": round(time.time() - t0)}
    REPORT.parent.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
