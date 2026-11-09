"""Which annual reports will have their numbers revised? A point-in-time model against the Beneish M-score.

    python -m filings.model          # writes reports/model.json and prints the comparison

Label: a company-year is positive if a later filing (within 24 months of the original 10-K) changes
that year's net income, operating cash flow, total assets or equity by more than 1% and $100K.
Exact sign flips are excluded: those are XBRL tagging fixes (a loss tagged as a profit), not a change
to the accounts. Revenue is excluded because moving a business to discontinued operations restates
prior revenue routinely without anything having been wrong.

Features are only what was known on the 10-K's filing date: that 10-K's own figures and its prior-year
comparatives, how late it was filed, filer status, and how many revisions the company had filed before.

Split by filing date: train on filings up to 2019, test on 2021-2024 (2020 is a gap year, so no
label from the test period leaks into training; filings after 2024 don't yet have 24 months of
history to label them).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .pipeline import read, spark_session

REPORT = Path(__file__).resolve().parent.parent / "reports" / "model.json"
LABEL_CONCEPTS = {"net_income": 4, "operating_cash_flow": 4, "total_assets": 0, "equity": 0}
HORIZON_DAYS = 730
BENEISH = ["dsri", "gmi", "aqi", "sgi", "depi", "sgai", "tata", "lvgi"]


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


def labels(annual: pd.DataFrame, revisions: pd.DataFrame) -> pd.Series:
    r = revisions[revisions.concept.isin(LABEL_CONCEPTS)
                  & (revisions.qtrs == revisions.concept.map(LABEL_CONCEPTS))
                  & ~np.isclose(revisions.value, -revisions.prev_value)
                  & (revisions.days_after_first <= HORIZON_DAYS)]
    hit = set(zip(r.cik, pd.to_datetime(r.ddate)))
    return pd.Series([(c, p) in hit for c, p in zip(annual.cik, pd.to_datetime(annual.period))], index=annual.index).astype(int)


def prior_revisions(annual: pd.DataFrame, revisions: pd.DataFrame) -> pd.Series:
    """Revisions the company had filed strictly before this 10-K: known on the day, so fair to use."""
    r = revisions[~np.isclose(revisions.value, -revisions.prev_value)]
    by = {c: np.sort(g.filed.values.astype("datetime64[D]")) for c, g in r.groupby("cik")}
    filed = annual.filed.values.astype("datetime64[D]")
    return pd.Series([np.searchsorted(by.get(c, np.array([], "datetime64[D]")), f) for c, f in zip(annual.cik, filed)],
                     index=annual.index)


def features(a: pd.DataFrame, revisions: pd.DataFrame) -> pd.DataFrame:
    x = beneish(a)
    x["log_assets"] = np.log10(a.total_assets.clip(lower=1))
    x["loss"] = (a.net_income < 0).astype(float).where(a.net_income.notna())
    x["accruals"] = x.tata
    x["revenue_growth"] = x.sgi - 1
    x["equity_to_assets"] = (a.equity / a.total_assets).replace([np.inf, -np.inf], np.nan)
    x["cash_to_assets"] = (a.cash / a.total_assets).replace([np.inf, -np.inf], np.nan)
    x["filing_lag_days"] = (pd.to_datetime(a.filed) - pd.to_datetime(a.period)).dt.days
    x["prior_revisions"] = prior_revisions(a, revisions)
    x["missing_items"] = a.filter(regex="^(revenue|net_income|total_assets|receivables|ppe|sga|depreciation)$").isna().sum(axis=1)
    for status in ("1-LAF", "2-ACC", "3-SRA", "4-NON", "5-SML"):
        x[f"afs_{status[2:]}"] = (a.afs == status).astype(float)
    x["sic_division"] = pd.to_numeric(a.sic, errors="coerce") // 1000
    return x


def precision_at(y: np.ndarray, score: np.ndarray, frac: float) -> float:
    k = max(1, int(len(y) * frac))
    top = np.argsort(-np.nan_to_num(score, nan=-np.inf))[:k]
    return float(y[top].mean())


def evaluate(y: np.ndarray, s: np.ndarray) -> dict:
    return {"auc": round(roc_auc_score(y, s), 4), "avg_precision": round(average_precision_score(y, s), 4),
            "precision_top5pct": round(precision_at(y, s, 0.05), 4), "precision_top10pct": round(precision_at(y, s, 0.10), 4)}


def load() -> tuple[pd.DataFrame, pd.DataFrame]:
    spark = spark_session()
    annual = read(spark, "gold.annual").toPandas()
    rev = read(spark, "gold.revisions").select("cik", "concept", "ddate", "qtrs", "filed", "value", "prev_value",
                                               "days_after_first").toPandas()
    return annual, rev


def main() -> None:
    annual, rev = load()
    annual = annual[annual.total_assets.notna() & (annual.total_assets > 0)].reset_index(drop=True)
    x = features(annual, rev)
    y = labels(annual, rev).values
    year = pd.to_datetime(annual.filed).dt.year
    train, test = (year <= 2019).values, ((year >= 2021) & (year <= 2024)).values

    gbm = HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0,
                                         class_weight="balanced", random_state=0).fit(x[train], y[train])
    p_gbm = gbm.predict_proba(x[test])[:, 1]
    # Beneish as published, and a fair version: the same 8 ratios refitted on our training years
    m = x.m_score.values
    comp = x[BENEISH].notna().all(axis=1).values
    clip = x[BENEISH].clip(-10, 10).fillna(x[BENEISH][train].median())
    lr = make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, class_weight="balanced")).fit(clip[train], y[train])
    p_lr = lr.predict_proba(clip[test])[:, 1]
    m_test = np.where(np.isnan(m[test]), np.nanmedian(m[train]), m[test])

    yt = y[test]
    report = {
        "rows": {"company_years": int(len(annual)), "train": int(train.sum()), "test": int(test.sum()),
                 "positive_rate_train": round(float(y[train].mean()), 4), "positive_rate_test": round(float(yt.mean()), 4),
                 "m_score_computable_test": round(float(comp[test].mean()), 4)},
        "test_all": {"model": evaluate(yt, p_gbm), "beneish_m_score": evaluate(yt, m_test),
                     "beneish_ratios_refit": evaluate(yt, p_lr), "base_rate": round(float(yt.mean()), 4)},
        "test_m_score_computable": {
            "model": evaluate(yt[comp[test]], p_gbm[comp[test]]),
            "beneish_m_score": evaluate(yt[comp[test]], m[test][comp[test]]),
            "base_rate": round(float(yt[comp[test]].mean()), 4)},
    }
    imp = permutation_importance(gbm, x[test], yt, scoring="roc_auc", n_repeats=3, random_state=0)
    report["importance_auc_drop"] = {c: round(float(v), 4) for c, v in
                                     sorted(zip(x.columns, imp.importances_mean), key=lambda kv: -kv[1])[:12]}
    # does it work without the company's own revision history? (the obvious "they did it before" signal)
    no_hist = [c for c in x.columns if c != "prior_revisions"]
    g2 = HistGradientBoostingClassifier(max_iter=400, learning_rate=0.05, max_leaf_nodes=31, l2_regularization=1.0,
                                        class_weight="balanced", random_state=0).fit(x[train][no_hist], y[train])
    report["test_all"]["model_without_history"] = evaluate(yt, g2.predict_proba(x[test][no_hist])[:, 1])
    REPORT.parent.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
