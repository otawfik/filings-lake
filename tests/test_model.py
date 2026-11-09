"""The model's building blocks on hand-made data: Beneish maths, labels, point-in-time features."""
import numpy as np
import pandas as pd
import pytest

from filings.concepts import CONCEPTS, PRIORITY, TAGS
from filings.ingest import quarters
from filings.model import beneish, labels, precision_at, prior_revisions


def test_quarters_span_years():
    assert quarters("2012q3", "2013q2") == ["2012q3", "2012q4", "2013q1", "2013q2"]


def test_every_tag_maps_to_exactly_one_concept_with_a_priority():
    assert len(TAGS) == len(PRIORITY) == sum(len(t) for _, t in CONCEPTS.values())
    assert PRIORITY["Revenues"] == ("revenue", 0)
    assert PRIORITY["RevenueFromContractWithCustomerExcludingAssessedTax"][0] == "revenue"


def company(**over):
    base = dict(revenue=1000, revenue_prev=800, cost_of_revenue=600, cost_of_revenue_prev=500,
                receivables=200, receivables_prev=120, current_assets=500, current_assets_prev=450,
                ppe=300, ppe_prev=280, total_assets=1000, total_assets_prev=900, depreciation=30, depreciation_prev=30,
                sga=100, sga_prev=90, current_liabilities=200, current_liabilities_prev=180, long_term_debt=100,
                long_term_debt_prev=100, net_income=80, operating_cash_flow=20)
    return pd.DataFrame([base | over])


def test_beneish_ratios_by_hand():
    b = beneish(company()).iloc[0]
    assert b.dsri == pytest.approx((200 / 1000) / (120 / 800))
    assert b.sgi == pytest.approx(1.25)
    assert b.gmi == pytest.approx((300 / 800) / (400 / 1000))
    assert b.tata == pytest.approx((80 - 20) / 1000)
    assert b.lvgi == pytest.approx((300 / 1000) / (280 / 900))
    m = (-4.84 + .92 * b.dsri + .528 * b.gmi + .404 * b.aqi + .892 * b.sgi + .115 * b.depi - .172 * b.sgai
         + 4.679 * b.tata - .327 * b.lvgi)
    assert b.m_score == pytest.approx(m)


def test_beneish_is_nan_not_inf_on_zero_denominators():
    b = beneish(company(revenue_prev=0)).iloc[0]
    assert np.isnan(b.sgi) and np.isnan(b.m_score)


def rev(cik, ddate, concept, qtrs, prev, value, days, filed="2020-01-01"):
    return dict(cik=cik, ddate=pd.Timestamp(ddate), concept=concept, qtrs=qtrs, prev_value=prev, value=value,
                days_after_first=days, filed=pd.Timestamp(filed))


def test_labels_count_real_revisions_and_skip_sign_flips_revenue_and_late_ones():
    annual = pd.DataFrame({"cik": [1, 2, 3, 4, 5], "period": pd.to_datetime(["2019-12-31"] * 5)})
    revs = pd.DataFrame([
        rev(1, "2019-12-31", "net_income", 4, 100, 90, 365),     # real revision
        rev(2, "2019-12-31", "net_income", 4, 100, -100, 365),   # sign flip: a tagging fix
        rev(3, "2019-12-31", "revenue", 4, 100, 90, 365),        # revenue: discontinued-ops reclassification
        rev(4, "2019-12-31", "equity", 0, 100, 90, 900),         # after the 24-month horizon
        rev(5, "2019-12-31", "net_income", 1, 100, 90, 100),     # a quarter, not the year
    ])
    assert labels(annual, revs).tolist() == [1, 0, 0, 0, 0]


def test_prior_revisions_only_counts_what_was_filed_before():
    annual = pd.DataFrame({"cik": [1, 1, 2], "filed": pd.to_datetime(["2019-03-01", "2021-03-01", "2021-03-01"])})
    revs = pd.DataFrame([rev(1, "2018-12-31", "equity", 0, 1, 2, 10, "2019-05-01"),
                         rev(1, "2018-12-31", "equity", 0, 2, 3, 10, "2020-05-01"),
                         rev(1, "2018-12-31", "equity", 0, 3, -3, 10, "2020-06-01")])   # sign flip: ignored
    assert prior_revisions(annual, revs).tolist() == [0, 2, 0]


def test_precision_at_top_fraction():
    y = np.array([1, 0, 1, 0, 0, 0, 0, 0, 0, 0])
    s = np.array([.9, .8, .7, .1, .1, .1, .1, .1, .1, .1])
    assert precision_at(y, s, 0.2) == 0.5 and precision_at(y, s, 0.3) == pytest.approx(2 / 3)
