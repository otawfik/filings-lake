"""The Item 4.02 label, the event features and the EDGAR search plumbing, on hand-made data."""
from datetime import date

import pandas as pd

from filings import edgar
from filings.model import (count_between, event_features, names_period, nonreliance_labels, revision_labels)

T = pd.Timestamp


def ev(cik, filed, kind="item_4_02", form="8-K", dates="", years=""):
    return dict(kind=kind, cik=cik, filed=T(filed), form=form, adsh=f"{cik}-{filed}-{kind}-{form}",
                dates_named=dates, fiscal_years_named=years)


def test_count_between_is_start_inclusive_end_exclusive():
    at = pd.Series([T("2020-03-01")])
    d = pd.Series([T("2020-03-01"), T("2020-03-02"), T("2022-03-01"), T("2022-03-02")])
    c = pd.Series([1, 1, 1, 1])
    assert count_between(pd.Series([1]), at, c, d, 1, 731).tolist() == [2]   # day after through day 730
    assert count_between(pd.Series([1]), at, c, d, None, 0).tolist() == [0]  # strictly before
    assert count_between(pd.Series([1]), at, c, d, None, 1).tolist() == [1]  # on or before


def test_nonreliance_label_window_and_what_counts():
    annual = pd.DataFrame({"cik": [1, 2, 3, 4, 5, 6, 7], "filed": pd.to_datetime(["2020-03-01"] * 7)})
    events = pd.DataFrame([
        ev(1, "2020-03-02"),                       # the day after: counts
        ev(2, "2020-03-01"),                       # same day as the 10-K: about earlier statements
        ev(3, "2022-02-28"),                       # day 729: counts
        ev(4, "2022-03-02"),                       # day 731: too late
        ev(5, "2020-06-01", form="8-K/A"),         # an amendment, maybe of a 4.02 filed before the 10-K
        ev(6, "2020-06-01", kind="item_4_01"),     # an auditor change is not a non-reliance
        ev(99, "2020-06-01"),                      # another company
        ev(7, "2019-06-01"),                       # before the 10-K
    ])
    assert nonreliance_labels(annual, events).tolist() == [1, 0, 1, 0, 0, 0, 0]


def test_revision_label_drops_standard_adoption_years():
    annual = pd.DataFrame({"cik": [1, 2, 3], "period": pd.to_datetime(["2016-12-31", "2016-12-31", "2019-12-31"])})
    revs = pd.DataFrame([
        dict(cik=1, ddate=T("2016-12-31"), concept="operating_cash_flow", qtrs=4, prev_value=100, value=90, days_after_first=365),
        dict(cik=2, ddate=T("2016-12-31"), concept="net_income", qtrs=4, prev_value=100, value=90, days_after_first=365),
        dict(cik=3, ddate=T("2019-12-31"), concept="operating_cash_flow", qtrs=4, prev_value=100, value=90, days_after_first=365),
    ])
    # OCF for FY2016 changed for most companies under ASU 2016-09/15/18; net income in 2016 was not in a window
    assert revision_labels(annual, revs).tolist() == [0, 1, 1]
    assert revision_labels(annual, revs, exclude_adoptions=False).tolist() == [1, 1, 1]


def test_event_features_only_see_the_past():
    a = pd.DataFrame({"cik": [1, 1], "period": pd.to_datetime(["2019-12-31", "2020-12-31"]),
                      "filed": pd.to_datetime(["2020-04-15", "2021-03-01"])})
    events = pd.DataFrame([
        ev(1, "2018-03-01"),                        # an old 4.02: in prior_nonreliance, out of the 24 months
        ev(1, "2020-04-15"),                        # same day as the first 10-K: not yet "before" it
        ev(1, "2020-03-31", kind="nt_10k"),         # late notice for the FY2019 10-K
        ev(1, "2019-01-10", kind="item_4_01"),      # auditor change
    ])
    filings = pd.DataFrame({"cik": [1, 1, 1], "sic": ["6770", "1000", "1000"], "form": ["10-K", "10-Q/A", "10-K"],
                            "filed": pd.to_datetime(["2018-03-01", "2020-01-10", "2020-04-15"])})
    x = event_features(a, events, filings)
    assert x.prior_nonreliance.tolist() == [1, 2]
    assert x.nonreliance_24m.tolist() == [0, 1]
    assert x.late_notice.tolist() == [1, 0]
    assert x.late_notices_3y.tolist() == [0, 1]       # earlier years' notices
    assert x.auditor_changes_24m.tolist() == [1, 0]     # 2019-01-10 is more than 730 days before 2021-03-01
    assert x.amendments_24m.tolist() == [1, 1]
    assert x.prior_10ks.tolist() == [1, 2]
    assert x.was_blank_check.tolist() == [1.0, 1.0]


def test_names_period_reads_dates_and_fiscal_years():
    annual = pd.DataFrame({"cik": [1, 2, 3], "filed": pd.to_datetime(["2020-03-01"] * 3),
                           "period": pd.to_datetime(["2019-12-31", "2019-06-30", "2019-12-31"]), "fy": [2019, 2019, 2019]})
    events = pd.DataFrame([ev(1, "2020-11-01", dates="2019-12-31,2020-06-30"),
                           ev(2, "2020-11-01", years="2019"),
                           ev(3, "2020-11-01", dates="2020-06-30")])        # only a later quarter
    assert names_period(annual, events).tolist() == [1, 1, 0]


def test_rows_keep_only_filings_that_declare_the_item():
    hits = [
        {"_id": "A:a8k.htm", "_source": {"adsh": "A", "ciks": ["0000000001", "0000000002"], "file_date": "2021-05-03",
                                         "form": "8-K", "file_type": "8-K", "items": ["4.02", "9.01"], "sics": ["7372"]}},
        {"_id": "A:ex99.htm", "_source": {"adsh": "A", "ciks": ["0000000001"], "file_date": "2021-05-03",
                                          "form": "8-K", "file_type": "EX-99.1", "items": ["4.02", "9.01"]}},
        {"_id": "B:b.htm", "_source": {"adsh": "B", "ciks": ["0000000003"], "file_date": "2021-05-03",
                                       "form": "8-K", "file_type": "8-K", "items": ["8.01"]}},   # mentions 4.02 in text only
    ]
    r = edgar.rows(hits, "4.02")
    assert sorted((x["adsh"], x["cik"]) for x in r) == [("A", 1), ("A", 2)]
    assert {x["main_doc"] for x in r} == {"a8k.htm"}


def test_months_cover_the_range_without_gaps():
    m = list(edgar.months(date(2020, 1, 15), date(2020, 3, 10)))
    assert m == [(date(2020, 1, 15), date(2020, 1, 31)), (date(2020, 2, 1), date(2020, 2, 29)),
                 (date(2020, 3, 1), date(2020, 3, 10))]


def test_search_splits_ranges_that_hit_the_cap(monkeypatch):
    calls = []

    def fake_get(s, params):
        calls.append((params["startdt"], params["enddt"], params.get("from", 0)))
        days = (date.fromisoformat(params["enddt"]) - date.fromisoformat(params["startdt"])).days + 1
        if days > 10:
            return {"hits": {"total": {"value": 10_000, "relation": "gte"}, "hits": []}}
        n, frm = 150, params.get("from", 0)
        return {"hits": {"total": {"value": n, "relation": "eq"},
                         "hits": [{"_id": f"{params['startdt']}-{i}"} for i in range(frm, min(n, frm + 100))]}}

    monkeypatch.setattr(edgar, "get", fake_get)
    hits = edgar.search(None, "8-K", '"4.02"', date(2020, 1, 1), date(2020, 1, 31))
    # 31 days -> 16 + 15 -> 8 + 8 + 8 + 7, each paged twice (150 results, 100 a page)
    assert len(hits) == 4 * 150 and len({h["_id"] for h in hits}) == 600


def test_read_nonreliance_finds_named_periods_and_warrants():
    html = ("<p>Item 4.01 Changes in accountant.</p><p>Item&nbsp;4.02 Non-Reliance on Previously Issued Financial "
            "Statements.</p><p>On May 3, 2021 the Audit Committee concluded that the audited financial statements "
            "as of and for the year ended December 31, 2020, and fiscal 2019, should no longer be relied upon "
            "because the Company's warrants should have been classified as liabilities. " + "x " * 120 + "</p>"
            "<p>Item 9.01 Exhibits. March 31, 2018</p>")
    r = edgar.read_nonreliance(edgar.plain(html))
    assert r["section_found"] and r["mentions_warrants"] and not r["mentions_redeemable_shares"]
    assert r["dates_named"] == "2020-12-31,2021-05-03"     # the 9.01 section's date is not read
    assert r["fiscal_years_named"] == "2019"


def test_nonreliance_label_can_ignore_the_warrant_wave():
    annual = pd.DataFrame({"cik": [1, 2], "filed": pd.to_datetime(["2021-03-01"] * 2)})
    events = pd.DataFrame([ev(1, "2021-05-01"), ev(2, "2021-05-01")])
    events["mentions_warrants"] = [True, False]
    events["mentions_redeemable_shares"] = [None, None]
    assert nonreliance_labels(annual, events).tolist() == [1, 1]
    assert nonreliance_labels(annual, events, exclude_wave=True).tolist() == [0, 1]


def test_lift_table_precision_lift_and_recall():
    from filings.findings import lift_table

    y = pd.Series([1, 1, 0, 0, 0, 0, 0, 0, 0, 0]).values           # base rate 20%
    flags = {"good": pd.Series([True, True, True, False, False, False, False, False, False, False]),
             "useless": pd.Series([False, True, False, True, False, True, False, True, False, True]),
             "never": pd.Series([False] * 10)}
    t = lift_table(flags, y)
    assert t["base_rate"] == 0.2
    assert t["signals"]["good"] == {"flagged": 3, "positives_among_flagged": 2, "precision": 0.6667, "lift": 3.33,
                                    "recall": 1.0, "share_flagged": 0.3}
    assert t["signals"]["useless"]["lift"] == 1.0
    assert t["signals"]["never"]["precision"] is None and t["signals"]["never"]["lift"] is None
    assert list(t["signals"]) == ["good", "useless", "never"]            # sorted by lift


def test_annual_revisions_take_the_first_change_after_the_10k():
    from filings.findings import annual_revisions

    annual = pd.DataFrame({"adsh": ["k1"], "cik": [1], "period": [T("2019-12-31")], "filed": [T("2020-03-01")]})
    rev = pd.DataFrame([
        dict(cik=1, concept="net_income", qtrs=4, ddate=T("2019-12-31"), filed=T("2021-03-01"), value=90, prev_value=100),
        dict(cik=1, concept="net_income", qtrs=4, ddate=T("2019-12-31"), filed=T("2020-08-01"), value=95, prev_value=100),
        dict(cik=1, concept="net_income", qtrs=1, ddate=T("2019-12-31"), filed=T("2020-05-01"), value=20, prev_value=25),  # a quarter
        dict(cik=1, concept="total_assets", qtrs=0, ddate=T("2019-12-31"), filed=T("2020-02-01"), value=1, prev_value=2),  # before the 10-K
        dict(cik=1, concept="equity", qtrs=0, ddate=T("2019-12-31"), filed=T("2020-06-01"), value=-5, prev_value=5),  # sign flip
    ])
    r = annual_revisions(annual, rev)
    assert r.concept.tolist() == ["net_income"] and r.days.tolist() == [153] and r.rel_change.tolist() == [0.05]
