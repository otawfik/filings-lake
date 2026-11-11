# filings-lake

Every number in every SEC 10-K and 10-Q since 2012, turned into point-in-time financials on a
Databricks-style lakehouse (PySpark + Delta), with a model that predicts which annual reports a company
will later say can't be relied on, benchmarked against the Beneish M-score.

**Status: in progress.** Built so far: ingestion, the line-item mapping, the bronze/silver/gold
pipeline, the Item 4.02 event feed, the model and its tests, and the findings below. Still to come:
Databricks jobs, the Snowflake serving layer (Iceberg, dbt, Streams/Tasks) and a text-to-SQL agent with
an eval gate. Every number below was measured from the code in this repo; `reports/` holds the JSON it
was read from.

## Measured so far

- **17,600,102 facts** landed from **384,620** 10-K/10-Q filings (2012 Q1 to 2026 Q3, 59 quarterly SEC
  Financial Statement Data Sets), including **92,139** original 10-Ks.
- **Line-item coverage** (share of original 10-Ks in which the line item resolves for the fiscal year
  itself): total assets 97.9%, net income 97.5%, operating cash flow 96.6%, equity 95.1%, revenue 80.5%,
  receivables 54.3%. The gaps are real and expected: banks, insurers and REITs report revenue,
  inventory and cost of sales under different tags or not at all.
- **Pipeline output (full history):** 12,436,144 silver facts, 218,837 revisions, 91,958 company-years,
  384,620 filings; 47,037 event rows (8-K Item 4.02 and 4.01, NT 10-K). `python -m filings.pipeline`
  runs in **280 s** on a laptop (Spark local, 2 GB driver, 8 cores); `python -m filings.model` in **268 s**
(all four targets, model selection, baselines, bootstrap intervals and permutation importance).
- **Item 4.02 feed:** 5,198 8-K filings declaring Item 4.02 "Non-Reliance on Previously Issued Financial
  Statements" from 2009 to November 2026 (4,545 original 8-Ks, the rest 8-K/As), from EDGAR full-text
  search, keeping only filings whose header declares the item. Checked against EDGAR's own per-company
  filing index for 400 random 10-K filers: **116 of 116** Item 4.02 filings found, none extra
  (`reports/item_4_02_coverage.json`). The list is committed as `data/item_4_02.csv`.

### The label: an Item 4.02 within 24 months

A company-year (one original 10-K) is positive if the company files an original 8-K under Item 4.02 in
the 24 months after that 10-K's filing date (day 1 through day 730). Blank-check companies (SIC 6770)
are left out: in 2021 nearly every SPAC filed a 4.02 after the SEC staff statements on warrants and
redeemable shares, one accounting interpretation hitting a whole category.

What that window catches was measured, not assumed. The text of every non-SPAC Item 4.02 since 2009
(4,335 documents) was read for the period-end dates and fiscal years it names:

- **48.3%** of positive company-years have a 4.02 that names that 10-K's own year-end or fiscal year.
  Of those whose 4.02 came within 12 months, 64.3% name it; of those whose first 4.02 came in months
  13-24, only 29.9% do — the later ones are mostly about the *next* year's statements.
- So the label is a firm-level "this company will say its filed statements can't be relied on", not
  "this exact 10-K was wrong". A stricter target, *the 4.02 names this year*, is reported alongside.
- **20.9%** of the test-period positives have a 4.02 that mentions warrants or redeemable shares (the
  2021-22 SPAC-accounting questions, which also hit companies that had already completed a merger). A
  version of the label without those is reported too.

The old label, *a figure was later revised* (a later filing changed net income, operating cash flow,
total assets or equity by more than 1% and $100K under the same tag within 24 months), stays as the
second target, now without the years and line items where a retrospective standard moved most
companies' numbers at once (2,086 of its positives were standard adoptions; see `ADOPTIONS` in
`filings/model.py`).

### Model against Beneish

Split by filing date so no event is shared: **train** on 10-Ks filed 2012-2018 (their 24-month windows
close by the end of 2020), **test** on 10-Ks filed 2021 to November 2024 whose 24 months have passed.
Model and feature set were chosen on a validation split (train to 2015, validate on 2018), never on the
test years. Features are only what was known on the filing date: that 10-K's figures and prior-year
comparatives, how late it was filed, filer status, and the company's earlier revisions, 4.02s, auditor
changes, late-filing notices and amendments.

**Item 4.02 within 24 months** — 22,156 test 10-Ks, 984 positive (4.4%):

| | AUC | avg precision | precision, top 5% | precision, top 10% |
|---|---|---|---|---|
| model (gradient boosting) | **0.713** | **0.100** | **12.2%** | **11.7%** |
| model without the company's own history | 0.717 | 0.103 | 13.8% | 11.5% |
| **filing lag alone** (days from year-end to the 10-K, no model) | 0.698 | 0.081 | 10.3% | 9.7% |
| lateness only (lag + late-filing notices, gradient boosting) | 0.693 | 0.082 | 10.8% | 10.2% |
| model with every lateness feature removed | 0.694 | 0.085 | 9.8% | 9.5% |
| Beneish M-score (published coefficients) | 0.502 | 0.053 | 8.8% | 6.1% |
| the eight Beneish ratios, refitted | 0.578 | 0.064 | 7.1% | 7.2% |

On the 7,919 test 10-Ks where every Beneish input exists (342 positive, 4.3%): model 0.745 AUC / 14.9%
top-5% precision, M-score 0.511 / 9.1%, refitted ratios 0.601 / 8.9%. The AUC gap to the M-score is
0.211 (95% interval 0.187 to 0.238 from resampling whole companies). The published M-score does not
separate 4.02 filers from the rest: 0.50 AUC is a coin flip.

Same split, other targets:

| target | test positives | model AUC | M-score AUC | refit AUC | model top-5% | base rate |
|---|---|---|---|---|---|---|
| 4.02, excluding warrant / redeemable-share 4.02s | 804 (3.6%) | 0.731 | 0.500 | 0.582 | 11.5% | 3.6% |
| 4.02 that names this 10-K's year | 513 (2.3%) | 0.710 | 0.504 | 0.564 | 5.6% | 2.3% |
| revision (old label, adoptions removed) | 1,985 (9.0%) | 0.613 | 0.494 | 0.528 | 22.2% | 9.0% |

**What the model is actually using.** Permutation importance on the 4.02 target: filing lag (days from
year-end to the 10-K) costs 0.144 AUC when shuffled; everything else under 0.022 (equity/assets 0.021,
size 0.018, leverage index 0.008). The model is mostly a "late filers restate" rule with financial
ratios on top, and the baselines above measure exactly how much: **filing lag alone, with no model,
reaches 0.698 AUC**, so the full model adds only 0.015 AUC to it. The gain shows up where a screen is
judged: precision in the top 5% rises from 10.3% to 12.2%, and average precision from 0.081 to 0.100.
And lateness isn't the only signal: with every lateness feature removed the model still reaches 0.694,
so the financial statements carry nearly as much information on their own; the two overlap. The
no-history variant is as good as the full one, so "they did it before" adds nothing the lateness and
ratios don't already carry.

**Compared with the first result** (old label, old features: model 0.577 vs M-score 0.493): the
difference is almost entirely the label. The 4.02 label is the SEC's own restatement event rather than a
threshold on a number change, and the same features that barely beat Beneish on the old label beat it
clearly on this one.

## Findings

Universe for everything in this section: the **76,405** original 10-Ks outside SIC 6770, with total
assets, filed from January 2012 to November 2024 (so each had 24 months of follow-up by the last event
date). Each line gives its denominator. `python -m filings.findings` reproduces `reports/findings.json`.

### How often filed figures change

*Method: a figure a 10-K reported for its own fiscal year counts as changed if a later filing reported a
different value for the same company, period and tag, by more than 1% and $100K, within 24 months of
the 10-K. Sign flips (tagging fixes) are excluded. Changes forced by a new accounting standard are
included.*

- **11.9%** of 10-Ks (9,105 of 76,405) had at least one of net income, operating cash flow, total assets
  or equity changed within 24 months.
- **By line item** (share of reported figures changed; n reported): SG&A **10.2%** (60,013), operating
  income **9.8%** (56,656), gross profit 7.6% (28,406), cost of revenue 7.3% (37,640), revenue 5.9%
  (64,159), current assets 5.8% (59,242), operating cash flow 5.8% (74,797), receivables 5.8% (44,134),
  current liabilities 5.6% (58,720), inventory 5.0% (31,805), equity 5.0% (72,996), total liabilities
  4.8% (61,113), net income **4.6%** (75,247), depreciation 4.4%, PP&E 4.1%, long-term debt 3.8%,
  **total assets 3.7%** (76,405), cash 3.7%, capex 3.5%, securities 1.7% (5,158). Income-statement
  subtotals move most: reclassifying costs between lines (and discontinued operations) changes SG&A and
  operating income without changing net income. Balance-sheet totals move least.
- **Size of a change, when one happens** (median relative change): total assets 5.4%, total
  liabilities 6.0%, current assets 6.0%, operating cash flow 10.1%, SG&A 10.7%, revenue 14.6%, equity
  19.6%, net income **21.6%**, cash 28.7% (the ASU 2016-18 move to include restricted cash).
- **By fiscal year** (any of the four items changed; 10-Ks): FY2011 13.8% (5,150), FY2012 11.7%,
  FY2013 11.3%, FY2014 13.7%, **FY2015 16.2%**, **FY2016 16.6%**, **FY2017 16.3%**, FY2018 10.6%,
  FY2019 8.9%, FY2020 10.3%, FY2021 8.6%, FY2022 9.2%, FY2023 7.9% (5,526). The 2015-2017 bulge is
  the standards: deferred-tax and debt-cost presentation (ASU 2015-03/17), cash-flow classification
  (ASU 2016-09/15/18) and ASC 606 full-retrospective adopters. Outside those years the rate is 8-12%
  and drifting down.
- **By filer status: no difference.** Large accelerated 11.7% (25,023), accelerated 12.4% (14,488),
  non-accelerated 12.0% (19,436), smaller reporting 11.8% (17,384). Big companies' numbers change as
  often as small ones'.
- **When a change lands** (16,516 first changes to the four items; days after the 10-K): median
  **366 days**; p10 69, p25 240, p75 594, p90 735. **42.3%** land 301-450 days out — in the next
  10-K's comparative column — against 14.2% within 90 days and 14.3% in days 91-300. Most "revisions"
  are quietly recast prior-year columns, not amended filings.

### Which warning signs preceded an Item 4.02

*Method: for each sign known on the 10-K's filing date, the share of flagged 10-Ks followed by an
original Item 4.02 within 24 months (precision), divided by the base rate of 3.66% (2,794 of 76,405), and
the share of all 4.02 filers the sign caught (recall). Deciles are over the universe.*

| sign | flagged | precision | lift | recall |
|---|---|---|---|---|
| formerly a blank-check company | 1,908 | 14.0% | **3.8** | 9.6% |
| an Item 4.02 in the prior 24 months | 3,647 | 9.5% | **2.6** | 12.4% |
| 10-K filed more than 180 days after year-end | 1,535 | 8.8% | 2.4 | 4.8% |
| NT 10-K (late notice) filed for this 10-K | 9,779 | 8.4% | 2.3 | 29.5% |
| 10-K filed more than 90 days after year-end | 12,249 | 7.8% | 2.1 | 34.1% |
| top-decile asset growth | 7,539 | 7.2% | 2.0 | 19.4% |
| auditor change (Item 4.01) in the prior 24 months | 12,823 | 6.7% | 1.8 | 30.9% |
| top-decile receivables growth (over assets) | 4,301 | 6.6% | 1.8 | 10.2% |
| any earlier Item 4.02 | 10,438 | 6.6% | 1.8 | 24.5% |
| 10-K/A or 10-Q/A in the prior 24 months | 10,346 | 6.5% | 1.8 | 24.1% |
| Beneish M-score above -1.78 | 3,545 | 6.5% | 1.8 | 8.2% |
| negative equity | 13,370 | 6.1% | 1.7 | 29.4% |
| smallest decile by assets | 7,641 | 5.8% | 1.6 | 15.9% |
| top-decile accruals (NI − OCF, over assets) | 7,442 | 5.4% | 1.5 | 14.4% |
| smaller reporting / non-accelerated | 36,820 | 5.3% | 1.4 | 69.7% |
| net loss | 36,392 | 5.0% | 1.4 | 65.4% |
| first 10-K in the data | 11,348 | 4.9% | 1.3 | 20.0% |
| top-decile soft assets | 7,641 | 4.5% | 1.2 | 12.3% |
| three or more core line items missing | 12,950 | 4.2% | 1.2 | 19.6% |
| **any earlier revision of its figures** | 45,262 | 3.6% | **1.0** | 57.7% |
| large accelerated filer | 25,023 | 1.5% | 0.4 | 13.8% |
| largest decile by assets | 7,641 | 1.1% | 0.3 | 3.0% |

Three things a reader might not expect. **Lateness is the strongest broad signal**: a late-filing
notice more than doubles the odds and catches 30% of 4.02 filers; filing over 180 days late is 2.4×.
**A prior revision of the company's figures predicts nothing** (lift 1.0): quiet number changes and
formal non-reliance are different populations. **The classic accrual signals are weak**: top-decile
accruals 1.5×, the Beneish flag 1.8×, each catching under 15% of cases. Size works in reverse of the
"big frauds" intuition: the largest decile is at 0.3× the base rate.

### Beneish at its textbook cut-off

*Method: the published M-score with Beneish (1999) coefficients, flagging M > −1.78, on the 26,708
10-Ks where all eight ratios can be computed.*

- 13.3% of those 10-Ks (3,545) are flagged.
- **Item 4.02 within 24 months**: 229 of the 3,545 flagged (precision **6.5%**) against a base rate of
  3.6% among computable 10-Ks — lift 1.8, recall 23.7% (229 of 968).
- **Revision within 24 months**: 380 of 3,545 flagged (precision 10.7%) against 9.0% — lift 1.2, recall
  15.8%.

As a screen for "this company will file a non-reliance notice", the M-score at −1.78 is right one time
in fifteen and misses three-quarters of cases. It was built for earnings manipulation, on 1980s-90s
manipulators; this is a different population and a broader event, and the result should be read as a
limit of the tool for this job, not a general verdict on it.

## Pipeline

```
SEC quarterly zips ─► landing (Parquet, streamed out of each zip) ─► silver.facts ─► gold.versions
                                                                                  ├► gold.revisions
                                                                                  ├► gold.annual ─► model
                                                                                  └► gold.filings
EDGAR full-text search ─► landing/events (8-K 4.02 / 4.01, NT 10-K) ─► silver.events ─► model
```

- `filings/ingest.py` streams each quarterly zip (~120 MB, `num.txt` ~530 MB) without extracting it. It
  keeps whole-company USD values for the mapped tags, in 10-K and 10-Q filings and their amendments.
- `filings/edgar.py` finds 8-K Item 4.02 and 4.01 filings and NT 10-K notices through EDGAR full-text
  search (month by month, splitting ranges under the 10,000-document cap), keeps a filing only if its
  header declares the item, reads each non-SPAC 4.02's text for the periods it names, checks coverage
  against the per-company submissions index, and writes `data/item_4_02.csv`. Stays near 3 requests a
  second with a contact User-Agent.
- `filings/concepts.py` maps tags onto 21 standard line items, each with a priority-ordered tag list
  (revenue alone has nine tags across the ASC 606 change).
- `filings/pipeline.py` (PySpark + Delta, runs unchanged as a Databricks job):
  - **silver.facts** keeps one value per filing, line item and period; **silver.events** one row per
    event filing and company.
  - **gold.versions** keeps every value each filing ever reported for a company's line item and
    period, in filing order, as point-in-time history.
  - **gold.revisions** records each time a later filing changed an earlier figure by more than 1%
    and $100K under the same tag.
  - **gold.annual** has one row per company-year, as that year's own 10-K reported it; **gold.filings**
    one row per filing.
- `filings/model.py` holds the labels, features, model and baselines; `filings/findings.py` the tables
  above.

Two findings from building the labels:

- **Exact sign flips are excluded.** A loss tagged as a profit and fixed a year later is an XBRL
  tagging correction, not a revision of the accounts.
- **Revenue is excluded from the revision label.** Moving a business into discontinued operations
  restates prior revenue routinely without anything having been wrong.

## Limitations

- The 4.02 label is firm-level: about half of the positives' 4.02s name the 10-K's own year. The
  stricter "names this year" target is reported and the model is weaker on it (0.710 AUC, 5.6% top-5%
  precision at a 2.3% base rate).
- Filing lag carries most of the model's signal: lag alone scores 0.698 AUC against the model's 0.713.
  On the revision target, lag alone (0.621) even beats the model's AUC (0.613), though the model is
  still better at the top of the ranking (22.2% top-5% precision against 14.3%).
- Test years 2021-2024 include the SPAC-accounting wave; the ex-warrants target removes 4.02s whose text
  mentions warrants or redeemable shares, found by keyword, which will miss some and catch some that
  are only partly about them.
- EDGAR full-text search starts in 2001 and the submissions index was sampled, not read in full; the
  400-company check found no gaps but it is a sample.
- The per-line-item revision rates include standard adoptions by design; the model's revision label
  removes them by date window, which is approximate (early and late adopters exist).
- Nothing here is a fraud detector. An Item 4.02 is a company's own statement that its filed numbers
  can't be relied on, which includes honest errors and interpretation changes.

## Run

```bash
python3.12 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
export SEC_USER_AGENT="Your Name your@email"   # the SEC requires a contact (fair-access policy)
python -m filings.ingest 2012q1 2026q3
python -m filings.edgar 2009-01-01 2026-11-08  # 8-K 4.02 / 4.01 and NT 10-K events (~85 min, rate-limited)
python -m filings.edgar text                   # read each Item 4.02 (~40 min)
python -m filings.edgar coverage               # check the list against EDGAR's company index
python -m filings.edgar csv                    # data/item_4_02.csv
python -m filings.pipeline
python -m filings.model
python -m filings.findings
pytest -q
```

Data: SEC Financial Statement Data Sets and EDGAR filings, US government works in the public domain.
