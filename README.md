# filings-lake

Every number in every SEC 10-K and 10-Q since 2012, turned into point-in-time financials on a
Databricks-style lakehouse (PySpark + Delta), with a model that predicts which annual reports will
have their numbers revised, benchmarked against the Beneish M-score.

**Status: in progress.** Built so far: ingestion, the line-item mapping, the bronze/silver/gold
pipeline, and the model and its tests. Still to come: the full-history model results, Databricks
jobs, the Snowflake serving layer (Iceberg, dbt, Streams/Tasks) and a text-to-SQL agent with an eval
gate. Every number below was measured from the code in this repo.

## Measured so far

- **17,600,102 facts** landed from **384,620** 10-K/10-Q filings (2012 Q1 to 2026 Q3, 59 quarterly SEC
  Financial Statement Data Sets), including **92,139** original 10-Ks.
- **Line-item coverage** (share of original 10-Ks in which the line item resolves for the fiscal year
  itself): total assets 97.9%, net income 97.5%, operating cash flow 96.6%, equity 95.1%, revenue 80.5%,
  receivables 54.3%. The gaps are real and expected: banks, insurers and REITs report revenue,
  inventory and cost of sales under different tags or not at all. Sector-specific mappings and
  mapping companies' custom extension tags are next.

- **Pipeline output (full history):** 12,436,144 silver facts, 218,837 revisions, 91,958 company-years.
- **Revision model, first honest result: weak.** Test set: 23,917 10-Ks filed 2021–2024, 9.7% of them
  revised within 24 months.

  | | AUC | avg precision | precision, top 5% |
  |---|---|---|---|
  | model | 0.577 | 0.128 | 16.9% |
  | Beneish M-score (published coefficients) | 0.493 | 0.100 | 10.2% |
  | the eight Beneish ratios, refitted | 0.482 | 0.100 | 11.9% |

  On the third of companies where every Beneish input exists, the model falls to 0.532, and the
  M-score beats it on top-5% precision. Beneish is built to catch earnings *manipulation*. This label,
  "a figure was later revised", is broader and noisier, so neither result is a fraud detector.

  **How a first run reached 0.671 and why that was wrong.** It counted a "revision" whenever a
  line item's value changed between filings, including when the second filing used a different tag
  for it (equity with minority interests, after equity without). Those tag switches were 98,518 of
  317,355 "revisions", and the model learned to predict them. Counting a change only under the same
  tag fixed it. Next: a stronger label from 8-K Item 4.02 "non-reliance" filings (the SEC's own
  restatement disclosure), and excluding years when a new accounting standard was adopted.

## Pipeline

```
SEC quarterly zips ─► landing (Parquet, streamed out of each zip) ─► silver.facts ─► gold.versions
                                                                                  ├► gold.revisions
                                                                                  └► gold.annual ─► model
```

- `filings/ingest.py` streams each quarterly zip (~120 MB, `num.txt` ~530 MB) without extracting it. It
  keeps whole-company USD values for the mapped tags, in 10-K and 10-Q filings and their amendments.
- `filings/concepts.py` maps tags onto 21 standard line items, each with a priority-ordered tag list (revenue alone has
  nine tags across the ASC 606 change).
- `filings/pipeline.py` (PySpark + Delta, runs unchanged as a Databricks job):
  - **silver** keeps one value per filing, line item and period.
  - **gold.versions** keeps every value each filing ever reported for a company's line item and
    period, in filing order, as point-in-time history.
  - **gold.revisions** records each time a later filing changed an earlier figure by more than 1%
    and $100K.
  - **gold.annual** has one row per company-year, as that year's own 10-K reported it.
- `filings/model.py` holds the model and its baselines:
  - **Label:** that year's net income, operating cash flow, total assets or equity is revised within
    24 months.
  - **Features:** known on the filing date only.
  - **Split:** train on filings up to 2019, test on 2021–2024.
  - **Baselines:** the published M-score, and the same eight ratios refitted.

Two findings from building the labels:

- **Exact sign flips are excluded.** A loss tagged as a profit and fixed a year later is an XBRL
  tagging correction, not a revision of the accounts.
- **Revenue is excluded from the label.** Moving a business into discontinued operations restates
  prior revenue routinely without anything having been wrong.

## Run

```bash
python3.12 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
export SEC_USER_AGENT="Your Name your@email"   # the SEC requires a contact (fair-access policy)
python -m filings.ingest 2012q1 2026q3
python -m filings.pipeline
python -m filings.model
pytest -q
```

Data: SEC Financial Statement Data Sets, US government works in the public domain.
