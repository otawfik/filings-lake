"""Bronze → silver → gold, in PySpark + Delta. Runs locally (`python -m filings.pipeline`) and, unchanged,
as a Databricks job, where `spark` already exists and the tables land in Unity Catalog.

bronze.num / bronze.sub   the landed SEC rows, one Delta table each, with the source quarter
silver.facts              one value per (filing, standard line item, period): the highest-priority tag
                          the filing used (concepts.py), whole-company, USD
gold.versions             every value every filing ever reported for a company's line item and period,
                          in filing order: the point-in-time history
gold.revisions            a later filing changed a figure an earlier one reported under the same tag
                          (by > 1% and > $100K)
gold.annual               one row per company-year, as reported in that year's own 10-K (so every
                          feature is known on the filing date), with the prior year beside it
gold.filings              one row per 10-K / 10-Q filing and amendment: who filed what, when
silver.events             8-K Item 4.02 (non-reliance) and 4.01 (auditor change) filings and NT 10-K
                          notices, from EDGAR full-text search (edgar.py), with what each 4.02 says
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .concepts import CONCEPTS, PRIORITY

ROOT = Path(__file__).resolve().parent.parent
LAKE = Path(os.environ.get("FILINGS_LAKE", ROOT / "lake"))
KEY_CONCEPTS = ["net_income", "equity", "total_assets", "operating_cash_flow", "revenue"]
REVISION_REL, REVISION_ABS = 0.01, 100_000


def spark_session() -> SparkSession:
    if active := SparkSession.getActiveSession():  # on Databricks the session already exists
        return active
    import sys

    from delta import configure_spark_with_delta_pip

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers must match the driver's Python

    b = (SparkSession.builder.master("local[*]").appName("filings-lake")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "32")
         .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.local.dir", str(ROOT / "spark-tmp")))
    s = configure_spark_with_delta_pip(b).getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def path(table: str) -> str:
    return str(LAKE / table.replace(".", "/"))


def write(df: DataFrame, table: str) -> DataFrame:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path(table))
    return df.sparkSession.read.format("delta").load(path(table))


def read(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.format("delta").load(path(table))


def bronze(spark: SparkSession, landing: Path | None = None) -> tuple[DataFrame, DataFrame]:
    landing = landing or LAKE / "landing"
    q = F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{4}q\d)/", 1)
    num = spark.read.parquet(str(landing / "*" / "num.parquet")).withColumn("quarter", q)
    sub = spark.read.parquet(str(landing / "*" / "sub.parquet")).withColumn("quarter", q)
    if os.environ.get("FILINGS_LOCAL_BRONZE", "parquet") == "parquet":
        return num, sub  # on a laptop the landed Parquet is the bronze layer; no second copy
    return write(num, "bronze.num"), write(sub, "bronze.sub")


def silver(spark: SparkSession, num: DataFrame, sub: DataFrame) -> DataFrame:
    """One value per filing × standard line item × period, from the highest-priority tag reported."""
    pri = spark.createDataFrame([(t, c, p, CONCEPTS[c][0]) for t, (c, p) in PRIORITY.items()],
                                "tag string, concept string, priority int, kind string")
    sub = (sub.dropDuplicates(["adsh"])
           .withColumn("filed", F.to_date("filed", "yyyyMMdd"))
           .withColumn("period", F.to_date("period", "yyyyMMdd"))
           .select("adsh", "cik", "name", "sic", "form", "period", "fy", "fp", "filed", "fye", "afs"))
    f = (num.join(F.broadcast(pri), "tag")
         .where(((F.col("kind") == "instant") & (F.col("qtrs") == 0)) | ((F.col("kind") == "duration") & F.col("qtrs").isin(1, 4)))
         .where((F.col("concept") == "eps_diluted") == (F.col("uom") == "USD/shares"))
         .withColumn("ddate", F.to_date("ddate", "yyyyMMdd")))
    w = Window.partitionBy("adsh", "concept", "ddate", "qtrs").orderBy("priority", "tag", F.abs("value").desc())
    f = (f.withColumn("rn", F.row_number().over(w)).where("rn = 1")
         .select("adsh", "concept", "tag", "ddate", "qtrs", "value"))
    return write(f.join(sub, "adsh"), "silver.facts")


def gold(spark: SparkSession, facts: DataFrame) -> dict[str, DataFrame]:
    key = ["cik", "concept", "ddate", "qtrs"]
    order = Window.partitionBy(*key).orderBy("filed", "adsh")
    versions = (facts
                .withColumn("version", F.row_number().over(order))
                .withColumn("first_value", F.first("value").over(order))
                .withColumn("first_filed", F.first("filed").over(order))
                .withColumn("first_adsh", F.first("adsh").over(order))
                .withColumn("prev_value", F.lag("value").over(order))
                .withColumn("prev_tag", F.lag("tag").over(order))
                .select(*key, "version", "adsh", "form", "filed", "tag", "prev_tag", "value", "prev_value",
                        "first_value", "first_filed", "first_adsh"))
    versions = write(versions, "gold.versions")

    change = F.abs(F.col("value") - F.col("prev_value"))
    # same tag both times: a filing that only reported equity including minority interests, after one
    # that reported it without, is a different measure, not a revision
    revisions = (versions.where("version > 1").where(F.col("tag") == F.col("prev_tag"))
                 .where((change > REVISION_ABS) & (change > REVISION_REL * F.greatest(F.abs("prev_value"), F.lit(1.0))))
                 .withColumn("days_after_first", F.datediff("filed", "first_filed")))
    revisions = write(revisions, "gold.revisions")

    # company-year: the 10-K's own figures for its fiscal year, and the prior year as that 10-K shows it
    tenk = facts.where(F.col("form").isin("10-K", "10-KT")).where(F.col("fp") == "FY")
    cur = tenk.where(((F.col("qtrs") == 4) | (F.col("qtrs") == 0)) & (F.col("ddate") == F.col("period")))
    prior = (tenk.where(((F.col("qtrs") == 4) | (F.col("qtrs") == 0))
                        & F.col("ddate").between(F.add_months("period", -13), F.add_months("period", -11)))
             .withColumn("concept", F.concat("concept", F.lit("_prev"))))
    wide = (cur.unionByName(prior)
            .groupBy("adsh", "cik", "name", "sic", "fy", "period", "filed", "afs")
            .pivot("concept").agg(F.first("value")))
    # one 10-K per company-period: the original, not a later re-file
    first = Window.partitionBy("cik", "period").orderBy("filed", "adsh")
    annual = wide.withColumn("rn", F.row_number().over(first)).where("rn = 1").drop("rn")
    return {"versions": versions, "revisions": revisions, "annual": write(annual, "gold.annual")}


def filings(sub: DataFrame) -> DataFrame:
    f = (sub.dropDuplicates(["adsh"])
         .select("adsh", "cik", "name", "sic", "countryba", "form", "afs",
                 F.to_date("period", "yyyyMMdd").alias("period"), F.to_date("filed", "yyyyMMdd").alias("filed")))
    return write(f, "gold.filings")


EVENT_KINDS = ("item_4_02", "item_4_01", "nt_10k")


def events(spark: SparkSession, landing: Path | None = None) -> DataFrame | None:
    """silver.events: one row per (event filing, CIK), with the kind of event; 4.02s carry what their text says."""
    landing = landing or LAKE / "landing" / "events"
    parts = [spark.read.parquet(str(landing / f"{k}.parquet")).withColumn("kind", F.lit(k))
             .select("kind", "adsh", F.col("cik").cast("long"), F.to_date("filed").alias("filed"), "form", "items", "sic")
             for k in EVENT_KINDS if (landing / f"{k}.parquet").exists()]
    if not parts:
        return None
    ev = parts[0]
    for p in parts[1:]:
        ev = ev.unionByName(p)
    text = landing / "item_4_02_text.parquet"
    if text.exists():
        t = spark.read.parquet(str(text)).select("adsh", "dates_named", "fiscal_years_named", "mentions_warrants",
                                                 "mentions_redeemable_shares")
        ev = ev.join(t, "adsh", "left")
    return write(ev.dropDuplicates(["kind", "adsh", "cik"]), "silver.events")


def main() -> None:
    import time

    t0 = time.time()
    spark = spark_session()
    num, sub = bronze(spark)
    facts = silver(spark, num, sub)
    g = gold(spark, facts)
    g["filings"] = filings(sub)
    ev = events(spark)
    tables = [("bronze (landed facts)", num), ("silver.facts", facts), *[(f"gold.{k}", v) for k, v in g.items()]]
    for name, df in tables + ([("silver.events", ev)] if ev is not None else []):
        print(f"{name:14s} {df.count():>12,} rows")
    print(f"pipeline wall time {time.time() - t0:,.0f}s")


if __name__ == "__main__":
    main()
