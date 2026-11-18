import json
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from amfi_lakehouse.config import BRONZE_DIR, DATA_DIR, SILVER_DIR
from amfi_lakehouse.parse import parse_bronze_file
from amfi_lakehouse.quality import split_valid_and_quarantine
from amfi_lakehouse.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("amfi.silver")

IST = ZoneInfo("Asia/Kolkata")

RAW_PARSED_SCHEMA = StructType(
    [
        StructField("scheme_code", IntegerType(), nullable=False),
        StructField("isin_growth", StringType(), nullable=True),
        StructField("isin_reinvest", StringType(), nullable=True),
        StructField("scheme_name", StringType(), nullable=False),
        StructField("plan", StringType(), nullable=True),
        StructField("option", StringType(), nullable=True),
        StructField("nav", DoubleType(), nullable=True),
        StructField("nav_date", StringType(), nullable=True),
        StructField("amc_name", StringType(), nullable=True),
        StructField("scheme_category", StringType(), nullable=True),
        StructField("ingestion_date", StringType(), nullable=False),
    ]
)

ARROW_STAGING_SCHEMA = pa.schema(
    [
        ("scheme_code", pa.int32()),
        ("isin_growth", pa.string()),
        ("isin_reinvest", pa.string()),
        ("scheme_name", pa.string()),
        ("plan", pa.string()),
        ("option", pa.string()),
        ("nav", pa.float64()),
        ("nav_date", pa.string()),
        ("amc_name", pa.string()),
        ("scheme_category", pa.string()),
        ("ingestion_date", pa.string()),
    ]
)


def _dedupe_latest(df: DataFrame, keys: list[str], order_col: str) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def merge_into_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: Path,
    merge_condition: str,
    partition_cols: list[str] | None = None,
) -> None:
    target_str = str(target_path)
    if DeltaTable.isDeltaTable(spark, target_str):
        delta_tbl = DeltaTable.forPath(spark, target_str)
        (
            delta_tbl.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        target_path.mkdir(parents=True, exist_ok=True)
        writer = source_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_str)


def _write_valid_to_silver(
    spark: SparkSession,
    valid_df: DataFrame,
    quarantine_df: DataFrame,
    quarantined_rows_count: int,
) -> tuple[int, int, int]:
    """Shared helper to MERGE validated rows into nav_daily, scheme_dim, and quarantine."""
    fact_df = valid_df.select(
        "scheme_code",
        "nav_date",
        "nav",
        F.date_format(F.col("nav_date"), "yyyy-MM").alias("year_month"),
        "ingestion_date",
    )
    fact_deduped = _dedupe_latest(fact_df, ["scheme_code", "nav_date"], "ingestion_date")

    nav_daily_path = SILVER_DIR / "nav_daily"
    merge_into_delta(
        spark=spark,
        source_df=fact_deduped,
        target_path=nav_daily_path,
        merge_condition="target.scheme_code = source.scheme_code AND target.nav_date = source.nav_date",
        partition_cols=["year_month"],
    )

    dim_df = valid_df.select(
        "scheme_code",
        "scheme_name",
        "plan",
        "option",
        "amc_name",
        "scheme_category",
        "isin_growth",
        "isin_reinvest",
        F.col("nav_date").alias("last_seen_date"),
    )
    dim_deduped = _dedupe_latest(dim_df, ["scheme_code"], "last_seen_date")
    scheme_dim_path = SILVER_DIR / "scheme_dim"
    merge_into_delta(
        spark=spark,
        source_df=dim_deduped,
        target_path=scheme_dim_path,
        merge_condition="target.scheme_code = source.scheme_code",
    )

    quarantine_path = SILVER_DIR / "quarantine"
    if quarantined_rows_count > 0:
        quarantine_deduped = _dedupe_latest(
            quarantine_df, ["scheme_code", "ingestion_date"], "quarantined_at"
        )
        merge_into_delta(
            spark=spark,
            source_df=quarantine_deduped,
            target_path=quarantine_path,
            merge_condition=(
                "target.scheme_code = source.scheme_code "
                "AND target.ingestion_date = source.ingestion_date"
            ),
            partition_cols=["ingestion_date"],
        )

    nav_count = spark.read.format("delta").load(str(nav_daily_path)).count()
    dim_count = spark.read.format("delta").load(str(scheme_dim_path)).count()
    quarantine_total = (
        spark.read.format("delta").load(str(quarantine_path)).count()
        if quarantine_path.exists()
        else 0
    )
    return nav_count, dim_count, quarantine_total


def run_silver_pipeline(
    ingestion_date: str | None = None,
    min_expected_rows: int = 10_000,
) -> dict:
    start_time = time.perf_counter()
    run_date = ingestion_date or datetime.now(IST).strftime("%Y-%m-%d")
    bronze_file = BRONZE_DIR / "nav_all" / f"ingestion_date={run_date}" / "NAVAll.txt"

    if not bronze_file.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_file}")

    logger.info("Parsing Bronze file: %s", bronze_file)
    parsed_rows = parse_bronze_file(bronze_file, ingestion_date=run_date)
    logger.info("Parsed %d fund rows from %s", len(parsed_rows), bronze_file.name)

    spark = get_spark_session("amfi-silver-pipeline")
    try:
        raw_df = spark.createDataFrame(parsed_rows, schema=RAW_PARSED_SCHEMA)
        valid_df, quarantine_df, quality_report = split_valid_and_quarantine(
            raw_df=raw_df,
            run_date=run_date,
            min_expected_rows=min_expected_rows,
        )

        nav_count, dim_count, quarantine_total = _write_valid_to_silver(
            spark=spark,
            valid_df=valid_df,
            quarantine_df=quarantine_df,
            quarantined_rows_count=quality_report["quarantined_rows"],
        )
        elapsed = round(time.perf_counter() - start_time, 3)

        metrics = {
            "ingestion_date": run_date,
            "parsed_rows": len(parsed_rows),
            **quality_report,
            "silver_nav_daily_total_rows": nav_count,
            "silver_scheme_dim_total_rows": dim_count,
            "silver_quarantine_total_rows": quarantine_total,
            "runtime_sec": elapsed,
        }
        logger.info("Silver + Quality pipeline complete: %s", json.dumps(metrics))
        return metrics
    finally:
        spark.stop()


def run_backfill_silver(ingestion_date: str | None = None) -> dict:
    """
    Parse historical window files in Bronze one by one into compressed Parquet staging
    (keeping Python memory low), then bulk-MERGE into Silver Delta tables.
    """
    start_time = time.perf_counter()
    run_date = ingestion_date or datetime.now(IST).strftime("%Y-%m-%d")
    history_root = BRONZE_DIR / "nav_history"

    history_files = sorted(history_root.glob("window=*/NAVHistory.txt"))
    if not history_files:
        raise FileNotFoundError(f"No historical Bronze files found under {history_root}")

    staging_dir = DATA_DIR / "_staging_backfill"
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    total_parsed = 0
    for idx, fpath in enumerate(history_files):
        rows = parse_bronze_file(fpath, ingestion_date=run_date)
        total_parsed += len(rows)
        table = pa.Table.from_pylist(rows, schema=ARROW_STAGING_SCHEMA)
        pq.write_table(table, staging_dir / f"chunk_{idx:03d}.parquet", compression="snappy")
        logger.info(
            "[%d/%d] Staged %d rows from %s",
            idx + 1,
            len(history_files),
            len(rows),
            fpath.parent.name,
        )

    spark = get_spark_session("amfi-silver-backfill")
    try:
        raw_df = spark.read.parquet(str(staging_dir))

        valid_df, quarantine_df, quality_report = split_valid_and_quarantine(
            raw_df=raw_df,
            run_date=run_date,
            min_expected_rows=100_000,
        )

        nav_count, dim_count, quarantine_total = _write_valid_to_silver(
            spark=spark,
            valid_df=valid_df,
            quarantine_df=quarantine_df,
            quarantined_rows_count=quality_report["quarantined_rows"],
        )

        nav_df = spark.read.format("delta").load(str(SILVER_DIR / "nav_daily"))
        date_bounds = nav_df.agg(F.min("nav_date"), F.max("nav_date")).collect()[0]
        elapsed = round(time.perf_counter() - start_time, 3)

        metrics = {
            "files_processed": len(history_files),
            "parsed_rows": total_parsed,
            **quality_report,
            "silver_nav_daily_total_rows": nav_count,
            "silver_scheme_dim_total_rows": dim_count,
            "silver_quarantine_total_rows": quarantine_total,
            "min_nav_date": str(date_bounds[0]),
            "max_nav_date": str(date_bounds[1]),
            "runtime_sec": elapsed,
        }
        logger.info("Silver historical backfill complete: %s", json.dumps(metrics))
        return metrics
    finally:
        spark.stop()
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


if __name__ == "__main__":
    result = run_silver_pipeline()
    print(json.dumps(result, indent=2))