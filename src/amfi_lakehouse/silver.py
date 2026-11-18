import json
import logging
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from amfi_lakehouse.config import BRONZE_DIR, SILVER_DIR
from amfi_lakehouse.parse import parse_bronze_file
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
        StructField("nav", DoubleType(), nullable=True),
        StructField("nav_date", StringType(), nullable=True),
        StructField("amc_name", StringType(), nullable=True),
        StructField("scheme_category", StringType(), nullable=True),
        StructField("ingestion_date", StringType(), nullable=False),
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


def run_silver_pipeline(ingestion_date: str | None = None) -> dict:
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

        # Basic Silver filter (Phase 3 will route invalid rows to quarantine)
        valid_df = (
            raw_df.filter(
                F.col("nav").isNotNull()
                & (F.col("nav") > 0)
                & F.col("nav_date").isNotNull()
            )
            .withColumn("nav_date", F.to_date("nav_date", "yyyy-MM-dd").cast(DateType()))
            .withColumn("ingestion_date", F.to_date("ingestion_date", "yyyy-MM-dd").cast(DateType()))
        )

        # 1. Prepare nav_daily fact table
        fact_df = valid_df.select(
            "scheme_code",
            "nav_date",
            "nav",
            "ingestion_date",
        )
        fact_deduped = _dedupe_latest(fact_df, ["scheme_code", "nav_date"], "ingestion_date")

        nav_daily_path = SILVER_DIR / "nav_daily"
        merge_into_delta(
            spark=spark,
            source_df=fact_deduped,
            target_path=nav_daily_path,
            merge_condition="target.scheme_code = source.scheme_code AND target.nav_date = source.nav_date",
            partition_cols=["nav_date"],
        )

        # 2. Prepare scheme_dim dimension table
        dim_df = valid_df.select(
            "scheme_code",
            "scheme_name",
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

        # Verify via Spark SQL
        nav_count = spark.read.format("delta").load(str(nav_daily_path)).count()
        dim_count = spark.read.format("delta").load(str(scheme_dim_path)).count()
        invalid_count = raw_df.count() - valid_df.count()
        elapsed = round(time.perf_counter() - start_time, 3)

        metrics = {
            "ingestion_date": run_date,
            "parsed_rows": len(parsed_rows),
            "valid_rows": valid_df.count(),
            "filtered_out_rows": invalid_count,
            "silver_nav_daily_total_rows": nav_count,
            "silver_scheme_dim_total_rows": dim_count,
            "runtime_sec": elapsed,
        }
        logger.info("Silver MERGE complete: %s", json.dumps(metrics))
        return metrics
    finally:
        spark.stop()


if __name__ == "__main__":
    result = run_silver_pipeline()
    print(json.dumps(result, indent=2))