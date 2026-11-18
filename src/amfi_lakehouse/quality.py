from datetime import date, timedelta
import pandera.pyspark as pa
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType


SILVER_NAV_SCHEMA = pa.DataFrameSchema(
    {
        "scheme_code": pa.Column(IntegerType(), checks=pa.Check.gt(0), nullable=False),
        "scheme_name": pa.Column(StringType(), nullable=False),
        "nav": pa.Column(
            DoubleType(),
            checks=[pa.Check.gt(0.0), pa.Check.lt(1_000_000.0)],
            nullable=False,
        ),
        "nav_date": pa.Column(DateType(), nullable=False),
        "ingestion_date": pa.Column(DateType(), nullable=False),
    }
)


def split_valid_and_quarantine(
    raw_df: DataFrame,
    run_date: str,
    min_expected_rows: int = 10_000,
    previous_valid_count: int | None = None,
    max_drift_ratio: float = 0.25,
) -> tuple[DataFrame, DataFrame, dict]:
    """
    Evaluate data quality rules on parsed AMFI rows:
      1. Tag each row with an explicit quarantine_reason if any rule fails.
      2. Split into (valid_df, quarantine_df).
      3. Validate valid_df against Pandera SILVER_NAV_SCHEMA.
      4. Enforce row-count & drift guardrails.
    """
    ref_date = date.fromisoformat(run_date)
    min_valid_date = ref_date - timedelta(days=365 * 25)
    max_valid_date = ref_date + timedelta(days=1)

    parsed_date_col = F.to_date(F.col("nav_date"), "yyyy-MM-dd")

    # Classify failure reasons deterministically
    tagged_df = raw_df.withColumn(
        "quarantine_reason",
        F.when(F.col("scheme_code").isNull() | (F.col("scheme_code") <= 0), F.lit("INVALID_SCHEME_CODE"))
        .when(F.col("scheme_name").isNull() | (F.length(F.trim(F.col("scheme_name"))) < 2), F.lit("MISSING_SCHEME_NAME"))
        .when(F.col("nav").isNull(), F.lit("MISSING_OR_NA_NAV"))
        .when((F.col("nav") <= 0) | (F.col("nav") >= 1_000_000.0), F.lit("OUT_OF_RANGE_NAV"))
        .when(parsed_date_col.isNull(), F.lit("INVALID_OR_MISSING_NAV_DATE"))
        .when((parsed_date_col < F.lit(min_valid_date)) | (parsed_date_col > F.lit(max_valid_date)), F.lit("DATE_OUT_OF_BOUNDS"))
        .otherwise(F.lit(None)),
    )

    # Flag exact duplicate (scheme_code, nav_date) rows within the same batch
    dup_window = Window.partitionBy("scheme_code", "nav_date").orderBy(F.col("scheme_name").asc())
    tagged_df = (
        tagged_df.withColumn(
            "_batch_rn",
            F.when(F.col("quarantine_reason").isNull(), F.row_number().over(dup_window)).otherwise(F.lit(1)),
        )
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("quarantine_reason").isNull() & (F.col("_batch_rn") > 1),
                F.lit("DUPLICATE_SCHEME_DATE_IN_BATCH"),
            ).otherwise(F.col("quarantine_reason")),
        )
        .drop("_batch_rn")
    )

    quarantine_df = (
        tagged_df.filter(F.col("quarantine_reason").isNotNull())
        .withColumn("ingestion_date", F.to_date(F.col("ingestion_date"), "yyyy-MM-dd").cast(DateType()))
        .withColumn("quarantined_at", F.current_timestamp())
    )

    valid_df = (
        tagged_df.filter(F.col("quarantine_reason").isNull())
        .drop("quarantine_reason")
        .withColumn("nav_date", parsed_date_col.cast(DateType()))
        .withColumn("ingestion_date", F.to_date(F.col("ingestion_date"), "yyyy-MM-dd").cast(DateType()))
    )

    # Pandera schema validation on valid slice
    validated_df = SILVER_NAV_SCHEMA.validate(valid_df)
    if hasattr(validated_df, "pandera") and validated_df.pandera.errors:
        raise ValueError(f"Pandera validation failed: {validated_df.pandera.errors}")

    valid_count = validated_df.count()
    quarantine_count = quarantine_df.count()

    # Row-count & drift guardrails
    if valid_count < min_expected_rows:
        raise ValueError(
            f"Row-count guardrail triggered: valid_count={valid_count} is below "
            f"min_expected_rows={min_expected_rows}."
        )

    if previous_valid_count and previous_valid_count > 0:
        drift = abs(valid_count - previous_valid_count) / previous_valid_count
        if drift > max_drift_ratio:
            raise ValueError(
                f"Row-count drift guardrail triggered: valid_count={valid_count} vs "
                f"previous={previous_valid_count} (drift={drift:.1%} > {max_drift_ratio:.0%})."
            )

    # Breakdown of quarantine reasons for observability
    reason_rows = quarantine_df.groupBy("quarantine_reason").count().collect()
    reason_breakdown = {row["quarantine_reason"]: row["count"] for row in reason_rows}

    quality_report = {
        "valid_rows": valid_count,
        "quarantined_rows": quarantine_count,
        "quarantine_reasons": reason_breakdown,
    }
    return validated_df, quarantine_df, quality_report