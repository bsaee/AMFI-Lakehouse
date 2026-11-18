import json
import logging
import time

from amfi_lakehouse.gold import build_gold_tables
from amfi_lakehouse.ingest import ingest_daily_nav
from amfi_lakehouse.silver import run_silver_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("amfi.pipeline")


def run_daily_pipeline(ingestion_date: str | None = None) -> dict:
    """Execute the full end-to-end daily medallion pipeline: Bronze -> Silver -> Gold."""
    t0 = time.perf_counter()

    logger.info("=== Step 1/3: Bronze Ingestion ===")
    bronze_metrics = ingest_daily_nav(ingestion_date=ingestion_date)

    logger.info("=== Step 2/3: Silver Parsing, Quality & Delta MERGE ===")
    silver_metrics = run_silver_pipeline(ingestion_date=ingestion_date)

    logger.info("=== Step 3/3: Gold Window Analytics ===")
    gold_metrics = build_gold_tables()

    summary = {
        "bronze": bronze_metrics,
        "silver": silver_metrics,
        "gold": gold_metrics,
        "total_pipeline_runtime_sec": round(time.perf_counter() - t0, 3),
    }
    logger.info("End-to-end daily pipeline succeeded in %.2fs", summary["total_pipeline_runtime_sec"])
    return summary


if __name__ == "__main__":
    result = run_daily_pipeline()
    print(json.dumps(result, indent=2, default=str))