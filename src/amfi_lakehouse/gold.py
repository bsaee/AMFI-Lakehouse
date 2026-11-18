import json
import logging
import time
from pyspark.sql import functions as F

from amfi_lakehouse.config import GOLD_DIR, SILVER_DIR
from amfi_lakehouse.spark_session import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("amfi.gold")


def build_gold_tables() -> dict:
    """
    Build Gold analytics Delta tables from Silver (nav_daily + scheme_dim)
    using Spark SQL window functions:
      1. gold/fund_daily_metrics: daily return, 30D annualized volatility, running drawdown
      2. gold/category_leaderboard: latest snapshot with 1M/1Y/3Y returns & category ranks
    """
    start_time = time.perf_counter()
    nav_daily_path = SILVER_DIR / "nav_daily"
    scheme_dim_path = SILVER_DIR / "scheme_dim"

    if not nav_daily_path.exists() or not scheme_dim_path.exists():
        raise FileNotFoundError("Silver tables not found. Run Silver pipeline first.")

    spark = get_spark_session("amfi-gold-analytics")
    try:
        spark.read.format("delta").load(str(nav_daily_path)).createOrReplaceTempView("nav_daily")
        spark.read.format("delta").load(str(scheme_dim_path)).createOrReplaceTempView("scheme_dim")

        # 1. Compute daily returns, trailing returns, rolling 30D volatility, and drawdown
        daily_metrics_sql = """
        WITH base_nav AS (
            SELECT
                scheme_code,
                nav_date,
                year_month,
                nav,
                LAG(nav, 1) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS prev_nav,
                LAG(nav, 21) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS nav_1m_ago,
                LAG(nav, 252) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS nav_1y_ago,
                LAG(nav, 756) OVER (PARTITION BY scheme_code ORDER BY nav_date) AS nav_3y_ago,
                MAX(nav) OVER (
                    PARTITION BY scheme_code
                    ORDER BY nav_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_peak_nav
            FROM nav_daily
        ),
        returns_calc AS (
            SELECT
                scheme_code,
                nav_date,
                year_month,
                nav,
                CASE
                    WHEN prev_nav IS NOT NULL AND prev_nav > 0
                    THEN (nav - prev_nav) / prev_nav
                    ELSE NULL
                END AS daily_return,
                CASE
                    WHEN nav_1m_ago IS NOT NULL AND nav_1m_ago > 0
                    THEN (nav - nav_1m_ago) / nav_1m_ago
                    ELSE NULL
                END AS return_1m,
                CASE
                    WHEN nav_1y_ago IS NOT NULL AND nav_1y_ago > 0
                    THEN (nav - nav_1y_ago) / nav_1y_ago
                    ELSE NULL
                END AS return_1y,
                CASE
                    WHEN nav_3y_ago IS NOT NULL AND nav_3y_ago > 0
                    THEN POW(nav / nav_3y_ago, 1.0 / 3.0) - 1.0
                    ELSE NULL
                END AS cagr_3y,
                CASE
                    WHEN running_peak_nav > 0
                    THEN (nav - running_peak_nav) / running_peak_nav
                    ELSE 0.0
                END AS drawdown
            FROM base_nav
        )
        SELECT
            scheme_code,
            nav_date,
            year_month,
            ROUND(nav, 4) AS nav,
            ROUND(daily_return, 6) AS daily_return,
            ROUND(return_1m, 4) AS return_1m,
            ROUND(return_1y, 4) AS return_1y,
            ROUND(cagr_3y, 4) AS cagr_3y,
            ROUND(drawdown, 4) AS drawdown,
            ROUND(
                STDDEV_SAMP(daily_return) OVER (
                    PARTITION BY scheme_code
                    ORDER BY nav_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) * SQRT(252),
                4
            ) AS volatility_30d_ann
        FROM returns_calc
        """

        daily_metrics_df = spark.sql(daily_metrics_sql)
        daily_metrics_path = GOLD_DIR / "fund_daily_metrics"
        daily_metrics_path.mkdir(parents=True, exist_ok=True)

        logger.info("Writing Gold fund_daily_metrics Delta table...")
        (
            daily_metrics_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year_month")
            .save(str(daily_metrics_path))
        )

        # Register newly written Gold table for leaderboard query
        spark.read.format("delta").load(str(daily_metrics_path)).createOrReplaceTempView(
            "fund_daily_metrics"
        )

        # 2. Build latest scheme snapshot + category rankings
        leaderboard_sql = """
        WITH latest_per_scheme AS (
            SELECT
                m.*,
                ROW_NUMBER() OVER (PARTITION BY m.scheme_code ORDER BY m.nav_date DESC) AS rn
            FROM fund_daily_metrics m
        ),
        max_dd_per_scheme AS (
            SELECT
                scheme_code,
                MIN(drawdown) AS max_drawdown_history
            FROM fund_daily_metrics
            GROUP BY scheme_code
        ),
        enriched AS (
            SELECT
                l.scheme_code,
                d.scheme_name,
                d.amc_name,
                COALESCE(d.scheme_category, 'Uncategorized') AS scheme_category,
                d.plan,
                d.option,
                l.nav_date AS latest_nav_date,
                l.nav AS latest_nav,
                l.return_1m,
                l.return_1y,
                l.cagr_3y,
                l.volatility_30d_ann,
                l.drawdown AS current_drawdown,
                dd.max_drawdown_history
            FROM latest_per_scheme l
            JOIN scheme_dim d ON l.scheme_code = d.scheme_code
            LEFT JOIN max_dd_per_scheme dd ON l.scheme_code = dd.scheme_code
            WHERE l.rn = 1
        )
        SELECT
            *,
            CASE
                WHEN return_1y IS NOT NULL
                THEN DENSE_RANK() OVER (
                    PARTITION BY scheme_category
                    ORDER BY return_1y DESC NULLS LAST
                )
                ELSE NULL
            END AS category_rank_1y,
            CASE
                WHEN cagr_3y IS NOT NULL
                THEN DENSE_RANK() OVER (
                    PARTITION BY scheme_category
                    ORDER BY cagr_3y DESC NULLS LAST
                )
                ELSE NULL
            END AS category_rank_3y
        FROM enriched
        """

        leaderboard_df = spark.sql(leaderboard_sql)
        leaderboard_path = GOLD_DIR / "category_leaderboard"
        leaderboard_path.mkdir(parents=True, exist_ok=True)

        logger.info("Writing Gold category_leaderboard Delta table...")
        (
            leaderboard_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(leaderboard_path))
        )

        # Query sample top funds by 3Y CAGR to verify Gold output
        top_3y_sample = (
            spark.read.format("delta")
            .load(str(leaderboard_path))
            .filter(
                (F.col("category_rank_3y") == 1)
                & (F.col("plan") == "Direct Plan")
                & (F.col("scheme_category").contains("Equity Scheme"))
            )
            .select(
                "scheme_category",
                "scheme_name",
                "cagr_3y",
                "return_1y",
                "volatility_30d_ann",
                "max_drawdown_history",
            )
            .orderBy(F.col("cagr_3y").desc())
            .limit(5)
            .collect()
        )

        metrics_count = spark.read.format("delta").load(str(daily_metrics_path)).count()
        board_count = spark.read.format("delta").load(str(leaderboard_path)).count()
        elapsed = round(time.perf_counter() - start_time, 3)

        summary = {
            "gold_fund_daily_metrics_rows": metrics_count,
            "gold_category_leaderboard_rows": board_count,
            "runtime_sec": elapsed,
            "sample_top_equity_funds_3y": [row.asDict() for row in top_3y_sample],
        }
        logger.info("Gold analytics build complete: %s", json.dumps(summary, default=str))
        return summary
    finally:
        spark.stop()


if __name__ == "__main__":
    result = build_gold_tables()
    print(json.dumps(result, indent=2, default=str))