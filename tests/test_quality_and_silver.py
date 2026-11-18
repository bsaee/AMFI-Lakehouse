import pytest
from amfi_lakehouse.quality import split_valid_and_quarantine
from amfi_lakehouse.silver import RAW_PARSED_SCHEMA
from amfi_lakehouse.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("amfi-pytest")
    yield session
    session.stop()


def test_quality_quarantines_bad_rows_and_passes_valid(spark):
    sample_rows = [
        # Valid row
        {
            "scheme_code": 1001,
            "isin_growth": "INF0001",
            "isin_reinvest": None,
            "scheme_name": "Alpha Equity Fund",
            "plan": "Direct Plan",
            "option": "Growth Option",
            "nav": 54.25,
            "nav_date": "2026-11-17",
            "amc_name": "Alpha AMC",
            "scheme_category": "Open Ended Schemes(Equity)",
            "ingestion_date": "2026-11-18",
        },
        # Duplicate (scheme_code, nav_date) in same batch -> should quarantine
        {
            "scheme_code": 1001,
            "isin_growth": "INF0001",
            "isin_reinvest": None,
            "scheme_name": "Alpha Equity Fund Dup",
            "plan": "Direct Plan",
            "option": "Growth Option",
            "nav": 54.25,
            "nav_date": "2026-11-17",
            "amc_name": "Alpha AMC",
            "scheme_category": "Open Ended Schemes(Equity)",
            "ingestion_date": "2026-11-18",
        },
        # Zero NAV -> should quarantine
        {
            "scheme_code": 1002,
            "isin_growth": "INF0002",
            "isin_reinvest": None,
            "scheme_name": "Zero Nav NFO Fund",
            "plan": "Direct Plan",
            "option": "Growth Option",
            "nav": 0.0,
            "nav_date": "2026-11-17",
            "amc_name": "Alpha AMC",
            "scheme_category": "Open Ended Schemes(Equity)",
            "ingestion_date": "2026-11-18",
        },
        # Missing / N.A. NAV -> should quarantine
        {
            "scheme_code": 1003,
            "isin_growth": "INF0003",
            "isin_reinvest": None,
            "scheme_name": "Missing Nav Fund",
            "plan": "Regular Plan",
            "option": "Growth Option",
            "nav": None,
            "nav_date": "2026-11-17",
            "amc_name": "Alpha AMC",
            "scheme_category": "Open Ended Schemes(Equity)",
            "ingestion_date": "2026-11-18",
        },
    ]

    raw_df = spark.createDataFrame(sample_rows, schema=RAW_PARSED_SCHEMA)
    valid_df, quarantine_df, report = split_valid_and_quarantine(
        raw_df=raw_df,
        run_date="2026-11-18",
        min_expected_rows=1,
    )

    assert report["valid_rows"] == 1
    assert report["quarantined_rows"] == 3
    assert valid_df.collect()[0]["scheme_code"] == 1001
    assert quarantine_df.count() == 3


def test_quality_row_count_guardrail_raises_on_truncated_batch(spark):
    sample_rows = [
        {
            "scheme_code": 1001,
            "isin_growth": "INF0001",
            "isin_reinvest": None,
            "scheme_name": "Alpha Equity Fund",
            "plan": "Direct Plan",
            "option": "Growth Option",
            "nav": 54.25,
            "nav_date": "2026-11-17",
            "amc_name": "Alpha AMC",
            "scheme_category": "Open Ended Schemes(Equity)",
            "ingestion_date": "2026-11-18",
        }
    ]
    raw_df = spark.createDataFrame(sample_rows, schema=RAW_PARSED_SCHEMA)

    with pytest.raises(ValueError, match="Row-count guardrail triggered"):
        split_valid_and_quarantine(
            raw_df=raw_df,
            run_date="2026-11-18",
            min_expected_rows=10_000,
        )