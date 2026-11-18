# AMFI Mutual Fund Lakehouse

An end-to-end data engineering pipeline that ingests daily Net Asset Value (NAV) data from the Association of Mutual Funds in India (AMFI), processes it through a **Bronze → Silver → Gold** medallion architecture on **Delta Lake** using **PySpark**, enforces schema/quality contracts with **Pandera**, and serves fund performance analytics via Spark SQL and Streamlit.

> **Status:** Under active development (Phase 1: Bronze Ingestion)

## Architecture Overview

```text
AMFI Portal (NAVAll.txt + 90-day Historical Windows)
       │
       ▼
┌──────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Immutable Raw Landing)                     │
│ • Raw semicolon-delimited text partitioned by date       │
│ • Idempotent ingestion with retries & SHA-256 checks     │
└──────────────────────────┬───────────────────────────────┘
                           │ Parse, Type, Dedupe & Validate
                           ▼
┌──────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleansed Delta Tables)                     │
│ • nav_daily (Fact) & scheme_dim (Dimension)              │
│ • Quarantine table for malformed or anomalous records    │
└──────────────────────────┬───────────────────────────────┘
                           │ Spark SQL Window Functions
                           ▼
┌──────────────────────────────────────────────────────────┐
│ GOLD LAYER (Analytics & Serving)                         │
│ • Daily returns, rolling volatility, max drawdown        │
│ • Trailing 1Y/3Y returns & category rankings             │
└──────────────────────────────────────────────────────────┘
Tech Stack
Processing & Storage: Python 3.11, PySpark 3.5, Delta Lake 3.2

Data Quality: Pandera

Testing & CI/CD: pytest, GitHub Actions

Visualization: Streamlit

Project Roadmap
[x] Phase 0: Repository setup, Windows Hadoop/Spark environment, Delta verification

[ ] Phase 1: Idempotent Bronze ingestion (NAVAll.txt)

[ ] Phase 2: Multi-header file parser & Silver Delta MERGE upserts

[ ] Phase 3: Pandera validation & quarantine routing

[ ] Phase 4: Historical backfill (90-day windows)

[ ] Phase 5: Gold analytics layer (Spark SQL window functions)

[ ] Phase 6: Unit test suite (pytest) & GitHub Actions CI

[ ] Phase 7: Automated daily scheduling

[ ] Phase 8: Interactive Streamlit dashboard

[ ] Phase 9: Final documentation & performance benchmarks