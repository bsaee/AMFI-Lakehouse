# Pipeline Metrics & Benchmarks

## Phase 1: Bronze Ingestion
- **Daily Raw File Size:** 1.46 MB (1,531,648 bytes)
- **Raw Line Count (`NAVAll.txt`):** 18,281 lines (2026-11-18)
- **Ingestion Runtime:** ~0.77s (with SHA-256 idempotency check)

## Phase 2 & 3: Silver Star Schema + Data Quality
- **Daily Fund Records Parsed:** 14,465
- **Daily Clean Silver Records:** 14,212
- **Daily Quarantined Records:** 253 (`"N.A."` and zero-NAV schemes caught before Gold return math)
- **Idempotency:** Verified via Delta Lake `MERGE` on `(scheme_code, nav_date)` — 0 duplicate rows on rerun

## Phase 4: Historical Backfill (3.1 Years: Oct 2023 – Nov 2026)
- **Bronze Windows Downloaded:** 13 chunks (<=90-day windows), 6,112,483 raw lines (~770 MB raw text)
- **Memory Optimization:** Streamed chunk parsing to Snappy Parquet staging + monthly (`year_month`) Delta partitioning to avoid driver OOM and small-file overhead
- **Final Silver `nav_daily` Row Count:** 6,014,674 rows
- **Final Silver `scheme_dim` Row Count:** 14,433 schemes
- **Total Quarantined Rows (`quarantine`):** 8,848 rows