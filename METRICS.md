# Pipeline Metrics & Benchmarks

## Phase 1: Bronze Ingestion
- **Daily Raw File Size:** 1.46 MB (1,531,648 bytes)
- **Raw Line Count (`NAVAll.txt`):** 18,281 lines (2026-11-18)
- **Ingestion Runtime:** ~0.77s (with SHA-256 idempotency check)

## Phase 2 & 3: Silver Star Schema + Data Quality
- **Total Fund Records Parsed:** 14,465
- **Clean Silver Records (`nav_daily` & `scheme_dim`):** 14,212
- **Quarantined Records (`quarantine`):** 253 (241 `"N.A."` NAVs + 12 zero-NAV schemes caught before Gold return math)
- **Idempotency:** Verified via Delta Lake `MERGE` on `(scheme_code, nav_date)` — 0 duplicate rows on rerun