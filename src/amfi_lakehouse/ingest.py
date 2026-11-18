import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from amfi_lakehouse.config import (
    AMFI_DAILY_NAV_URL,
    BRONZE_DIR,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("amfi.ingest")

IST = ZoneInfo("Asia/Kolkata")


def _build_session() -> requests.Session:
    """Create a requests session with retry handling for transient HTTP errors."""
    session = requests.Session()
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_SECONDS,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "amfi-lakehouse-pipeline/0.1 (+https://github.com/data-engineering-portfolio)"
        }
    )
    return session


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def ingest_daily_nav(ingestion_date: str | None = None) -> dict:
    """
    Download AMFI NAVAll.txt into the Bronze layer partitioned by ingestion_date.
    Idempotent: if today's file already exists with an identical SHA-256 hash, skips overwrite.
    """
    start_time = time.perf_counter()
    run_date = ingestion_date or datetime.now(IST).strftime("%Y-%m-%d")

    partition_dir = BRONZE_DIR / "nav_all" / f"ingestion_date={run_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    target_file = partition_dir / "NAVAll.txt"
    metadata_file = partition_dir / "_metadata.json"

    logger.info("Fetching daily NAV data from %s for ingestion_date=%s", AMFI_DAILY_NAV_URL, run_date)
    session = _build_session()
    response = session.get(AMFI_DAILY_NAV_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    raw_bytes = response.content
    if len(raw_bytes) < 1024 or b"Scheme Code;ISIN" not in raw_bytes[:500]:
        raise ValueError(
            f"Unexpected payload from AMFI (size={len(raw_bytes)} bytes). "
            "Header 'Scheme Code;ISIN' not found."
        )

    new_hash = _sha256_bytes(raw_bytes)
    line_count = raw_bytes.count(b"\n")

    # Idempotency check
    if target_file.exists() and metadata_file.exists():
        existing_meta = json.loads(metadata_file.read_text(encoding="utf-8"))
        if existing_meta.get("sha256") == new_hash:
            elapsed = round(time.perf_counter() - start_time, 3)
            logger.info(
                "Idempotent skip: %s already exists with matching SHA-256 (%s).",
                target_file,
                new_hash[:12],
            )
            return {
                "status": "skipped_duplicate",
                "ingestion_date": run_date,
                "path": str(target_file),
                "line_count": line_count,
                "sha256": new_hash,
                "runtime_sec": elapsed,
            }

    target_file.write_bytes(raw_bytes)
    elapsed = round(time.perf_counter() - start_time, 3)

    metadata = {
        "source_url": AMFI_DAILY_NAV_URL,
        "ingestion_date": run_date,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "sha256": new_hash,
        "size_bytes": len(raw_bytes),
        "line_count": line_count,
        "runtime_sec": elapsed,
    }
    metadata_file.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    logger.info(
        "Saved %s (%d lines, %.2f MB) in %.3fs",
        target_file,
        line_count,
        len(raw_bytes) / (1024 * 1024),
        elapsed,
    )
    return {"status": "written", "path": str(target_file), **metadata}


if __name__ == "__main__":
    result = ingest_daily_nav()
    print(json.dumps(result, indent=2))