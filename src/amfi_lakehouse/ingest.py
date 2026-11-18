import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from zoneinfo import ZoneInfo
from amfi_lakehouse.config import AMFI_HISTORY_BASE_URL

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

def generate_date_windows(
    start_date: date,
    end_date: date,
    window_days: int = 89,
) -> list[tuple[date, date]]:
    """Split [start_date, end_date] into inclusive chunks of at most 90 days."""
    windows: list[tuple[date, date]] = []
    cursor = start_date
    while cursor <= end_date:
        window_end = min(cursor + timedelta(days=window_days), end_date)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def ingest_history_backfill(
    start_date_str: str,
    end_date_str: str,
    scheme_type: int = 1,
    sleep_between_requests_sec: float = 1.5,
) -> dict:
    """
    Backfill historical AMFI NAVs in <=90-day windows into Bronze.
    Resumable: skips windows that already exist on disk with valid metadata.
    scheme_type=1 -> Open Ended Schemes.
    """
    overall_start = time.perf_counter()
    start_dt = date.fromisoformat(start_date_str)
    end_dt = date.fromisoformat(end_date_str)
    windows = generate_date_windows(start_dt, end_dt, window_days=89)

    session = _build_session()
    downloaded = 0
    skipped = 0
    total_lines = 0

    for idx, (w_start, w_end) in enumerate(windows, start=1):
        w_label = f"{w_start.isoformat()}_to_{w_end.isoformat()}"
        w_dir = BRONZE_DIR / "nav_history" / f"window={w_label}"
        w_dir.mkdir(parents=True, exist_ok=True)
        target_file = w_dir / "NAVHistory.txt"
        meta_file = w_dir / "_metadata.json"

        if target_file.exists() and meta_file.exists():
            skipped += 1
            logger.info("[%d/%d] Skipping existing window %s", idx, len(windows), w_label)
            continue

        params = {
            "tp": scheme_type,
            "frmdt": w_start.strftime("%d-%b-%Y"),
            "todt": w_end.strftime("%d-%b-%Y"),
        }
        logger.info(
            "[%d/%d] Downloading AMFI history window %s -> %s",
            idx,
            len(windows),
            params["frmdt"],
            params["todt"],
        )
        t0 = time.perf_counter()
        resp = session.get(AMFI_HISTORY_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()

        raw_bytes = resp.content
        if len(raw_bytes) < 512 or b"Scheme Code;" not in raw_bytes[:500]:
            raise ValueError(
                f"Unexpected AMFI history payload for {w_label} (size={len(raw_bytes)} bytes)."
            )

        target_file.write_bytes(raw_bytes)
        lines = raw_bytes.count(b"\n")
        total_lines += lines
        downloaded += 1

        meta = {
            "source_url": resp.url,
            "window_start": w_start.isoformat(),
            "window_end": w_end.isoformat(),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "sha256": _sha256_bytes(raw_bytes),
            "size_bytes": len(raw_bytes),
            "line_count": lines,
            "runtime_sec": round(time.perf_counter() - t0, 3),
        }
        meta_file.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        logger.info(
            "[%d/%d] Saved %s (%d lines, %.2f MB) in %.2fs",
            idx,
            len(windows),
            w_label,
            lines,
            len(raw_bytes) / (1024 * 1024),
            meta["runtime_sec"],
        )

        if idx < len(windows):
            time.sleep(sleep_between_requests_sec)

    return {
        "start_date": start_date_str,
        "end_date": end_date_str,
        "total_windows": len(windows),
        "downloaded_windows": downloaded,
        "skipped_windows": skipped,
        "newly_downloaded_lines": total_lines,
        "total_runtime_sec": round(time.perf_counter() - overall_start, 3),
    }

if __name__ == "__main__":
    result = ingest_daily_nav()
    print(json.dumps(result, indent=2))