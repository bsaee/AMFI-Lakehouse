from datetime import datetime
from pathlib import Path


def _parse_nav(raw_nav: str) -> float | None:
    cleaned = raw_nav.strip()
    if not cleaned or cleaned.upper() in {"N.A.", "NA", "-"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_date(raw_date: str) -> str | None:
    """Convert AMFI date format (e.g., '18-Nov-2026') to ISO 'YYYY-MM-DD'."""
    cleaned = raw_date.strip()
    if not cleaned:
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _clean_isin(raw_isin: str) -> str | None:
    cleaned = raw_isin.strip()
    return None if cleaned in {"", "-", "N.A.", "NA"} else cleaned


def parse_amfi_lines(lines: list[str], ingestion_date: str) -> list[dict]:
    """
    Parse AMFI NAV text lines containing interleaved category headers,
    AMC headers, and semicolon-delimited fund NAV rows.
    """
    records: list[dict] = []
    current_category: str | None = None
    current_amc: str | None = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # Skip the column header row
        if line.startswith("Scheme Code;"):
            continue

        parts = line.split(";")

        # Header line (no semicolons): either Scheme Category or AMC Name
        if len(parts) == 1:
            # AMFI category headers typically contain '(' e.g., "Open Ended Schemes(Equity Scheme - Large Cap Fund)"
            if "(" in line and ")" in line:
                current_category = line
            else:
                current_amc = line
            continue

        # Data row: expect 6 semicolon-separated fields
        if len(parts) >= 6:
            scheme_code_str = parts[0].strip()
            if not scheme_code_str.isdigit():
                continue

            records.append(
                {
                    "scheme_code": int(scheme_code_str),
                    "isin_growth": _clean_isin(parts[1]),
                    "isin_reinvest": _clean_isin(parts[2]),
                    "scheme_name": parts[3].strip(),
                    "nav": _parse_nav(parts[4]),
                    "nav_date": _parse_date(parts[5]),
                    "amc_name": current_amc,
                    "scheme_category": current_category,
                    "ingestion_date": ingestion_date,
                }
            )

    return records


def parse_bronze_file(file_path: Path, ingestion_date: str) -> list[dict]:
    text = file_path.read_text(encoding="utf-8", errors="replace")
    return parse_amfi_lines(text.splitlines(), ingestion_date=ingestion_date)