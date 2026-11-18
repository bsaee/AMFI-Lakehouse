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
    """Convert AMFI date format (e.g., '17-Nov-2026') to ISO 'YYYY-MM-DD'."""
    cleaned = raw_date.strip()
    if not cleaned:
        return None
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _clean_optional_str(raw_val: str) -> str | None:
    cleaned = raw_val.strip()
    return None if cleaned in {"", "-", "N.A.", "NA"} else cleaned


def parse_amfi_lines(lines: list[str], ingestion_date: str) -> list[dict]:
    """
    Parse AMFI NAV text lines containing interleaved category headers,
    AMC headers, and semicolon-delimited fund NAV rows.
    Supports both 8-column (with Plan, Option) and legacy 6-column AMFI formats.
    """
    records: list[dict] = []
    current_category: str | None = None
    current_amc: str | None = None

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line.startswith("Scheme Code;"):
            continue

        parts = line.split(";")

        # Header line (no semicolons): either Scheme Category or AMC Name
        if len(parts) == 1:
            if "(" in line and ")" in line:
                current_category = line
            else:
                current_amc = line
            continue

        scheme_code_str = parts[0].strip()
        if not scheme_code_str.isdigit():
            continue

        # Current 8-column layout:
        # Scheme Code;ISIN Growth;ISIN Reinvest;Scheme Name;Plan;Option;NAV;Date
        if len(parts) >= 8:
            plan = _clean_optional_str(parts[4])
            option = _clean_optional_str(parts[5])
            nav = _parse_nav(parts[6])
            nav_date = _parse_date(parts[7])
        # Legacy 6-column layout (used in some historical exports):
        # Scheme Code;ISIN Growth;ISIN Reinvest;Scheme Name;NAV;Date
        elif len(parts) >= 6:
            plan = None
            option = None
            nav = _parse_nav(parts[4])
            nav_date = _parse_date(parts[5])
        else:
            continue

        records.append(
            {
                "scheme_code": int(scheme_code_str),
                "isin_growth": _clean_optional_str(parts[1]),
                "isin_reinvest": _clean_optional_str(parts[2]),
                "scheme_name": parts[3].strip(),
                "plan": plan,
                "option": option,
                "nav": nav,
                "nav_date": nav_date,
                "amc_name": current_amc,
                "scheme_category": current_category,
                "ingestion_date": ingestion_date,
            }
        )

    return records


def parse_bronze_file(file_path: Path, ingestion_date: str) -> list[dict]:
    text = file_path.read_text(encoding="utf-8", errors="replace")
    return parse_amfi_lines(text.splitlines(), ingestion_date=ingestion_date)