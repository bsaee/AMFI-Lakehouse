from pathlib import Path

# Project Root: .../amfi-lakehouse/
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Data Lake Layers
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Local Windows Hadoop Binaries (used only on Windows)
HADOOP_HOME = PROJECT_ROOT / "hadoop"

# AMFI Endpoints
AMFI_DAILY_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
AMFI_HISTORY_BASE_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

# HTTP Request Settings
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2