"""
Central config for the SIF (Specialized Investment Fund) pipeline.

Mirror of the MF config/constants.py, pointed at a SEPARATE Blob container so
the two asset classes never share data or scheme codes. The Azure storage
account (and therefore the AZURE_STORAGE_CONNECTION_STRING secret) is shared;
only the container differs.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# -- Project root (repo root, two levels up from sif/config/) --
ROOT_DIR = Path(__file__).resolve().parents[2]

# Load .env (gitignored) so AZURE_STORAGE_CONNECTION_STRING is available
# locally. In CI the var comes from the environment directly; load_dotenv is
# a no-op there.
load_dotenv(ROOT_DIR / ".env")

# -- Local data paths (staging before upload + inspection) --
SIF_DATA_DIR = ROOT_DIR / "sif" / "data"
RAW_NAV_DIR = SIF_DATA_DIR / "raw" / "nav"
PROCESSED_DIR = SIF_DATA_DIR / "processed"

NAV_HISTORY_PARQUET = PROCESSED_DIR / "nav_history.parquet"
SCHEME_MASTER_PARQUET = PROCESSED_DIR / "scheme_master.parquet"

# -- Azure Blob (the source of truth -- serverless lakehouse) --
# Connection string is read from the environment, never hard-coded.
# Locally: put it in a .env file (gitignored). In CI: a GitHub Actions secret.
# SAME account/secret as MF -- only the container is different.
AZURE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

BLOB_CONTAINER = "sifnavdata"

# Logical paths inside the container. Identical layout to the MF container so
# the DuckDB view DDL is unchanged; nav_history is partitioned by year so the
# daily job only rewrites the current year.
BLOB_RAW_PREFIX = "raw/nav"  # raw/nav/year=YYYY/*.parquet
BLOB_NAV_HISTORY_DIR = "processed/nav_history"  # processed/nav_history/year=YYYY/data.parquet
BLOB_SCHEME_MASTER = "processed/scheme_master.parquet"

# az:// URIs DuckDB reads from (hive_partitioning picks up the year=YYYY dirs)
AZ_NAV_HISTORY_GLOB = f"az://{BLOB_CONTAINER}/{BLOB_NAV_HISTORY_DIR}/year=*/*.parquet"
AZ_SCHEME_MASTER = f"az://{BLOB_CONTAINER}/{BLOB_SCHEME_MASTER}"

# -- AMFI SIF endpoints --
# Daily feed: single file, all SIFs, like NAVAll.txt but for SIF.
SIF_DAILY_URL = "https://portal.amfiindia.com/spages/SIF_NAVAll.txt"
# History: bulk download, NO mf= parameter (one request per date window returns
# every SIF), unlike the MF history endpoint.
SIF_HISTORY_URL = (
    "https://portal.amfiindia.com/SIF_DownloadNAVHistoryReport.aspx"
    "?frmdt={frmdt}&todt={todt}"
)

# -- Fetch settings --
REQUEST_TIMEOUT = 30  # seconds per request
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0  # exponential backoff multiplier
CONCURRENT_WORKERS = 10  # threads for parallel fetching

# -- Retention --
RETENTION_YEARS = 5  # keep only last N years of raw files

# -- Logging --
LOG_DIR = ROOT_DIR / "logs"
LOG_FILE = LOG_DIR / "sif_pipeline.log"
LOG_LEVEL = "INFO"
