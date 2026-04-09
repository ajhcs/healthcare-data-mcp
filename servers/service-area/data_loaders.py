"""Data loaders for CMS HSAF and Dartmouth Atlas crosswalk files."""

import io
import logging
import zipfile
from pathlib import Path


from shared.utils.http_client import resilient_request
import pandas as pd

import sys as _sys
_project_root = __import__("pathlib").Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in _sys.path:
    _sys.path.insert(0, str(_project_root))

from shared.utils.cache import is_cache_valid  # noqa: E402
from shared.utils.column_detection import find_df_column  # noqa: E402

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# CMS Hospital Service Area File — most recent available
# The HSAF is published under the Medicare Inpatient Hospitals provider summary.
# Direct CSV download URL (2024 release, published July 2025):
HSAF_CSV_URL = (
    "https://data.cms.gov/sites/default/files/2025-07/"
    "8fca1932-adaa-411d-a912-78fb0854a286/Hospital_Service_Area_2024.csv"
)
# API endpoint (JSON):
HSAF_API_URL = "https://data.cms.gov/data-api/v1/dataset/8708ca8b-8636-44ed-8303-724cbfaf78ad/data"
HSAF_CACHE_PATH = CACHE_DIR / "hsaf.csv"

# Dartmouth Atlas ZIP crosswalk (CSV in ZIP archive, 2019 is the most recent)
DARTMOUTH_CROSSWALK_URL = "https://data.dartmouthatlas.org/downloads/geography/ZipHsaHrr19.csv.zip"
DARTMOUTH_CACHE_PATH = CACHE_DIR / "dartmouth_zip_crosswalk.csv"

# Column name variants across HSAF releases
_CCN_COLS = [
    "MEDICARE_PROV_NUM", "Rndrng_Prvdr_CCN", "Provider CCN", "provider_ccn", "CCN", "Prvdr_Num",
]
_NAME_COLS = [
    "Rndrng_Prvdr_Org_Name", "Provider Name", "provider_name", "Prvdr_Name",
]
_ZIP_COLS = [
    "ZIP_CD_OF_RESIDENCE", "Bene_ZIP_CD", "Beneficiary ZIP Code", "bene_zip_cd", "ZIP_CD", "zip_code",
]
_CASES_COLS = [
    "TOTAL_CASES", "Tot_Cases", "Total Discharges", "total_discharges", "Tot_Dschrgs", "tot_cases",
]


_BULK_TTL_DAYS = 90  # CMS bulk data refresh cadence


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name from a list of candidates."""
    return find_df_column(df, candidates)


def _normalize_hsaf(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize HSAF column names to a consistent schema."""
    ccn_col = _find_column(df, _CCN_COLS)
    name_col = _find_column(df, _NAME_COLS)
    zip_col = _find_column(df, _ZIP_COLS)
    cases_col = _find_column(df, _CASES_COLS)

    if not ccn_col or not zip_col or not cases_col:
        available = list(df.columns)
        raise ValueError(
            f"Cannot find required HSAF columns. Available: {available}. "
            f"Found CCN={ccn_col}, ZIP={zip_col}, Cases={cases_col}"
        )

    rename = {ccn_col: "ccn", zip_col: "zip_code", cases_col: "discharges"}
    if name_col:
        rename[name_col] = "facility_name"

    df = df.rename(columns=rename)
    df["ccn"] = df["ccn"].astype(str).str.strip().str.zfill(6)
    df["zip_code"] = df["zip_code"].astype(str).str.strip().str.zfill(5)
    df["discharges"] = pd.to_numeric(df["discharges"], errors="coerce").fillna(0).astype(int)

    if "facility_name" not in df.columns:
        df["facility_name"] = ""

    return df[["ccn", "facility_name", "zip_code", "discharges"]]


async def download_hsaf(force: bool = False) -> pd.DataFrame:
    """Download and cache the CMS Hospital Service Area File.

    Returns a normalized DataFrame with columns: ccn, facility_name, zip_code, discharges.
    """
    if not force and is_cache_valid(HSAF_CACHE_PATH, max_age_days=_BULK_TTL_DAYS):
        logger.info("Loading cached HSAF from %s", HSAF_CACHE_PATH)
        df = pd.read_csv(HSAF_CACHE_PATH, dtype=str)
        return _normalize_hsaf(df)

    logger.info("Downloading HSAF from %s", HSAF_CSV_URL)
    resp = await resilient_request("GET", HSAF_CSV_URL, timeout=300.0)
    HSAF_CACHE_PATH.write_bytes(resp.content)

    logger.info("HSAF cached to %s (%d bytes)", HSAF_CACHE_PATH, HSAF_CACHE_PATH.stat().st_size)
    df = pd.read_csv(HSAF_CACHE_PATH, dtype=str)
    return _normalize_hsaf(df)


async def download_dartmouth_crosswalk(force: bool = False) -> pd.DataFrame:
    """Download and cache the Dartmouth Atlas ZIP-to-HSA/HRR crosswalk.

    Returns a DataFrame with columns: zip_code, hsanum, hsacity, hsastate, hrrnum, hrrcity, hrrstate.
    """
    if not force and is_cache_valid(DARTMOUTH_CACHE_PATH, max_age_days=_BULK_TTL_DAYS):
        logger.info("Loading cached Dartmouth crosswalk from %s", DARTMOUTH_CACHE_PATH)
        df = pd.read_csv(DARTMOUTH_CACHE_PATH, dtype=str)
        return _normalize_dartmouth(df)

    logger.info("Downloading Dartmouth crosswalk from %s", DARTMOUTH_CROSSWALK_URL)
    resp = await resilient_request("GET", DARTMOUTH_CROSSWALK_URL, timeout=120.0)

    # Handle ZIP archive: extract the CSV inside
    content = resp.content
    if DARTMOUTH_CROSSWALK_URL.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if csv_names:
                content = zf.read(csv_names[0])
            else:
                # Fallback: read the first file
                content = zf.read(zf.namelist()[0])

    DARTMOUTH_CACHE_PATH.write_bytes(content)
    logger.info("Dartmouth crosswalk cached to %s", DARTMOUTH_CACHE_PATH)
    df = pd.read_csv(DARTMOUTH_CACHE_PATH, dtype=str)
    return _normalize_dartmouth(df)


# Hospital General Info for facility name cross-reference
_HOSP_INFO_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv"
_HOSP_INFO_CACHE = CACHE_DIR / "hospital_general_info.csv"


async def load_hospital_names() -> dict[str, str]:
    """Load a CCN → facility name mapping from CMS Hospital General Info.

    Returns dict mapping CCN strings to facility name strings.
    """
    if not _HOSP_INFO_CACHE.exists():
        logger.info("Downloading Hospital General Info for name lookup...")
        resp = await resilient_request("GET", _HOSP_INFO_URL, timeout=300.0)
        _HOSP_INFO_CACHE.write_bytes(resp.content)

    df = pd.read_csv(_HOSP_INFO_CACHE, dtype=str, keep_default_na=False,
                      usecols=lambda c: c.strip() in ("Facility ID", "Facility Name"))
    return dict(zip(
        df["Facility ID"].str.strip(),
        df["Facility Name"].str.strip(),
    ))


def _normalize_dartmouth(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Dartmouth crosswalk columns."""
    # The Dartmouth CSV columns are typically: zipcode18, hsanum, hsacity, hsastate, hrrnum, hrrcity, hrrstate
    col_map = {}
    for col in df.columns:
        lc = col.lower().strip()
        if lc in ("zipcode19", "zipcode18", "zipcode", "zip_code", "zip"):
            col_map[col] = "zip_code"
        elif lc == "hsanum":
            col_map[col] = "hsanum"
        elif lc == "hsacity":
            col_map[col] = "hsacity"
        elif lc == "hsastate":
            col_map[col] = "hsastate"
        elif lc == "hrrnum":
            col_map[col] = "hrrnum"
        elif lc == "hrrcity":
            col_map[col] = "hrrcity"
        elif lc == "hrrstate":
            col_map[col] = "hrrstate"

    df = df.rename(columns=col_map)
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).str.strip().str.zfill(5)
    return df
