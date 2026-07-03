"""Data loading and caching for AHRQ Compendium, CMS POS, and NPPES."""

import logging
from datetime import datetime, timezone
from pathlib import Path


from shared.utils.cache import CacheMetadata, write_atomic_bytes, write_cache_metadata
from shared.utils.http_client import resilient_request
from shared.utils.tabular_normalization import read_csv_strings
import pandas as pd

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# --- AHRQ Compendium URLs (2023 release) ---
AHRQ_SYSTEM_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv"
)
AHRQ_HOSPITAL_LINKAGE_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv"
)

AHRQ_SYSTEM_CACHE = CACHE_DIR / "ahrq_system_2023.csv"
AHRQ_HOSPITAL_LINKAGE_CACHE = CACHE_DIR / "ahrq_hospital_linkage_2023.csv"

# --- CMS POS File (Q4 2025) ---
POS_URL = (
    "https://data.cms.gov/sites/default/files/2026-01/"
    "c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv"
)
POS_CACHE = CACHE_DIR / "pos_q4_2025.csv"

# --- NPPES API ---
NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"

# --- In-memory caches ---
_ahrq_systems_df: pd.DataFrame | None = None
_ahrq_hospitals_df: pd.DataFrame | None = None
_pos_df: pd.DataFrame | None = None


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name (case-insensitive)."""
    lower_map = {col.lower().strip(): col for col in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower().strip() in lower_map:
            return lower_map[c.lower().strip()]
    return None


# ============================================================
# AHRQ Compendium loaders
# ============================================================

_AHRQ_SYSTEM_INT_COLUMNS = (
    "total_mds",
    "prim_care_mds",
    "total_nps",
    "total_pas",
    "grp_cnt",
    "grp_cnt_restricted",
    "hosp_cnt",
    "acutehosp_cnt",
    "nh_cnt",
    "nh_cnt_restricted",
    "hhco_cnt",
    "hhco_cnt_restricted",
    "sys_beds",
    "sys_dsch",
    "hosp_count",
    "phys_grp_count",
)

_AHRQ_HOSPITAL_INT_COLUMNS = (
    "acutehosp_flag",
    "hos_beds",
    "hos_dsch",
    "hos_children",
    "hos_majteach",
    "hos_vmajteach",
)


def _nullable_int_column(df: pd.DataFrame, column: str) -> None:
    if column in df.columns:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")


def parse_ahrq_system_file(path: Path) -> pd.DataFrame:
    """Parse the AHRQ Compendium system file.

    Returns a normalized DataFrame that preserves the source snapshot fields
    used for source-disciplined system metrics.
    """
    df = read_csv_strings(path, normalize_columns=True)

    col_map = {}
    id_col = _find_column(df, ["health_sys_id", "sys_id", "system_id", "id"])
    name_col = _find_column(df, ["health_sys_name", "sys_name", "system_name", "name"])
    city_col = _find_column(df, ["health_sys_city", "sys_city", "city"])
    state_col = _find_column(df, ["health_sys_state", "sys_state", "state"])
    hosp_col = _find_column(df, ["hosp_count", "hospital_count", "num_hospitals", "n_hosp"])
    phys_col = _find_column(df, ["phys_grp_count", "physician_group_count", "n_phys_grp", "grp_cnt"])

    if id_col:
        col_map[id_col] = "health_sys_id"
    if name_col:
        col_map[name_col] = "health_sys_name"
    if city_col:
        col_map[city_col] = "health_sys_city"
    if state_col:
        col_map[state_col] = "health_sys_state"
    if hosp_col:
        col_map[hosp_col] = "hosp_count"
    if phys_col and phys_col != "grp_cnt":
        col_map[phys_col] = "phys_grp_count"

    df = df.rename(columns=col_map)
    if "hosp_count" not in df.columns and "hosp_cnt" in df.columns:
        df["hosp_count"] = df["hosp_cnt"]
    if "phys_grp_count" not in df.columns and "grp_cnt" in df.columns:
        df["phys_grp_count"] = df["grp_cnt"]

    for int_col in _AHRQ_SYSTEM_INT_COLUMNS:
        _nullable_int_column(df, int_col)

    return df


def parse_ahrq_hospital_linkage(path: Path) -> pd.DataFrame:
    """Parse the AHRQ Compendium hospital linkage file.

    Returns a normalized DataFrame with source identifiers preserved as strings.
    """
    df = read_csv_strings(path, normalize_columns=True)

    col_map = {}
    ccn_col = _find_column(df, ["ccn", "medicare_provider_number", "provider_number", "prvdr_num"])
    street_col = _find_column(df, ["hospital_street", "hosp_addr", "hospital_address", "address"])
    city_col = _find_column(df, ["hospital_city", "hosp_city", "city"])
    state_col = _find_column(df, ["hospital_state", "hosp_state", "state"])
    zip_col = _find_column(df, ["hospital_zip", "hosp_zip", "zip", "zip_code"])
    ownership_col = _find_column(df, ["hos_ownership", "ownership"])
    teaching_col = _find_column(df, ["hos_teachint", "teaching"])
    if ccn_col:
        col_map[ccn_col] = "ccn"
    if street_col and street_col != "hospital_street":
        col_map[street_col] = "hospital_street"
    if city_col and city_col != "hospital_city":
        col_map[city_col] = "hospital_city"
    if state_col and state_col != "hospital_state":
        col_map[state_col] = "hospital_state"
    if zip_col and zip_col != "hospital_zip":
        col_map[zip_col] = "hospital_zip"
    if ownership_col and ownership_col != "hos_ownership":
        col_map[ownership_col] = "hos_ownership"
    if teaching_col and teaching_col != "hos_teachint":
        col_map[teaching_col] = "hos_teachint"

    df = df.rename(columns=col_map)

    if "ccn" in df.columns:
        df["ccn"] = df["ccn"].astype(str).str.strip().map(lambda value: value.zfill(6) if value else "")
    if "hospital_zip" in df.columns:
        df["hospital_zip"] = df["hospital_zip"].astype(str).str.strip()

    for int_col in _AHRQ_HOSPITAL_INT_COLUMNS:
        _nullable_int_column(df, int_col)

    return df


async def _download_if_missing(url: str, cache_path: Path) -> Path:
    """Download a file if not cached. Returns cache path.

    Raises RuntimeError with instructions if AHRQ WAF blocks the download.
    """
    if cache_path.exists():
        logger.info("Using cached file: %s", cache_path)
        return cache_path

    logger.info("Downloading %s ...", url)
    resp = await resilient_request("GET", url, timeout=300.0)

    # Detect WAF challenge (AHRQ returns 202 with empty body or HTML)
    if resp.status_code == 202 or (
        resp.status_code == 200
        and b"<!DOCTYPE" in resp.content[:200]
    ):
        raise RuntimeError(
            f"Download blocked by WAF for {url}. "
            f"Run 'python scripts/download_ahrq.py' to download via browser, "
            f"or manually download and place at {cache_path}"
        )

    resp.raise_for_status()
    write_atomic_bytes(cache_path, resp.content)
    write_cache_metadata(
        cache_path,
        CacheMetadata(
            source_url=url,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            content_length=len(resp.content),
            cache_key=cache_path.name,
        ),
    )

    logger.info("Saved to: %s (%d bytes)", cache_path, cache_path.stat().st_size)
    return cache_path


async def load_ahrq_systems(force: bool = False) -> pd.DataFrame:
    """Load the AHRQ Compendium system file."""
    global _ahrq_systems_df
    if not force and _ahrq_systems_df is not None:
        return _ahrq_systems_df

    path = await _download_if_missing(AHRQ_SYSTEM_URL, AHRQ_SYSTEM_CACHE)
    _ahrq_systems_df = parse_ahrq_system_file(path)
    return _ahrq_systems_df


async def load_ahrq_hospital_linkage(force: bool = False) -> pd.DataFrame:
    """Load the AHRQ Compendium hospital linkage file."""
    global _ahrq_hospitals_df
    if not force and _ahrq_hospitals_df is not None:
        return _ahrq_hospitals_df

    path = await _download_if_missing(AHRQ_HOSPITAL_LINKAGE_URL, AHRQ_HOSPITAL_LINKAGE_CACHE)
    _ahrq_hospitals_df = parse_ahrq_hospital_linkage(path)
    return _ahrq_hospitals_df


# ============================================================
# CMS POS File loader
# ============================================================

def parse_pos_file(path: Path) -> pd.DataFrame:
    """Parse the CMS Provider of Services file.

    Reads with dtype=str to avoid type issues, then converts numeric columns.
    POS has 470+ columns — we read all with dtype=str and filter later.
    """
    df = read_csv_strings(path, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]
    return df


async def load_pos(force: bool = False) -> pd.DataFrame:
    """Load the CMS Provider of Services file."""
    global _pos_df
    if not force and _pos_df is not None:
        return _pos_df

    path = await _download_if_missing(POS_URL, POS_CACHE)
    _pos_df = parse_pos_file(path)
    return _pos_df


# ============================================================
# NPPES API
# ============================================================

async def search_nppes(
    organization_name: str | None = None,
    state: str | None = None,
    taxonomy_description: str | None = None,
    enumeration_type: str = "NPI-2",
    limit: int = 200,
) -> list[dict]:
    """Search the NPPES NPI Registry for organizations."""
    params: dict = {"version": "2.1", "limit": min(limit, 200)}
    if organization_name:
        params["organization_name"] = organization_name
    if state:
        params["state"] = state
    if taxonomy_description:
        params["taxonomy_description"] = taxonomy_description
    if enumeration_type:
        params["enumeration_type"] = enumeration_type

    resp = await resilient_request("GET", NPPES_API_URL, params=params, timeout=30.0)
    data = resp.json()
    return data.get("results", [])
