"""Data loading and caching for AHRQ Compendium, CMS POS, and NPPES."""

import logging
from pathlib import Path

import httpx
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

def parse_ahrq_system_file(path: Path) -> pd.DataFrame:
    """Parse the AHRQ Compendium system file.

    Returns DataFrame with columns: health_sys_id, health_sys_name,
    health_sys_city, health_sys_state, hosp_count, phys_grp_count.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    col_map = {}
    id_col = _find_column(df, ["health_sys_id", "sys_id", "system_id", "id"])
    name_col = _find_column(df, ["health_sys_name", "sys_name", "system_name", "name"])
    city_col = _find_column(df, ["health_sys_city", "sys_city", "city"])
    state_col = _find_column(df, ["health_sys_state", "sys_state", "state"])
    hosp_col = _find_column(df, ["hosp_count", "hospital_count", "num_hospitals", "n_hosp"])
    phys_col = _find_column(df, ["phys_grp_count", "physician_group_count", "n_phys_grp"])

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
    if phys_col:
        col_map[phys_col] = "phys_grp_count"

    df = df.rename(columns=col_map)

    for int_col in ["hosp_count", "phys_grp_count"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)

    return df


def parse_ahrq_hospital_linkage(path: Path) -> pd.DataFrame:
    """Parse the AHRQ Compendium hospital linkage file.

    Returns DataFrame with columns: health_sys_id, ccn, hospital_name,
    hosp_city, hosp_state, hosp_zip, hos_beds, hos_dsch, ownership, teaching.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    col_map = {}
    ccn_col = _find_column(df, ["ccn", "medicare_provider_number", "provider_number", "prvdr_num"])
    if ccn_col:
        col_map[ccn_col] = "ccn"

    df = df.rename(columns=col_map)

    if "ccn" in df.columns:
        df["ccn"] = df["ccn"].astype(str).str.strip().str.zfill(6)

    for int_col in ["hos_beds", "hos_dsch"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)

    return df


async def _download_if_missing(url: str, cache_path: Path) -> Path:
    """Download a file if not cached. Returns cache path.

    Raises RuntimeError with instructions if AHRQ WAF blocks the download.
    """
    if cache_path.exists():
        logger.info("Using cached file: %s", cache_path)
        return cache_path

    logger.info("Downloading %s ...", url)
    async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
        resp = await client.get(url)

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
        cache_path.write_bytes(resp.content)

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
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace", low_memory=False)
    df.columns = [c.strip() for c in df.columns]
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

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(NPPES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
