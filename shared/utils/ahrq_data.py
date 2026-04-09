"""Shared AHRQ Compendium and CMS POS data loaders.

These functions are used by multiple servers (health-system-profiler,
physician-referral-network) and must be importable without cross-server
dependencies.

Extracted from servers/health-system-profiler/data_loaders.py to resolve
HDM-5w3: cross-server import breaks physician-referral-network.
"""

import logging
from pathlib import Path

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

AHRQ_SYSTEM_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv"
)
AHRQ_HOSPITAL_LINKAGE_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv"
)
AHRQ_SYSTEM_CACHE = CACHE_DIR / "ahrq_system_2023.csv"
AHRQ_HOSPITAL_LINKAGE_CACHE = CACHE_DIR / "ahrq_hospital_linkage_2023.csv"

POS_URL = (
    "https://data.cms.gov/sites/default/files/2026-01/"
    "c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv"
)
POS_CACHE = CACHE_DIR / "pos_q4_2025.csv"

_ahrq_systems_df: pd.DataFrame | None = None
_ahrq_hospitals_df: pd.DataFrame | None = None
_pos_df: pd.DataFrame | None = None


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower().strip(): col for col in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower().strip() in lower_map:
            return lower_map[c.lower().strip()]
    return None


def parse_ahrq_system_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    col_map = {}
    for orig, target in [
        (_find_column(df, ["health_sys_id", "sys_id", "system_id", "id"]), "health_sys_id"),
        (_find_column(df, ["health_sys_name", "sys_name", "system_name", "name"]), "health_sys_name"),
        (_find_column(df, ["health_sys_city", "sys_city", "city"]), "health_sys_city"),
        (_find_column(df, ["health_sys_state", "sys_state", "state"]), "health_sys_state"),
        (_find_column(df, ["hosp_count", "hospital_count", "num_hospitals", "n_hosp"]), "hosp_count"),
        (_find_column(df, ["phys_grp_count", "physician_group_count", "n_phys_grp"]), "phys_grp_count"),
    ]:
        if orig:
            col_map[orig] = target
    df = df.rename(columns=col_map)
    for int_col in ["hosp_count", "phys_grp_count"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)
    return df


def parse_ahrq_hospital_linkage(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    ccn_col = _find_column(df, ["ccn", "medicare_provider_number", "provider_number", "prvdr_num"])
    if ccn_col:
        df = df.rename(columns={ccn_col: "ccn"})
    if "ccn" in df.columns:
        df["ccn"] = df["ccn"].astype(str).str.strip().str.zfill(6)
    for int_col in ["hos_beds", "hos_dsch"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)
    return df


def parse_pos_file(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace", low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    return df


async def _download_if_missing(url: str, cache_path: Path) -> Path:
    if cache_path.exists():
        logger.info("Using cached file: %s", cache_path)
        return cache_path
    logger.info("Downloading %s ...", url)
    resp = await resilient_request("GET", url, timeout=300.0)
    if resp.status_code == 202 or (resp.status_code == 200 and b"<!DOCTYPE" in resp.content[:200]):
        raise RuntimeError(
            f"Download blocked by WAF for {url}. "
            f"Run 'python scripts/download_ahrq.py' to download via browser, "
            f"or manually download and place at {cache_path}"
        )
    cache_path.write_bytes(resp.content)
    logger.info("Saved to: %s (%d bytes)", cache_path, cache_path.stat().st_size)
    return cache_path


async def load_ahrq_systems(force: bool = False) -> pd.DataFrame:
    global _ahrq_systems_df
    if not force and _ahrq_systems_df is not None:
        return _ahrq_systems_df
    path = await _download_if_missing(AHRQ_SYSTEM_URL, AHRQ_SYSTEM_CACHE)
    _ahrq_systems_df = parse_ahrq_system_file(path)
    return _ahrq_systems_df


async def load_ahrq_hospital_linkage(force: bool = False) -> pd.DataFrame:
    global _ahrq_hospitals_df
    if not force and _ahrq_hospitals_df is not None:
        return _ahrq_hospitals_df
    path = await _download_if_missing(AHRQ_HOSPITAL_LINKAGE_URL, AHRQ_HOSPITAL_LINKAGE_CACHE)
    _ahrq_hospitals_df = parse_ahrq_hospital_linkage(path)
    return _ahrq_hospitals_df


async def load_pos(force: bool = False) -> pd.DataFrame:
    global _pos_df
    if not force and _pos_df is not None:
        return _pos_df
    path = await _download_if_missing(POS_URL, POS_CACHE)
    _pos_df = parse_pos_file(path)
    return _pos_df
