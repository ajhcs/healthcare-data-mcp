"""Data loading and caching for CMS facility datasets."""

import asyncio
import logging
from pathlib import Path

import pandas as pd
from shared.utils.cms_client import cms_discover_download_url, load_hospital_general_info
from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"

_BULK_TTL_DAYS = 90  # CMS bulk data refresh cadence
_COST_REPORT_DATASET_TITLE = "Hospital Provider Cost Report"

# In-memory DataFrames to avoid re-reading CSV on every call
_cost_report_df: pd.DataFrame | None = None
_df_lock = asyncio.Lock()


async def _download_csv(url: str, cache_name: str) -> Path:
    """Download a CSV from CMS and cache it locally."""
    from shared.utils.cache import is_cache_valid

    cached = CACHE_DIR / cache_name
    if is_cache_valid(cached, max_age_days=_BULK_TTL_DAYS):
        logger.info("Using cached file: %s", cached)
        return cached

    logger.info("Downloading %s ...", url)
    resp = await resilient_request("GET", url, timeout=300.0)
    cached.write_bytes(resp.content)

    logger.info("Saved to: %s", cached)
    return cached


async def load_hospital_info() -> pd.DataFrame:
    """Load the Hospital General Information dataset, downloading if needed.

    Delegates to the shared loader so the file is downloaded and cached once
    across all servers.  Columns are normalized to lowercase with underscores
    to preserve the existing API of this function.
    """
    return await load_hospital_general_info(normalize_columns=True)


async def load_cost_report() -> pd.DataFrame:
    """Load the Hospital Cost Report PUF dataset, downloading if needed.

    The Cost Report PUF is available from CMS. We use the most recent
    consolidated file. If the download fails (large file, URL changes),
    we return an empty DataFrame so the server stays functional.
    """
    global _cost_report_df
    async with _df_lock:
        if _cost_report_df is not None:
            return _cost_report_df

        # CMS Cost Report PUF — direct CSV download (2023 Final, published Jan 2026)
        fallback_url = (
            "https://data.cms.gov/sites/default/files/2026-01/"
            "3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv"
        )
        try:
            cost_report_url = await cms_discover_download_url(
                title=_COST_REPORT_DATASET_TITLE,
                fallback_url=fallback_url,
            )
            if not cost_report_url:
                raise RuntimeError("Unable to resolve Hospital Provider Cost Report download URL")
            path = await _download_csv(cost_report_url, "hospital_cost_report.csv")
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            _cost_report_df = df
            return df
        except Exception:
            logger.warning("Could not load cost report data — returning empty DataFrame", exc_info=True)
            _cost_report_df = pd.DataFrame()
            return _cost_report_df


async def search_nppes(
    npi: str | None = None,
    organization_name: str | None = None,
    state: str | None = None,
    taxonomy_description: str | None = None,
    enumeration_type: str = "NPI-2",
    limit: int = 50,
) -> list[dict]:
    """Query the NPPES NPI Registry REST API."""
    params: dict = {"version": "2.1", "limit": min(limit, 200)}
    if npi:
        params["number"] = npi
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
