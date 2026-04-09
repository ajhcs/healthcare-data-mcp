"""Data loading and caching for CMS hospital quality datasets.

Uses the shared cms_client for HTTP downloads and caching.
"""

import asyncio
import logging
import sys
from pathlib import Path

import pandas as pd

# Add project root to path so shared utils are importable
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from shared.utils.cms_client import cms_download_csv  # noqa: E402
from shared.utils.cms_url_resolver import resolve_cms_download_url  # noqa: E402

logger = logging.getLogger(__name__)

# Dataset IDs on data.cms.gov Provider Data Catalog
DATASETS = {
    "hospital_info": "xubh-q36u",
    "hrrp": "9n3s-kdb3",
    "hac": "yq43-i98g",
    "hcahps": "dgck-syfz",
    "complications": "ynj2-r877",
}
_COST_REPORT_DATASET_TITLE = "Hospital Provider Cost Report"

# In-memory DataFrame cache to avoid re-reading CSV on every call
_BULK_TTL_DAYS = 90  # CMS bulk data refresh cadence

_df_cache: dict[str, pd.DataFrame] = {}
_df_lock = asyncio.Lock()


async def _load_dataset(key: str) -> pd.DataFrame:
    """Load a CMS dataset by key, downloading and caching as needed."""
    async with _df_lock:
        if key in _df_cache:
            return _df_cache[key]

    dataset_id = DATASETS[key]
    cache_key = f"hospital_quality_{key}"
    url = await resolve_cms_download_url(dataset_id)
    if not url:
        raise RuntimeError(f"Unable to resolve download URL for dataset {dataset_id!r}")

    try:
        path = await cms_download_csv(url, cache_key=cache_key)
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        _df_cache[key] = df
        return df
    except Exception:
        logger.warning("Could not load %s dataset — returning empty DataFrame", key, exc_info=True)
        _df_cache[key] = pd.DataFrame()
        return _df_cache[key]


async def load_hospital_info() -> pd.DataFrame:
    """Load the Hospital General Information dataset (xubh-q36u)."""
    return await _load_dataset("hospital_info")


async def load_hrrp() -> pd.DataFrame:
    """Load the Hospital Readmissions Reduction Program dataset (9n3s-kdb3)."""
    return await _load_dataset("hrrp")


async def load_hac() -> pd.DataFrame:
    """Load the HAC Reduction Program dataset (yq43-i98g)."""
    return await _load_dataset("hac")


async def load_hcahps() -> pd.DataFrame:
    """Load the HCAHPS Patient Experience dataset (dgck-syfz)."""
    return await _load_dataset("hcahps")


async def load_complications() -> pd.DataFrame:
    """Load the Complications and Deaths dataset (ynj2-r877)."""
    return await _load_dataset("complications")


async def load_cost_report() -> pd.DataFrame:
    """Load CMS Hospital Cost Report data for financial profiling.

    Uses the CMS Cost Report PUF direct CSV download.
    Falls back to empty DataFrame if unavailable.
    """
    key = "cost_report"
    if key in _df_cache:
        return _df_cache[key]

    try:
        url = await resolve_cms_download_url("cost-report-puf", "CostReport_")
        if not url:
            raise RuntimeError("Unable to resolve Hospital Provider Cost Report download URL")
        path = await cms_download_csv(url, cache_key="hospital_quality_cost_report")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        _df_cache[key] = df
        return df
    except Exception:
        logger.warning("Could not load cost report data — returning empty DataFrame", exc_info=True)
        _df_cache[key] = pd.DataFrame()
        return _df_cache[key]
