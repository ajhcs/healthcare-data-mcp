"""Shared HTTP client utilities for CMS data.cms.gov API and bulk downloads."""

import hashlib
import json
import logging
from pathlib import Path

from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

DATA_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CMS_API_BASE = "https://data.cms.gov"
NPPES_API_BASE = "https://npiregistry.cms.hhs.gov/api/"


def get_cache_path(key: str, suffix: str = ".json") -> Path:
    """Get a deterministic cache file path for a given key."""
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return DATA_DIR / f"{h}{suffix}"


async def cms_api_query(dataset_id: str, params: dict | None = None, size: int = 1000, offset: int = 0) -> list[dict]:
    """Query a data.cms.gov Provider Data Catalog dataset.

    Uses the DKAN API pattern: /provider-data/api/1/datastore/query/{id}/0
    """
    url = f"{CMS_API_BASE}/provider-data/api/1/datastore/query/{dataset_id}/0"
    query_params = {"size": size, "offset": offset}
    if params:
        query_params.update(params)

    resp = await resilient_request("GET", url, params=query_params, timeout=60.0)
    data = resp.json()
    return data.get("results", [])


async def cms_download_csv(url: str, cache_key: str | None = None) -> Path:
    """Download a CSV file from CMS, caching locally.

    Returns the local file path.
    """
    if cache_key:
        cached = get_cache_path(cache_key, suffix=".csv")
        if cached.exists():
            logger.info("Using cached file: %s", cached)
            return cached
    else:
        cached = get_cache_path(url, suffix=".csv")

    logger.info("Downloading: %s", url)
    resp = await resilient_request("GET", url, timeout=300.0)
    cached.write_bytes(resp.content)

    logger.info("Saved to: %s", cached)
    return cached


async def nppes_lookup(number: str | None = None, **kwargs) -> list[dict]:
    """Query the NPPES NPI Registry REST API.

    Args:
        number: Exact NPI number lookup.
        **kwargs: Additional query params (organization_name, state, taxonomy_description, etc.)

    Returns:
        List of provider result dicts.
    """
    params = {"version": "2.1", "limit": 200}
    if number:
        params["number"] = number
    params.update({k: v for k, v in kwargs.items() if v is not None})

    resp = await resilient_request("GET", NPPES_API_BASE, params=params, timeout=30.0)
    data = resp.json()
    return data.get("results", [])
