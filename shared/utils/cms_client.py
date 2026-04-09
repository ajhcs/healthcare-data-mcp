"""Shared HTTP client utilities for CMS data.cms.gov APIs and bulk downloads."""

import hashlib
import json
import logging
from pathlib import Path
import re

from shared.utils.cache import is_cache_valid
from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

DATA_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CMS_API_BASE = "https://data.cms.gov"
CMS_CATALOG_URL = f"{CMS_API_BASE}/data.json"
NPPES_API_BASE = "https://npiregistry.cms.hhs.gov/api/"
_CATALOG_CACHE_TTL_DAYS = 1


def get_cache_path(key: str, suffix: str = ".json") -> Path:
    """Get a deterministic cache file path for a given key."""
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return DATA_DIR / f"{h}{suffix}"


async def get_cms_data_catalog(force_refresh: bool = False) -> dict:
    """Return the cached CMS ``data.json`` catalog.

    CMS explicitly recommends resolving download URLs from ``data.json`` rather
    than hard-coding ``sites/default/files/...`` paths, which change whenever a
    new release is published.
    """
    cache_path = get_cache_path("cms_data_catalog")
    if not force_refresh and is_cache_valid(cache_path, max_age_days=_CATALOG_CACHE_TTL_DAYS):
        return json.loads(cache_path.read_text(encoding="utf-8"))

    resp = await resilient_request("GET", CMS_CATALOG_URL, timeout=120.0)
    catalog = resp.json()
    cache_path.write_text(json.dumps(catalog), encoding="utf-8")
    return catalog


def _dataset_matches(
    dataset: dict,
    *,
    title: str | None = None,
    title_contains: str | None = None,
    landing_page_contains: str | None = None,
) -> bool:
    dataset_title = str(dataset.get("title", ""))
    landing_page = str(dataset.get("landingPage", ""))

    if title and dataset_title != title:
        return False
    if title_contains and title_contains.lower() not in dataset_title.lower():
        return False
    if landing_page_contains and landing_page_contains.lower() not in landing_page.lower():
        return False
    return bool(title or title_contains or landing_page_contains)


def find_cms_dataset(
    catalog: dict,
    *,
    title: str | None = None,
    title_contains: str | None = None,
    landing_page_contains: str | None = None,
) -> dict | None:
    """Find a CMS dataset entry from ``data.json``."""
    datasets = catalog.get("dataset", [])
    if title:
        exact = next((d for d in datasets if d.get("title") == title), None)
        if exact:
            return exact

    for dataset in datasets:
        if _dataset_matches(
            dataset,
            title=title,
            title_contains=title_contains,
            landing_page_contains=landing_page_contains,
        ):
            return dataset
    return None


def _distribution_release_year(distribution: dict) -> str | None:
    """Extract the release year from a distribution title."""
    title = str(distribution.get("title", ""))
    match = re.search(r":\s*(\d{4})-\d{2}-\d{2}", title)
    return match.group(1) if match else None


def select_cms_distribution(
    dataset: dict,
    *,
    url_field: str = "downloadURL",
    media_type: str | None = None,
    format_name: str | None = None,
    description: str | None = None,
    release_year: str | None = None,
    distribution_title_contains: str | None = None,
) -> dict | None:
    """Select a matching distribution from a CMS dataset entry."""
    distributions = dataset.get("distribution") or []

    for distribution in distributions:
        if not distribution.get(url_field):
            continue
        if media_type and distribution.get("mediaType") != media_type:
            continue
        if format_name and distribution.get("format") != format_name:
            continue
        if description and distribution.get("description") != description:
            continue
        if release_year and _distribution_release_year(distribution) != str(release_year):
            continue
        if distribution_title_contains:
            title = str(distribution.get("title", ""))
            if distribution_title_contains.lower() not in title.lower():
                continue
        return distribution

    return None


async def cms_discover_distribution_url(
    *,
    title: str | None = None,
    title_contains: str | None = None,
    landing_page_contains: str | None = None,
    url_field: str = "downloadURL",
    media_type: str | None = "text/csv",
    format_name: str | None = None,
    description: str | None = None,
    release_year: str | None = None,
    distribution_title_contains: str | None = None,
    fallback_url: str | None = None,
) -> str | None:
    """Resolve a rotating CMS distribution URL from ``data.json``.

    Returns ``fallback_url`` if the catalog lookup fails or there is no
    matching distribution.
    """
    try:
        catalog = await get_cms_data_catalog()
        dataset = find_cms_dataset(
            catalog,
            title=title,
            title_contains=title_contains,
            landing_page_contains=landing_page_contains,
        )
        if not dataset:
            logger.warning(
                "CMS catalog dataset not found for title=%r title_contains=%r landing_page_contains=%r; using fallback",
                title,
                title_contains,
                landing_page_contains,
            )
            return fallback_url

        distribution = select_cms_distribution(
            dataset,
            url_field=url_field,
            media_type=media_type,
            format_name=format_name,
            description=description,
            release_year=release_year,
            distribution_title_contains=distribution_title_contains,
        )
        if distribution:
            return distribution.get(url_field)

        logger.warning(
            "CMS catalog distribution not found for dataset %r (release_year=%r); using fallback",
            dataset.get("title"),
            release_year,
        )
    except Exception:
        logger.warning("CMS catalog lookup failed; using fallback", exc_info=True)

    return fallback_url


async def cms_discover_download_url(
    *,
    title: str | None = None,
    title_contains: str | None = None,
    landing_page_contains: str | None = None,
    release_year: str | None = None,
    distribution_title_contains: str | None = None,
    fallback_url: str | None = None,
) -> str | None:
    """Resolve a CSV download URL for a CMS dataset release."""
    return await cms_discover_distribution_url(
        title=title,
        title_contains=title_contains,
        landing_page_contains=landing_page_contains,
        url_field="downloadURL",
        media_type="text/csv",
        release_year=release_year,
        distribution_title_contains=distribution_title_contains,
        fallback_url=fallback_url,
    )


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


_HOSPITAL_INFO_DATASET_ID = "xubh-q36u"
_HOSPITAL_INFO_URL = (
    f"{CMS_API_BASE}/provider-data/api/1/datastore/query/{_HOSPITAL_INFO_DATASET_ID}/0/download?format=csv"
)
_HOSPITAL_INFO_CACHE_PATH = DATA_DIR / "hospital_general_info.csv"
_HOSPITAL_INFO_TTL_DAYS = 90  # CMS bulk data refresh cadence

# In-memory cache — keyed by column normalization mode
_hospital_info_raw: "pd.DataFrame | None" = None
_hospital_info_normalized: "pd.DataFrame | None" = None


async def load_hospital_general_info(normalize_columns: bool = False) -> "pd.DataFrame":
    """Download and cache the CMS Hospital General Information CSV (xubh-q36u).

    The file is stored at ``~/.healthcare-data-mcp/cache/hospital_general_info.csv``
    and refreshed after ``_HOSPITAL_INFO_TTL_DAYS`` days.

    Args:
        normalize_columns: When *True*, strip and lowercase column names,
            replacing spaces with underscores.  When *False*, original
            CMS column names are preserved (e.g. ``"Facility ID"``,
            ``"Facility Name"``).

    Returns:
        A :class:`pandas.DataFrame` with all columns as ``str``.
    """
    import pandas as pd

    global _hospital_info_raw, _hospital_info_normalized

    if normalize_columns and _hospital_info_normalized is not None:
        return _hospital_info_normalized
    if not normalize_columns and _hospital_info_raw is not None:
        return _hospital_info_raw

    if not is_cache_valid(_HOSPITAL_INFO_CACHE_PATH, max_age_days=_HOSPITAL_INFO_TTL_DAYS):
        logger.info("Downloading Hospital General Info from CMS (%s)…", _HOSPITAL_INFO_URL)
        resp = await resilient_request("GET", _HOSPITAL_INFO_URL, timeout=300.0)
        _HOSPITAL_INFO_CACHE_PATH.write_bytes(resp.content)
        logger.info(
            "Hospital General Info cached to %s (%d bytes)",
            _HOSPITAL_INFO_CACHE_PATH,
            _HOSPITAL_INFO_CACHE_PATH.stat().st_size,
        )
        # Invalidate in-memory copies after a fresh download
        _hospital_info_raw = None
        _hospital_info_normalized = None

    df = pd.read_csv(_HOSPITAL_INFO_CACHE_PATH, dtype=str, keep_default_na=False)
    if normalize_columns:
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        _hospital_info_normalized = df
    else:
        _hospital_info_raw = df

    return df


async def load_hospital_names() -> dict[str, str]:
    """Return a CCN → facility-name mapping from CMS Hospital General Info.

    Convenience wrapper around :func:`load_hospital_general_info` that returns
    a plain dict suitable for quick lookups.
    """
    df = await load_hospital_general_info(normalize_columns=False)
    # Find the columns regardless of minor whitespace variations in the CSV header.
    facility_id_col = next(
        (c for c in df.columns if c.strip() == "Facility ID"),
        None,
    )
    facility_name_col = next(
        (c for c in df.columns if c.strip() == "Facility Name"),
        None,
    )
    if not facility_id_col or not facility_name_col:
        raise ValueError(
            f"Expected 'Facility ID' and 'Facility Name' columns; found: {list(df.columns)}"
        )
    return dict(zip(df[facility_id_col].str.strip(), df[facility_name_col].str.strip()))


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
