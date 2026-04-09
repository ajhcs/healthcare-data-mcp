"""CMS URL discovery layer with hardcoded fallbacks.

Resolves current download URLs for CMS datasets by querying the CMS Provider
Data Catalog metastore API. Falls back to hardcoded URLs when discovery fails.
Caches resolved URLs for 7 days to avoid hammering the API.

Usage::

    from shared.utils.cms_url_resolver import resolve_cms_download_url

    url = await resolve_cms_download_url("xubh-q36u")

Discovery order:
1. 7-day local cache (~/.healthcare-data-mcp/cache/url_registry.json)
2. CMS Provider Data Catalog metastore API:
   https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{id}
3. CMS data.json catalog (bulk catalog search by title)
4. Hardcoded fallback URL from CMS_DATASETS registry
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache configuration
# ---------------------------------------------------------------------------

_REGISTRY_CACHE_TTL_DAYS = 7
_REGISTRY_CACHE_PATH = (
    Path.home() / ".healthcare-data-mcp" / "cache" / "url_registry.json"
)
_REGISTRY_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Dataset registry
# ---------------------------------------------------------------------------


class DatasetMeta(TypedDict):
    """Metadata for a single CMS dataset."""

    dataset_id: str
    description: str
    hardcoded_fallback_url: str
    filename_pattern: str  # Substring hint used to pick the right distribution


# Registry maps dataset IDs -> metadata.
# Fallback URLs use the most recent known CMS sites/default/files path and will
# be used only when all discovery paths fail.
CMS_DATASETS: dict[str, DatasetMeta] = {
    # --- Hospital General Information ---
    "xubh-q36u": {
        "dataset_id": "xubh-q36u",
        "description": "Hospital General Information",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/provider-data/api/1/datastore/query/"
            "xubh-q36u/0/download?format=csv"
        ),
        "filename_pattern": "Hospital_General_Information",
    },
    # --- Hospital Readmissions Reduction Program ---
    "9n3s-kdb3": {
        "dataset_id": "9n3s-kdb3",
        "description": "Hospital Readmissions Reduction Program",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/provider-data/api/1/datastore/query/"
            "9n3s-kdb3/0/download?format=csv"
        ),
        "filename_pattern": "FY_Hospital_Readmissions",
    },
    # --- HAC Reduction Program ---
    "yq43-i98g": {
        "dataset_id": "yq43-i98g",
        "description": "Hospital-Acquired Condition Reduction Program",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/provider-data/api/1/datastore/query/"
            "yq43-i98g/0/download?format=csv"
        ),
        "filename_pattern": "HAC_Reduction_Program",
    },
    # --- HCAHPS Patient Survey ---
    "dgck-syfz": {
        "dataset_id": "dgck-syfz",
        "description": "HCAHPS - Hospital",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/provider-data/api/1/datastore/query/"
            "dgck-syfz/0/download?format=csv"
        ),
        "filename_pattern": "HCAHPS-Hospital",
    },
    # --- Complications and Deaths ---
    "ynj2-r877": {
        "dataset_id": "ynj2-r877",
        "description": "Complications and Deaths - Hospital",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/provider-data/api/1/datastore/query/"
            "ynj2-r877/0/download?format=csv"
        ),
        "filename_pattern": "Complications_and_Deaths-Hospital",
    },
    # --- Hospital Provider Cost Report (PUF) ---
    "cost-report-puf": {
        "dataset_id": "cost-report-puf",
        "description": "Hospital Provider Cost Report",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2026-01/"
            "3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv"
        ),
        "filename_pattern": "CostReport_",
    },
    # --- Provider of Services (POS) File ---
    "pos-file": {
        "dataset_id": "pos-file",
        "description": (
            "Provider of Services File - Quality Improvement and Evaluation System"
        ),
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2026-01/"
            "c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv"
        ),
        "filename_pattern": "Hospital_and_other.DATA",
    },
    # --- Hospital Service Area File (HSAF) ---
    "hsaf": {
        "dataset_id": "hsaf",
        "description": "Hospital Service Area",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2025-07/"
            "8fca1932-adaa-411d-a912-78fb0854a286/Hospital_Service_Area_2024.csv"
        ),
        "filename_pattern": "Hospital_Service_Area_",
    },
    # --- Geographic Variation PUF ---
    "gv-puf": {
        "dataset_id": "gv-puf",
        "description": "Medicare Geographic Variation - by National, State & County",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2025-03/"
            "a40ac71d-9f80-4d99-92d2-fd149433d7d8/"
            "2014-2023%20Medicare%20Fee-for-Service%20Geographic%20Variation%20Public%20Use%20File.csv"
        ),
        "filename_pattern": "Geographic_Variation",
    },
    # --- Medicare Inpatient Hospitals - by Provider and Service ---
    "inpatient-puf-2023": {
        "dataset_id": "inpatient-puf-2023",
        "description": "Medicare Inpatient Hospitals - by Provider and Service (DY2023)",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2025-05/"
            "ca1c9013-8c7c-4560-a4a1-28cf7e43ccc8/MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV"
        ),
        "filename_pattern": "MUP_INP_",
    },
    "inpatient-puf-2022": {
        "dataset_id": "inpatient-puf-2022",
        "description": "Medicare Inpatient Hospitals - by Provider and Service (DY2022)",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2024-05/"
            "7d1f4bcd-7dd9-4fd1-aa7f-91cd69e452d3/MUP_INP_RY24_P03_V10_DY22_PrvSvc.CSV"
        ),
        "filename_pattern": "MUP_INP_",
    },
    "inpatient-puf-2021": {
        "dataset_id": "inpatient-puf-2021",
        "description": "Medicare Inpatient Hospitals - by Provider and Service (DY2021)",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2023-05/"
            "a754bf0b-0c51-4daf-876e-272f90a11c05/MUP_IHP_RY23_P03_V10_DY21_PRVSVC.CSV"
        ),
        "filename_pattern": "MUP_INP_",
    },
    # --- Medicare Outpatient Hospitals - by Provider and Service ---
    "outpatient-puf-2023": {
        "dataset_id": "outpatient-puf-2023",
        "description": "Medicare Outpatient Hospitals - by Provider and Service (DY2023)",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2025-08/"
            "bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv"
        ),
        "filename_pattern": "MUP_OUT_",
    },
    "outpatient-puf-2022": {
        "dataset_id": "outpatient-puf-2022",
        "description": "Medicare Outpatient Hospitals - by Provider and Service (DY2022)",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2024-06/"
            "860428c0-6102-4fff-812d-57c7860613e5/MUP_OUT_RY24_P04_V10_DY22_Prov_Svc.csv"
        ),
        "filename_pattern": "MUP_OUT_",
    },
    "outpatient-puf-2021": {
        "dataset_id": "outpatient-puf-2021",
        "description": "Medicare Outpatient Hospitals - by Provider and Service (DY2021)",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/sites/default/files/2024-06/"
            "657f3480-6e15-4978-8e4d-78bfa03b7a79/MUP_OUT_RY24_P04_V10_DY21_Prov_Svc.csv"
        ),
        "filename_pattern": "MUP_OUT_",
    },
    # --- Promoting Interoperability (PI) ---
    "pi-hospital": {
        "dataset_id": "pi-hospital",
        "description": "Promoting Interoperability - Hospital",
        "hardcoded_fallback_url": (
            "https://data.cms.gov/provider-data/sites/default/files/resources/"
            "5462b19a756c53c1becccf13787d9157_1770163678/"
            "Promoting_Interoperability-Hospital.csv"
        ),
        "filename_pattern": "Promoting_Interoperability-Hospital",
    },
}


# ---------------------------------------------------------------------------
# URL registry cache (7-day TTL)
# ---------------------------------------------------------------------------


def _load_registry() -> dict:
    """Load the persistent URL registry from disk. Returns {} on any error."""
    try:
        if _REGISTRY_CACHE_PATH.exists():
            return json.loads(_REGISTRY_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.debug("Could not read URL registry cache", exc_info=True)
    return {}


def _save_registry(registry: dict) -> None:
    """Persist the URL registry to disk."""
    try:
        _REGISTRY_CACHE_PATH.write_text(
            json.dumps(registry, indent=2), encoding="utf-8"
        )
    except Exception:
        logger.debug("Could not write URL registry cache", exc_info=True)


def _registry_entry_valid(entry: dict) -> bool:
    """Return True if a registry entry is within the 7-day TTL."""
    try:
        ts = entry.get("resolved_at", 0.0)
        age_days = (datetime.now(timezone.utc).timestamp() - float(ts)) / 86_400
        return age_days < _REGISTRY_CACHE_TTL_DAYS
    except Exception:
        return False


def _cache_key(dataset_id: str, filename_hint: str) -> str:
    return f"{dataset_id}::{filename_hint}" if filename_hint else dataset_id


def _get_cached_url(dataset_id: str, filename_hint: str) -> str | None:
    """Return a cached URL if present and not expired."""
    registry = _load_registry()
    key = _cache_key(dataset_id, filename_hint)
    entry = registry.get(key)
    if entry and _registry_entry_valid(entry):
        url = entry.get("url")
        if url:
            logger.debug("URL registry hit for %s: %s", key, url[:80])
            return url
    return None


def _store_cached_url(dataset_id: str, filename_hint: str, url: str) -> None:
    """Store a resolved URL in the registry cache."""
    registry = _load_registry()
    key = _cache_key(dataset_id, filename_hint)
    registry[key] = {
        "url": url,
        "resolved_at": datetime.now(timezone.utc).timestamp(),
        "dataset_id": dataset_id,
        "filename_hint": filename_hint,
    }
    _save_registry(registry)


# ---------------------------------------------------------------------------
# Discovery via CMS Provider Data Catalog metastore API
# ---------------------------------------------------------------------------

_CMS_METASTORE_URL = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
    "/{dataset_id}?show-reference-ids=false"
)

_CMS_DATASTORE_DOWNLOAD_URL = (
    "https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0/download?format=csv"
)


def _extract_download_from_metastore(item: dict, filename_hint: str) -> str | None:
    """Pull the best download URL out of a metastore item dict.

    Tries ``distribution`` entries for downloadURL fields, optionally filtered
    by *filename_hint*.
    """
    distributions = item.get("distribution") or []
    candidates: list[str] = []

    for dist in distributions:
        url = (
            dist.get("downloadURL")
            or dist.get("data", {}).get("downloadURL")
            or ""
        )
        if not url:
            continue
        if filename_hint:
            if filename_hint.lower() in url.lower():
                return url  # Best match — return immediately
            candidates.append(url)
        else:
            candidates.append(url)

    # Return the first CSV-looking URL
    for url in candidates:
        if url.lower().endswith(".csv") or "format=csv" in url.lower():
            return url

    return candidates[0] if candidates else None


async def _try_metastore_api(dataset_id: str, filename_hint: str) -> str | None:
    """Query the CMS metastore API for a dataset item and extract a download URL."""
    from shared.utils.http_client import resilient_request

    # Only attempt for real CMS dataset IDs (alphanumeric-hyphen, ~8 chars)
    if not _looks_like_cms_dataset_id(dataset_id):
        return None

    url = _CMS_METASTORE_URL.format(dataset_id=dataset_id)
    try:
        resp = await resilient_request("GET", url, timeout=30.0)
        item = resp.json()
    except Exception:
        logger.debug("Metastore API unavailable for %s", dataset_id, exc_info=True)
        return None

    # Try explicit distribution extraction
    found = _extract_download_from_metastore(item, filename_hint)
    if found:
        return found

    # Fall back to the direct datastore download endpoint (works for most DKAN datasets)
    logger.debug(
        "Metastore for %s has no matching distribution; trying datastore download endpoint",
        dataset_id,
    )
    return _CMS_DATASTORE_DOWNLOAD_URL.format(dataset_id=dataset_id)


def _looks_like_cms_dataset_id(dataset_id: str) -> bool:
    """Return True for IDs that look like CMS DKAN UUIDs (e.g. 'xubh-q36u')."""
    import re
    return bool(re.fullmatch(r"[a-z0-9]{4}-[a-z0-9]{4}", dataset_id))


# ---------------------------------------------------------------------------
# Discovery via CMS data.json catalog
# ---------------------------------------------------------------------------


async def _try_data_json_catalog(description: str, filename_hint: str) -> str | None:
    """Search the CMS data.json catalog for a CSV download URL by title."""
    if not description:
        return None
    try:
        from shared.utils.cms_client import cms_discover_download_url

        url = await cms_discover_download_url(
            title=description,
            distribution_title_contains=filename_hint if filename_hint else None,
        )
        return url
    except Exception:
        logger.debug("data.json catalog lookup failed", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def resolve_cms_download_url(
    dataset_id: str,
    filename_hint: str = "",
) -> str:
    """Resolve the current download URL for a CMS dataset.

    Tries, in order:
    1. 7-day local cache (url_registry.json)
    2. CMS Provider Data Catalog metastore API (if *dataset_id* looks like
       a real DKAN ID such as ``xubh-q36u``)
    3. CMS data.json catalog search by dataset description
    4. Hardcoded fallback URL from ``CMS_DATASETS``

    Args:
        dataset_id: The logical dataset identifier.  Can be a real CMS DKAN
            ID (``xubh-q36u``) or a symbolic key from ``CMS_DATASETS``
            (e.g. ``"cost-report-puf"``).
        filename_hint: Optional substring used to pick the right file when a
            dataset has multiple distributions (e.g. ``"MUP_INP_"``).

    Returns:
        The resolved download URL.  Raises ``RuntimeError`` if no URL can be
        determined at all (unlikely since every entry in ``CMS_DATASETS`` has a
        hardcoded fallback).
    """
    # 1. Registry cache
    cached = _get_cached_url(dataset_id, filename_hint)
    if cached:
        return cached

    meta = CMS_DATASETS.get(dataset_id)
    fallback_url: str | None = meta.get("hardcoded_fallback_url") if meta else None
    description: str = meta.get("description", "") if meta else ""
    effective_hint = filename_hint or (meta.get("filename_pattern", "") if meta else "")

    # Resolve the real dataset ID to use with the metastore API.
    # Symbolic keys like "cost-report-puf" won't match; use the underlying
    # DKAN ID if known.
    dkan_id = dataset_id if _looks_like_cms_dataset_id(dataset_id) else None

    # 2. CMS Provider Data Catalog metastore API
    if dkan_id:
        discovered = await _try_metastore_api(dkan_id, effective_hint)
        if discovered:
            logger.info(
                "Resolved %s via metastore API: %s", dataset_id, discovered[:80]
            )
            _store_cached_url(dataset_id, filename_hint, discovered)
            return discovered

    # 3. CMS data.json catalog
    catalog_url = await _try_data_json_catalog(description, effective_hint)
    if catalog_url:
        logger.info(
            "Resolved %s via data.json catalog: %s", dataset_id, catalog_url[:80]
        )
        _store_cached_url(dataset_id, filename_hint, catalog_url)
        return catalog_url

    # 4. Hardcoded fallback
    if fallback_url:
        logger.warning(
            "Could not discover current URL for %s; using hardcoded fallback: %s",
            dataset_id,
            fallback_url[:80],
        )
        # Don't cache the fallback — we want to retry discovery next time
        return fallback_url

    raise RuntimeError(
        f"Unable to resolve a download URL for CMS dataset '{dataset_id}'. "
        "Add an entry to CMS_DATASETS with a hardcoded_fallback_url."
    )


def list_known_datasets() -> list[dict]:
    """Return a summary list of all registered CMS datasets."""
    return [
        {
            "dataset_id": v["dataset_id"],
            "description": v["description"],
            "filename_pattern": v["filename_pattern"],
        }
        for v in CMS_DATASETS.values()
    ]
