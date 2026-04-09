"""Data loading and caching for AHRQ Compendium, CMS POS, and NPPES.

AHRQ/POS loaders are now shared across servers via shared.utils.ahrq_data.
This module re-exports them for backward compatibility and adds NPPES search.
"""

import logging
from pathlib import Path

import httpx
import pandas as pd

# Re-export shared AHRQ/POS loaders for backward compatibility.
# Other servers should import directly from shared.utils.ahrq_data.
from shared.utils.ahrq_data import (  # noqa: F401
    AHRQ_HOSPITAL_LINKAGE_CACHE,
    AHRQ_HOSPITAL_LINKAGE_URL,
    AHRQ_SYSTEM_CACHE,
    AHRQ_SYSTEM_URL,
    CACHE_DIR,
    POS_CACHE,
    POS_URL,
    load_ahrq_hospital_linkage,
    load_ahrq_systems,
    load_pos,
    parse_ahrq_hospital_linkage,
    parse_ahrq_system_file,
    parse_pos_file,
)

logger = logging.getLogger(__name__)


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
