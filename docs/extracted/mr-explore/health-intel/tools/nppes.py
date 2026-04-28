"""NPPES NPI Registry API tools.

Fetches provider data from the National Plan and Provider Enumeration System:
- Organization lookups
- Provider counts by specialty
- Affiliated provider details
"""

import httpx
from typing import Any

from tools.validation import validate_npi

BASE_URL = "https://npiregistry.cms.hhs.gov/api/"

# Primary NPIs for our target systems
PRIMARY_NPIS = {
    "jefferson_health": "1215916002",
    "cooper_health": "1215165832",
    "temple_health": "1962579029",
}

# Search terms for org-level queries (use wildcard-friendly names)
ORG_SEARCH = {
    "jefferson_health": {"organization_name": "Jefferson*", "state": "PA"},
    "cooper_health": {"organization_name": "Cooper*", "state": "NJ"},
    "temple_health": {"organization_name": "Temple*", "state": "PA"},
}


async def lookup_npi(npi: str) -> dict[str, Any]:
    """Look up a specific NPI number."""
    npi = validate_npi(npi)
    params = {"version": "2.1", "number": npi}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    if not results:
        return {"npi": npi, "error": "NPI not found"}

    r = results[0]
    return {
        "npi": r.get("number"),
        "type": r.get("enumeration_type"),
        "basic": r.get("basic", {}),
        "addresses": r.get("addresses", []),
        "taxonomies": r.get("taxonomies", []),
    }


async def search_organizations(
    organization_name: str,
    state: str = "",
    city: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    """Search for organizations by name, state, and city."""
    params = {
        "version": "2.1",
        "enumeration_type": "NPI-2",
        "organization_name": organization_name,
        "limit": str(limit),
    }
    if state:
        params["state"] = state
    if city:
        params["city"] = city

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    return {
        "query": {"name": organization_name, "state": state, "city": city},
        "result_count": data.get("result_count", 0),
        "organizations": [
            {
                "npi": r.get("number"),
                "name": r.get("basic", {}).get("organization_name", ""),
                "authorized_official": r.get("basic", {}).get("authorized_official_first_name", "")
                + " "
                + r.get("basic", {}).get("authorized_official_last_name", ""),
                "taxonomies": [
                    {
                        "code": t.get("code"),
                        "desc": t.get("desc"),
                        "primary": t.get("primary"),
                    }
                    for t in r.get("taxonomies", [])
                ],
                "addresses": [
                    {
                        "purpose": a.get("address_purpose"),
                        "line1": a.get("address_1"),
                        "city": a.get("city"),
                        "state": a.get("state"),
                        "postal": a.get("postal_code"),
                    }
                    for a in r.get("addresses", [])
                ],
            }
            for r in results
        ],
    }


async def get_provider_taxonomy_summary(
    organization_name: str,
    state: str = "",
    city: str = "",
) -> dict[str, Any]:
    """Get a summary of provider types/specialties for an organization."""
    data = await search_organizations(organization_name, state, city, limit=1200)

    taxonomy_counts: dict[str, int] = {}
    for org in data.get("organizations", []):
        for tax in org.get("taxonomies", []):
            desc = tax.get("desc", "Unknown")
            taxonomy_counts[desc] = taxonomy_counts.get(desc, 0) + 1

    sorted_taxonomies = sorted(
        taxonomy_counts.items(), key=lambda x: x[1], reverse=True
    )

    return {
        "organization_name": organization_name,
        "total_npis": data.get("result_count", 0),
        "taxonomy_distribution": [
            {"specialty": k, "count": v} for k, v in sorted_taxonomies
        ],
    }


async def get_health_system_providers(system_key: str) -> dict[str, Any]:
    """Get comprehensive provider data for a health system."""
    if system_key not in ORG_SEARCH:
        return {"error": f"Unknown system key: {system_key}"}

    search = ORG_SEARCH[system_key]

    # Get org-level NPIs
    orgs = await search_organizations(**search)

    # Get taxonomy summary
    taxonomy = await get_provider_taxonomy_summary(**search)

    # Get primary NPI details
    primary_npi = PRIMARY_NPIS.get(system_key)
    primary = await lookup_npi(primary_npi) if primary_npi else {}

    return {
        "system": system_key,
        "primary_npi": primary,
        "affiliated_organizations": orgs,
        "taxonomy_summary": taxonomy,
    }
