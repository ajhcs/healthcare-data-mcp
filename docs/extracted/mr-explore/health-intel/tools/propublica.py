"""ProPublica Nonprofit Explorer API tools.

Fetches IRS Form 990 data for nonprofit hospitals:
- Revenue, expenses, assets
- Executive compensation
- Program service details
- Charity care information
"""

import httpx
from typing import Any

from tools.validation import validate_ein

BASE_URL = "https://projects.propublica.org/nonprofits/api/v2"

# EINs for our target systems (no dashes in API calls)
EINS = {
    "jefferson_health": "232829095",
    "cooper_health": "210634462",
    "temple_health": "232825878",
}

# Related entities for deeper analysis
RELATED_EINS = {
    "jefferson_health": [
        ("232829095", "Thomas Jefferson University Hospital"),
        ("230596940", "Jefferson Health Northeast"),
        ("232809585", "Jefferson University Physicians"),
    ],
    "cooper_health": [
        ("210634462", "Cooper Health System"),
        ("210662542", "Cooper University Hospital Cape Regional"),
    ],
    "temple_health": [
        ("232825878", "Temple University Hospital Inc"),
        ("232825881", "Temple University Health System Inc"),
    ],
}


async def get_organization_990(ein: str) -> dict[str, Any]:
    """Get all 990 filings for an organization by EIN."""
    ein = validate_ein(ein)
    url = f"{BASE_URL}/organizations/{ein}.json"
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    org = data.get("organization", {})
    filings = data.get("filings_with_data", [])
    filings_no_data = data.get("filings_without_data", [])

    return {
        "ein": ein,
        "name": org.get("name", ""),
        "city": org.get("city", ""),
        "state": org.get("state", ""),
        "ntee_code": org.get("ntee_code", ""),
        "subsection_code": org.get("subsection_code", ""),
        "total_revenue": org.get("income_amount", 0),
        "total_assets": org.get("asset_amount", 0),
        "filing_count": len(filings),
        "filings_with_data": filings,
        "filings_without_data": filings_no_data,
    }


async def search_organizations(query: str, state: str = "") -> dict[str, Any]:
    """Search for nonprofit organizations by name."""
    url = f"{BASE_URL}/search.json"
    params = {"q": query}
    if state:
        params["state[id]"] = state

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    orgs = data.get("organizations", [])
    return {
        "query": query,
        "state": state,
        "total_results": data.get("total_results", 0),
        "organizations": [
            {
                "ein": o.get("ein"),
                "name": o.get("name"),
                "city": o.get("city"),
                "state": o.get("state"),
                "ntee_code": o.get("ntee_code"),
                "total_revenue": o.get("income_amount"),
                "total_assets": o.get("asset_amount"),
            }
            for o in orgs
        ],
    }


def extract_financials(filing: dict) -> dict[str, Any]:
    """Extract key financial metrics from a single 990 filing."""
    return {
        "tax_period": filing.get("tax_prd"),
        "tax_year": filing.get("tax_prd_yr"),
        "total_revenue": filing.get("totrevenue"),
        "total_expenses": filing.get("totfuncexpns"),
        "net_income": filing.get("totrevenue", 0) - filing.get("totfuncexpns", 0)
        if filing.get("totrevenue") and filing.get("totfuncexpns")
        else None,
        "total_assets": filing.get("totassetsend"),
        "total_liabilities": filing.get("totliabend"),
        "net_assets": filing.get("totnetassetend"),
        "contributions_grants": filing.get("totcntrbgfts"),
        "program_service_revenue": filing.get("totprgmrevnue"),
        "investment_income": filing.get("invstmntinc"),
        "other_revenue": filing.get("othrevnue"),
        "compensation_current_officers": filing.get("compnsatncurrofcr"),
        "other_salaries": filing.get("othrsalwam"),
        "total_employee_count": filing.get("totemployee"),
        "total_volunteer_count": filing.get("totvolunteer"),
    }


async def get_health_system_financials(system_key: str) -> dict[str, Any]:
    """Get comprehensive financial data for a health system and related entities."""
    import asyncio

    if system_key not in RELATED_EINS:
        return {"error": f"Unknown system key: {system_key}"}

    entities = RELATED_EINS[system_key]
    tasks = [get_organization_990(ein) for ein, _ in entities]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    system_data = []
    for (ein, name), result in zip(entities, results):
        if isinstance(result, Exception):
            system_data.append({"ein": ein, "name": name, "error": str(result)})
        else:
            # Extract financials from most recent filings
            filings = result.get("filings_with_data", [])
            financials = [extract_financials(f) for f in filings[:5]]  # Last 5 years
            system_data.append({
                "ein": ein,
                "name": result.get("name", name),
                "city": result.get("city"),
                "state": result.get("state"),
                "recent_financials": financials,
                "filing_count": result.get("filing_count", 0),
            })

    return {
        "system": system_key,
        "entities": system_data,
    }
