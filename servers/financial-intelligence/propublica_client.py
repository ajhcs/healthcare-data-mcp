"""ProPublica Nonprofit Explorer API client."""

import logging

import httpx

logger = logging.getLogger(__name__)

PROPUBLICA_BASE = "https://projects.propublica.org/nonprofits/api/v2"


async def search_organizations(query: str, state: str = "", ntee_code: str = "", page: int = 0) -> dict:
    """Search nonprofits via ProPublica Nonprofit Explorer.

    Returns raw JSON response with 'organizations' list and 'total_results' count.
    """
    params: dict[str, str | int] = {"q": query, "page": page}
    if state:
        params["state[id]"] = state.upper()
    if ntee_code:
        params["ntee[id]"] = ntee_code

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{PROPUBLICA_BASE}/search.json", params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("ProPublica search failed: %s", e)
        return {"organizations": [], "total_results": 0}


async def get_organization(ein: str) -> dict:
    """Get organization details and filing list from ProPublica.

    Returns raw JSON with 'organization' dict and 'filings_with_data' list.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{PROPUBLICA_BASE}/organizations/{ein}.json")
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("ProPublica org lookup failed for EIN %s: %s", ein, e)
        return {}
