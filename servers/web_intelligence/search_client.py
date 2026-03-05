"""Google Custom Search API client.

Wraps the CSE JSON API v1 for web search, site-scoped search, and
news-style search. All 5 tools route through this module.

API docs: https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.googleapis.com/customsearch/v1"
_TIMEOUT = 20.0


def _get_credentials() -> tuple[str | None, str | None]:
    """Return (api_key, cse_id) from environment."""
    return (
        os.environ.get("GOOGLE_CSE_API_KEY"),
        os.environ.get("GOOGLE_CSE_ID"),
    )


async def search(
    query: str,
    num: int = 5,
    site_search: str = "",
    date_restrict: str = "",
    start: int = 1,
) -> dict:
    """Execute a Google Custom Search.

    Args:
        query: Search query string.
        num: Number of results (1-10, API maximum).
        site_search: Restrict results to this domain (e.g. "intermountainhealth.org").
        date_restrict: Recency filter (e.g. "d90" for last 90 days, "m6" for 6 months).
        start: Result offset for pagination (1-based).

    Returns:
        Raw API response dict, or error dict on failure.
    """
    api_key, cse_id = _get_credentials()
    if not api_key:
        return {
            "error": "GOOGLE_CSE_API_KEY not set",
            "instructions": (
                "Get a Google Custom Search JSON API key at "
                "https://developers.google.com/custom-search/v1/introduction "
                "(100 free queries/day, $5/1000 after)"
            ),
        }
    if not cse_id:
        return {
            "error": "GOOGLE_CSE_ID not set",
            "instructions": (
                "Create a Programmable Search Engine at "
                "https://programmablesearchengine.google.com/ "
                "and use the cx ID"
            ),
        }

    params: dict[str, str | int] = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": min(num, 10),
        "start": start,
    }
    if site_search:
        params["siteSearch"] = site_search
        params["siteSearchFilter"] = "i"  # include only results from this site
    if date_restrict:
        params["dateRestrict"] = date_restrict

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(_BASE_URL, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            logger.warning("Google CSE quota exceeded")
            return {"error": "Google CSE daily quota exceeded (100 free/day)"}
        logger.warning("Google CSE HTTP error: %s", e)
        return {"error": f"Google CSE request failed: {e.response.status_code}"}
    except Exception as e:
        logger.warning("Google CSE request failed: %s", e)
        return {"error": str(e)}


def extract_results(raw: dict) -> list[dict]:
    """Extract simplified search results from raw CSE response.

    Returns list of dicts with keys: title, link, snippet, displayLink.
    """
    if "error" in raw:
        return []
    items = raw.get("items", [])
    results = []
    for item in items:
        results.append({
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "snippet": item.get("snippet", ""),
            "display_link": item.get("displayLink", ""),
        })
    return results
