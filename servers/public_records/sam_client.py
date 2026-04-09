"""SAM.gov Opportunities API client.

Searches federal contract opportunities/solicitations.
Requires SAM_GOV_API_KEY environment variable.
API docs: https://open.gsa.gov/api/get-opportunities-public-api/
"""

import logging
import os
from datetime import datetime, timedelta

import httpx

from shared.utils.http_client import resilient_request, get_client

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.sam.gov/prod/opportunities/v2/search"
_TIMEOUT = 30.0


def _get_api_key() -> str | None:
    return os.environ.get("SAM_GOV_API_KEY")


async def search_opportunities(
    keyword: str,
    posted_from: str = "",
    posted_to: str = "",
    ptype: str = "",
    limit: int = 25,
) -> dict:
    """Search SAM.gov opportunities by keyword.

    Returns raw API response dict or error dict.
    """
    api_key = _get_api_key()
    if not api_key:
        return {
            "error": "SAM_GOV_API_KEY not set",
            "instructions": "Register for a free API key at https://sam.gov/profile/details (Public API Key section)",
        }

    if not posted_from:
        posted_from = (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y")
    if not posted_to:
        posted_to = datetime.now().strftime("%m/%d/%Y")

    params: dict = {
        "api_key": api_key,
        "keyword": keyword,
        "postedFrom": posted_from,
        "postedTo": posted_to,
        "limit": min(limit, 100),
    }
    if ptype:
        params["ptype"] = ptype

    try:
        resp = await resilient_request("GET", _BASE_URL, params=params, timeout=_TIMEOUT)
        return resp.json()
    except Exception as e:
        logger.warning("SAM.gov search failed: %s", e)
        return {"error": str(e)}
