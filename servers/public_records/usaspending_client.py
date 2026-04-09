"""USAspending.gov REST API client.

Searches federal awards by recipient name with optional filters.
API docs: https://api.usaspending.gov/
"""

import logging
from datetime import datetime


from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.usaspending.gov/api/v2"
_TIMEOUT = 30.0


async def search_awards(
    recipient_name: str,
    award_type: str = "",
    fiscal_year: str = "",
    limit: int = 25,
) -> dict:
    """Search federal awards by recipient name.

    Returns raw API response dict.
    """
    if fiscal_year:
        fy = int(fiscal_year)
    else:
        # Federal fiscal year runs Oct 1 -- Sep 30.
        # FY 2026 = Oct 1 2025 through Sep 30 2026.
        # If today is Oct-Dec 2025, we are in FY 2026.
        now = datetime.now()
        fy = now.year + 1 if now.month >= 10 else now.year

    # Map friendly award_type to USAspending codes
    type_map = {
        "contracts": ["A", "B", "C", "D"],
        "grants": ["02", "03", "04", "05"],
        "direct_payments": ["06", "10"],
        "loans": ["07", "08"],
    }
    award_types = type_map.get(award_type.lower(), []) if award_type else []

    filters: dict = {
        "recipient_search_text": [recipient_name],
        "time_period": [{"start_date": f"{fy - 1}-10-01", "end_date": f"{fy}-09-30"}],
    }
    if award_types:
        filters["award_type_codes"] = award_types

    payload = {
        "filters": filters,
        "fields": [
            "Award ID",
            "Recipient Name",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Award Type",
            "Award Amount",
            "Total Outlays",
            "Description",
            "Start Date",
            "End Date",
            "NAICS Code",
            "NAICS Description",
        ],
        "limit": min(limit, 100),
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
    }

    try:
        resp = await resilient_request("POST", f"{_BASE_URL}/search/spending_by_award/", json=payload, timeout=_TIMEOUT)
        return resp.json()
    except Exception as e:
        logger.warning("USAspending search failed: %s", e)
        return {"error": str(e)}
