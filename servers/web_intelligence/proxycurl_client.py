"""Proxycurl API client for LinkedIn profile enrichment.

Optional — gracefully returns empty results when PROXYCURL_API_KEY is not set.
API docs: https://nubela.co/proxycurl/docs
Pricing: ~$0.01 per profile lookup.
"""

import logging
import os

import httpx

from shared.utils.http_client import resilient_request, get_client

logger = logging.getLogger(__name__)

_BASE_URL = "https://nubela.co/proxycurl/api/v2/linkedin"
_TIMEOUT = 15.0


def _get_api_key() -> str | None:
    return os.environ.get("PROXYCURL_API_KEY")


def is_available() -> bool:
    """Check if Proxycurl API key is configured."""
    return bool(_get_api_key())


async def lookup_profile(linkedin_url: str) -> dict:
    """Fetch a LinkedIn profile by URL.

    Args:
        linkedin_url: Full LinkedIn profile URL (e.g. "https://www.linkedin.com/in/john-doe").

    Returns:
        Dict with headline, summary, education, experiences, or error dict.
    """
    api_key = _get_api_key()
    if not api_key:
        return {}

    try:
        resp = await client.get(
            _BASE_URL,
            params={"linkedin_profile_url": linkedin_url, "use_cache": "if-recent"},
            headers={"Authorization": f"Bearer {api_key}"},
        )
        data = resp.json()

        return {
            "headline": data.get("headline", ""),
            "summary": data.get("summary", ""),
            "education": _format_education(data.get("education", [])),
            "linkedin_url": linkedin_url,
        }
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            logger.warning("Proxycurl rate limit hit")
        else:
            logger.warning("Proxycurl HTTP error: %s", e.response.status_code)
        return {}
    except Exception as e:
        logger.warning("Proxycurl lookup failed: %s", e)
        return {}


def _format_education(edu_list: list) -> str:
    """Format education entries into a readable string."""
    parts = []
    for edu in edu_list[:3]:  # limit to 3 entries
        school = edu.get("school", "")
        degree = edu.get("degree_name", "")
        if school:
            parts.append(f"{degree}, {school}" if degree else school)
    return "; ".join(parts)
