"""Web Intelligence & OSINT MCP Server.

Provides tools for health system competitive intelligence via web search,
executive profiling, EHR detection, and news monitoring. Port 8014.
"""

import json
import logging
import os as _os
import re

import httpx
from bs4 import BeautifulSoup
from mcp.server.fastmcp import FastMCP

from . import data_loaders, search_client, proxycurl_client  # pyright: ignore[reportAttributeAccessIssue]
from .models import (  # pyright: ignore[reportAttributeAccessIssue]
    SystemProfileResponse,
    LocationEntry,
    EhrDetectionResponse,
    ExecutiveProfilesResponse,
    ExecutiveProfile,
    LinkedInData,
    NewsroomResponse,
    NewsItem,
    GpoAffiliationResponse,
    GpoMatch,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "web-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8014"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Shared HTML fetch + parse helper
# ---------------------------------------------------------------------------

async def _fetch_and_parse(url: str) -> tuple[str, BeautifulSoup | None]:
    """Fetch a URL and return (raw_html, parsed_soup).

    Returns ("", None) on failure. Timeout 15s.
    """
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(url, headers={
                "User-Agent": "Mozilla/5.0 (compatible; HealthcareDataMCP/1.0)",
            })
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "lxml")
            return html, soup
    except Exception as e:
        logger.debug("Fetch failed for %s: %s", url, e)
        return "", None


def _extract_meta(html: str) -> dict[str, str]:
    """Extract og:description and meta description from raw HTML.

    These are almost always server-rendered even on SPA sites.
    """
    result: dict[str, str] = {}
    if not html:
        return result

    # Use simple regex -- faster than full parse for just meta tags
    og_match = re.search(
        r'<meta\s+[^>]*property=["\']og:description["\']\s+content=["\']([^"\']+)',
        html, re.IGNORECASE,
    )
    if og_match:
        result["og_description"] = og_match.group(1).strip()

    meta_match = re.search(
        r'<meta\s+[^>]*name=["\']description["\']\s+content=["\']([^"\']+)',
        html, re.IGNORECASE,
    )
    if meta_match:
        result["meta_description"] = meta_match.group(1).strip()

    return result


def _extract_text_content(soup: BeautifulSoup) -> str:
    """Extract visible text from a BeautifulSoup parse, removing scripts/styles."""
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:5000]  # cap at 5k chars


# ---------------------------------------------------------------------------
# Tool 1: scrape_system_profile
# ---------------------------------------------------------------------------
@mcp.tool()
async def scrape_system_profile(
    system_name: str,
    system_domain: str = "",
) -> str:
    """Extract mission, vision, leadership summary, and locations from a health system website.

    Uses Google Custom Search to find relevant pages, then targeted HTML fetch
    and parse. Falls back to search snippets + meta tags if page content is thin.

    Args:
        system_name: Health system name (e.g. "Intermountain Health").
        system_domain: Website domain (e.g. "intermountainhealth.org"). Discovered via search if omitted.
    """
    try:
        # Check cache
        cache_params = {"system_name": system_name, "system_domain": system_domain}
        cached = data_loaders.load_cached_response("profile", cache_params, data_loaders._PAGE_TTL_DAYS)
        if cached is not None:
            return json.dumps(cached)

        # Step 1: Search for About/Mission pages
        about_query = f'"{system_name}" about us mission vision'
        about_raw = await search_client.search(
            about_query,
            num=5,
            site_search=system_domain if system_domain else "",
        )
        if "error" in about_raw:
            return json.dumps(about_raw)

        about_results = search_client.extract_results(about_raw)

        # If no domain was provided, detect it from results
        if not system_domain and about_results:
            system_domain = about_results[0].get("display_link", "")

        # Step 2: Fetch and parse the top About page
        mission = ""
        vision = ""
        values = ""
        tagline = ""
        data_quality = "snippets_only"
        source_urls: list[str] = []

        for result in about_results[:2]:
            url = result.get("link", "")
            if not url:
                continue
            source_urls.append(url)

            html, soup = await _fetch_and_parse(url)
            if soup:
                text = _extract_text_content(soup)
                if len(text) > 100:
                    data_quality = "full_parse"
                    # Look for mission/vision keywords in paragraphs
                    paragraphs = [p.get_text(strip=True) for p in soup.find_all("p") if len(p.get_text(strip=True)) > 30]
                    for p in paragraphs:
                        lower = p.lower()
                        if "mission" in lower and not mission:
                            mission = p[:500]
                        elif "vision" in lower and not vision:
                            vision = p[:500]
                        elif "values" in lower and not values:
                            values = p[:500]

                    # If still empty, use the longest paragraph as mission
                    if not mission and paragraphs:
                        mission = max(paragraphs, key=len)[:500]
                    break
                else:
                    # Thin content -- try meta tags
                    meta = _extract_meta(html)
                    if meta:
                        data_quality = "meta_tags_only"
                        mission = meta.get("og_description", meta.get("meta_description", ""))
                        break

        # Fallback: use CSE snippets
        if not mission and about_results:
            mission = " ".join(r.get("snippet", "") for r in about_results[:3])[:500]

        # Step 3: Search for locations (separate query)
        locations: list[LocationEntry] = []
        if system_domain:
            loc_raw = await search_client.search(
                f"locations facilities",
                num=5,
                site_search=system_domain,
            )
            if "error" not in loc_raw:
                loc_results = search_client.extract_results(loc_raw)
                for r in loc_results:
                    # Extract location hints from snippet
                    snippet = r.get("snippet", "")
                    title = r.get("title", "")
                    if snippet:
                        locations.append(LocationEntry(
                            name=title[:100],
                            address=snippet[:200],
                        ))

        response = SystemProfileResponse(
            system_name=system_name,
            domain=system_domain,
            mission=mission,
            vision=vision,
            values=values,
            tagline=tagline,
            location_count=len(locations),
            locations=locations,
            source_urls=source_urls,
            data_quality=data_quality,
        )
        result = response.model_dump()
        data_loaders.cache_response("profile", cache_params, result)
        return json.dumps(result)
    except Exception as e:
        logger.exception("scrape_system_profile failed")
        return json.dumps({"error": f"scrape_system_profile failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: detect_ehr_vendor
# ---------------------------------------------------------------------------
@mcp.tool()
async def detect_ehr_vendor(
    system_name: str,
    ccn: str = "",
    state: str = "",
) -> str:
    """Identify the EHR vendor for a health system or facility.

    Uses a waterfall strategy:
    1. CMS Promoting Interoperability data (authoritative)
    2. Career page keyword search (inferred)
    3. News mention search (weak signal)

    Returns confidence level: PI_DATA > CAREER_PAGE > NEWS_MENTION.

    Args:
        system_name: Health system or facility name.
        ccn: CMS Certification Number for precise PI lookup.
        state: State filter for PI data disambiguation.
    """
    try:
        # Check cache
        cache_params = {"system_name": system_name, "ccn": ccn, "state": state}
        cached = data_loaders.load_cached_response("ehr", cache_params, data_loaders._SEARCH_TTL_DAYS)
        if cached is not None:
            return json.dumps(cached)

        # Strategy 1: CMS Promoting Interoperability (authoritative)
        await data_loaders.ensure_pi_cached()
        pi_rows = data_loaders.query_pi_for_ehr(
            facility_name=system_name, ccn=ccn, state=state,
        )

        if pi_rows:
            # Find the best row (one with ehr_developer populated)
            best = pi_rows[0]
            for row in pi_rows:
                if row.get("ehr_developer"):
                    best = row
                    break

            raw_dev = best.get("ehr_developer", "")
            vendor = data_loaders.resolve_vendor_name(raw_dev) if raw_dev else ""
            product = best.get("ehr_product_name", "")
            cehrt = best.get("cehrt_id", "")

            if vendor or product:
                response = EhrDetectionResponse(
                    system_name=system_name,
                    vendor_name=vendor,
                    product_name=product,
                    confidence="PI_DATA",
                    evidence_summary=f"CMS Promoting Interoperability attestation (CCN: {best.get('ccn', '')})",
                    source_url="https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
                    cehrt_id=cehrt,
                )
                result = response.model_dump()
                data_loaders.cache_response("ehr", cache_params, result)
                return json.dumps(result)

        # Strategy 2: Career page keyword search (inferred)
        vendor_terms = " OR ".join(f'"{v}"' for v in [
            "Epic", "Cerner", "Oracle Health", "MEDITECH",
            "Altera", "athenahealth", "eClinicalWorks",
        ])
        career_query = f'"{system_name}" careers jobs ({vendor_terms})'
        career_raw = await search_client.search(career_query, num=5)

        if "error" not in career_raw:
            career_results = search_client.extract_results(career_raw)
            for r in career_results:
                snippet = (r.get("snippet", "") + " " + r.get("title", "")).lower()
                for keyword, canonical in data_loaders.VENDOR_KEYWORDS.items():
                    if keyword in snippet:
                        response = EhrDetectionResponse(
                            system_name=system_name,
                            vendor_name=canonical,
                            confidence="CAREER_PAGE",
                            evidence_summary=f"Found '{keyword}' in: {r.get('snippet', '')[:200]}",
                            source_url=r.get("link", ""),
                        )
                        result = response.model_dump()
                        data_loaders.cache_response("ehr", cache_params, result)
                        return json.dumps(result)

        # Strategy 3: News mention (weak signal)
        news_query = f'"{system_name}" EHR "electronic health record"'
        news_raw = await search_client.search(news_query, num=5, date_restrict="m12")

        if "error" not in news_raw:
            news_results = search_client.extract_results(news_raw)
            for r in news_results:
                snippet = (r.get("snippet", "") + " " + r.get("title", "")).lower()
                for keyword, canonical in data_loaders.VENDOR_KEYWORDS.items():
                    if keyword in snippet:
                        response = EhrDetectionResponse(
                            system_name=system_name,
                            vendor_name=canonical,
                            confidence="NEWS_MENTION",
                            evidence_summary=f"Found '{keyword}' in news: {r.get('snippet', '')[:200]}",
                            source_url=r.get("link", ""),
                        )
                        result = response.model_dump()
                        data_loaders.cache_response("ehr", cache_params, result)
                        return json.dumps(result)

        # No match found
        response = EhrDetectionResponse(
            system_name=system_name,
            confidence="NOT_FOUND",
            evidence_summary="No EHR vendor identified from PI data, career pages, or news.",
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("detect_ehr_vendor failed")
        return json.dumps({"error": f"detect_ehr_vendor failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
