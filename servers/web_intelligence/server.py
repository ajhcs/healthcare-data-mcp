"""Web Intelligence & OSINT MCP Server.

Provides tools for health system competitive intelligence via web search,
executive profiling, EHR detection, and news monitoring. Port 8014.
"""

import json
import logging
import os as _os
import re

import httpx

from shared.utils.http_client import resilient_request, get_client
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
        resp = await resilient_request(
            "GET", url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HealthcareDataMCP/1.0)"},
            timeout=15.0,
        )
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


# ---------------------------------------------------------------------------
# Tool 3: get_executive_profiles
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_executive_profiles(
    system_name: str,
    system_domain: str = "",
    include_linkedin: bool = True,
    max_results: int = 20,
) -> str:
    """Pull executive bios, titles, and tenure from official sites and LinkedIn.

    Searches for the health system's leadership page, parses executive entries,
    and optionally enriches with LinkedIn data via Google CSE + Proxycurl.

    Args:
        system_name: Health system name.
        system_domain: Website domain for site-scoped search. Discovered if omitted.
        include_linkedin: Enable LinkedIn enrichment (default true).
        max_results: Max executives to return (default 20).
    """
    try:
        cache_params = {
            "system_name": system_name, "system_domain": system_domain,
            "include_linkedin": include_linkedin, "max_results": max_results,
        }
        cached = data_loaders.load_cached_response("exec", cache_params, data_loaders._EXEC_TTL_DAYS)
        if cached is not None:
            return json.dumps(cached)

        # Step 1: Find the leadership page
        lead_query = f'"{system_name}" leadership "executive team" OR "senior leadership" OR "board of"'
        lead_raw = await search_client.search(
            lead_query, num=5,
            site_search=system_domain if system_domain else "",
        )
        if "error" in lead_raw:
            return json.dumps(lead_raw)

        lead_results = search_client.extract_results(lead_raw)

        if not system_domain and lead_results:
            system_domain = lead_results[0].get("display_link", "")

        # Step 2: Fetch and parse leadership page
        executives: list[ExecutiveProfile] = []
        source_urls: list[str] = []

        for result in lead_results[:2]:
            url = result.get("link", "")
            if not url:
                continue
            source_urls.append(url)

            html, soup = await _fetch_and_parse(url)
            if not soup:
                continue

            text = _extract_text_content(soup)
            if len(text) < 50:
                continue

            # Parse executive entries -- look for common patterns:
            # 1. Heading tags (h2/h3/h4) followed by title text
            # 2. Structured divs with name and title classes
            # 3. Bold/strong tags as names

            # Pattern 1: heading + adjacent text
            for heading in soup.find_all(["h2", "h3", "h4"]):
                name_text = heading.get_text(strip=True)
                # Skip obviously non-name headings
                if len(name_text) > 60 or len(name_text) < 4:
                    continue
                if any(skip in name_text.lower() for skip in [
                    "leadership", "executive", "team", "board", "about",
                    "contact", "news", "menu", "search",
                ]):
                    continue

                # Get the next sibling text as title/bio
                title_text = ""
                bio_text = ""
                sibling = heading.find_next_sibling()
                if sibling:
                    sib_text = sibling.get_text(strip=True)
                    if len(sib_text) < 200:
                        title_text = sib_text
                    else:
                        bio_text = sib_text[:300]

                if name_text:
                    executives.append(ExecutiveProfile(
                        name=name_text[:100],
                        title=title_text[:200],
                        bio_snippet=bio_text[:300],
                        source_url=url,
                    ))

            if executives:
                break  # Got results from first page, no need to try second

        # Fallback: if parsing yielded nothing, extract from CSE snippets
        if not executives:
            for r in lead_results:
                snippet = r.get("snippet", "")
                title = r.get("title", "")
                if snippet:
                    executives.append(ExecutiveProfile(
                        name=title[:100],
                        bio_snippet=snippet[:300],
                        source_url=r.get("link", ""),
                    ))

        # Limit results
        executives = executives[:max_results]

        # Step 3: LinkedIn enrichment
        if include_linkedin and executives:
            for exec_profile in executives[:10]:  # cap LinkedIn lookups
                if not exec_profile.name:
                    continue

                # Google CSE to find LinkedIn profile
                li_query = f'site:linkedin.com/in/ "{exec_profile.name}" "{system_name}"'
                li_raw = await search_client.search(li_query, num=2)
                if "error" not in li_raw:
                    li_results = search_client.extract_results(li_raw)
                    for li in li_results:
                        link = li.get("link", "")
                        if "linkedin.com/in/" in link:
                            exec_profile.linkedin_url = link

                            # Optional Proxycurl enrichment
                            if proxycurl_client.is_available():
                                profile_data = await proxycurl_client.lookup_profile(link)
                                if profile_data:
                                    exec_profile.linkedin_data = LinkedInData(**profile_data)
                            break

        response = ExecutiveProfilesResponse(
            system_name=system_name,
            total_results=len(executives),
            executives=executives,
            source_urls=source_urls,
        )
        result = response.model_dump()
        data_loaders.cache_response("exec", cache_params, result)
        return json.dumps(result)
    except Exception as e:
        logger.exception("get_executive_profiles failed")
        return json.dumps({"error": f"get_executive_profiles failed: {e}"})


# ---------------------------------------------------------------------------
# Google News RSS helper (fallback for monitor_newsroom)
# ---------------------------------------------------------------------------

async def _fetch_google_news_rss(query: str, days_back: int = 90) -> list[dict]:
    """Fetch Google News RSS as fallback. Returns list of {title, link, date, source, snippet}."""
    import xml.etree.ElementTree as ET
    from urllib.parse import quote

    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        resp = await resilient_request("GET", url, timeout=15.0)

        root = ET.fromstring(resp.text)
        items = []
        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")
            source_el = item.find("source")
            source = source_el.text if source_el is not None else ""
            description = item.findtext("description", "")

            items.append({
                "headline": title,
                "url": link,
                "date": pub_date,
                "source": source,
                "snippet": description[:300] if description else "",
            })

        return items[:50]
    except Exception as e:
        logger.debug("Google News RSS failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Tool 4: monitor_newsroom
# ---------------------------------------------------------------------------
@mcp.tool()
async def monitor_newsroom(
    system_name: str,
    days_back: int = 90,
    max_results: int = 25,
) -> str:
    """Retrieve recent press releases and news mentions for a health system.

    Primary: Google Custom Search with date restriction.
    Fallback: Google News RSS feed.

    Args:
        system_name: Health system name.
        days_back: How many days of news to retrieve (default 90, max 365).
        max_results: Max news items (default 25, max 100).
    """
    try:
        days_back = min(days_back, 365)
        max_results = min(max_results, 100)

        cache_params = {"system_name": system_name, "days_back": days_back}
        cached = data_loaders.load_cached_response("news", cache_params, data_loaders._NEWS_TTL_DAYS)
        if cached is not None:
            return json.dumps(cached)

        items: list[NewsItem] = []

        # Primary: Google CSE with date restriction
        news_query = f'"{system_name}"'
        date_restrict = f"d{days_back}"

        news_raw = await search_client.search(
            news_query, num=10, date_restrict=date_restrict,
        )

        if "error" not in news_raw:
            cse_results = search_client.extract_results(news_raw)
            for r in cse_results:
                items.append(NewsItem(
                    headline=r.get("title", ""),
                    source=r.get("display_link", ""),
                    snippet=r.get("snippet", ""),
                    url=r.get("link", ""),
                ))

        # Fallback: Google News RSS (if CSE returned few results or errored)
        if len(items) < 5:
            rss_items = await _fetch_google_news_rss(f'"{system_name}"', days_back)
            seen_headlines = {i.headline.lower() for i in items}
            for ri in rss_items:
                if ri["headline"].lower() not in seen_headlines:
                    items.append(NewsItem(
                        headline=ri["headline"],
                        source=ri["source"],
                        date=ri["date"],
                        snippet=ri["snippet"],
                        url=ri["url"],
                    ))
                    seen_headlines.add(ri["headline"].lower())

        items = items[:max_results]

        response = NewsroomResponse(
            system_name=system_name,
            days_back=days_back,
            total_results=len(items),
            items=items,
        )
        result = response.model_dump()
        data_loaders.cache_response("news", cache_params, result)
        return json.dumps(result)
    except Exception as e:
        logger.exception("monitor_newsroom failed")
        return json.dumps({"error": f"monitor_newsroom failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: detect_gpo_affiliation
# ---------------------------------------------------------------------------
@mcp.tool()
async def detect_gpo_affiliation(
    system_name: str,
) -> str:
    """Match a health system to known Group Purchasing Organization partners.

    Searches for the system name alongside GPO-related keywords and matches
    results against a curated GPO directory.

    Args:
        system_name: Health system name.
    """
    try:
        cache_params = {"system_name": system_name}
        cached = data_loaders.load_cached_response("gpo", cache_params, data_loaders._SEARCH_TTL_DAYS)
        if cached is not None:
            return json.dumps(cached)

        gpo_list = data_loaders.load_gpo_directory()
        if not gpo_list:
            return json.dumps({"error": "GPO directory not found"})

        # Build search query with top GPO names
        top_gpos = "OR".join(f' "{g["gpo_name"]}"' for g in gpo_list[:6])
        search_query = f'"{system_name}" GPO OR "group purchasing"{top_gpos}'

        raw = await search_client.search(search_query, num=10)
        if "error" in raw:
            return json.dumps(raw)

        results = search_client.extract_results(raw)

        # Match each result's snippet/title against GPO names
        matches: list[GpoMatch] = []
        seen_gpos: set[str] = set()

        for r in results:
            combined = r.get("title", "") + " " + r.get("snippet", "")
            matched = data_loaders.match_gpo_in_text(combined, gpo_list)

            for m in matched:
                gpo_name = m["gpo_name"]
                if gpo_name in seen_gpos:
                    continue
                seen_gpos.add(gpo_name)

                matches.append(GpoMatch(
                    gpo_name=gpo_name,
                    confidence="strong" if gpo_name.lower() in r.get("snippet", "").lower() else "moderate",
                    evidence_snippet=r.get("snippet", "")[:300],
                    evidence_url=r.get("link", ""),
                ))

        response = GpoAffiliationResponse(
            system_name=system_name,
            matches=matches,
            search_terms_used=search_query[:200],
        )
        result = response.model_dump()
        data_loaders.cache_response("gpo", cache_params, result)
        return json.dumps(result)
    except Exception as e:
        logger.exception("detect_gpo_affiliation failed")
        return json.dumps({"error": f"detect_gpo_affiliation failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
