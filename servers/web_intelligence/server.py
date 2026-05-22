"""Web Intelligence & OSINT MCP Server.

Provides tools for health system competitive intelligence via web search,
executive profiling, EHR detection, and news monitoring. Port 8014.
"""

from typing import Any, Mapping
from datetime import datetime, timezone
import ipaddress
import logging
import os as _os
import re
import socket
from urllib.parse import ParseResult, urlparse

from shared.utils.http_client import resilient_request
from bs4 import BeautifulSoup
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils.healthcare_identity import MatchDecision, identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_name

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


def _web_evidence(
    *,
    query: dict[str, Any],
    source_url: str = "",
    source_name: str = "Public web intelligence",
    dataset_id: str = "public_web",
    source_period: str = "runtime public web request",
    cache_status: str = "live_request",
    cache_freshness: str = "live request; not cached",
    match_basis: str,
    confidence: str,
    caveat: str = "Web content and search snippets are untrusted public evidence; verify source pages before citing facts.",
    next_step: str = "Open the source URL, confirm date/context, and preserve the page URL in report fact rows.",
) -> dict[str, Any]:
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope="web_osint",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _web_source_metadata(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a public-web evidence receipt."""

    return {
        "source_name": evidence.get("source_name", ""),
        "source_url": evidence.get("source_url", ""),
        "dataset_id": evidence.get("dataset_id", ""),
        "source_period": evidence.get("source_period", ""),
        "retrieved_at": evidence.get("retrieved_at", ""),
        "cache_status": evidence.get("cache_status", ""),
        "cache_freshness": evidence.get("cache_freshness", ""),
        "entity_scope": evidence.get("entity_scope", "web_osint"),
        "source_type": "public_web_osint",
    }


def _with_web_evidence(
    payload: dict[str, Any] | list[Any],
    *,
    query: dict[str, Any],
    source_url: str = "",
    source_name: str = "Public web intelligence",
    dataset_id: str = "public_web",
    source_period: str = "runtime public web request",
    cache_status: str = "live_request",
    cache_freshness: str = "live request; not cached",
    match_basis: str,
    confidence: str,
    caveat: str = "Web content and search snippets are untrusted public evidence; verify source pages before citing facts.",
    next_step: str = "Open the source URL, confirm date/context, and preserve the page URL in report fact rows.",
) -> dict[str, Any] | list[Any]:
    if not isinstance(payload, dict):
        return payload
    enriched = dict(payload)
    evidence = _web_evidence(
        query=query,
        source_url=source_url,
        source_name=source_name,
        dataset_id=dataset_id,
        source_period=source_period,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )
    enriched["evidence"] = evidence
    enriched["source_metadata"] = _web_source_metadata(evidence)
    if "identity" not in enriched:
        identity = _web_identity_from_query(
            query=query,
            source_name=source_name,
            source_url=source_url,
            match_basis=match_basis,
            confidence=confidence,
        )
        if identity:
            enriched["identity"] = identity
    _attach_web_row_evidence(enriched, query=query, dataset_id=dataset_id)
    enriched["identity_map"] = _web_identity_map(
        query=query,
        payload=enriched,
        dataset_id=dataset_id,
        source_url=source_url,
    )
    return enriched


def _cached_receipt_value(cached: Any, key: str, default: str = "") -> str:
    if isinstance(cached, dict) and isinstance(cached.get("evidence"), dict):
        return str(cached["evidence"].get(key) or default)
    return default


def _web_row_evidence(
    *,
    parent_query: dict[str, Any],
    row: dict[str, Any],
    dataset_id: str,
    source_name: str,
    match_basis: str,
    confidence: str,
    url_keys: tuple[str, ...] = ("link", "url", "source_url", "evidence_url", "linkedin_url"),
    source_period: str = "runtime public web request",
    cache_status: str = "live_request",
    cache_freshness: str = "live request; not cached",
    caveat: str = "This row is an unverified public web lead; verify the linked source page before citing facts.",
    next_step: str = "Open the row source URL, confirm date/context, and preserve this evidence receipt with the report fact row.",
) -> dict[str, Any]:
    source_url = ""
    for key in url_keys:
        value = str(row.get(key) or "").strip()
        if value:
            source_url = value
            break
    row_query = {
        **parent_query,
        "row_title": row.get("name") or row.get("gpo_name") or row.get("headline") or row.get("title") or "",
        "row_url": source_url,
        "row_display_link": row.get("display_link") or row.get("displayLink") or "",
        "row_source": row.get("source") or source_name,
        "row_date": row.get("published_at") or row.get("publication_date") or row.get("date") or "",
        "row_confidence": row.get("confidence") or confidence,
    }
    if row.get("snippet"):
        row_query["row_snippet"] = str(row["snippet"])[:300]
    if row.get("evidence_snippet"):
        row_query["row_evidence_snippet"] = str(row["evidence_snippet"])[:300]
    return _web_evidence(
        query={key: value for key, value in row_query.items() if value not in ("", None, [], {})},
        source_url=source_url,
        source_name=source_name,
        dataset_id=dataset_id,
        source_period=source_period,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _attach_web_row_evidence(payload: dict[str, Any], *, query: dict[str, Any], dataset_id: str) -> None:
    """Backfill row-level receipts for cached or model-dumped web list outputs."""

    if dataset_id == "public_web_system_profile":
        source_urls = payload.get("source_urls") if isinstance(payload.get("source_urls"), list) else []
        fallback_source_url = str(source_urls[0]) if source_urls else ""
        for row in payload.get("locations", []):
            if isinstance(row, dict) and "evidence" not in row:
                row_with_source = {**row, "source_url": row.get("source_url") or fallback_source_url}
                row["evidence"] = _web_row_evidence(
                    parent_query=query,
                    row=row_with_source,
                    dataset_id=dataset_id,
                    source_name="Public web system profile location row",
                    match_basis="system_profile_location_row",
                    confidence="public_location_snippet_unverified",
                    url_keys=("source_url",),
                    caveat=(
                        "Location rows are public website/search snippet leads and may be stale, partial, "
                        "or describe non-facility locations; verify the source page before citing."
                    ),
                    next_step="Open the source URL and confirm the location, address, facility type, and date/context before citing.",
                )
    elif dataset_id == "public_web_executive_profiles":
        for row in payload.get("executives", []):
            if isinstance(row, dict) and "evidence" not in row:
                row["evidence"] = _web_row_evidence(
                    parent_query=query,
                    row=row,
                    dataset_id=dataset_id,
                    source_name="Public web leadership profile row",
                    match_basis="leadership_profile_row",
                    confidence="public_leadership_profile_unverified",
                    url_keys=("source_url", "linkedin_url"),
                    caveat=(
                        "Executive profile rows are public web or LinkedIn leads and may be stale; "
                        "verify current title and organization on the source page before citing."
                    ),
                    next_step="Open the executive source URL and confirm title, organization, and date/context before citing.",
                )
    elif dataset_id == "public_news_search":
        for row in payload.get("items", []):
            if isinstance(row, dict) and "evidence" not in row:
                row["evidence"] = _web_row_evidence(
                    parent_query=query,
                    row=row,
                    dataset_id=dataset_id,
                    source_name="Public news search result row",
                    source_period=str(payload.get("evidence", {}).get("source_period") or "runtime public news request"),
                    match_basis="public_news_result_row",
                    confidence="public_news_result_unverified",
                    url_keys=("url",),
                    caveat=(
                        "News rows are public search/RSS leads and may be duplicated, stale, or partial; "
                        "verify the article or press release before citing."
                    ),
                    next_step="Open the news URL and confirm article date, source, and context before citing.",
                )
    elif dataset_id == "bundled_gpo_directory_public_web":
        for row in payload.get("matches", []):
            if isinstance(row, dict) and "evidence" not in row:
                row["evidence"] = _web_row_evidence(
                    parent_query=query,
                    row=row,
                    dataset_id=dataset_id,
                    source_name="Bundled GPO directory public web match row",
                    source_period="bundled directory plus runtime public web search",
                    match_basis="gpo_directory_match_row",
                    confidence=str(row.get("confidence") or "public_snippet_match"),
                    url_keys=("evidence_url",),
                    caveat=(
                        "GPO rows are public web snippet leads matched to a bundled GPO name; "
                        "they do not prove a current purchasing relationship."
                    ),
                    next_step="Open the evidence URL or direct GPO/organization record and verify the relationship before citing.",
                )


def _web_identity_from_query(
    *,
    query: dict[str, Any],
    source_name: str,
    source_url: str,
    match_basis: str,
    confidence: str,
) -> dict[str, Any] | None:
    name = str(query.get("system_name") or query.get("entity_name") or "").strip()
    if not name:
        return None
    identity = identity_from_public_record(
        name=name,
        entity_type="organization",
        ccn=query.get("ccn", ""),
        source_name=source_name,
        source_url=source_url,
    )
    identity.match_decisions.append(
        MatchDecision(
            basis=match_basis,
            confidence=confidence,
            decided_at=datetime.now(timezone.utc).isoformat(),
            notes=(
                "Web-intelligence identity is query-seed context for public web evidence; "
                "web snippets or no-hit results do not establish legal affiliation, ownership, or current status."
            ),
        )
    )
    for identifier_type, value in (
        ("system_domain", query.get("system_domain")),
        ("state", query.get("state")),
    ):
        if value not in ("", None):
            identity.unresolved_identifiers.append({"type": identifier_type, "value": str(value)})
    return identity.to_dict()


def _web_source_error(
    *,
    message: str,
    code: str,
    query: dict[str, Any],
    source_url: str = "https://programmablesearchengine.google.com/",
    source_name: str = "Public web intelligence",
    dataset_id: str = "public_web",
    match_basis: str,
    confidence: str,
    instructions: str = "",
    retryable: bool = False,
    detail: Any | None = None,
) -> dict[str, Any]:
    evidence = _web_evidence(
        query=query,
        source_url=source_url,
        source_name=source_name,
        dataset_id=dataset_id,
        cache_status="not_evaluated",
        cache_freshness=message,
        match_basis=match_basis,
        confidence=confidence,
        caveat=(
            "This public web source was not evaluated. Do not treat this response "
            "as evidence that a web fact, affiliation, executive, vendor, or news item is absent."
        ),
        next_step=instructions or "Resolve the source/search configuration, then rerun the query before citing facts.",
    )
    response = error_response(
        message,
        code=code,
        retryable=retryable,
        detail=detail,
        instructions=instructions,
        evidence=evidence,
    )
    response["source_metadata"] = _web_source_metadata(evidence)
    identity = _web_identity_from_query(
        query=query,
        source_name=source_name,
        source_url=source_url,
        match_basis=match_basis,
        confidence=confidence,
    )
    if identity:
        response["identity"] = identity
    response["identity_map"] = _web_identity_map(
        query=query,
        payload=response,
        dataset_id=dataset_id,
        source_url=source_url,
    )
    return response


def _web_identity_map(
    *,
    query: dict[str, Any],
    payload: dict[str, Any] | None = None,
    dataset_id: str = "",
    source_url: str = "",
) -> dict[str, Any]:
    """Return public-web source joins and OSINT caveat boundaries."""

    data = payload or {}
    evidence = data.get("evidence") if isinstance(data.get("evidence"), dict) else {}
    effective_dataset_id = dataset_id or str(evidence.get("dataset_id") or "")
    source_urls = _web_payload_source_urls(data, source_url=source_url)
    result_urls = _web_payload_result_urls(data)
    join_values = {
        "canonical_name": _web_identity_values(
            "canonical_name",
            query.get("system_name"),
            query.get("entity_name"),
            data.get("system_name"),
            data.get("entity_name"),
        ),
        "ccn": _web_identity_values("ccn", query.get("ccn"), data.get("ccn")),
        "system_domain": _web_identity_values(
            "system_domain",
            query.get("system_domain"),
            query.get("site_search"),
            data.get("domain"),
            data.get("site_search"),
        ),
        "query_text": _web_identity_values(
            "query_text",
            query.get("query"),
            query.get("search_query"),
            query.get("about_query"),
            query.get("leadership_query"),
        ),
        "source_url": _web_identity_values("source_url", *source_urls),
        "result_url": _web_identity_values("result_url", *result_urls),
    }
    source_claims = _web_source_claims(dataset_id=effective_dataset_id)
    return {
        "entity_scope": "web_osint",
        "join_keys": [
            {
                "field": field,
                "values": values,
                "status": "provided" if values else "missing",
                "used_by": _web_join_key_usage(field, source_claims),
            }
            for field, values in join_values.items()
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Treat public web pages, snippets, RSS entries, LinkedIn enrichment, and bundled GPO matches as OSINT leads unless exact public identifiers support the fact.",
            "Use CCN only for CMS Promoting Interoperability rows; keep web-inferred EHR or GPO signals separate from CMS-sourced facts.",
            "Keep domains, URLs, result snippets, titles, executive names, and GPO names as source-specific aliases or candidate evidence, not legal affiliation proof.",
            "Preserve source URL, retrieval time, cache status, match basis, and evidence caveat before citing web-derived facts.",
        ],
        "missing_data_policy": (
            "No-hit or not-evaluated web-intelligence responses describe only the configured public web/search/source scope; "
            "they are not proof of no web presence, no current leadership, no vendor, no GPO relationship, no news, or no affiliation."
        ),
    }


def _web_payload_source_urls(payload: dict[str, Any], *, source_url: str = "") -> list[Any]:
    values: list[Any] = [source_url, payload.get("source_url"), payload.get("url")]
    source_urls = payload.get("source_urls")
    if isinstance(source_urls, list):
        values.extend(source_urls)
    return values


def _web_payload_result_urls(payload: dict[str, Any]) -> list[Any]:
    values: list[Any] = []
    for key in ("results", "items", "locations", "executives", "matches"):
        rows = payload.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            values.extend(
                [
                    row.get("link"),
                    row.get("url"),
                    row.get("source_url"),
                    row.get("evidence_url"),
                    row.get("linkedin_url"),
                ]
            )
            evidence = row.get("evidence")
            if isinstance(evidence, dict):
                values.append(evidence.get("source_url"))
    return values


def _web_identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        normalized = _normalize_web_identity_value(field, value)
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _normalize_web_identity_value(field: str, value: Any) -> str:
    if value in ("", None):
        return ""
    if field == "canonical_name":
        return normalize_name(value, remove_legal_suffixes=True)
    if field == "ccn":
        return normalize_ccn(value) or ""
    if field == "system_domain":
        text = str(value).strip().lower()
        parsed = urlparse(text if "://" in text else f"https://{text}")
        return (parsed.hostname or text).strip().rstrip(".").lower()
    return str(value).strip()


def _web_source_claims(*, dataset_id: str) -> list[dict[str, Any]]:
    claims_by_dataset = {
        "google_cse_public_web_search": [
            {
                "collection": "google_cse_public_web_search",
                "identity_paths": ["query.query", "query.site_search", "results.title", "results.link", "results.display_link"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["results[].evidence"],
                "match_policy": "search_results_are_unverified_public_web_pointers",
            }
        ],
        "public_web_page_fetch": [
            {
                "collection": "public_web_page_fetch",
                "identity_paths": ["query.url", "url", "title", "meta", "text"],
                "evidence_path": "evidence",
                "match_policy": "direct_fetch_returns_untrusted_static_page_text_only",
            }
        ],
        "public_web_system_profile": [
            {
                "collection": "public_web_system_profile",
                "identity_paths": [
                    "query.system_name",
                    "query.system_domain",
                    "domain",
                    "source_urls",
                    "locations.name",
                    "locations.address",
                    "locations.evidence.source_url",
                ],
                "evidence_path": "evidence",
                "row_evidence_paths": ["locations[].evidence"],
                "match_policy": "system_name_domain_search_returns_candidate_profile_content",
            }
        ],
        "cms_promoting_interoperability_hospital": [
            {
                "collection": "cms_promoting_interoperability_hospital",
                "identity_paths": ["query.system_name", "query.ccn", "ccn", "vendor_name", "product_name", "cehrt_id"],
                "evidence_path": "evidence",
                "match_policy": "ccn_or_name_filter_for_cms_pi_ehr_fields",
            }
        ],
        "cms_pi_and_public_web_ehr_search": [
            {
                "collection": "cms_pi_and_public_web_ehr_search",
                "identity_paths": ["query.system_name", "query.ccn", "query.state", "source_url", "vendor_name", "product_name"],
                "evidence_path": "evidence",
                "match_policy": "waterfall_result_keeps_cms_pi_and_web_keyword_signals_separate",
            }
        ],
        "public_web_executive_profiles": [
            {
                "collection": "public_web_executive_profiles",
                "identity_paths": ["query.system_name", "query.system_domain", "source_urls", "executives.name", "executives.source_url", "executives.linkedin_url"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["executives[].evidence"],
                "match_policy": "leadership_pages_and_linkedin_are_candidate_currentness_sensitive_osint",
            }
        ],
        "public_news_search": [
            {
                "collection": "public_news_search",
                "identity_paths": ["query.system_name", "items.headline", "items.url", "items.source"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["items[].evidence"],
                "match_policy": "news_search_results_are_unverified_snippets_or_rss_items",
            }
        ],
        "bundled_gpo_directory_public_web": [
            {
                "collection": "bundled_gpo_directory_public_web",
                "identity_paths": ["query.system_name", "matches.gpo_name", "matches.evidence_url", "matches.evidence_snippet"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["matches[].evidence"],
                "match_policy": "bundled_gpo_name_match_against_public_web_snippets_returns_leads_only",
            }
        ],
    }
    return claims_by_dataset.get(
        dataset_id,
        [
            {
                "collection": "public_web_source_query",
                "identity_paths": ["query.system_name", "query.entity_name", "query.query", "source_url", "results.link"],
                "evidence_path": "evidence",
                "match_policy": "public_web_output_is_osint_context_not_resolved_affiliation",
            }
        ],
    )


def _web_join_key_usage(field: str, source_claims: list[dict[str, Any]]) -> list[str]:
    path_tokens = {
        "canonical_name": ("system_name", "entity_name"),
        "ccn": ("ccn",),
        "system_domain": ("system_domain", "site_search", "domain", "display_link"),
        "query_text": ("query", "search_query", "about_query", "leadership_query"),
        "source_url": ("source_url", "source_urls"),
        "result_url": ("link", "url", "evidence_url", "linkedin_url"),
    }[field]
    used_by = []
    for claim in source_claims:
        paths = " ".join(str(path) for path in claim.get("identity_paths", []))
        if any(token in paths for token in path_tokens):
            used_by.append(str(claim.get("collection") or ""))
    return sorted(item for item in used_by if item)

_BLOCKED_HOSTNAMES = {
    "localhost",
    "localhost.localdomain",
    "metadata",
    "metadata.google.internal",
}
_BLOCKED_HOSTNAME_SUFFIXES = (
    ".localhost",
    ".local",
    ".localdomain",
    ".lan",
    ".home",
    ".internal",
)
_METADATA_SERVICE_IPS = {
    ipaddress.ip_address("169.254.169.254"),
    ipaddress.ip_address("100.100.100.200"),
    ipaddress.ip_address("fd00:ec2::254"),
}

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "web-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8014"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Shared HTML fetch + parse helper
# ---------------------------------------------------------------------------

def _is_public_ip_address(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Return whether an IP address is safe for public web fetches."""
    if ip in _METADATA_SERVICE_IPS:
        return False
    return ip.is_global and not (
        ip.is_loopback
        or ip.is_private
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_unspecified
    )


def _url_host_is_public(parsed_url: ParseResult) -> bool:
    """Validate that a parsed http(s) URL targets a public internet host."""
    hostname = (parsed_url.hostname or "").strip().rstrip(".").lower()
    if not hostname:
        return False

    if hostname in _BLOCKED_HOSTNAMES or hostname.endswith(_BLOCKED_HOSTNAME_SUFFIXES):
        return False

    try:
        return _is_public_ip_address(ipaddress.ip_address(hostname))
    except ValueError:
        pass

    try:
        resolved_addresses = {
            sockaddr[0]
            for *_prefix, sockaddr in socket.getaddrinfo(hostname, parsed_url.port, type=socket.SOCK_STREAM)
        }
    except socket.gaierror:
        return False

    if not resolved_addresses:
        return False

    for address in resolved_addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError:
            return False
        if not _is_public_ip_address(ip):
            return False
    return True


async def _fetch_and_parse(url: str) -> tuple[str, BeautifulSoup | None]:
    """Fetch a URL and return (raw_html, parsed_soup).

    Returns ("", None) on failure. Timeout 15s.
    """
    try:
        resp = await resilient_request(
            "GET",
            url,
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
# Generic Web Search and Fetch Tools
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_web(
    query: str,
    max_results: int = 5,
    site_search: str = "",
) -> dict[str, Any]:
    """Search the public web through the configured Google Custom Search Engine.

    Args:
        query: Search query string.
        max_results: Maximum result count. Values outside 1-10 are clamped.
        site_search: Optional domain restriction such as "example.org".
    """
    try:
        cleaned_query = str(query or "").strip()
        if not cleaned_query:
            return error_response("query is required.", code="invalid_params")

        bounded_results = max(1, min(int(max_results or 5), 10))
        raw = await search_client.search(cleaned_query, num=bounded_results, site_search=site_search.strip())
        if "error" in raw:
            return _web_source_error(
                message=str(raw["error"]),
                code="source_unavailable",
                query={"query": cleaned_query, "site_search": site_search.strip(), "max_results": bounded_results},
                source_url="https://programmablesearchengine.google.com/",
                source_name="Google Custom Search public web results",
                dataset_id="google_cse_public_web_search",
                match_basis="google_cse_query_source_unavailable",
                confidence="not_evaluated_search_unavailable",
                instructions=str(raw.get("instructions") or "Configure Google CSE or retry after quota/rate-limit reset."),
                retryable=bool(raw.get("retry_after_seconds")),
                detail={"quota": raw.get("quota", raw.get("_search_meta", {})), "source": "google_cse"},
            )

        results = search_client.extract_results(raw)[:bounded_results]
        match_basis = "google_cse_query" if results else "google_cse_query_no_match"
        confidence = "snippet_level_unverified" if results else "no_google_cse_public_web_results"
        query_payload = {"query": cleaned_query, "site_search": site_search.strip(), "max_results": bounded_results}
        for result_row in results:
            result_row["evidence"] = _web_row_evidence(
                parent_query=query_payload,
                row=result_row,
                dataset_id="google_cse_public_web_search",
                source_name="Google Custom Search public web result row",
                match_basis="google_cse_result_row",
                confidence="snippet_level_unverified",
            )
        evidence = _web_evidence(
            query=query_payload,
            source_url="https://programmablesearchengine.google.com/",
            source_name="Google Custom Search public web results",
            dataset_id="google_cse_public_web_search",
            match_basis=match_basis,
            confidence=confidence,
            caveat=(
                "Google CSE snippets are untrusted public web pointers. A zero-result response "
                "means only that the configured CSE returned no results for this query."
            ),
            next_step="Open result URLs before citing facts; for zero results, broaden the query or verify with other public sources.",
        )
        payload = {
            "query": cleaned_query,
            "site_search": site_search.strip(),
            "count": len(results),
            "results": results,
            "evidence": evidence,
            "source_metadata": _web_source_metadata(evidence),
            "metadata": {
                "source": "google_cse",
                "quota": raw.get("_search_meta", {}),
                "warning": "Search snippets are untrusted public web content and should be verified against source pages.",
            },
        }
        payload["identity_map"] = _web_identity_map(
            query=query_payload,
            payload=payload,
            dataset_id="google_cse_public_web_search",
            source_url="https://programmablesearchengine.google.com/",
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("search_web failed")
        return error_response(f"search_web failed: {e}")


@mcp.tool(structured_output=True)
async def fetch_web_page(url: str, max_chars: int = 5000) -> dict[str, Any]:
    """Fetch one public web page and extract bounded visible text.

    Args:
        url: HTTP or HTTPS URL to fetch.
        max_chars: Maximum extracted text characters to return. Values outside 500-20000 are clamped.
    """
    try:
        cleaned_url = str(url or "").strip()
        parsed = urlparse(cleaned_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return error_response("url must be an absolute http(s) URL.", code="invalid_params")
        if not _url_host_is_public(parsed):
            return _web_source_error(
                message="url host must resolve to public internet addresses.",
                code="invalid_params",
                query={"url": cleaned_url},
                source_url=cleaned_url,
                source_name="Direct public HTTP fetch",
                dataset_id="public_web_page_fetch",
                match_basis="direct_public_http_fetch_rejected_non_public_host",
                confidence="not_fetched_non_public_host",
                instructions="Use only public internet http(s) URLs for web-intelligence fetches.",
            )

        bounded_chars = max(500, min(int(max_chars or 5000), 20000))
        html, soup = await _fetch_and_parse(cleaned_url)
        if soup is None:
            return _web_source_error(
                message=f"Unable to fetch or parse URL: {cleaned_url}",
                code="source_unavailable",
                query={"url": cleaned_url, "max_chars": bounded_chars},
                source_url=cleaned_url,
                source_name="Direct public HTTP fetch",
                dataset_id="public_web_page_fetch",
                match_basis="direct_public_http_fetch_source_unavailable",
                confidence="not_evaluated_fetch_failed",
                instructions="Retry later or inspect the page manually before citing it.",
                retryable=True,
            )

        title = soup.title.get_text(strip=True) if soup.title else ""
        text = _extract_text_content(soup)[:bounded_chars]
        query_payload = {"url": cleaned_url, "max_chars": bounded_chars}
        evidence = _web_evidence(
            query=query_payload,
            source_url=cleaned_url,
            source_name="Direct public HTTP fetch",
            dataset_id="public_web_page_fetch",
            match_basis="direct_public_http_fetch",
            confidence="fetched_page_text_unverified",
        )
        payload = {
            "url": cleaned_url,
            "title": title,
            "text": text,
            "text_char_count": len(text),
            "evidence": evidence,
            "source_metadata": _web_source_metadata(evidence),
            "meta": _extract_meta(html),
            "metadata": {
                "source": "direct_http_fetch",
                "content_type": "html",
                "max_chars": bounded_chars,
                "warning": (
                    "Fetched page content is untrusted public web content. "
                    "This is a static HTTP fetch and may miss JavaScript-rendered content."
                ),
            },
        }
        payload["identity_map"] = _web_identity_map(
            query=query_payload,
            payload=payload,
            dataset_id="public_web_page_fetch",
            source_url=cleaned_url,
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("fetch_web_page failed")
        return error_response(f"fetch_web_page failed: {e}")


# ---------------------------------------------------------------------------
# Tool 1: scrape_system_profile
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def scrape_system_profile(
    system_name: str,
    system_domain: str = "",
) -> dict[str, Any]:
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
            return to_structured(
                _with_web_evidence(
                    cached,
                    query=cache_params,
                    source_url=(cached.get("source_urls") or [""])[0] if isinstance(cached, dict) else "",
                    dataset_id="public_web_system_profile",
                    cache_status="cache_hit",
                    cache_freshness=f"web-intelligence response cache within {data_loaders._PAGE_TTL_DAYS} days",
                    match_basis="cached_system_profile_lookup",
                    confidence=str(cached.get("data_quality") or "cached_public_web_profile") if isinstance(cached, dict) else "cached_public_web_profile",
                )
            )

        # Step 1: Search for About/Mission pages
        about_query = f'"{system_name}" about us mission vision'
        about_raw = await search_client.search(
            about_query,
            num=5,
            site_search=system_domain if system_domain else "",
        )
        if "error" in about_raw:
            return _web_source_error(
                message=str(about_raw["error"]),
                code="source_unavailable",
                query={"system_name": system_name, "system_domain": system_domain, "about_query": about_query},
                source_url="https://programmablesearchengine.google.com/",
                source_name="Google Custom Search public web results",
                dataset_id="public_web_system_profile",
                match_basis="about_page_search_source_unavailable",
                confidence="not_evaluated_search_unavailable",
                instructions=str(about_raw.get("instructions") or "Configure Google CSE or retry after quota/rate-limit reset."),
                retryable=bool(about_raw.get("retry_after_seconds")),
                detail={"quota": about_raw.get("quota", about_raw.get("_search_meta", {})), "source": "google_cse"},
            )

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
                "locations facilities",
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
        result = _with_web_evidence(
            result,
            query={"system_name": system_name, "system_domain": system_domain, "about_query": about_query},
            source_url=source_urls[0] if source_urls else "https://programmablesearchengine.google.com/",
            dataset_id="public_web_system_profile",
            match_basis="about_page_search_and_fetch" if mission or source_urls else "about_page_search_and_fetch_no_match",
            confidence=data_quality if mission or source_urls else "no_public_system_profile_match",
            caveat=(
                "System profile content is public web OSINT and may be stale, promotional, or incomplete. "
                "A no-match result does not prove the system lacks public profile pages or locations."
            ),
            next_step="Open each source URL and confirm current context before using mission, leadership, or location facts.",
        )
        data_loaders.cache_response("profile", cache_params, result)
        return to_structured(result)
    except Exception as e:
        logger.exception("scrape_system_profile failed")
        return error_response(f"scrape_system_profile failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: detect_ehr_vendor
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def detect_ehr_vendor(
    system_name: str,
    ccn: str = "",
    state: str = "",
) -> dict[str, Any]:
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
            return to_structured(
                _with_web_evidence(
                    cached,
                    query=cache_params,
                    source_url=(
                        cached.get("source_url", "")
                        if isinstance(cached, dict)
                        else ""
                    ) or _cached_receipt_value(cached, "source_url"),
                    source_name=_cached_receipt_value(cached, "source_name", "Cached public EHR/source waterfall"),
                    dataset_id=_cached_receipt_value(cached, "dataset_id", "cms_pi_and_public_web_ehr_search"),
                    source_period=_cached_receipt_value(cached, "source_period", "cached web-intelligence response"),
                    cache_status="cache_hit",
                    cache_freshness=f"web-intelligence response cache within {data_loaders._SEARCH_TTL_DAYS} days",
                    match_basis="cached_ehr_vendor_waterfall",
                    confidence=str(cached.get("confidence") or "cached_ehr_vendor_result") if isinstance(cached, dict) else "cached_ehr_vendor_result",
                )
            )

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
                result = _with_web_evidence(
                    result,
                    query={"system_name": system_name, "ccn": ccn, "state": state},
                    source_url="https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
                    source_name="CMS Promoting Interoperability Hospital",
                    dataset_id="cms_promoting_interoperability_hospital",
                    source_period="current cached CMS Promoting Interoperability public file",
                    cache_status="cms_pi_cache_or_download",
                    cache_freshness=f"CMS PI cache valid for {data_loaders._PI_TTL_DAYS} days when present",
                    match_basis="cms_pi_ccn_or_name_lookup",
                    confidence=response.confidence,
                )
                data_loaders.cache_response("ehr", cache_params, result)
                return to_structured(result)

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
                        result = _with_web_evidence(
                            result,
                            query={"system_name": system_name, "search_query": career_query},
                            source_url=r.get("link", ""),
                            match_basis="career_page_keyword_search",
                            confidence=response.confidence,
                        )
                        data_loaders.cache_response("ehr", cache_params, result)
                        return to_structured(result)

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
                        result = _with_web_evidence(
                            result,
                            query={"system_name": system_name, "search_query": news_query, "date_restrict": "m12"},
                            source_url=r.get("link", ""),
                            match_basis="news_keyword_search",
                            confidence=response.confidence,
                        )
                        data_loaders.cache_response("ehr", cache_params, result)
                        return to_structured(result)

        # No match found
        response = EhrDetectionResponse(
            system_name=system_name,
            confidence="NOT_FOUND",
            evidence_summary="No EHR vendor identified from PI data, career pages, or news.",
        )
        return to_structured(
            _with_web_evidence(
                response.model_dump(),
                query={"system_name": system_name, "ccn": ccn, "state": state},
                source_url="https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
                source_name="CMS Promoting Interoperability plus public web search",
                dataset_id="cms_pi_and_public_web_ehr_search",
                cache_status="no_match",
                cache_freshness="CMS PI cache/search completed but no source row or web signal matched",
                match_basis="pi_career_news_waterfall_no_match",
                confidence="not_found",
                caveat=(
                    "EHR vendor detection uses CMS PI rows when available and otherwise public web keyword signals; "
                    "a no-match result is not proof that an organization lacks an EHR vendor."
                ),
                next_step="Use a CCN for CMS PI lookup where possible and verify any web-inferred vendor against the source page.",
            )
        )
    except Exception as e:
        logger.exception("detect_ehr_vendor failed")
        return error_response(f"detect_ehr_vendor failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: get_executive_profiles
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_executive_profiles(
    system_name: str,
    system_domain: str = "",
    include_linkedin: bool = True,
    max_results: int = 20,
) -> dict[str, Any]:
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
            return to_structured(
                _with_web_evidence(
                    cached,
                    query=cache_params,
                    source_url=(cached.get("source_urls") or [""])[0] if isinstance(cached, dict) else "",
                    source_name=_cached_receipt_value(cached, "source_name", "Cached public web leadership profile"),
                    dataset_id=_cached_receipt_value(cached, "dataset_id", "public_web_executive_profiles"),
                    source_period=_cached_receipt_value(cached, "source_period", "cached public leadership profile"),
                    cache_status="cache_hit",
                    cache_freshness=f"web-intelligence response cache within {data_loaders._EXEC_TTL_DAYS} days",
                    match_basis="cached_executive_profile_lookup",
                    confidence="cached_public_leadership_profile",
                )
            )

        # Step 1: Find the leadership page
        lead_query = f'"{system_name}" leadership "executive team" OR "senior leadership" OR "board of"'
        lead_raw = await search_client.search(
            lead_query, num=5,
            site_search=system_domain if system_domain else "",
        )
        if "error" in lead_raw:
            return _web_source_error(
                message=str(lead_raw["error"]),
                code="source_unavailable",
                query={"system_name": system_name, "system_domain": system_domain, "leadership_query": lead_query},
                source_url="https://programmablesearchengine.google.com/",
                source_name="Google Custom Search public leadership results",
                dataset_id="public_web_executive_profiles",
                match_basis="leadership_page_search_source_unavailable",
                confidence="not_evaluated_search_unavailable",
                instructions=str(lead_raw.get("instructions") or "Configure Google CSE or retry after quota/rate-limit reset."),
                retryable=bool(lead_raw.get("retry_after_seconds")),
                detail={"quota": lead_raw.get("quota", lead_raw.get("_search_meta", {})), "source": "google_cse"},
            )

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
        exec_query = {"system_name": system_name, "system_domain": system_domain, "include_linkedin": include_linkedin}
        for executive in result["executives"]:
            executive["evidence"] = _web_row_evidence(
                parent_query=exec_query,
                row=executive,
                dataset_id="public_web_executive_profiles",
                source_name="Public web leadership profile row",
                match_basis="leadership_profile_row",
                confidence=(
                    "parsed_public_leadership_page"
                    if executive.get("source_url") in source_urls
                    else "search_snippet_candidate_profile"
                ),
                url_keys=("source_url", "linkedin_url"),
                caveat=(
                    "Executive profile rows are public web or LinkedIn leads and may be stale; "
                    "verify current title and organization on the source page before citing."
                ),
                next_step="Open the executive source URL and confirm title, organization, and date/context before citing.",
            )
        result = _with_web_evidence(
            result,
            query=exec_query,
            source_url=source_urls[0] if source_urls else "https://programmablesearchengine.google.com/",
            dataset_id="public_web_executive_profiles",
            match_basis="leadership_page_search_and_parse" if executives else "leadership_page_search_and_parse_no_match",
            confidence=(
                "parsed_public_leadership_page"
                if source_urls and executives
                else "search_snippet_candidate_profiles"
                if executives
                else "no_public_executive_profile_match"
            ),
            caveat=(
                "Executive profiles are inferred from public web pages, snippets, and optional LinkedIn enrichment; "
                "titles may be stale and no-hit results are not proof that no executive page exists."
            ),
            next_step="Verify each executive title against the source page and preserve the source URL/date before citing.",
        )
        data_loaders.cache_response("exec", cache_params, result)
        return to_structured(result)
    except Exception as e:
        logger.exception("get_executive_profiles failed")
        return error_response(f"get_executive_profiles failed: {e}")


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
@mcp.tool(structured_output=True)
async def monitor_newsroom(
    system_name: str,
    days_back: int = 90,
    max_results: int = 25,
) -> dict[str, Any]:
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
            return to_structured(
                _with_web_evidence(
                    cached,
                    query=cache_params,
                    source_url="https://programmablesearchengine.google.com/",
                    source_name=_cached_receipt_value(cached, "source_name", "Cached public news search results"),
                    dataset_id=_cached_receipt_value(cached, "dataset_id", "public_news_search"),
                    source_period=_cached_receipt_value(cached, "source_period", f"last {days_back} days requested"),
                    cache_status="cache_hit",
                    cache_freshness=f"web-intelligence response cache within {data_loaders._NEWS_TTL_DAYS} days",
                    match_basis="cached_newsroom_monitor",
                    confidence="cached_public_news_results",
                )
            )

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
        news_query_payload = {"system_name": system_name, "days_back": days_back, "max_results": max_results}
        for item in result["items"]:
            item["evidence"] = _web_row_evidence(
                parent_query=news_query_payload,
                row=item,
                dataset_id="public_news_search",
                source_name="Public news search result row",
                source_period=f"last {days_back} days requested",
                match_basis="public_news_result_row",
                confidence="public_news_result_unverified",
                url_keys=("url",),
                caveat=(
                    "News rows are public search/RSS leads and may be duplicated, stale, or partial; "
                    "verify the article or press release before citing."
                ),
                next_step="Open the news URL and confirm article date, source, and context before citing.",
            )
        result = _with_web_evidence(
            result,
            query=news_query_payload,
            source_url="https://programmablesearchengine.google.com/",
            source_name="Google Custom Search and Google News RSS public results",
            dataset_id="public_news_search",
            source_period=f"last {days_back} days requested",
            match_basis="google_cse_news_or_rss" if items else "google_cse_news_or_rss_no_match",
            confidence="public_news_results_unverified" if items else "no_public_news_results",
            caveat=(
                "Public news search results are snippets/RSS entries and may omit relevant coverage. "
                "A zero-result response only means these configured sources returned no items."
            ),
            next_step="Open each news URL before citing; for zero results, broaden the query or inspect the organization's newsroom directly.",
        )
        data_loaders.cache_response("news", cache_params, result)
        return to_structured(result)
    except Exception as e:
        logger.exception("monitor_newsroom failed")
        return error_response(f"monitor_newsroom failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: detect_gpo_affiliation
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def detect_gpo_affiliation(
    system_name: str,
) -> dict[str, Any]:
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
            return to_structured(
                _with_web_evidence(
                    cached,
                    query=cache_params,
                    source_url="https://programmablesearchengine.google.com/",
                    source_name="Bundled GPO directory plus public web search",
                    dataset_id="bundled_gpo_directory_public_web",
                    cache_status="cache_hit",
                    cache_freshness=f"web-intelligence response cache within {data_loaders._SEARCH_TTL_DAYS} days",
                    match_basis="cached_gpo_affiliation_search",
                    confidence="cached_public_gpo_match",
                )
            )

        gpo_list = data_loaders.load_gpo_directory()
        if not gpo_list:
            return _web_source_error(
                message="GPO directory not found",
                code="source_unavailable",
                query={"system_name": system_name},
                source_name="Bundled GPO directory plus public web search",
                dataset_id="bundled_gpo_directory_public_web",
                match_basis="bundled_gpo_directory_missing",
                confidence="not_evaluated_source_missing",
                instructions="Restore the bundled GPO directory before running GPO affiliation detection.",
            )

        # Build search query with top GPO names
        top_gpos = "OR".join(f' "{g["gpo_name"]}"' for g in gpo_list[:6])
        search_query = f'"{system_name}" GPO OR "group purchasing"{top_gpos}'

        raw = await search_client.search(search_query, num=10)
        if "error" in raw:
            return _web_source_error(
                message=str(raw["error"]),
                code="source_unavailable",
                query={"system_name": system_name, "search_query": search_query[:200]},
                source_url="https://programmablesearchengine.google.com/",
                source_name="Bundled GPO directory plus public web search",
                dataset_id="bundled_gpo_directory_public_web",
                match_basis="gpo_public_search_source_unavailable",
                confidence="not_evaluated_search_unavailable",
                instructions=str(raw.get("instructions") or "Configure Google CSE or retry after quota/rate-limit reset."),
                retryable=bool(raw.get("retry_after_seconds")),
                detail={"quota": raw.get("quota", raw.get("_search_meta", {})), "source": "google_cse"},
            )

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
        gpo_query_payload = {"system_name": system_name, "search_query": search_query[:200]}
        for match in result["matches"]:
            match["evidence"] = _web_row_evidence(
                parent_query=gpo_query_payload,
                row=match,
                dataset_id="bundled_gpo_directory_public_web",
                source_name="Bundled GPO directory public web match row",
                source_period="bundled directory plus runtime public web search",
                match_basis="gpo_directory_match_row",
                confidence=str(match.get("confidence") or "public_snippet_match"),
                url_keys=("evidence_url",),
                caveat=(
                    "GPO rows are public web snippet leads matched to a bundled GPO name; "
                    "they do not prove a current purchasing relationship."
                ),
                next_step="Open the evidence URL or direct GPO/organization record and verify the relationship before citing.",
            )
        result = _with_web_evidence(
            result,
            query=gpo_query_payload,
            source_url="https://programmablesearchengine.google.com/",
            source_name="Bundled GPO directory plus public web search",
            dataset_id="bundled_gpo_directory_public_web",
            source_period="bundled directory plus runtime public web search",
            match_basis=(
                "gpo_directory_name_match_against_search_results"
                if matches
                else "gpo_directory_name_match_against_search_results_no_match"
            ),
            confidence="public_snippet_match" if matches else "no_public_gpo_match",
            caveat=(
                "GPO affiliation detection compares bundled GPO names against public web snippets; "
                "matches are leads, and no-hit results are not proof of no GPO affiliation."
            ),
            next_step="Verify any GPO match against the linked source or direct organization/GPO records before citing.",
        )
        data_loaders.cache_response("gpo", cache_params, result)
        return to_structured(result)
    except Exception as e:
        logger.exception("detect_gpo_affiliation failed")
        return error_response(f"detect_gpo_affiliation failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
