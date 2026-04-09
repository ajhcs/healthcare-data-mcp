"""MRF Registry -- discover hospital MRF file URLs.

Two-layer lookup:
1. Curated JSON registry (cached at ~/.healthcare-data-mcp/cache/mrf/registry.json)
2. Live discovery via CMS Provider Data Catalog + hospital cms-hpt.txt files
"""

import json
import logging
import re
from pathlib import Path

import httpx

from shared.utils.http_client import resilient_request, get_client

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "mrf"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_REGISTRY_PATH = _CACHE_DIR / "registry.json"

CMS_PROVIDER_DATASET = "xubh-q36u"
CMS_PROVIDER_API = f"https://data.cms.gov/provider-data/api/1/datastore/query/{CMS_PROVIDER_DATASET}/0"


# ---------------------------------------------------------------------------
# Layer 1: Curated local registry
# ---------------------------------------------------------------------------

def _load_registry() -> dict:
    """Load the curated registry from disk."""
    if _REGISTRY_PATH.exists():
        try:
            return json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to load registry, starting fresh")
    return {"hospitals": {}}


def _save_registry(registry: dict) -> None:
    """Persist the registry to disk."""
    _REGISTRY_PATH.write_text(json.dumps(registry, indent=2), encoding="utf-8")


def search_registry(query: str, state: str = "") -> list[dict]:
    """Search the local curated registry by name, CCN, or EIN.

    Returns list of matching hospital entries (dicts with keys:
    name, ccn, ein, domain, mrf_urls, city, state).
    """
    registry = _load_registry()
    query_lower = query.strip().lower()
    results = []

    for ccn, entry in registry.get("hospitals", {}).items():
        name = entry.get("name", "").lower()
        ein = entry.get("ein", "").lower()
        entry_state = entry.get("state", "").upper()

        if state and entry_state != state.upper():
            continue

        if query_lower in name or query_lower == ccn.lower() or query_lower == ein:
            results.append({"ccn": ccn, **entry})

    return results


# ---------------------------------------------------------------------------
# Layer 2: CMS Provider Data Catalog
# ---------------------------------------------------------------------------

async def search_cms_providers(query: str, state: str = "") -> list[dict]:
    """Query CMS Provider Data Catalog for hospitals matching name or ID.

    Uses POST against the data.cms.gov datastore API.  For 6-digit queries
    (CCN-style), uses exact match on facility_id.  For text queries, uses
    LIKE with % wildcards on facility_name.

    Returns list of provider records from data.cms.gov.
    """
    conditions: list[dict] = []

    # CCN-style query (6 digits) -> exact match
    if re.match(r"^\d{6}$", query.strip()):
        conditions.append({
            "property": "facility_id",
            "value": query.strip(),
            "operator": "=",
        })
    else:
        conditions.append({
            "property": "facility_name",
            "value": f"%{query}%",
            "operator": "LIKE",
        })

    if state:
        conditions.append({
            "property": "state",
            "value": state.upper(),
            "operator": "=",
        })

    payload = {
        "conditions": conditions,
        "limit": 25,
        "offset": 0,
    }

    try:
        resp = await resilient_request("POST", CMS_PROVIDER_API, json=payload, timeout=30.0)
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.warning("CMS provider search failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Layer 3: cms-hpt.txt fetcher
# ---------------------------------------------------------------------------

async def fetch_cms_hpt_txt(domain: str) -> list[dict]:
    """Fetch and parse a hospital's cms-hpt.txt file.

    Per CMS requirements, hospitals must host a machine-readable file at
    https://{domain}/cms-hpt.txt describing their MRF locations.

    Returns list of location entries, each with keys:
    location_name, source_page_url, mrf_url, contact_name, contact_email.
    """
    url = f"https://{domain}/cms-hpt.txt"
    try:
        resp = await resilient_request("GET", url, timeout=30.0)
        return _parse_cms_hpt_txt(resp.text)
    except Exception as e:
        logger.warning("Failed to fetch cms-hpt.txt from %s: %s", domain, e)
        return []


def _parse_cms_hpt_txt(text: str) -> list[dict]:
    """Parse cms-hpt.txt plaintext format into location entries.

    Format is blocks of key: value lines separated by blank lines::

        location-name: Hospital Name
        source-page-url: https://...
        mrf-url: https://..._standardcharges.csv
        contact-name: Person
        contact-email: email@...

    Keys are normalized: lowercased, hyphens replaced with underscores.
    """
    entries: list[dict] = []
    current: dict[str, str] = {}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue

        # Parse "key: value" lines
        match = re.match(r"^([a-z_-]+)\s*:\s*(.+)$", line, re.IGNORECASE)
        if match:
            key = match.group(1).strip().lower().replace("-", "_")
            value = match.group(2).strip()
            current[key] = value

    # Don't lose a trailing block with no final blank line
    if current:
        entries.append(current)

    return entries


# ---------------------------------------------------------------------------
# Full discovery pipeline
# ---------------------------------------------------------------------------

async def discover_mrf_urls(query: str, state: str = "") -> list[dict]:
    """Full discovery pipeline: registry -> CMS provider lookup -> cache result.

    Returns list of hospital dicts with mrf_urls populated (may be empty if
    no cms-hpt.txt has been fetched yet for that hospital).
    Caches new discoveries in the registry.
    """
    # Layer 1: Check curated registry first
    results = search_registry(query, state)
    if results:
        return results

    # Layer 2: Search CMS Provider Data Catalog
    providers = await search_cms_providers(query, state)
    if not providers:
        return []

    discovered: list[dict] = []
    registry = _load_registry()

    for provider in providers[:5]:  # Limit to 5 to avoid slow crawling
        ccn = provider.get("facility_id", "")
        name = provider.get("facility_name", "")

        # Check if already in registry
        if ccn in registry.get("hospitals", {}):
            discovered.append({"ccn": ccn, **registry["hospitals"][ccn]})
            continue

        entry = {
            "name": name,
            "ein": "",
            "city": provider.get("citytown", ""),
            "state": provider.get("state", ""),
            "domain": "",
            "mrf_urls": [],
        }

        discovered.append({"ccn": ccn, **entry})

        # Cache in registry (even without MRF URL -- marks it as "looked up")
        registry.setdefault("hospitals", {})[ccn] = entry

    _save_registry(registry)
    return discovered


async def discover_and_fetch_hpt(domain: str, ccn: str = "") -> list[dict]:
    """Fetch cms-hpt.txt from a domain and update registry if CCN provided.

    This is the manual domain-based lookup path: the caller already knows the
    hospital's website domain and (optionally) CCN.

    Returns parsed location entries with mrf_url fields.
    """
    entries = await fetch_cms_hpt_txt(domain)
    if not entries or not ccn:
        return entries

    # Update registry with discovered MRF URLs
    registry = _load_registry()
    if ccn in registry.get("hospitals", {}):
        mrf_urls: list[dict] = []
        for entry in entries:
            mrf_url = entry.get("mrf_url", "")
            if mrf_url:
                fmt = "json" if mrf_url.lower().endswith(".json") else "csv"
                mrf_urls.append({"url": mrf_url, "format": fmt, "last_verified": ""})
        if mrf_urls:
            registry["hospitals"][ccn]["mrf_urls"] = mrf_urls
            registry["hospitals"][ccn]["domain"] = domain
            _save_registry(registry)

    return entries
