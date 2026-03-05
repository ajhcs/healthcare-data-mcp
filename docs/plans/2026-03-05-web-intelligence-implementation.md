# Web Intelligence & OSINT — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build Server 13 (port 8014) — an OSINT server using Google Custom Search API for health system competitive intelligence.

**Architecture:** 5 tools built on a single `search_client.py` Google CSE wrapper. `data_loaders.py` manages PI Parquet cache and API response caching. Optional `proxycurl_client.py` for LinkedIn enrichment. All tools async, return `json.dumps()`, follow existing FastMCP patterns.

**Tech Stack:** FastMCP, httpx, BeautifulSoup4, lxml, DuckDB, pandas, Pydantic

**Design doc:** `docs/plans/2026-03-05-web-intelligence-design.md`

---

### Task 1: Scaffold module with empty server

**Files:**
- Create: `servers/web_intelligence/__init__.py`
- Create: `servers/web_intelligence/server.py`

**Step 1: Create directory and __init__.py**

```bash
mkdir -p servers/web_intelligence/data
```

Write `servers/web_intelligence/__init__.py` — empty file (0 bytes, matches project convention).

**Step 2: Write minimal server.py**

```python
"""Web Intelligence & OSINT MCP Server.

Provides tools for health system competitive intelligence via web search,
executive profiling, EHR detection, and news monitoring. Port 8014.
"""

import json
import logging
import os as _os

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "web-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8014"))
mcp = FastMCP(**_mcp_kwargs)


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
```

**Step 3: Verify it boots**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && timeout 5 python -m servers.web_intelligence.server 2>&1 || true`
Expected: Clean startup (no import errors), exits on timeout or runs.

**Step 4: Commit**

```bash
git add servers/web_intelligence/
git commit -m "feat(web-intelligence): scaffold Server 13 module (port 8014)"
```

---

### Task 2: Pydantic response models

**Files:**
- Create: `servers/web_intelligence/models.py`

**Step 1: Write models.py**

All fields defaulted (project convention). Grouped by tool with comment separators.

```python
"""Pydantic response models for the web-intelligence MCP server.

Covers 5 tools:
  1. scrape_system_profile  — Health system website profile extraction
  2. detect_ehr_vendor      — EHR vendor identification
  3. get_executive_profiles — Executive bios from websites + LinkedIn
  4. monitor_newsroom       — Press releases and news mentions
  5. detect_gpo_affiliation — Group Purchasing Organization matching
"""

from pydantic import BaseModel, Field


# --- Tool 1: scrape_system_profile ---


class LocationEntry(BaseModel):
    """A single facility/location extracted from a health system website."""

    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    location_type: str = ""


class SystemProfileResponse(BaseModel):
    """Response from scrape_system_profile."""

    system_name: str = ""
    domain: str = ""
    mission: str = ""
    vision: str = ""
    values: str = ""
    tagline: str = ""
    location_count: int = 0
    locations: list[LocationEntry] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)
    data_quality: str = ""  # "full_parse" | "meta_tags_only" | "snippets_only"


# --- Tool 2: detect_ehr_vendor ---


class EhrDetectionResponse(BaseModel):
    """Response from detect_ehr_vendor."""

    system_name: str = ""
    vendor_name: str = ""
    product_name: str = ""
    confidence: str = ""  # "PI_DATA" | "CAREER_PAGE" | "NEWS_MENTION" | "NOT_FOUND"
    evidence_summary: str = ""
    source_url: str = ""
    cehrt_id: str = ""


# --- Tool 3: get_executive_profiles ---


class LinkedInData(BaseModel):
    """LinkedIn enrichment data from Proxycurl (optional)."""

    headline: str = ""
    summary: str = ""
    education: str = ""
    linkedin_url: str = ""


class ExecutiveProfile(BaseModel):
    """A single executive profile."""

    name: str = ""
    title: str = ""
    bio_snippet: str = ""
    source_url: str = ""
    linkedin_url: str = ""
    linkedin_data: LinkedInData | None = None


class ExecutiveProfilesResponse(BaseModel):
    """Response from get_executive_profiles."""

    system_name: str = ""
    total_results: int = 0
    executives: list[ExecutiveProfile] = Field(default_factory=list)
    source_urls: list[str] = Field(default_factory=list)


# --- Tool 4: monitor_newsroom ---


class NewsItem(BaseModel):
    """A single news item or press release."""

    headline: str = ""
    source: str = ""
    date: str = ""
    snippet: str = ""
    url: str = ""


class NewsroomResponse(BaseModel):
    """Response from monitor_newsroom."""

    system_name: str = ""
    days_back: int = 0
    total_results: int = 0
    items: list[NewsItem] = Field(default_factory=list)


# --- Tool 5: detect_gpo_affiliation ---


class GpoMatch(BaseModel):
    """A single GPO match with supporting evidence."""

    gpo_name: str = ""
    confidence: str = ""  # "strong" | "moderate" | "weak"
    evidence_snippet: str = ""
    evidence_url: str = ""


class GpoAffiliationResponse(BaseModel):
    """Response from detect_gpo_affiliation."""

    system_name: str = ""
    matches: list[GpoMatch] = Field(default_factory=list)
    search_terms_used: str = ""
```

**Step 2: Verify models import**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python -c "from servers.web_intelligence.models import *; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add servers/web_intelligence/models.py
git commit -m "feat(web-intelligence): add Pydantic response models for 5 tools"
```

---

### Task 3: Google Custom Search API client

**Files:**
- Create: `servers/web_intelligence/search_client.py`

This is the core dependency — all 5 tools route through this. The Google CSE JSON API v1 endpoint is `https://www.googleapis.com/customsearch/v1`.

Key params: `key`, `cx`, `q`, `num` (1-10 results), `start`, `siteSearch` (scope to domain), `dateRestrict` (e.g. `d90` for last 90 days), `sort` (e.g. `date` for recency).

**Step 1: Write search_client.py**

```python
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
```

**Step 2: Verify module imports**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python -c "from servers.web_intelligence import search_client; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add servers/web_intelligence/search_client.py
git commit -m "feat(web-intelligence): add Google Custom Search API client"
```

---

### Task 4: Data loaders (PI cache + GPO lookup + response cache)

**Files:**
- Create: `servers/web_intelligence/data_loaders.py`

This module handles three concerns:
1. CMS Promoting Interoperability CSV → Parquet cache (self-contained copy of Server 12's PI logic)
2. Static GPO directory lookup from bundled CSV
3. SHA256-keyed API response cache (same pattern as Server 12)

**Step 1: Write data_loaders.py**

```python
"""Data loaders for web-intelligence server.

Manages:
1. CMS Promoting Interoperability CSV -> Parquet cache (EHR vendor detection)
2. Static GPO directory lookup from bundled CSV
3. SHA256-keyed API/page response cache
"""

import csv
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache configuration
# ---------------------------------------------------------------------------

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "web-intelligence"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_PI_TTL_DAYS = 90
_SEARCH_TTL_DAYS = 30
_NEWS_TTL_DAYS = 7
_EXEC_TTL_DAYS = 90
_PAGE_TTL_DAYS = 30

# CMS Promoting Interoperability bulk CSV
PI_URL = (
    "https://data.cms.gov/provider-data/sites/default/files/resources/"
    "5462b19a756c53c1becccf13787d9157_1770163678/Promoting_Interoperability-Hospital.csv"
)
_PI_PARQUET = _CACHE_DIR / "pi_hospital.parquet"

# Static data paths
_GPO_CSV = Path(__file__).parent / "data" / "gpo_directory.csv"

# ---------------------------------------------------------------------------
# CEHRT ID prefix -> vendor name (common vendors covering ~95% of hospitals)
# Used as last-resort fallback when PI data lacks ehr_developer column.
# ---------------------------------------------------------------------------

VENDOR_KEYWORDS: dict[str, str] = {
    "epic": "Epic Systems",
    "cerner": "Oracle Health (Cerner)",
    "oracle health": "Oracle Health (Cerner)",
    "oracle cerner": "Oracle Health (Cerner)",
    "meditech": "MEDITECH",
    "altera": "Altera Digital Health",
    "allscripts": "Altera Digital Health",
    "athenahealth": "athenahealth",
    "athena": "athenahealth",
    "cpsi": "CPSI (TruBridge)",
    "trubridge": "CPSI (TruBridge)",
    "veradigm": "Veradigm",
    "nextgen": "NextGen Healthcare",
    "eclinicalworks": "eClinicalWorks",
}

# ---------------------------------------------------------------------------
# TTL helpers
# ---------------------------------------------------------------------------


def _is_cache_valid(path: Path, ttl_days: int) -> bool:
    """Check if a cached file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def _get_con(parquet_path: Path, view_name: str = "data") -> duckdb.DuckDBPyConnection | None:
    """Create DuckDB in-memory connection with a view over a Parquet file."""
    if not parquet_path.exists():
        return None
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        return con
    except Exception:
        logger.warning("Corrupt Parquet cache, deleting: %s", parquet_path)
        con.close()
        parquet_path.unlink(missing_ok=True)
        return None


def _s(row: dict, col: str | None) -> str:
    """Safe string extraction."""
    if not col or col not in row:
        return ""
    v = row.get(col)
    return str(v).strip() if v is not None else ""


def _detect_columns(con: duckdb.DuckDBPyConnection, view_name: str = "data") -> list[str]:
    return [
        r[0]
        for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{view_name}'"
        ).fetchall()
    ]


def _find_col(cols: list[str], candidates: list[str]) -> str | None:
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


# ============================================================
# Promoting Interoperability data (EHR vendor detection)
# ============================================================


async def ensure_pi_cached() -> bool:
    """Download CMS PI CSV and cache as Parquet. Returns True if available."""
    if _is_cache_valid(_PI_PARQUET, _PI_TTL_DAYS):
        return True

    logger.info("Downloading CMS PI file for EHR detection ...")
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(PI_URL)
            resp.raise_for_status()

        csv_path = _CACHE_DIR / "pi_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(
            csv_path, dtype=str, keep_default_na=False,
            low_memory=False, encoding_errors="replace",
        )
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_PI_PARQUET, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("PI cached: %d records -> %s", len(df), _PI_PARQUET.name)
        return True
    except Exception as e:
        logger.warning("Failed to download CMS PI file: %s", e)
        return False


def query_pi_for_ehr(
    facility_name: str = "",
    ccn: str = "",
    state: str = "",
) -> list[dict]:
    """Query PI data for EHR vendor information.

    Returns list of dicts with facility_name, ccn, ehr_developer, ehr_product_name, cehrt_id.
    """
    con = _get_con(_PI_PARQUET, "data")
    if con is None:
        return []

    try:
        cols = _detect_columns(con)

        ccn_col = _find_col(cols, ["facility_id", "ccn", "provider_id", "provider_number"])
        name_col = _find_col(cols, ["facility_name", "hospital_name", "provider_name"])
        state_col = _find_col(cols, ["state", "state_cd", "provider_state"])
        cehrt_col = _find_col(cols, ["cehrt_id", "ehr_certification_id"])
        ehr_product_col = _find_col(cols, ["ehr_product_name", "ehr_product", "product_name"])
        ehr_dev_col = _find_col(cols, ["ehr_developer", "ehr_vendor", "developer"])

        where_parts: list[str] = []
        params: list[str] = []

        if ccn and ccn_col:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        if facility_name and name_col:
            where_parts.append(f"{name_col} ILIKE ?")
            params.append(f"%{facility_name.strip()}%")
        if state and state_col:
            where_parts.append(f"UPPER(TRIM({state_col})) = ?")
            params.append(state.strip().upper())

        if not where_parts:
            return []

        where = " AND ".join(where_parts)
        sql = f"SELECT * FROM data WHERE {where} LIMIT 50"
        rows = con.execute(sql, params).fetchdf()

        results: list[dict] = []
        for _, row in rows.iterrows():
            r = row.to_dict()
            results.append({
                "facility_name": _s(r, name_col),
                "ccn": _s(r, ccn_col),
                "ehr_developer": _s(r, ehr_dev_col),
                "ehr_product_name": _s(r, ehr_product_col),
                "cehrt_id": _s(r, cehrt_col),
            })

        return results
    except Exception as e:
        logger.warning("PI EHR query failed: %s", e)
        return []
    finally:
        con.close()


def resolve_vendor_name(raw_developer: str) -> str:
    """Normalize a raw EHR developer string to a standard vendor name.

    E.g. "Epic Systems Corporation" -> "Epic Systems".
    """
    lower = raw_developer.lower()
    for keyword, canonical in VENDOR_KEYWORDS.items():
        if keyword in lower:
            return canonical
    return raw_developer


# ============================================================
# GPO directory lookup
# ============================================================


def load_gpo_directory() -> list[dict]:
    """Load the bundled GPO directory CSV.

    Returns list of dicts with gpo_name, aliases (comma-separated), gpo_type.
    """
    if not _GPO_CSV.exists():
        logger.warning("GPO directory CSV not found at %s", _GPO_CSV)
        return []
    results = []
    with open(_GPO_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            results.append({
                "gpo_name": row.get("gpo_name", "").strip(),
                "aliases": row.get("aliases", "").strip(),
                "gpo_type": row.get("gpo_type", "").strip(),
            })
    return results


def match_gpo_in_text(text: str, gpo_list: list[dict]) -> list[dict]:
    """Check if any GPO names or aliases appear in the given text.

    Returns list of matched GPO dicts.
    """
    lower = text.lower()
    matches = []
    for gpo in gpo_list:
        names_to_check = [gpo["gpo_name"]]
        if gpo["aliases"]:
            names_to_check.extend(a.strip() for a in gpo["aliases"].split(","))
        for name in names_to_check:
            if name.lower() in lower:
                matches.append(gpo)
                break
    return matches


# ============================================================
# API / page response cache
# ============================================================


def _api_cache_path(prefix: str, params: dict) -> Path:
    param_str = json.dumps(params, sort_keys=True, default=str)
    h = hashlib.sha256(param_str.encode()).hexdigest()[:16]
    return _CACHE_DIR / f"api_{prefix}_{h}.json"


def cache_response(prefix: str, params: dict, data: dict | list) -> None:
    """Save a response to the cache."""
    path = _api_cache_path(prefix, params)
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "params": params,
        "data": data,
    }
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")


def load_cached_response(prefix: str, params: dict, ttl_days: int) -> dict | list | None:
    """Load a cached response if within TTL. Returns data or None."""
    path = _api_cache_path(prefix, params)
    if not _is_cache_valid(path, ttl_days):
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload.get("data")
    except Exception as e:
        logger.warning("Failed to load cache %s: %s", path.name, e)
        return None
```

**Step 2: Verify imports**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python -c "from servers.web_intelligence import data_loaders; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add servers/web_intelligence/data_loaders.py
git commit -m "feat(web-intelligence): add data loaders (PI cache, GPO lookup, response cache)"
```

---

### Task 5: Proxycurl LinkedIn enrichment client

**Files:**
- Create: `servers/web_intelligence/proxycurl_client.py`

Optional module — returns empty results gracefully when `PROXYCURL_API_KEY` is not set.

**Step 1: Write proxycurl_client.py**

```python
"""Proxycurl API client for LinkedIn profile enrichment.

Optional — gracefully returns empty results when PROXYCURL_API_KEY is not set.
API docs: https://nubela.co/proxycurl/docs
Pricing: ~$0.01 per profile lookup.
"""

import logging
import os

import httpx

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
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                _BASE_URL,
                params={"linkedin_profile_url": linkedin_url, "use_cache": "if-recent"},
                headers={"Authorization": f"Bearer {api_key}"},
            )
            resp.raise_for_status()
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
```

**Step 2: Verify import**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python -c "from servers.web_intelligence import proxycurl_client; print(proxycurl_client.is_available())"`
Expected: `False` (no API key set)

**Step 3: Commit**

```bash
git add servers/web_intelligence/proxycurl_client.py
git commit -m "feat(web-intelligence): add optional Proxycurl LinkedIn client"
```

---

### Task 6: Static GPO directory data

**Files:**
- Create: `servers/web_intelligence/data/gpo_directory.csv`

Major hospital GPOs in the US. This is a curated reference list used by `detect_gpo_affiliation` to match search results.

**Step 1: Write gpo_directory.csv**

```csv
gpo_name,aliases,gpo_type
Vizient,"University HealthSystem Consortium,UHC,Novation,VHA Inc",national
Premier,"Premier Inc,Premier Healthcare Alliance",national
HealthTrust,"HealthTrust Performance Group,HealthTrust Purchasing Group,HPG",national
Intalere,"Amerinet,Intalere LLC",national
ROi,"Resource Optimization & Innovation,Mercy ROi",regional
Yankee Alliance,"Yankee Alliance Inc",regional
GNYHA Services,"Greater New York Hospital Association",regional
Provista,"Provista LLC",national
Conductiv,"Conductiv Inc",specialty
Med Assets,"MedAssets,Vizient (MedAssets)",legacy
```

**Step 2: Verify CSV loads**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python -c "from servers.web_intelligence.data_loaders import load_gpo_directory; gpos = load_gpo_directory(); print(f'{len(gpos)} GPOs loaded'); print(gpos[0])"`
Expected: `10 GPOs loaded` + first row dict

**Step 3: Commit**

```bash
git add servers/web_intelligence/data/gpo_directory.csv
git commit -m "feat(web-intelligence): add static GPO directory (10 major GPOs)"
```

---

### Task 7: Tool 1 — scrape_system_profile

**Files:**
- Modify: `servers/web_intelligence/server.py`

Add imports and the first tool. Uses Google CSE to find About/Mission pages, then does targeted httpx fetch + BeautifulSoup parse. Falls back to meta tags and CSE snippets if HTML parse yields thin content.

**Step 1: Update server.py imports and add tool**

Add these imports at the top of server.py (after the existing ones):

```python
import json
import logging
import os as _os
import re

import httpx
from bs4 import BeautifulSoup
from mcp.server.fastmcp import FastMCP

from . import data_loaders, search_client, proxycurl_client  # pyright: ignore[reportAttributeAccessIssue]
from .models import (
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
```

Then add the HTML fetch helper and the tool:

```python
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

    # Use simple regex — faster than full parse for just meta tags
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
                    # Thin content — try meta tags
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
```

**Step 2: Verify server boots with new imports**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && timeout 5 python -m servers.web_intelligence.server 2>&1 || true`
Expected: Clean startup (no import errors)

**Step 3: Commit**

```bash
git add servers/web_intelligence/server.py
git commit -m "feat(web-intelligence): add scrape_system_profile tool (Tool 1)"
```

---

### Task 8: Tool 2 — detect_ehr_vendor

**Files:**
- Modify: `servers/web_intelligence/server.py`

Waterfall strategy: PI_DATA → CAREER_PAGE → NEWS_MENTION.

**Step 1: Add tool to server.py**

Append after Tool 1:

```python
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
```

**Step 2: Verify server boots**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && timeout 5 python -m servers.web_intelligence.server 2>&1 || true`
Expected: Clean startup

**Step 3: Commit**

```bash
git add servers/web_intelligence/server.py
git commit -m "feat(web-intelligence): add detect_ehr_vendor tool (Tool 2)"
```

---

### Task 9: Tool 3 — get_executive_profiles

**Files:**
- Modify: `servers/web_intelligence/server.py`

Searches for leadership pages, parses executive names/titles, optionally enriches via LinkedIn CSE search + Proxycurl.

**Step 1: Add tool to server.py**

Append after Tool 2:

```python
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

            # Parse executive entries — look for common patterns:
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
```

**Step 2: Verify server boots**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && timeout 5 python -m servers.web_intelligence.server 2>&1 || true`
Expected: Clean startup

**Step 3: Commit**

```bash
git add servers/web_intelligence/server.py
git commit -m "feat(web-intelligence): add get_executive_profiles tool (Tool 3)"
```

---

### Task 10: Tool 4 — monitor_newsroom

**Files:**
- Modify: `servers/web_intelligence/server.py`

Primary: Google CSE with date restriction. Fallback: Google News RSS.

**Step 1: Add RSS parse helper and tool**

Append after Tool 3:

```python
# ---------------------------------------------------------------------------
# Google News RSS helper (fallback for monitor_newsroom)
# ---------------------------------------------------------------------------

async def _fetch_google_news_rss(query: str, days_back: int = 90) -> list[dict]:
    """Fetch Google News RSS as fallback. Returns list of {title, link, date, source, snippet}."""
    import xml.etree.ElementTree as ET
    from urllib.parse import quote

    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()

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
```

**Step 2: Verify server boots**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && timeout 5 python -m servers.web_intelligence.server 2>&1 || true`
Expected: Clean startup

**Step 3: Commit**

```bash
git add servers/web_intelligence/server.py
git commit -m "feat(web-intelligence): add monitor_newsroom tool (Tool 4)"
```

---

### Task 11: Tool 5 — detect_gpo_affiliation

**Files:**
- Modify: `servers/web_intelligence/server.py`

Google CSE search for system name + GPO keywords, match results against static GPO directory.

**Step 1: Add tool to server.py**

Append after Tool 4:

```python
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
```

**Step 2: Verify server boots**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && timeout 5 python -m servers.web_intelligence.server 2>&1 || true`
Expected: Clean startup

**Step 3: Commit**

```bash
git add servers/web_intelligence/server.py
git commit -m "feat(web-intelligence): add detect_gpo_affiliation tool (Tool 5)"
```

---

### Task 12: Docker + dependency integration

**Files:**
- Modify: `pyproject.toml`
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`

**Step 1: Add new dependencies to pyproject.toml**

Add to the `dependencies` list:
```toml
    "duckdb>=1.0.0",
    "beautifulsoup4>=4.12.0",
    "lxml>=5.0.0",
```

Note: `duckdb` was already used by servers 9-12 but missing from pyproject.toml. This fixes that gap for all servers.

**Step 2: Add packages to Dockerfile**

Add to the `pip install` line:
```dockerfile
    duckdb \
    beautifulsoup4 \
    lxml \
    rapidfuzz \
    pyarrow
```

Note: `rapidfuzz` and `pyarrow` were in pyproject.toml but missing from Dockerfile. This fixes that gap.

**Step 3: Add service to docker-compose.yml**

Add before the `volumes:` block:

```yaml
  web-intelligence:
    build: .
    command: python -m servers.web_intelligence.server
    ports:
      - "8014:8014"
    environment:
      - MCP_TRANSPORT=streamable-http
      - MCP_PORT=8014
      - GOOGLE_CSE_API_KEY=${GOOGLE_CSE_API_KEY:-}
      - GOOGLE_CSE_ID=${GOOGLE_CSE_ID:-}
      - PROXYCURL_API_KEY=${PROXYCURL_API_KEY:-}
    volumes:
      - healthcare-cache:/root/.healthcare-data-mcp/cache
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8014),5); s.close()"]
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 30s
```

**Step 4: Commit**

```bash
git add pyproject.toml Dockerfile docker-compose.yml
git commit -m "feat(web-intelligence): add Docker and MCP registration (port 8014)"
```

---

### Task 13: Final verification and Pyright check

**Files:**
- Read: All `servers/web_intelligence/*.py`

**Step 1: Run import smoke test**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp"
python -c "
from servers.web_intelligence.server import mcp
from servers.web_intelligence.models import (
    SystemProfileResponse, EhrDetectionResponse, ExecutiveProfilesResponse,
    NewsroomResponse, GpoAffiliationResponse
)
from servers.web_intelligence.data_loaders import load_gpo_directory, VENDOR_KEYWORDS
from servers.web_intelligence.search_client import search, extract_results
from servers.web_intelligence.proxycurl_client import is_available
print('All imports OK')
print(f'GPOs: {len(load_gpo_directory())}')
print(f'Vendor keywords: {len(VENDOR_KEYWORDS)}')
print(f'Proxycurl available: {is_available()}')
"
```

Expected:
```
All imports OK
GPOs: 10
Vendor keywords: 14
Proxycurl available: False
```

**Step 2: Run Pyright if available**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python -m pyright servers/web_intelligence/ 2>&1 || echo "Pyright not installed, skipping"
```

Fix any diagnostics that appear (likely just the `reportAttributeAccessIssue` on relative imports, which is already suppressed).

**Step 3: Final commit if Pyright fixes needed**

```bash
git add servers/web_intelligence/
git commit -m "fix(web-intelligence): resolve Pyright diagnostics"
```

---

## Summary

| Task | What | Files | Est. |
|------|------|-------|------|
| 1 | Scaffold + boot test | `__init__.py`, `server.py` | 2 min |
| 2 | Pydantic models | `models.py` | 3 min |
| 3 | Google CSE client | `search_client.py` | 3 min |
| 4 | Data loaders | `data_loaders.py` | 5 min |
| 5 | Proxycurl client | `proxycurl_client.py` | 3 min |
| 6 | GPO directory CSV | `data/gpo_directory.csv` | 2 min |
| 7 | Tool 1: scrape_system_profile | `server.py` | 5 min |
| 8 | Tool 2: detect_ehr_vendor | `server.py` | 5 min |
| 9 | Tool 3: get_executive_profiles | `server.py` | 5 min |
| 10 | Tool 4: monitor_newsroom | `server.py` | 5 min |
| 11 | Tool 5: detect_gpo_affiliation | `server.py` | 3 min |
| 12 | Docker + deps | `pyproject.toml`, `Dockerfile`, `docker-compose.yml` | 3 min |
| 13 | Final verification | All | 3 min |
