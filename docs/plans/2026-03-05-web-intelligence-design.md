# Web Intelligence & OSINT — Design Document

**Server:** 13 of 13 | **Port:** 8014 | **Name:** `web-intelligence`

## Overview

OSINT server for health system competitive intelligence. Unlike servers 1–12 which query structured datasets, this server uses **search engine APIs for discovery** with **targeted HTML fetches** for enrichment. No full-site crawling — every tool is built around Google Custom Search API as the primary discovery mechanism, with optional page-level fetches for specific known URLs.

## 5 Tools

| # | Tool | Primary Source | Fallback | Cache TTL |
|---|------|---------------|----------|-----------|
| 1 | `scrape_system_profile` | Google CSE scoped to system domain → targeted fetch of About/Locations pages | CSE snippets + meta/og tags if BS4 parse is empty | 30 days |
| 2 | `detect_ehr_vendor` | CMS Promoting Interoperability Parquet (self-contained copy) | Google CSE career page search for vendor keywords | 90 days (PI) / 30 days (search) |
| 3 | `get_executive_profiles` | Google CSE scoped to system domain → targeted fetch of leadership page | CSE snippets with titles extracted; optional Proxycurl enrichment for LinkedIn data | 90 days |
| 4 | `monitor_newsroom` | Google CSE with `&tbm=nws` (news search) | Google News RSS as fallback | 7 days |
| 5 | `detect_gpo_affiliation` | Google CSE for system + GPO keywords, matched against static `gpo_directory.csv` | None — search-only | 30 days |

## Architecture

```
servers/web_intelligence/
├── __init__.py
├── server.py              # FastMCP + 5 tools
├── models.py              # Pydantic response models
├── data_loaders.py        # PI Parquet cache, GPO lookup, parsed-page cache
├── search_client.py       # Google Custom Search API wrapper
├── proxycurl_client.py    # Optional LinkedIn enrichment via Proxycurl API
└── data/
    └── gpo_directory.csv  # Major GPOs: Vizient, Premier, HealthTrust, etc.
```

### Module Responsibilities

**server.py** — Standard FastMCP boilerplate. Each tool orchestrates: search → fetch → parse → cache → respond. All tools async, return `json.dumps()`, wrap in try/except.

**search_client.py** — Single Google Custom Search API wrapper. All 5 tools route through this. Handles:
- Standard web search (tools 1, 2, 3, 5)
- News search via `searchType` / `tbm=nws` parameter (tool 4)
- Site-scoped search via `siteSearch` parameter (tools 1, 3)
- LinkedIn profile search via `site:linkedin.com/in/` query (tool 3)

**proxycurl_client.py** — Optional Proxycurl API client for LinkedIn profile enrichment. Returns structured data (name, title, company, tenure, education) when `PROXYCURL_API_KEY` is set. Graceful no-op when key is absent — tool still works via CSE snippets.

**data_loaders.py** — Three data management concerns:
1. CMS Promoting Interoperability CSV → Parquet cache + DuckDB query (for `detect_ehr_vendor`)
2. GPO directory CSV lookup (for `detect_gpo_affiliation`)
3. Parsed page cache (SHA256-keyed JSON files for fetched/parsed HTML)

**models.py** — Pydantic response models. All fields defaulted. Notable additions vs. other servers: `source_url` on executive profiles and GPO matches, `confidence` level on EHR detection, `evidence_url` on GPO affiliation.

## Tool Specifications

### Tool 1: scrape_system_profile

**Purpose:** Extract mission, vision, leadership summary, and locations from a health system's website.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `system_name` | str | Yes | Health system name (e.g., "Intermountain Health") |
| `system_domain` | str | No | Primary website domain (e.g., "intermountainhealth.org"). If omitted, discovered via CSE. |

**Strategy:**
1. Google CSE scoped to `system_domain` for "about us" OR "mission" OR "our story"
2. Fetch top 1-2 result URLs with `httpx`
3. Parse with BeautifulSoup — extract `<h1>`-`<h3>` headings, paragraph text, structured lists
4. **Fallback:** If BS4 parse yields suspiciously little content (< 100 chars of text), fall back to:
   - `og:description` and `<meta name="description">` tags from raw HTML (almost always server-rendered even on SPA sites)
   - CSE snippet text as-is
5. Repeat for "locations" OR "find a location" to extract location data

**Returns:** `SystemProfileResponse` — mission, vision, values, location_count, locations list, source_urls

### Tool 2: detect_ehr_vendor

**Purpose:** Identify the EHR vendor for a health system or facility.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `system_name` | str | Yes | Health system or facility name |
| `ccn` | str | No | CMS Certification Number for precise PI lookup |
| `state` | str | No | State filter for PI data disambiguation |

**Strategy (waterfall with confidence levels):**
1. **PI_DATA (authoritative):** Query local Promoting Interoperability Parquet by CCN or facility name. CEHRT ID → vendor via static mapping dict (~10 entries: Epic, Oracle Health, MEDITECH, Altera, athenahealth, etc.).
2. **CAREER_PAGE (inferred):** Google CSE for `site:careers.{domain} "Epic" OR "Cerner" OR "MEDITECH" OR "Oracle Health"`. Search snippets alone usually reveal the vendor — no page fetch needed.
3. **NEWS_MENTION (weak signal):** Google CSE for `"{system_name}" EHR OR "electronic health record"` in news results.

**Returns:** `EhrDetectionResponse` — vendor_name, confidence ("PI_DATA" | "CAREER_PAGE" | "NEWS_MENTION"), evidence_summary, source_url

### Tool 3: get_executive_profiles

**Purpose:** Pull executive bios, titles, and tenure from official sites and LinkedIn.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `system_name` | str | Yes | Health system name |
| `system_domain` | str | No | Website domain for site-scoped search |
| `include_linkedin` | bool | No | Enable LinkedIn enrichment (default true) |
| `max_results` | int | No | Max executives to return (default 20) |

**Strategy:**
1. Google CSE scoped to `system_domain` for "leadership" OR "executive team" OR "board of directors"
2. Fetch the leadership page, parse executive entries (name, title, bio snippet)
3. If `include_linkedin` and names found:
   - Google CSE: `site:linkedin.com/in/ "Name" "System Name"` to find LinkedIn profile URLs
   - If `PROXYCURL_API_KEY` set: enrich top results with Proxycurl (full title, tenure, education)
   - If no key: return LinkedIn URL + whatever Google's snippet contains

**Returns:** `ExecutiveProfilesResponse` — list of `ExecutiveProfile` (name, title, bio_snippet, source_url, linkedin_url, linkedin_data if enriched)

### Tool 4: monitor_newsroom

**Purpose:** Retrieve recent press releases and news mentions for a health system.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `system_name` | str | Yes | Health system name |
| `days_back` | int | No | How many days of news to retrieve (default 90, max 365) |
| `max_results` | int | No | Max news items (default 25, max 100) |

**Strategy:**
1. **Primary:** Google CSE with news search parameters (`tbm=nws` or `searchType` equivalent) for `"{system_name}"`, date-restricted to `days_back`
2. **Fallback:** Google News RSS (`https://news.google.com/rss/search?q=...&when={days}d`) — parse XML for headline, date, source, snippet
3. Deduplicate by headline similarity (fuzzy match)

**Returns:** `NewsroomResponse` — list of `NewsItem` (headline, source, date, snippet, url), total_count

### Tool 5: detect_gpo_affiliation

**Purpose:** Match a health system to its Group Purchasing Organization partners.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `system_name` | str | Yes | Health system name |

**Strategy:**
1. Load static `gpo_directory.csv` (columns: gpo_name, gpo_type, common_aliases)
2. Google CSE: `"{system_name}" GPO OR "group purchasing" OR "Vizient" OR "Premier" OR "HealthTrust"`
3. Match CSE snippet text against known GPO names/aliases
4. Each match includes the `evidence_url` (the search result URL where the match was found)

**Returns:** `GpoAffiliationResponse` — list of `GpoMatch` (gpo_name, confidence, evidence_snippet, evidence_url), search_terms_used

## Data Sources & Caching

| Dataset | Source | Cache Path | Format | TTL |
|---------|--------|-----------|--------|-----|
| Promoting Interoperability | CMS data.cms.gov CSV | `cache/web-intelligence/pi_data.parquet` | Parquet (zstd) | 90 days |
| GPO Directory | Bundled static CSV | `data/gpo_directory.csv` | CSV (in-repo) | N/A |
| CEHRT → Vendor Map | Static dict in code | N/A | Python dict | N/A |
| CSE Search Results | Google Custom Search API | `cache/web-intelligence/api_cse_{hash}.json` | JSON | 7-30 days (per tool) |
| Parsed Pages | httpx fetch + BS4 | `cache/web-intelligence/page_{hash}.json` | JSON | 30-90 days |
| Proxycurl Profiles | Proxycurl API | `cache/web-intelligence/api_proxycurl_{hash}.json` | JSON | 90 days |

### Cache Strategy

- **Location:** `~/.healthcare-data-mcp/cache/web-intelligence/`
- **Bulk data (PI):** Parquet with zstd, auto-downloaded on first EHR detection call
- **API responses:** SHA256-keyed JSON files (key = sorted params hash), per-tool TTLs
- **Parsed pages:** SHA256-keyed JSON (key = URL hash), includes raw text + extracted structured data

## Cost Model

Google Custom Search API: **100 free queries/day**, then **$5 per 1,000 queries**.

Typical per-system query cost:
| Tool | CSE Queries | Notes |
|------|-------------|-------|
| scrape_system_profile | 1-2 | About + Locations search |
| detect_ehr_vendor | 0-2 | 0 if PI data covers it, 1-2 for career/news fallback |
| get_executive_profiles | 1-3 | Leadership page + 1-2 LinkedIn searches |
| monitor_newsroom | 1 | Single news search |
| detect_gpo_affiliation | 1 | Single GPO search |

**Per system: ~4-9 CSE queries.** A batch of 10 health systems ≈ 40-90 queries ≈ **$0.20-$0.45** beyond free tier. A batch of 50 systems ≈ **$1.25-$2.25**.

Proxycurl (optional): **~$0.01/profile**. Enriching 10 executives per system × 10 systems = **$1.00**.

## Error Handling

| Scenario | Response |
|----------|----------|
| `GOOGLE_CSE_API_KEY` not set | `{"error": "GOOGLE_CSE_API_KEY not set", "instructions": "..."}` |
| `GOOGLE_CSE_ID` not set | `{"error": "GOOGLE_CSE_ID not set", "instructions": "..."}` |
| CSE daily quota exceeded | `{"error": "Google CSE quota exceeded", "cached_results": [...]}` — return stale cache if available |
| CSE returns 0 results | Tool returns empty result set, no error |
| Page fetch returns empty/JS-only content | Fallback to meta tags + CSE snippets |
| Page fetch 403/429/timeout | Skip page fetch, use CSE snippets only |
| `PROXYCURL_API_KEY` not set | LinkedIn enrichment silently skipped, tool still returns CSE-based results |
| Proxycurl rate limit / error | Log warning, return CSE-based LinkedIn data only |
| PI data not available for facility | Fall through to CSE-based EHR detection |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GOOGLE_CSE_API_KEY` | Yes | Google Custom Search JSON API key |
| `GOOGLE_CSE_ID` | Yes | Programmable Search Engine ID (cx parameter) |
| `PROXYCURL_API_KEY` | No | Proxycurl API key for LinkedIn profile enrichment (~$0.01/profile) |

## New Dependencies

| Package | Purpose |
|---------|---------|
| `beautifulsoup4` | HTML parsing for targeted page extraction (tools 1, 3) |
| `lxml` | Fast HTML parser backend for BeautifulSoup |

Both must be added to `pyproject.toml` and `Dockerfile`.

## Docker Integration

```yaml
# docker-compose.yml addition
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

```json
// .mcp.json addition
{
  "mcpServers": {
    "web-intelligence": {
      "command": "python",
      "args": ["-m", "servers.web_intelligence.server"],
      "cwd": "/path/to/healthcare-data-mcp",
      "env": {
        "GOOGLE_CSE_API_KEY": "your-key",
        "GOOGLE_CSE_ID": "your-cx-id",
        "PROXYCURL_API_KEY": "optional-key"
      }
    }
  }
}
```
