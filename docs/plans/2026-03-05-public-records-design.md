# Public Records & Regulatory Server — Design Document

**Server:** public-records (port 8013)
**Covers:** TOC §6 (Technology/Cyber), §7 (Operations, Supply Chain, Real Estate)

## Goal

Provide programmatic access to public regulatory, accreditation, federal spending, and compliance data for healthcare organizations through 6 MCP tools.

## Architecture

```
servers/public_records/              # physical directory (no symlink)
├── __init__.py
├── server.py                        # FastMCP, port 8013, 6 tools
├── models.py                        # Pydantic response models
├── data_loaders.py                  # Bulk CSV download/cache (CMS POS, CMS PI)
├── usaspending_client.py            # USAspending.gov API client
├── sam_client.py                    # SAM.gov Opportunities API client
├── data/
│   └── accreditation_codes.csv      # Static lookup: ACRDTN_TYPE_CD → org name
```

**Key design decisions:**
- Physical dir is `public_records` (underscore, valid Python module name). No symlink needed.
- Lazy-load on first tool call (consistent with all other servers).
- Two data access patterns: (A) auto-download bulk files, (B) manual-seed files from portals without APIs.

## Data Sources & Access Patterns

### Pattern A: Auto-Download (fully automated)

| Dataset | Source | Format | Size | TTL |
|---------|--------|--------|------|-----|
| CMS Provider of Services | data.cms.gov | CSV → Parquet | ~156 MB | 90 days |
| CMS Promoting Interoperability | data.cms.gov | CSV → Parquet | ~5 MB | 90 days |
| USAspending awards | api.usaspending.gov | JSON (per-query) | ~50 KB/query | 7 days |
| SAM.gov opportunities | api.sam.gov | JSON (per-query) | ~50 KB/query | 7 days |

### Pattern B: Manual-Seed (user downloads once, server caches + queries)

| Dataset | Source | User Action | Cache Format | TTL |
|---------|--------|-------------|-------------|-----|
| 340B Covered Entities | 340bopais.hrsa.gov/Reports | Download JSON from Reports page | JSON → Parquet | 90 days |
| HIPAA Breach Reports | ocrportal.hhs.gov | Export CSV from search results | CSV → Parquet | 90 days |

For manual-seed tools, when data file is missing the tool returns a structured error with download instructions (URL, steps).

## Tool Specifications

### Tool 1: `search_usaspending`

Federal spending awarded to a health system.

**Source:** USAspending.gov API (fully open, no auth)
**Endpoint:** `POST https://api.usaspending.gov/api/v2/search/spending_by_award/`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| recipient_name | str | yes | Health system or hospital name |
| award_type | str | no | "contracts", "grants", "direct_payments", or "" for all |
| fiscal_year | str | no | e.g. "2024". Default: current FY |
| limit | int | no | Max results (default 25, max 100) |

**Returns:** List of awards with: award_id, recipient_name, awarding_agency, total_obligation, description, start_date, end_date, award_type.

### Tool 2: `search_sam_gov`

Federal contract opportunities and solicitations for a health system.

**Source:** SAM.gov Opportunities API (requires API key via `SAM_GOV_API_KEY` env var)
**Endpoint:** `GET https://api.sam.gov/prod/opportunities/v2/search`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| keyword | str | yes | Search keyword (organization name, service type) |
| posted_from | str | no | Start date (MM/DD/YYYY). Default: 1 year ago |
| posted_to | str | no | End date. Default: today |
| ptype | str | no | Procurement type: "o" (solicitation), "p" (presolicitation), etc. |
| limit | int | no | Max results (default 25) |

**Returns:** List of opportunities with: notice_id, title, solicitation_number, department, sub_tier, posted_date, response_deadline, naics_code, set_aside_type, description.

**Auth:** Returns error with registration instructions if `SAM_GOV_API_KEY` not set.

### Tool 3: `get_340b_status`

340B Drug Pricing Program enrollment and contract pharmacy data.

**Source:** HRSA 340B OPAIS daily JSON export (manual-seed)
**Cache file:** `~/.healthcare-data-mcp/cache/public-records/340b_covered_entities.json`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_name | str | no* | Search by covered entity name |
| state | str | no | Filter by state abbreviation |
| entity_id | str | no* | Search by 340B ID |

*At least one of entity_name or entity_id required.

**Returns:** List of covered entities with: entity_id, entity_name, entity_type, address, city, state, zip, grant_number, participating_status, contract_pharmacy_count.

**When data missing:** Returns `{"error": "340B data not found", "instructions": "Download JSON from https://340bopais.hrsa.gov/Reports → Covered Entity Daily Export (JSON). Place file at ~/.healthcare-data-mcp/cache/public-records/340b_covered_entities.json"}`.

### Tool 4: `get_breach_history`

HIPAA breach reports for an organization.

**Source:** HHS OCR Breach Portal (manual-seed CSV)
**Cache file:** `~/.healthcare-data-mcp/cache/public-records/hipaa_breaches.csv`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_name | str | yes | Organization name to search |
| state | str | no | Filter by state |
| min_individuals | int | no | Minimum individuals affected (default 0) |

**Returns:** List of breaches with: entity_name, state, covered_entity_type, individuals_affected, breach_submission_date, breach_type, location_of_breached_info, business_associate_present, web_description.

**When data missing:** Returns error with instructions to export CSV from https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf.

### Tool 5: `get_accreditation`

Accreditation and certification status for a hospital.

**Source:** CMS Provider of Services (POS) file (auto-download)
**URL:** `https://data.cms.gov/sites/default/files/2026-01/.../Hospital_and_other.DATA.Q4_2025.csv`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| ccn | str | no* | CMS Certification Number (6-digit) |
| provider_name | str | no* | Search by name (ILIKE match) |
| state | str | no | Filter by state |

*At least one of ccn or provider_name required.

**Returns:** List of providers with: ccn, provider_name, state, city, accreditation_org (decoded from ACRDTN_TYPE_CD), accreditation_effective_date, accreditation_expiration_date, certification_date, ownership_type, bed_count, medicare_medicaid_participation.

**Static data:** `accreditation_codes.csv` maps ACRDTN_TYPE_CD numeric codes to organization names (The Joint Commission, DNV GL, HFAP/CIHQ, etc.).

### Tool 6: `get_interop_status`

Promoting Interoperability attestation and EHR certification for a hospital.

**Source:** CMS Promoting Interoperability CSV (auto-download) + ONC CHPL API (optional)
**PI CSV:** `https://data.cms.gov/provider-data/sites/default/files/.../Promoting_Interoperability-Hospital.csv`
**CHPL API:** `https://chpl.healthit.gov/rest/certification_ids/{cehrt_id}` (requires `CHPL_API_KEY` env var)

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| ccn | str | no* | CMS Certification Number |
| facility_name | str | no* | Search by name (ILIKE match) |
| state | str | no | Filter by state |

*At least one of ccn or facility_name required.

**Returns:** facility_name, ccn, state, meets_pi_criteria (Y/N), cehrt_id, reporting_period_start, reporting_period_end. If CHPL API key available: ehr_product_name, ehr_developer.

## Caching Strategy

All caches stored at `~/.healthcare-data-mcp/cache/public-records/`.

| File | Source | Auto-download? | TTL |
|------|--------|---------------|-----|
| `pos_q4_2025.parquet` | CMS POS CSV | Yes | 90 days |
| `promoting_interop.parquet` | CMS PI CSV | Yes | 90 days |
| `340b_covered_entities.parquet` | HRSA JSON (manual seed) | No | 90 days |
| `hipaa_breaches.parquet` | OCR CSV (manual seed) | No | 90 days |
| `usaspending_{hash}.json` | USAspending API | Yes (per-query) | 7 days |
| `sam_{hash}.json` | SAM.gov API | Yes (per-query) | 7 days |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SAM_GOV_API_KEY` | For search_sam_gov | SAM.gov public API key (free registration at sam.gov) |
| `CHPL_API_KEY` | Optional | ONC CHPL API key for EHR product lookup in get_interop_status |
| `MCP_TRANSPORT` | No | "stdio" (default) or "streamable-http" |
| `MCP_PORT` | No | Default 8013 |

## Docker Integration

```yaml
public-records:
  build: .
  command: python -m servers.public_records.server
  ports:
    - "8013:8013"
  environment:
    - MCP_TRANSPORT=streamable-http
    - MCP_PORT=8013
    - SAM_GOV_API_KEY=${SAM_GOV_API_KEY:-}
    - CHPL_API_KEY=${CHPL_API_KEY:-}
  volumes:
    - healthcare-cache:/root/.healthcare-data-mcp/cache
  restart: unless-stopped
  healthcheck:
    test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8013),5); s.close()"]
    interval: 60s
    timeout: 10s
    retries: 3
    start_period: 30s
```

**.mcp.json entry:**
```json
"public-records": {
  "type": "http",
  "url": "http://localhost:8013/mcp"
}
```
