# Healthcare Data MCP

> 13 MCP servers exposing 68 tools for healthcare facility analytics, quality metrics, financial intelligence, and market research -- all backed by public government data.

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![MCP 1.0](https://img.shields.io/badge/MCP-1.0-green)
![Servers](https://img.shields.io/badge/servers-13-purple)
![Tools](https://img.shields.io/badge/tools-68-orange)
![License: MIT](https://img.shields.io/badge/license-MIT-yellow)

## What is this

The [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) lets AI assistants call structured tools the same way a developer calls an API. This project packages 13 MCP servers that pull live data from CMS, Census, BLS, SEC, HRSA, and other public sources so an AI can answer questions like:

- "What's the case mix index at Jefferson Hospital?"
- "Show me 30-minute drive-time competitors for this facility."
- "Compare readmission rates across these five hospitals."
- "Who are the top employers of registered nurses in Philadelphia?"

Every tool returns structured JSON. No API keys are required for the core servers -- optional keys unlock deeper features like Census demographics, isochrone generation, and web intelligence.

## Architecture

```
MCP Client (Claude Code, VS Code, Cursor, etc.)
    |
    |  stdio / streamable-http
    |
    v
+-----------------------------------------------------+
|                   13 MCP Servers                     |
|                                                      |
|  cms-facility ---- hospital-quality ---- claims      |
|  service-area ---- geo-demographics ---- drive-time  |
|  health-system --- physician-network --- financial   |
|  price-transparency -- workforce ------- public-rec  |
|  web-intelligence                                    |
+-----------------------------------------------------+
    |                     |
    v                     v
+------------------+  +------------------+
|  Shared Layer    |  |  Cache Layer     |
|  cms_client.py   |  |  ~/.healthcare-  |
|  utils/          |  |   data-mcp/cache |
+------------------+  +------------------+
    |
    v
+--------------------------------------------------+
|          Public Data Sources                      |
|  CMS Provider Data  |  Census ACS  |  SEC EDGAR  |
|  NPPES NPI Registry |  BLS OES     |  HRSA HPSA  |
|  AHRQ Compendium    |  Dartmouth   |  OSRM       |
|  USAspending.gov    |  SAM.gov     |  ProPublica  |
|  NLRB Elections     |  HUD USPS    |  ORS         |
+--------------------------------------------------+
```

## Server Catalog

| # | Server | Tools | Data Sources | API Key |
|---|--------|-------|-------------|---------|
| 1 | **cms-facility** | `search_facilities`, `get_facility`, `search_npi`, `get_facility_financials`, `get_hospital_info` (5) | CMS Hospital General Info, NPPES NPI Registry, CMS Cost Report PUF | None |
| 2 | **service-area** | `compute_service_area`, `get_market_share`, `get_hsa_hrr_mapping`, `compare_to_dartmouth` (4) | CMS Hospital Service Area File, Dartmouth Atlas | None |
| 3 | **geo-demographics** | `get_zcta_demographics`, `get_zcta_demographics_batch`, `get_zcta_adjacency`, `get_medicare_enrollment`, `get_geographic_variation`, `crosswalk_zip` (6) | Census ACS 5-Year, TIGER/Line Shapefiles, CMS Geographic Variation PUF, HUD USPS Crosswalk | `CENSUS_API_KEY`, `HUD_API_TOKEN` |
| 4 | **drive-time** | `compute_drive_time`, `compute_drive_time_matrix`, `generate_isochrone`, `find_competing_facilities`, `compute_accessibility_score` (5) | OSRM routing engine, OpenRouteService, CMS Hospital General Info, Census Gazetteer | `ORS_API_KEY` (isochrones only) |
| 5 | **hospital-quality** | `get_quality_scores`, `get_readmission_data`, `get_safety_scores`, `get_patient_experience`, `get_financial_profile`, `compare_hospitals` (6) | CMS Hospital General Info, HRRP, HAC Reduction Program, HCAHPS, Cost Report PUF | None |
| 6 | **health-system-profiler** | `search_health_systems`, `get_system_profile`, `get_system_facilities` (3) | AHRQ Compendium, CMS Provider of Services, NPPES | None |
| 7 | **financial-intelligence** | `search_form990`, `get_form990_details`, `search_sec_filings`, `get_sec_filing`, `search_muni_bonds`, `get_muni_bond_details` (6) | IRS Form 990 via ProPublica, SEC EDGAR XBRL, Municipal Bond Official Statements | `SEC_USER_AGENT` |
| 8 | **price-transparency** | `search_mrf_index`, `get_negotiated_rates`, `compute_rate_dispersion`, `compare_rates_system`, `benchmark_rates` (5) | Hospital MRF files, CMS Physician Fee Schedule, Medicare Utilization | None |
| 9 | **physician-referral-network** | `search_physicians`, `get_physician_profile`, `map_referral_network`, `analyze_physician_mix`, `detect_leakage` (5) | NPPES, CMS Physician Compare, Medicare Utilization, DocGraph | None |
| 10 | **workforce-analytics** | `get_bls_employment`, `get_hrsa_workforce`, `get_gme_profile`, `get_residency_programs`, `search_union_activity`, `get_staffing_benchmarks`, `get_cost_report_staffing` (7) | BLS OES, HRSA HPSA, CMS HCRIS, ACGME, NLRB Elections, CMS PBJ | `BLS_API_KEY` |
| 11 | **claims-analytics** | `get_inpatient_volumes`, `get_outpatient_volumes`, `trend_service_lines`, `compute_case_mix`, `analyze_market_volumes` (5) | CMS Medicare Inpatient/Outpatient PUF | None |
| 12 | **public-records** | `search_usaspending`, `search_sam_gov`, `get_340b_status`, `get_breach_history`, `get_accreditation`, `get_interop_status` (6) | USAspending.gov, SAM.gov, HRSA 340B OPAIS, HHS OCR Breach Portal, CMS POS, CMS Promoting Interoperability | `SAM_GOV_API_KEY`, `CHPL_API_KEY` |
| 13 | **web-intelligence** | `scrape_system_profile`, `detect_ehr_vendor`, `get_executive_profiles`, `monitor_newsroom`, `detect_gpo_affiliation` (5) | Google Custom Search, CMS Promoting Interoperability, Proxycurl, Google News RSS | `GOOGLE_CSE_API_KEY`, `GOOGLE_CSE_ID`, `PROXYCURL_API_KEY` |

## Quick Start

### Option A: pip install (single server, stdio)

```bash
git clone https://github.com/Open-Informatics/healthcare-data-mcp.git
cd healthcare-data-mcp
pip install -e ".[dev]"

# Run a single server
python -m servers.cms_facility.server
```

### Option B: Docker Compose (all servers, HTTP)

```bash
git clone https://github.com/Open-Informatics/healthcare-data-mcp.git
cd healthcare-data-mcp
cp .env.example .env          # edit to add API keys
docker compose up -d
```

All servers start on ports 8002-8014 with `streamable-http` transport.

### Option C: Interactive setup

```bash
git clone https://github.com/Open-Informatics/healthcare-data-mcp.git
cd healthcare-data-mcp
bash scripts/setup.sh
```

The setup script checks prerequisites, configures API keys interactively, and reports which servers will run at full capacity.

## Configuration

Copy `.env.example` to `.env` and fill in the keys you have. Every key is optional -- servers that need a missing key will run in degraded mode or return a clear error message explaining what to register.

```bash
cp .env.example .env
```

### API Key Registration

| Key | Required By | Free? | Registration Link |
|-----|------------|-------|------------------|
| `CENSUS_API_KEY` | geo-demographics | Yes | [api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html) |
| `HUD_API_TOKEN` | geo-demographics (crosswalk) | Yes | [huduser.gov/portal/dataset/uspszip-api.html](https://www.huduser.gov/portal/dataset/uspszip-api.html) |
| `ORS_API_KEY` | drive-time (isochrones) | Yes (2,000 req/day) | [openrouteservice.org/dev/#/signup](https://openrouteservice.org/dev/#/signup) |
| `SEC_USER_AGENT` | financial-intelligence | N/A (just a header) | Set to `"YourApp your@email.com"` |
| `BLS_API_KEY` | workforce-analytics | Yes (v2, 500 req/day) | [bls.gov/developers/home.htm](https://www.bls.gov/developers/home.htm) |
| `SAM_GOV_API_KEY` | public-records (SAM.gov) | Yes | [sam.gov/content/entity-registration](https://sam.gov/content/entity-registration) |
| `CHPL_API_KEY` | public-records (interop) | Yes | [chpl.healthit.gov/#/resources/api](https://chpl.healthit.gov/#/resources/api) |
| `GOOGLE_CSE_API_KEY` | web-intelligence | Yes (100 queries/day) | [developers.google.com/custom-search](https://developers.google.com/custom-search/v1/introduction) |
| `GOOGLE_CSE_ID` | web-intelligence | Yes | Created alongside CSE API key |
| `PROXYCURL_API_KEY` | web-intelligence (LinkedIn) | Paid | [proxycurl.com](https://nubela.co/proxycurl/) |

### Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `MCP_TRANSPORT` | `stdio` | Transport mode: `stdio`, `sse`, or `streamable-http` |
| `MCP_PORT` | per-server | HTTP port when using non-stdio transport |
| `OSRM_BASE_URL` | `http://router.project-osrm.org` | OSRM routing backend (self-host for production) |

## MCP Client Setup

### Claude Code

Add to your project's `.mcp.json`:

```json
{
  "mcpServers": {
    "cms-facility": {
      "command": "python",
      "args": ["-m", "servers.cms_facility.server"],
      "cwd": "/path/to/healthcare-data-mcp"
    },
    "hospital-quality": {
      "command": "python",
      "args": ["-m", "servers.hospital_quality.server"],
      "cwd": "/path/to/healthcare-data-mcp"
    }
  }
}
```

Or with Docker (streamable-http):

```json
{
  "mcpServers": {
    "cms-facility": {
      "type": "streamable-http",
      "url": "http://localhost:8006/mcp"
    },
    "hospital-quality": {
      "type": "streamable-http",
      "url": "http://localhost:8005/mcp"
    }
  }
}
```

### VS Code (Copilot MCP)

Add to `.vscode/settings.json`:

```json
{
  "mcp.servers": {
    "cms-facility": {
      "command": "python",
      "args": ["-m", "servers.cms_facility.server"],
      "cwd": "/path/to/healthcare-data-mcp"
    }
  }
}
```

### Cursor

Add to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "cms-facility": {
      "command": "python",
      "args": ["-m", "servers.cms_facility.server"],
      "cwd": "/path/to/healthcare-data-mcp"
    }
  }
}
```

### OpenCode / Codex

Set `MCP_TRANSPORT=streamable-http` and point your client to the Docker URLs:

```
http://localhost:8006/mcp   # cms-facility
http://localhost:8002/mcp   # service-area
http://localhost:8003/mcp   # geo-demographics
http://localhost:8004/mcp   # drive-time
http://localhost:8005/mcp   # hospital-quality
http://localhost:8007/mcp   # health-system-profiler
http://localhost:8008/mcp   # financial-intelligence
http://localhost:8009/mcp   # price-transparency
http://localhost:8010/mcp   # physician-referral-network
http://localhost:8011/mcp   # workforce-analytics
http://localhost:8012/mcp   # claims-analytics
http://localhost:8013/mcp   # public-records
http://localhost:8014/mcp   # web-intelligence
```

## Development

### Project structure

```
healthcare-data-mcp/
  servers/
    cms-facility/          # Server package
      __init__.py
      server.py            # MCP tool definitions
      data_loaders.py      # Data download + cache logic
      models.py            # Pydantic response models
    service-area/
    ...                    # 13 server packages total
  shared/
    utils/
      cms_client.py        # Shared CMS data access
  tests/
  scripts/
  docker-compose.yml
  pyproject.toml
```

### Adding a new server

1. Create `servers/your-server/` with `__init__.py`, `server.py`, `models.py`, and `data_loaders.py`.
2. In `server.py`, create a `FastMCP` instance and register tools with `@mcp.tool()`.
3. Add a service entry in `docker-compose.yml` with a unique port (8015+).
4. Add any new API keys to `.env.example`.
5. Write tests in `tests/servers/your_server/`.

### Running tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

### Code style

```bash
ruff check .
ruff format .
```

### Running a single server in development

```bash
# stdio mode (default, for MCP clients)
python -m servers.cms_facility.server

# HTTP mode (for browser testing or Docker)
MCP_TRANSPORT=streamable-http MCP_PORT=8006 python -m servers.cms_facility.server
```

## Data Sources

All data comes from publicly available US government sources. No PHI, no HIPAA-covered data, no proprietary datasets.

| Source | Agency | URL |
|--------|--------|-----|
| Hospital General Information | CMS | [data.cms.gov](https://data.cms.gov/provider-data/topics/hospitals/) |
| Hospital Service Area File | CMS | [data.cms.gov](https://data.cms.gov/provider-data/dataset/Hospital-Service-Area-File) |
| Provider of Services | CMS | [data.cms.gov](https://data.cms.gov/provider-characteristics/hospital-general-information/provider-of-services-file) |
| Hospital Cost Reports (HCRIS) | CMS | [data.cms.gov](https://data.cms.gov/provider-compliance/cost-report) |
| Medicare Inpatient/Outpatient PUF | CMS | [data.cms.gov](https://data.cms.gov/provider-summary-by-type-of-service/) |
| Geographic Variation PUF | CMS | [data.cms.gov](https://data.cms.gov/summary-statistics-on-use-and-payments/) |
| HRRP Readmissions | CMS | [data.cms.gov](https://data.cms.gov/provider-data/dataset/9n3s-kdb3) |
| HAC Reduction Program | CMS | [data.cms.gov](https://data.cms.gov/provider-data/dataset/yq43-i2r4) |
| HCAHPS Patient Experience | CMS | [data.cms.gov](https://data.cms.gov/provider-data/dataset/dgck-syfz) |
| Promoting Interoperability | CMS | [data.cms.gov](https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability) |
| NPPES NPI Registry | CMS | [npiregistry.cms.hhs.gov](https://npiregistry.cms.hhs.gov/) |
| Physician Compare | CMS | [data.cms.gov](https://data.cms.gov/provider-data/topics/doctors-clinicians) |
| Medicare Physician Utilization | CMS | [data.cms.gov](https://data.cms.gov/provider-summary-by-type-of-service/) |
| Physician Fee Schedule RVUs | CMS | [cms.gov/medicare/payment/fee-schedules](https://www.cms.gov/medicare/payment/fee-schedules/physician) |
| Payroll-Based Journal (PBJ) | CMS | [data.cms.gov](https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing) |
| AHRQ Compendium of US Health Systems | AHRQ | [ahrq.gov](https://www.ahrq.gov/chsp/data-resources/compendium.html) |
| American Community Survey (ACS) | Census | [census.gov](https://www.census.gov/data/developers/data-sets/acs-5year.html) |
| TIGER/Line Shapefiles | Census | [census.gov](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html) |
| Census Gazetteer | Census | [census.gov](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html) |
| Dartmouth Atlas | Dartmouth | [dartmouthatlas.org](https://data.dartmouthatlas.org/) |
| OSRM (Open Source Routing Machine) | OSM community | [project-osrm.org](https://project-osrm.org/) |
| OpenRouteService | HeiGIT | [openrouteservice.org](https://openrouteservice.org/) |
| OES Employment Statistics | BLS | [bls.gov/oes](https://www.bls.gov/oes/) |
| NLRB Union Elections | NLRB | [nlrb.gov](https://www.nlrb.gov/search/case) |
| BLS Work Stoppages | BLS | [bls.gov/wsp](https://www.bls.gov/wsp/) |
| HPSA Shortage Areas | HRSA | [data.hrsa.gov](https://data.hrsa.gov/topics/health-workforce/shortage-areas) |
| ACGME Data Resource Book | ACGME | [acgme.org](https://www.acgme.org/about-us/publications-and-resources/graduate-medical-education-data-resource-book/) |
| IRS Form 990 (via ProPublica) | IRS / ProPublica | [projects.propublica.org/nonprofits](https://projects.propublica.org/nonprofits/) |
| SEC EDGAR | SEC | [sec.gov/edgar](https://www.sec.gov/edgar/) |
| USAspending.gov | Treasury | [usaspending.gov](https://www.usaspending.gov/) |
| SAM.gov Opportunities | GSA | [sam.gov](https://sam.gov/) |
| 340B Drug Pricing Program | HRSA | [340bopais.hrsa.gov](https://340bopais.hrsa.gov/) |
| HIPAA Breach Portal | HHS OCR | [ocrportal.hhs.gov](https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf) |
| ONC CHPL | ONC | [chpl.healthit.gov](https://chpl.healthit.gov/) |
| HUD USPS Crosswalk | HUD | [huduser.gov](https://www.huduser.gov/portal/dataset/uspszip-api.html) |

## License

MIT
