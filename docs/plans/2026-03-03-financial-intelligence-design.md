# Financial Intelligence MCP Server — Design Document

**Date:** 2026-03-03
**Server:** `financial-intelligence` (port 8008)
**Covers:** IRS Form 990 nonprofit financials, SEC EDGAR corporate filings, Municipal bond data

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Single server, 6 tools | Matches project convention (one domain = one server) |
| 990 search | ProPublica Nonprofit Explorer API | Free, no auth, fast REST JSON |
| 990 details | IRS e-file XML (via ProPublica filing URLs) | Full Schedule H, Part IX, compensation data |
| SEC search | EDGAR EFTS full-text search | Free, no auth, official SEC API |
| SEC details | XBRL companyfacts + HTML section parsing | Structured financials + narrative sections (MD&A, risk factors) |
| Municipal bonds | EDGAR EFTS with form=OS filter | EMMA has no public API; SEC EDGAR has Official Statement filings |
| Module structure | Three data modules | 1:1 mapping to data sources, matches health-system-profiler pattern |
| Auth | None required | All APIs are public. EDGAR requires User-Agent header only. |

## File Structure

```
servers/financial-intelligence/
├── server.py              # FastMCP + 6 tool definitions
├── models.py              # Pydantic response models
├── propublica_client.py   # ProPublica Nonprofit Explorer API
├── irs990_parser.py       # IRS 990 e-file XML download + parse
├── edgar_client.py        # EDGAR EFTS search + XBRL + HTML parse
└── __init__.py
```

Symlink: `servers/financial_intelligence -> servers/financial-intelligence`

## Tool Specifications

### Tool 1: `search_form990`

- **Params:** `query: str` (org name or EIN), `state: str = ""`, `ntee_code: str = ""`
- **Source:** ProPublica `GET /search.json?q={query}&state={state}&ntee={ntee_code}`
- **Returns:** JSON list of orgs with EIN, name, city/state, NTEE category, total revenue, total expenses, net assets (from latest filing)

### Tool 2: `get_form990_details`

- **Params:** `ein: str`
- **Source:** ProPublica `GET /organizations/{ein}.json` for filing list → download 990 XML from IRS e-file URL for latest year
- **Returns:** JSON with:
  - Revenue breakdown (contributions, program service, investment, other)
  - Functional expenses — Part IX (program services, management, fundraising)
  - Schedule H community benefit (if hospital/health system 990)
  - Officer/director compensation (name, title, compensation)
  - Program service descriptions
- **Fallback:** If XML unavailable, return ProPublica summary data only

### Tool 3: `search_sec_filings`

- **Params:** `query: str` (company name or CIK), `filing_type: str = "10-K"`, `date_from: str = ""`, `date_to: str = ""`
- **Source:** EDGAR EFTS `GET https://efts.sec.gov/LATEST/search-index?q={query}&forms={filing_type}&dateRange=custom&startdt={date_from}&enddt={date_to}`
- **Returns:** JSON list of filings with accession number, company name, CIK, filing date, form type, filing URL

### Tool 4: `get_sec_filing`

- **Params:** `accession_number: str`, `sections: list[str] = ["financials"]`
  - Valid sections: `"financials"`, `"mda"`, `"risk_factors"`, `"debt"`
- **Source:**
  - `financials` / `debt`: XBRL companyfacts from `data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json`
  - `mda` / `risk_factors`: HTML filing from EDGAR, extract sections by heading regex
- **Returns:** JSON with requested sections:
  - `financials`: revenue, net income, total assets, stockholders equity (with fiscal year)
  - `debt`: long-term debt, short-term borrowings, debt-to-equity
  - `mda`: extracted MD&A narrative text (truncated to ~2000 chars for token efficiency)
  - `risk_factors`: extracted risk factors text (truncated similarly)

### Tool 5: `search_muni_bonds`

- **Params:** `query: str` (issuer name), `state: str = ""`, `date_from: str = ""`, `date_to: str = ""`
- **Source:** EDGAR EFTS with `forms="OS"` (Official Statement) filter
- **Returns:** JSON list of municipal bond offerings with issuer, filing date, accession number, URL

### Tool 6: `get_muni_bond_details`

- **Params:** `accession_number: str`
- **Source:** EDGAR filing index for the Official Statement accession number
- **Returns:** JSON with issuer info, bond description, filing documents list, links to official statement PDF

## Data Flow

```
search_form990 ──► ProPublica REST API ──► JSON response
get_form990_details ──► ProPublica (filing list) ──► IRS e-file XML ──► parsed sections
search_sec_filings ──► EDGAR EFTS ──► JSON response
get_sec_filing ──► EDGAR XBRL (financials) + HTML (narratives) ──► merged JSON
search_muni_bonds ──► EDGAR EFTS (form=OS) ──► JSON response
get_muni_bond_details ──► EDGAR filing index ──► filing metadata + doc links
```

## External APIs

### ProPublica Nonprofit Explorer API v2

- **Base URL:** `https://projects.propublica.org/nonprofits/api/v2`
- **Auth:** None
- **Rate limits:** None documented (rate-limited on PDF downloads only)
- **Endpoints used:**
  - `GET /search.json` — search orgs by name/EIN
  - `GET /organizations/{ein}.json` — org details + filing list with XML URLs

### SEC EDGAR APIs

- **EFTS Base URL:** `https://efts.sec.gov/LATEST/search-index`
- **XBRL Base URL:** `https://data.sec.gov/api/xbrl/companyfacts/`
- **Filing Base URL:** `https://www.sec.gov/Archives/edgar/data/`
- **Auth:** `User-Agent` header required (no API key). Format: `AppName email@domain.com`
- **Rate limits:** Fair access policy, ~10 req/sec recommended
- **Endpoints used:**
  - EFTS search — full-text filing search
  - companyfacts — structured XBRL financial data
  - Filing index — HTML/document listing per accession number

### IRS 990 e-file XML

- **Source:** URLs provided by ProPublica API (point to IRS e-file S3 or direct download)
- **Auth:** None
- **Format:** XML following IRS 990 schema
- **Parsing:** ElementTree with namespace handling for revenue, expenses, Schedule H, compensation

## Caching Strategy

| Data | Cache Type | Location | TTL |
|------|-----------|----------|-----|
| ProPublica search | None (real-time) | — | — |
| ProPublica org details | In-memory | — | Session |
| IRS 990 XML files | Filesystem | `~/.healthcare-data-mcp/cache/` | Permanent (immutable filings) |
| EDGAR EFTS search | None (real-time) | — | — |
| EDGAR XBRL companyfacts | Filesystem | `~/.healthcare-data-mcp/cache/` | 24 hours |
| EDGAR HTML filings | Filesystem | `~/.healthcare-data-mcp/cache/` | Permanent (immutable filings) |

## Error Handling

Per project convention:
- Tools catch all exceptions, return `{"error": "descriptive message"}` JSON
- Data modules log warnings, return empty results on failure
- Server stays running even if one data source is unreachable

## Configuration

- **New env var:** `SEC_USER_AGENT` — User-Agent string for EDGAR requests. Default: `healthcare-data-mcp support@example.com`
- **Docker:** New service in `docker-compose.yml` on port 8008
- **Registration:** New entry in `.mcp.json` → `http://localhost:8008/mcp`
- **No API keys required**

## Pydantic Models

```python
class Form990Summary(BaseModel):
    ein: str
    name: str
    city: str
    state: str
    ntee_code: str
    total_revenue: float | None
    total_expenses: float | None
    net_assets: float | None
    tax_period: str

class Form990Details(BaseModel):
    ein: str
    name: str
    tax_period: str
    # Revenue breakdown
    contributions: float | None
    program_service_revenue: float | None
    investment_income: float | None
    other_revenue: float | None
    total_revenue: float | None
    # Expenses
    total_expenses: float | None
    program_expenses: float | None
    management_expenses: float | None
    fundraising_expenses: float | None
    # Schedule H (hospitals)
    community_benefit_total: float | None
    community_benefit_pct: float | None
    # Compensation
    officers: list[dict]  # [{name, title, compensation}]
    # Program descriptions
    program_descriptions: list[str]

class SecFiling(BaseModel):
    accession_number: str
    company_name: str
    cik: str
    form_type: str
    filing_date: str
    filing_url: str

class SecFilingDetail(BaseModel):
    accession_number: str
    company_name: str
    cik: str
    form_type: str
    filing_date: str
    financials: dict | None  # revenue, net_income, total_assets, etc.
    mda_text: str | None
    risk_factors_text: str | None
    debt_summary: dict | None

class MuniBond(BaseModel):
    accession_number: str
    issuer_name: str
    state: str
    filing_date: str
    filing_url: str

class MuniBondDetails(BaseModel):
    accession_number: str
    issuer_name: str
    filing_date: str
    documents: list[dict]  # [{name, url, type}]
    description: str
```
