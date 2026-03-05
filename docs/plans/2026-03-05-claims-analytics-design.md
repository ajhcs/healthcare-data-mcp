# Claims & Service Line Analytics — Design Document

**Server:** 11 of N | **Port:** 8012 | **Name:** `claims-analytics`

## Overview

MCP server providing claims-based hospital analytics: inpatient discharge volumes by DRG and service line, outpatient procedure volumes by APC, multi-year volume trends, case mix index computation, and geographic market share analysis. All data sourced from CMS Medicare Provider Utilization PUFs (public bulk CSV downloads), with static bundled DRG-to-service-line mappings and IPPS Final Rule DRG weights.

## 5 Tools

| # | Tool | Data Sources | Access Pattern |
|---|------|-------------|----------------|
| 1 | `get_inpatient_volumes` | CMS Inpatient PUF (by Provider and Service) | Bulk CSV → Parquet cache |
| 2 | `get_outpatient_volumes` | CMS Outpatient PUF (by Provider and Service) | Bulk CSV → Parquet cache |
| 3 | `trend_service_lines` | Historical Inpatient + Outpatient PUFs (3 years) | Multi-year Parquet cache |
| 4 | `compute_case_mix` | Inpatient PUF + bundled IPPS DRG weights | Parquet cache + static CSV |
| 5 | `analyze_market_volumes` | Inpatient + Outpatient PUFs | Parquet cache |

## Architecture

```
servers/claims-analytics/
├── __init__.py
├── server.py              # FastMCP port 8012, 5 tools
├── models.py              # Pydantic response models
├── data_loaders.py        # Bulk download/cache for MedPAR + Outpatient PUFs
├── service_lines.py       # DRG→service-line mapping + case mix computation
└── data/
    ├── drg_service_line_map.csv   # Curated DRG→service-line mapping (~800 rows)
    └── drg_weights_fy2024.csv     # IPPS Final Rule MS-DRG relative weights (~1,000 rows)
```

Symlink: `servers/claims-analytics/` ↔ `servers/claims_analytics/`

### Module Responsibilities

**data_loaders.py** — Bulk dataset management for both inpatient and outpatient PUFs:
- Downloads CSV files from data.cms.gov for 3 discharge years (DY21, DY22, DY23)
- Converts to Parquet with zstd compression, caches at `~/.healthcare-data-mcp/cache/claims-analytics/`
- TTL-based invalidation (90 days)
- DuckDB queries on cached Parquet for all filtering/aggregation
- Handles inconsistent column naming across years

**service_lines.py** — Service line classification and case mix:
- Loads bundled `drg_service_line_map.csv` for DRG→service-line mapping
- Loads bundled `drg_weights_fy2024.csv` for CMI computation
- Provides `map_drg_to_service_line()` and `compute_cmi()` functions

**models.py** — Pydantic response models for all 5 tools.

**server.py** — Standard FastMCP boilerplate (port 8012, streamable-http transport).

## Data Sources & Caching

### Inpatient PUF (Medicare Inpatient Hospitals — by Provider and Service)

| Year | Source URL | Cache Path |
|------|-----------|------------|
| DY23 | `https://data.cms.gov/sites/default/files/2025-05/ca1c9013-8c7c-4560-a4a1-28cf7e43ccc8/MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV` | `inpatient_dy23.parquet` |
| DY22 | Discoverable via data.cms.gov catalog (same pattern, RY24) | `inpatient_dy22.parquet` |
| DY21 | Discoverable via data.cms.gov catalog (same pattern, RY23) | `inpatient_dy21.parquet` |

**Key columns:** `Rndrng_Prvdr_CCN`, `Rndrng_Prvdr_Org_Name`, `Rndrng_Prvdr_State_Abrvtn`, `DRG_Cd`, `DRG_Desc`, `Tot_Dschrgs`, `Avg_Submtd_Chrgs`, `Avg_Tot_Pymt_Amt`, `Avg_Mdcr_Pymt_Amt`

**Approx size:** 30-50MB per year CSV → ~10-20MB Parquet each

### Outpatient PUF (Medicare Outpatient Hospitals — by Provider and Service)

| Year | Source URL | Cache Path |
|------|-----------|------------|
| DY23 | `https://data.cms.gov/sites/default/files/2025-08/bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv` | `outpatient_dy23.parquet` |
| DY22 | Discoverable via data.cms.gov catalog (same pattern, RY24) | `outpatient_dy22.parquet` |
| DY21 | Discoverable via data.cms.gov catalog (same pattern, RY23) | `outpatient_dy21.parquet` |

**Key columns:** `Rndrng_Prvdr_CCN`, `Rndrng_Prvdr_Org_Name`, `Rndrng_Prvdr_State_Abrvtn`, `APC_Cd`, `APC_Desc`, `Outptnt_Srvcs`, `Avg_Submtd_Chrgs`, `Avg_Tot_Pymt_Amt`, `Avg_Mdcr_Pymt_Amt`

**Approx size:** 20-40MB per year CSV → ~8-15MB Parquet each

### Cache Strategy

- **Location:** `~/.healthcare-data-mcp/cache/claims-analytics/`
- **Format:** Parquet with zstd compression
- **TTL:** 90 days (CMS updates annually, generous TTL)
- **Download:** `httpx.AsyncClient(timeout=300, follow_redirects=True)`
- **Pattern:** `ensure_inpatient_cached(year)` / `ensure_outpatient_cached(year)` called by each tool before querying

### Static Data Files

**`data/drg_service_line_map.csv`** — Curated mapping of ~800 MS-DRGs to ~15-20 service lines:

```csv
drg_code,drg_description,mdc,service_line
001,Heart Transplant or Implant of Heart Assist System w MCC,05,Cardiovascular
039,Extracranial Procedures w/o CC/MCC,01,Neurosciences
470,Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity w/o MCC,08,Orthopedics
```

Service line categories: Cardiovascular, Orthopedics, Neurosciences, Oncology, General Surgery, Pulmonary, Gastroenterology, Renal, Women's Health, Neonatal, Behavioral Health, Trauma, Transplant, Rehabilitation, Other.

**`data/drg_weights_fy2024.csv`** — IPPS Final Rule MS-DRG relative weights:

```csv
drg_code,drg_description,weight,geometric_mean_los,arithmetic_mean_los
001,Heart Transplant or Implant of Heart Assist System w MCC,25.4988,31.4,39.5
470,Major Hip and Knee Joint Replacement or Reattachment of Lower Extremity w/o MCC,1.7394,1.9,2.3
```

Source: [CMS IPPS FY2024 Final Rule](https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/fy-2024-ipps-final-rule-home-page), Table 5.

## Tool Signatures

### Tool 1: get_inpatient_volumes

```
Input:
  ccn: str               # CMS Certification Number (required)
  drg_code: str = ""     # Filter to specific DRG (e.g. "470")
  service_line: str = "" # Filter to service line (e.g. "Cardiovascular")
  year: str = ""         # Discharge year (default: latest available)

Output: InpatientVolumesResponse
  ccn: str
  provider_name: str
  state: str
  year: str
  total_discharges: int
  total_drgs: int
  service_line_summary: list[ServiceLineSummary]
    service_line: str
    discharges: int
    pct_of_total: float
    avg_charges: float
    avg_medicare_payment: float
  drg_details: list[DRGDetail]
    drg_code: str
    drg_description: str
    service_line: str
    discharges: int
    avg_charges: float
    avg_total_payment: float
    avg_medicare_payment: float
```

### Tool 2: get_outpatient_volumes

```
Input:
  ccn: str               # CMS Certification Number (required)
  apc_code: str = ""     # Filter to specific APC
  year: str = ""         # Discharge year (default: latest available)

Output: OutpatientVolumesResponse
  ccn: str
  provider_name: str
  state: str
  year: str
  total_services: int
  total_apcs: int
  apc_details: list[APCDetail]
    apc_code: str
    apc_description: str
    services: int
    avg_charges: float
    avg_total_payment: float
    avg_medicare_payment: float
```

### Tool 3: trend_service_lines

```
Input:
  ccn: str                       # CMS Certification Number (required)
  service_line: str = ""         # Filter to one service line
  include_outpatient: bool = True

Output: ServiceLineTrendResponse
  ccn: str
  provider_name: str
  years: list[str]               # e.g. ["2021", "2022", "2023"]
  inpatient_trends: list[ServiceLineTrend]
    service_line: str
    volumes_by_year: dict[str, int]        # {"2021": 500, "2022": 520, "2023": 540}
    yoy_change_pct: dict[str, float]       # {"2022": 4.0, "2023": 3.8}
    cagr_pct: float                        # Compound annual growth rate
  outpatient_trends: list[OutpatientTrend] | None
    apc_code: str
    apc_description: str
    volumes_by_year: dict[str, int]
    yoy_change_pct: dict[str, float]
    cagr_pct: float
```

### Tool 4: compute_case_mix

```
Input:
  ccn: str               # CMS Certification Number (required)
  year: str = ""         # Discharge year (default: latest)

Output: CaseMixResponse
  ccn: str
  provider_name: str
  year: str
  case_mix_index: float                    # Weighted average DRG weight
  total_discharges: int
  service_line_acuity: list[ServiceLineAcuity]
    service_line: str
    discharges: int
    avg_drg_weight: float                  # Service-line-specific CMI
    pct_of_total_weight: float
  top_drgs_by_weight: list[DRGWeightContribution]
    drg_code: str
    drg_description: str
    service_line: str
    discharges: int
    drg_weight: float
    total_weight_contribution: float       # discharges × drg_weight
    pct_of_total_weight: float
```

### Tool 5: analyze_market_volumes

```
Input:
  provider_ccns: list[str]       # CCNs of providers in market area (required)
  service_line: str = ""         # Filter to one service line
  year: str = ""                 # Default: latest

Output: MarketVolumesResponse
  year: str
  total_market_discharges: int
  total_providers: int
  provider_shares: list[ProviderMarketShare]
    ccn: str
    provider_name: str
    state: str
    total_discharges: int
    market_share_pct: float
    service_line_breakdown: list[ServiceLineShare]
      service_line: str
      discharges: int
      market_share_pct: float              # Share of this SL in market
  service_line_totals: list[ServiceLineMarketTotal]
    service_line: str
    total_discharges: int
    pct_of_market: float
    top_provider_ccn: str
    top_provider_name: str
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MCP_TRANSPORT` | No | Transport type (default: `stdio`, Docker: `streamable-http`) |
| `MCP_PORT` | No | Port number (default: `8012`) |

No API keys needed — all CMS PUF data is freely downloadable.

## Error Handling

| Scenario | Response |
|----------|----------|
| Invalid/unknown CCN | `{"error": "No data found for provider: {ccn}"}` |
| Data download fails | `{"error": "Failed to download {dataset}: {details}"}` |
| DRG not in weight table | Use weight of 1.0 as fallback |
| Empty results for filter | Return response with empty arrays, totals = 0 |
| Parquet cache corrupted | Delete and re-download on next request |

## Docker Integration

```yaml
# Addition to docker-compose.yml
claims-analytics:
  build: .
  command: python -m servers.claims_analytics.server
  ports:
    - "8012:8012"
  environment:
    - MCP_TRANSPORT=streamable-http
    - MCP_PORT=8012
  volumes:
    - healthcare-cache:/root/.healthcare-data-mcp/cache
  restart: unless-stopped
  healthcheck:
    test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8012),5); s.close()"]
    interval: 60s
    timeout: 10s
    retries: 3
    start_period: 30s
```

```json
// Addition to .mcp.json
"claims-analytics": {
  "type": "http",
  "url": "http://localhost:8012/mcp"
}
```
