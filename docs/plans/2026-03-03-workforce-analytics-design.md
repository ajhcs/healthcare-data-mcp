# Workforce & Labor Analytics — Design Document

**Server:** 10 of N | **Port:** 8011 | **Name:** `workforce-analytics`

## Overview

MCP server providing healthcare workforce intelligence: occupation-level employment and wages from BLS, shortage area analysis from HRSA, graduate medical education profiles from CMS cost reports, residency program data from ACGME, union election/work stoppage data from NLRB, and hospital/nursing home staffing benchmarks from CMS PBJ and HCRIS.

## 7 Tools

| # | Tool | Data Sources | Access Pattern |
|---|------|-------------|----------------|
| 1 | `get_bls_employment` | BLS OES API v2 + Employment Projections | Real-time API (keyed) |
| 2 | `get_hrsa_workforce` | HRSA HPSA CSV + AHRF county data | Bulk CSV → Parquet cache |
| 3 | `get_gme_profile` | CMS HCRIS Worksheet S-2 | Bulk CSV → Parquet cache |
| 4 | `get_residency_programs` | Static ACGME Data Resource Book | Bundled CSV |
| 5 | `search_union_activity` | NLRB SQLite + BLS work stoppages | Bulk download → SQLite/Parquet |
| 6 | `get_staffing_benchmarks` | CMS PBJ (Socrata) + HCRIS S-3 | API + Bulk |
| 7 | `get_cost_report_staffing` | CMS HCRIS Worksheet S-3 | Bulk CSV → Parquet cache |

## Architecture

```
servers/workforce-analytics/
├── __init__.py
├── models.py              # Pydantic response models
├── bls_client.py          # BLS OES API v2 (real-time, keyed)
├── workforce_data.py      # HRSA, HCRIS, PBJ, ACGME data loaders
├── labor_data.py          # NLRB SQLite, BLS work stoppages
├── server.py              # FastMCP port 8011, 7 tools
```

### Module Responsibilities

**bls_client.py** — BLS OES API v2 client. Requires `BLS_API_KEY` env var. Handles series ID construction for healthcare SOC codes (prefix `29-`, `31-`), area code mapping, and 10-year employment projections from bulk EP tables. Rate limit: 500 req/day, 50 series/req, 20 years/req.

**workforce_data.py** — Bulk dataset management:
- **HRSA HPSA**: Downloads `BCD_HPSA_FCT_DET_DH.csv` (~15MB), caches as Parquet. Fields: HPSA_Name, HPSA_ID, HPSA_Score, Designation_Type, Provider_Ratio.
- **HRSA AHRF**: County-level health workforce supply (6000+ variables). Downloads from `data.hrsa.gov/data/download`.
- **CMS HCRIS**: Downloads hospital cost report nmrc file (>2GB), filters to Worksheet S-2 and S-3 rows only (~50MB filtered), caches as Parquet. Uses worksheet/line/column codes to extract specific fields.
- **CMS PBJ**: Queries Socrata API at `data.cms.gov` for Payroll-Based Journal daily nurse staffing (nursing homes). Fields: RN_HRPPD, LPN_HRPPD, CNA_HRPPD.
- **ACGME static**: Bundled CSV extract from ACGME Data Resource Book with program-level data (specialty, institution, positions, accreditation status).

**labor_data.py** — Union/labor data:
- **NLRB**: Downloads SQLite DB from `labordata/nlrb-data` GitHub releases. Tables: elections, unfair_labor_practices. Healthcare filter: NAICS codes 62xxxx.
- **BLS work stoppages**: Download from BLS website, cache as Parquet.

**server.py** — Standard FastMCP boilerplate (port 8011, streamable-http transport).

## Tool Signatures

### Tool 1: get_bls_employment

```
Input:
  occupation: str          # e.g. "Registered Nurses", "Physicians", SOC code "29-1141"
  area_code: str = ""      # MSA FIPS code, or state code, or "" for national
  state: str = ""          # Two-letter state code (alternative to area_code)
  include_projections: bool = True

Output: BLSEmploymentResponse
  occupation_title: str
  soc_code: str
  area_name: str
  employment: int
  mean_wage: float
  median_wage: float
  pct_10_wage: float
  pct_90_wage: float
  employment_change_pct: float | None    # 10-year projection
  annual_openings: int | None
  data_year: str
```

**BLS OES Series ID format:** `OEUM{area_code}{soc_code}{datatype}`
- Area codes: `0000000` (national), state FIPS + `000`, MSA codes
- SOC codes: `29-1141` (RNs), `29-1228` (Physicians), `31-1131` (Nursing Assistants), etc.
- Data types: `01` (employment), `04` (mean wage), `13` (median wage), `07` (10th pct), `11` (90th pct)

**Healthcare SOC prefixes:**
- `29-` Healthcare Practitioners and Technical Occupations
- `31-` Healthcare Support Occupations
- `11-9111` Medical and Health Services Managers

### Tool 2: get_hrsa_workforce

```
Input:
  state: str               # Two-letter state code
  county_fips: str = ""    # 5-digit FIPS code
  discipline: str = ""     # "Primary Care", "Dental", "Mental Health", or "" for all

Output: HRSAWorkforceResponse
  state: str
  hpsas: list[HPSARecord]
    hpsa_name: str
    hpsa_id: str
    hpsa_score: int
    designation_type: str    # Geographic, Population, Facility
    discipline: str
    designation_date: str
    provider_ratio: str
    est_underserved_pop: int
  county_stats: CountyWorkforceStats | None
    county_name: str
    total_mds: int
    total_dos: int
    total_rns: int
    total_dentists: int
    total_pharmacists: int
```

### Tool 3: get_gme_profile

```
Input:
  hospital_name: str = ""  # Fuzzy search
  ccn: str = ""            # 6-digit CMS Certification Number

Output: GMEProfileResponse
  hospital_name: str
  ccn: str
  teaching_status: str     # "Major Teaching", "Minor Teaching", "Non-Teaching"
  total_resident_ftes: float
  primary_care_ftes: float
  total_intern_ftes: float
  ime_payment: float | None
  dgme_payment: float | None
  beds: int
  fiscal_year: str
```

**HCRIS Worksheet S-2 extraction:**
- Line 28-33: Intern and Resident FTEs
- Line 35: Total beds
- Columns map to different categories (primary care, other)

### Tool 4: get_residency_programs

```
Input:
  institution: str = ""    # Institution name (fuzzy match)
  specialty: str = ""      # e.g. "Internal Medicine", "Surgery"
  state: str = ""

Output: ResidencyProgramsResponse
  total_programs: int
  programs: list[ResidencyProgram]
    program_id: str
    specialty: str
    institution: str
    city: str
    state: str
    total_positions: int
    filled_positions: int
    accreditation_status: str
```

### Tool 5: search_union_activity

```
Input:
  employer_name: str = ""  # Employer or health system name
  state: str = ""
  year_start: int = 2015
  year_end: int = 2026

Output: UnionActivityResponse
  total_elections: int
  total_stoppages: int
  elections: list[NLRBElection]
    case_number: str
    employer: str
    union: str
    date: str
    result: str            # "Certified", "Decertified", "Dismissed", etc.
    unit_size: int
    city: str
    state: str
  work_stoppages: list[WorkStoppage]
    employer: str
    union: str
    start_date: str
    end_date: str
    workers_involved: int
    duration_days: int
```

### Tool 6: get_staffing_benchmarks

```
Input:
  ccn: str = ""            # Specific facility
  state: str = ""          # State-level benchmarks
  facility_type: str = "hospital"  # "hospital" or "nursing_home"

Output: StaffingBenchmarksResponse
  facility_name: str
  ccn: str
  facility_type: str
  rn_hprd: float | None         # Hours per resident/patient day
  lpn_hprd: float | None
  cna_hprd: float | None
  total_nurse_hprd: float | None
  peer_median_rn_hprd: float | None
  peer_pct_rank: float | None   # 0-100 percentile
  data_source: str               # "CMS_PBJ" or "CMS_HCRIS"
  data_period: str
```

### Tool 7: get_cost_report_staffing

```
Input:
  ccn: str                 # 6-digit CMS Certification Number
  year: int = 0            # Fiscal year (0 = most recent)

Output: CostReportStaffingResponse
  hospital_name: str
  ccn: str
  fiscal_year: str
  departments: list[DepartmentStaffing]
    dept_name: str
    total_ftes: float
    rn_ftes: float
    lpn_ftes: float
    aide_ftes: float
    salary_expense: float | None
    benefits_expense: float | None
  total_ftes: float
  total_salary_expense: float | None
```

**HCRIS Worksheet S-3 Part I extraction:**
- Lines 1-25: Departments (General Service, ICU, OR, ER, etc.)
- Columns: Total FTE, RN FTE, LPN FTE, Aide FTE
- Part II: Cost allocation (salary, benefits)

## Data Downloads & Caching

All bulk datasets cached at `~/.healthcare-data-mcp/cache/workforce/`:

| Dataset | URL / Source | Cache File | Size | TTL |
|---------|-------------|------------|------|-----|
| HPSA | `data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv` | `hpsa.parquet` | ~15MB raw | 30 days |
| AHRF | `data.hrsa.gov/data/download` (county file) | `ahrf.parquet` | ~50MB | 90 days |
| HCRIS Nmrc | `cms.gov/.../HOSP10_NMRC.CSV` (filtered S-2/S-3) | `hcris_staffing.parquet` | ~50MB filtered | 30 days |
| ACGME | Bundled `data/acgme_programs.csv` | N/A (static) | ~2MB | N/A |
| NLRB | `github.com/labordata/nlrb-data/releases` (SQLite) | `nlrb.db` | ~100MB | 7 days |
| BLS stoppages | `bls.gov/wsp/` (CSV) | `work_stoppages.parquet` | ~1MB | 30 days |
| BLS projections | `bls.gov/emp/` (CSV tables) | `bls_projections.parquet` | ~5MB | 90 days |

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BLS_API_KEY` | Yes | BLS API v2 registration key |
| `MCP_TRANSPORT` | No | `stdio` (default) or `streamable-http` |
| `MCP_PORT` | No | Default 8011 |

## Error Handling

- BLS API key missing → return clear error with registration URL
- Bulk download fails → return error with manual download instructions
- HCRIS filter returns no rows for CCN → return "not found" with suggestions
- NLRB DB not cached → return error with download trigger
- Rate limit hit (BLS) → return error with retry-after guidance
