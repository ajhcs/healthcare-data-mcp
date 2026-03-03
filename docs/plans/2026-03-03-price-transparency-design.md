# Price Transparency / MRF Engine — Design Document

**Date:** 2026-03-03
**Server:** 8 (port 8009)
**Scope:** On-demand hospital MRF parsing with benchmark analytics

## Problem

Hospital Machine-Readable Files (MRFs) contain negotiated rates between hospitals and insurers, mandated by CMS price transparency rules. These files are 50MB–15GB each, hosted individually on hospital websites with no centralized API. Analysts need a way to query specific CPT code rates, compare across payers and hospitals, and benchmark against Medicare without manually downloading and parsing multi-gigabyte files.

## Architecture

Four modules + models, following the project's FastMCP server pattern:

```
servers/price-transparency/
├── __init__.py
├── server.py              # FastMCP server, 5 tools
├── models.py              # Pydantic response models
├── mrf_registry.py        # Hospital → MRF URL discovery
├── mrf_processor.py       # Download, parse CSV/JSON, build + query Parquet index
└── benchmark_client.py    # CMS PFS API, Medicare utilization data, cross-hospital percentiles
```

Symlink: `servers/price_transparency` → `servers/price-transparency`

## Data Strategy

**On-demand streaming + cache:** When rates are requested for a hospital, the server downloads the full MRF file, parses it (CSV or JSON), and builds a Parquet index. Subsequent requests for any CPT code from that hospital are instant from cache.

**Cache structure:**
```
~/.healthcare-data-mcp/cache/mrf/
├── registry.json              # Curated hospital → MRF URL mappings
├── {ccn_or_ein}/
│   ├── metadata.json          # Hospital name, MRF URL, download date, row count
│   ├── charges.parquet        # All rates (normalized)
│   ├── payers.parquet         # Payer lookup table
│   └── descriptions.parquet   # Description lookup table
└── pfs/
    └── indicators_{year}.parquet  # Cached PFS fee schedule
```

## Module 1: `mrf_registry.py` — Hospital MRF Discovery

**Purpose:** Map hospital identifiers (name, CCN, EIN) to MRF file URLs.

### Curated Registry

Bundled `registry.json` seeded from MR-Explore's 31 Philadelphia hospital files (EINs extracted from filenames). Format:

```json
{
  "hospitals": {
    "390223": {
      "name": "Hospital of the University of Pennsylvania",
      "ein": "231352685",
      "domain": "www.pennmedicine.org",
      "mrf_urls": [
        {"url": "https://..._standardcharges.csv", "format": "csv", "last_verified": "2025-12-01"}
      ]
    }
  }
}
```

### cms-hpt.txt Fallback

When a hospital isn't in the registry:
1. Query CMS Provider Data Catalog API (`data.cms.gov`, dataset `xubh-q36u`) to find the hospital and its website domain
2. Fetch `https://{domain}/cms-hpt.txt`
3. Parse plaintext blocks to extract `mrf-url` entries
4. Add to registry cache for future lookups

### Functions

- `search_registry(query, state)` — fuzzy search curated registry by name/CCN/EIN
- `discover_mrf_urls(hospital_name_or_id)` — full pipeline: registry → CMS provider lookup → cms-hpt.txt → cache result

### Data Sources

| Source | URL | Auth |
|--------|-----|------|
| CMS Provider Data Catalog | `https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0` | None |
| Hospital cms-hpt.txt | `https://{hospital-domain}/cms-hpt.txt` | None |

## Module 2: `mrf_processor.py` — Download, Parse, Index

**Purpose:** Full MRF lifecycle — download, detect format, parse, normalize to Parquet, query.

### Code Reuse from MR-Explore

Adapted (copy + modify) from `/mnt/d/Coding Projects/MR-Explore/src/data/`:

| MR-Explore Module | What We Adapt |
|-------------------|---------------|
| `importer.py` | Header detection, Polars CSV reading, schema inference |
| `recognition.py` | 3-pass fuzzy column matching (exact → keyword heuristic → SequenceMatcher) |
| `models.py` | `CMS_COLUMN_MAPPING` (21 standard field mappings) |
| `sources/mrf/json_parser.py` | JSON MRF parsing for `in_network` / `allowed_amounts` sections |
| `normalizer.py` | Parquet schema definitions, lookup table extraction |

### Download Pipeline

- `httpx` async streaming download with Content-Length check (warn if >5GB)
- Resume support via HTTP Range headers for interrupted downloads
- Cache raw file at `~/.healthcare-data-mcp/cache/mrf/{id}/raw.*`

### Parse Pipeline

1. Detect format from Content-Type header or file extension (`.csv` vs `.json`)
2. **CSV path:** Polars `scan_csv` → detect header row → apply CMS column mapping (exact + fuzzy) → cast numerics → normalize to Parquet with lookup tables
3. **JSON path:** `ijson` streaming parser → extract `standard_charge_information` → map to flat records → normalize to Parquet
4. Write `charges.parquet` + `payers.parquet` + `descriptions.parquet` + `metadata.json`

### Query Interface

- `get_rates(hospital_id, cpt_codes, payer_filter)` — DuckDB query over Parquet, returns rates grouped by CPT code and payer
- `get_rate_stats(hospital_id, cpt_codes)` — min, max, median, IQR, coefficient of variation across payers
- `get_all_rates_for_codes(cpt_codes)` — across all cached hospitals, for percentile computation

### Parquet Schema

```
charges.parquet:
  id            Int64
  hospital_id   Int32 (always 1 for single-hospital packs)
  description_id Int32 → descriptions.parquet
  code1         Utf8 (CPT/HCPCS/DRG)
  code1_type    Utf8
  setting       Utf8 (inpatient/outpatient/both)
  billing_class Utf8
  gross_charge  Float64
  discounted_cash Float64
  payer_id      Int32 → payers.parquet
  negotiated_dollar    Float64
  negotiated_percentage Float64
  methodology   Utf8
  min_charge    Float64
  max_charge    Float64
```

## Module 3: `benchmark_client.py` — Benchmark Data

**Purpose:** Fetch and cache external benchmark data for rate comparison.

### Source 1: CMS Physician Fee Schedule

- **API:** `https://pfs.data.cms.gov/api/1/datastore/query/{dataset_id}/0`
- **Auth:** None
- **2026 Indicators dataset ID:** `7c7df311-5315-4f38-b9ed-fd62f8bebe11`
- **2026 Localities dataset ID:** `81f942b8-3f6c-4b36-a151-0888376d9ca0`
- **Conversion factor:** $33.4009 (2026)
- **Medicare allowed amount:**
  ```
  (Work_RVU × Work_GPCI + PE_RVU × PE_GPCI + MP_RVU × MP_GPCI) × $33.4009
  ```
- **Coverage:** 19,232 HCPCS codes, 110 localities
- **Cache:** Download full indicators + localities datasets, store as Parquet

### Source 2: CMS Medicare Provider Utilization & Payment Data

- **API:** `https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0`
- **Dataset:** Medicare Physician & Other Practitioners - by Provider and Service
- **Auth:** None
- **Key fields:** `average_medicare_allowed_amt`, `average_medicare_payment_amt`, `average_submitted_chrg_amt`, `number_of_services`
- **Purpose:** Shows what Medicare *actually paid* vs fee schedule theoretical amounts

### Source 3: Cross-Hospital Percentiles (Internal)

- Computed from all cached hospital Parquet data in `~/.healthcare-data-mcp/cache/mrf/*/`
- For each CPT code, compute 25th/50th/75th/90th percentile across cached hospitals
- More hospitals cached = better percentile data

### Benchmark Output

```python
{
    "cpt_code": "99213",
    "hospital_rate": 185.00,
    "medicare_allowed": 112.40,
    "pct_of_medicare": 164.6,
    "medicare_actual_avg_payment": 89.50,
    "peer_percentile": 72,
    "peer_25th": 95.00,
    "peer_50th": 145.00,
    "peer_75th": 195.00,
    "peer_90th": 250.00
}
```

## Module 4: `models.py` — Pydantic Models

| Model | Purpose |
|-------|---------|
| `MRFIndexResult` | `search_mrf_index` response — hospital info + MRF URLs + cache status |
| `NegotiatedRate` | Individual rate record (code, payer, plan, dollar, methodology) |
| `NegotiatedRatesResponse` | `get_negotiated_rates` response — rates grouped by code |
| `RateDispersion` | Per-code stats: min, max, median, IQR, CV, payer count |
| `HospitalRateComparison` | Per-hospital rates for system comparison |
| `BenchmarkResult` | Rate vs Medicare/peers/utilization data |

## Tools

| # | Tool | Params | Returns |
|---|------|--------|---------|
| 1 | `search_mrf_index` | `query: str`, `state: str = ""` | MRF URLs, hospital info, cache status |
| 2 | `get_negotiated_rates` | `hospital_id: str`, `cpt_codes: list[str]`, `payer: str = ""` | Rates by payer/plan for each code |
| 3 | `compute_rate_dispersion` | `hospital_id: str`, `cpt_codes: list[str]` | Min, max, median, IQR, CV per code |
| 4 | `compare_rates_system` | `system_name: str`, `cpt_codes: list[str]` | Side-by-side rates across system hospitals |
| 5 | `benchmark_rates` | `hospital_id: str`, `cpt_codes: list[str]`, `locality: str = ""` | Medicare %, peer percentile, utilization comparison |

## Dependencies

- `polars` — CSV parsing, Parquet I/O (already in project)
- `duckdb` — Parquet querying (already in MR-Explore, add to project)
- `ijson` — Streaming JSON MRF parsing (new dependency)
- `httpx` — Async HTTP (already in project)

## Caching

| Data | Cache | TTL |
|------|-------|-----|
| Curated registry JSON | Filesystem | Manual update |
| Downloaded MRF files | Filesystem | 30 days (re-download) |
| Parsed Parquet indexes | Filesystem | 30 days (re-parse) |
| PFS indicators/localities | Filesystem Parquet | 1 year (annual release) |
| CMS utilization data | Filesystem | 1 year (annual release) |

## Known Constraints

- First request for an uncached hospital triggers a full MRF download (minutes for large files)
- ~60% of hospitals properly host cms-hpt.txt; ~25% have non-compliant MRF schemas
- Cross-hospital percentiles improve as more hospitals are cached
- Turquoise Health has no free API; we rely on CMS public data only
