# Health System Profiler MCP Server — Design Document

**Date:** 2026-03-02
**Status:** Approved

## Problem

Profiling a health system's geographic footprint currently requires 80+ individual MCP tool calls, ~30 of which fail, supplemented by web searches and sitemap scraping. A Jefferson Health + LVHN analysis consumed ~100K+ tokens and 5+ minutes. The data quality was poor: guessed bed counts, missing facilities, no service capability data, and incomplete outpatient inventories.

## Solution

A new MCP server (`health-system-profiler`, port 8007) that returns a complete health system profile in 1-3 tool calls by combining three authoritative public data sources.

## Three-Layer Architecture

### Layer 1: AHRQ Compendium (System Discovery)

**Source:** [AHRQ Compendium of U.S. Health Systems](https://www.ahrq.gov/chsp/data-resources/compendium-2023.html)

**Purpose:** Authoritative mapping of health system names → constituent hospital CCNs.

**Files:**
- System File (~112KB): `health_sys_id`, `health_sys_name`, `health_sys_city`, `health_sys_state`, hospital count, physician group count
- Hospital Linkage File: `health_sys_id` → `ccn`, `hospital_name`, address, `hos_beds`, `hos_dsch`, ownership, revenue, teaching status
- Outpatient Site Linkage File: `health_sys_id` → outpatient location details

**Download:** Requires Playwright (AHRQ uses AWS WAF bot protection). Cached locally, updated annually.

**URLs:**
- Hospital Linkage: `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv`
- System File: `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv`
- Outpatient Linkage: similar pattern (TBD via Playwright)

### Layer 2: CMS Provider of Services File (Facility Enrichment)

**Source:** [CMS POS File](https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities) (Q4 2025, ~50MB)

**Purpose:** Granular facility-level data with 470+ columns. Enriches AHRQ data with:
- Bed counts by type: `BED_CNT`, `CRTFD_BED_CNT`, `PSYCH_UNIT_BED_CNT`, `REHAB_UNIT_BED_CNT`, `HOSPC_BED_CNT`, `VNTLTR_BED_CNT`, `AIDS_BED_CNT`, `ALZHMR_BED_CNT`, `DLYS_BED_CNT`
- 100+ service flags: cardiac cath, open heart surgery, MRI, CT, PET scan, nuclear medicine, trauma, burn care, NICU, OB, transplant, etc.
- Staffing counts: RN, LPN, physicians, therapists, pharmacists, etc.
- Off-site location counts: `TOT_OFSITE_EMER_DEPT_CNT`, `TOT_OFSITE_URGNT_CARE_CNTR_CNT`, `TOT_OFSITE_PSYCH_UNIT_CNT`, `TOT_OFSITE_REHAB_HOSP_CNT`, etc.
- `RELATED_PROVIDER_NUMBER` for graph expansion to sub-entities (dialysis, rehab, behavioral health)
- Operating room count, endoscopy rooms, cardiac cath rooms

**Download URL:** `https://data.cms.gov/sites/default/files/2026-01/c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv`

**Join key:** CCN (`PRVDR_NUM` in POS = `ccn` in AHRQ)

### Layer 3: NPPES + HSAF (Outpatient & Service Areas)

**NPPES (live API):**
- Wildcard search: `https://npiregistry.cms.hhs.gov/api/?version=2.1&organization_name={pattern}*&state={state}&enumeration_type=NPI-2`
- Categorize results by taxonomy code (family medicine, cardiology, rehab, pharmacy, etc.)
- Filter noise by matching against AHRQ outpatient linkage + known system name patterns

**HSAF (cached):**
- Hospital Service Area File (already used by service-area MCP server)
- Compute PSA (75% threshold) and SSA (95% threshold) for each inpatient CCN
- Reuse existing `service_area_engine.py` logic

## Data Flow

```
User: "Jefferson Health"
  │
  ├─ AHRQ Compendium: fuzzy match health_sys_name → health_sys_id → CCN list
  │
  ├─ POS File: JOIN on CCN → beds, services, staffing, off-site counts
  │   └─ Graph expansion: RELATED_PROVIDER_NUMBER → sub-entities
  │
  ├─ Hospital General Info: JOIN on CCN → quality ratings
  │
  ├─ HSAF: Compute PSA/SSA per CCN
  │
  └─ NPPES: Wildcard search → outpatient sites categorized by taxonomy
      └─ Cross-reference against AHRQ outpatient linkage file
  │
  └─ Structured JSON response
```

## MCP Tool Interface

### Tool 1: `search_health_systems(query: str) → list`

Fuzzy search against AHRQ Compendium system names.

**Parameters:**
- `query` (str): System name to search for

**Returns:** List of matching systems:
```json
[
  {
    "system_id": "AHRQ_1234",
    "name": "Jefferson Health",
    "hq_city": "Philadelphia",
    "hq_state": "PA",
    "hospital_count": 14,
    "total_beds": 5218
  }
]
```

### Tool 2: `get_system_profile(system_id: str | system_name: str) → dict`

Full system profile in one call.

**Parameters:**
- `system_id` (str, optional): AHRQ system ID
- `system_name` (str, optional): Fuzzy system name (auto-resolved)
- `include_service_areas` (bool, default True): Include PSA/SSA
- `include_outpatient` (bool, default True): Include NPPES outpatient sites

**Returns:** Structured JSON with sections:
- `system`: System-level summary (name, HQ, total beds, total hospitals, total discharges)
- `inpatient_facilities[]`: Per-hospital: CCN, name, address, beds by type, services, quality rating, service area
- `sub_entities[]`: RELATED_PROVIDER_NUMBER-linked entities (dialysis, rehab, etc.)
- `outpatient_sites`: NPPES-discovered sites categorized by taxonomy
- `off_site_summary`: Aggregated off-site location counts from POS

### Tool 3: `get_system_facilities(system_id: str, facility_type: str = "all") → list`

Detailed facility data with full POS enrichment.

**Parameters:**
- `system_id` (str): AHRQ system ID
- `facility_type` (str): Filter: "inpatient", "outpatient", "rehab", "behavioral_health", "all"

**Returns:** Detailed facility list with full POS columns (staffing, services, bed breakdowns).

## Server Architecture

```
servers/health-system-profiler/
├── server.py              # FastMCP server, tool definitions
├── data_loaders.py        # Download/cache AHRQ + POS files
├── system_discovery.py    # AHRQ fuzzy search, system→CCN resolution
├── facility_enrichment.py # POS join, service parsing, bed breakdowns
├── graph_expansion.py     # RELATED_PROVIDER_NUMBER walk
├── outpatient_discovery.py # NPPES wildcard search + taxonomy categorization
└── __init__.py
```

**Dependencies:**
- Existing: `shared/utils/cms_client.py` for CSV download/caching
- Existing: `servers/service-area/service_area_engine.py` for PSA/SSA computation
- New: Playwright (or pre-cached AHRQ files) for WAF-protected downloads

## Token Efficiency

| Metric | Old Approach | New System |
|--------|-------------|------------|
| Tool calls | 80+ | 1-3 |
| Failed calls | 30+ | 0 |
| Web searches | 6 | 0 |
| Data quality | Guessed, incomplete | Authoritative |
| Token usage | ~100K+ | ~5-10K |
| Bed count accuracy | ~60% (guessed) | 100% (POS) |
| Service capabilities | None | 100+ flags |
| Staffing data | None | Full counts |

## Data Refresh Strategy

| Source | Size | Update Frequency | Download Method |
|--------|------|-----------------|-----------------|
| AHRQ Compendium | ~500KB total | Annual | Playwright (WAF) |
| CMS POS File | ~50MB | Quarterly | Direct HTTP |
| Hospital General Info | ~30MB | Quarterly | Direct HTTP (existing) |
| HSAF | ~50MB | Annual | Direct HTTP (existing) |
| NPPES | Live API | Real-time | REST API |

## Key Findings from Research

1. **AHRQ Compendium is the only public dataset** that authoritatively maps health systems to their hospitals. The CMS POS `MLT_FAC_ORG_NAME` field is only populated for dialysis chains.

2. **POS file has 10x more data** than Hospital General Info. The old conversation got `beds: null` because of column name drift in the General Info file; POS reliably has `BED_CNT` and `CRTFD_BED_CNT`.

3. **POS reveals facilities the old approach missed entirely:**
   - LVH has TWO CCNs at Cedar Crest (390133 with 1,190 beds AND 390261 with 514 beds)
   - LVH Muhlenberg has its own CCN (390263, 184 beds) — not under 390133
   - LVH Schuylkill has TWO campuses (390030 + 390031)
   - Jefferson has Ford Road campus (390149, 133 beds) — completely missed

4. **AHRQ Compendium has outpatient linkage** — eliminates the need for sitemap scraping.

5. **NPPES authorized official clustering** was explored but not viable — the NPPES API doesn't support searching by authorized official name, and results within a system have different officials per NPI.
