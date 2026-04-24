# Platform Bug Fixes — Design Document

**Date**: 2026-03-06
**Scope**: Fix 6 broken tools across 5 servers, register 1 missing MCP server

## Summary

Level 3-4 integration testing with Jefferson Health (CCN 390174) revealed 7 issues.
6 are code/data fixes; 1 (physician search) is deferred as an upstream API quirk.

## Fix 1: CMS Geographic Variation 410 Gone (HIGH)

**Affected tools**: `get_medicare_enrollment`, `get_geographic_variation`
**Server**: geo-demographics (port 8003)
**File**: `servers/geo-demographics/server.py`

### Root Cause
Socrata endpoints `nw2u-nkit` and `jhbs-ydf5` retired by CMS (HTTP 410 Gone).

### Solution
Replace Socrata API calls with bulk CSV download → Parquet cache → DuckDB query.

**New data source**:
```
https://data.cms.gov/sites/default/files/2025-03/a40ac71d-9f80-4d99-92d2-fd149433d7d8/
2014-2023%20Medicare%20Fee-for-Service%20Geographic%20Variation%20Public%20Use%20File.csv
```

**Column mapping** (verified against live endpoint):
- `BENE_GEO_LVL` → "State" / "County" / "National"
- `BENE_GEO_CD` → state abbr or FIPS code
- `YEAR` → filter to most recent year
- `BENES_TOTAL_CNT` → total_beneficiaries
- `MA_PRTCPTN_RATE` → ma_penetration_pct
- `BENE_AVG_AGE` → avg_age
- `BENE_FEML_PCT` → pct_female
- `BENE_DUAL_PCT` → pct_dual_eligible
- `TOT_MDCR_PYMT_PC` → per_capita_spending (replaces `actual_per_capita_costs`)
- `IP_MDCR_PYMT_PC` → ip_spending_per_capita
- `IP_CVRD_STAYS_PER_1000_BENES` → discharges_per_1000
- `ER_VISITS_PER_1000_BENES` → er_visits_per_1000
- `ACUTE_HOSP_READMSN_PCT` → readmission_rate

### Implementation
1. Add `_ensure_gv_cached()` function in a new `data_loaders.py` for geo-demographics
2. Download CSV → normalize columns → save as Parquet (zstd) in `~/.healthcare-data-mcp/cache/geo-demographics/`
3. Replace Socrata queries in both tool functions with DuckDB queries against cached Parquet
4. Filter by `BENE_GEO_LVL` + `BENE_GEO_CD` + latest `YEAR`

---

## Fix 2: Cost Report PUF 404 (HIGH)

**Affected tools**: `get_facility_financials` (cms-facility), `get_financial_profile` (hospital-quality), `get_gme_profile` + `get_cost_report_staffing` (workforce-analytics)
**Files**: `servers/cms-facility/data_loaders.py`, `servers/hospital-quality/data_loaders.py`

### Root Cause
Dataset ID `di4u-7yu6` no longer exists on CMS Provider Data API (HTTP 404).

### Solution
Replace with direct CSV download of `CostReport_2023_Final.csv`.

**New URL**:
```
https://data.cms.gov/sites/default/files/2026-01/3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv
```

**Key columns** (verified):
- `Provider CCN` → join key
- `Hospital Name`, `Street Address`, `City`, `State Code`, `Zip Code`
- `Number of Beds`, `Total Bed Days Available`
- `FTE - Employees on Payroll`, `Number of Interns and Residents (FTE)`
- `Total Discharges Title XVIII` (Medicare discharges)
- `Rural Versus Urban`, `CCN Facility Type`, `Provider Type`, `Type of Control`

### Implementation
1. Update `cost_report_url` in `cms-facility/data_loaders.py` to new CSV URL
2. Update `_csv_url("di4u-7yu6")` in `hospital-quality/data_loaders.py` to new CSV URL
3. Update column name mappings to match new CSV headers (e.g., `Provider CCN` not `provider_ccn`)
4. Workforce-analytics HCRIS ZIP download (FY2024) already works — no change needed there

---

## Fix 3: Register web-intelligence in .mcp.json (LOW)

**File**: `.mcp.json`

### Root Cause
Server 13 (web-intelligence, port 8014) missing from project MCP config.

### Fix
Add one entry:
```json
"web-intelligence": {
  "type": "http",
  "url": "http://localhost:8014/mcp"
}
```

---

## Fix 4: find_competing_facilities No Lat/Lon (MEDIUM)

**Affected tool**: `find_competing_facilities`
**Server**: drive-time (port 8004)
**File**: `servers/drive-time/server.py`

### Root Cause
CMS Hospital General Info CSV has no latitude/longitude columns. Only: Facility ID, Facility Name, Address, City/Town, State, ZIP Code, County/Parish, etc.

### Solution
Use ZIP code centroids as approximate coordinates. The ZCTA shapefile (already used by geo-demographics) has centroids, but downloading 800MB for this is overkill. Instead, use a lightweight centroid lookup:

1. Build a ZIP→centroid mapping from the Census Gazetteer file (~1MB CSV):
   `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_zcta_national.txt`
   Columns: GEOID (ZIP), INTPTLAT, INTPTLONG

2. After loading Hospital General Info, join facility ZIP codes to centroids
3. Filter facilities within bounding box first (fast), then compute OSRM drive times for candidates

### Implementation
1. Add `_ensure_zip_centroids()` in drive-time data loader
2. Modify `_parse_lat_lon()` to fall back to ZIP centroid lookup when no lat/lon columns exist
3. Cache centroids as small Parquet file

---

## Fix 5: NLRB Parser Empty Fields (MEDIUM)

**Affected tool**: `search_union_activity`
**Server**: workforce-analytics (port 8011)
**File**: `servers/workforce-analytics/labor_data.py`

### Root Cause
Dynamic schema detection queries only the `election` table. But:
- `filing` table has: name (employer), city, state, date_filed, status, number_of_eligible_voters
- `election` table has: case_number, date, unit_size
- `participant` table has: name (union), role (Petitioner/Employer)

The code looks for `name`/`employer` columns in `election`, which doesn't have them.

### Solution
Replace dynamic schema detection with explicit multi-table JOIN:

```sql
SELECT
    f.case_number,
    f.name AS employer,
    f.city,
    f.state,
    f.date_filed,
    f.status,
    f.number_of_eligible_voters,
    e.date AS election_date,
    e.unit_size,
    p.name AS union_name
FROM filing f
LEFT JOIN election e ON f.case_number = e.case_number
LEFT JOIN participant p ON f.case_number = p.case_number AND p.role = 'Petitioner'
WHERE f.name LIKE '%{employer_name}%'
  AND ({state_filter})
  AND ({year_filter})
```

### Implementation
1. Replace `search_nlrb_elections()` in `labor_data.py` with explicit JOIN query
2. Remove dynamic schema detection — the NLRB database schema is stable (nightly builds from same source)
3. Map: `f.name` → employer, `p.name` → union, `f.city/state` → location, `e.date` → election_date

---

## Fix 6: Market Share Missing Facility Names (LOW)

**Affected tool**: `get_market_share`
**Server**: service-area (port 8002)
**File**: `servers/service-area/service_area_engine.py`

### Root Cause
HSAF CSV has only 5 columns: `MEDICARE_PROV_NUM, ZIP_CD_OF_RESIDENCE, TOTAL_DAYS_OF_CARE, TOTAL_CHARGES, TOTAL_CASES`. No facility name column.

### Solution
After computing market share by CCN, cross-reference against Hospital General Info (already downloaded by cms-facility server) to enrich with facility names.

### Implementation
1. In `compute_market_share()`, after grouping by CCN, load Hospital General Info CSV
2. Join on CCN → `Facility ID` to get `Facility Name`
3. Use same download/cache pattern as other servers

---

## Fix 7: Physician Search (DEFERRED)

NPPES API upstream behavior — returns results that don't perfectly match `last_name` parameter. Code is correct. Not worth patching around.

---

## Execution Order

1. Fix 3 (MCP registration) — 1 line, instant
2. Fix 1 (Geographic Variation) — new data loader + rewrite 2 tool functions
3. Fix 2 (Cost Report) — URL + column mapping update in 2 servers
4. Fix 5 (NLRB parser) — rewrite query function
5. Fix 6 (Market share names) — add cross-reference join
6. Fix 4 (Geocoding) — new centroid data source + fallback logic
