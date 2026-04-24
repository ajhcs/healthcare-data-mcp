# Platform Bug Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix 6 broken tools across 5 MCP servers so all 68 tools return valid data for the Jefferson Health golden-path test.

**Architecture:** Each fix targets a specific data pipeline failure — broken URLs, wrong table joins, or missing cross-references. All fixes follow the existing pattern: download CSV → cache → query via pandas/DuckDB.

**Tech Stack:** Python 3.11+, FastMCP, pandas, DuckDB, httpx, pydantic

---

### Task 1: Register web-intelligence in .mcp.json

**Files:**
- Modify: `.mcp.json` (add 1 entry after line 49)

**Step 1: Add the web-intelligence entry**

In `.mcp.json`, add the web-intelligence server entry inside the `mcpServers` object, after the `public-records` entry:

```json
    "public-records": {
      "type": "http",
      "url": "http://localhost:8013/mcp"
    },
    "web-intelligence": {
      "type": "http",
      "url": "http://localhost:8014/mcp"
    }
```

**Step 2: Validate JSON syntax**

Run: `python3 -c "import json; json.load(open('.mcp.json')); print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add .mcp.json
git commit -m "fix: register web-intelligence server in MCP config (port 8014)"
```

---

### Task 2: Fix CMS Geographic Variation 410 Gone

**Files:**
- Create: `servers/geo-demographics/data_loaders.py`
- Modify: `servers/geo-demographics/server.py` (lines 36-42 constants, lines 114-229 two tool functions)

**Step 1: Create the data loader**

Create `servers/geo-demographics/data_loaders.py` with a bulk CSV download → Parquet cache → DuckDB query pattern.

```python
"""Data loaders for CMS Geographic Variation PUF."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "geo-demographics"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_GV_PARQUET = _CACHE_DIR / "geographic_variation.parquet"
_CACHE_TTL_DAYS = 90

GV_CSV_URL = (
    "https://data.cms.gov/sites/default/files/2025-03/"
    "a40ac71d-9f80-4d99-92d2-fd149433d7d8/"
    "2014-2023%20Medicare%20Fee-for-Service%20Geographic%20Variation%20Public%20Use%20File.csv"
)


def _is_cache_valid(path: Path) -> bool:
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < _CACHE_TTL_DAYS


async def ensure_gv_cached() -> bool:
    """Download GV PUF CSV and convert to Parquet if needed."""
    if _is_cache_valid(_GV_PARQUET):
        return True

    logger.info("Downloading Geographic Variation PUF...")
    try:
        async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
            resp = await client.get(GV_CSV_URL)
            resp.raise_for_status()

        csv_path = _CACHE_DIR / "gv_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.to_parquet(_GV_PARQUET, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("GV PUF cached: %d rows", len(df))
        return True
    except Exception as e:
        logger.warning("Failed to cache GV PUF: %s", e)
        return False


def query_gv(geo_level: str, geo_code: str) -> dict | None:
    """Query cached GV Parquet for a geography, returning most recent year.

    Args:
        geo_level: "State" or "County"
        geo_code: State abbreviation (e.g. "PA") or 5-digit county FIPS.
    """
    if not _GV_PARQUET.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW gv AS SELECT * FROM read_parquet('{_GV_PARQUET}')")

        rows = con.execute("""
            SELECT * FROM gv
            WHERE BENE_GEO_LVL = ?
              AND BENE_GEO_CD = ?
            ORDER BY CAST(YEAR AS INTEGER) DESC
            LIMIT 1
        """, [geo_level, geo_code]).fetchdf()
        con.close()

        if rows.empty:
            return None

        row = rows.iloc[0]

        def _f(col: str) -> float | None:
            v = row.get(col)
            if v is None or str(v).strip() in ("", "*"):
                return None
            try:
                return float(str(v).replace(",", ""))
            except (ValueError, TypeError):
                return None

        def _i(col: str) -> int | None:
            f = _f(col)
            return int(f) if f is not None else None

        return {
            "year": str(row.get("YEAR", "")),
            "geo_level": str(row.get("BENE_GEO_LVL", "")),
            "geo_code": str(row.get("BENE_GEO_CD", "")),
            "geo_desc": str(row.get("BENE_GEO_DESC", "")),
            "total_beneficiaries": _i("BENES_FFS_CNT"),
            "ma_penetration_pct": _f("MA_PRTCPTN_RATE"),
            "avg_age": _f("BENE_AVG_AGE"),
            "pct_female": _f("BENE_FEML_PCT"),
            "pct_dual_eligible": _f("BENE_DUAL_PCT"),
            "per_capita_spending": _f("TOT_MDCR_PYMT_PC"),
            "ip_spending_per_capita": _f("IP_MDCR_PYMT_PC"),
            "op_spending_per_capita": _f("OP_MDCR_PYMT_PC"),
            "physician_spending_per_capita": _f("PHYS_MDCR_PYMT_PC"),
            "snf_spending_per_capita": _f("SNF_MDCR_PYMT_PC"),
            "discharges_per_1000": _f("IP_CVRD_STAYS_PER_1000_BENES"),
            "er_visits_per_1000": _f("ER_VISITS_PER_1000_BENES"),
            "readmission_rate": _f("ACUTE_HOSP_READMSN_PCT"),
        }
    except Exception as e:
        logger.warning("GV query failed: %s", e)
        return None
```

**Step 2: Rewrite get_medicare_enrollment in server.py**

Replace lines 36-42 (old Socrata constants) and lines 114-166 (get_medicare_enrollment function) in `servers/geo-demographics/server.py`:

Remove the old Socrata constants (lines 36-42):
```python
# CMS data.cms.gov Socrata API base
CMS_SOCRATA_BASE = "https://data.cms.gov/resource"

# Medicare Geographic Variation — State/County level
# Dataset identifier on data.cms.gov (Socrata)
MEDICARE_GEO_VAR_STATE = "nw2u-nkit"  # State-level
MEDICARE_GEO_VAR_COUNTY = "jhbs-ydf5"  # County-level
```

Replace with import:
```python
from . import data_loaders as gv_loaders
```

(Add this import near the top of the file, after line 14.)

Replace the `get_medicare_enrollment` function (lines 113-166) with:
```python
@mcp.tool()
async def get_medicare_enrollment(state: str | None = None, county_fips: str | None = None) -> str:
    """Get Medicare enrollment and spending data from the CMS Geographic Variation PUF.

    Provide either a state abbreviation or county FIPS code.

    Args:
        state: Two-letter state abbreviation (e.g., "IL"). Returns state-level data.
        county_fips: 5-digit county FIPS code (e.g., "17031" for Cook County, IL). Returns county-level data.
    """
    if not state and not county_fips:
        return json.dumps({"error": "Provide either 'state' or 'county_fips'"})

    try:
        await gv_loaders.ensure_gv_cached()

        if county_fips:
            data = gv_loaders.query_gv("County", county_fips)
            geo_type, geo_code = "county", county_fips
        else:
            data = gv_loaders.query_gv("State", state.upper())
            geo_type, geo_code = "state", state.upper()

        if not data:
            return json.dumps({"error": f"No Medicare data found for {geo_type} {geo_code}"})

        result = MedicareEnrollment(
            geography_type=geo_type,
            geography_code=geo_code,
            geography_name=data.get("geo_desc", ""),
            total_beneficiaries=data.get("total_beneficiaries"),
            ma_penetration_pct=data.get("ma_penetration_pct"),
            avg_age=data.get("avg_age"),
            pct_female=data.get("pct_female"),
            pct_dual_eligible=data.get("pct_dual_eligible"),
            pct_a_b_coverage=None,
            per_capita_spending=data.get("per_capita_spending"),
        )
        return result.model_dump_json(indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})
```

**Step 3: Rewrite get_geographic_variation**

Replace lines 169-229 with:
```python
@mcp.tool()
async def get_geographic_variation(geography_type: str = "county", geography_code: str | None = None) -> str:
    """Get CMS Geographic Variation PUF data including spending and utilization.

    Returns demographics, per-capita spending breakdown (IP, OP, physician, SNF),
    utilization rates (discharges, ER visits per 1000), and readmission rate.

    Args:
        geography_type: "county" (FIPS code) or "state" (abbreviation)
        geography_code: County FIPS (e.g., "17031") or state abbreviation (e.g., "IL")
    """
    if not geography_code:
        return json.dumps({"error": "geography_code is required"})

    try:
        await gv_loaders.ensure_gv_cached()

        geo_level = "County" if geography_type == "county" else "State"
        code = geography_code.upper() if geography_type != "county" else geography_code
        data = gv_loaders.query_gv(geo_level, code)

        if not data:
            return json.dumps({"error": f"No data found for {geography_type} {geography_code}"})

        result = GeographicVariation(
            geography_type=geography_type,
            geography_code=geography_code,
            geography_name=data.get("geo_desc", ""),
            total_beneficiaries=data.get("total_beneficiaries"),
            avg_age=data.get("avg_age"),
            pct_female=data.get("pct_female"),
            pct_dual_eligible=data.get("pct_dual_eligible"),
            per_capita_spending=data.get("per_capita_spending"),
            ip_spending_per_capita=data.get("ip_spending_per_capita"),
            op_spending_per_capita=data.get("op_spending_per_capita"),
            physician_spending_per_capita=data.get("physician_spending_per_capita"),
            snf_spending_per_capita=data.get("snf_spending_per_capita"),
            discharges_per_1000=data.get("discharges_per_1000"),
            er_visits_per_1000=data.get("er_visits_per_1000"),
            readmission_rate=data.get("readmission_rate"),
        )
        return result.model_dump_json(indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})
```

**Step 4: Smoke test**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && source .venv/bin/activate && python3 -c "import asyncio; from servers.geo_demographics.data_loaders import ensure_gv_cached, query_gv; asyncio.run(ensure_gv_cached()); print(query_gv('State', 'PA'))"`

Expected: Dict with Pennsylvania Medicare data (total_beneficiaries, per_capita_spending, etc.)

Then test county: `python3 -c "from servers.geo_demographics.data_loaders import query_gv; print(query_gv('County', '42101'))"`

Expected: Dict with Philadelphia County data.

**Step 5: Commit**

```bash
git add servers/geo-demographics/data_loaders.py servers/geo-demographics/server.py
git commit -m "fix(geo-demographics): replace retired Socrata API with bulk CSV + DuckDB

CMS retired the data.cms.gov Socrata endpoints (nw2u-nkit, jhbs-ydf5)
returning 410 Gone. Now downloads the full GV PUF CSV, caches as
Parquet, and queries via DuckDB — matching the pattern used by other
servers."
```

---

### Task 3: Fix Cost Report PUF 404

**Files:**
- Modify: `servers/cms-facility/data_loaders.py:65-67` (URL update)
- Modify: `servers/hospital-quality/data_loaders.py:85-105` (URL update)

**Step 1: Update cms-facility cost report URL**

In `servers/cms-facility/data_loaders.py`, replace lines 64-67:

Old:
```python
    # CMS Cost Report data — try the Provider Data Catalog endpoint
    cost_report_url = (
        "https://data.cms.gov/provider-data/api/1/datastore/query/di4u-7yu6/0/download?format=csv"
    )
```

New:
```python
    # CMS Cost Report PUF — direct CSV download (2023 Final, published Jan 2026)
    cost_report_url = (
        "https://data.cms.gov/sites/default/files/2026-01/"
        "3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv"
    )
```

Also, the Cost Report CSV column names after normalization are like `provider_ccn`, `number_of_beds`, `fiscal_year_end_date`, etc. The existing column lookups in `server.py` already try `provider_ccn` as a candidate (line 221), so they should match. But the column for fiscal year end will be `fiscal_year_end_date` — the existing lookup tries `fiscal_year_end` and `fiscal_year_end_date` (line 230), so that should also match. No column mapping changes needed in the server.

**Additionally**: Delete any stale cached file so the new URL is fetched:

Run: `rm -f ~/.healthcare-data-mcp/cache/hospital_cost_report.csv`

**Step 2: Update hospital-quality cost report URL**

In `servers/hospital-quality/data_loaders.py`, replace lines 85-105 (`load_cost_report` function):

Old (line 95):
```python
    url = _csv_url("di4u-7yu6")
```

New — replace the entire function to use a direct URL instead of the dead Provider Data API:
```python
async def load_cost_report() -> pd.DataFrame:
    """Load CMS Hospital Cost Report data for financial profiling.

    Uses the CMS Cost Report PUF direct CSV download.
    Falls back to empty DataFrame if unavailable.
    """
    key = "cost_report"
    if key in _df_cache:
        return _df_cache[key]

    url = (
        "https://data.cms.gov/sites/default/files/2026-01/"
        "3c39f483-c7e0-4025-8396-4df76942e10f/CostReport_2023_Final.csv"
    )
    try:
        path = await cms_download_csv(url, cache_key="hospital_quality_cost_report")
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        _df_cache[key] = df
        return df
    except Exception:
        logger.warning("Could not load cost report data — returning empty DataFrame", exc_info=True)
        _df_cache[key] = pd.DataFrame()
        return _df_cache[key]
```

Delete stale cache: `rm -f ~/.healthcare-data-mcp/cache/*cost_report*`

**Step 3: Smoke test**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && source .venv/bin/activate && python3 -c "
import asyncio
from servers.cms_facility import data_loaders
df = asyncio.run(data_loaders.load_cost_report())
print(f'Rows: {len(df)}, Columns: {list(df.columns)[:10]}')
matches = df[df['provider_ccn'].str.strip() == '390174']
print(f'Jefferson matches: {len(matches)}')
if not matches.empty:
    r = matches.iloc[0]
    print(f'Beds: {r.get(\"number_of_beds\")}, Discharges: {r.get(\"total_discharges_title_xviii\")}')
"`

Expected: Non-empty DataFrame, Jefferson found with bed count and discharge data.

**Step 4: Commit**

```bash
git add servers/cms-facility/data_loaders.py servers/hospital-quality/data_loaders.py
git commit -m "fix(cost-report): update Cost Report PUF URL from dead di4u-7yu6 to 2023 CSV

The Provider Data Catalog endpoint for di4u-7yu6 returns 404. CMS now
publishes annual Cost Report CSVs directly. Updated both cms-facility
and hospital-quality servers to use CostReport_2023_Final.csv."
```

---

### Task 4: Fix NLRB Union Activity Parser

**Files:**
- Modify: `servers/workforce-analytics/labor_data.py:88-179` (rewrite `search_nlrb_elections`)

**Step 1: Rewrite search_nlrb_elections with explicit JOINs**

Replace the `search_nlrb_elections` function (lines 88-179) in `servers/workforce-analytics/labor_data.py`:

```python
def search_nlrb_elections(
    employer_name: str = "",
    state: str = "",
    year_start: int = 2015,
    year_end: int = 2026,
    limit: int = 50,
) -> list[dict]:
    """Search NLRB election records, filtered to healthcare employers.

    Joins filing (employer info) + election (dates/unit size) + participant (union name).
    """
    if not _NLRB_DB.exists():
        return []

    try:
        con = sqlite3.connect(str(_NLRB_DB))
        con.row_factory = sqlite3.Row

        where_parts = []
        params: list = []

        if employer_name:
            where_parts.append("LOWER(f.name) LIKE ?")
            params.append(f"%{employer_name.lower()}%")

        if state:
            where_parts.append("UPPER(f.state) = ?")
            params.append(state.upper())

        where_parts.append("SUBSTR(f.date_filed, 1, 4) BETWEEN ? AND ?")
        params.extend([str(year_start), str(year_end)])

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        query = f"""
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
            LEFT JOIN (
                SELECT case_number, name
                FROM participant
                WHERE role = 'Petitioner' AND type = 'Union'
                GROUP BY case_number
            ) p ON f.case_number = p.case_number
            WHERE {where_clause}
            ORDER BY f.date_filed DESC
            LIMIT ?
        """
        # If no specific employer, overfetch for healthcare filtering
        fetch_limit = limit * 3 if not employer_name else limit
        params.append(fetch_limit)

        rows = con.execute(query, params).fetchall()
        con.close()

        results = []
        for row in rows:
            r = dict(row)
            name = r.get("employer", "")

            # Filter to healthcare if no specific employer search
            if not employer_name and not _is_healthcare_employer(name):
                continue

            results.append({
                "case_number": r.get("case_number", ""),
                "employer": name,
                "union": r.get("union_name", "") or "",
                "date": r.get("election_date", "") or r.get("date_filed", ""),
                "result": r.get("status", ""),
                "unit_size": int(r.get("unit_size") or r.get("number_of_eligible_voters") or 0),
                "city": r.get("city", ""),
                "state": r.get("state", ""),
            })

            if len(results) >= limit:
                break

        return results

    except Exception as e:
        logger.warning("NLRB query failed: %s", e)
        return []
```

**Step 2: Smoke test**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && source .venv/bin/activate && python3 -c "
from servers.workforce_analytics.labor_data import search_nlrb_elections
results = search_nlrb_elections(employer_name='Jefferson Health', state='PA')
for r in results[:3]:
    print(r)
"`

Expected: Results with populated employer, city, state, date, union fields. Should find Jefferson Health filings.

**Step 3: Commit**

```bash
git add servers/workforce-analytics/labor_data.py
git commit -m "fix(workforce): rewrite NLRB parser with explicit table JOINs

The dynamic schema detection was querying the election table which lacks
employer/union/city/state columns. Now JOINs filing + election +
participant tables for complete data."
```

---

### Task 5: Fix Market Share Missing Facility Names

**Files:**
- Modify: `servers/service-area/service_area_engine.py:133-164` (add cross-reference)
- Modify: `servers/service-area/data_loaders.py` (add Hospital General Info loader)

**Step 1: Add Hospital General Info loader to data_loaders.py**

Add at end of `servers/service-area/data_loaders.py` (after line 164):

```python
# Hospital General Info for facility name cross-reference
_HOSP_INFO_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv"
_HOSP_INFO_CACHE = CACHE_DIR / "hospital_general_info.csv"


async def load_hospital_names() -> dict[str, str]:
    """Load a CCN → facility name mapping from CMS Hospital General Info.

    Returns dict mapping CCN strings to facility name strings.
    """
    if not _HOSP_INFO_CACHE.exists():
        logger.info("Downloading Hospital General Info for name lookup...")
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(_HOSP_INFO_URL)
            resp.raise_for_status()
            _HOSP_INFO_CACHE.write_bytes(resp.content)

    df = pd.read_csv(_HOSP_INFO_CACHE, dtype=str, keep_default_na=False,
                      usecols=lambda c: c.strip() in ("Facility ID", "Facility Name"))
    return dict(zip(
        df["Facility ID"].str.strip(),
        df["Facility Name"].str.strip(),
    ))
```

**Step 2: Add name enrichment in service_area_engine.py**

In `servers/service-area/service_area_engine.py`, modify the `compute_market_share` function to accept an optional name lookup dict. The function signature (line 133) becomes:

```python
def compute_market_share(hsaf_df: pd.DataFrame, zip_code: str, limit: int = 20,
                         name_lookup: dict[str, str] | None = None) -> dict:
```

Then after line 151 (the agg groupby), add name enrichment:

```python
    # Enrich with facility names from external lookup if available
    if name_lookup:
        agg["facility_name"] = agg["ccn"].map(
            lambda c: name_lookup.get(c, "")
        ).fillna("")
```

**Step 3: Update the server.py call site**

Find where `compute_market_share` is called in `servers/service-area/server.py` and pass the name lookup.

Read the server.py to find the call:

The call is in the `get_market_share` tool function. Update it to load names and pass them:

```python
@mcp.tool()
async def get_market_share(zip_code: str, limit: int = 20) -> str:
    ...
    hsaf_df = await data_loaders.download_hsaf()
    name_lookup = await data_loaders.load_hospital_names()
    result = service_area_engine.compute_market_share(hsaf_df, zip_code, limit,
                                                       name_lookup=name_lookup)
    ...
```

**Step 4: Smoke test**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && source .venv/bin/activate && python3 -c "
import asyncio
from servers.service_area import data_loaders
from servers.service_area.service_area_engine import compute_market_share
async def test():
    hsaf = await data_loaders.download_hsaf()
    names = await data_loaders.load_hospital_names()
    result = compute_market_share(hsaf, '19107', name_lookup=names)
    for h in result['hospitals'][:5]:
        print(f'{h[\"ccn\"]} | {h[\"facility_name\"]:45s} | {h[\"market_share_pct\"]}%')
asyncio.run(test())
"`

Expected: Facility names populated (e.g., "THOMAS JEFFERSON UNIVERSITY HOSPITAL").

**Step 5: Commit**

```bash
git add servers/service-area/data_loaders.py servers/service-area/service_area_engine.py servers/service-area/server.py
git commit -m "fix(service-area): enrich market share results with facility names

HSAF CSV lacks a facility name column. Now cross-references CCNs against
CMS Hospital General Info to populate facility names in market share
results."
```

---

### Task 6: Fix find_competing_facilities Geocoding

**Files:**
- Modify: `servers/drive-time/server.py:100-127` (update `_parse_lat_lon` fallback)

**Step 1: Add ZIP centroid data source**

Add a centroid loader at the module level in `servers/drive-time/server.py` (near the top, after the imports/constants section):

```python
# Census Gazetteer ZIP centroid file (~1MB, tab-delimited)
GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_zcta_national.txt"
_ZIP_CENTROIDS: dict[str, tuple[float, float]] | None = None


async def _ensure_zip_centroids() -> dict[str, tuple[float, float]]:
    """Load ZIP → (lat, lon) centroid mapping from Census Gazetteer."""
    global _ZIP_CENTROIDS
    if _ZIP_CENTROIDS is not None:
        return _ZIP_CENTROIDS

    cache_path = os.path.join(CACHE_DIR, "zip_centroids.csv")
    if not os.path.exists(cache_path):
        logger.info("Downloading Census ZIP centroids...")
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(GAZETTEER_URL)
            resp.raise_for_status()
            with open(cache_path, "wb") as f:
                f.write(resp.content)

    df = pd.read_csv(cache_path, sep="\t", dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    _ZIP_CENTROIDS = {}
    for _, row in df.iterrows():
        z = str(row.get("GEOID", "")).strip().zfill(5)
        try:
            lat = float(row.get("INTPTLAT", 0))
            lon = float(row.get("INTPTLONG", row.get("INTPTLON", 0)))
            if lat and lon:
                _ZIP_CENTROIDS[z] = (lat, lon)
        except (ValueError, TypeError):
            pass

    logger.info("Loaded %d ZIP centroids", len(_ZIP_CENTROIDS))
    return _ZIP_CENTROIDS
```

**Step 2: Update _parse_lat_lon to use centroid fallback**

Replace `_parse_lat_lon` (lines 100-127) with:

```python
async def _parse_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    """Extract lat/lon columns from the facility DataFrame.

    Tries direct lat/lon columns first, then JSON 'location' column,
    then falls back to ZIP code centroid geocoding.
    """
    if "latitude" in df.columns and "longitude" in df.columns:
        df = df.copy()
        df["_lat"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["_lon"] = pd.to_numeric(df["longitude"], errors="coerce")
        return df

    if "location" in df.columns:
        df = df.copy()

        def _extract(val: str, key: str) -> float | None:
            try:
                obj = json.loads(val)
                return float(obj[key])
            except Exception:
                return None

        df["_lat"] = df["location"].apply(lambda v: _extract(v, "latitude"))
        df["_lon"] = df["location"].apply(lambda v: _extract(v, "longitude"))
        return df

    # Fallback: geocode via ZIP centroid
    zip_col = next((c for c in df.columns if c in ("zip_code", "zip", "zipcode")), None)
    if zip_col:
        centroids = await _ensure_zip_centroids()
        df = df.copy()
        df["_lat"] = df[zip_col].apply(
            lambda z: centroids.get(str(z).strip().zfill(5), (None, None))[0]
        )
        df["_lon"] = df[zip_col].apply(
            lambda z: centroids.get(str(z).strip().zfill(5), (None, None))[1]
        )
        logger.info("Geocoded %d/%d facilities via ZIP centroids",
                     df["_lat"].notna().sum(), len(df))
        return df

    raise ValueError("Cannot find latitude/longitude columns in facility data")
```

Note: This function is now `async` since it may need to download centroids. Update the call site in `find_competing_facilities` — the `_parse_lat_lon` call needs `await`.

Find the call in `find_competing_facilities` and add `await`:

```python
# Old:
df = _parse_lat_lon(df)
# New:
df = await _parse_lat_lon(df)
```

**Step 3: Smoke test**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && source .venv/bin/activate && python3 -c "
import asyncio, json
from servers.drive_time.server import mcp
# Test the tool via direct invocation
print('Tool registered:', 'find_competing_facilities' in [t for t in mcp._tool_manager._tools])
"`

Expected: `True`

Full integration test requires OSRM running, so just verify the centroid loader works:

Run: `python3 -c "
import asyncio
from servers.drive_time.server import _ensure_zip_centroids
centroids = asyncio.run(_ensure_zip_centroids())
print(f'Total centroids: {len(centroids)}')
print(f'Philly 19107: {centroids.get(\"19107\")}')
"`

Expected: ~33,000+ centroids loaded, 19107 returns (lat, lon) near (39.95, -75.16).

**Step 4: Commit**

```bash
git add servers/drive-time/server.py
git commit -m "fix(drive-time): add ZIP centroid geocoding fallback for facilities

CMS Hospital General Info CSV has no lat/lon columns. Now falls back to
Census Gazetteer ZIP centroids (~33K ZIPs, ~1MB download) when direct
coordinates are unavailable."
```

---

## Verification: Re-run Golden Path Test

After all 6 fixes are implemented, re-run the Jefferson Health golden-path test:

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && source .venv/bin/activate
python3 << 'EOF'
import asyncio, json

async def test():
    # Fix 2: Cost Report
    from servers.cms_facility import data_loaders as cms_dl
    df = await cms_dl.load_cost_report()
    print(f"Cost Report: {len(df)} rows, Jefferson: {len(df[df.get('provider_ccn','').str.strip()=='390174']) if 'provider_ccn' in df.columns else 'col missing'}")

    # Fix 1: Geographic Variation
    from servers.geo_demographics.data_loaders import ensure_gv_cached, query_gv
    await ensure_gv_cached()
    pa = query_gv("State", "PA")
    print(f"GV State PA: benes={pa.get('total_beneficiaries') if pa else 'FAIL'}")
    philly = query_gv("County", "42101")
    print(f"GV County 42101: benes={philly.get('total_beneficiaries') if philly else 'FAIL'}")

    # Fix 4: NLRB
    from servers.workforce_analytics.labor_data import search_nlrb_elections
    nlrb = search_nlrb_elections(employer_name="Jefferson", state="PA")
    print(f"NLRB Jefferson PA: {len(nlrb)} results, first employer: {nlrb[0]['employer'] if nlrb else 'NONE'}")

    # Fix 5: Market share names
    from servers.service_area import data_loaders as sa_dl
    from servers.service_area.service_area_engine import compute_market_share
    hsaf = await sa_dl.download_hsaf()
    names = await sa_dl.load_hospital_names()
    ms = compute_market_share(hsaf, "19107", name_lookup=names)
    top = ms["hospitals"][0] if ms["hospitals"] else {}
    print(f"Market Share 19107: top={top.get('facility_name','EMPTY')} ({top.get('market_share_pct')}%)")

asyncio.run(test())
EOF
```

Expected: All lines show real data, no FAIL/EMPTY/NONE.
