# Public Records & Regulatory Server — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build MCP server 12 (port 8013) with 6 tools for public regulatory, accreditation, federal spending, and compliance data.

**Architecture:** Single FastMCP server with two data access patterns: (A) auto-download bulk CSV→Parquet for CMS datasets, (B) manual-seed file ingestion for 340B/breach data, plus direct REST API clients for USAspending and SAM.gov.

**Tech Stack:** Python 3.12, FastMCP, httpx, pandas, duckdb, pydantic

---

## Task 1: Directory Structure

**Files:**
- Create: `servers/public_records/__init__.py`
- Create: `servers/public_records/data/` (directory)

**Step 1: Create directory and init file**

```bash
mkdir -p servers/public_records/data
touch servers/public_records/__init__.py
```

**Step 2: Verify structure**

```bash
ls -la servers/public_records/
```

Expected: `__init__.py` and `data/` directory.

**Step 3: Commit**

```bash
git add servers/public_records/
git commit -m "chore: scaffold public_records server directory"
```

---

## Task 2: Static Data — Accreditation Codes

**Files:**
- Create: `servers/public_records/data/accreditation_codes.csv`

The CMS POS file uses `ACRDTN_TYPE_CD` numeric codes to identify accrediting organizations. We need a static lookup table.

**Step 1: Create accreditation codes CSV**

```csv
code,organization,abbreviation
0,No Accreditation,NONE
1,The Joint Commission,TJC
2,American Osteopathic Association,AOA
3,DNV Healthcare,DNV
4,Commission on Accreditation of Rehabilitation Facilities,CARF
5,Community Health Accreditation Partner,CHAP
6,Accreditation Commission for Health Care,ACHC
7,Center for Improvement in Healthcare Quality,CIHQ
9,Other/Unknown,OTHER
```

Note: These codes are from the CMS POS data dictionary. The exact code values should be verified against the POS record layout documentation, but these are the standard CMS-approved accrediting organizations.

**Step 2: Commit**

```bash
git add servers/public_records/data/
git commit -m "feat(public-records): add accreditation code lookup table"
```

---

## Task 3: Pydantic Response Models

**Files:**
- Create: `servers/public_records/models.py`

One response model per tool, plus nested detail models.

**Models needed:**

```python
"""Pydantic response models for the public-records server."""

from pydantic import BaseModel, Field


# ---------- Tool 1: search_usaspending ----------

class USAspendingAward(BaseModel):
    award_id: str = ""
    recipient_name: str = ""
    awarding_agency: str = ""
    awarding_sub_agency: str = ""
    award_type: str = ""
    total_obligation: float = 0.0
    description: str = ""
    start_date: str = ""
    end_date: str = ""
    naics_code: str = ""
    naics_description: str = ""

class USAspendingResponse(BaseModel):
    recipient_search: str = ""
    fiscal_year: str = ""
    total_awards: int = 0
    total_obligation: float = 0.0
    awards: list[USAspendingAward] = Field(default_factory=list)


# ---------- Tool 2: search_sam_gov ----------

class SAMOpportunity(BaseModel):
    notice_id: str = ""
    title: str = ""
    solicitation_number: str = ""
    department: str = ""
    sub_tier: str = ""
    posted_date: str = ""
    response_deadline: str = ""
    naics_code: str = ""
    set_aside_type: str = ""
    description: str = ""
    active: bool = True

class SAMResponse(BaseModel):
    keyword: str = ""
    total_results: int = 0
    opportunities: list[SAMOpportunity] = Field(default_factory=list)


# ---------- Tool 3: get_340b_status ----------

class CoveredEntity340B(BaseModel):
    entity_id: str = ""
    entity_name: str = ""
    entity_type: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    grant_number: str = ""
    participating: bool = True
    contract_pharmacy_count: int = 0

class Status340BResponse(BaseModel):
    search_term: str = ""
    total_results: int = 0
    entities: list[CoveredEntity340B] = Field(default_factory=list)


# ---------- Tool 4: get_breach_history ----------

class BreachRecord(BaseModel):
    entity_name: str = ""
    state: str = ""
    covered_entity_type: str = ""
    individuals_affected: int = 0
    breach_submission_date: str = ""
    breach_type: str = ""
    location_of_breached_info: str = ""
    business_associate_present: str = ""
    web_description: str = ""

class BreachHistoryResponse(BaseModel):
    search_entity: str = ""
    total_breaches: int = 0
    total_individuals_affected: int = 0
    breaches: list[BreachRecord] = Field(default_factory=list)


# ---------- Tool 5: get_accreditation ----------

class AccreditationRecord(BaseModel):
    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    city: str = ""
    accreditation_org: str = ""
    accreditation_type_code: str = ""
    accreditation_effective_date: str = ""
    accreditation_expiration_date: str = ""
    certification_date: str = ""
    ownership_type: str = ""
    bed_count: int = 0
    medicare_medicaid: str = ""
    compliance_status: str = ""

class AccreditationResponse(BaseModel):
    search_term: str = ""
    total_results: int = 0
    providers: list[AccreditationRecord] = Field(default_factory=list)


# ---------- Tool 6: get_interop_status ----------

class InteropRecord(BaseModel):
    facility_name: str = ""
    ccn: str = ""
    state: str = ""
    city: str = ""
    meets_pi_criteria: str = ""
    cehrt_id: str = ""
    reporting_period_start: str = ""
    reporting_period_end: str = ""
    ehr_product_name: str = ""
    ehr_developer: str = ""

class InteropResponse(BaseModel):
    search_term: str = ""
    total_results: int = 0
    records: list[InteropRecord] = Field(default_factory=list)
```

**Step 1: Create models.py with the above content.**

**Step 2: Verify imports**

```bash
cd /mnt/d/Coding\ Projects/healthcare-data-mcp
python3 -c "from servers.public_records.models import USAspendingResponse, SAMResponse, Status340BResponse, BreachHistoryResponse, AccreditationResponse, InteropResponse; print('OK')"
```

**Step 3: Commit**

```bash
git add servers/public_records/models.py
git commit -m "feat(public-records): add Pydantic response models for 6 tools"
```

---

## Task 4: Data Loaders — Bulk CSV Download/Cache

**Files:**
- Create: `servers/public_records/data_loaders.py`

Handles auto-download and caching for CMS Provider of Services and CMS Promoting Interoperability datasets, plus manual-seed file detection for 340B and breach data.

**Key functions:**

```python
"""Bulk data loaders for CMS POS, CMS PI, and manual-seed files.

Downloads CSV files from data.cms.gov, converts to Parquet with zstd,
and queries with DuckDB. Also loads user-provided 340B and breach files.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "public-records"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_CACHE_TTL_DAYS = 90
_API_CACHE_TTL_DAYS = 7

# --- CMS Provider of Services (POS) ---
POS_URL = "https://data.cms.gov/sites/default/files/2026-01/c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv"
POS_PARQUET = _CACHE_DIR / "pos_hospital.parquet"

# --- CMS Promoting Interoperability ---
PI_URL = "https://data.cms.gov/provider-data/sites/default/files/resources/5462b19a756c53c1becccf13787d9157_1770163678/Promoting_Interoperability-Hospital.csv"
PI_PARQUET = _CACHE_DIR / "promoting_interop.parquet"

# --- Manual-seed file paths ---
OPAIS_340B_JSON = _CACHE_DIR / "340b_covered_entities.json"
OPAIS_340B_PARQUET = _CACHE_DIR / "340b_covered_entities.parquet"
BREACH_CSV = _CACHE_DIR / "hipaa_breaches.csv"
BREACH_PARQUET = _CACHE_DIR / "hipaa_breaches.parquet"


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


async def _download_csv_to_parquet(url: str, parquet_path: Path, name: str) -> bool:
    """Download CSV from URL and cache as Parquet."""
    logger.info("Downloading %s from %s ...", name, url[:80])
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        csv_path = _CACHE_DIR / f"{parquet_path.stem}_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(parquet_path, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("%s cached: %d rows -> %s", name, len(df), parquet_path.name)
        return True
    except Exception as e:
        logger.warning("Failed to download %s: %s", name, e)
        return False


async def ensure_pos_cached() -> bool:
    """Ensure CMS POS file is downloaded and cached as Parquet."""
    if _is_cache_valid(POS_PARQUET):
        return True
    return await _download_csv_to_parquet(POS_URL, POS_PARQUET, "CMS POS File")


async def ensure_pi_cached() -> bool:
    """Ensure CMS Promoting Interoperability file is cached."""
    if _is_cache_valid(PI_PARQUET):
        return True
    return await _download_csv_to_parquet(PI_URL, PI_PARQUET, "CMS Promoting Interop")


def ensure_340b_loaded() -> bool:
    """Check if 340B data is available (manual-seed).

    Looks for the JSON file and converts to Parquet on first load.
    """
    if _is_cache_valid(OPAIS_340B_PARQUET):
        return True
    if not OPAIS_340B_JSON.exists():
        return False
    try:
        data = json.loads(OPAIS_340B_JSON.read_text(encoding="utf-8"))
        # The OPAIS JSON is a nested structure; flatten to records
        records = _flatten_340b_json(data)
        df = pd.DataFrame(records)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(OPAIS_340B_PARQUET, compression="zstd", index=False)
        logger.info("340B data cached: %d entities -> %s", len(df), OPAIS_340B_PARQUET.name)
        return True
    except Exception as e:
        logger.warning("Failed to load 340B JSON: %s", e)
        return False


def _flatten_340b_json(data: list | dict) -> list[dict]:
    """Flatten OPAIS JSON export into flat records.

    The OPAIS JSON groups all data per 340B ID. This extracts
    entity-level fields into flat dicts.
    """
    records: list[dict] = []
    items = data if isinstance(data, list) else data.get("entities", data.get("records", [data]))
    for item in items:
        record = {}
        for key, val in item.items():
            if isinstance(val, (str, int, float, bool)):
                record[key] = str(val)
            elif isinstance(val, list):
                # Count nested items (e.g., contract pharmacies)
                record[f"{key}_count"] = len(val)
        records.append(record)
    return records


def ensure_breach_loaded() -> bool:
    """Check if breach data is available (manual-seed CSV)."""
    if _is_cache_valid(BREACH_PARQUET):
        return True
    if not BREACH_CSV.exists():
        return False
    try:
        df = pd.read_csv(BREACH_CSV, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(BREACH_PARQUET, compression="zstd", index=False)
        logger.info("Breach data cached: %d records -> %s", len(df), BREACH_PARQUET.name)
        return True
    except Exception as e:
        logger.warning("Failed to load breach CSV: %s", e)
        return False


def _get_con(parquet_path: Path, view_name: str = "data") -> duckdb.DuckDBPyConnection | None:
    """Create DuckDB connection with a view for a Parquet file.

    Handles corrupt Parquet by deleting the cache and returning None.
    """
    if not parquet_path.exists():
        return None
    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}')")
        return con
    except Exception:
        logger.warning("Corrupt Parquet cache, deleting: %s", parquet_path)
        con.close()
        parquet_path.unlink(missing_ok=True)
        return None


def query_pos(ccn: str = "", provider_name: str = "", state: str = "") -> list[dict]:
    """Query cached CMS POS file."""
    con = _get_con(POS_PARQUET)
    if con is None:
        return []
    try:
        # Detect column names (POS has 473 columns, names vary by quarter)
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='data'"
        ).fetchall()]

        ccn_col = next((c for c in cols if c in ("prvdr_num", "provider_number", "ccn")), None)
        name_col = next((c for c in cols if c in ("fac_name", "facility_name", "prvdr_name", "provider_name")), None)
        state_col = next((c for c in cols if c in ("state_cd", "state_code", "state")), None)
        city_col = next((c for c in cols if c in ("city_name", "city")), None)
        accr_type_col = next((c for c in cols if c in ("acrdtn_type_cd", "accreditation_type_code")), None)
        accr_eff_col = next((c for c in cols if c in ("acrdtn_efctv_dt", "accreditation_effective_date")), None)
        accr_exp_col = next((c for c in cols if c in ("acrdtn_exprtn_dt", "accreditation_expiration_date")), None)
        cert_dt_col = next((c for c in cols if c in ("crtfctn_dt", "certification_date")), None)
        ctrl_type_col = next((c for c in cols if c in ("gnrl_cntl_type_cd", "control_type")), None)
        bed_col = next((c for c in cols if c in ("bed_cnt", "bed_count", "crtfd_bed_cnt")), None)
        pgm_col = next((c for c in cols if c in ("pgm_prtcptn_cd", "program_participation")), None)
        cmpl_col = next((c for c in cols if c in ("cmplnc_stus_cd", "compliance_status")), None)

        if not ccn_col:
            return []

        where_parts: list[str] = []
        params: list[str] = []

        if ccn:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        if provider_name:
            where_parts.append(f"LOWER({name_col}) LIKE ?")
            params.append(f"%{provider_name.strip().lower()}%")
        if state and state_col:
            where_parts.append(f"TRIM({state_col}) = ?")
            params.append(state.strip().upper())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        df = con.execute(f"SELECT * FROM data WHERE {where} LIMIT 100", params).fetchdf()

        def _s(row: pd.Series, col: str | None) -> str:
            return str(row.get(col, "")).strip() if col and col in row.index else ""

        def _i(row: pd.Series, col: str | None) -> int:
            v = _s(row, col)
            try:
                return int(float(v)) if v else 0
            except ValueError:
                return 0

        results: list[dict] = []
        for _, row in df.iterrows():
            results.append({
                "ccn": _s(row, ccn_col),
                "provider_name": _s(row, name_col),
                "state": _s(row, state_col),
                "city": _s(row, city_col),
                "accreditation_type_code": _s(row, accr_type_col),
                "accreditation_effective_date": _s(row, accr_eff_col),
                "accreditation_expiration_date": _s(row, accr_exp_col),
                "certification_date": _s(row, cert_dt_col),
                "ownership_type": _s(row, ctrl_type_col),
                "bed_count": _i(row, bed_col),
                "medicare_medicaid": _s(row, pgm_col),
                "compliance_status": _s(row, cmpl_col),
            })
        return results
    except Exception as e:
        logger.warning("POS query failed: %s", e)
        return []
    finally:
        con.close()


def query_pi(ccn: str = "", facility_name: str = "", state: str = "") -> list[dict]:
    """Query cached CMS Promoting Interoperability file."""
    con = _get_con(PI_PARQUET)
    if con is None:
        return []
    try:
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='data'"
        ).fetchall()]

        ccn_col = next((c for c in cols if c in ("facility_id", "ccn", "provider_ccn")), None)
        name_col = next((c for c in cols if c in ("facility_name", "hospital_name")), None)
        state_col = next((c for c in cols if c in ("state", "state_code")), None)
        city_col = next((c for c in cols if c in ("city/town", "city_town", "city")), None)
        meets_col = next((c for c in cols if "meets" in c or "promoting" in c.lower()), None)
        cehrt_col = next((c for c in cols if "cehrt" in c.lower()), None)
        start_col = next((c for c in cols if c in ("start_date", "start")), None)
        end_col = next((c for c in cols if c in ("end_date", "end")), None)

        if not ccn_col:
            return []

        where_parts: list[str] = []
        params: list[str] = []

        if ccn:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        if facility_name and name_col:
            where_parts.append(f"LOWER({name_col}) LIKE ?")
            params.append(f"%{facility_name.strip().lower()}%")
        if state and state_col:
            where_parts.append(f"TRIM({state_col}) = ?")
            params.append(state.strip().upper())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        df = con.execute(f"SELECT * FROM data WHERE {where} LIMIT 100", params).fetchdf()

        def _s(row: pd.Series, col: str | None) -> str:
            return str(row.get(col, "")).strip() if col and col in row.index else ""

        results: list[dict] = []
        for _, row in df.iterrows():
            results.append({
                "ccn": _s(row, ccn_col),
                "facility_name": _s(row, name_col),
                "state": _s(row, state_col),
                "city": _s(row, city_col),
                "meets_pi_criteria": _s(row, meets_col),
                "cehrt_id": _s(row, cehrt_col),
                "reporting_period_start": _s(row, start_col),
                "reporting_period_end": _s(row, end_col),
            })
        return results
    except Exception as e:
        logger.warning("PI query failed: %s", e)
        return []
    finally:
        con.close()


def query_340b(entity_name: str = "", entity_id: str = "", state: str = "") -> list[dict]:
    """Query cached 340B data."""
    con = _get_con(OPAIS_340B_PARQUET)
    if con is None:
        return []
    try:
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='data'"
        ).fetchall()]

        # Build query dynamically based on available columns
        where_parts: list[str] = []
        params: list[str] = []

        # Search by name (try common column names)
        name_col = next((c for c in cols if "name" in c and "entity" in c), next((c for c in cols if "name" in c), None))
        id_col = next((c for c in cols if "340b" in c.lower() or "id" in c.lower()), None)
        state_col = next((c for c in cols if c in ("state", "state_code")), None)

        if entity_name and name_col:
            where_parts.append(f"LOWER({name_col}) LIKE ?")
            params.append(f"%{entity_name.strip().lower()}%")
        if entity_id and id_col:
            where_parts.append(f"TRIM({id_col}) = ?")
            params.append(entity_id.strip())
        if state and state_col:
            where_parts.append(f"TRIM({state_col}) = ?")
            params.append(state.strip().upper())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        df = con.execute(f"SELECT * FROM data WHERE {where} LIMIT 100", params).fetchdf()
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning("340B query failed: %s", e)
        return []
    finally:
        con.close()


def query_breaches(entity_name: str = "", state: str = "", min_individuals: int = 0) -> list[dict]:
    """Query cached HIPAA breach data."""
    con = _get_con(BREACH_PARQUET)
    if con is None:
        return []
    try:
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='data'"
        ).fetchall()]

        name_col = next((c for c in cols if "name" in c and "entity" in c), next((c for c in cols if "name" in c), None))
        state_col = next((c for c in cols if c in ("state", "state_code")), None)
        indiv_col = next((c for c in cols if "individuals" in c.lower() or "affected" in c.lower()), None)

        where_parts: list[str] = []
        params: list = []

        if entity_name and name_col:
            where_parts.append(f"LOWER({name_col}) LIKE ?")
            params.append(f"%{entity_name.strip().lower()}%")
        if state and state_col:
            where_parts.append(f"TRIM({state_col}) = ?")
            params.append(state.strip().upper())
        if min_individuals > 0 and indiv_col:
            where_parts.append(f"TRY_CAST({indiv_col} AS INTEGER) >= ?")
            params.append(min_individuals)

        where = " AND ".join(where_parts) if where_parts else "1=1"
        df = con.execute(f"SELECT * FROM data WHERE {where} LIMIT 100", params).fetchdf()
        return df.to_dict(orient="records")
    except Exception as e:
        logger.warning("Breach query failed: %s", e)
        return []
    finally:
        con.close()


def cache_api_response(prefix: str, params: dict, data: dict) -> None:
    """Cache a JSON API response keyed by parameter hash."""
    h = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:12]
    path = _CACHE_DIR / f"{prefix}_{h}.json"
    path.write_text(json.dumps(data), encoding="utf-8")


def load_cached_api_response(prefix: str, params: dict) -> dict | None:
    """Load a cached API response if still valid."""
    h = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()[:12]
    path = _CACHE_DIR / f"{prefix}_{h}.json"
    if _is_cache_valid(path, _API_CACHE_TTL_DAYS):
        return json.loads(path.read_text(encoding="utf-8"))
    return None
```

**Step 1: Create data_loaders.py with the above content.**

**Step 2: Verify imports**

```bash
python3 -c "from servers.public_records import data_loaders; print('data_loaders OK')"
```

**Step 3: Commit**

```bash
git add servers/public_records/data_loaders.py
git commit -m "feat(public-records): add bulk data loaders and manual-seed file handlers"
```

---

## Task 5: USAspending API Client

**Files:**
- Create: `servers/public_records/usaspending_client.py`

REST client for USAspending.gov. No auth required. POST-based search API.

```python
"""USAspending.gov REST API client.

Searches federal awards by recipient name with optional filters.
API docs: https://api.usaspending.gov/
"""

import logging
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.usaspending.gov/api/v2"
_TIMEOUT = 30.0


async def search_awards(
    recipient_name: str,
    award_type: str = "",
    fiscal_year: str = "",
    limit: int = 25,
) -> dict:
    """Search federal awards by recipient name.

    Returns raw API response dict.
    """
    fy = fiscal_year or str(datetime.now().year)

    # Map friendly award_type to USAspending codes
    type_map = {
        "contracts": ["A", "B", "C", "D"],
        "grants": ["02", "03", "04", "05"],
        "direct_payments": ["06", "10"],
        "loans": ["07", "08"],
    }
    award_types = type_map.get(award_type.lower(), [])

    filters: dict = {
        "recipient_search_text": [recipient_name],
        "time_period": [{"start_date": f"{fy}-10-01", "end_date": f"{int(fy)+1}-09-30"}],
    }
    if award_types:
        filters["award_type_codes"] = award_types

    payload = {
        "filters": filters,
        "fields": [
            "Award ID",
            "Recipient Name",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Award Type",
            "Award Amount",
            "Total Outlays",
            "Description",
            "Start Date",
            "End Date",
            "NAICS Code",
            "NAICS Description",
        ],
        "limit": min(limit, 100),
        "page": 1,
        "sort": "Award Amount",
        "order": "desc",
    }

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(f"{_BASE_URL}/search/spending_by_award/", json=payload)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("USAspending search failed: %s", e)
        return {"error": str(e)}
```

**Step 1: Create usaspending_client.py with the above content.**

**Step 2: Verify imports**

```bash
python3 -c "from servers.public_records.usaspending_client import search_awards; print('OK')"
```

**Step 3: Commit**

```bash
git add servers/public_records/usaspending_client.py
git commit -m "feat(public-records): add USAspending.gov API client"
```

---

## Task 6: SAM.gov API Client

**Files:**
- Create: `servers/public_records/sam_client.py`

REST client for SAM.gov Opportunities API. Requires API key.

```python
"""SAM.gov Opportunities API client.

Searches federal contract opportunities/solicitations.
Requires SAM_GOV_API_KEY environment variable.
API docs: https://open.gsa.gov/api/get-opportunities-public-api/
"""

import logging
import os
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.sam.gov/prod/opportunities/v2/search"
_TIMEOUT = 30.0


def _get_api_key() -> str | None:
    return os.environ.get("SAM_GOV_API_KEY")


async def search_opportunities(
    keyword: str,
    posted_from: str = "",
    posted_to: str = "",
    ptype: str = "",
    limit: int = 25,
) -> dict:
    """Search SAM.gov opportunities by keyword.

    Returns raw API response dict or error dict.
    """
    api_key = _get_api_key()
    if not api_key:
        return {
            "error": "SAM_GOV_API_KEY not set",
            "instructions": "Register for a free API key at https://sam.gov/profile/details (Public API Key section)",
        }

    if not posted_from:
        posted_from = (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y")
    if not posted_to:
        posted_to = datetime.now().strftime("%m/%d/%Y")

    params: dict = {
        "api_key": api_key,
        "keyword": keyword,
        "postedFrom": posted_from,
        "postedTo": posted_to,
        "limit": min(limit, 100),
    }
    if ptype:
        params["ptype"] = ptype

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(_BASE_URL, params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("SAM.gov search failed: %s", e)
        return {"error": str(e)}
```

**Step 1: Create sam_client.py with the above content.**

**Step 2: Verify imports**

```bash
python3 -c "from servers.public_records.sam_client import search_opportunities; print('OK')"
```

**Step 3: Commit**

```bash
git add servers/public_records/sam_client.py
git commit -m "feat(public-records): add SAM.gov Opportunities API client"
```

---

## Task 7: Server — Wire Up All 6 Tools

**Files:**
- Create: `servers/public_records/server.py`

FastMCP server on port 8013 with 6 tools. Each tool:
1. Validates inputs
2. Ensures data is cached (or checks manual-seed file)
3. Queries data
4. Returns JSON response via Pydantic model

```python
"""Public Records & Regulatory MCP Server.

Provides tools for federal spending, 340B status, HIPAA breaches,
accreditation, and interoperability data. Port 8013.
"""

import json
import logging
import os as _os
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from . import data_loaders, usaspending_client, sam_client
from .models import (
    AccreditationRecord,
    AccreditationResponse,
    BreachHistoryResponse,
    BreachRecord,
    CoveredEntity340B,
    InteropRecord,
    InteropResponse,
    SAMOpportunity,
    SAMResponse,
    Status340BResponse,
    USAspendingAward,
    USAspendingResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "public-records"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8013"))
mcp = FastMCP(**_mcp_kwargs)

# Load accreditation code lookup once
_ACCR_CODES: dict[str, str] = {}
_codes_csv = Path(__file__).parent / "data" / "accreditation_codes.csv"
if _codes_csv.exists():
    import csv
    with open(_codes_csv, newline="") as f:
        for row in csv.DictReader(f):
            _ACCR_CODES[row["code"].strip()] = row["organization"].strip()


# ---------------------------------------------------------------------------
# Tool 1: search_usaspending
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_usaspending(
    recipient_name: str, award_type: str = "", fiscal_year: str = "", limit: int = 25,
) -> str:
    """Search federal spending awarded to a health system or hospital.

    Uses USAspending.gov (fully open, no auth needed).

    Args:
        recipient_name: Health system or hospital name to search.
        award_type: Filter: "contracts", "grants", "direct_payments", or "" for all.
        fiscal_year: e.g. "2024". Default: current fiscal year.
        limit: Max results (default 25, max 100).
    """
    try:
        # Check API response cache
        cache_params = {"recipient": recipient_name, "type": award_type, "fy": fiscal_year}
        cached = data_loaders.load_cached_api_response("usaspending", cache_params)
        if cached:
            return json.dumps(cached)

        raw = await usaspending_client.search_awards(
            recipient_name=recipient_name,
            award_type=award_type,
            fiscal_year=fiscal_year,
            limit=limit,
        )

        if "error" in raw:
            return json.dumps(raw)

        results = raw.get("results", [])
        awards = []
        total_obligation = 0.0
        for r in results:
            amt = float(r.get("Award Amount", 0) or 0)
            total_obligation += amt
            awards.append(USAspendingAward(
                award_id=str(r.get("Award ID", "")),
                recipient_name=str(r.get("Recipient Name", "")),
                awarding_agency=str(r.get("Awarding Agency", "")),
                awarding_sub_agency=str(r.get("Awarding Sub Agency", "")),
                award_type=str(r.get("Award Type", "")),
                total_obligation=amt,
                description=str(r.get("Description", ""))[:500],
                start_date=str(r.get("Start Date", "")),
                end_date=str(r.get("End Date", "")),
                naics_code=str(r.get("NAICS Code", "")),
                naics_description=str(r.get("NAICS Description", "")),
            ))

        resp = USAspendingResponse(
            recipient_search=recipient_name,
            fiscal_year=fiscal_year or str(__import__("datetime").datetime.now().year),
            total_awards=raw.get("page_metadata", {}).get("total", len(awards)),
            total_obligation=total_obligation,
            awards=awards,
        )
        result = resp.model_dump()
        data_loaders.cache_api_response("usaspending", cache_params, result)
        return json.dumps(result)

    except Exception as e:
        logger.exception("search_usaspending failed")
        return json.dumps({"error": f"search_usaspending failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: search_sam_gov
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_sam_gov(
    keyword: str, posted_from: str = "", posted_to: str = "", ptype: str = "", limit: int = 25,
) -> str:
    """Search federal contract opportunities and solicitations.

    Uses SAM.gov Opportunities API. Requires SAM_GOV_API_KEY env var.

    Args:
        keyword: Search keyword (organization name, service type, NAICS code).
        posted_from: Start date (MM/DD/YYYY). Default: 1 year ago.
        posted_to: End date. Default: today.
        ptype: Procurement type: "o" (solicitation), "p" (presolicitation), "k" (combined), or "" for all.
        limit: Max results (default 25).
    """
    try:
        cache_params = {"kw": keyword, "from": posted_from, "to": posted_to, "ptype": ptype}
        cached = data_loaders.load_cached_api_response("sam", cache_params)
        if cached:
            return json.dumps(cached)

        raw = await sam_client.search_opportunities(
            keyword=keyword,
            posted_from=posted_from,
            posted_to=posted_to,
            ptype=ptype,
            limit=limit,
        )

        if "error" in raw:
            return json.dumps(raw)

        opp_data = raw.get("opportunitiesData", [])
        opportunities = []
        for o in opp_data:
            opportunities.append(SAMOpportunity(
                notice_id=str(o.get("noticeId", "")),
                title=str(o.get("title", "")),
                solicitation_number=str(o.get("solicitationNumber", "")),
                department=str(o.get("department", "")),
                sub_tier=str(o.get("subTier", "")),
                posted_date=str(o.get("postedDate", "")),
                response_deadline=str(o.get("responseDeadLine", "")),
                naics_code=str(o.get("naicsCode", "")),
                set_aside_type=str(o.get("typeOfSetAsideDescription", "")),
                description=str(o.get("description", ""))[:500],
                active=o.get("active", "Yes") == "Yes",
            ))

        resp = SAMResponse(
            keyword=keyword,
            total_results=raw.get("totalRecords", len(opportunities)),
            opportunities=opportunities,
        )
        result = resp.model_dump()
        data_loaders.cache_api_response("sam", cache_params, result)
        return json.dumps(result)

    except Exception as e:
        logger.exception("search_sam_gov failed")
        return json.dumps({"error": f"search_sam_gov failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: get_340b_status
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_340b_status(
    entity_name: str = "", entity_id: str = "", state: str = "",
) -> str:
    """Look up 340B Drug Pricing Program enrollment and contract pharmacy data.

    Requires manual download of the HRSA 340B OPAIS daily JSON export.

    Args:
        entity_name: Search by covered entity name.
        entity_id: Search by 340B ID.
        state: Filter by state abbreviation (e.g. "PA").
    """
    try:
        if not entity_name and not entity_id:
            return json.dumps({"error": "Provide entity_name or entity_id"})

        if not data_loaders.ensure_340b_loaded():
            return json.dumps({
                "error": "340B data not found in cache",
                "instructions": (
                    "Download the JSON export from https://340bopais.hrsa.gov/Reports "
                    "(click 'Covered Entity Daily Export (JSON)'). "
                    f"Place the file at: {data_loaders.OPAIS_340B_JSON}"
                ),
            })

        rows = data_loaders.query_340b(entity_name=entity_name, entity_id=entity_id, state=state)
        if not rows:
            return json.dumps({"error": f"No 340B entities found matching search criteria"})

        entities = [CoveredEntity340B(**{k: v for k, v in r.items() if k in CoveredEntity340B.model_fields}) for r in rows]
        resp = Status340BResponse(
            search_term=entity_name or entity_id,
            total_results=len(entities),
            entities=entities,
        )
        return json.dumps(resp.model_dump())

    except Exception as e:
        logger.exception("get_340b_status failed")
        return json.dumps({"error": f"get_340b_status failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: get_breach_history
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_breach_history(
    entity_name: str, state: str = "", min_individuals: int = 0,
) -> str:
    """Look up HIPAA breach reports for an organization.

    Requires manual download of breach data CSV from HHS OCR portal.

    Args:
        entity_name: Organization name to search.
        state: Filter by state abbreviation.
        min_individuals: Minimum individuals affected (default 0).
    """
    try:
        if not data_loaders.ensure_breach_loaded():
            return json.dumps({
                "error": "HIPAA breach data not found in cache",
                "instructions": (
                    "Export breach data CSV from https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf "
                    "(search for breaches, then export results to CSV). "
                    f"Place the file at: {data_loaders.BREACH_CSV}"
                ),
            })

        rows = data_loaders.query_breaches(
            entity_name=entity_name, state=state, min_individuals=min_individuals,
        )
        if not rows:
            return json.dumps({"error": f"No breach records found for: {entity_name}"})

        breaches = []
        total_affected = 0
        for r in rows:
            affected = 0
            for k, v in r.items():
                if "individuals" in k.lower() or "affected" in k.lower():
                    try:
                        affected = int(float(str(v).replace(",", "")))
                    except (ValueError, TypeError):
                        pass
            total_affected += affected
            breaches.append(BreachRecord(
                entity_name=str(r.get("name_of_covered_entity", r.get("entity_name", ""))),
                state=str(r.get("state", "")),
                covered_entity_type=str(r.get("covered_entity_type", "")),
                individuals_affected=affected,
                breach_submission_date=str(r.get("breach_submission_date", "")),
                breach_type=str(r.get("type_of_breach", r.get("breach_type", ""))),
                location_of_breached_info=str(r.get("location_of_breached_information", "")),
                business_associate_present=str(r.get("business_associate_present", "")),
                web_description=str(r.get("web_description", ""))[:500],
            ))

        resp = BreachHistoryResponse(
            search_entity=entity_name,
            total_breaches=len(breaches),
            total_individuals_affected=total_affected,
            breaches=breaches,
        )
        return json.dumps(resp.model_dump())

    except Exception as e:
        logger.exception("get_breach_history failed")
        return json.dumps({"error": f"get_breach_history failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: get_accreditation
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_accreditation(
    ccn: str = "", provider_name: str = "", state: str = "",
) -> str:
    """Look up hospital accreditation and certification status.

    Uses CMS Provider of Services (POS) file. Includes Joint Commission,
    DNV, CIHQ, and other CMS-approved accrediting organizations.

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        provider_name: Search by provider name (partial match).
        state: Filter by state abbreviation.
    """
    try:
        if not ccn and not provider_name:
            return json.dumps({"error": "Provide ccn or provider_name"})

        await data_loaders.ensure_pos_cached()
        rows = data_loaders.query_pos(ccn=ccn, provider_name=provider_name, state=state)
        if not rows:
            return json.dumps({"error": f"No providers found matching search criteria"})

        providers = []
        for r in rows:
            code = r.get("accreditation_type_code", "")
            r["accreditation_org"] = _ACCR_CODES.get(code, f"Code {code}" if code else "Unknown")
            providers.append(AccreditationRecord(**{k: v for k, v in r.items() if k in AccreditationRecord.model_fields}))

        resp = AccreditationResponse(
            search_term=ccn or provider_name,
            total_results=len(providers),
            providers=providers,
        )
        return json.dumps(resp.model_dump())

    except Exception as e:
        logger.exception("get_accreditation failed")
        return json.dumps({"error": f"get_accreditation failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 6: get_interop_status
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_interop_status(
    ccn: str = "", facility_name: str = "", state: str = "",
) -> str:
    """Check Promoting Interoperability attestation and EHR certification.

    Uses CMS Promoting Interoperability dataset. Optionally enriches with
    ONC CHPL API for EHR product details (requires CHPL_API_KEY env var).

    Args:
        ccn: CMS Certification Number.
        facility_name: Search by facility name (partial match).
        state: Filter by state abbreviation.
    """
    try:
        if not ccn and not facility_name:
            return json.dumps({"error": "Provide ccn or facility_name"})

        await data_loaders.ensure_pi_cached()
        rows = data_loaders.query_pi(ccn=ccn, facility_name=facility_name, state=state)
        if not rows:
            return json.dumps({"error": f"No facilities found matching search criteria"})

        # Optional CHPL enrichment
        chpl_key = _os.environ.get("CHPL_API_KEY")
        records = []
        for r in rows:
            rec = InteropRecord(**{k: v for k, v in r.items() if k in InteropRecord.model_fields})
            # If CHPL key is available and record has a CEHRT ID, look up EHR product
            if chpl_key and rec.cehrt_id:
                ehr_info = await _lookup_chpl(rec.cehrt_id, chpl_key)
                if ehr_info:
                    rec.ehr_product_name = ehr_info.get("product_name", "")
                    rec.ehr_developer = ehr_info.get("developer", "")
            records.append(rec)

        resp = InteropResponse(
            search_term=ccn or facility_name,
            total_results=len(records),
            records=records,
        )
        return json.dumps(resp.model_dump())

    except Exception as e:
        logger.exception("get_interop_status failed")
        return json.dumps({"error": f"get_interop_status failed: {e}"})


async def _lookup_chpl(cehrt_id: str, api_key: str) -> dict | None:
    """Look up EHR product from ONC CHPL by CEHRT ID."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"https://chpl.healthit.gov/rest/certification_ids/{cehrt_id}",
                headers={"API-key": api_key},
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            products = data.get("products", [])
            if products:
                return {
                    "product_name": products[0].get("name", ""),
                    "developer": products[0].get("developer", ""),
                }
    except Exception:
        pass
    return None


if __name__ == "__main__":
    mcp.run(transport=_transport)
```

**Step 1: Create server.py with the above content.**

**Step 2: Verify syntax and imports**

```bash
python3 -c "import ast; ast.parse(open('servers/public_records/server.py').read()); print('syntax OK')"
```

**Step 3: Verify tool registration**

```bash
python3 -c "
from servers.public_records.server import mcp
tools = mcp._tool_manager._tools
print(f'{len(tools)} tools registered:')
for name in sorted(tools): print(f'  - {name}')
"
```

Expected: 6 tools registered.

**Step 4: Commit**

```bash
git add servers/public_records/server.py
git commit -m "feat(public-records): wire up all 6 tools in server.py"
```

---

## Task 8: Docker & MCP Registration

**Files:**
- Modify: `docker-compose.yml` — add public-records service block
- Modify: `.mcp.json` — add public-records entry

**Step 1: Add to docker-compose.yml** (before `volumes:` section)

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

**Step 2: Add to .mcp.json**

Add inside `mcpServers`:
```json
"public-records": {
  "type": "http",
  "url": "http://localhost:8013/mcp"
}
```

**Step 3: Commit**

```bash
git add docker-compose.yml .mcp.json
git commit -m "feat(public-records): add Docker and MCP registration (port 8013)"
```

---

## Task 9: Validation & Smoke Test

**Step 1: Verify all modules import cleanly**

```bash
python3 -c "
from servers.public_records.models import (
    USAspendingResponse, SAMResponse, Status340BResponse,
    BreachHistoryResponse, AccreditationResponse, InteropResponse,
)
from servers.public_records import data_loaders, usaspending_client, sam_client
from servers.public_records.server import mcp
tools = mcp._tool_manager._tools
print(f'Models: 6 response models OK')
print(f'Tools: {len(tools)} registered')
for name in sorted(tools): print(f'  - {name}')
print(f'Cache dir: {data_loaders._CACHE_DIR}')
print(f'Manual-seed paths:')
print(f'  340B JSON: {data_loaders.OPAIS_340B_JSON}')
print(f'  Breach CSV: {data_loaders.BREACH_CSV}')
print('All imports verified.')
"
```

**Step 2: Verify Pydantic models serialize**

```bash
python3 -c "
import json
from servers.public_records.models import AccreditationResponse, AccreditationRecord
resp = AccreditationResponse(
    search_term='Test Hospital',
    total_results=1,
    providers=[AccreditationRecord(ccn='390223', provider_name='Test Hospital', accreditation_org='TJC')],
)
print(json.dumps(resp.model_dump(), indent=2))
"
```

**Step 3: Commit any fixes found during validation**
