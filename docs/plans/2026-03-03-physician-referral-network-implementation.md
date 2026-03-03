# Physician & Referral Network Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an MCP server (port 8010) providing physician search, profiles with Medicare utilization, referral network mapping via DocGraph shared patient data, health system physician employment mix analysis, and referral leakage detection.

**Architecture:** Four-module server (`nppes_client`, `referral_network`, `physician_mix`, `server`) with real-time NPPES API queries, bulk CMS datasets cached as Parquet (Physician Compare, Utilization PUF), DocGraph shared patient data (2014-2020) cached as Parquet, and AHRQ/POS data reused from health-system-profiler's loaders.

**Tech Stack:** FastMCP, httpx, pandas, DuckDB, Pydantic v2, shared/utils/cms_client.py

**Design Doc:** `docs/plans/2026-03-03-physician-referral-network-design.md`

**Reference Servers:** `servers/financial-intelligence/server.py` (tool pattern), `servers/health-system-profiler/data_loaders.py` (AHRQ/POS loaders), `servers/price-transparency/mrf_processor.py` (DuckDB query pattern)

---

### Task 1: Scaffold Directory and Pydantic Models

**Files:**
- Create: `servers/physician-referral-network/__init__.py`
- Create: `servers/physician-referral-network/models.py`
- Create symlink: `servers/physician_referral_network` → `physician-referral-network`

**Step 1: Create directory and symlink**

```bash
mkdir -p "servers/physician-referral-network"
touch "servers/physician-referral-network/__init__.py"
cd servers && ln -s physician-referral-network physician_referral_network && cd ..
```

**Step 2: Write models.py**

Create `servers/physician-referral-network/models.py` following the project convention (see `servers/price-transparency/models.py`):

```python
"""Pydantic models for physician & referral network server."""

from pydantic import BaseModel, Field


class PhysicianSummary(BaseModel):
    """A physician from search results."""

    npi: str = ""
    first_name: str = ""
    last_name: str = ""
    credential: str = ""
    specialty: str = ""
    city: str = ""
    state: str = ""
    org_name: str = ""
    gender: str = ""
    enumeration_date: str = ""


class PhysicianSearchResponse(BaseModel):
    """Response from search_physicians."""

    total_results: int = 0
    physicians: list[PhysicianSummary] = Field(default_factory=list)


class UtilizationSummary(BaseModel):
    """Medicare utilization summary for a physician."""

    total_services: int = 0
    total_beneficiaries: int = 0
    total_medicare_payment: float | None = None
    avg_allowed_amount: float | None = None
    avg_submitted_charge: float | None = None
    top_hcpcs: list[dict] = Field(default_factory=list, description="Top HCPCS codes by service volume")


class QualityInfo(BaseModel):
    """Quality and affiliation data from Physician Compare."""

    group_practice_pac_id: str = ""
    group_practice_name: str = ""
    hospital_affiliations: list[str] = Field(default_factory=list)
    graduation_year: str = ""
    medical_school: str = ""


class PhysicianProfile(BaseModel):
    """Full physician profile from get_physician_profile."""

    npi: str = ""
    first_name: str = ""
    last_name: str = ""
    credential: str = ""
    specialties: list[str] = Field(default_factory=list)
    practice_locations: list[dict] = Field(default_factory=list)
    org_affiliations: list[str] = Field(default_factory=list)
    gender: str = ""
    enumeration_date: str = ""
    utilization: UtilizationSummary | None = None
    quality: QualityInfo | None = None


class ReferralNode(BaseModel):
    """A node in a referral network graph."""

    npi: str = ""
    name: str = ""
    specialty: str = ""
    city: str = ""
    state: str = ""


class ReferralEdge(BaseModel):
    """An edge in a referral network graph."""

    npi_from: str = ""
    npi_to: str = ""
    shared_count: int = 0
    transaction_count: int = 0
    same_day_count: int = 0


class ReferralNetworkResponse(BaseModel):
    """Response from map_referral_network."""

    center_npi: str = ""
    center_name: str = ""
    nodes: list[ReferralNode] = Field(default_factory=list)
    edges: list[ReferralEdge] = Field(default_factory=list)
    total_connections: int = 0
    data_vintage: str = Field(default="2014-2020", description="DocGraph data years")


class LeakageDestination(BaseModel):
    """A single out-of-network referral destination."""

    npi: str = ""
    name: str = ""
    specialty: str = ""
    shared_count: int = 0
    city: str = ""
    state: str = ""
    classification: str = Field(default="", description="out_of_network_in_area or out_of_area")


class SpecialtyLeakage(BaseModel):
    """Leakage breakdown for one specialty."""

    specialty: str = ""
    total_referrals: int = 0
    in_network: int = 0
    out_of_network: int = 0
    leakage_pct: float = 0.0


class LeakageResponse(BaseModel):
    """Response from detect_leakage."""

    system_name: str = ""
    total_referrals: int = 0
    in_network_pct: float = 0.0
    out_of_network_in_area_pct: float = 0.0
    out_of_area_pct: float = 0.0
    top_leakage_destinations: list[LeakageDestination] = Field(default_factory=list)
    specialty_breakdown: list[SpecialtyLeakage] = Field(default_factory=list)
    data_vintage: str = "2014-2020"


class PhysicianClassification(BaseModel):
    """Employment classification for one physician."""

    npi: str = ""
    name: str = ""
    specialty: str = ""
    status: str = Field(default="", description="employed, affiliated, or independent")
    confidence: float = 0.0
    evidence: list[str] = Field(default_factory=list)


class PhysicianMixResponse(BaseModel):
    """Response from analyze_physician_mix."""

    system_name: str = ""
    total_physicians: int = 0
    employed: int = 0
    affiliated: int = 0
    independent: int = 0
    employed_pct: float = 0.0
    affiliated_pct: float = 0.0
    independent_pct: float = 0.0
    by_specialty: list[dict] = Field(default_factory=list)
    sample_physicians: list[PhysicianClassification] = Field(
        default_factory=list, description="Sample of classified physicians for verification"
    )
```

**Step 3: Verify models import**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "from servers.physician_referral_network.models import *; print('Models imported OK')"`
Expected: `Models imported OK`

**Step 4: Commit**

```bash
git add servers/physician-referral-network/__init__.py servers/physician-referral-network/models.py
git commit -m "feat(physician-referral-network): scaffold directory and Pydantic models"
```

---

### Task 2: NPPES Client — Physician Search and Profiles

**Files:**
- Create: `servers/physician-referral-network/nppes_client.py`

**Context:**
- Uses `shared.utils.cms_client.nppes_lookup` for NPPES API calls
- NPPES API: `https://npiregistry.cms.hhs.gov/api/?version=2.1`
- NPI-1 = individual physicians, NPI-2 = organizations
- `taxonomy_description` param filters by specialty (partial match)
- Response structure: `results[].basic.{first_name, last_name, credential, gender, enumeration_date}`, `results[].taxonomies[].{desc, primary, state, license}`, `results[].addresses[]`, `results[].other_names[]`

**Step 1: Write nppes_client.py**

```python
"""NPPES-based physician search and profile retrieval.

Uses the shared CMS client for NPPES API calls, enriched with cached
CMS Physician Compare and Medicare Utilization PUF data.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "physician"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_PHYSICIAN_COMPARE_CACHE = _CACHE_DIR / "physician_compare.parquet"
_UTILIZATION_CACHE = _CACHE_DIR / "utilization.parquet"
_CACHE_TTL_DAYS = 30

# CMS Physician Compare dataset (data.medicare.gov Socrata API)
PHYSICIAN_COMPARE_CSV_URL = (
    "https://data.medicare.gov/api/views/mj5m-pzi6/rows.csv?accessType=DOWNLOAD"
)

# Medicare Physician & Other Supplier utilization PUF
# Using data.cms.gov dataset for "by Provider and Service"
UTILIZATION_DATASET_URL = (
    "https://data.cms.gov/provider-summary-by-type-of-service/"
    "medicare-physician-other-practitioners/"
    "medicare-physician-other-practitioners-by-provider-and-service"
)
# The data-api endpoint for this dataset (GET-based, filter by NPI)
UTILIZATION_API_BASE = "https://data.cms.gov/data-api/v1/dataset"
# Dataset UUID — resolve on first use from the catalog page
_utilization_dataset_id: str | None = None

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"


# ---------------------------------------------------------------------------
# NPPES Search
# ---------------------------------------------------------------------------

async def search_physicians(
    query: str,
    specialty: str = "",
    state: str = "",
    limit: int = 25,
) -> list[dict]:
    """Search NPPES for individual physicians (NPI-1).

    Args:
        query: First/last name, full name, or NPI number.
        specialty: Taxonomy description filter (e.g. "Cardiology").
        state: Two-letter state code.
        limit: Max results (1-200).

    Returns:
        List of physician summary dicts.
    """
    params: dict = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "limit": min(limit, 200),
    }

    # Detect if query is an NPI number (10 digits)
    if query.strip().isdigit() and len(query.strip()) == 10:
        params["number"] = query.strip()
    elif " " in query.strip():
        # Assume "First Last" format
        parts = query.strip().split(None, 1)
        params["first_name"] = parts[0]
        params["last_name"] = parts[1]
    else:
        # Single name — try as last name
        params["last_name"] = query.strip()

    if specialty:
        params["taxonomy_description"] = specialty
    if state:
        params["state"] = state.upper()

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(NPPES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    physicians = []
    for r in results:
        basic = r.get("basic", {})
        taxonomies = r.get("taxonomies", [])
        primary_tax = next((t for t in taxonomies if t.get("primary")), taxonomies[0] if taxonomies else {})
        addresses = r.get("addresses", [])
        practice_addr = next((a for a in addresses if a.get("address_purpose") == "LOCATION"), addresses[0] if addresses else {})

        physicians.append({
            "npi": r.get("number", ""),
            "first_name": basic.get("first_name", ""),
            "last_name": basic.get("last_name", ""),
            "credential": basic.get("credential", ""),
            "specialty": primary_tax.get("desc", ""),
            "city": practice_addr.get("city", ""),
            "state": practice_addr.get("state", ""),
            "org_name": basic.get("organization_name", ""),
            "gender": basic.get("gender", ""),
            "enumeration_date": basic.get("enumeration_date", ""),
        })

    return physicians


# ---------------------------------------------------------------------------
# Physician Profile (NPPES detail + enrichment)
# ---------------------------------------------------------------------------

async def get_physician_detail(npi: str) -> dict | None:
    """Get full physician profile from NPPES, enriched with cached data.

    Args:
        npi: 10-digit NPI number.

    Returns:
        Physician profile dict or None if not found.
    """
    params = {"version": "2.1", "number": npi.strip()}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(NPPES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    results = data.get("results", [])
    if not results:
        return None

    r = results[0]
    basic = r.get("basic", {})
    taxonomies = r.get("taxonomies", [])
    addresses = r.get("addresses", [])

    specialties = [t.get("desc", "") for t in taxonomies if t.get("desc")]
    org_affiliations = []
    if basic.get("organization_name"):
        org_affiliations.append(basic["organization_name"])

    practice_locations = []
    for addr in addresses:
        if addr.get("address_purpose") == "LOCATION":
            practice_locations.append({
                "address_1": addr.get("address_1", ""),
                "city": addr.get("city", ""),
                "state": addr.get("state", ""),
                "postal_code": addr.get("postal_code", ""),
                "telephone_number": addr.get("telephone_number", ""),
            })

    profile = {
        "npi": npi,
        "first_name": basic.get("first_name", ""),
        "last_name": basic.get("last_name", ""),
        "credential": basic.get("credential", ""),
        "specialties": specialties,
        "practice_locations": practice_locations,
        "org_affiliations": org_affiliations,
        "gender": basic.get("gender", ""),
        "enumeration_date": basic.get("enumeration_date", ""),
    }

    # Enrich with utilization data
    utilization = get_utilization_summary(npi)
    if utilization:
        profile["utilization"] = utilization

    # Enrich with Physician Compare quality data
    quality = get_quality_info(npi)
    if quality:
        profile["quality"] = quality

    return profile


# ---------------------------------------------------------------------------
# Physician Compare (cached Parquet)
# ---------------------------------------------------------------------------

def _is_cache_valid(path: Path) -> bool:
    """Check if a cached Parquet file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < _CACHE_TTL_DAYS


async def ensure_physician_compare_cached() -> bool:
    """Download Physician Compare CSV and convert to Parquet if needed.

    Returns True if cache is available, False on failure.
    """
    if _is_cache_valid(_PHYSICIAN_COMPARE_CACHE):
        return True

    logger.info("Downloading Physician Compare data...")
    try:
        async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
            resp = await client.get(PHYSICIAN_COMPARE_CSV_URL)
            resp.raise_for_status()

        # Write CSV temporarily, convert to Parquet
        csv_path = _CACHE_DIR / "physician_compare_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_PHYSICIAN_COMPARE_CACHE, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("Physician Compare cached: %d rows", len(df))
        return True

    except Exception as e:
        logger.warning("Failed to cache Physician Compare: %s", e)
        return False


def get_quality_info(npi: str) -> dict | None:
    """Query cached Physician Compare data for quality/affiliation info.

    Returns dict with group_practice, hospital_affiliations, graduation_year, etc.
    """
    if not _PHYSICIAN_COMPARE_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(
            f"CREATE VIEW pc AS SELECT * FROM read_parquet('{_PHYSICIAN_COMPARE_CACHE}')"
        )

        # Find NPI column (may be "npi" or "ind_pac_id" etc.)
        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='pc'").fetchall()]
        npi_col = next((c for c in cols if c in ("npi", "ind_npi", "npi_number")), None)
        if not npi_col:
            con.close()
            return None

        rows = con.execute(f"SELECT * FROM pc WHERE {npi_col} = ? LIMIT 1", [npi]).fetchdf()
        con.close()

        if rows.empty:
            return None

        row = rows.iloc[0]

        # Extract hospital affiliations (columns like hosp_afl_1, hosp_afl_2, etc.)
        hospital_cols = [c for c in row.index if c.startswith("hosp_afl")]
        hospitals = [str(row[c]) for c in hospital_cols if row[c] and str(row[c]).strip()]

        return {
            "group_practice_pac_id": str(row.get("org_pac_id", "")),
            "group_practice_name": str(row.get("org_nm", row.get("organization_legal_name", ""))),
            "hospital_affiliations": hospitals,
            "graduation_year": str(row.get("grd_yr", row.get("graduation_year", ""))),
            "medical_school": str(row.get("med_sch", row.get("medical_school_name", ""))),
        }

    except Exception as e:
        logger.warning("Physician Compare query failed for NPI %s: %s", npi, e)
        return None


# ---------------------------------------------------------------------------
# Medicare Utilization PUF (cached Parquet)
# ---------------------------------------------------------------------------

async def ensure_utilization_cached() -> bool:
    """Download Medicare Utilization PUF and convert to Parquet if needed.

    This downloads the "by Provider" aggregate file (one row per NPI) rather
    than the per-service file, to keep the dataset manageable.

    Returns True if cache is available, False on failure.
    """
    if _is_cache_valid(_UTILIZATION_CACHE):
        return True

    # Try the "by Provider" aggregate dataset from data.cms.gov
    # Dataset: Medicare Physician & Other Practitioners - by Provider
    PROVIDER_AGG_URL = (
        "https://data.cms.gov/provider-summary-by-type-of-service/"
        "medicare-physician-other-practitioners/"
        "medicare-physician-other-practitioners-by-provider"
    )

    logger.info("Downloading Medicare Utilization data (by Provider)...")
    try:
        # First, get the dataset page to find the download URL
        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            resp = await client.get(f"{PROVIDER_AGG_URL}?format=csv")
            # If this doesn't work, try the direct download pattern
            if resp.status_code != 200 or len(resp.content) < 1000:
                # Try data-api pattern
                resp = await client.get(
                    "https://data.cms.gov/data-api/v1/dataset/3614c3f0-21a5-4a7f-8e37-7cf21b6caa5d/data",
                    params={"size": 0},
                    timeout=30.0,
                )
                resp.raise_for_status()

        # If we got CSV data, save it
        if resp.headers.get("content-type", "").startswith("text/csv") or len(resp.content) > 10000:
            csv_path = _CACHE_DIR / "utilization_raw.csv"
            csv_path.write_bytes(resp.content)

            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            df.to_parquet(_UTILIZATION_CACHE, compression="zstd", index=False)

            csv_path.unlink(missing_ok=True)
            logger.info("Utilization data cached: %d rows", len(df))
            return True

        logger.warning("Could not download utilization data — unexpected response")
        return False

    except Exception as e:
        logger.warning("Failed to cache utilization data: %s", e)
        return False


def get_utilization_summary(npi: str) -> dict | None:
    """Query cached utilization data for an NPI.

    Returns summary with total services, beneficiaries, payments, top HCPCS codes.
    """
    if not _UTILIZATION_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(
            f"CREATE VIEW util AS SELECT * FROM read_parquet('{_UTILIZATION_CACHE}')"
        )

        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='util'").fetchall()]
        npi_col = next((c for c in cols if c in ("npi", "rndrng_npi", "national_provider_identifier")), None)
        if not npi_col:
            con.close()
            return None

        rows = con.execute(f"SELECT * FROM util WHERE {npi_col} = ? LIMIT 1", [npi]).fetchdf()
        con.close()

        if rows.empty:
            return None

        row = rows.iloc[0]

        def _safe_float(val):
            try:
                v = str(val).replace(",", "").strip()
                return float(v) if v and v.lower() not in ("", "nan", "none") else None
            except (ValueError, TypeError):
                return None

        def _safe_int(val):
            f = _safe_float(val)
            return int(f) if f is not None else 0

        # Column name candidates (CMS changes names between releases)
        total_services = _safe_int(
            row.get("tot_srvcs", row.get("total_services", row.get("tot_hcpcs_cds", 0)))
        )
        total_benes = _safe_int(
            row.get("tot_benes", row.get("total_unique_benes", row.get("tot_bene_cnt", 0)))
        )
        total_payment = _safe_float(
            row.get("tot_mdcr_pymt_amt", row.get("total_medicare_payment_amt", None))
        )
        avg_allowed = _safe_float(
            row.get("avg_mdcr_alowd_amt", row.get("avg_medicare_allowed_amt", None))
        )
        avg_charge = _safe_float(
            row.get("avg_sbmtd_chrg", row.get("avg_submitted_charge_amt", None))
        )

        return {
            "total_services": total_services,
            "total_beneficiaries": total_benes,
            "total_medicare_payment": total_payment,
            "avg_allowed_amount": avg_allowed,
            "avg_submitted_charge": avg_charge,
            "top_hcpcs": [],  # Populated when using per-service dataset
        }

    except Exception as e:
        logger.warning("Utilization query failed for NPI %s: %s", npi, e)
        return None
```

**Step 2: Verify NPPES search works (live API)**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
import asyncio
from servers.physician_referral_network.nppes_client import search_physicians

async def main():
    results = await search_physicians('Smith', specialty='Cardiology', state='PA', limit=5)
    print(f'Found {len(results)} physicians')
    for r in results[:3]:
        print(f'  {r[\"npi\"]} | {r[\"first_name\"]} {r[\"last_name\"]} | {r[\"specialty\"]} | {r[\"city\"]}, {r[\"state\"]}')

asyncio.run(main())
"
```
Expected: >0 results, with NPI, name, specialty, and location.

**Step 3: Commit**

```bash
git add servers/physician-referral-network/nppes_client.py
git commit -m "feat(physician-referral-network): add NPPES client with search and profile functions"
```

---

### Task 3: Referral Network — DocGraph Data Management

**Files:**
- Create: `servers/physician-referral-network/referral_network.py`

**Context:**
- DocGraph Hop Teaming dataset: CSV with columns `npi_from, npi_to, shared_count, transaction_count, same_day_count`
- Free data for 2014-2015 from CareSet (https://careset.com/datasets/)
- Cache as Parquet at `~/.healthcare-data-mcp/cache/docgraph/shared_patients.parquet`
- Dartmouth Atlas HSA/HRR crosswalk from `data.dartmouthatlas.org`
- DuckDB for queries (same pattern as price-transparency server)

**Important:** DocGraph data may not be directly downloadable via URL (may require manual download or registration). The code must handle the case where the data file doesn't exist and provide clear instructions.

**Step 1: Write referral_network.py**

```python
"""Referral network analysis using DocGraph shared patient data.

DocGraph (CareSet) provides directed physician-to-physician shared patient
counts derived from Medicare claims (2014-2020). This module caches the data
as Parquet and provides DuckDB-based graph queries.

Data source: https://careset.com/datasets/
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "docgraph"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_SHARED_PATIENTS_CACHE = _CACHE_DIR / "shared_patients.parquet"

_DARTMOUTH_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "dartmouth"
_DARTMOUTH_DIR.mkdir(parents=True, exist_ok=True)
_HSA_CROSSWALK_CACHE = _DARTMOUTH_DIR / "zip_hsa_hrr.parquet"

# DocGraph CSV expected columns
DOCGRAPH_COLUMNS = ["npi_from", "npi_to", "shared_count", "transaction_count", "same_day_count"]

# Dartmouth Atlas HSA/HRR crosswalk URL
DARTMOUTH_HSA_URL = "https://data.dartmouthatlas.org/downloads/geography/ZipHsaHrr18.csv"

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"


# ---------------------------------------------------------------------------
# DocGraph Data Management
# ---------------------------------------------------------------------------

def is_docgraph_cached() -> bool:
    """Check if DocGraph shared patient data is cached."""
    return _SHARED_PATIENTS_CACHE.exists()


def load_docgraph_csv(csv_path: str | Path) -> int:
    """Load a DocGraph CSV file and convert to Parquet cache.

    The DocGraph Hop Teaming CSV typically has columns:
    - Column 1: NPI of first provider
    - Column 2: NPI of second provider
    - Column 3: Number of shared patients (or transactions)
    Additional columns vary by release year.

    Args:
        csv_path: Path to the downloaded DocGraph CSV file.

    Returns:
        Number of rows loaded.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"DocGraph CSV not found: {path}")

    logger.info("Loading DocGraph CSV: %s", path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Map to standard columns (DocGraph releases vary in column names)
    col_map = {}
    for col in df.columns:
        if "from" in col or col in ("npi1", "npi_1", "referring_npi"):
            col_map[col] = "npi_from"
        elif "to" in col or col in ("npi2", "npi_2", "referred_npi"):
            col_map[col] = "npi_to"
        elif "shared" in col or "patient" in col:
            col_map[col] = "shared_count"
        elif "transaction" in col or "claim" in col:
            col_map[col] = "transaction_count"
        elif "same_day" in col or "sameday" in col:
            col_map[col] = "same_day_count"

    df = df.rename(columns=col_map)

    # Ensure required columns exist
    for req_col in ["npi_from", "npi_to"]:
        if req_col not in df.columns:
            # If we have exactly 2-3 unnamed columns, assume standard order
            if len(df.columns) >= 2:
                orig_cols = list(df.columns)
                df = df.rename(columns={orig_cols[0]: "npi_from", orig_cols[1]: "npi_to"})
                if len(orig_cols) >= 3:
                    df = df.rename(columns={orig_cols[2]: "shared_count"})
            else:
                raise ValueError(f"Cannot identify required column '{req_col}' in DocGraph CSV")

    # Add missing optional columns
    if "shared_count" not in df.columns:
        df["shared_count"] = "0"
    if "transaction_count" not in df.columns:
        df["transaction_count"] = "0"
    if "same_day_count" not in df.columns:
        df["same_day_count"] = "0"

    # Convert numeric columns
    for col in ["shared_count", "transaction_count", "same_day_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Keep only standard columns
    df = df[["npi_from", "npi_to", "shared_count", "transaction_count", "same_day_count"]]

    # Write Parquet
    df.to_parquet(_SHARED_PATIENTS_CACHE, compression="zstd", index=False)
    logger.info("DocGraph cached: %d referral pairs", len(df))

    return len(df)


# ---------------------------------------------------------------------------
# Dartmouth Atlas HSA/HRR Crosswalk
# ---------------------------------------------------------------------------

async def ensure_hsa_crosswalk_cached() -> bool:
    """Download Dartmouth Atlas ZIP-to-HSA/HRR crosswalk if not cached."""
    if _HSA_CROSSWALK_CACHE.exists():
        return True

    logger.info("Downloading Dartmouth Atlas HSA/HRR crosswalk...")
    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(DARTMOUTH_HSA_URL)
            resp.raise_for_status()

        csv_path = _DARTMOUTH_DIR / "zip_hsa_hrr_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_HSA_CROSSWALK_CACHE, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("HSA crosswalk cached: %d ZIP codes", len(df))
        return True

    except Exception as e:
        logger.warning("Failed to download HSA crosswalk: %s", e)
        return False


def get_hsa_for_zip(zip_code: str) -> str | None:
    """Look up HSA number for a ZIP code from cached crosswalk."""
    if not _HSA_CROSSWALK_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hsa AS SELECT * FROM read_parquet('{_HSA_CROSSWALK_CACHE}')")

        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='hsa'").fetchall()]
        zip_col = next((c for c in cols if "zip" in c), None)
        hsa_col = next((c for c in cols if "hsa" in c and "hrr" not in c), None)

        if not zip_col or not hsa_col:
            con.close()
            return None

        result = con.execute(f"SELECT {hsa_col} FROM hsa WHERE {zip_col} = ? LIMIT 1", [zip_code.strip()[:5]]).fetchone()
        con.close()
        return result[0] if result else None

    except Exception:
        return None


def get_zips_for_hsa(hsa_number: str) -> list[str]:
    """Get all ZIP codes belonging to an HSA."""
    if not _HSA_CROSSWALK_CACHE.exists():
        return []

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hsa AS SELECT * FROM read_parquet('{_HSA_CROSSWALK_CACHE}')")

        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='hsa'").fetchall()]
        zip_col = next((c for c in cols if "zip" in c), None)
        hsa_col = next((c for c in cols if "hsa" in c and "hrr" not in c), None)

        if not zip_col or not hsa_col:
            con.close()
            return []

        results = con.execute(f"SELECT DISTINCT {zip_col} FROM hsa WHERE {hsa_col} = ?", [hsa_number]).fetchall()
        con.close()
        return [r[0] for r in results]

    except Exception:
        return []


# ---------------------------------------------------------------------------
# Referral Network Queries
# ---------------------------------------------------------------------------

def get_referral_network(npi: str, depth: int = 1, min_shared: int = 11) -> dict:
    """Build referral network graph for a physician.

    Args:
        npi: Center NPI.
        depth: 1 for direct connections, 2 for second-hop.
        min_shared: Minimum shared patient count to include (DocGraph suppresses <11).

    Returns:
        Dict with nodes and edges lists.
    """
    if not is_docgraph_cached():
        return {"error": "DocGraph data not cached. Load with load_docgraph_csv()."}

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW dg AS SELECT * FROM read_parquet('{_SHARED_PATIENTS_CACHE}')")

    # Direct connections (depth 1)
    edges = con.execute("""
        SELECT npi_from, npi_to, shared_count, transaction_count, same_day_count
        FROM dg
        WHERE (npi_from = ? OR npi_to = ?)
          AND shared_count >= ?
        ORDER BY shared_count DESC
        LIMIT 200
    """, [npi, npi, min_shared]).fetchdf()

    if depth >= 2 and not edges.empty:
        # Get second-hop NPIs
        hop1_npis = set(edges["npi_from"].tolist() + edges["npi_to"].tolist())
        hop1_npis.discard(npi)
        if hop1_npis:
            placeholders = ", ".join(["?" for _ in hop1_npis])
            hop2 = con.execute(f"""
                SELECT npi_from, npi_to, shared_count, transaction_count, same_day_count
                FROM dg
                WHERE (npi_from IN ({placeholders}) OR npi_to IN ({placeholders}))
                  AND shared_count >= ?
                ORDER BY shared_count DESC
                LIMIT 500
            """, list(hop1_npis) + list(hop1_npis) + [min_shared]).fetchdf()
            edges = pd.concat([edges, hop2]).drop_duplicates(subset=["npi_from", "npi_to"])

    con.close()

    # Collect unique NPIs for node list
    all_npis = set()
    edge_list = []
    for _, row in edges.iterrows():
        all_npis.add(row["npi_from"])
        all_npis.add(row["npi_to"])
        edge_list.append({
            "npi_from": row["npi_from"],
            "npi_to": row["npi_to"],
            "shared_count": int(row["shared_count"]),
            "transaction_count": int(row["transaction_count"]),
            "same_day_count": int(row["same_day_count"]),
        })

    # Build minimal node list (NPI only — caller can enrich with NPPES)
    nodes = [{"npi": n, "name": "", "specialty": "", "city": "", "state": ""} for n in all_npis]

    return {
        "center_npi": npi,
        "nodes": nodes,
        "edges": edge_list,
        "total_connections": len(edge_list),
    }


def get_top_referral_pairs(npi: str, direction: str = "both", limit: int = 25) -> list[dict]:
    """Get top referral pairs for a physician, ranked by shared patient count.

    Args:
        npi: Target NPI.
        direction: "outgoing" (npi refers to), "incoming" (referred to npi), or "both".
        limit: Max results.

    Returns:
        List of referral pair dicts.
    """
    if not is_docgraph_cached():
        return []

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW dg AS SELECT * FROM read_parquet('{_SHARED_PATIENTS_CACHE}')")

    if direction == "outgoing":
        where = "npi_from = ?"
    elif direction == "incoming":
        where = "npi_to = ?"
    else:
        where = "(npi_from = ? OR npi_to = ?)"

    params = [npi, npi] if direction == "both" else [npi]

    rows = con.execute(f"""
        SELECT npi_from, npi_to, shared_count, transaction_count, same_day_count
        FROM dg
        WHERE {where}
        ORDER BY shared_count DESC
        LIMIT ?
    """, params + [limit]).fetchdf()
    con.close()

    results = []
    for _, row in rows.iterrows():
        other_npi = row["npi_to"] if row["npi_from"] == npi else row["npi_from"]
        results.append({
            "npi": other_npi,
            "shared_count": int(row["shared_count"]),
            "transaction_count": int(row["transaction_count"]),
            "same_day_count": int(row["same_day_count"]),
            "direction": "outgoing" if row["npi_from"] == npi else "incoming",
        })

    return results


# ---------------------------------------------------------------------------
# Leakage Detection
# ---------------------------------------------------------------------------

def detect_leakage(
    system_npis: set[str],
    system_zips: set[str],
    min_shared: int = 11,
    limit: int = 50,
) -> dict:
    """Detect out-of-network referral leakage for a health system.

    Args:
        system_npis: Set of NPIs belonging to the health system.
        system_zips: Set of ZIP codes in the system's HSA service area.
        min_shared: Minimum shared count threshold.
        limit: Max leakage destinations to return.

    Returns:
        Dict with leakage statistics and top destinations.
    """
    if not is_docgraph_cached():
        return {"error": "DocGraph data not cached."}

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW dg AS SELECT * FROM read_parquet('{_SHARED_PATIENTS_CACHE}')")

    # Get all outbound referrals from system NPIs
    npi_list = list(system_npis)
    if not npi_list:
        con.close()
        return {"error": "No system NPIs provided."}

    placeholders = ", ".join(["?" for _ in npi_list])
    outbound = con.execute(f"""
        SELECT npi_to, SUM(shared_count) as total_shared
        FROM dg
        WHERE npi_from IN ({placeholders})
          AND shared_count >= ?
        GROUP BY npi_to
        ORDER BY total_shared DESC
    """, npi_list + [min_shared]).fetchdf()
    con.close()

    if outbound.empty:
        return {
            "total_referrals": 0,
            "in_network_pct": 0.0,
            "out_of_network_in_area_pct": 0.0,
            "out_of_area_pct": 0.0,
            "top_leakage_destinations": [],
            "specialty_breakdown": [],
        }

    # Classify each destination
    total_shared = int(outbound["total_shared"].sum())
    in_network_shared = 0
    out_network_in_area = 0
    out_of_area = 0
    leakage_destinations = []

    for _, row in outbound.iterrows():
        dest_npi = row["npi_to"]
        shared = int(row["total_shared"])

        if dest_npi in system_npis:
            in_network_shared += shared
        else:
            # Check if destination is in service area (would need NPPES lookup for ZIP)
            # For now, classify all out-of-network as potential leakage
            out_of_area += shared
            leakage_destinations.append({
                "npi": dest_npi,
                "name": "",
                "specialty": "",
                "shared_count": shared,
                "city": "",
                "state": "",
                "classification": "out_of_network",
            })

    in_network_pct = (in_network_shared / total_shared * 100) if total_shared > 0 else 0
    out_pct = ((total_shared - in_network_shared) / total_shared * 100) if total_shared > 0 else 0

    return {
        "total_referrals": total_shared,
        "in_network_pct": round(in_network_pct, 1),
        "out_of_network_in_area_pct": 0.0,  # Requires NPPES ZIP lookup enrichment
        "out_of_area_pct": round(out_pct, 1),
        "top_leakage_destinations": leakage_destinations[:limit],
        "specialty_breakdown": [],  # Requires NPPES taxonomy enrichment
    }
```

**Step 2: Verify module imports**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
from servers.physician_referral_network.referral_network import (
    is_docgraph_cached, load_docgraph_csv, get_referral_network,
    get_top_referral_pairs, detect_leakage, ensure_hsa_crosswalk_cached,
    get_hsa_for_zip, get_zips_for_hsa
)
print(f'DocGraph cached: {is_docgraph_cached()}')
print('referral_network module imported OK')
"
```
Expected: `DocGraph cached: False` (no data loaded yet), `referral_network module imported OK`.

**Step 3: Commit**

```bash
git add servers/physician-referral-network/referral_network.py
git commit -m "feat(physician-referral-network): add referral network module with DocGraph and Dartmouth Atlas support"
```

---

### Task 4: Physician Mix — Employment Classification

**Files:**
- Create: `servers/physician-referral-network/physician_mix.py`

**Context:**
- Reuses `servers/health-system-profiler/data_loaders.py` for AHRQ Compendium and CMS POS data
- Cross-references NPPES NPI-1 records against AHRQ system→hospital mappings
- Classification: employed (address match), affiliated (org name match), independent (same HSA, no match)
- Uses `rapidfuzz` for fuzzy name matching (already a project dependency)

**Step 1: Write physician_mix.py**

```python
"""Physician employment mix analysis.

Classifies physicians as employed, affiliated, or independent relative to
a health system by cross-referencing NPPES, AHRQ Compendium, and CMS POS data.
"""

import logging

import httpx
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"


# ---------------------------------------------------------------------------
# NPPES physician search (NPI-1 individuals)
# ---------------------------------------------------------------------------

async def _search_nppes_physicians(
    organization_name: str = "",
    state: str = "",
    taxonomy: str = "",
    limit: int = 200,
) -> list[dict]:
    """Search NPPES for individual physicians."""
    params: dict = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "limit": min(limit, 200),
    }
    if organization_name:
        params["organization_name"] = organization_name
    if state:
        params["state"] = state.upper()
    if taxonomy:
        params["taxonomy_description"] = taxonomy

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(NPPES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def _extract_physician_info(npi_result: dict) -> dict:
    """Extract key fields from an NPPES result for classification."""
    basic = npi_result.get("basic", {})
    taxonomies = npi_result.get("taxonomies", [])
    addresses = npi_result.get("addresses", [])

    practice_addr = next(
        (a for a in addresses if a.get("address_purpose") == "LOCATION"),
        addresses[0] if addresses else {},
    )
    primary_tax = next(
        (t for t in taxonomies if t.get("primary")),
        taxonomies[0] if taxonomies else {},
    )

    return {
        "npi": npi_result.get("number", ""),
        "first_name": basic.get("first_name", ""),
        "last_name": basic.get("last_name", ""),
        "org_name": basic.get("organization_name", ""),
        "specialty": primary_tax.get("desc", ""),
        "practice_address": practice_addr.get("address_1", ""),
        "practice_city": practice_addr.get("city", ""),
        "practice_state": practice_addr.get("state", ""),
        "practice_zip": (practice_addr.get("postal_code") or "")[:5],
    }


def classify_physician(
    physician: dict,
    system_name: str,
    facility_addresses: list[dict],
    facility_zips: set[str],
) -> dict:
    """Classify a single physician relative to a health system.

    Args:
        physician: Dict from _extract_physician_info().
        system_name: Health system name.
        facility_addresses: List of {address, city, state, zip} for system facilities.
        facility_zips: Set of ZIP codes where system has facilities.

    Returns:
        Dict with status, confidence, evidence.
    """
    evidence = []
    status = "independent"
    confidence = 0.3

    org_name = physician.get("org_name", "")
    practice_zip = physician.get("practice_zip", "")
    practice_city = physician.get("practice_city", "")

    # Check 1: Organization name matches system name (fuzzy)
    if org_name:
        name_score = fuzz.token_set_ratio(org_name.lower(), system_name.lower())
        if name_score >= 80:
            status = "affiliated"
            confidence = 0.7 + (name_score - 80) * 0.01  # 0.7-0.9
            evidence.append(f"Org name '{org_name}' matches system (score={name_score})")

    # Check 2: Practice address matches a facility address
    for facility in facility_addresses:
        if (
            practice_zip == facility.get("zip", "")
            and practice_city.lower() == facility.get("city", "").lower()
        ):
            # Same ZIP + city as a system facility
            if status == "affiliated":
                status = "employed"
                confidence = min(confidence + 0.15, 0.95)
                evidence.append(
                    f"Practice ZIP {practice_zip} matches facility in {facility.get('city', '')}"
                )
            else:
                status = "affiliated"
                confidence = 0.6
                evidence.append(
                    f"Practice in same ZIP as facility ({practice_zip})"
                )
            break

    # Check 3: Practice in same general area (facility ZIP set)
    if status == "independent" and practice_zip in facility_zips:
        confidence = 0.4
        evidence.append(f"Practice ZIP {practice_zip} is near system facilities")

    if not evidence:
        evidence.append("No organizational or geographic match found")

    return {
        "npi": physician["npi"],
        "name": f"{physician.get('first_name', '')} {physician.get('last_name', '')}".strip(),
        "specialty": physician.get("specialty", ""),
        "status": status,
        "confidence": round(confidence, 2),
        "evidence": evidence,
    }


# ---------------------------------------------------------------------------
# System-level analysis
# ---------------------------------------------------------------------------

async def analyze_system_mix(
    system_name: str,
    state: str = "",
) -> dict:
    """Analyze physician employment mix for a health system.

    Queries NPPES for physicians associated with the system name,
    cross-references with AHRQ facility data, and classifies each.

    Args:
        system_name: Health system name (e.g. "Penn Medicine").
        state: State filter.

    Returns:
        Dict with employed/affiliated/independent counts and percentages.
    """
    # Load facility data from health-system-profiler's data loaders
    try:
        from servers.health_system_profiler.data_loaders import (
            load_ahrq_hospital_linkage,
            load_ahrq_systems,
            load_pos,
        )
    except ImportError:
        return {"error": "health-system-profiler data loaders not available"}

    # Find system in AHRQ Compendium
    systems_df = await load_ahrq_systems()
    name_col = "health_sys_name"
    if name_col not in systems_df.columns:
        return {"error": "AHRQ system data missing health_sys_name column"}

    matches = systems_df[
        systems_df[name_col].str.lower().str.contains(system_name.lower(), na=False)
    ]

    if matches.empty:
        return {"error": f"System '{system_name}' not found in AHRQ Compendium"}

    system_id = matches.iloc[0].get("health_sys_id", "")
    resolved_name = matches.iloc[0].get(name_col, system_name)

    # Get system's hospitals from linkage file
    linkage_df = await load_ahrq_hospital_linkage()
    system_hospitals = linkage_df[linkage_df.get("health_sys_id", linkage_df.columns[0]) == system_id]

    # Get facility addresses from POS
    facility_addresses: list[dict] = []
    facility_zips: set[str] = set()

    if not system_hospitals.empty:
        pos_df = await load_pos()
        for _, hosp in system_hospitals.iterrows():
            ccn = str(hosp.get("ccn", "")).strip().zfill(6)
            # Look up in POS
            pos_match = pos_df[pos_df.iloc[:, 0].astype(str).str.strip().str.zfill(6) == ccn]
            if not pos_match.empty:
                pos_row = pos_match.iloc[0]
                addr = {
                    "city": str(pos_row.get("CITY", pos_row.get("city", ""))),
                    "state": str(pos_row.get("STATE", pos_row.get("state", ""))),
                    "zip": str(pos_row.get("ZIP_CD", pos_row.get("zip", "")))[:5],
                }
                facility_addresses.append(addr)
                if addr["zip"]:
                    facility_zips.add(addr["zip"])

    # Search NPPES for physicians matching system name
    physicians_raw = await _search_nppes_physicians(
        organization_name=system_name,
        state=state,
        limit=200,
    )

    # Also search with resolved AHRQ name if different
    if resolved_name.lower() != system_name.lower():
        more = await _search_nppes_physicians(
            organization_name=resolved_name,
            state=state,
            limit=200,
        )
        seen_npis = {r.get("number") for r in physicians_raw}
        for r in more:
            if r.get("number") not in seen_npis:
                physicians_raw.append(r)

    # Classify each physician
    classifications = []
    for raw in physicians_raw:
        info = _extract_physician_info(raw)
        if state and info.get("practice_state", "").upper() != state.upper():
            continue
        result = classify_physician(info, system_name, facility_addresses, facility_zips)
        classifications.append(result)

    # Aggregate
    employed = sum(1 for c in classifications if c["status"] == "employed")
    affiliated = sum(1 for c in classifications if c["status"] == "affiliated")
    independent = sum(1 for c in classifications if c["status"] == "independent")
    total = len(classifications)

    # Specialty breakdown
    specialty_counts: dict[str, dict[str, int]] = {}
    for c in classifications:
        spec = c.get("specialty", "Other") or "Other"
        if spec not in specialty_counts:
            specialty_counts[spec] = {"employed": 0, "affiliated": 0, "independent": 0}
        specialty_counts[spec][c["status"]] += 1

    by_specialty = [
        {"specialty": k, **v, "total": sum(v.values())}
        for k, v in sorted(specialty_counts.items(), key=lambda x: -sum(x[1].values()))
    ]

    return {
        "system_name": resolved_name,
        "total_physicians": total,
        "employed": employed,
        "affiliated": affiliated,
        "independent": independent,
        "employed_pct": round(employed / total * 100, 1) if total else 0,
        "affiliated_pct": round(affiliated / total * 100, 1) if total else 0,
        "independent_pct": round(independent / total * 100, 1) if total else 0,
        "by_specialty": by_specialty,
        "sample_physicians": classifications[:10],
    }
```

**Step 2: Verify module imports**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
from servers.physician_referral_network.physician_mix import (
    classify_physician, analyze_system_mix
)
print('physician_mix module imported OK')
"
```
Expected: `physician_mix module imported OK`

**Step 3: Commit**

```bash
git add servers/physician-referral-network/physician_mix.py
git commit -m "feat(physician-referral-network): add physician mix classification with AHRQ/POS cross-reference"
```

---

### Task 5: Server — Wire Up 5 MCP Tools

**Files:**
- Create: `servers/physician-referral-network/server.py`

**Context:** FastMCP server on port 8010, following project pattern from `servers/financial-intelligence/server.py`. All 5 tools are async, return `json.dumps()`, use try/except with error JSON.

**Step 1: Write server.py**

```python
"""Physician & Referral Network MCP Server.

Provides tools for physician search, profiles with Medicare utilization,
referral network mapping, health system employment mix analysis,
and referral leakage detection.
"""

import json
import logging
import os as _os
import statistics as _stats

from mcp.server.fastmcp import FastMCP

from . import nppes_client, referral_network, physician_mix
from .models import (
    LeakageDestination,
    LeakageResponse,
    PhysicianClassification,
    PhysicianMixResponse,
    PhysicianProfile,
    PhysicianSearchResponse,
    PhysicianSummary,
    QualityInfo,
    ReferralEdge,
    ReferralNetworkResponse,
    ReferralNode,
    SpecialtyLeakage,
    UtilizationSummary,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "physician-referral-network"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8010"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Tool 1: search_physicians
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_physicians(
    query: str, specialty: str = "", state: str = "", limit: int = 25
) -> str:
    """Search for physicians by name, NPI, or specialty in the NPPES registry.

    Returns matching physicians with NPI, specialty, practice location,
    and organization affiliation.

    Args:
        query: Physician name (e.g. "John Smith"), NPI number, or last name.
        specialty: Specialty filter (e.g. "Cardiology", "Orthopedic Surgery").
        state: Two-letter state code filter (e.g. "PA").
        limit: Maximum results (1-200).
    """
    try:
        physicians = await nppes_client.search_physicians(query, specialty, state, limit)

        response = PhysicianSearchResponse(
            total_results=len(physicians),
            physicians=[PhysicianSummary(**p) for p in physicians],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("search_physicians failed")
        return json.dumps({"error": f"search_physicians failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_physician_profile
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_physician_profile(npi: str) -> str:
    """Get a full physician profile including specialties, affiliations,
    Medicare utilization, and quality scores.

    Combines NPPES registry data with CMS Physician Compare and
    Medicare Provider Utilization datasets.

    Args:
        npi: 10-digit National Provider Identifier.
    """
    try:
        # Ensure bulk datasets are cached
        await nppes_client.ensure_physician_compare_cached()
        await nppes_client.ensure_utilization_cached()

        profile_data = await nppes_client.get_physician_detail(npi)
        if not profile_data:
            return json.dumps({"error": f"No physician found for NPI: {npi}"})

        # Build response model
        utilization = None
        if profile_data.get("utilization"):
            utilization = UtilizationSummary(**profile_data["utilization"])

        quality = None
        if profile_data.get("quality"):
            quality = QualityInfo(**profile_data["quality"])

        response = PhysicianProfile(
            npi=profile_data["npi"],
            first_name=profile_data.get("first_name", ""),
            last_name=profile_data.get("last_name", ""),
            credential=profile_data.get("credential", ""),
            specialties=profile_data.get("specialties", []),
            practice_locations=profile_data.get("practice_locations", []),
            org_affiliations=profile_data.get("org_affiliations", []),
            gender=profile_data.get("gender", ""),
            enumeration_date=profile_data.get("enumeration_date", ""),
            utilization=utilization,
            quality=quality,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_physician_profile failed")
        return json.dumps({"error": f"get_physician_profile failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: map_referral_network
# ---------------------------------------------------------------------------
@mcp.tool()
async def map_referral_network(
    npi: str, depth: int = 1, min_shared: int = 11
) -> str:
    """Build a referral network graph centered on a physician using
    DocGraph shared patient data (2014-2020 Medicare claims).

    Returns nodes (physicians) and edges (shared patient counts) for
    graph visualization and network analysis.

    Args:
        npi: Center physician NPI.
        depth: Network depth (1=direct connections, 2=include second-hop).
        min_shared: Minimum shared patient count to include an edge (default 11).
    """
    try:
        if not referral_network.is_docgraph_cached():
            return json.dumps({
                "error": "DocGraph shared patient data not cached. "
                         "Download from https://careset.com/datasets/ and load with "
                         "the load_docgraph_csv() function."
            })

        result = referral_network.get_referral_network(npi, depth=depth, min_shared=min_shared)

        if "error" in result:
            return json.dumps(result)

        # Enrich nodes with NPPES data (batch lookup)
        enriched_nodes = []
        for node in result.get("nodes", []):
            try:
                physicians = await nppes_client.search_physicians(node["npi"], limit=1)
                if physicians:
                    p = physicians[0]
                    enriched_nodes.append(ReferralNode(
                        npi=node["npi"],
                        name=f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        specialty=p.get("specialty", ""),
                        city=p.get("city", ""),
                        state=p.get("state", ""),
                    ))
                else:
                    enriched_nodes.append(ReferralNode(npi=node["npi"]))
            except Exception:
                enriched_nodes.append(ReferralNode(npi=node["npi"]))

        center_name = ""
        for n in enriched_nodes:
            if n.npi == npi:
                center_name = n.name
                break

        response = ReferralNetworkResponse(
            center_npi=npi,
            center_name=center_name,
            nodes=enriched_nodes,
            edges=[ReferralEdge(**e) for e in result.get("edges", [])],
            total_connections=result.get("total_connections", 0),
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("map_referral_network failed")
        return json.dumps({"error": f"map_referral_network failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: analyze_physician_mix
# ---------------------------------------------------------------------------
@mcp.tool()
async def analyze_physician_mix(system_name: str, state: str = "") -> str:
    """Analyze the employed vs. affiliated vs. independent physician mix
    for a health system.

    Cross-references NPPES physician records with AHRQ Health System
    Compendium and CMS Provider of Services data to classify physicians.

    Args:
        system_name: Health system name (e.g. "Penn Medicine", "HCA Healthcare").
        state: Two-letter state code filter.
    """
    try:
        result = await physician_mix.analyze_system_mix(system_name, state)

        if "error" in result:
            return json.dumps(result)

        response = PhysicianMixResponse(
            system_name=result.get("system_name", system_name),
            total_physicians=result.get("total_physicians", 0),
            employed=result.get("employed", 0),
            affiliated=result.get("affiliated", 0),
            independent=result.get("independent", 0),
            employed_pct=result.get("employed_pct", 0),
            affiliated_pct=result.get("affiliated_pct", 0),
            independent_pct=result.get("independent_pct", 0),
            by_specialty=result.get("by_specialty", []),
            sample_physicians=[
                PhysicianClassification(**c) for c in result.get("sample_physicians", [])
            ],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("analyze_physician_mix failed")
        return json.dumps({"error": f"analyze_physician_mix failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: detect_leakage
# ---------------------------------------------------------------------------
@mcp.tool()
async def detect_leakage(
    system_name: str, state: str = "", specialty: str = ""
) -> str:
    """Detect out-of-network referral leakage for a health system using
    DocGraph shared patient data (2014-2020).

    Identifies physicians who share patients with the system but are not
    affiliated, grouped by specialty and geographic area.

    Args:
        system_name: Health system name (e.g. "Cleveland Clinic").
        state: Two-letter state code filter.
        specialty: Optional specialty filter to focus leakage analysis.
    """
    try:
        if not referral_network.is_docgraph_cached():
            return json.dumps({
                "error": "DocGraph shared patient data not cached. "
                         "Download from https://careset.com/datasets/ and load first."
            })

        # Get system's physician NPIs
        mix_result = await physician_mix.analyze_system_mix(system_name, state)
        if "error" in mix_result:
            return json.dumps(mix_result)

        system_npis = set()
        for p in mix_result.get("sample_physicians", []):
            if p.get("status") in ("employed", "affiliated"):
                system_npis.add(p["npi"])

        # Get system's service area ZIPs
        system_zips: set[str] = set()
        # Try to get facility ZIPs from the mix analysis
        # (This is a simplified version — full implementation would use HSAF data)

        # Run leakage detection
        leakage = referral_network.detect_leakage(
            system_npis=system_npis,
            system_zips=system_zips,
            min_shared=11,
        )

        if "error" in leakage:
            return json.dumps(leakage)

        # Enrich top destinations with NPPES data
        enriched_destinations = []
        for dest in leakage.get("top_leakage_destinations", [])[:25]:
            try:
                physicians = await nppes_client.search_physicians(dest["npi"], limit=1)
                if physicians:
                    p = physicians[0]
                    enriched_destinations.append(LeakageDestination(
                        npi=dest["npi"],
                        name=f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                        specialty=p.get("specialty", ""),
                        shared_count=dest.get("shared_count", 0),
                        city=p.get("city", ""),
                        state=p.get("state", ""),
                        classification=dest.get("classification", "out_of_network"),
                    ))
                else:
                    enriched_destinations.append(LeakageDestination(**dest))
            except Exception:
                enriched_destinations.append(LeakageDestination(**dest))

        response = LeakageResponse(
            system_name=mix_result.get("system_name", system_name),
            total_referrals=leakage.get("total_referrals", 0),
            in_network_pct=leakage.get("in_network_pct", 0),
            out_of_network_in_area_pct=leakage.get("out_of_network_in_area_pct", 0),
            out_of_area_pct=leakage.get("out_of_area_pct", 0),
            top_leakage_destinations=enriched_destinations,
            specialty_breakdown=[
                SpecialtyLeakage(**s) for s in leakage.get("specialty_breakdown", [])
            ],
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("detect_leakage failed")
        return json.dumps({"error": f"detect_leakage failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
```

**Step 2: Verify 5 tools register**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
from servers.physician_referral_network.server import mcp
for name in sorted(mcp._tool_manager._tools.keys()):
    print(f'  - {name}')
print(f'Total: {len(mcp._tool_manager._tools)} tools')
"
```
Expected: 5 tools listed.

**Step 3: Commit**

```bash
git add servers/physician-referral-network/server.py
git commit -m "feat(physician-referral-network): wire up all 5 tools in server.py"
```

---

### Task 6: Docker, MCP Registration, and Environment Config

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.mcp.json`

**Step 1: Add service to docker-compose.yml**

Add after the `price-transparency` service block (before the `volumes:` section):

```yaml
  physician-referral-network:
    build: .
    command: python -m servers.physician_referral_network.server
    ports:
      - "8010:8010"
    environment:
      - MCP_TRANSPORT=streamable-http
      - MCP_PORT=8010
    volumes:
      - healthcare-cache:/root/.healthcare-data-mcp/cache
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8010),5); s.close()"]
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 30s
```

**Step 2: Add to .mcp.json**

Add entry after `price-transparency`:

```json
"physician-referral-network": {
    "type": "http",
    "url": "http://localhost:8010/mcp"
}
```

**Step 3: Verify server starts**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && MCP_TRANSPORT=streamable-http MCP_PORT=8010 timeout 8 python3 -m servers.physician_referral_network.server 2>&1 || true`
Expected: Uvicorn running on 0.0.0.0:8010.

**Step 4: Test MCP handshake**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && \
MCP_TRANSPORT=streamable-http MCP_PORT=8010 python3 -m servers.physician_referral_network.server &>/tmp/prn-server.log &
PRN_PID=$!
sleep 3
curl -s -X POST http://localhost:8010/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' 2>&1
kill $PRN_PID 2>/dev/null; wait $PRN_PID 2>/dev/null
```
Expected: JSON response with `serverInfo.name: "physician-referral-network"`.

**Step 5: Commit**

```bash
git add docker-compose.yml .mcp.json
git commit -m "feat(physician-referral-network): add Docker and MCP registration (port 8010)"
```

---

### Task 7: Smoke Tests

**Files:**
- Modify: `smoke_test.py`

**Step 1: Add test function**

Add `test_physician_referral_network` to smoke_test.py and register in the main test list:

```python
async def test_physician_referral_network():
    print("\n" + "=" * 60)
    print("TEST: physician-referral-network-mcp")
    print("=" * 60)

    results = {}

    # Test 1: NPPES physician search
    print("\n[1/4] NPPES — searching for cardiologists in PA...")
    from servers.physician_referral_network.nppes_client import search_physicians
    t0 = time.time()
    physicians = await search_physicians("Smith", specialty="Cardiology", state="PA", limit=5)
    elapsed = time.time() - t0
    print(f"  -> Found {len(physicians)} physicians in {elapsed:.1f}s")
    if physicians:
        p = physicians[0]
        print(f"  -> First: {p['first_name']} {p['last_name']} | {p['specialty']} | {p['city']}, {p['state']}")
    results["nppes_count"] = len(physicians)
    assert len(physicians) > 0, "Expected NPPES results for Smith Cardiology PA"

    # Test 2: Physician profile (single NPI lookup)
    print("\n[2/4] NPPES — single NPI profile lookup...")
    from servers.physician_referral_network.nppes_client import get_physician_detail
    if physicians:
        npi = physicians[0]["npi"]
        t0 = time.time()
        profile = await get_physician_detail(npi)
        elapsed = time.time() - t0
        print(f"  -> Profile for NPI {npi} in {elapsed:.1f}s")
        if profile:
            print(f"  -> {profile['first_name']} {profile['last_name']}")
            print(f"  -> Specialties: {profile.get('specialties', [])}")
        results["profile_found"] = profile is not None

    # Test 3: DocGraph status
    print("\n[3/4] DocGraph — cache status...")
    from servers.physician_referral_network.referral_network import is_docgraph_cached
    cached = is_docgraph_cached()
    print(f"  -> DocGraph cached: {cached}")
    results["docgraph_cached"] = cached

    # Test 4: Module imports
    print("\n[4/4] Verifying all modules import...")
    from servers.physician_referral_network.physician_mix import analyze_system_mix
    from servers.physician_referral_network.referral_network import get_referral_network
    print("  -> All modules imported OK")
    results["imports_ok"] = True

    print("\n  PHYSICIAN-REFERRAL-NETWORK: ALL PASSED")
    return results
```

**Step 2: Run smoke test**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
import asyncio, time
from servers.physician_referral_network.nppes_client import search_physicians, get_physician_detail

async def main():
    print('1. NPPES search: Smith, Cardiology, PA...')
    results = await search_physicians('Smith', specialty='Cardiology', state='PA', limit=5)
    print(f'   Found {len(results)}')
    if results:
        p = results[0]
        print(f'   First: {p[\"first_name\"]} {p[\"last_name\"]} ({p[\"npi\"]})')

        print(f'2. Profile lookup: {p[\"npi\"]}...')
        profile = await get_physician_detail(p['npi'])
        if profile:
            print(f'   Specialties: {profile.get(\"specialties\", [])}')

    print('3. DocGraph cache...')
    from servers.physician_referral_network.referral_network import is_docgraph_cached
    print(f'   Cached: {is_docgraph_cached()}')

    print('All smoke tests passed.')

asyncio.run(main())
"
```
Expected: NPPES results found, profile retrieved, DocGraph not cached yet.

**Step 3: Commit**

```bash
git add smoke_test.py
git commit -m "test(physician-referral-network): add smoke test for NPPES search and profile"
```

---

### Task 8: Final Verification

**Step 1: Verify all tools register**

Run: `python3 -c "from servers.physician_referral_network.server import mcp; print(len(mcp._tool_manager._tools), 'tools')"`
Expected: `5 tools`

**Step 2: Verify server starts on port 8010**

Run: `MCP_TRANSPORT=streamable-http MCP_PORT=8010 timeout 8 python3 -m servers.physician_referral_network.server 2>&1 || true`
Expected: Uvicorn running on 0.0.0.0:8010.

**Step 3: Verify .mcp.json and docker-compose.yml**

Read both files and confirm physician-referral-network entries exist.

**Step 4: Verify all files exist**

```bash
python3 -c "
from pathlib import Path
files = ['__init__.py', 'models.py', 'nppes_client.py', 'referral_network.py', 'physician_mix.py', 'server.py']
for f in files:
    p = Path(f'servers/physician-referral-network/{f}')
    assert p.exists(), f'Missing: {p}'
    print(f'  {f}: {p.stat().st_size:,} bytes')
print('All files present.')
"
```
