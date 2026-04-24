# Health System Profiler — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an MCP server (`health-system-profiler`, port 8007) that returns a complete health system profile in 1-3 tool calls by combining AHRQ Compendium, CMS POS, NPPES, and HSAF data.

**Architecture:** Three-layer pipeline — Layer 1: AHRQ Compendium for system→CCN discovery. Layer 2: CMS POS file for facility enrichment (beds, services, staffing). Layer 3: NPPES wildcard search for outpatient sites + HSAF for service areas. FastMCP server exposing 3 tools.

**Tech Stack:** Python 3.11+, FastMCP, pandas, httpx, rapidfuzz, pydantic, pytest + pytest-asyncio

**Design Doc:** `docs/plans/2026-03-02-health-system-profiler-design.md`

---

## Task 1: Project Scaffolding

**Files:**
- Create: `servers/health-system-profiler/__init__.py`
- Create: `servers/health-system-profiler/server.py` (stub)
- Create: `tests/servers/health_system_profiler/__init__.py`
- Create symlink: `servers/health_system_profiler -> health-system-profiler`
- Modify: `pyproject.toml`

**Step 1: Create the server directory and symlink**

```bash
mkdir -p "servers/health-system-profiler"
touch "servers/health-system-profiler/__init__.py"
ln -s health-system-profiler servers/health_system_profiler
mkdir -p "tests/servers/health_system_profiler"
touch "tests/__init__.py"
touch "tests/servers/__init__.py"
touch "tests/servers/health_system_profiler/__init__.py"
```

**Step 2: Create stub server.py**

```python
# servers/health-system-profiler/server.py
"""Health System Profiler MCP Server.

Returns complete health system profiles in 1-3 tool calls by combining
AHRQ Compendium, CMS Provider of Services, NPPES, and HSAF data.
"""

import json
import logging
import os as _os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "health-system-profiler"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8007"))
mcp = FastMCP(**_mcp_kwargs)


@mcp.tool()
async def search_health_systems(query: str) -> str:
    """Search for health systems by name. Stub."""
    return json.dumps({"error": "Not implemented"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
```

**Step 3: Add `rapidfuzz` dependency to pyproject.toml**

In `pyproject.toml`, add `"rapidfuzz>=3.0.0"` to the `dependencies` list.

**Step 4: Verify the stub server starts**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp"
python -m servers.health_system_profiler.server &
PID=$!
sleep 2
kill $PID 2>/dev/null
echo "Server started and stopped OK"
```

Expected: Server starts without import errors.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/ servers/health_system_profiler tests/ pyproject.toml
git commit -m "feat(health-system-profiler): scaffold server directory and stub"
```

---

## Task 2: Pydantic Response Models

**Files:**
- Create: `servers/health-system-profiler/models.py`
- Create: `tests/servers/health_system_profiler/test_models.py`

**Step 1: Write the failing test**

```python
# tests/servers/health_system_profiler/test_models.py
"""Tests for health system profiler response models."""

from servers.health_system_profiler.models import (
    BedBreakdown,
    FacilitySummary,
    HealthSystemSummary,
    OffSiteSummary,
    OutpatientSite,
    ServiceCapabilities,
    StaffingCounts,
    SubEntity,
    SystemProfileResponse,
    SystemSearchResult,
)


def test_system_search_result_defaults():
    result = SystemSearchResult(system_id="SYS_001", name="Test Health")
    assert result.system_id == "SYS_001"
    assert result.name == "Test Health"
    assert result.hq_city == ""
    assert result.hq_state == ""
    assert result.hospital_count == 0
    assert result.total_beds == 0


def test_bed_breakdown_defaults():
    beds = BedBreakdown()
    assert beds.total == 0
    assert beds.certified == 0
    assert beds.psychiatric == 0
    assert beds.rehabilitation == 0


def test_facility_summary_serialization():
    facility = FacilitySummary(
        ccn="390133",
        name="Test Hospital",
        beds=BedBreakdown(total=500, certified=480),
    )
    d = facility.model_dump()
    assert d["ccn"] == "390133"
    assert d["beds"]["total"] == 500
    assert d["beds"]["certified"] == 480


def test_system_profile_response_structure():
    profile = SystemProfileResponse(
        system=HealthSystemSummary(
            system_id="SYS_001",
            name="Test Health",
            hq_city="Philadelphia",
            hq_state="PA",
            hospital_count=3,
            total_beds=1500,
            total_discharges=50000,
        ),
        inpatient_facilities=[],
        sub_entities=[],
        outpatient_sites=[],
        off_site_summary=OffSiteSummary(),
    )
    d = profile.model_dump()
    assert d["system"]["name"] == "Test Health"
    assert d["system"]["hospital_count"] == 3
    assert isinstance(d["inpatient_facilities"], list)
    assert isinstance(d["outpatient_sites"], list)
```

**Step 2: Run test to verify it fails**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp"
python -m pytest tests/servers/health_system_profiler/test_models.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'servers.health_system_profiler.models'`

**Step 3: Write the models**

```python
# servers/health-system-profiler/models.py
"""Pydantic models for health system profiler responses."""

from pydantic import BaseModel, Field


class SystemSearchResult(BaseModel):
    """A health system found by search."""

    system_id: str = Field(description="AHRQ health system ID")
    name: str = ""
    hq_city: str = ""
    hq_state: str = ""
    hospital_count: int = 0
    total_beds: int = 0


class BedBreakdown(BaseModel):
    """Bed counts by type from POS file."""

    total: int = 0
    certified: int = 0
    psychiatric: int = 0
    rehabilitation: int = 0
    hospice: int = 0
    ventilator: int = 0
    aids: int = 0
    alzheimer: int = 0
    dialysis: int = 0


class ServiceCapabilities(BaseModel):
    """Clinical service flags from POS file."""

    cardiac_catheterization: bool = False
    open_heart_surgery: bool = False
    mri: bool = False
    ct_scanner: bool = False
    pet_scanner: bool = False
    nuclear_medicine: bool = False
    trauma_center: bool = False
    trauma_level: str = ""
    burn_care: bool = False
    neonatal_icu: bool = False
    obstetrics: bool = False
    transplant: bool = False
    emergency_department: bool = False
    operating_rooms: int = 0
    endoscopy_rooms: int = 0
    cardiac_cath_rooms: int = 0


class StaffingCounts(BaseModel):
    """Staffing counts from POS file."""

    rn: int = 0
    lpn: int = 0
    physicians: int = 0
    pharmacists: int = 0
    therapists: int = 0
    total_fte: float = 0.0


class ServiceArea(BaseModel):
    """PSA/SSA for a facility."""

    psa_zips: list[str] = Field(default_factory=list)
    psa_discharge_count: int = 0
    psa_pct: float = 0.0
    ssa_zips: list[str] = Field(default_factory=list)
    ssa_discharge_count: int = 0
    ssa_pct: float = 0.0
    total_discharges: int = 0


class FacilitySummary(BaseModel):
    """A single inpatient facility within the system."""

    ccn: str = Field(description="CMS Certification Number")
    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    county: str = ""
    phone: str = ""
    hospital_type: str = ""
    ownership: str = ""
    teaching_status: str = ""
    beds: BedBreakdown = Field(default_factory=BedBreakdown)
    services: ServiceCapabilities = Field(default_factory=ServiceCapabilities)
    staffing: StaffingCounts = Field(default_factory=StaffingCounts)
    overall_quality_rating: str = ""
    service_area: ServiceArea | None = None


class SubEntity(BaseModel):
    """A related sub-entity (dialysis, rehab, etc.) linked via RELATED_PROVIDER_NUMBER."""

    ccn: str = ""
    name: str = ""
    parent_ccn: str = ""
    facility_type: str = ""
    city: str = ""
    state: str = ""
    beds: int = 0


class OutpatientSite(BaseModel):
    """An outpatient site discovered via NPPES."""

    npi: str = ""
    name: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    phone: str = ""
    taxonomy_code: str = ""
    taxonomy_description: str = ""
    category: str = ""


class OffSiteSummary(BaseModel):
    """Aggregated off-site location counts from POS."""

    emergency_departments: int = 0
    urgent_care_centers: int = 0
    psychiatric_units: int = 0
    rehabilitation_hospitals: int = 0
    total_off_site: int = 0


class HealthSystemSummary(BaseModel):
    """System-level aggregated summary."""

    system_id: str = ""
    name: str = ""
    hq_city: str = ""
    hq_state: str = ""
    hospital_count: int = 0
    total_beds: int = 0
    total_discharges: int = 0
    physician_group_count: int = 0


class SystemProfileResponse(BaseModel):
    """Complete system profile — the main response type."""

    system: HealthSystemSummary = Field(default_factory=HealthSystemSummary)
    inpatient_facilities: list[FacilitySummary] = Field(default_factory=list)
    sub_entities: list[SubEntity] = Field(default_factory=list)
    outpatient_sites: list[OutpatientSite] = Field(default_factory=list)
    off_site_summary: OffSiteSummary = Field(default_factory=OffSiteSummary)
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_models.py -v
```

Expected: All 4 tests PASS.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/models.py tests/servers/health_system_profiler/test_models.py
git commit -m "feat(health-system-profiler): add pydantic response models"
```

---

## Task 3: AHRQ Compendium Data Loader

**Files:**
- Create: `servers/health-system-profiler/data_loaders.py`
- Create: `tests/servers/health_system_profiler/test_data_loaders.py`
- Create: `scripts/download_ahrq.py` (Playwright-based one-time download)

**Context:** AHRQ files are behind AWS WAF. Strategy: try HTTP first → if blocked (status 202 or non-CSV response), raise informative error pointing user to run the Playwright download script. Files are cached at `~/.healthcare-data-mcp/cache/ahrq_*.csv`.

**Step 1: Write the failing test for AHRQ parsing**

```python
# tests/servers/health_system_profiler/test_data_loaders.py
"""Tests for health system profiler data loaders."""

import pandas as pd
import pytest

from servers.health_system_profiler.data_loaders import (
    parse_ahrq_hospital_linkage,
    parse_ahrq_system_file,
    parse_pos_file,
)


@pytest.fixture
def sample_ahrq_system_csv(tmp_path):
    """Create a minimal AHRQ system file CSV."""
    csv_path = tmp_path / "system.csv"
    csv_path.write_text(
        "health_sys_id,health_sys_name,health_sys_city,health_sys_state,hosp_count,phys_grp_count\n"
        "SYS_001,Jefferson Health,Philadelphia,PA,14,25\n"
        "SYS_002,Lehigh Valley Health Network,Allentown,PA,8,12\n"
        "SYS_003,Penn Medicine,Philadelphia,PA,6,30\n"
    )
    return csv_path


@pytest.fixture
def sample_ahrq_hospital_linkage_csv(tmp_path):
    """Create a minimal AHRQ hospital linkage CSV."""
    csv_path = tmp_path / "hospital_linkage.csv"
    csv_path.write_text(
        "health_sys_id,ccn,hospital_name,hosp_addr,hosp_city,hosp_state,hosp_zip,hos_beds,hos_dsch,ownership,revenue,teaching\n"
        "SYS_001,390001,Thomas Jefferson University Hospital,111 S 11th St,Philadelphia,PA,19107,900,40000,Voluntary nonprofit,1000000,Yes\n"
        "SYS_001,390149,Jefferson Einstein Philadelphia,5501 Old York Rd,Philadelphia,PA,19141,500,20000,Voluntary nonprofit,500000,Yes\n"
        "SYS_002,390133,Lehigh Valley Hospital-Cedar Crest,1200 S Cedar Crest Blvd,Allentown,PA,18103,1190,55000,Voluntary nonprofit,2000000,Yes\n"
        "SYS_002,390263,Lehigh Valley Hospital-Muhlenberg,2545 Schoenersville Rd,Bethlehem,PA,18017,184,8000,Voluntary nonprofit,300000,No\n"
    )
    return csv_path


def test_parse_ahrq_system_file(sample_ahrq_system_csv):
    df = parse_ahrq_system_file(sample_ahrq_system_csv)
    assert len(df) == 3
    assert "health_sys_id" in df.columns
    assert "health_sys_name" in df.columns
    assert df.iloc[0]["health_sys_name"] == "Jefferson Health"


def test_parse_ahrq_hospital_linkage(sample_ahrq_hospital_linkage_csv):
    df = parse_ahrq_hospital_linkage(sample_ahrq_hospital_linkage_csv)
    assert len(df) == 4
    assert "health_sys_id" in df.columns
    assert "ccn" in df.columns
    # CCN should be 6-char zero-padded string
    assert df.iloc[0]["ccn"] == "390001"
    # Beds should be int
    assert df.iloc[0]["hos_beds"] == 900
    # Filter by system
    jefferson = df[df["health_sys_id"] == "SYS_001"]
    assert len(jefferson) == 2
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/servers/health_system_profiler/test_data_loaders.py -v
```

Expected: FAIL with `ImportError`

**Step 3: Implement data_loaders.py**

```python
# servers/health-system-profiler/data_loaders.py
"""Data loading and caching for AHRQ Compendium, CMS POS, and NPPES."""

import logging
from pathlib import Path

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# --- AHRQ Compendium URLs (2023 release) ---
AHRQ_SYSTEM_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv"
)
AHRQ_HOSPITAL_LINKAGE_URL = (
    "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv"
)

AHRQ_SYSTEM_CACHE = CACHE_DIR / "ahrq_system_2023.csv"
AHRQ_HOSPITAL_LINKAGE_CACHE = CACHE_DIR / "ahrq_hospital_linkage_2023.csv"

# --- CMS POS File (Q4 2025) ---
POS_URL = (
    "https://data.cms.gov/sites/default/files/2026-01/"
    "c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv"
)
POS_CACHE = CACHE_DIR / "pos_q4_2025.csv"

# --- NPPES API ---
NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"

# --- In-memory caches ---
_ahrq_systems_df: pd.DataFrame | None = None
_ahrq_hospitals_df: pd.DataFrame | None = None
_pos_df: pd.DataFrame | None = None


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Find the first matching column name (case-insensitive)."""
    lower_map = {col.lower().strip(): col for col in df.columns}
    for c in candidates:
        if c in df.columns:
            return c
        if c.lower().strip() in lower_map:
            return lower_map[c.lower().strip()]
    return None


# ============================================================
# AHRQ Compendium loaders
# ============================================================

def parse_ahrq_system_file(path: Path) -> pd.DataFrame:
    """Parse the AHRQ Compendium system file.

    Returns DataFrame with columns: health_sys_id, health_sys_name,
    health_sys_city, health_sys_state, hosp_count, phys_grp_count.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Normalize expected columns
    col_map = {}
    id_col = _find_column(df, ["health_sys_id", "sys_id", "system_id", "id"])
    name_col = _find_column(df, ["health_sys_name", "sys_name", "system_name", "name"])
    city_col = _find_column(df, ["health_sys_city", "sys_city", "city"])
    state_col = _find_column(df, ["health_sys_state", "sys_state", "state"])
    hosp_col = _find_column(df, ["hosp_count", "hospital_count", "num_hospitals", "n_hosp"])
    phys_col = _find_column(df, ["phys_grp_count", "physician_group_count", "n_phys_grp"])

    if id_col:
        col_map[id_col] = "health_sys_id"
    if name_col:
        col_map[name_col] = "health_sys_name"
    if city_col:
        col_map[city_col] = "health_sys_city"
    if state_col:
        col_map[state_col] = "health_sys_state"
    if hosp_col:
        col_map[hosp_col] = "hosp_count"
    if phys_col:
        col_map[phys_col] = "phys_grp_count"

    df = df.rename(columns=col_map)

    for int_col in ["hosp_count", "phys_grp_count"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)

    return df


def parse_ahrq_hospital_linkage(path: Path) -> pd.DataFrame:
    """Parse the AHRQ Compendium hospital linkage file.

    Returns DataFrame with columns: health_sys_id, ccn, hospital_name,
    hosp_city, hosp_state, hosp_zip, hos_beds, hos_dsch, ownership, teaching.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    col_map = {}
    ccn_col = _find_column(df, ["ccn", "medicare_provider_number", "provider_number", "prvdr_num"])
    if ccn_col:
        col_map[ccn_col] = "ccn"

    df = df.rename(columns=col_map)

    if "ccn" in df.columns:
        df["ccn"] = df["ccn"].astype(str).str.strip().str.zfill(6)

    for int_col in ["hos_beds", "hos_dsch"]:
        if int_col in df.columns:
            df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype(int)

    return df


async def _download_if_missing(url: str, cache_path: Path) -> Path:
    """Download a file if not cached. Returns cache path.

    Raises RuntimeError with instructions if AHRQ WAF blocks the download.
    """
    if cache_path.exists():
        logger.info("Using cached file: %s", cache_path)
        return cache_path

    logger.info("Downloading %s ...", url)
    async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
        resp = await client.get(url)

        # Detect WAF challenge (AHRQ returns 202 with empty body or HTML)
        if resp.status_code == 202 or (
            resp.status_code == 200
            and b"<!DOCTYPE" in resp.content[:200]
        ):
            raise RuntimeError(
                f"Download blocked by WAF for {url}. "
                f"Run 'python scripts/download_ahrq.py' to download via browser, "
                f"or manually download and place at {cache_path}"
            )

        resp.raise_for_status()
        cache_path.write_bytes(resp.content)

    logger.info("Saved to: %s (%d bytes)", cache_path, cache_path.stat().st_size)
    return cache_path


async def load_ahrq_systems(force: bool = False) -> pd.DataFrame:
    """Load the AHRQ Compendium system file."""
    global _ahrq_systems_df
    if not force and _ahrq_systems_df is not None:
        return _ahrq_systems_df

    path = await _download_if_missing(AHRQ_SYSTEM_URL, AHRQ_SYSTEM_CACHE)
    _ahrq_systems_df = parse_ahrq_system_file(path)
    return _ahrq_systems_df


async def load_ahrq_hospital_linkage(force: bool = False) -> pd.DataFrame:
    """Load the AHRQ Compendium hospital linkage file."""
    global _ahrq_hospitals_df
    if not force and _ahrq_hospitals_df is not None:
        return _ahrq_hospitals_df

    path = await _download_if_missing(AHRQ_HOSPITAL_LINKAGE_URL, AHRQ_HOSPITAL_LINKAGE_CACHE)
    _ahrq_hospitals_df = parse_ahrq_hospital_linkage(path)
    return _ahrq_hospitals_df


# ============================================================
# CMS POS File loader
# ============================================================

def parse_pos_file(path: Path) -> pd.DataFrame:
    """Parse the CMS Provider of Services file.

    Reads with dtype=str to avoid type issues, then converts numeric columns.
    Only reads the columns we need (~30 of 470+) for memory efficiency.
    """
    # Columns we care about — read all first time to discover names,
    # then filter. POS has 470+ columns, but pandas handles it fine with dtype=str.
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace", low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    return df


async def load_pos(force: bool = False) -> pd.DataFrame:
    """Load the CMS Provider of Services file."""
    global _pos_df
    if not force and _pos_df is not None:
        return _pos_df

    path = await _download_if_missing(POS_URL, POS_CACHE)
    _pos_df = parse_pos_file(path)
    return _pos_df


# ============================================================
# NPPES API
# ============================================================

async def search_nppes(
    organization_name: str | None = None,
    state: str | None = None,
    taxonomy_description: str | None = None,
    enumeration_type: str = "NPI-2",
    limit: int = 200,
) -> list[dict]:
    """Search the NPPES NPI Registry for organizations."""
    params: dict = {"version": "2.1", "limit": min(limit, 200)}
    if organization_name:
        params["organization_name"] = organization_name
    if state:
        params["state"] = state
    if taxonomy_description:
        params["taxonomy_description"] = taxonomy_description
    if enumeration_type:
        params["enumeration_type"] = enumeration_type

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(NPPES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_data_loaders.py -v
```

Expected: All 3 tests PASS.

**Step 5: Create the Playwright download script**

```python
# scripts/download_ahrq.py
"""One-time download of AHRQ Compendium files using Playwright.

AHRQ uses AWS WAF bot protection. This script uses a real browser
to bypass the WAF and download the CSV files.

Usage:
    pip install playwright
    playwright install chromium
    python scripts/download_ahrq.py
"""

import asyncio
import sys
from pathlib import Path

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DOWNLOADS = [
    (
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv",
        CACHE_DIR / "ahrq_system_2023.csv",
    ),
    (
        "https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv",
        CACHE_DIR / "ahrq_hospital_linkage_2023.csv",
    ),
]


async def download_with_playwright():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Install playwright: pip install playwright && playwright install chromium")
        sys.exit(1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for url, dest in DOWNLOADS:
            if dest.exists():
                print(f"Already cached: {dest}")
                continue

            print(f"Downloading: {url}")
            # Navigate to trigger WAF challenge resolution
            resp = await page.goto(url, wait_until="networkidle", timeout=60000)
            if resp and resp.ok:
                body = await resp.body()
                dest.write_bytes(body)
                print(f"Saved: {dest} ({len(body)} bytes)")
            else:
                print(f"FAILED: {url} — status {resp.status if resp else 'None'}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(download_with_playwright())
```

**Step 6: Commit**

```bash
git add servers/health-system-profiler/data_loaders.py tests/servers/health_system_profiler/test_data_loaders.py scripts/download_ahrq.py
git commit -m "feat(health-system-profiler): add data loaders for AHRQ, POS, NPPES"
```

---

## Task 4: System Discovery Engine

**Files:**
- Create: `servers/health-system-profiler/system_discovery.py`
- Create: `tests/servers/health_system_profiler/test_system_discovery.py`

**Step 1: Write the failing test**

```python
# tests/servers/health_system_profiler/test_system_discovery.py
"""Tests for AHRQ-based system discovery."""

import pandas as pd
import pytest

from servers.health_system_profiler.system_discovery import (
    fuzzy_search_systems,
    resolve_system_ccns,
)


@pytest.fixture
def systems_df():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "health_sys_name": "Jefferson Health", "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 14},
        {"health_sys_id": "SYS_002", "health_sys_name": "Lehigh Valley Health Network", "health_sys_city": "Allentown", "health_sys_state": "PA", "hosp_count": 8},
        {"health_sys_id": "SYS_003", "health_sys_name": "Penn Medicine", "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 6},
        {"health_sys_id": "SYS_004", "health_sys_name": "Thomas Jefferson University Hospitals", "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 3},
    ])


@pytest.fixture
def hospitals_df():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "ccn": "390001", "hospital_name": "Thomas Jefferson University Hospital", "hos_beds": 900},
        {"health_sys_id": "SYS_001", "ccn": "390149", "hospital_name": "Jefferson Einstein Philadelphia", "hos_beds": 500},
        {"health_sys_id": "SYS_002", "ccn": "390133", "hospital_name": "Lehigh Valley Hospital-Cedar Crest", "hos_beds": 1190},
        {"health_sys_id": "SYS_002", "ccn": "390263", "hospital_name": "Lehigh Valley Hospital-Muhlenberg", "hos_beds": 184},
    ])


def test_fuzzy_search_exact(systems_df):
    results = fuzzy_search_systems("Jefferson Health", systems_df)
    assert len(results) >= 1
    assert results[0]["system_id"] == "SYS_001"


def test_fuzzy_search_partial(systems_df):
    results = fuzzy_search_systems("Jefferson", systems_df)
    # Should match both Jefferson entries
    assert len(results) >= 1
    names = [r["name"] for r in results]
    assert any("Jefferson" in n for n in names)


def test_fuzzy_search_case_insensitive(systems_df):
    results = fuzzy_search_systems("lehigh valley", systems_df)
    assert len(results) >= 1
    assert results[0]["system_id"] == "SYS_002"


def test_fuzzy_search_no_match(systems_df):
    results = fuzzy_search_systems("Mayo Clinic", systems_df)
    assert len(results) == 0


def test_resolve_system_ccns(hospitals_df):
    ccns = resolve_system_ccns("SYS_001", hospitals_df)
    assert set(ccns) == {"390001", "390149"}


def test_resolve_system_ccns_not_found(hospitals_df):
    ccns = resolve_system_ccns("SYS_999", hospitals_df)
    assert ccns == []
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/servers/health_system_profiler/test_system_discovery.py -v
```

Expected: FAIL with `ImportError`

**Step 3: Implement system_discovery.py**

```python
# servers/health-system-profiler/system_discovery.py
"""AHRQ Compendium-based health system discovery.

Fuzzy search against system names, resolve system_id → CCN list.
"""

import logging

import pandas as pd
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)


def fuzzy_search_systems(
    query: str,
    systems_df: pd.DataFrame,
    limit: int = 10,
    score_cutoff: float = 60.0,
) -> list[dict]:
    """Fuzzy search health system names from AHRQ Compendium.

    Uses rapidfuzz token_set_ratio for robust matching against abbreviations,
    partial names, and reordered tokens.

    Args:
        query: User's search string (e.g. "Jefferson Health", "LVHN").
        systems_df: AHRQ system file DataFrame.
        limit: Maximum results to return.
        score_cutoff: Minimum fuzzy match score (0-100).

    Returns:
        List of dicts with system_id, name, hq_city, hq_state, hospital_count.
    """
    if systems_df.empty or "health_sys_name" not in systems_df.columns:
        return []

    names = systems_df["health_sys_name"].tolist()
    matches = process.extract(
        query,
        names,
        scorer=fuzz.token_set_ratio,
        limit=limit,
        score_cutoff=score_cutoff,
    )

    results = []
    for name, score, idx in matches:
        row = systems_df.iloc[idx]
        beds_col = "total_beds" if "total_beds" in systems_df.columns else None
        results.append({
            "system_id": str(row.get("health_sys_id", "")),
            "name": str(row.get("health_sys_name", "")),
            "hq_city": str(row.get("health_sys_city", "")),
            "hq_state": str(row.get("health_sys_state", "")),
            "hospital_count": int(row.get("hosp_count", 0)),
            "match_score": round(score, 1),
        })

    return results


def resolve_system_ccns(system_id: str, hospitals_df: pd.DataFrame) -> list[str]:
    """Get all CCNs for a given AHRQ system_id.

    Args:
        system_id: AHRQ health_sys_id.
        hospitals_df: AHRQ hospital linkage DataFrame.

    Returns:
        List of 6-char CCN strings.
    """
    if hospitals_df.empty or "health_sys_id" not in hospitals_df.columns:
        return []

    matches = hospitals_df[hospitals_df["health_sys_id"] == system_id]
    if "ccn" not in matches.columns:
        return []

    return matches["ccn"].tolist()
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_system_discovery.py -v
```

Expected: All 6 tests PASS.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/system_discovery.py tests/servers/health_system_profiler/test_system_discovery.py
git commit -m "feat(health-system-profiler): add AHRQ fuzzy search system discovery"
```

---

## Task 5: Facility Enrichment Engine

**Files:**
- Create: `servers/health-system-profiler/facility_enrichment.py`
- Create: `tests/servers/health_system_profiler/test_facility_enrichment.py`

**Context:** POS column names are ALL_CAPS. Key columns:
- CCN: `PRVDR_NUM`
- Name: `FAC_NAME`
- Address: `ST_ADR`, `CITY_NAME`, `STATE_CD`, `ZIP_CD`, `COUNTY_NAME`
- Beds: `BED_CNT`, `CRTFD_BED_CNT`, `PSYCH_UNIT_BED_CNT`, `REHAB_UNIT_BED_CNT`, `HOSPC_BED_CNT`, `VNTLTR_BED_CNT`, `AIDS_BED_CNT`, `ALZHMR_BED_CNT`, `DLYS_BED_CNT`
- Services (Y/N flags): `CRDAC_CTHRTZTN_LAB_SW`, `OPN_HRT_SRGRY_SW`, `MRI_SRVC_SW`, `CT_SCNR_SW`, `PET_SCNR_SW`, `NUCLR_MED_SRVC_SW`, `TRMA_CTR_SW`, `TRMA_CTR_LVL_CD`, `BRNCTR_SW`, `NNTL_ICU_SW`, `OBSTTRCL_SRVC_SW`, `ORNG_TRNSP_SW`, `EMER_DEPT_SW`
- Staffing: `RN_CNT`, `LPN_CNT`, `MDCL_STAFF_PHYSCN_CNT`, `PHRMCST_CNT`, `THRPST_CNT`, `TOT_STFNG`
- Off-site: `TOT_OFSITE_EMER_DEPT_CNT`, `TOT_OFSITE_URGNT_CARE_CNTR_CNT`, `TOT_OFSITE_PSYCH_UNIT_CNT`, `TOT_OFSITE_REHAB_HOSP_CNT`
- Rooms: `OPRTN_RM_CNT`, `ENDSCPY_RM_CNT`, `CRDAC_CTHRTZTN_LAB_RM_CNT`
- Related: `RLTD_PRVDR_NMBR`
- Type: `PRVDR_CTGRY_CD`, `PRVDR_CTGRY_SBTYP_CD`

**Step 1: Write the failing test**

```python
# tests/servers/health_system_profiler/test_facility_enrichment.py
"""Tests for POS-based facility enrichment."""

import pandas as pd
import pytest

from servers.health_system_profiler.facility_enrichment import (
    enrich_facility,
    aggregate_off_site,
)
from servers.health_system_profiler.models import BedBreakdown, FacilitySummary, OffSiteSummary


@pytest.fixture
def sample_pos_df():
    """Minimal POS-like DataFrame with key columns."""
    return pd.DataFrame([
        {
            "PRVDR_NUM": "390001",
            "FAC_NAME": "Thomas Jefferson University Hospital",
            "ST_ADR": "111 S 11th St",
            "CITY_NAME": "Philadelphia",
            "STATE_CD": "PA",
            "ZIP_CD": "19107",
            "COUNTY_NAME": "Philadelphia",
            "PHNE_NUM": "2155556789",
            "BED_CNT": "900",
            "CRTFD_BED_CNT": "880",
            "PSYCH_UNIT_BED_CNT": "50",
            "REHAB_UNIT_BED_CNT": "30",
            "HOSPC_BED_CNT": "0",
            "VNTLTR_BED_CNT": "10",
            "AIDS_BED_CNT": "0",
            "ALZHMR_BED_CNT": "0",
            "DLYS_BED_CNT": "0",
            "CRDAC_CTHRTZTN_LAB_SW": "Y",
            "OPN_HRT_SRGRY_SW": "Y",
            "MRI_SRVC_SW": "Y",
            "CT_SCNR_SW": "Y",
            "PET_SCNR_SW": "N",
            "NUCLR_MED_SRVC_SW": "Y",
            "TRMA_CTR_SW": "Y",
            "TRMA_CTR_LVL_CD": "1",
            "BRNCTR_SW": "N",
            "NNTL_ICU_SW": "Y",
            "OBSTTRCL_SRVC_SW": "Y",
            "ORNG_TRNSP_SW": "N",
            "EMER_DEPT_SW": "Y",
            "RN_CNT": "2000",
            "LPN_CNT": "150",
            "MDCL_STAFF_PHYSCN_CNT": "500",
            "PHRMCST_CNT": "50",
            "THRPST_CNT": "100",
            "TOT_STFNG": "4500.5",
            "OPRTN_RM_CNT": "30",
            "ENDSCPY_RM_CNT": "8",
            "CRDAC_CTHRTZTN_LAB_RM_CNT": "4",
            "TOT_OFSITE_EMER_DEPT_CNT": "2",
            "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "5",
            "TOT_OFSITE_PSYCH_UNIT_CNT": "1",
            "TOT_OFSITE_REHAB_HOSP_CNT": "1",
            "RLTD_PRVDR_NMBR": "",
            "PRVDR_CTGRY_CD": "01",
            "PRVDR_CTGRY_SBTYP_CD": "01",
            "GNRL_CNTL_TYPE_CD": "04",
        },
    ])


def test_enrich_facility(sample_pos_df):
    facility = enrich_facility("390001", sample_pos_df)
    assert facility is not None
    assert facility.ccn == "390001"
    assert facility.name == "Thomas Jefferson University Hospital"
    assert facility.city == "Philadelphia"
    assert facility.state == "PA"
    # Beds
    assert facility.beds.total == 900
    assert facility.beds.certified == 880
    assert facility.beds.psychiatric == 50
    assert facility.beds.rehabilitation == 30
    # Services
    assert facility.services.cardiac_catheterization is True
    assert facility.services.open_heart_surgery is True
    assert facility.services.pet_scanner is False
    assert facility.services.trauma_center is True
    assert facility.services.trauma_level == "1"
    assert facility.services.emergency_department is True
    assert facility.services.operating_rooms == 30
    # Staffing
    assert facility.staffing.rn == 2000
    assert facility.staffing.physicians == 500
    assert facility.staffing.total_fte == 4500.5


def test_enrich_facility_not_found(sample_pos_df):
    facility = enrich_facility("999999", sample_pos_df)
    assert facility is None


def test_aggregate_off_site(sample_pos_df):
    summary = aggregate_off_site(["390001"], sample_pos_df)
    assert summary.emergency_departments == 2
    assert summary.urgent_care_centers == 5
    assert summary.psychiatric_units == 1
    assert summary.rehabilitation_hospitals == 1
    assert summary.total_off_site == 9
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/servers/health_system_profiler/test_facility_enrichment.py -v
```

Expected: FAIL with `ImportError`

**Step 3: Implement facility_enrichment.py**

```python
# servers/health-system-profiler/facility_enrichment.py
"""POS-based facility enrichment — beds, services, staffing, off-site counts."""

import logging

import pandas as pd

from .models import (
    BedBreakdown,
    FacilitySummary,
    OffSiteSummary,
    ServiceCapabilities,
    StaffingCounts,
)

logger = logging.getLogger(__name__)

# POS column name candidates (in priority order)
_CCN_COLS = ["PRVDR_NUM", "PROVIDER_NUMBER", "CCN"]
_NAME_COLS = ["FAC_NAME", "FACILITY_NAME", "PRVDR_NAME"]
_ADDR_COLS = ["ST_ADR", "STREET_ADDRESS", "ADDRESS"]
_CITY_COLS = ["CITY_NAME", "CITY"]
_STATE_COLS = ["STATE_CD", "STATE"]
_ZIP_COLS = ["ZIP_CD", "ZIP_CODE", "ZIP"]
_COUNTY_COLS = ["COUNTY_NAME", "COUNTY"]
_PHONE_COLS = ["PHNE_NUM", "PHONE_NUMBER", "PHONE"]


def _find(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_int(row, col: str, default: int = 0) -> int:
    if col not in row.index:
        return default
    try:
        return int(float(str(row[col]).strip() or "0"))
    except (ValueError, TypeError):
        return default


def _safe_float(row, col: str, default: float = 0.0) -> float:
    if col not in row.index:
        return default
    try:
        return float(str(row[col]).strip() or "0")
    except (ValueError, TypeError):
        return default


def _is_yes(row, col: str) -> bool:
    if col not in row.index:
        return False
    return str(row[col]).strip().upper() in ("Y", "YES", "1", "TRUE")


def enrich_facility(ccn: str, pos_df: pd.DataFrame) -> FacilitySummary | None:
    """Look up a CCN in the POS DataFrame and return an enriched FacilitySummary.

    Returns None if CCN not found.
    """
    ccn_col = _find(pos_df, _CCN_COLS)
    if not ccn_col:
        logger.warning("Cannot find CCN column in POS data")
        return None

    matches = pos_df[pos_df[ccn_col].astype(str).str.strip().str.zfill(6) == ccn.strip().zfill(6)]
    if matches.empty:
        return None

    row = matches.iloc[0]

    name_col = _find(pos_df, _NAME_COLS) or ""
    addr_col = _find(pos_df, _ADDR_COLS) or ""
    city_col = _find(pos_df, _CITY_COLS) or ""
    state_col = _find(pos_df, _STATE_COLS) or ""
    zip_col = _find(pos_df, _ZIP_COLS) or ""
    county_col = _find(pos_df, _COUNTY_COLS) or ""
    phone_col = _find(pos_df, _PHONE_COLS) or ""

    beds = BedBreakdown(
        total=_safe_int(row, "BED_CNT"),
        certified=_safe_int(row, "CRTFD_BED_CNT"),
        psychiatric=_safe_int(row, "PSYCH_UNIT_BED_CNT"),
        rehabilitation=_safe_int(row, "REHAB_UNIT_BED_CNT"),
        hospice=_safe_int(row, "HOSPC_BED_CNT"),
        ventilator=_safe_int(row, "VNTLTR_BED_CNT"),
        aids=_safe_int(row, "AIDS_BED_CNT"),
        alzheimer=_safe_int(row, "ALZHMR_BED_CNT"),
        dialysis=_safe_int(row, "DLYS_BED_CNT"),
    )

    services = ServiceCapabilities(
        cardiac_catheterization=_is_yes(row, "CRDAC_CTHRTZTN_LAB_SW"),
        open_heart_surgery=_is_yes(row, "OPN_HRT_SRGRY_SW"),
        mri=_is_yes(row, "MRI_SRVC_SW"),
        ct_scanner=_is_yes(row, "CT_SCNR_SW"),
        pet_scanner=_is_yes(row, "PET_SCNR_SW"),
        nuclear_medicine=_is_yes(row, "NUCLR_MED_SRVC_SW"),
        trauma_center=_is_yes(row, "TRMA_CTR_SW"),
        trauma_level=str(row.get("TRMA_CTR_LVL_CD", "") or "").strip(),
        burn_care=_is_yes(row, "BRNCTR_SW"),
        neonatal_icu=_is_yes(row, "NNTL_ICU_SW"),
        obstetrics=_is_yes(row, "OBSTTRCL_SRVC_SW"),
        transplant=_is_yes(row, "ORNG_TRNSP_SW"),
        emergency_department=_is_yes(row, "EMER_DEPT_SW"),
        operating_rooms=_safe_int(row, "OPRTN_RM_CNT"),
        endoscopy_rooms=_safe_int(row, "ENDSCPY_RM_CNT"),
        cardiac_cath_rooms=_safe_int(row, "CRDAC_CTHRTZTN_LAB_RM_CNT"),
    )

    staffing = StaffingCounts(
        rn=_safe_int(row, "RN_CNT"),
        lpn=_safe_int(row, "LPN_CNT"),
        physicians=_safe_int(row, "MDCL_STAFF_PHYSCN_CNT"),
        pharmacists=_safe_int(row, "PHRMCST_CNT"),
        therapists=_safe_int(row, "THRPST_CNT"),
        total_fte=_safe_float(row, "TOT_STFNG"),
    )

    return FacilitySummary(
        ccn=ccn,
        name=str(row.get(name_col, "") or "").strip() if name_col else "",
        address=str(row.get(addr_col, "") or "").strip() if addr_col else "",
        city=str(row.get(city_col, "") or "").strip() if city_col else "",
        state=str(row.get(state_col, "") or "").strip() if state_col else "",
        zip_code=str(row.get(zip_col, "") or "").strip() if zip_col else "",
        county=str(row.get(county_col, "") or "").strip() if county_col else "",
        phone=str(row.get(phone_col, "") or "").strip() if phone_col else "",
        beds=beds,
        services=services,
        staffing=staffing,
    )


def aggregate_off_site(ccns: list[str], pos_df: pd.DataFrame) -> OffSiteSummary:
    """Aggregate off-site location counts across all system CCNs."""
    ccn_col = _find(pos_df, _CCN_COLS)
    if not ccn_col:
        return OffSiteSummary()

    ccn_set = {c.strip().zfill(6) for c in ccns}
    matches = pos_df[pos_df[ccn_col].astype(str).str.strip().str.zfill(6).isin(ccn_set)]

    ed = 0
    uc = 0
    psych = 0
    rehab = 0

    for _, row in matches.iterrows():
        ed += _safe_int(row, "TOT_OFSITE_EMER_DEPT_CNT")
        uc += _safe_int(row, "TOT_OFSITE_URGNT_CARE_CNTR_CNT")
        psych += _safe_int(row, "TOT_OFSITE_PSYCH_UNIT_CNT")
        rehab += _safe_int(row, "TOT_OFSITE_REHAB_HOSP_CNT")

    return OffSiteSummary(
        emergency_departments=ed,
        urgent_care_centers=uc,
        psychiatric_units=psych,
        rehabilitation_hospitals=rehab,
        total_off_site=ed + uc + psych + rehab,
    )
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_facility_enrichment.py -v
```

Expected: All 3 tests PASS.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/facility_enrichment.py tests/servers/health_system_profiler/test_facility_enrichment.py
git commit -m "feat(health-system-profiler): add POS-based facility enrichment"
```

---

## Task 6: Graph Expansion (Related Providers)

**Files:**
- Create: `servers/health-system-profiler/graph_expansion.py`
- Create: `tests/servers/health_system_profiler/test_graph_expansion.py`

**Context:** POS `RLTD_PRVDR_NMBR` links sub-entities (dialysis centers, rehab hospitals, psychiatric facilities) back to a parent hospital CCN. We walk this graph to discover all related providers not in the AHRQ linkage.

**Step 1: Write the failing test**

```python
# tests/servers/health_system_profiler/test_graph_expansion.py
"""Tests for RELATED_PROVIDER_NUMBER graph expansion."""

import pandas as pd
import pytest

from servers.health_system_profiler.graph_expansion import expand_related_providers
from servers.health_system_profiler.models import SubEntity


@pytest.fixture
def pos_with_related():
    """POS data with related provider linkages."""
    return pd.DataFrame([
        # Parent hospital
        {"PRVDR_NUM": "390001", "FAC_NAME": "Jefferson Main", "RLTD_PRVDR_NMBR": "",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "BED_CNT": "900",
         "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01"},
        # Related dialysis center
        {"PRVDR_NUM": "392001", "FAC_NAME": "Jefferson Dialysis", "RLTD_PRVDR_NMBR": "390001",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "BED_CNT": "0",
         "PRVDR_CTGRY_CD": "11", "PRVDR_CTGRY_SBTYP_CD": ""},
        # Related rehab facility
        {"PRVDR_NUM": "393001", "FAC_NAME": "Jefferson Rehab", "RLTD_PRVDR_NMBR": "390001",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "BED_CNT": "40",
         "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "02"},
        # Unrelated facility
        {"PRVDR_NUM": "390500", "FAC_NAME": "Some Other Hospital", "RLTD_PRVDR_NMBR": "",
         "CITY_NAME": "Allentown", "STATE_CD": "PA", "BED_CNT": "200",
         "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01"},
    ])


def test_expand_related_providers(pos_with_related):
    subs = expand_related_providers(["390001"], pos_with_related)
    assert len(subs) == 2
    ccns = {s.ccn for s in subs}
    assert "392001" in ccns
    assert "393001" in ccns
    assert "390500" not in ccns  # unrelated


def test_expand_no_related(pos_with_related):
    subs = expand_related_providers(["390500"], pos_with_related)
    assert len(subs) == 0


def test_sub_entity_fields(pos_with_related):
    subs = expand_related_providers(["390001"], pos_with_related)
    dialysis = next(s for s in subs if s.ccn == "392001")
    assert dialysis.name == "Jefferson Dialysis"
    assert dialysis.parent_ccn == "390001"
    assert dialysis.city == "Philadelphia"
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/servers/health_system_profiler/test_graph_expansion.py -v
```

Expected: FAIL with `ImportError`

**Step 3: Implement graph_expansion.py**

```python
# servers/health-system-profiler/graph_expansion.py
"""Graph expansion via RELATED_PROVIDER_NUMBER in POS file.

Discovers sub-entities (dialysis, rehab, psychiatric, etc.) linked to
parent hospital CCNs.
"""

import logging

import pandas as pd

from .models import SubEntity

logger = logging.getLogger(__name__)

# POS provider category codes
CATEGORY_LABELS = {
    "01": "Hospital",
    "02": "Skilled Nursing Facility",
    "03": "Home Health Agency",
    "04": "Religious Nonmedical Health Care Institution",
    "05": "Federally Qualified Health Center",
    "06": "End-Stage Renal Disease Facility",
    "07": "Rural Health Clinic",
    "08": "Ambulatory Surgical Center",
    "09": "Hospice",
    "10": "Organ Procurement Organization",
    "11": "Renal Dialysis Facility",
    "12": "Outpatient Physical Therapy",
    "13": "Community Mental Health Center",
    "14": "Portable X-Ray Supplier",
    "15": "Comprehensive Outpatient Rehabilitation Facility",
}

_RLTD_COLS = ["RLTD_PRVDR_NMBR", "RELATED_PROVIDER_NUMBER", "RLTD_PRVDR_NUM"]
_CCN_COLS = ["PRVDR_NUM", "PROVIDER_NUMBER", "CCN"]


def _find(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_int(row, col: str, default: int = 0) -> int:
    if col not in row.index:
        return default
    try:
        return int(float(str(row[col]).strip() or "0"))
    except (ValueError, TypeError):
        return default


def expand_related_providers(
    parent_ccns: list[str],
    pos_df: pd.DataFrame,
) -> list[SubEntity]:
    """Find all POS rows whose RELATED_PROVIDER_NUMBER points to one of parent_ccns.

    Walks one level of the graph (direct children only — sufficient for
    hospital→sub-entity relationships).

    Args:
        parent_ccns: List of parent hospital CCNs.
        pos_df: Full POS DataFrame.

    Returns:
        List of SubEntity models.
    """
    rltd_col = _find(pos_df, _RLTD_COLS)
    ccn_col = _find(pos_df, _CCN_COLS)

    if not rltd_col or not ccn_col:
        return []

    parent_set = {c.strip().zfill(6) for c in parent_ccns}

    # Find rows whose related provider is one of our parents
    related_values = pos_df[rltd_col].astype(str).str.strip().str.zfill(6)
    mask = related_values.isin(parent_set)

    # Exclude rows that ARE parent CCNs (self-references)
    own_ccns = pos_df[ccn_col].astype(str).str.strip().str.zfill(6)
    mask = mask & ~own_ccns.isin(parent_set)

    children = pos_df[mask]
    results = []

    for _, row in children.iterrows():
        ccn = str(row.get(ccn_col, "")).strip().zfill(6)
        parent_ccn = str(row.get(rltd_col, "")).strip().zfill(6)
        category_code = str(row.get("PRVDR_CTGRY_CD", "")).strip()
        facility_type = CATEGORY_LABELS.get(category_code, f"Category {category_code}")

        results.append(SubEntity(
            ccn=ccn,
            name=str(row.get("FAC_NAME", row.get("FACILITY_NAME", ""))).strip(),
            parent_ccn=parent_ccn,
            facility_type=facility_type,
            city=str(row.get("CITY_NAME", row.get("CITY", ""))).strip(),
            state=str(row.get("STATE_CD", row.get("STATE", ""))).strip(),
            beds=_safe_int(row, "BED_CNT"),
        ))

    return results
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_graph_expansion.py -v
```

Expected: All 3 tests PASS.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/graph_expansion.py tests/servers/health_system_profiler/test_graph_expansion.py
git commit -m "feat(health-system-profiler): add related provider graph expansion"
```

---

## Task 7: Outpatient Discovery (NPPES)

**Files:**
- Create: `servers/health-system-profiler/outpatient_discovery.py`
- Create: `tests/servers/health_system_profiler/test_outpatient_discovery.py`

**Context:** Uses NPPES wildcard search with system name patterns to find outpatient sites, then categorizes them by healthcare taxonomy code. Common NPPES taxonomy prefixes:
- `207Q` = Family Medicine
- `207R` = Internal Medicine
- `261Q` = Clinic/Center subtypes
- `208` = various specialties (cardiology, etc.)
- `332` = Pharmacy
- `225` = Physical Therapy / Rehab
- `363` = Nurse Practitioner

**Step 1: Write the failing test**

```python
# tests/servers/health_system_profiler/test_outpatient_discovery.py
"""Tests for NPPES-based outpatient site discovery."""

import pytest

from servers.health_system_profiler.outpatient_discovery import (
    categorize_taxonomy,
    build_search_patterns,
    parse_nppes_results,
)
from servers.health_system_profiler.models import OutpatientSite


def test_categorize_taxonomy():
    assert categorize_taxonomy("207Q00000X") == "Family Medicine"
    assert categorize_taxonomy("261QP2300X") == "Clinic/Center"
    assert categorize_taxonomy("332B00000X") == "Pharmacy"
    assert categorize_taxonomy("225100000X") == "Rehabilitation"
    assert categorize_taxonomy("999Z00000X") == "Other"


def test_build_search_patterns():
    patterns = build_search_patterns("Jefferson Health", "PA")
    assert len(patterns) >= 1
    # Should include the name itself
    assert any("Jefferson" in p["organization_name"] for p in patterns)
    assert all(p.get("state") == "PA" for p in patterns)


def test_build_search_patterns_multi_word():
    patterns = build_search_patterns("Lehigh Valley Health Network", "PA")
    assert len(patterns) >= 1
    assert any("Lehigh Valley" in p["organization_name"] for p in patterns)


def test_parse_nppes_results():
    raw = [
        {
            "number": "1234567890",
            "enumeration_type": "NPI-2",
            "basic": {
                "organization_name": "Jefferson Family Medicine",
                "status": "A",
            },
            "addresses": [
                {
                    "address_purpose": "LOCATION",
                    "address_1": "123 Main St",
                    "city": "Philadelphia",
                    "state": "PA",
                    "postal_code": "191070000",
                    "telephone_number": "215-555-1234",
                }
            ],
            "taxonomies": [
                {
                    "code": "207Q00000X",
                    "desc": "Family Medicine",
                    "primary": True,
                }
            ],
        }
    ]
    sites = parse_nppes_results(raw)
    assert len(sites) == 1
    assert sites[0].npi == "1234567890"
    assert sites[0].name == "Jefferson Family Medicine"
    assert sites[0].city == "Philadelphia"
    assert sites[0].taxonomy_code == "207Q00000X"
    assert sites[0].category == "Family Medicine"
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/servers/health_system_profiler/test_outpatient_discovery.py -v
```

Expected: FAIL with `ImportError`

**Step 3: Implement outpatient_discovery.py**

```python
# servers/health-system-profiler/outpatient_discovery.py
"""NPPES-based outpatient site discovery and taxonomy categorization."""

import logging

from .models import OutpatientSite

logger = logging.getLogger(__name__)

# Taxonomy code prefix → human-readable category
TAXONOMY_CATEGORIES = {
    "207Q": "Family Medicine",
    "207R": "Internal Medicine",
    "207X": "Orthopedic Surgery",
    "207Y": "Ophthalmology",
    "2084": "Psychiatry",
    "2085": "Radiology",
    "2086": "Surgery",
    "208C": "Cardiology",
    "208D": "Dermatology",
    "208G": "Gastroenterology",
    "208M": "Nephrology",
    "208U": "Neurology",
    "261Q": "Clinic/Center",
    "225": "Rehabilitation",
    "332B": "Pharmacy",
    "332": "Pharmacy",
    "363": "Nurse Practitioner",
    "367": "Physician Assistant",
    "174": "Dentist",
    "122": "Optometrist",
    "111": "Chiropractor",
    "133": "Psychologist",
    "341": "Home Health",
    "281": "Hospital",
    "282": "Hospital",
    "283": "Hospital",
    "291": "Laboratory",
    "302": "Nursing Facility",
    "311": "Hospice",
    "314": "Skilled Nursing",
    "324": "Behavioral Health",
}


def categorize_taxonomy(code: str) -> str:
    """Map a taxonomy code to a human-readable category.

    Checks longest prefix first (4 chars) down to 3 chars.
    """
    code = str(code).strip()
    # Try 4-char prefix first, then 3-char
    for length in (4, 3):
        prefix = code[:length]
        if prefix in TAXONOMY_CATEGORIES:
            return TAXONOMY_CATEGORIES[prefix]
    return "Other"


def build_search_patterns(system_name: str, state: str) -> list[dict]:
    """Generate NPPES search patterns from a system name.

    Creates wildcard-friendly search parameters. NPPES API supports
    partial name matching with trailing wildcard behavior.

    Args:
        system_name: Health system name (e.g. "Jefferson Health").
        state: Two-letter state code.

    Returns:
        List of param dicts for NPPES queries.
    """
    patterns = []
    # Remove common suffixes to get the distinctive name portion
    name = system_name.strip()
    for suffix in ["Health System", "Health Network", "Health", "Medical Center", "Medicine"]:
        if name.lower().endswith(suffix.lower()):
            name = name[: -len(suffix)].strip()
            break

    # Primary pattern: the distinctive name part + wildcard
    if name:
        patterns.append({
            "organization_name": f"{name}*",
            "state": state,
            "enumeration_type": "NPI-2",
        })

    # Full system name pattern
    patterns.append({
        "organization_name": f"{system_name.strip()}*",
        "state": state,
        "enumeration_type": "NPI-2",
    })

    return patterns


def parse_nppes_results(raw_results: list[dict]) -> list[OutpatientSite]:
    """Parse NPPES API results into OutpatientSite models.

    Extracts location address, primary taxonomy, and categorizes.
    """
    sites = []
    for r in raw_results:
        basic = r.get("basic", {})
        if basic.get("status", "").upper() != "A":
            continue  # skip deactivated NPIs

        name = basic.get("organization_name", "")
        npi = str(r.get("number", ""))

        # Get location address (not mailing)
        address = ""
        city = ""
        state = ""
        zip_code = ""
        phone = ""
        for addr in r.get("addresses", []):
            if addr.get("address_purpose", "").upper() == "LOCATION":
                address = addr.get("address_1", "")
                city = addr.get("city", "")
                state = addr.get("state", "")
                raw_zip = addr.get("postal_code", "")
                zip_code = raw_zip[:5] if len(raw_zip) >= 5 else raw_zip
                phone = addr.get("telephone_number", "")
                break

        # Get primary taxonomy
        taxonomy_code = ""
        taxonomy_desc = ""
        for tax in r.get("taxonomies", []):
            if tax.get("primary", False):
                taxonomy_code = tax.get("code", "")
                taxonomy_desc = tax.get("desc", "")
                break
        if not taxonomy_code and r.get("taxonomies"):
            taxonomy_code = r["taxonomies"][0].get("code", "")
            taxonomy_desc = r["taxonomies"][0].get("desc", "")

        category = categorize_taxonomy(taxonomy_code)

        sites.append(OutpatientSite(
            npi=npi,
            name=name,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            phone=phone,
            taxonomy_code=taxonomy_code,
            taxonomy_description=taxonomy_desc,
            category=category,
        ))

    return sites
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_outpatient_discovery.py -v
```

Expected: All 4 tests PASS.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/outpatient_discovery.py tests/servers/health_system_profiler/test_outpatient_discovery.py
git commit -m "feat(health-system-profiler): add NPPES outpatient discovery"
```

---

## Task 8: MCP Server — Three Tools

**Files:**
- Modify: `servers/health-system-profiler/server.py`
- Create: `tests/servers/health_system_profiler/test_server.py`

**Context:** The server wires together all modules into 3 MCP tools:
1. `search_health_systems(query)` — AHRQ fuzzy search
2. `get_system_profile(system_id | system_name)` — full profile
3. `get_system_facilities(system_id, facility_type)` — detailed facility list

**Step 1: Write the failing test**

```python
# tests/servers/health_system_profiler/test_server.py
"""Tests for the health-system-profiler MCP server tools.

Uses monkeypatching to avoid real data downloads.
"""

import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
import pytest_asyncio

from servers.health_system_profiler import server


@pytest.fixture
def mock_ahrq_systems():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "health_sys_name": "Jefferson Health",
         "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 2, "phys_grp_count": 10},
    ])


@pytest.fixture
def mock_ahrq_hospitals():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "ccn": "390001", "hospital_name": "Jefferson Main",
         "hosp_city": "Philadelphia", "hosp_state": "PA", "hosp_zip": "19107", "hos_beds": 900, "hos_dsch": 40000},
        {"health_sys_id": "SYS_001", "ccn": "390149", "hospital_name": "Jefferson Einstein",
         "hosp_city": "Philadelphia", "hosp_state": "PA", "hosp_zip": "19141", "hos_beds": 500, "hos_dsch": 20000},
    ])


@pytest.fixture
def mock_pos():
    return pd.DataFrame([
        {"PRVDR_NUM": "390001", "FAC_NAME": "Jefferson Main", "ST_ADR": "111 S 11th St",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "ZIP_CD": "19107", "COUNTY_NAME": "Philadelphia",
         "PHNE_NUM": "2155551234", "BED_CNT": "900", "CRTFD_BED_CNT": "880",
         "PSYCH_UNIT_BED_CNT": "50", "REHAB_UNIT_BED_CNT": "30", "HOSPC_BED_CNT": "0",
         "VNTLTR_BED_CNT": "10", "AIDS_BED_CNT": "0", "ALZHMR_BED_CNT": "0", "DLYS_BED_CNT": "0",
         "CRDAC_CTHRTZTN_LAB_SW": "Y", "OPN_HRT_SRGRY_SW": "Y", "MRI_SRVC_SW": "Y",
         "CT_SCNR_SW": "Y", "PET_SCNR_SW": "N", "NUCLR_MED_SRVC_SW": "Y",
         "TRMA_CTR_SW": "Y", "TRMA_CTR_LVL_CD": "1", "BRNCTR_SW": "N", "NNTL_ICU_SW": "Y",
         "OBSTTRCL_SRVC_SW": "Y", "ORNG_TRNSP_SW": "N", "EMER_DEPT_SW": "Y",
         "RN_CNT": "2000", "LPN_CNT": "150", "MDCL_STAFF_PHYSCN_CNT": "500",
         "PHRMCST_CNT": "50", "THRPST_CNT": "100", "TOT_STFNG": "4500",
         "OPRTN_RM_CNT": "30", "ENDSCPY_RM_CNT": "8", "CRDAC_CTHRTZTN_LAB_RM_CNT": "4",
         "TOT_OFSITE_EMER_DEPT_CNT": "2", "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "5",
         "TOT_OFSITE_PSYCH_UNIT_CNT": "1", "TOT_OFSITE_REHAB_HOSP_CNT": "1",
         "RLTD_PRVDR_NMBR": "", "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01",
         "GNRL_CNTL_TYPE_CD": "04"},
        {"PRVDR_NUM": "390149", "FAC_NAME": "Jefferson Einstein", "ST_ADR": "5501 Old York Rd",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "ZIP_CD": "19141", "COUNTY_NAME": "Philadelphia",
         "PHNE_NUM": "2155555678", "BED_CNT": "500", "CRTFD_BED_CNT": "490",
         "PSYCH_UNIT_BED_CNT": "20", "REHAB_UNIT_BED_CNT": "10", "HOSPC_BED_CNT": "0",
         "VNTLTR_BED_CNT": "5", "AIDS_BED_CNT": "0", "ALZHMR_BED_CNT": "0", "DLYS_BED_CNT": "0",
         "CRDAC_CTHRTZTN_LAB_SW": "Y", "OPN_HRT_SRGRY_SW": "N", "MRI_SRVC_SW": "Y",
         "CT_SCNR_SW": "Y", "PET_SCNR_SW": "N", "NUCLR_MED_SRVC_SW": "N",
         "TRMA_CTR_SW": "N", "TRMA_CTR_LVL_CD": "", "BRNCTR_SW": "N", "NNTL_ICU_SW": "N",
         "OBSTTRCL_SRVC_SW": "Y", "ORNG_TRNSP_SW": "N", "EMER_DEPT_SW": "Y",
         "RN_CNT": "1000", "LPN_CNT": "80", "MDCL_STAFF_PHYSCN_CNT": "200",
         "PHRMCST_CNT": "25", "THRPST_CNT": "50", "TOT_STFNG": "2200",
         "OPRTN_RM_CNT": "15", "ENDSCPY_RM_CNT": "4", "CRDAC_CTHRTZTN_LAB_RM_CNT": "2",
         "TOT_OFSITE_EMER_DEPT_CNT": "1", "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "3",
         "TOT_OFSITE_PSYCH_UNIT_CNT": "0", "TOT_OFSITE_REHAB_HOSP_CNT": "0",
         "RLTD_PRVDR_NMBR": "", "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01",
         "GNRL_CNTL_TYPE_CD": "04"},
    ])


@pytest.mark.asyncio
async def test_search_health_systems(mock_ahrq_systems):
    with patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems):
        result = json.loads(await server.search_health_systems("Jefferson"))
    assert "results" in result
    assert len(result["results"]) >= 1
    assert result["results"][0]["name"] == "Jefferson Health"


@pytest.mark.asyncio
async def test_get_system_profile(mock_ahrq_systems, mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
        patch.object(server, "_search_nppes", new_callable=AsyncMock, return_value=[]),
    ):
        result = json.loads(await server.get_system_profile(system_name="Jefferson Health"))
    assert result["system"]["name"] == "Jefferson Health"
    assert result["system"]["hospital_count"] == 2
    assert len(result["inpatient_facilities"]) == 2
    # Check enrichment worked
    main = next(f for f in result["inpatient_facilities"] if f["ccn"] == "390001")
    assert main["beds"]["total"] == 900
    assert main["services"]["cardiac_catheterization"] is True
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/servers/health_system_profiler/test_server.py -v
```

Expected: FAIL (server.py doesn't have the full implementation)

**Step 3: Implement the full server.py**

Replace `servers/health-system-profiler/server.py` with:

```python
# servers/health-system-profiler/server.py
"""Health System Profiler MCP Server.

Returns complete health system profiles in 1-3 tool calls by combining
AHRQ Compendium, CMS Provider of Services, NPPES, and HSAF data.
"""

import json
import logging
import os as _os
import sys
from pathlib import Path

import pandas as pd
from mcp.server.fastmcp import FastMCP

# Support running both as a package and as a standalone script
try:
    from .data_loaders import (
        load_ahrq_hospital_linkage,
        load_ahrq_systems,
        load_pos,
        search_nppes,
    )
    from .facility_enrichment import aggregate_off_site, enrich_facility
    from .graph_expansion import expand_related_providers
    from .models import (
        FacilitySummary,
        HealthSystemSummary,
        OffSiteSummary,
        SystemProfileResponse,
        SystemSearchResult,
    )
    from .outpatient_discovery import build_search_patterns, parse_nppes_results
    from .system_discovery import fuzzy_search_systems, resolve_system_ccns
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from data_loaders import (
        load_ahrq_hospital_linkage,
        load_ahrq_systems,
        load_pos,
        search_nppes,
    )
    from facility_enrichment import aggregate_off_site, enrich_facility
    from graph_expansion import expand_related_providers
    from models import (
        FacilitySummary,
        HealthSystemSummary,
        OffSiteSummary,
        SystemProfileResponse,
        SystemSearchResult,
    )
    from outpatient_discovery import build_search_patterns, parse_nppes_results
    from system_discovery import fuzzy_search_systems, resolve_system_ccns

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "health-system-profiler"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8007"))
mcp = FastMCP(**_mcp_kwargs)


# ---- Internal loader wrappers (mockable for tests) ----

async def _load_ahrq_systems() -> pd.DataFrame:
    return await load_ahrq_systems()

async def _load_ahrq_hospitals() -> pd.DataFrame:
    return await load_ahrq_hospital_linkage()

async def _load_pos() -> pd.DataFrame:
    return await load_pos()

async def _search_nppes(**kwargs) -> list[dict]:
    return await search_nppes(**kwargs)


# ---- MCP Tools ----

@mcp.tool()
async def search_health_systems(query: str, limit: int = 10) -> str:
    """Search for health systems by name using AHRQ Compendium.

    Performs fuzzy matching against ~700 US health system names.

    Args:
        query: System name to search for (e.g. "Jefferson Health", "LVHN", "Penn Medicine").
        limit: Maximum results to return (default 10).
    """
    systems_df = await _load_ahrq_systems()
    results = fuzzy_search_systems(query, systems_df, limit=limit)
    return json.dumps({"count": len(results), "results": results})


@mcp.tool()
async def get_system_profile(
    system_id: str | None = None,
    system_name: str | None = None,
    include_outpatient: bool = True,
) -> str:
    """Get a complete health system profile in one call.

    Combines AHRQ Compendium (system→hospitals), CMS POS (beds, services,
    staffing), NPPES (outpatient sites), and related provider graph expansion.

    Provide either system_id (from search_health_systems) or system_name
    (auto-resolved via fuzzy search — takes the top match).

    Args:
        system_id: AHRQ system ID (e.g. "SYS_001"). Preferred.
        system_name: System name for auto-resolution (e.g. "Jefferson Health").
        include_outpatient: Include NPPES outpatient site discovery (default True).
    """
    systems_df = await _load_ahrq_systems()
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    # Resolve system_id if only name provided
    if not system_id and system_name:
        matches = fuzzy_search_systems(system_name, systems_df, limit=1)
        if not matches:
            return json.dumps({"error": f"No health system found matching '{system_name}'"})
        system_id = matches[0]["system_id"]

    if not system_id:
        return json.dumps({"error": "Provide either system_id or system_name"})

    # Get system info
    sys_row = systems_df[systems_df["health_sys_id"] == system_id]
    if sys_row.empty:
        return json.dumps({"error": f"System ID '{system_id}' not found in AHRQ Compendium"})

    sys_info = sys_row.iloc[0]
    sys_name = str(sys_info.get("health_sys_name", ""))
    sys_city = str(sys_info.get("health_sys_city", ""))
    sys_state = str(sys_info.get("health_sys_state", ""))

    # Resolve CCNs
    ccns = resolve_system_ccns(system_id, hospitals_df)

    # Enrich each facility from POS
    facilities: list[FacilitySummary] = []
    total_beds = 0
    for ccn in ccns:
        facility = enrich_facility(ccn, pos_df)
        if facility:
            total_beds += facility.beds.total
            facilities.append(facility)
        else:
            # Fallback: use AHRQ data if POS has no match
            ahrq_row = hospitals_df[hospitals_df["ccn"] == ccn]
            if not ahrq_row.empty:
                r = ahrq_row.iloc[0]
                from .models import BedBreakdown
                beds = int(r.get("hos_beds", 0) or 0)
                total_beds += beds
                facilities.append(FacilitySummary(
                    ccn=ccn,
                    name=str(r.get("hospital_name", "")),
                    city=str(r.get("hosp_city", "")),
                    state=str(r.get("hosp_state", "")),
                    zip_code=str(r.get("hosp_zip", "")),
                    beds=BedBreakdown(total=beds),
                ))

    # Graph expansion — find sub-entities
    sub_entities = expand_related_providers(ccns, pos_df)

    # Aggregate off-site counts
    off_site = aggregate_off_site(ccns, pos_df)

    # NPPES outpatient discovery
    outpatient_sites = []
    if include_outpatient and sys_state:
        patterns = build_search_patterns(sys_name, sys_state)
        for params in patterns:
            try:
                raw = await _search_nppes(**params)
                outpatient_sites.extend(parse_nppes_results(raw))
            except Exception as e:
                logger.warning("NPPES search failed for %s: %s", params, e)

        # Deduplicate by NPI
        seen_npis = set()
        unique_sites = []
        for site in outpatient_sites:
            if site.npi not in seen_npis:
                seen_npis.add(site.npi)
                unique_sites.append(site)
        outpatient_sites = unique_sites

    # Compute total discharges from AHRQ linkage
    sys_hospitals = hospitals_df[hospitals_df["health_sys_id"] == system_id]
    total_dsch = int(sys_hospitals["hos_dsch"].sum()) if "hos_dsch" in sys_hospitals.columns else 0

    # Build response
    profile = SystemProfileResponse(
        system=HealthSystemSummary(
            system_id=system_id,
            name=sys_name,
            hq_city=sys_city,
            hq_state=sys_state,
            hospital_count=len(ccns),
            total_beds=total_beds,
            total_discharges=total_dsch,
            physician_group_count=int(sys_info.get("phys_grp_count", 0) or 0),
        ),
        inpatient_facilities=[f.model_dump() for f in facilities],
        sub_entities=[s.model_dump() for s in sub_entities],
        outpatient_sites=[o.model_dump() for o in outpatient_sites],
        off_site_summary=off_site.model_dump(),
    )
    return json.dumps(profile.model_dump(), indent=2)


@mcp.tool()
async def get_system_facilities(
    system_id: str,
    facility_type: str = "all",
) -> str:
    """Get detailed facility data for a health system with full POS enrichment.

    Args:
        system_id: AHRQ system ID (from search_health_systems).
        facility_type: Filter: "inpatient", "outpatient", "rehab", "behavioral_health", "all" (default).
    """
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    ccns = resolve_system_ccns(system_id, hospitals_df)
    if not ccns:
        return json.dumps({"error": f"No hospitals found for system ID '{system_id}'"})

    facilities = []
    for ccn in ccns:
        facility = enrich_facility(ccn, pos_df)
        if facility:
            facilities.append(facility)

    # Include sub-entities if not filtered to inpatient-only
    sub_entities = []
    if facility_type in ("all", "rehab", "behavioral_health"):
        sub_entities = expand_related_providers(ccns, pos_df)

    result = {
        "system_id": system_id,
        "facility_count": len(facilities) + len(sub_entities),
        "inpatient_facilities": [f.model_dump() for f in facilities],
    }
    if sub_entities:
        result["sub_entities"] = [s.model_dump() for s in sub_entities]

    return json.dumps(result, indent=2)


if __name__ == "__main__":
    mcp.run(transport=_transport)
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/servers/health_system_profiler/test_server.py -v
```

Expected: All tests PASS.

**Step 5: Commit**

```bash
git add servers/health-system-profiler/server.py tests/servers/health_system_profiler/test_server.py
git commit -m "feat(health-system-profiler): wire up 3 MCP tools in server"
```

---

## Task 9: Infrastructure — Docker Compose + .mcp.json

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.mcp.json`

**Step 1: Add service to docker-compose.yml**

Append the new service block before the `volumes:` section:

```yaml
  health-system-profiler:
    build: .
    command: python -m servers.health_system_profiler.server
    ports:
      - "8007:8007"
    environment:
      - MCP_TRANSPORT=streamable-http
      - MCP_PORT=8007
    volumes:
      - healthcare-cache:/root/.healthcare-data-mcp/cache
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8007),5); s.close()"]
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 30s
```

**Step 2: Add server to .mcp.json**

Add to the `mcpServers` object:

```json
"health-system-profiler": {
  "type": "http",
  "url": "http://localhost:8007/mcp"
}
```

**Step 3: Verify server starts locally**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp"
timeout 5 python -m servers.health_system_profiler.server || true
echo "Server startup check complete"
```

Expected: Server starts without import errors (may timeout waiting for stdio, which is fine).

**Step 4: Commit**

```bash
git add docker-compose.yml .mcp.json
git commit -m "feat(health-system-profiler): add docker-compose and .mcp.json registration"
```

---

## Task 10: Run Full Test Suite

**Step 1: Install rapidfuzz dependency**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp"
pip install rapidfuzz
```

**Step 2: Run all tests**

```bash
python -m pytest tests/servers/health_system_profiler/ -v --tb=short
```

Expected: All tests pass (models, data_loaders, system_discovery, facility_enrichment, graph_expansion, outpatient_discovery, server).

**Step 3: Run ruff linter**

```bash
ruff check servers/health-system-profiler/ tests/servers/health_system_profiler/
```

Expected: No lint errors.

**Step 4: Fix any issues found, then commit**

```bash
git add -A
git commit -m "chore(health-system-profiler): fix lint issues and finalize tests"
```

---

## Task 11: Download AHRQ Data and Smoke Test

**Step 1: Attempt AHRQ download via Playwright**

```bash
pip install playwright
playwright install chromium
python scripts/download_ahrq.py
```

Expected: AHRQ CSV files downloaded to `~/.healthcare-data-mcp/cache/ahrq_*.csv`.

If Playwright fails, manually download from a browser:
- `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-system-2023.csv` → save as `~/.healthcare-data-mcp/cache/ahrq_system_2023.csv`
- `https://www.ahrq.gov/sites/default/files/wysiwyg/chsp/compendium/chsp-hospital-linkage-2023.csv` → save as `~/.healthcare-data-mcp/cache/ahrq_hospital_linkage_2023.csv`

**Step 2: Smoke test — search for Jefferson Health**

```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp"
python -c "
import asyncio, json
from servers.health_system_profiler.server import search_health_systems
result = asyncio.run(search_health_systems('Jefferson Health'))
print(json.dumps(json.loads(result), indent=2))
"
```

Expected: JSON output with Jefferson Health system details.

**Step 3: Smoke test — full profile**

```bash
python -c "
import asyncio, json
from servers.health_system_profiler.server import get_system_profile
result = asyncio.run(get_system_profile(system_name='Jefferson Health'))
data = json.loads(result)
print(f'System: {data[\"system\"][\"name\"]}')
print(f'Hospitals: {data[\"system\"][\"hospital_count\"]}')
print(f'Total beds: {data[\"system\"][\"total_beds\"]}')
print(f'Facilities found: {len(data[\"inpatient_facilities\"])}')
print(f'Sub-entities: {len(data[\"sub_entities\"])}')
print(f'Outpatient sites: {len(data[\"outpatient_sites\"])}')
"
```

Expected: Full profile with 10+ hospitals, thousands of beds, and enrichment data.

**Step 4: Final commit**

```bash
git add -A
git commit -m "feat(health-system-profiler): complete implementation with smoke tests passing"
```

---

## Summary of Files Created

```
servers/health-system-profiler/
├── __init__.py
├── server.py                  # FastMCP server with 3 tools
├── models.py                  # Pydantic response models
├── data_loaders.py            # AHRQ + POS + NPPES download/cache
├── system_discovery.py        # AHRQ fuzzy search, system→CCN
├── facility_enrichment.py     # POS join for beds/services/staffing
├── graph_expansion.py         # RELATED_PROVIDER_NUMBER walk
└── outpatient_discovery.py    # NPPES search + taxonomy categorization

servers/health_system_profiler -> health-system-profiler (symlink)

scripts/
└── download_ahrq.py           # Playwright-based AHRQ download

tests/servers/health_system_profiler/
├── __init__.py
├── test_models.py
├── test_data_loaders.py
├── test_system_discovery.py
├── test_facility_enrichment.py
├── test_graph_expansion.py
├── test_outpatient_discovery.py
└── test_server.py
```

## Files Modified

- `pyproject.toml` — add `rapidfuzz>=3.0.0` dependency
- `docker-compose.yml` — add `health-system-profiler` service on port 8007
- `.mcp.json` — register `health-system-profiler` at `http://localhost:8007/mcp`
