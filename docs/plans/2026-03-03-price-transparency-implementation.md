# Price Transparency / MRF Engine Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an MCP server (port 8009) that downloads hospital MRF files on demand, parses them into cached Parquet indexes, and provides tools for querying negotiated rates, computing dispersion statistics, comparing across health systems, and benchmarking against Medicare.

**Architecture:** Four-module server (`mrf_registry`, `mrf_processor`, `benchmark_client`, `server`) with on-demand MRF download + Parquet caching. CSV/JSON MRF parsing adapted from MR-Explore. Benchmarking via CMS Physician Fee Schedule and Medicare Utilization APIs.

**Tech Stack:** FastMCP, Polars, DuckDB, ijson, httpx, Pydantic v2

**Reference Code:** Adapted from `/mnt/d/Coding Projects/MR-Explore/src/data/` (importer.py, recognition.py, json_parser.py, normalizer.py, models.py)

---

### Task 1: Scaffold Directory and Pydantic Models

**Files:**
- Create: `servers/price-transparency/__init__.py`
- Create: `servers/price-transparency/models.py`
- Create symlink: `servers/price_transparency` → `price-transparency`

**Step 1: Create directory and __init__.py**

```bash
mkdir -p "servers/price-transparency"
touch "servers/price-transparency/__init__.py"
```

**Step 2: Create symlink for Python imports**

```bash
cd servers && ln -s price-transparency price_transparency && cd ..
```

**Step 3: Write models.py**

Create `servers/price-transparency/models.py` with these Pydantic models following the project convention (see `servers/financial-intelligence/models.py` for pattern: `BaseModel` subclasses, `Field(description=...)`, `float | None = None`, `str = ""`):

```python
"""Pydantic models for price transparency / MRF engine."""

from pydantic import BaseModel, Field


class MRFLocation(BaseModel):
    """A single MRF file location for a hospital."""
    url: str = Field(description="URL to the MRF file (CSV or JSON)")
    format: str = Field(default="csv", description="File format: 'csv' or 'json'")
    last_verified: str = ""


class MRFIndexResult(BaseModel):
    """Result from search_mrf_index — hospital info + MRF URLs."""
    hospital_name: str = ""
    ccn: str = Field(default="", description="CMS Certification Number")
    ein: str = Field(default="", description="Employer Identification Number")
    city: str = ""
    state: str = ""
    mrf_urls: list[MRFLocation] = Field(default_factory=list)
    cached: bool = Field(default=False, description="Whether Parquet index exists for this hospital")
    cache_date: str = Field(default="", description="Date the Parquet index was built")
    row_count: int | None = Field(default=None, description="Number of charge records in cache")


class NegotiatedRate(BaseModel):
    """A single negotiated rate for a CPT code from a payer/plan."""
    cpt_code: str = ""
    description: str = ""
    payer_name: str = ""
    plan_name: str = ""
    negotiated_dollar: float | None = None
    negotiated_percentage: float | None = None
    methodology: str = ""
    setting: str = Field(default="", description="inpatient, outpatient, or both")
    billing_class: str = ""
    gross_charge: float | None = None
    min_charge: float | None = None
    max_charge: float | None = None


class NegotiatedRatesResponse(BaseModel):
    """Response from get_negotiated_rates."""
    hospital_name: str = ""
    hospital_id: str = ""
    cpt_codes_requested: list[str] = Field(default_factory=list)
    rates: list[NegotiatedRate] = Field(default_factory=list)
    total_rates: int = 0
    source: str = Field(default="", description="'parquet_cache' or 'live_download'")


class RateDispersion(BaseModel):
    """Rate dispersion statistics for a single CPT code across payers."""
    cpt_code: str = ""
    description: str = ""
    payer_count: int = 0
    min_rate: float | None = None
    max_rate: float | None = None
    median_rate: float | None = None
    mean_rate: float | None = None
    iqr: float | None = Field(default=None, description="Interquartile range (Q3 - Q1)")
    q25: float | None = None
    q75: float | None = None
    cv: float | None = Field(default=None, description="Coefficient of variation (std/mean)")
    std_dev: float | None = None


class HospitalRateComparison(BaseModel):
    """Rates for one hospital within a system comparison."""
    hospital_name: str = ""
    hospital_id: str = ""
    rates: list[NegotiatedRate] = Field(default_factory=list)


class SystemComparisonResponse(BaseModel):
    """Response from compare_rates_system."""
    system_name: str = ""
    cpt_codes: list[str] = Field(default_factory=list)
    hospitals: list[HospitalRateComparison] = Field(default_factory=list)


class BenchmarkComparison(BaseModel):
    """Benchmark data for a single CPT code."""
    cpt_code: str = ""
    description: str = ""
    hospital_median_rate: float | None = None
    medicare_allowed_amount: float | None = Field(default=None, description="PFS-calculated Medicare allowed amount")
    pct_of_medicare: float | None = Field(default=None, description="Hospital rate as % of Medicare")
    medicare_actual_avg_payment: float | None = Field(default=None, description="From CMS utilization data")
    peer_percentile: float | None = Field(default=None, description="Where this rate falls among cached peers (0-100)")
    peer_25th: float | None = None
    peer_50th: float | None = None
    peer_75th: float | None = None
    peer_90th: float | None = None
    peer_hospital_count: int = Field(default=0, description="Number of peer hospitals with data for this code")


class BenchmarkResponse(BaseModel):
    """Response from benchmark_rates."""
    hospital_name: str = ""
    hospital_id: str = ""
    locality: str = Field(default="", description="Medicare GPCI locality used")
    benchmarks: list[BenchmarkComparison] = Field(default_factory=list)
```

**Step 4: Verify models import correctly**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "from servers.price_transparency.models import MRFIndexResult, NegotiatedRate, BenchmarkResponse; print('OK')"`
Expected: `OK`

**Step 5: Commit**

```bash
git add servers/price-transparency/__init__.py servers/price-transparency/models.py servers/price_transparency
git commit -m "feat(price-transparency): scaffold directory and Pydantic models"
```

---

### Task 2: MRF Registry — Hospital Discovery

**Files:**
- Create: `servers/price-transparency/mrf_registry.py`

**Context:** This module maps hospital identifiers (name, CCN, EIN) to MRF file URLs. It has two layers: (1) a curated JSON registry cached locally, and (2) a live discovery pipeline that queries CMS Provider Data Catalog to find a hospital's website, then fetches `cms-hpt.txt` from that domain.

**Reference:** CMS Provider Data Catalog API at `https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0` — no auth, POST with `conditions` array, returns `results` array with `facility_id`, `facility_name`, `address`, `citytown`, `state`, `zip_code`, `hospital_overall_rating`. The `facility_id` is the CCN.

**Reference:** `cms-hpt.txt` files are plaintext at hospital domain roots. Format:
```
location-name: Hospital Name
source-page-url: https://...
mrf-url: https://..._standardcharges.csv
contact-name: Person
contact-email: email@...
```
Multiple location blocks may exist separated by blank lines.

**Step 1: Write mrf_registry.py**

```python
"""MRF Registry — discover hospital MRF file URLs.

Two-layer lookup:
1. Curated JSON registry (cached at ~/.healthcare-data-mcp/cache/mrf/registry.json)
2. Live discovery via CMS Provider Data Catalog + hospital cms-hpt.txt files
"""

import json
import logging
import re
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "mrf"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_REGISTRY_PATH = _CACHE_DIR / "registry.json"

CMS_PROVIDER_DATASET = "xubh-q36u"
CMS_PROVIDER_API = f"https://data.cms.gov/provider-data/api/1/datastore/query/{CMS_PROVIDER_DATASET}/0"


def _load_registry() -> dict:
    """Load the curated registry from disk."""
    if _REGISTRY_PATH.exists():
        try:
            return json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to load registry, starting fresh")
    return {"hospitals": {}}


def _save_registry(registry: dict) -> None:
    """Persist the registry to disk."""
    _REGISTRY_PATH.write_text(json.dumps(registry, indent=2), encoding="utf-8")


def search_registry(query: str, state: str = "") -> list[dict]:
    """Search the local curated registry by name, CCN, or EIN.

    Returns list of matching hospital entries (dicts with keys:
    name, ccn, ein, domain, mrf_urls, city, state).
    """
    registry = _load_registry()
    query_lower = query.strip().lower()
    results = []

    for ccn, entry in registry.get("hospitals", {}).items():
        name = entry.get("name", "").lower()
        ein = entry.get("ein", "").lower()
        entry_state = entry.get("state", "").upper()

        if state and entry_state != state.upper():
            continue

        if query_lower in name or query_lower == ccn.lower() or query_lower == ein:
            results.append({"ccn": ccn, **entry})

    return results


async def search_cms_providers(query: str, state: str = "") -> list[dict]:
    """Query CMS Provider Data Catalog for hospitals matching name or ID.

    Returns list of provider records from data.cms.gov.
    """
    conditions = []

    # Try as facility_id (CCN) first — exact match
    if re.match(r"^\d{6}$", query.strip()):
        conditions.append({"property": "facility_id", "value": query.strip(), "operator": "="})
    else:
        conditions.append({"property": "facility_name", "value": f"%{query}%", "operator": "LIKE"})

    if state:
        conditions.append({"property": "state", "value": state.upper(), "operator": "="})

    payload = {
        "conditions": conditions,
        "limit": 25,
        "offset": 0,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(CMS_PROVIDER_API, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
    except Exception as e:
        logger.warning("CMS provider search failed: %s", e)
        return []


async def fetch_cms_hpt_txt(domain: str) -> list[dict]:
    """Fetch and parse a hospital's cms-hpt.txt file.

    Returns list of location entries, each with keys:
    location_name, source_page_url, mrf_url, contact_name, contact_email.
    """
    url = f"https://{domain}/cms-hpt.txt"
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return _parse_cms_hpt_txt(resp.text)
    except Exception as e:
        logger.warning("Failed to fetch cms-hpt.txt from %s: %s", domain, e)
        return []


def _parse_cms_hpt_txt(text: str) -> list[dict]:
    """Parse cms-hpt.txt plaintext format into location entries."""
    entries = []
    current: dict[str, str] = {}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            if current:
                entries.append(current)
                current = {}
            continue

        # Parse "key: value" lines
        match = re.match(r"^([a-z_-]+)\s*:\s*(.+)$", line, re.IGNORECASE)
        if match:
            key = match.group(1).strip().lower().replace("-", "_")
            value = match.group(2).strip()
            current[key] = value

    if current:
        entries.append(current)

    return entries


async def discover_mrf_urls(query: str, state: str = "") -> list[dict]:
    """Full discovery pipeline: registry → CMS provider lookup → cms-hpt.txt.

    Returns list of hospital dicts with mrf_urls populated.
    Caches new discoveries in the registry.
    """
    # Layer 1: Check curated registry
    results = search_registry(query, state)
    if results:
        return results

    # Layer 2: Search CMS Provider Data Catalog
    providers = await search_cms_providers(query, state)
    if not providers:
        return []

    discovered = []
    registry = _load_registry()

    for provider in providers[:5]:  # Limit to 5 to avoid slow crawling
        ccn = provider.get("facility_id", "")
        name = provider.get("facility_name", "")

        # Check if already in registry
        if ccn in registry.get("hospitals", {}):
            discovered.append({"ccn": ccn, **registry["hospitals"][ccn]})
            continue

        entry = {
            "name": name,
            "ein": "",
            "city": provider.get("citytown", ""),
            "state": provider.get("state", ""),
            "domain": "",
            "mrf_urls": [],
        }

        discovered.append({"ccn": ccn, **entry})

        # Cache in registry (even without MRF URL — marks it as "looked up")
        registry.setdefault("hospitals", {})[ccn] = entry

    _save_registry(registry)
    return discovered


async def discover_and_fetch_hpt(domain: str, ccn: str = "") -> list[dict]:
    """Fetch cms-hpt.txt from a domain and update registry if CCN provided.

    Returns parsed location entries with mrf_url fields.
    """
    entries = await fetch_cms_hpt_txt(domain)
    if not entries or not ccn:
        return entries

    # Update registry with discovered MRF URLs
    registry = _load_registry()
    if ccn in registry.get("hospitals", {}):
        mrf_urls = []
        for entry in entries:
            mrf_url = entry.get("mrf_url", "")
            if mrf_url:
                fmt = "json" if mrf_url.lower().endswith(".json") else "csv"
                mrf_urls.append({"url": mrf_url, "format": fmt, "last_verified": ""})
        if mrf_urls:
            registry["hospitals"][ccn]["mrf_urls"] = mrf_urls
            registry["hospitals"][ccn]["domain"] = domain
            _save_registry(registry)

    return entries
```

**Step 2: Verify the module imports**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "from servers.price_transparency.mrf_registry import search_registry, discover_mrf_urls; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add servers/price-transparency/mrf_registry.py
git commit -m "feat(price-transparency): add MRF registry with CMS provider lookup and cms-hpt.txt discovery"
```

---

### Task 3: MRF Processor — CSV/JSON Parsing (Adapted from MR-Explore)

**Files:**
- Create: `servers/price-transparency/mrf_processor.py`

**Context:** This is the core module. It downloads MRF files, detects format (CSV/JSON), parses them using logic adapted from MR-Explore, normalizes to Parquet with lookup tables, and queries the cached Parquet via DuckDB. This task covers the parsing and normalization. Task 4 adds download and query.

**Reference — MR-Explore code to adapt:**
- `/mnt/d/Coding Projects/MR-Explore/src/data/models.py` — `CMS_COLUMN_MAPPING` (22 column mappings)
- `/mnt/d/Coding Projects/MR-Explore/src/data/recognition.py` — `fuzzy_match_columns()` (3-pass: exact, keyword heuristic, SequenceMatcher)
- `/mnt/d/Coding Projects/MR-Explore/src/data/importer.py` — `ChargeFileImporter` (header detection, Polars CSV read, column mapping + fuzzy fallback, numeric casting)
- `/mnt/d/Coding Projects/MR-Explore/src/data/sources/mrf/json_parser.py` — `CMSJSONParser` (in_network + allowed_amounts parsing)
- `/mnt/d/Coding Projects/MR-Explore/src/data/normalizer.py` — `normalize_dataframe()` (build lookup tables, replace text with IDs, write Parquet)

**Step 1: Write mrf_processor.py with parsing + normalization**

Create `servers/price-transparency/mrf_processor.py`. This file combines adapted versions of recognition.py, importer.py, json_parser.py, and normalizer.py into a single module. Key adaptations from MR-Explore:

1. Remove PyQt6/GUI dependencies (progress callbacks become logging)
2. Make download async (httpx streaming)
3. Use ijson for streaming JSON (large files)
4. Simplify normalizer (no duckdb_store import, inline schemas)
5. Add DuckDB query interface

The full file should contain these sections in order:

**Section A — Constants and column mapping** (from MR-Explore models.py):
```python
CMS_COLUMN_MAPPING = { ... }  # Copy exact dict from MR-Explore models.py
```

**Section B — Fuzzy column recognition** (from MR-Explore recognition.py):
```python
# Copy: _KEYWORD_RULES, _CHARGE_RULES, _SKIP_PATTERNS regex lists
# Copy: fuzzy_match_columns() function
# Copy: normalize_payer_name() function
# Drop: format_icd10_code (not needed), deduplicate_payer_names (not needed)
```

**Section C — CSV header detection and parsing** (from MR-Explore importer.py):
```python
def _looks_like_data_header(values: list[str]) -> bool: ...  # Copy from importer
def _find_data_header_row(file_path: Path) -> int: ...  # Adapt from importer (remove class, make standalone)
def _parse_csv_line(line: str) -> list[str]: ...  # Copy from importer
def _extract_hospital_info(file_path: Path) -> dict: ...  # Adapt: return dict instead of HospitalInfo dataclass

def parse_csv_mrf(file_path: Path) -> pl.DataFrame:
    """Parse a CMS CSV MRF file into a Polars DataFrame.

    Adapted from MR-Explore ChargeFileImporter.import_file().
    Handles multi-row headers, CMS column mapping, fuzzy fallback, numeric casting.
    """
    header_row = _find_data_header_row(file_path)
    df = pl.read_csv(
        file_path,
        skip_rows=header_row,
        infer_schema_length=10000,
        ignore_errors=True,
        truncate_ragged_lines=True,
        null_values=["", "N/A", "NA", "null"],
    )

    # Exact column mapping
    rename_mapping = {}
    for orig_col in df.columns:
        if orig_col in CMS_COLUMN_MAPPING:
            rename_mapping[orig_col] = CMS_COLUMN_MAPPING[orig_col]

    # Fuzzy fallback if <3 exact matches
    if len(rename_mapping) < 3:
        fuzzy_mapping = fuzzy_match_columns(df.columns, CMS_COLUMN_MAPPING)
        for orig, target in fuzzy_mapping.items():
            if orig == target or orig in rename_mapping:
                continue
            if target not in rename_mapping.values() and target not in df.columns:
                rename_mapping[orig] = target

    if rename_mapping:
        df = df.rename(rename_mapping)

    # Cast numeric columns
    for col in ["gross_charge", "discounted_cash", "negotiated_dollar",
                "negotiated_percentage", "estimated_amount", "min_charge", "max_charge"]:
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

    return df
```

**Section D — JSON MRF parsing** (from MR-Explore json_parser.py):
```python
def parse_json_mrf(file_path: Path) -> pl.DataFrame:
    """Parse a CMS JSON MRF file into a Polars DataFrame.

    For files <500MB, uses json.load(). For larger files, uses ijson streaming.
    Adapted from MR-Explore CMSJSONParser.
    """
    file_size = file_path.stat().st_size

    if file_size < 500_000_000:  # <500MB — load into memory
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        records = _parse_in_network(data.get("in_network", []))
        records += _parse_allowed_amounts(data.get("allowed_amounts", []))
    else:
        records = _parse_json_streaming(file_path)

    if not records:
        raise ValueError("No records found in JSON MRF file")

    return pl.DataFrame(records)


def _parse_in_network(items: list) -> list[dict]:
    # Adapted from CMSJSONParser._parse_in_network
    ...

def _parse_allowed_amounts(items: list) -> list[dict]:
    # Adapted from CMSJSONParser._parse_allowed_amounts
    ...

def _parse_json_streaming(file_path: Path) -> list[dict]:
    """Stream-parse large JSON MRF files using ijson."""
    import ijson
    records = []
    with open(file_path, "rb") as f:
        for item in ijson.items(f, "in_network.item"):
            records.extend(_parse_in_network([item]))
    # Note: allowed_amounts less common in large files, skip for streaming
    return records
```

**Section E — Parquet normalization** (from MR-Explore normalizer.py):
```python
# Simplified Parquet schema (subset of MR-Explore's full schema)
CHARGES_SCHEMA = {
    "id": pl.Int64,
    "description_id": pl.Int32,
    "code1": pl.Utf8,
    "code1_type": pl.Utf8,
    "setting": pl.Utf8,
    "billing_class": pl.Utf8,
    "gross_charge": pl.Float64,
    "discounted_cash": pl.Float64,
    "payer_id": pl.Int32,
    "plan_id": pl.Int32,
    "negotiated_dollar": pl.Float64,
    "negotiated_percentage": pl.Float64,
    "methodology": pl.Utf8,
    "min_charge": pl.Float64,
    "max_charge": pl.Float64,
}

# Column renames from importer output to schema names
_IMPORTER_TO_SCHEMA = {
    "code_1": "code1",
    "code_1_type": "code1_type",
    "code_2": "code2",
    "code_2_type": "code2_type",
}

# Lookup columns: (source_col, table_name, value_col, id_col)
_LOOKUP_COLUMNS = [
    ("description", "descriptions", "text", "description_id"),
    ("payer_name", "payers", "name", "payer_id"),
    ("plan_name", "plans", "name", "plan_id"),
]


def normalize_to_parquet(df: pl.DataFrame, hospital_name: str, output_dir: Path) -> dict:
    """Normalize a parsed DataFrame and write as Parquet data pack.

    Adapted from MR-Explore normalize_dataframe() + write_data_pack().

    Creates:
      output_dir/charges.parquet
      output_dir/descriptions.parquet
      output_dir/payers.parquet
      output_dir/plans.parquet (if plan_name column exists)
      output_dir/metadata.json

    Returns metadata dict.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Rename importer columns to schema names
    rename_map = {k: v for k, v in _IMPORTER_TO_SCHEMA.items() if k in df.columns}
    if rename_map:
        df = df.rename(rename_map)

    # Build lookup tables and replace text with IDs
    lookups = {}
    for source_col, table_name, value_col, id_col in _LOOKUP_COLUMNS:
        if source_col in df.columns:
            unique_vals = df.select(source_col).unique().drop_nulls().sort(source_col)
            if len(unique_vals) > 0:
                lookup_df = unique_vals.with_row_index("id", offset=1).rename(
                    {source_col: value_col, "id": "id"}
                ).cast({"id": pl.Int32})
                lookups[table_name] = lookup_df

                join_df = lookup_df.rename({value_col: source_col, "id": id_col})
                df = df.join(join_df, on=source_col, how="left").drop(source_col)
            else:
                lookups[table_name] = pl.DataFrame({"id": pl.Series([], dtype=pl.Int32), value_col: pl.Series([], dtype=pl.Utf8)})
                df = df.with_columns(pl.lit(None).cast(pl.Int32).alias(id_col))
        else:
            lookups[table_name] = pl.DataFrame({"id": pl.Series([], dtype=pl.Int32), value_col: pl.Series([], dtype=pl.Utf8)})
            df = df.with_columns(pl.lit(None).cast(pl.Int32).alias(id_col))

    # Add auto-increment ID
    df = df.with_row_index("id", offset=1)

    # Ensure schema columns exist
    for col_name, col_type in CHARGES_SCHEMA.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    # Select only schema columns
    existing_cols = [c for c in CHARGES_SCHEMA.keys() if c in df.columns]
    df = df.select(existing_cols)

    # Write Parquet files
    df.write_parquet(output_dir / "charges.parquet", compression="zstd", compression_level=3)
    for table_name, lookup_df in lookups.items():
        lookup_df.write_parquet(output_dir / f"{table_name}.parquet", compression="zstd", compression_level=3)

    # Write metadata
    metadata = {
        "hospital_name": hospital_name,
        "row_count": len(df),
        "payer_count": len(lookups.get("payers", pl.DataFrame())),
        "description_count": len(lookups.get("descriptions", pl.DataFrame())),
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    return metadata
```

**Step 2: Verify parsing works with a real MR-Explore CSV file**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
from servers.price_transparency.mrf_processor import parse_csv_mrf
import polars as pl
from pathlib import Path

# Use one of the MR-Explore sample files
sample_dir = Path('/mnt/d/Coding Projects/MR-Explore/data_source')
csvs = list(sample_dir.glob('*.csv'))
if csvs:
    df = parse_csv_mrf(csvs[0])
    print(f'Parsed {len(df)} rows, columns: {df.columns[:5]}...')
    print(f'Has negotiated_dollar: {\"negotiated_dollar\" in df.columns}')
else:
    print('No sample CSVs found')
"`
Expected: Parsed N rows with negotiated_dollar column present.

**Step 3: Commit**

```bash
git add servers/price-transparency/mrf_processor.py
git commit -m "feat(price-transparency): add MRF processor with CSV/JSON parsing adapted from MR-Explore"
```

---

### Task 4: MRF Processor — Download Pipeline and DuckDB Query Interface

**Files:**
- Modify: `servers/price-transparency/mrf_processor.py`

**Context:** Add the download pipeline (httpx async streaming for large MRF files) and the DuckDB query interface over cached Parquet files. This completes the mrf_processor module.

**Step 1: Add download + cache management functions to mrf_processor.py**

Add these functions to the module (at the top, after imports):

```python
import time

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "mrf"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_CACHE_TTL_DAYS = 30


def _hospital_cache_dir(hospital_id: str) -> Path:
    """Get cache directory for a hospital (by CCN or EIN)."""
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", hospital_id)
    return _CACHE_DIR / safe_id


def is_cached(hospital_id: str) -> tuple[bool, dict]:
    """Check if a hospital's MRF data is already cached as Parquet.

    Returns (cached: bool, metadata: dict).
    """
    cache_dir = _hospital_cache_dir(hospital_id)
    metadata_path = cache_dir / "metadata.json"
    charges_path = cache_dir / "charges.parquet"

    if not metadata_path.exists() or not charges_path.exists():
        return False, {}

    try:
        metadata = json.loads(metadata_path.read_text())
        # Check TTL
        cache_date = metadata.get("download_date", "")
        if cache_date:
            from datetime import datetime, timedelta
            cached_dt = datetime.fromisoformat(cache_date)
            if datetime.now() - cached_dt > timedelta(days=_CACHE_TTL_DAYS):
                return False, metadata
        return True, metadata
    except Exception:
        return False, {}


async def download_mrf(url: str, hospital_id: str) -> Path:
    """Download an MRF file (CSV or JSON) to cache.

    Uses httpx streaming for large files. Returns path to downloaded file.
    """
    cache_dir = _hospital_cache_dir(hospital_id)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Determine filename from URL
    ext = ".json" if url.lower().endswith(".json") else ".csv"
    raw_path = cache_dir / f"raw{ext}"

    logger.info("Downloading MRF: %s -> %s", url, raw_path)

    async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()

            content_length = resp.headers.get("content-length")
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                logger.info("MRF file size: %.1f MB", size_mb)

            with open(raw_path, "wb") as f:
                downloaded = 0
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):  # 1MB chunks
                    f.write(chunk)
                    downloaded += len(chunk)

    logger.info("Download complete: %.1f MB", downloaded / (1024 * 1024))
    return raw_path


async def process_mrf(url: str, hospital_id: str, hospital_name: str = "") -> dict:
    """Full pipeline: download MRF → parse → normalize to Parquet → return metadata.

    If already cached, returns cached metadata.
    """
    cached, metadata = is_cached(hospital_id)
    if cached:
        logger.info("Using cached Parquet for %s", hospital_id)
        return metadata

    # Download
    raw_path = await download_mrf(url, hospital_id)

    # Parse based on format
    if raw_path.suffix == ".json":
        df = parse_json_mrf(raw_path)
    else:
        df = parse_csv_mrf(raw_path)

    if hospital_name == "":
        hospital_name = _extract_hospital_info(raw_path).get("name", hospital_id) if raw_path.suffix == ".csv" else hospital_id

    # Normalize and write Parquet
    cache_dir = _hospital_cache_dir(hospital_id)
    metadata = normalize_to_parquet(df, hospital_name, cache_dir)

    # Add download metadata
    from datetime import datetime
    metadata["download_date"] = datetime.now().isoformat()
    metadata["source_url"] = url
    metadata["source_format"] = raw_path.suffix.lstrip(".")
    (cache_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    # Clean up raw file to save disk space
    raw_path.unlink(missing_ok=True)

    return metadata
```

**Step 2: Add DuckDB query interface**

Add at the bottom of mrf_processor.py:

```python
import duckdb


def _query_parquet(hospital_id: str, sql: str, params: list | None = None) -> list[dict]:
    """Execute a DuckDB query against a hospital's cached Parquet files.

    Registers charges.parquet, descriptions.parquet, payers.parquet as views,
    then runs the provided SQL.
    """
    cache_dir = _hospital_cache_dir(hospital_id)
    charges_path = cache_dir / "charges.parquet"
    if not charges_path.exists():
        raise FileNotFoundError(f"No cached data for hospital {hospital_id}")

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"CREATE VIEW charges AS SELECT * FROM read_parquet('{charges_path}')")

        desc_path = cache_dir / "descriptions.parquet"
        if desc_path.exists():
            con.execute(f"CREATE VIEW descriptions AS SELECT * FROM read_parquet('{desc_path}')")

        payers_path = cache_dir / "payers.parquet"
        if payers_path.exists():
            con.execute(f"CREATE VIEW payers AS SELECT * FROM read_parquet('{payers_path}')")

        plans_path = cache_dir / "plans.parquet"
        if plans_path.exists():
            con.execute(f"CREATE VIEW plans AS SELECT * FROM read_parquet('{plans_path}')")

        if params:
            result = con.execute(sql, params)
        else:
            result = con.execute(sql)

        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        con.close()


def get_rates(hospital_id: str, cpt_codes: list[str], payer: str = "") -> list[dict]:
    """Get negotiated rates for specific CPT codes from cached Parquet data."""
    placeholders = ", ".join(["?" for _ in cpt_codes])
    params = list(cpt_codes)

    payer_clause = ""
    if payer:
        payer_clause = "AND p.name LIKE ?"
        params.append(f"%{payer}%")

    sql = f"""
        SELECT
            c.code1 AS cpt_code,
            d.text AS description,
            p.name AS payer_name,
            COALESCE(pl.name, '') AS plan_name,
            c.negotiated_dollar,
            c.negotiated_percentage,
            c.methodology,
            c.setting,
            c.billing_class,
            c.gross_charge,
            c.min_charge,
            c.max_charge
        FROM charges c
        LEFT JOIN descriptions d ON c.description_id = d.id
        LEFT JOIN payers p ON c.payer_id = p.id
        LEFT JOIN plans pl ON c.plan_id = pl.id
        WHERE c.code1 IN ({placeholders})
        AND c.negotiated_dollar IS NOT NULL
        {payer_clause}
        ORDER BY c.code1, p.name
    """
    return _query_parquet(hospital_id, sql, params)


def get_rate_stats(hospital_id: str, cpt_codes: list[str]) -> list[dict]:
    """Compute rate dispersion statistics for CPT codes across payers."""
    placeholders = ", ".join(["?" for _ in cpt_codes])
    sql = f"""
        SELECT
            c.code1 AS cpt_code,
            d.text AS description,
            COUNT(DISTINCT p.name) AS payer_count,
            MIN(c.negotiated_dollar) AS min_rate,
            MAX(c.negotiated_dollar) AS max_rate,
            MEDIAN(c.negotiated_dollar) AS median_rate,
            AVG(c.negotiated_dollar) AS mean_rate,
            QUANTILE_CONT(c.negotiated_dollar, 0.25) AS q25,
            QUANTILE_CONT(c.negotiated_dollar, 0.75) AS q75,
            STDDEV(c.negotiated_dollar) AS std_dev
        FROM charges c
        LEFT JOIN descriptions d ON c.description_id = d.id
        LEFT JOIN payers p ON c.payer_id = p.id
        WHERE c.code1 IN ({placeholders})
        AND c.negotiated_dollar IS NOT NULL
        AND c.negotiated_dollar > 0
        GROUP BY c.code1, d.text
        ORDER BY c.code1
    """
    results = _query_parquet(hospital_id, sql, list(cpt_codes))

    # Compute derived stats
    for r in results:
        q25 = r.get("q25")
        q75 = r.get("q75")
        r["iqr"] = (q75 - q25) if q25 is not None and q75 is not None else None

        mean = r.get("mean_rate")
        std = r.get("std_dev")
        r["cv"] = (std / mean) if mean and std and mean > 0 else None

    return results


def get_all_cached_hospitals() -> list[dict]:
    """List all hospitals with cached Parquet data."""
    results = []
    for d in _CACHE_DIR.iterdir():
        if d.is_dir() and (d / "metadata.json").exists() and (d / "charges.parquet").exists():
            try:
                metadata = json.loads((d / "metadata.json").read_text())
                results.append({
                    "hospital_id": d.name,
                    "hospital_name": metadata.get("hospital_name", ""),
                    "row_count": metadata.get("row_count", 0),
                    "download_date": metadata.get("download_date", ""),
                })
            except Exception:
                continue
    return results


def get_cross_hospital_rates(cpt_codes: list[str]) -> list[dict]:
    """Get rates for CPT codes across ALL cached hospitals.

    Used for computing peer percentiles in benchmarking.
    """
    all_rates = []
    for hospital in get_all_cached_hospitals():
        hid = hospital["hospital_id"]
        try:
            rates = get_rates(hid, cpt_codes)
            for r in rates:
                r["hospital_id"] = hid
                r["hospital_name"] = hospital["hospital_name"]
            all_rates.extend(rates)
        except Exception:
            continue
    return all_rates
```

**Step 3: Verify DuckDB query works**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
import duckdb
print(f'DuckDB version: {duckdb.__version__}')
print('DuckDB import OK')
"`
Expected: DuckDB version printed. If duckdb is not installed, run `pip install duckdb`.

**Step 4: Commit**

```bash
git add servers/price-transparency/mrf_processor.py
git commit -m "feat(price-transparency): add MRF download pipeline and DuckDB query interface"
```

---

### Task 5: Benchmark Client — CMS PFS + Utilization + Peer Percentiles

**Files:**
- Create: `servers/price-transparency/benchmark_client.py`

**Context:** This module fetches benchmark data from two CMS APIs (Physician Fee Schedule and Medicare Utilization) and computes cross-hospital percentiles from cached data.

**Reference:**
- PFS API: `https://pfs.data.cms.gov/api/1/datastore/query/{dataset_id}/0` — POST with conditions, returns RVU components. 2026 indicators dataset: `7c7df311-5315-4f38-b9ed-fd62f8bebe11`. 2026 localities: `81f942b8-3f6c-4b36-a151-0888376d9ca0`. Conversion factor: $33.4009.
- Medicare Utilization: `https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0` — query by HCPCS code for actual payment amounts.

**Step 1: Write benchmark_client.py**

```python
"""Benchmark client — CMS Physician Fee Schedule, Medicare utilization data, peer percentiles."""

import json
import logging
import statistics
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CMS Physician Fee Schedule (PFS) API
# ---------------------------------------------------------------------------

PFS_API_BASE = "https://pfs.data.cms.gov/api/1/datastore/query"

# Dataset IDs for 2026 (update annually)
PFS_INDICATORS_2026 = "7c7df311-5315-4f38-b9ed-fd62f8bebe11"
PFS_LOCALITIES_2026 = "81f942b8-3f6c-4b36-a151-0888376d9ca0"

# 2026 Medicare conversion factor
CONVERSION_FACTOR_2026 = 33.4009

# Default locality (national)
DEFAULT_LOCALITY = "0000000"  # National average

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "mrf" / "pfs"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


async def get_pfs_rate(hcpcs_code: str, locality: str = "") -> dict | None:
    """Look up Physician Fee Schedule data for an HCPCS/CPT code.

    Returns dict with rvu_work, full_fac_total, full_nfac_total, conv_fact,
    or None if not found.
    """
    dataset_id = PFS_INDICATORS_2026
    conditions = [{"property": "hcpc", "value": hcpcs_code.strip(), "operator": "="}]

    payload = {"conditions": conditions, "limit": 10, "offset": 0}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{PFS_API_BASE}/{dataset_id}/0", json=payload)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if results:
                return results[0]  # First matching record
            return None
    except Exception as e:
        logger.warning("PFS lookup failed for %s: %s", hcpcs_code, e)
        return None


async def get_locality_gpci(locality: str = "") -> dict | None:
    """Look up GPCI values for a Medicare locality.

    Returns dict with gpci_work, gpci_pe, gpci_mp.
    """
    if not locality:
        locality = DEFAULT_LOCALITY

    dataset_id = PFS_LOCALITIES_2026
    conditions = [{"property": "locality", "value": locality, "operator": "="}]
    payload = {"conditions": conditions, "limit": 5, "offset": 0}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(f"{PFS_API_BASE}/{dataset_id}/0", json=payload)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if results:
                return results[0]
            return None
    except Exception as e:
        logger.warning("GPCI lookup failed for locality %s: %s", locality, e)
        return None


def calculate_medicare_allowed(pfs_data: dict, gpci_data: dict | None = None) -> float | None:
    """Calculate Medicare allowed amount from PFS RVUs and GPCIs.

    Formula: (Work_RVU * Work_GPCI + PE_RVU * PE_GPCI + MP_RVU * MP_GPCI) * CF

    If no GPCI data, uses the pre-calculated full_nfac_total * CF.
    """
    try:
        if gpci_data:
            work_rvu = float(pfs_data.get("rvu_work", 0))
            pe_rvu = float(pfs_data.get("full_nfac_pe", 0))
            mp_rvu = float(pfs_data.get("rvu_mp", 0))

            gpci_work = float(gpci_data.get("gpci_work", 1.0))
            gpci_pe = float(gpci_data.get("gpci_pe", 1.0))
            gpci_mp = float(gpci_data.get("gpci_mp", 1.0))

            cf = float(pfs_data.get("conv_fact", CONVERSION_FACTOR_2026))

            return round((work_rvu * gpci_work + pe_rvu * gpci_pe + mp_rvu * gpci_mp) * cf, 2)
        else:
            # Use pre-calculated total RVU * conversion factor
            total_rvu = float(pfs_data.get("full_nfac_total", 0))
            cf = float(pfs_data.get("conv_fact", CONVERSION_FACTOR_2026))
            return round(total_rvu * cf, 2) if total_rvu > 0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# CMS Medicare Utilization Data
# ---------------------------------------------------------------------------

CMS_UTILIZATION_API = "https://data.cms.gov/provider-data/api/1/datastore/query"
# Medicare Physician & Other Practitioners - by Provider and Service
# This dataset ID changes annually — search data.cms.gov for current version
UTILIZATION_DATASET = "fs4p-t5eq"  # 2022 dataset (most recent available)


async def get_utilization_data(hcpcs_code: str) -> dict | None:
    """Get aggregate Medicare utilization data for an HCPCS code.

    Returns average allowed amount, average payment, average submitted charge,
    aggregated across all providers nationally.
    """
    conditions = [{"property": "hcpcs_cd", "value": hcpcs_code.strip(), "operator": "="}]
    payload = {"conditions": conditions, "limit": 500, "offset": 0}

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(f"{CMS_UTILIZATION_API}/{UTILIZATION_DATASET}/0", json=payload)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])

            if not results:
                return None

            # Aggregate across all providers
            allowed_amts = []
            payment_amts = []
            submitted_amts = []
            total_services = 0

            for r in results:
                try:
                    allowed = float(r.get("avg_mdcr_alowd_amt", 0))
                    payment = float(r.get("avg_mdcr_pymt_amt", 0))
                    submitted = float(r.get("avg_sbmtd_chrg", 0))
                    services = int(float(r.get("tot_srvcs", 0)))

                    if allowed > 0:
                        allowed_amts.append(allowed)
                    if payment > 0:
                        payment_amts.append(payment)
                    if submitted > 0:
                        submitted_amts.append(submitted)
                    total_services += services
                except (ValueError, TypeError):
                    continue

            return {
                "hcpcs_code": hcpcs_code,
                "avg_medicare_allowed": round(statistics.mean(allowed_amts), 2) if allowed_amts else None,
                "avg_medicare_payment": round(statistics.mean(payment_amts), 2) if payment_amts else None,
                "avg_submitted_charge": round(statistics.mean(submitted_amts), 2) if submitted_amts else None,
                "total_services": total_services,
                "provider_count": len(results),
            }
    except Exception as e:
        logger.warning("Utilization data lookup failed for %s: %s", hcpcs_code, e)
        return None


# ---------------------------------------------------------------------------
# Cross-Hospital Peer Percentiles
# ---------------------------------------------------------------------------

def compute_peer_percentiles(rates: list[float]) -> dict:
    """Compute percentile statistics from a list of rates.

    Returns dict with p25, p50 (median), p75, p90 and count.
    """
    if not rates:
        return {"p25": None, "p50": None, "p75": None, "p90": None, "count": 0}

    sorted_rates = sorted(rates)
    n = len(sorted_rates)

    def percentile(p):
        k = (n - 1) * (p / 100)
        f = int(k)
        c = f + 1
        if c >= n:
            return sorted_rates[-1]
        return sorted_rates[f] + (k - f) * (sorted_rates[c] - sorted_rates[f])

    return {
        "p25": round(percentile(25), 2),
        "p50": round(percentile(50), 2),
        "p75": round(percentile(75), 2),
        "p90": round(percentile(90), 2),
        "count": n,
    }


def compute_percentile_rank(value: float, rates: list[float]) -> float | None:
    """Compute where a value falls in a distribution (0-100)."""
    if not rates or value is None:
        return None
    below = sum(1 for r in rates if r < value)
    return round((below / len(rates)) * 100, 1)
```

**Step 2: Verify PFS API call works**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
import asyncio
from servers.price_transparency.benchmark_client import get_pfs_rate, calculate_medicare_allowed

async def test():
    r = await get_pfs_rate('99213')
    if r:
        print(f'CPT 99213: work_rvu={r.get(\"rvu_work\")}, total={r.get(\"full_nfac_total\")}')
        allowed = calculate_medicare_allowed(r)
        print(f'Medicare allowed (national): \${allowed}')
    else:
        print('PFS lookup returned None')

asyncio.run(test())
"`
Expected: CPT 99213 work_rvu and calculated Medicare allowed amount.

**Step 3: Commit**

```bash
git add servers/price-transparency/benchmark_client.py
git commit -m "feat(price-transparency): add benchmark client with CMS PFS, utilization data, and peer percentiles"
```

---

### Task 6: Server — Wire Up 5 MCP Tools

**Files:**
- Create: `servers/price-transparency/server.py`

**Context:** FastMCP server with 5 tools following the project pattern (see `servers/financial-intelligence/server.py`): transport from env vars, `@mcp.tool()` async functions returning `json.dumps()`, try/except with error JSON.

**Step 1: Write server.py**

```python
"""Price Transparency / MRF Engine MCP Server.

Provides tools for hospital Machine-Readable File (MRF) analysis including
negotiated rate lookup, rate dispersion statistics, health system comparison,
and Medicare benchmarking.
"""

import json
import logging
import os as _os

from mcp.server.fastmcp import FastMCP

from . import mrf_registry, mrf_processor, benchmark_client
from .models import (
    BenchmarkComparison,
    BenchmarkResponse,
    HospitalRateComparison,
    MRFIndexResult,
    MRFLocation,
    NegotiatedRate,
    NegotiatedRatesResponse,
    RateDispersion,
    SystemComparisonResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "price-transparency"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8009"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Tool 1: search_mrf_index
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_mrf_index(query: str, state: str = "") -> str:
    """Find a hospital's Machine-Readable File (MRF) URL from the CMS MRF lookup.

    Searches the curated hospital registry and CMS Provider Data Catalog.
    Returns hospital info, MRF file URLs, and whether data is already cached.

    Args:
        query: Hospital name, CMS Certification Number (CCN), or EIN.
        state: Two-letter state code filter (e.g. "PA").
    """
    try:
        hospitals = await mrf_registry.discover_mrf_urls(query, state)

        results = []
        for h in hospitals:
            cached, metadata = mrf_processor.is_cached(h.get("ccn", ""))
            results.append(MRFIndexResult(
                hospital_name=h.get("name", ""),
                ccn=h.get("ccn", ""),
                ein=h.get("ein", ""),
                city=h.get("city", ""),
                state=h.get("state", ""),
                mrf_urls=[MRFLocation(**u) for u in h.get("mrf_urls", [])],
                cached=cached,
                cache_date=metadata.get("download_date", ""),
                row_count=metadata.get("row_count"),
            ).model_dump())

        return json.dumps({"total_results": len(results), "hospitals": results})
    except Exception as e:
        logger.exception("search_mrf_index failed")
        return json.dumps({"error": f"search_mrf_index failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_negotiated_rates
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_negotiated_rates(hospital_id: str, cpt_codes: list[str], payer: str = "") -> str:
    """Retrieve negotiated rates for specific CPT/HCPCS codes from a hospital's MRF.

    If the hospital's MRF is not yet cached, provide the MRF URL via mrf_url parameter.
    Use search_mrf_index first to find the hospital and its MRF URL.

    Args:
        hospital_id: Hospital identifier (CCN or EIN from search_mrf_index).
        cpt_codes: List of CPT/HCPCS codes to look up (e.g. ["99213", "99214"]).
        payer: Optional payer name filter (partial match).
    """
    try:
        cached, metadata = mrf_processor.is_cached(hospital_id)
        if not cached:
            return json.dumps({
                "error": f"No cached data for hospital {hospital_id}. "
                         "Use search_mrf_index to find the hospital, then call "
                         "get_negotiated_rates again after the MRF is downloaded. "
                         "To trigger a download, provide the MRF URL."
            })

        rates = mrf_processor.get_rates(hospital_id, cpt_codes, payer)

        result = NegotiatedRatesResponse(
            hospital_name=metadata.get("hospital_name", ""),
            hospital_id=hospital_id,
            cpt_codes_requested=cpt_codes,
            rates=[NegotiatedRate(**r) for r in rates],
            total_rates=len(rates),
            source="parquet_cache",
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_negotiated_rates failed")
        return json.dumps({"error": f"get_negotiated_rates failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: compute_rate_dispersion
# ---------------------------------------------------------------------------
@mcp.tool()
async def compute_rate_dispersion(hospital_id: str, cpt_codes: list[str]) -> str:
    """Calculate rate dispersion statistics across payers for specified CPT codes.

    Computes min, max, median, mean, IQR, standard deviation, and coefficient
    of variation for each CPT code's negotiated rates across all payers.

    Args:
        hospital_id: Hospital identifier (CCN or EIN).
        cpt_codes: List of CPT/HCPCS codes to analyze.
    """
    try:
        cached, _ = mrf_processor.is_cached(hospital_id)
        if not cached:
            return json.dumps({"error": f"No cached data for hospital {hospital_id}. Use search_mrf_index first."})

        stats = mrf_processor.get_rate_stats(hospital_id, cpt_codes)

        results = []
        for s in stats:
            results.append(RateDispersion(
                cpt_code=s.get("cpt_code", ""),
                description=s.get("description", ""),
                payer_count=s.get("payer_count", 0),
                min_rate=s.get("min_rate"),
                max_rate=s.get("max_rate"),
                median_rate=s.get("median_rate"),
                mean_rate=s.get("mean_rate"),
                iqr=s.get("iqr"),
                q25=s.get("q25"),
                q75=s.get("q75"),
                cv=s.get("cv"),
                std_dev=s.get("std_dev"),
            ).model_dump())

        return json.dumps({"hospital_id": hospital_id, "dispersion": results})
    except Exception as e:
        logger.exception("compute_rate_dispersion failed")
        return json.dumps({"error": f"compute_rate_dispersion failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: compare_rates_system
# ---------------------------------------------------------------------------
@mcp.tool()
async def compare_rates_system(system_name: str, cpt_codes: list[str]) -> str:
    """Compare negotiated rates across hospitals within the same health system.

    Looks up all cached hospitals matching the system name and returns
    side-by-side rate comparisons for the requested CPT codes.

    Args:
        system_name: Health system name (e.g. "Penn Medicine", "Jefferson").
        cpt_codes: List of CPT/HCPCS codes to compare.
    """
    try:
        # Find hospitals matching system name
        all_hospitals = mrf_processor.get_all_cached_hospitals()
        matching = [
            h for h in all_hospitals
            if system_name.lower() in h.get("hospital_name", "").lower()
        ]

        if not matching:
            return json.dumps({
                "error": f"No cached hospitals found matching '{system_name}'. "
                         "Cache hospital data using search_mrf_index + get_negotiated_rates first."
            })

        comparisons = []
        for h in matching:
            hid = h["hospital_id"]
            try:
                rates = mrf_processor.get_rates(hid, cpt_codes)
                comparisons.append(HospitalRateComparison(
                    hospital_name=h.get("hospital_name", ""),
                    hospital_id=hid,
                    rates=[NegotiatedRate(**r) for r in rates],
                ).model_dump())
            except Exception:
                continue

        result = SystemComparisonResponse(
            system_name=system_name,
            cpt_codes=cpt_codes,
            hospitals=comparisons,
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("compare_rates_system failed")
        return json.dumps({"error": f"compare_rates_system failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: benchmark_rates
# ---------------------------------------------------------------------------
@mcp.tool()
async def benchmark_rates(hospital_id: str, cpt_codes: list[str], locality: str = "") -> str:
    """Compare a hospital's rates against Medicare and peer hospital benchmarks.

    For each CPT code, returns:
    - Medicare allowed amount (from CMS Physician Fee Schedule)
    - Hospital rate as % of Medicare
    - Actual average Medicare payment (from CMS utilization data)
    - Peer percentile ranking (from all cached hospitals)

    Args:
        hospital_id: Hospital identifier (CCN or EIN).
        cpt_codes: List of CPT/HCPCS codes to benchmark.
        locality: Medicare GPCI locality code (e.g. "0100000" for Alabama). Defaults to national average.
    """
    try:
        cached, metadata = mrf_processor.is_cached(hospital_id)
        if not cached:
            return json.dumps({"error": f"No cached data for hospital {hospital_id}."})

        # Get GPCI data for locality
        gpci_data = await benchmark_client.get_locality_gpci(locality) if locality else None

        # Get all peer rates for percentile computation
        peer_rates_all = mrf_processor.get_cross_hospital_rates(cpt_codes)

        benchmarks = []
        for code in cpt_codes:
            # Get hospital's median rate for this code
            hospital_stats = mrf_processor.get_rate_stats(hospital_id, [code])
            hospital_median = hospital_stats[0].get("median_rate") if hospital_stats else None

            # Get Medicare PFS rate
            pfs_data = await benchmark_client.get_pfs_rate(code)
            medicare_allowed = None
            if pfs_data:
                medicare_allowed = benchmark_client.calculate_medicare_allowed(pfs_data, gpci_data)

            # Get Medicare utilization data
            utilization = await benchmark_client.get_utilization_data(code)
            actual_payment = utilization.get("avg_medicare_payment") if utilization else None

            # Compute peer percentiles
            peer_rates = [r["negotiated_dollar"] for r in peer_rates_all
                         if r.get("cpt_code") == code and r.get("negotiated_dollar")]
            peer_stats = benchmark_client.compute_peer_percentiles(peer_rates)
            peer_rank = benchmark_client.compute_percentile_rank(hospital_median, peer_rates) if hospital_median else None

            # Compute % of Medicare
            pct_of_medicare = None
            if hospital_median and medicare_allowed and medicare_allowed > 0:
                pct_of_medicare = round((hospital_median / medicare_allowed) * 100, 1)

            description = hospital_stats[0].get("description", "") if hospital_stats else ""

            benchmarks.append(BenchmarkComparison(
                cpt_code=code,
                description=description,
                hospital_median_rate=hospital_median,
                medicare_allowed_amount=medicare_allowed,
                pct_of_medicare=pct_of_medicare,
                medicare_actual_avg_payment=actual_payment,
                peer_percentile=peer_rank,
                peer_25th=peer_stats.get("p25"),
                peer_50th=peer_stats.get("p50"),
                peer_75th=peer_stats.get("p75"),
                peer_90th=peer_stats.get("p90"),
                peer_hospital_count=peer_stats.get("count", 0),
            ).model_dump())

        result = BenchmarkResponse(
            hospital_name=metadata.get("hospital_name", ""),
            hospital_id=hospital_id,
            locality=locality or "national",
            benchmarks=benchmarks,
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("benchmark_rates failed")
        return json.dumps({"error": f"benchmark_rates failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
```

**Step 2: Verify server imports and tools register**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
from servers.price_transparency.server import mcp
for name in sorted(mcp._tool_manager._tools.keys()):
    print(f'  - {name}')
print(f'Total: {len(mcp._tool_manager._tools)} tools')
"`
Expected: 5 tools listed (search_mrf_index, get_negotiated_rates, compute_rate_dispersion, compare_rates_system, benchmark_rates).

**Step 3: Commit**

```bash
git add servers/price-transparency/server.py
git commit -m "feat(price-transparency): wire up all 5 tools in server.py"
```

---

### Task 7: Docker, MCP Registration, and Environment Config

**Files:**
- Modify: `docker-compose.yml`
- Modify: `.mcp.json`
- Modify: `.env.example` (if needed — no new env vars for this server)

**Step 1: Add service to docker-compose.yml**

Add after the `financial-intelligence` service block:

```yaml
  price-transparency:
    build: .
    command: python -m servers.price_transparency.server
    ports:
      - "8009:8009"
    environment:
      - MCP_TRANSPORT=streamable-http
      - MCP_PORT=8009
    volumes:
      - healthcare-cache:/root/.healthcare-data-mcp/cache
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8009),5); s.close()"]
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 30s
```

**Step 2: Add to .mcp.json**

Add entry:
```json
"price-transparency": {
    "type": "http",
    "url": "http://localhost:8009/mcp"
}
```

**Step 3: Verify server starts**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && MCP_TRANSPORT=streamable-http MCP_PORT=8009 timeout 8 python3 -m servers.price_transparency.server 2>&1 || true`
Expected: Uvicorn output showing server running on port 8009.

**Step 4: Test MCP initialize handshake**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && \
MCP_TRANSPORT=streamable-http MCP_PORT=8009 python3 -m servers.price_transparency.server &>/tmp/pt-server.log &
PT_PID=$!
sleep 3
curl -s -X POST http://localhost:8009/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' 2>&1
kill $PT_PID 2>/dev/null; wait $PT_PID 2>/dev/null
```
Expected: JSON response with `serverInfo.name: "price-transparency"`.

**Step 5: Commit**

```bash
git add docker-compose.yml .mcp.json
git commit -m "feat(price-transparency): add Docker, MCP registration, and env config"
```

---

### Task 8: Smoke Tests and API Validation

**Files:**
- Modify: `smoke_test.py`

**Context:** Add smoke tests for the price-transparency server. Test the CMS Provider Data Catalog API, CMS PFS API, and the registry/processor modules.

**Step 1: Add test functions to smoke_test.py**

```python
async def test_price_transparency():
    """Smoke test for price-transparency server."""
    print("\n=== Price Transparency / MRF Engine ===\n")

    # Test 1: CMS Provider Data Catalog lookup
    print("1. CMS Provider Data Catalog — searching for 'MAYO'...")
    from servers.price_transparency.mrf_registry import search_cms_providers
    providers = await search_cms_providers("MAYO")
    print(f"   Found {len(providers)} providers")
    if providers:
        p = providers[0]
        print(f"   First: {p.get('facility_name')} ({p.get('state')})")

    # Test 2: CMS Physician Fee Schedule lookup
    print("\n2. CMS PFS — looking up CPT 99213...")
    from servers.price_transparency.benchmark_client import get_pfs_rate, calculate_medicare_allowed
    pfs = await get_pfs_rate("99213")
    if pfs:
        print(f"   Work RVU: {pfs.get('rvu_work')}")
        print(f"   Total RVU (non-facility): {pfs.get('full_nfac_total')}")
        allowed = calculate_medicare_allowed(pfs)
        print(f"   Medicare allowed (national): ${allowed}")
    else:
        print("   PFS lookup returned None")

    # Test 3: Registry search
    print("\n3. MRF Registry — search and discovery...")
    from servers.price_transparency.mrf_registry import search_registry
    local = search_registry("test")
    print(f"   Local registry entries matching 'test': {len(local)}")

    # Test 4: MRF processor — cache status
    print("\n4. MRF Processor — cache status...")
    from servers.price_transparency.mrf_processor import get_all_cached_hospitals
    cached = get_all_cached_hospitals()
    print(f"   Cached hospitals: {len(cached)}")

    print("\n=== Price Transparency smoke test complete ===")
```

**Step 2: Run smoke test**

Run: `cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
import asyncio
import sys
sys.path.insert(0, '.')

async def main():
    from servers.price_transparency.mrf_registry import search_cms_providers
    providers = await search_cms_providers('MAYO')
    print(f'CMS Provider lookup: {len(providers)} results')

    from servers.price_transparency.benchmark_client import get_pfs_rate, calculate_medicare_allowed
    pfs = await get_pfs_rate('99213')
    if pfs:
        allowed = calculate_medicare_allowed(pfs)
        print(f'PFS 99213 Medicare allowed: \${allowed}')
    else:
        print('PFS lookup: no data')

asyncio.run(main())
"`
Expected: CMS Provider lookup returns results, PFS returns Medicare allowed amount for 99213.

**Step 3: Commit**

```bash
git add smoke_test.py
git commit -m "test(price-transparency): add smoke test for CMS provider, PFS, and MRF processor"
```

---

### Task 9: End-to-End Validation with Real MRF File

**Context:** This is the integration test — download a real hospital MRF CSV, parse it, cache as Parquet, and query rates. Uses a small hospital file to keep the test fast.

**Step 1: Test the full pipeline with a local MR-Explore CSV file**

Run:
```bash
cd "/mnt/d/Coding Projects/healthcare-data-mcp" && python3 -c "
import asyncio
from pathlib import Path
from servers.price_transparency import mrf_processor

# Use a local MR-Explore CSV (skip download, just parse + cache)
sample_dir = Path('/mnt/d/Coding Projects/MR-Explore/data_source')
csvs = sorted(sample_dir.glob('*.csv'))

if csvs:
    # Pick a smaller file
    smallest = min(csvs, key=lambda p: p.stat().st_size)
    print(f'Testing with: {smallest.name} ({smallest.stat().st_size / 1024 / 1024:.1f} MB)')

    # Parse
    df = mrf_processor.parse_csv_mrf(smallest)
    print(f'Parsed: {len(df)} rows, columns: {len(df.columns)}')
    print(f'Has negotiated_dollar: {\"negotiated_dollar\" in df.columns}')

    # Normalize to Parquet
    test_id = 'test_hospital'
    cache_dir = mrf_processor._hospital_cache_dir(test_id)
    metadata = mrf_processor.normalize_to_parquet(df, 'Test Hospital', cache_dir)
    print(f'Parquet cached: {metadata.get(\"row_count\")} rows, {metadata.get(\"payer_count\")} payers')

    # Query rates
    rates = mrf_processor.get_rates(test_id, ['99213', '99214', '27447'])
    print(f'Rates for 99213/99214/27447: {len(rates)} results')
    for r in rates[:3]:
        print(f'  {r[\"cpt_code\"]} | {r[\"payer_name\"][:30]} | \${r[\"negotiated_dollar\"]}')

    # Rate stats
    stats = mrf_processor.get_rate_stats(test_id, ['99213'])
    if stats:
        s = stats[0]
        print(f'99213 dispersion: min=\${s[\"min_rate\"]}, max=\${s[\"max_rate\"]}, median=\${s[\"median_rate\"]}, payers={s[\"payer_count\"]}')

    # Clean up test data
    import shutil
    shutil.rmtree(cache_dir, ignore_errors=True)
    print('Test cache cleaned up.')
else:
    print('No sample CSV files found in MR-Explore/data_source/')
"
```
Expected: Full pipeline works — parse → normalize → query → stats.

**Step 2: Fix any issues found**

If the test reveals issues (column mapping mismatches, DuckDB query errors, Pydantic validation), fix them in the relevant module.

**Step 3: Commit any fixes**

```bash
git add -u
git commit -m "fix(price-transparency): address issues found in e2e validation"
```

---

### Task 10: Final Verification

**Step 1: Verify all tools register**

Run: `python3 -c "from servers.price_transparency.server import mcp; print(len(mcp._tool_manager._tools), 'tools')"`
Expected: `5 tools`

**Step 2: Verify server starts on port 8009**

Run: `MCP_TRANSPORT=streamable-http MCP_PORT=8009 timeout 8 python3 -m servers.price_transparency.server 2>&1 || true`
Expected: Uvicorn running on 0.0.0.0:8009.

**Step 3: Verify MCP handshake**

Start server, send initialize request with proper Accept headers, verify `serverInfo.name: "price-transparency"` in response.

**Step 4: Verify .mcp.json and docker-compose.yml are correct**

Read both files and confirm price-transparency entries exist.

**Step 5: Commit final state**

Only if changes were needed.
