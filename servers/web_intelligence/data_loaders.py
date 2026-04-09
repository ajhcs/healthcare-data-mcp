"""Data loaders for web-intelligence server.

Manages:
1. CMS Promoting Interoperability CSV -> Parquet cache (EHR vendor detection)
2. Static GPO directory lookup from bundled CSV
3. SHA256-keyed API/page response cache
"""

import csv
import hashlib
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

# Ensure shared utils are importable
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from shared.utils.cache import is_cache_valid  # noqa: E402
from shared.utils.duckdb_helpers import get_connection  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache configuration
# ---------------------------------------------------------------------------

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "web-intelligence"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_PI_TTL_DAYS = 90
_SEARCH_TTL_DAYS = 30
_NEWS_TTL_DAYS = 7
_EXEC_TTL_DAYS = 90
_PAGE_TTL_DAYS = 30

# CMS Promoting Interoperability bulk CSV
PI_URL = (
    "https://data.cms.gov/provider-data/sites/default/files/resources/"
    "5462b19a756c53c1becccf13787d9157_1770163678/Promoting_Interoperability-Hospital.csv"
)
_PI_PARQUET = _CACHE_DIR / "pi_hospital.parquet"

# Static data paths
_GPO_CSV = Path(__file__).parent / "data" / "gpo_directory.csv"

# ---------------------------------------------------------------------------
# CEHRT ID prefix -> vendor name (common vendors covering ~95% of hospitals)
# Used as last-resort fallback when PI data lacks ehr_developer column.
# ---------------------------------------------------------------------------

VENDOR_KEYWORDS: dict[str, str] = {
    "epic": "Epic Systems",
    "cerner": "Oracle Health (Cerner)",
    "oracle health": "Oracle Health (Cerner)",
    "oracle cerner": "Oracle Health (Cerner)",
    "meditech": "MEDITECH",
    "altera": "Altera Digital Health",
    "allscripts": "Altera Digital Health",
    "athenahealth": "athenahealth",
    "athena": "athenahealth",
    "cpsi": "CPSI (TruBridge)",
    "trubridge": "CPSI (TruBridge)",
    "veradigm": "Veradigm",
    "nextgen": "NextGen Healthcare",
    "eclinicalworks": "eClinicalWorks",
}

# ---------------------------------------------------------------------------
# TTL helpers
# ---------------------------------------------------------------------------


def _is_cache_valid(path: Path, ttl_days: int) -> bool:
    """Check if a cached file exists and is within TTL."""
    return is_cache_valid(path, max_age_days=ttl_days)


# ---------------------------------------------------------------------------
# DuckDB helpers
# ---------------------------------------------------------------------------


def _get_con(parquet_path: Path, view_name: str = "data") -> duckdb.DuckDBPyConnection | None:
    """Create DuckDB in-memory connection with a view over a Parquet file."""
    return get_connection(parquet_path, view_name=view_name)


def _s(row: dict, col: str | None) -> str:
    """Safe string extraction."""
    if not col or col not in row:
        return ""
    v = row.get(col)
    return str(v).strip() if v is not None else ""


def _detect_columns(con: duckdb.DuckDBPyConnection, view_name: str = "data") -> list[str]:
    return [
        r[0]
        for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{view_name}'"
        ).fetchall()
    ]


def _find_col(cols: list[str], candidates: list[str]) -> str | None:
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


# ============================================================
# Promoting Interoperability data (EHR vendor detection)
# ============================================================


async def ensure_pi_cached() -> bool:
    """Download CMS PI CSV and cache as Parquet. Returns True if available."""
    if _is_cache_valid(_PI_PARQUET, _PI_TTL_DAYS):
        return True

    logger.info("Downloading CMS PI file for EHR detection ...")
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(PI_URL)
            resp.raise_for_status()

        csv_path = _CACHE_DIR / "pi_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(
            csv_path, dtype=str, keep_default_na=False,
            low_memory=False, encoding_errors="replace",
        )
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_PI_PARQUET, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("PI cached: %d records -> %s", len(df), _PI_PARQUET.name)
        return True
    except Exception as e:
        logger.warning("Failed to download CMS PI file: %s", e)
        return False


def query_pi_for_ehr(
    facility_name: str = "",
    ccn: str = "",
    state: str = "",
) -> list[dict]:
    """Query PI data for EHR vendor information.

    Returns list of dicts with facility_name, ccn, ehr_developer, ehr_product_name, cehrt_id.
    """
    con = _get_con(_PI_PARQUET, "data")
    if con is None:
        return []

    try:
        cols = _detect_columns(con)

        ccn_col = _find_col(cols, ["facility_id", "ccn", "provider_id", "provider_number"])
        name_col = _find_col(cols, ["facility_name", "hospital_name", "provider_name"])
        state_col = _find_col(cols, ["state", "state_cd", "provider_state"])
        cehrt_col = _find_col(cols, ["cehrt_id", "ehr_certification_id"])
        ehr_product_col = _find_col(cols, ["ehr_product_name", "ehr_product", "product_name"])
        ehr_dev_col = _find_col(cols, ["ehr_developer", "ehr_vendor", "developer"])

        where_parts: list[str] = []
        params: list[str] = []

        if ccn and ccn_col:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        if facility_name and name_col:
            where_parts.append(f"{name_col} ILIKE ?")
            params.append(f"%{facility_name.strip()}%")
        if state and state_col:
            where_parts.append(f"UPPER(TRIM({state_col})) = ?")
            params.append(state.strip().upper())

        if not where_parts:
            return []

        where = " AND ".join(where_parts)
        sql = f"SELECT * FROM data WHERE {where} LIMIT 50"
        rows = con.execute(sql, params).fetchdf()

        results: list[dict] = []
        for _, row in rows.iterrows():
            r = row.to_dict()
            results.append({
                "facility_name": _s(r, name_col),
                "ccn": _s(r, ccn_col),
                "ehr_developer": _s(r, ehr_dev_col),
                "ehr_product_name": _s(r, ehr_product_col),
                "cehrt_id": _s(r, cehrt_col),
            })

        return results
    except Exception as e:
        logger.warning("PI EHR query failed: %s", e)
        return []
    finally:
        con.close()


def resolve_vendor_name(raw_developer: str) -> str:
    """Normalize a raw EHR developer string to a standard vendor name.

    E.g. "Epic Systems Corporation" -> "Epic Systems".
    """
    lower = raw_developer.lower()
    for keyword, canonical in VENDOR_KEYWORDS.items():
        if keyword in lower:
            return canonical
    return raw_developer


# ============================================================
# GPO directory lookup
# ============================================================


def load_gpo_directory() -> list[dict]:
    """Load the bundled GPO directory CSV.

    Returns list of dicts with gpo_name, aliases (comma-separated), gpo_type.
    """
    if not _GPO_CSV.exists():
        logger.warning("GPO directory CSV not found at %s", _GPO_CSV)
        return []
    results = []
    with open(_GPO_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            results.append({
                "gpo_name": row.get("gpo_name", "").strip(),
                "aliases": row.get("aliases", "").strip(),
                "gpo_type": row.get("gpo_type", "").strip(),
            })
    return results


def match_gpo_in_text(text: str, gpo_list: list[dict]) -> list[dict]:
    """Check if any GPO names or aliases appear in the given text.

    Returns list of matched GPO dicts.
    """
    lower = text.lower()
    matches = []
    for gpo in gpo_list:
        names_to_check = [gpo["gpo_name"]]
        if gpo["aliases"]:
            names_to_check.extend(a.strip() for a in gpo["aliases"].split(","))
        for name in names_to_check:
            if name.lower() in lower:
                matches.append(gpo)
                break
    return matches


# ============================================================
# API / page response cache
# ============================================================


def _api_cache_path(prefix: str, params: dict) -> Path:
    param_str = json.dumps(params, sort_keys=True, default=str)
    h = hashlib.sha256(param_str.encode()).hexdigest()[:16]
    return _CACHE_DIR / f"api_{prefix}_{h}.json"


def cache_response(prefix: str, params: dict, data: dict | list) -> None:
    """Save a response to the cache."""
    path = _api_cache_path(prefix, params)
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "params": params,
        "data": data,
    }
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")


def load_cached_response(prefix: str, params: dict, ttl_days: int) -> dict | list | None:
    """Load a cached response if within TTL. Returns data or None."""
    path = _api_cache_path(prefix, params)
    if not _is_cache_valid(path, ttl_days):
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload.get("data")
    except Exception as e:
        logger.warning("Failed to load cache %s: %s", path.name, e)
        return None
