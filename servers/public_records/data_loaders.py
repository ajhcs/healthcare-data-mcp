"""Bulk data loaders and manual-seed file handlers for public-records server.

Downloads CMS Provider-of-Services and Promoting Interoperability CSVs,
converts to Parquet with zstd compression, and queries with DuckDB.
Also handles manually-seeded 340B covered-entity JSON and HIPAA breach CSV.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx

from shared.utils.http_client import resilient_request, get_client
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache configuration
# ---------------------------------------------------------------------------

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "public-records"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_BULK_TTL_DAYS = 90       # TTL for bulk CMS CSV-to-Parquet files
_API_TTL_DAYS = 7          # TTL for API response cache

# ---------------------------------------------------------------------------
# CMS bulk download URLs
# ---------------------------------------------------------------------------

POS_URL = (
    "https://data.cms.gov/sites/default/files/2026-01/"
    "c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv"
)
PI_URL = (
    "https://data.cms.gov/provider-data/sites/default/files/resources/"
    "5462b19a756c53c1becccf13787d9157_1770163678/Promoting_Interoperability-Hospital.csv"
)

# Parquet cache paths
_POS_PARQUET = _CACHE_DIR / "pos_q4_2025.parquet"
_PI_PARQUET = _CACHE_DIR / "pi_hospital.parquet"
_340B_PARQUET = _CACHE_DIR / "340b_covered_entities.parquet"
_BREACH_PARQUET = _CACHE_DIR / "hipaa_breaches.parquet"

# Manual-seed source files (user drops these into the cache dir)
_340B_JSON = _CACHE_DIR / "340b_covered_entities.json"
_BREACH_CSV = _CACHE_DIR / "hipaa_breaches.csv"


# ---------------------------------------------------------------------------
# TTL helpers
# ---------------------------------------------------------------------------

def _is_cache_valid(path: Path, ttl_days: int) -> bool:
    """Check if a cached file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


# ---------------------------------------------------------------------------
# DuckDB connection helper
# ---------------------------------------------------------------------------

def _get_con(parquet_path: Path, view_name: str = "data") -> duckdb.DuckDBPyConnection | None:
    """Create DuckDB in-memory connection with a view over the Parquet file.

    If the Parquet file is corrupted, deletes it and returns None so the
    caller can trigger a re-download on the next request.
    """
    if not parquet_path.exists():
        return None
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        return con
    except Exception:
        logger.warning("Corrupt Parquet cache, deleting: %s", parquet_path)
        con.close()
        parquet_path.unlink(missing_ok=True)
        return None


# ---------------------------------------------------------------------------
# Safe row extraction helpers
# ---------------------------------------------------------------------------

def _s(row: dict, col: str | None) -> str:
    """Safe string extraction from a dict row."""
    if not col or col not in row:
        return ""
    v = row.get(col)
    return str(v).strip() if v is not None else ""


def _i(row: dict, col: str | None) -> int:
    """Safe int extraction from a dict row."""
    v = _s(row, col)
    try:
        return int(float(v)) if v else 0
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# Dynamic column detection
# ---------------------------------------------------------------------------

def _detect_columns(con: duckdb.DuckDBPyConnection, view_name: str = "data") -> list[str]:
    """Return list of column names in the view."""
    return [
        r[0]
        for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{view_name}'"
        ).fetchall()
    ]


def _find_col(cols: list[str], candidates: list[str]) -> str | None:
    """Find the first matching column name from a list of candidates."""
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None


# ============================================================
# Auto-download functions (CMS bulk CSV -> Parquet)
# ============================================================

async def ensure_pos_cached() -> bool:
    """Download CMS Provider of Services CSV and cache as Parquet.

    Returns True if the Parquet file is available for queries.
    """
    if _is_cache_valid(_POS_PARQUET, _BULK_TTL_DAYS):
        return True

    logger.info("Downloading CMS POS file from %s ...", POS_URL[:80])
    try:
        resp = await resilient_request("GET", POS_URL, timeout=300.0)

        csv_path = _CACHE_DIR / "pos_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(
            csv_path, dtype=str, keep_default_na=False,
            low_memory=False, encoding_errors="replace",
        )
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_POS_PARQUET, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("POS cached: %d records -> %s", len(df), _POS_PARQUET.name)
        return True

    except Exception as e:
        logger.warning("Failed to download CMS POS file: %s", e)
        return False


async def ensure_pi_cached() -> bool:
    """Download CMS Promoting Interoperability CSV and cache as Parquet.

    Returns True if the Parquet file is available for queries.
    """
    if _is_cache_valid(_PI_PARQUET, _BULK_TTL_DAYS):
        return True

    logger.info("Downloading CMS PI file from %s ...", PI_URL[:80])
    try:
        resp = await resilient_request("GET", PI_URL, timeout=300.0)

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


# ============================================================
# Manual-seed file handlers
# ============================================================

def ensure_340b_loaded() -> bool:
    """Check for 340b_covered_entities.json in cache dir, convert to Parquet.

    The JSON is nested (all data per 340B ID grouped together), so we
    flatten to records before saving.

    Returns True if the Parquet file is available for queries.
    """
    if _is_cache_valid(_340B_PARQUET, _BULK_TTL_DAYS):
        return True

    if not _340B_JSON.exists():
        logger.warning(
            "340B seed file not found at %s — place the JSON file there "
            "to enable get_340b_status queries.",
            _340B_JSON,
        )
        return False

    logger.info("Converting 340B JSON to Parquet ...")
    try:
        with open(_340B_JSON, encoding="utf-8") as f:
            raw = json.load(f)

        # Flatten nested structure to list of flat records
        records: list[dict] = []
        if isinstance(raw, list):
            # Already a list of records
            for item in raw:
                if isinstance(item, dict):
                    records.append(_flatten_dict(item))
        elif isinstance(raw, dict):
            # Keyed by entity ID — flatten each entry
            for _key, value in raw.items():
                if isinstance(value, dict):
                    flat = _flatten_dict(value)
                    if "entity_id" not in flat or not flat["entity_id"]:
                        flat["entity_id"] = str(_key)
                    records.append(flat)
                elif isinstance(value, list):
                    for sub in value:
                        if isinstance(sub, dict):
                            flat = _flatten_dict(sub)
                            if "entity_id" not in flat or not flat["entity_id"]:
                                flat["entity_id"] = str(_key)
                            records.append(flat)

        if not records:
            logger.warning("340B JSON produced zero records after flattening")
            return False

        df = pd.DataFrame(records).astype(str)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_340B_PARQUET, compression="zstd", index=False)

        logger.info("340B cached: %d records -> %s", len(df), _340B_PARQUET.name)
        return True

    except Exception as e:
        logger.warning("Failed to process 340B JSON: %s", e)
        return False


def _flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    """Recursively flatten a nested dict into a single-level dict."""
    items: list[tuple[str, object]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep).items())
        elif isinstance(v, list):
            # Convert lists to JSON strings for Parquet compatibility
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def ensure_breach_loaded() -> bool:
    """Check for hipaa_breaches.csv in cache dir, convert to Parquet.

    Returns True if the Parquet file is available for queries.
    """
    if _is_cache_valid(_BREACH_PARQUET, _BULK_TTL_DAYS):
        return True

    if not _BREACH_CSV.exists():
        logger.warning(
            "HIPAA breach seed file not found at %s — place the CSV file "
            "there to enable get_breach_history queries.",
            _BREACH_CSV,
        )
        return False

    logger.info("Converting HIPAA breach CSV to Parquet ...")
    try:
        df = pd.read_csv(
            _BREACH_CSV, dtype=str, keep_default_na=False,
            low_memory=False, encoding_errors="replace",
        )
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_BREACH_PARQUET, compression="zstd", index=False)

        logger.info("Breach data cached: %d records -> %s", len(df), _BREACH_PARQUET.name)
        return True

    except Exception as e:
        logger.warning("Failed to process HIPAA breach CSV: %s", e)
        return False


# ============================================================
# Query functions (DuckDB on Parquet)
# ============================================================

def query_pos(
    ccn: str = "",
    provider_name: str = "",
    state: str = "",
) -> list[dict]:
    """Query the CMS Provider of Services Parquet for accreditation data.

    Uses dynamic column detection since CMS uses abbreviated names like
    prvdr_num, fac_name, state_cd, etc.
    """
    con = _get_con(_POS_PARQUET, "data")
    if con is None:
        return []

    try:
        cols = _detect_columns(con)

        # Map logical names to actual column names
        ccn_col = _find_col(cols, ["prvdr_num", "provider_number", "ccn", "provider_id"])
        name_col = _find_col(cols, ["fac_name", "facility_name", "provider_name", "hospital_name"])
        state_col = _find_col(cols, ["state_cd", "state", "prvdr_state_cd", "provider_state"])
        city_col = _find_col(cols, ["city", "prvdr_city", "facility_city"])
        accrd_type_col = _find_col(cols, ["acrdtn_type_cd", "accreditation_type_code", "accreditation_type"])
        accrd_eff_col = _find_col(cols, ["acrdtn_efctv_dt", "accreditation_effective_date"])
        accrd_exp_col = _find_col(cols, ["acrdtn_exprtn_dt", "accreditation_expiration_date"])
        cert_col = _find_col(cols, ["crtfctn_dt", "certification_date"])
        owner_col = _find_col(cols, ["gnrl_cntl_type_cd", "ownership_type", "ownership_type_code"])
        bed_col = _find_col(cols, ["bed_cnt", "bed_count", "number_of_beds", "beds"])
        pgm_col = _find_col(cols, ["pgm_prtcptn_cd", "program_participation_code", "medicare_medicaid"])
        cmplnc_col = _find_col(cols, ["cmplnc_stus_cd", "compliance_status", "compliance_status_code"])

        where_parts: list[str] = []
        params: list[str] = []

        if ccn and ccn_col:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        if provider_name and name_col:
            where_parts.append(f"{name_col} ILIKE ?")
            params.append(f"%{provider_name.strip()}%")
        if state and state_col:
            where_parts.append(f"UPPER(TRIM({state_col})) = ?")
            params.append(state.strip().upper())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT * FROM data WHERE {where} LIMIT 500"
        rows = con.execute(sql, params).fetchdf()

        results: list[dict] = []
        for _, row in rows.iterrows():
            r = row.to_dict()
            results.append({
                "ccn": _s(r, ccn_col),
                "provider_name": _s(r, name_col),
                "state": _s(r, state_col),
                "city": _s(r, city_col),
                "accreditation_type_code": _s(r, accrd_type_col),
                "accreditation_effective_date": _s(r, accrd_eff_col),
                "accreditation_expiration_date": _s(r, accrd_exp_col),
                "certification_date": _s(r, cert_col),
                "ownership_type": _s(r, owner_col),
                "bed_count": _i(r, bed_col),
                "medicare_medicaid": _s(r, pgm_col),
                "compliance_status": _s(r, cmplnc_col),
            })

        return results

    except Exception as e:
        logger.warning("POS query failed: %s", e)
        return []
    finally:
        con.close()


def query_pi(
    ccn: str = "",
    facility_name: str = "",
    state: str = "",
) -> list[dict]:
    """Query the CMS Promoting Interoperability Parquet.

    Known columns: facility_id, facility_name, state, city/town, cehrt_id,
    meets_criteria_for_promoting_interoperability_of_ehrs, start_date, end_date.
    """
    con = _get_con(_PI_PARQUET, "data")
    if con is None:
        return []

    try:
        cols = _detect_columns(con)

        ccn_col = _find_col(cols, ["facility_id", "ccn", "provider_id", "provider_number"])
        name_col = _find_col(cols, ["facility_name", "hospital_name", "provider_name"])
        state_col = _find_col(cols, ["state", "state_cd", "provider_state"])
        city_col = _find_col(cols, ["city/town", "city", "facility_city"])
        cehrt_col = _find_col(cols, ["cehrt_id", "ehr_certification_id"])
        meets_col = _find_col(cols, [
            "meets_criteria_for_promoting_interoperability_of_ehrs",
            "meets_pi_criteria", "pi_criteria",
        ])
        start_col = _find_col(cols, ["start_date", "reporting_period_start", "period_start"])
        end_col = _find_col(cols, ["end_date", "reporting_period_end", "period_end"])
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

        where = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT * FROM data WHERE {where} LIMIT 500"
        rows = con.execute(sql, params).fetchdf()

        results: list[dict] = []
        for _, row in rows.iterrows():
            r = row.to_dict()
            results.append({
                "facility_name": _s(r, name_col),
                "ccn": _s(r, ccn_col),
                "state": _s(r, state_col),
                "city": _s(r, city_col),
                "meets_pi_criteria": _s(r, meets_col),
                "cehrt_id": _s(r, cehrt_col),
                "reporting_period_start": _s(r, start_col),
                "reporting_period_end": _s(r, end_col),
                "ehr_product_name": _s(r, ehr_product_col),
                "ehr_developer": _s(r, ehr_dev_col),
            })

        return results

    except Exception as e:
        logger.warning("PI query failed: %s", e)
        return []
    finally:
        con.close()


def query_340b(
    entity_name: str = "",
    entity_id: str = "",
    state: str = "",
) -> list[dict]:
    """Query the 340B covered-entities Parquet.

    Uses dynamic column detection since the JSON schema may vary.
    """
    con = _get_con(_340B_PARQUET, "data")
    if con is None:
        return []

    try:
        cols = _detect_columns(con)

        id_col = _find_col(cols, ["entity_id", "id", "340b_id", "covered_entity_id"])
        name_col = _find_col(cols, ["entity_name", "name", "covered_entity_name", "organization_name"])
        state_col = _find_col(cols, ["state", "state_code", "entity_state"])
        type_col = _find_col(cols, ["entity_type", "type", "covered_entity_type"])
        addr_col = _find_col(cols, ["address", "street_address", "address_line_1"])
        city_col = _find_col(cols, ["city", "entity_city"])
        zip_col = _find_col(cols, ["zip_code", "zip", "postal_code"])
        grant_col = _find_col(cols, ["grant_number", "grant_num", "grant_id"])
        participating_col = _find_col(cols, ["participating", "active", "status"])
        pharmacy_col = _find_col(cols, [
            "contract_pharmacy_count", "pharmacy_count",
            "num_contract_pharmacies", "contract_pharmacies",
        ])

        where_parts: list[str] = []
        params: list[str] = []

        if entity_id and id_col:
            where_parts.append(f"TRIM({id_col}) = ?")
            params.append(entity_id.strip())
        if entity_name and name_col:
            where_parts.append(f"{name_col} ILIKE ?")
            params.append(f"%{entity_name.strip()}%")
        if state and state_col:
            where_parts.append(f"UPPER(TRIM({state_col})) = ?")
            params.append(state.strip().upper())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT * FROM data WHERE {where} LIMIT 500"
        rows = con.execute(sql, params).fetchdf()

        results: list[dict] = []
        for _, row in rows.iterrows():
            r = row.to_dict()
            part_val = _s(r, participating_col).lower()
            is_participating = part_val not in ("false", "0", "no", "inactive", "n")

            results.append({
                "entity_id": _s(r, id_col),
                "entity_name": _s(r, name_col),
                "entity_type": _s(r, type_col),
                "address": _s(r, addr_col),
                "city": _s(r, city_col),
                "state": _s(r, state_col),
                "zip_code": _s(r, zip_col),
                "grant_number": _s(r, grant_col),
                "participating": is_participating,
                "contract_pharmacy_count": _i(r, pharmacy_col),
            })

        return results

    except Exception as e:
        logger.warning("340B query failed: %s", e)
        return []
    finally:
        con.close()


def query_breaches(
    entity_name: str = "",
    state: str = "",
    min_individuals: int = 0,
) -> list[dict]:
    """Query the HIPAA breach Parquet.

    Uses dynamic column detection and TRY_CAST for the individuals filter.
    """
    con = _get_con(_BREACH_PARQUET, "data")
    if con is None:
        return []

    try:
        cols = _detect_columns(con)

        name_col = _find_col(cols, [
            "name_of_covered_entity", "entity_name", "covered_entity",
            "organization_name", "name",
        ])
        state_col = _find_col(cols, ["state", "state_code", "entity_state"])
        type_col = _find_col(cols, [
            "covered_entity_type", "entity_type", "type",
        ])
        individuals_col = _find_col(cols, [
            "individuals_affected", "individuals_notified",
            "number_of_individuals_affected", "affected_individuals",
        ])
        date_col = _find_col(cols, [
            "breach_submission_date", "submission_date", "date_of_breach",
            "report_date", "date_submitted",
        ])
        breach_type_col = _find_col(cols, [
            "type_of_breach", "breach_type", "breach_category",
        ])
        location_col = _find_col(cols, [
            "location_of_breached_information", "location_of_breached_info",
            "breach_location", "location",
        ])
        ba_col = _find_col(cols, [
            "business_associate_present", "business_associate_involved",
            "business_associate",
        ])
        desc_col = _find_col(cols, [
            "web_description", "description", "summary",
            "breach_description",
        ])

        where_parts: list[str] = []
        params: list[object] = []

        if entity_name and name_col:
            where_parts.append(f"{name_col} ILIKE ?")
            params.append(f"%{entity_name.strip()}%")
        if state and state_col:
            where_parts.append(f"UPPER(TRIM({state_col})) = ?")
            params.append(state.strip().upper())
        if min_individuals > 0 and individuals_col:
            where_parts.append(
                f"TRY_CAST({individuals_col} AS INTEGER) >= ?"
            )
            params.append(min_individuals)

        where = " AND ".join(where_parts) if where_parts else "1=1"
        sql = f"SELECT * FROM data WHERE {where} LIMIT 500"
        rows = con.execute(sql, params).fetchdf()

        results: list[dict] = []
        for _, row in rows.iterrows():
            r = row.to_dict()
            results.append({
                "entity_name": _s(r, name_col),
                "state": _s(r, state_col),
                "covered_entity_type": _s(r, type_col),
                "individuals_affected": _i(r, individuals_col),
                "breach_submission_date": _s(r, date_col),
                "breach_type": _s(r, breach_type_col),
                "location_of_breached_info": _s(r, location_col),
                "business_associate_present": _s(r, ba_col),
                "web_description": _s(r, desc_col),
            })

        return results

    except Exception as e:
        logger.warning("Breach query failed: %s", e)
        return []
    finally:
        con.close()


# ============================================================
# API response caching
# ============================================================

def _api_cache_path(prefix: str, params: dict) -> Path:
    """Build a cache file path from a prefix and hashed params."""
    param_str = json.dumps(params, sort_keys=True, default=str)
    h = hashlib.sha256(param_str.encode()).hexdigest()[:16]
    return _CACHE_DIR / f"api_{prefix}_{h}.json"


def cache_api_response(prefix: str, params: dict, data: dict | list) -> None:
    """Save an API response to the cache, keyed by SHA256 hash of params."""
    path = _api_cache_path(prefix, params)
    payload = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "params": params,
        "data": data,
    }
    path.write_text(json.dumps(payload, default=str), encoding="utf-8")
    logger.debug("Cached API response: %s", path.name)


def load_cached_api_response(prefix: str, params: dict) -> dict | list | None:
    """Load a cached API response if within the 7-day TTL.

    Returns the cached data or None if expired/missing.
    """
    path = _api_cache_path(prefix, params)
    if not _is_cache_valid(path, _API_TTL_DAYS):
        return None

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload.get("data")
    except Exception as e:
        logger.warning("Failed to load cached API response %s: %s", path.name, e)
        return None
