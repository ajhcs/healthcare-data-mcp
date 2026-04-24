"""Bulk data loaders and manual-seed file handlers for public-records server.

Downloads CMS Provider-of-Services and Promoting Interoperability CSVs,
converts to Parquet with zstd compression, and queries with DuckDB.
Also handles manually-seeded 340B covered-entity JSON and HIPAA breach CSV.
"""

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from shared.utils.identity import (
    conservative_fuzzy_score,
    normalize_name,
    normalize_npi,
    normalize_state,
)
from shared.utils.duckdb_safe import safe_parquet_sql

from shared.utils.http_client import resilient_request
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache configuration
# ---------------------------------------------------------------------------

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "public-records"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_BULK_TTL_DAYS = 90       # TTL for bulk CMS CSV-to-Parquet files
_API_TTL_DAYS = 7          # TTL for API response cache
_LEIE_TTL_DAYS = 31
_LEIE_STALE_MAX_DAYS = 45

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
_LEIE_PARQUET = _CACHE_DIR / "leie_current.parquet"
_LEIE_META = _CACHE_DIR / "leie_current.meta.json"
_LEIE_CSV = _CACHE_DIR / "leie_current.csv"

# Manual-seed source files (user drops these into the cache dir)
_340B_JSON = _CACHE_DIR / "340b_covered_entities.json"
_BREACH_CSV = _CACHE_DIR / "hipaa_breaches.csv"

# ---------------------------------------------------------------------------
# HHS OIG LEIE source configuration
# ---------------------------------------------------------------------------

LEIE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
LEIE_LANDING_PAGE_URL = "https://oig.hhs.gov/exclusions/exclusions_list.asp"
LEIE_RECORD_LAYOUT_URL = "https://www.oig.hhs.gov/exclusions/files/leie_record_layout.pdf"
LEIE_LAYOUT_COLUMNS = [
    "LASTNAME",
    "FIRSTNAME",
    "MIDNAME",
    "BUSNAME",
    "GENERAL",
    "SPECIALTY",
    "UPIN",
    "NPI",
    "DOB",
    "ADDRESS",
    "CITY",
    "STATE",
    "ZIP",
    "EXCLTYPE",
    "EXCLDATE",
    "REINDATE",
    "WAIVERDATE",
    "WVRSTATE",
]
_LEIE_COLUMN_MAP = {
    "LASTNAME": "last_name",
    "FIRSTNAME": "first_name",
    "MIDNAME": "middle_name",
    "BUSNAME": "business_name",
    "GENERAL": "general_category",
    "SPECIALTY": "specialty",
    "UPIN": "upin",
    "NPI": "npi",
    "DOB": "dob",
    "ADDRESS": "address",
    "CITY": "city",
    "STATE": "state",
    "ZIP": "zip_code",
    "EXCLTYPE": "exclusion_type",
    "EXCLDATE": "exclusion_date",
    "REINDATE": "reinstatement_date",
    "WAIVERDATE": "waiver_date",
    "WVRSTATE": "waiver_state",
}

_SENSITIVE_IDENTIFIER_KEYS = {
    "ssn",
    "social_security_number",
    "social_security_num",
    "social_security",
    "ein",
    "fein",
    "tin",
    "tax_id",
    "tax_identifier",
    "taxpayer_id",
    "taxpayer_identifier",
    "taxpayer_identification_number",
    "employer_identification_number",
    "federal_tax_id",
    "federal_tax_identifier",
}


# ---------------------------------------------------------------------------
# TTL helpers
# ---------------------------------------------------------------------------

def _is_cache_valid(path: Path, ttl_days: int) -> bool:
    """Check if a cached file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


def _cache_age_days(path: Path) -> float | None:
    """Return a cached file's age in days, or None if it does not exist."""
    if not path.exists():
        return None
    age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return round(age_seconds / 86400, 3)


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception as e:
        logger.warning("Failed to read JSON cache metadata %s: %s", path.name, e)
        return {}


def _write_dataframe_parquet(
    df: pd.DataFrame,
    path: Path,
    *,
    compression: str = "zstd",
) -> None:
    """Write Parquet with pandas, falling back to DuckDB when no pandas engine is installed."""
    try:
        df.to_parquet(path, compression=compression, index=False)
        return
    except ImportError:
        logger.info("pandas Parquet engine unavailable; writing %s with DuckDB", path.name)

    con = duckdb.connect(":memory:")
    try:
        con.register("df_to_write", df)
        con.execute(
            "COPY df_to_write TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(path)],
        )
    finally:
        con.close()


def _read_parquet_dataframe(path: Path) -> pd.DataFrame:
    """Read Parquet with pandas, falling back to DuckDB when no pandas engine is installed."""
    try:
        return pd.read_parquet(path)
    except ImportError:
        logger.info("pandas Parquet engine unavailable; reading %s with DuckDB", path.name)

    con = duckdb.connect(":memory:")
    try:
        return con.execute("SELECT * FROM read_parquet(?)", [str(path)]).fetchdf()
    finally:
        con.close()


def _normalized_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _has_sensitive_identifier_key(payload: dict) -> bool:
    return bool(_SENSITIVE_IDENTIFIER_KEYS & {_normalized_key(key) for key in payload})


def _normalize_leie_date(value: object) -> str:
    """Normalize LEIE eight-digit dates to ISO strings, preserving blanks."""
    digits = re.sub(r"\D+", "", "" if value is None else str(value).strip())
    if not digits or digits in {"00000000", "99999999"}:
        return ""
    if len(digits) != 8:
        return str(value).strip()

    for fmt in ("%Y%m%d", "%m%d%Y"):
        try:
            return datetime.strptime(digits, fmt).date().isoformat()
        except ValueError:
            continue
    return digits


def _source_metadata_base() -> dict:
    return {
        "source_name": "HHS OIG LEIE",
        "source_url": LEIE_URL,
        "landing_page_url": LEIE_LANDING_PAGE_URL,
        "record_layout_url": LEIE_RECORD_LAYOUT_URL,
        "downloaded_at": "",
        "source_last_modified": "",
        "source_etag": "",
        "record_count": 0,
        "cache_path": str(_LEIE_PARQUET),
        "csv_path": str(_LEIE_CSV),
        "cache_status": "missing",
        "cache_age_days": _cache_age_days(_LEIE_PARQUET),
        "layout_columns": LEIE_LAYOUT_COLUMNS,
        "last_error": "",
    }


def _leie_metadata(cache_status: str | None = None, **updates: object) -> dict:
    meta = _source_metadata_base()
    meta.update(_read_json(_LEIE_META))
    meta["cache_path"] = str(_LEIE_PARQUET)
    meta["csv_path"] = str(_LEIE_CSV)
    meta["layout_columns"] = LEIE_LAYOUT_COLUMNS
    meta["cache_age_days"] = _cache_age_days(_LEIE_PARQUET)
    if cache_status is not None:
        meta["cache_status"] = cache_status
    meta.update({k: v for k, v in updates.items() if v is not None})
    return meta


def _write_leie_metadata(meta: dict) -> dict:
    payload = _leie_metadata(**meta)
    _LEIE_META.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def _leie_cache_is_younger_than(days: int) -> bool:
    age = _cache_age_days(_LEIE_PARQUET)
    return age is not None and age < days


def _leie_download_is_older_than(days: int) -> bool:
    meta = _read_json(_LEIE_META)
    downloaded_at = _parse_iso_datetime(str(meta.get("downloaded_at", "")))
    if downloaded_at is None:
        return True
    if downloaded_at.tzinfo is None:
        downloaded_at = downloaded_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - downloaded_at).days >= days


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
            f"CREATE VIEW {view_name} AS SELECT * FROM {safe_parquet_sql(parquet_path)}"
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
        _write_dataframe_parquet(df, _POS_PARQUET, compression="zstd")

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
        _write_dataframe_parquet(df, _PI_PARQUET, compression="zstd")

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
        _write_dataframe_parquet(df, _340B_PARQUET, compression="zstd")

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
        _write_dataframe_parquet(df, _BREACH_PARQUET, compression="zstd")

        logger.info("Breach data cached: %d records -> %s", len(df), _BREACH_PARQUET.name)
        return True

    except Exception as e:
        logger.warning("Failed to process HIPAA breach CSV: %s", e)
        return False


# ============================================================
# HHS OIG LEIE cache, parsing, and query helpers
# ============================================================

async def ensure_leie_cached(force_refresh: bool = False) -> dict:
    """Ensure the current HHS OIG LEIE file is cached as CSV and Parquet.

    The source is refreshed when forced, when Last-Modified/ETag changes, or
    when the local download is at least 31 days old. If upstream checks or
    downloads fail, an existing Parquet cache may be served as stale for up to
    45 days.
    """
    existing_meta = _read_json(_LEIE_META)
    has_cache = _LEIE_PARQUET.exists()
    should_refresh = force_refresh or not has_cache or _leie_download_is_older_than(_LEIE_TTL_DAYS)
    source_last_modified = str(existing_meta.get("source_last_modified", ""))
    source_etag = str(existing_meta.get("source_etag", ""))
    head_error = ""

    try:
        head = await resilient_request("HEAD", LEIE_URL, timeout=30.0)
        remote_last_modified = head.headers.get("last-modified", "")
        remote_etag = head.headers.get("etag", "")
        if remote_last_modified and remote_last_modified != source_last_modified:
            should_refresh = True
        if remote_etag and remote_etag != source_etag:
            should_refresh = True
        source_last_modified = remote_last_modified or source_last_modified
        source_etag = remote_etag or source_etag
    except Exception as e:
        head_error = f"HEAD failed: {e}"
        if has_cache and not should_refresh and _leie_cache_is_younger_than(_LEIE_STALE_MAX_DAYS):
            return _leie_metadata("stale", last_error=head_error)
        should_refresh = True

    if not should_refresh and has_cache:
        return _leie_metadata("fresh", last_error=head_error)

    try:
        logger.info("Downloading HHS OIG LEIE file from %s", LEIE_URL)
        resp = await resilient_request("GET", LEIE_URL, timeout=300.0)
        source_last_modified = resp.headers.get("last-modified", source_last_modified)
        source_etag = resp.headers.get("etag", source_etag)
        _LEIE_CSV.write_bytes(resp.content)

        df = parse_leie_csv(_LEIE_CSV)
        _write_dataframe_parquet(df, _LEIE_PARQUET, compression="zstd")

        meta = {
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "source_last_modified": source_last_modified,
            "source_etag": source_etag,
            "record_count": int(len(df)),
            "cache_status": "refreshed",
            "last_error": head_error,
        }
        written = _write_leie_metadata(meta)
        logger.info("LEIE cached: %d records -> %s", len(df), _LEIE_PARQUET.name)
        return written
    except Exception as e:
        message = f"{head_error}; download failed: {e}" if head_error else f"download failed: {e}"
        logger.warning("Failed to refresh HHS OIG LEIE file: %s", message)
        if has_cache and _leie_cache_is_younger_than(_LEIE_STALE_MAX_DAYS):
            return _leie_metadata("stale", last_error=message)
        return _leie_metadata("unavailable", last_error=message)


def parse_leie_csv(csv_path: Path) -> pd.DataFrame:
    """Parse and normalize an HHS OIG LEIE CSV using the documented layout."""
    df = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        low_memory=False,
        encoding_errors="replace",
    )
    raw_columns = [str(c).strip().upper() for c in df.columns]
    missing = [col for col in LEIE_LAYOUT_COLUMNS if col not in raw_columns]
    if missing:
        raise ValueError(f"LEIE CSV missing required columns: {', '.join(missing)}")

    df.columns = raw_columns
    df = df[LEIE_LAYOUT_COLUMNS].copy()
    df.rename(columns=_LEIE_COLUMN_MAP, inplace=True)
    df = df.map(lambda value: str(value).strip() if value is not None else "")

    for date_col in ("dob", "exclusion_date", "reinstatement_date", "waiver_date"):
        df[date_col] = df[date_col].map(_normalize_leie_date)

    df["npi"] = df["npi"].map(lambda value: normalize_npi(value) or "")
    df["upin"] = df["upin"].map(lambda value: "" if str(value).strip() in {"000000", "0000000"} else str(value).strip())
    df["normalized_npi"] = df["npi"]
    df["normalized_state"] = df["state"].map(lambda value: normalize_state(value) or "")
    df["state"] = df["normalized_state"].where(df["normalized_state"] != "", df["state"].str.upper().str.strip())
    df["waiver_state"] = df["waiver_state"].map(lambda value: normalize_state(value) or str(value).strip().upper())

    individual_name = (
        df["first_name"].fillna("") + " " + df["middle_name"].fillna("") + " " + df["last_name"].fillna("")
    ).str.strip()
    df["normalized_individual_name"] = individual_name.map(normalize_name)
    df["normalized_business_name"] = df["business_name"].map(
        lambda value: normalize_name(value, remove_legal_suffixes=True)
    )
    df["entity_type"] = df["normalized_business_name"].map(lambda value: "entity" if value else "individual")
    df["display_name"] = df.apply(_leie_display_name, axis=1)

    return df


def query_leie_by_npi(npi: str) -> list[dict]:
    """Return exact LEIE records for a valid 10-digit NPI."""
    normalized = normalize_npi(npi)
    if not normalized or not _LEIE_PARQUET.exists():
        return []

    df = _read_parquet_dataframe(_LEIE_PARQUET)
    matches = df[df["normalized_npi"] == normalized].head(500)
    return [
        _leie_record_from_row(row, match_basis="npi_exact", match_score=100, verification_status="strong_potential_match")
        for _, row in matches.iterrows()
    ]


def query_leie_by_individual(
    last_name: str,
    first_name: str = "",
    state: str = "",
    dob: str = "",
    limit: int = 25,
) -> list[dict]:
    """Search LEIE individual records by name with optional state/DOB ranking."""
    norm_last = normalize_name(last_name)
    if not norm_last or not _LEIE_PARQUET.exists():
        return []
    norm_first = normalize_name(first_name)
    norm_state = normalize_state(state) or ""
    norm_dob = _normalize_leie_date(dob)

    df = _read_parquet_dataframe(_LEIE_PARQUET)
    candidates = df[df["entity_type"] == "individual"].copy()
    last_col = candidates["last_name"].map(normalize_name)
    candidates = candidates[last_col.str.startswith(norm_last) | (last_col == norm_last)]
    if norm_state:
        candidates = candidates[candidates["normalized_state"] == norm_state]

    scored: list[dict] = []
    for _, row in candidates.iterrows():
        score = 80
        basis = "last_name_prefix"
        if normalize_name(row.get("last_name", "")) == norm_last:
            score = 88
            basis = "last_name_exact"
        if norm_first:
            first_score = conservative_fuzzy_score(norm_first, row.get("first_name", ""))
            score = max(score, int(round((score + first_score) / 2)))
            basis = "name_fuzzy" if first_score < 100 else "name_exact"
        if norm_state:
            score = min(100, score + 4)
            basis = "name_state"
        if norm_dob and row.get("dob", "") == norm_dob:
            score = min(100, score + 6)
            basis = "name_dob" if not norm_state else "name_state_dob"
        scored.append(_leie_record_from_row(row, match_basis=basis, match_score=score))

    scored.sort(key=lambda item: item["match_score"], reverse=True)
    return scored[:max(1, min(limit, 100))]


def query_leie_by_entity(
    entity_name: str,
    state: str = "",
    npi: str = "",
    limit: int = 25,
) -> list[dict]:
    """Search LEIE entity records by exact NPI or normalized business name."""
    npi_matches = query_leie_by_npi(npi) if npi else []
    if npi_matches:
        return npi_matches[:max(1, min(limit, 100))]

    norm_name = normalize_name(entity_name, remove_legal_suffixes=True)
    if not norm_name or not _LEIE_PARQUET.exists():
        return []
    norm_state = normalize_state(state) or ""

    df = _read_parquet_dataframe(_LEIE_PARQUET)
    candidates = df[df["entity_type"] == "entity"].copy()
    if norm_state:
        candidates = candidates[candidates["normalized_state"] == norm_state]

    tokens = norm_name.split()
    prefix = tokens[0] if tokens else norm_name
    business_col = candidates["normalized_business_name"].fillna("")
    candidates = candidates[
        business_col.str.startswith(prefix)
        | business_col.str.contains(re.escape(norm_name), regex=True)
        | business_col.map(lambda value: prefix in value.split())
    ]

    scored: list[dict] = []
    for _, row in candidates.iterrows():
        score = conservative_fuzzy_score(norm_name, row.get("normalized_business_name", ""))
        if row.get("normalized_business_name", "").startswith(norm_name):
            score = max(score, 94)
        if norm_state:
            score = min(100, score + 3)
        basis = "entity_name_state" if norm_state else "entity_name"
        scored.append(_leie_record_from_row(row, match_basis=basis, match_score=score))

    scored = [item for item in scored if item["match_score"] >= 70]
    scored.sort(key=lambda item: item["match_score"], reverse=True)
    return scored[:max(1, min(limit, 100))]


def screen_leie_candidates(candidates: list[dict], limit_per_candidate: int = 5) -> list[dict]:
    """Screen up to 100 candidate dictionaries against the current LEIE cache."""
    if len(candidates) > 100:
        raise ValueError("screen_leie_candidates accepts at most 100 candidates per call")

    limit = max(1, min(limit_per_candidate, 25))
    results: list[dict] = []
    screened_at = datetime.now(timezone.utc).isoformat()
    source_metadata = get_leie_source_metadata()

    for index, candidate in enumerate(candidates):
        if _has_sensitive_identifier_key(candidate):
            raise ValueError(
                "LEIE batch screening does not accept SSN, EIN, TIN, or tax identifier fields"
            )

        normalized_candidate = {
            "candidate_id": str(candidate.get("candidate_id") or f"candidate-{index + 1}"),
            "entity_type": str(candidate.get("entity_type", "")).strip().lower(),
            "npi": str(candidate.get("npi", "")).strip(),
            "first_name": str(candidate.get("first_name", "")).strip(),
            "last_name": str(candidate.get("last_name", "")).strip(),
            "entity_name": str(candidate.get("entity_name", "")).strip(),
            "state": str(candidate.get("state", "")).strip(),
            "dob": str(candidate.get("dob", "")).strip(),
        }

        matches: list[dict] = []
        if normalized_candidate["npi"]:
            matches = query_leie_by_npi(normalized_candidate["npi"])
        if not matches and normalized_candidate["entity_name"]:
            matches = query_leie_by_entity(
                entity_name=normalized_candidate["entity_name"],
                state=normalized_candidate["state"],
                limit=limit,
            )
        if not matches and normalized_candidate["last_name"]:
            matches = query_leie_by_individual(
                last_name=normalized_candidate["last_name"],
                first_name=normalized_candidate["first_name"],
                state=normalized_candidate["state"],
                dob=normalized_candidate["dob"],
                limit=limit,
            )

        matches = matches[:limit]
        best_score = max((int(match.get("match_score", 0)) for match in matches), default=0)
        results.append({
            "candidate": normalized_candidate,
            "status": _leie_status_for_matches(matches),
            "match_count": len(matches),
            "best_match_score": best_score,
            "matches": matches,
            "screened_at": screened_at,
            "source_metadata": source_metadata,
        })

    return results


def get_leie_source_metadata() -> dict:
    """Return source/cache metadata for the LEIE cache without network access."""
    status = "fresh" if _LEIE_PARQUET.exists() else "missing"
    if _LEIE_PARQUET.exists() and not _leie_cache_is_younger_than(_LEIE_TTL_DAYS):
        status = "stale"
    return _leie_metadata(status)


def _leie_display_name(row: pd.Series) -> str:
    business_name = str(row.get("business_name", "")).strip()
    if business_name:
        return business_name
    parts = [
        str(row.get("first_name", "")).strip(),
        str(row.get("middle_name", "")).strip(),
        str(row.get("last_name", "")).strip(),
    ]
    return " ".join(part for part in parts if part)


def _leie_record_from_row(
    row: pd.Series | dict,
    *,
    match_basis: str,
    match_score: int,
    verification_status: str = "potential_match",
) -> dict:
    fields = [
        "entity_type",
        "display_name",
        "last_name",
        "first_name",
        "middle_name",
        "business_name",
        "general_category",
        "specialty",
        "upin",
        "npi",
        "dob",
        "address",
        "city",
        "state",
        "zip_code",
        "exclusion_type",
        "exclusion_date",
        "reinstatement_date",
        "waiver_date",
        "waiver_state",
    ]
    record = {field: str(row.get(field, "") or "") for field in fields}
    record.update({
        "match_basis": match_basis,
        "match_score": int(match_score),
        "verification_status": verification_status,
    })
    return record


def _leie_status_for_matches(matches: list[dict]) -> str:
    if not matches:
        return "no_current_leie_match_found"
    if any(match.get("verification_status") == "strong_potential_match" for match in matches):
        return "strong_potential_match"
    return "potential_match"


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
