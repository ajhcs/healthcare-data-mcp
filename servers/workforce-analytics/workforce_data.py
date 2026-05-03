"""Workforce data loaders for HRSA, HCRIS, PBJ, and ACGME datasets.

Handles bulk dataset downloads, Parquet caching, and DuckDB queries
for healthcare workforce analysis.
"""

import logging
import os
import zipfile
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx

from shared.utils.duckdb_safe import safe_parquet_sql
from shared.utils.http_client import resilient_request, get_client
import pandas as pd

import sys as _sys
_project_root = __import__("pathlib").Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in _sys.path:
    _sys.path.insert(0, str(_project_root))

from shared.utils.cache import is_cache_valid  # noqa: E402

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "workforce"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_HPSA_CACHE = _CACHE_DIR / "hpsa.parquet"
_HCRIS_CACHE = _CACHE_DIR / "hcris_staffing.parquet"
_CACHE_TTL_DAYS = 30

# ACGME static data (bundled with server)
_ACGME_DATA_DIR = Path(__file__).parent / "data"
_ACGME_CSV = _ACGME_DATA_DIR / "acgme_programs.csv"
_ACGME_CACHE_CSV = _CACHE_DIR / "acgme_programs.csv"
_ACGME_CACHE_META = _CACHE_DIR / "acgme_programs.meta.json"
_ACGME_ENV_VAR = "ACGME_PROGRAMS_CSV"
_ACGME_SOURCE_URL = "https://acgmecloud.org/analytics/explore-public-data/program-search"
_ACGME_SOURCE_URLS = [
    "https://support.acgmecloud.org/hc/en-us/articles/36070312362391-Advance-Search-Feature",
    "https://support.acgmecloud.org/hc/en-us/articles/31576594571927-Explore-Public-Data-Programs",
    "https://apps.acgme-i.org/ads/Public/Request/GetDataDictionary",
]
_ACGME_IMPORT_HINT = (
    "Import a public ACGME Program Search export with "
    "python3 scripts/import_acgme_programs.py /path/to/acgme-program-search-export.csv. "
    f"Source: {_ACGME_SOURCE_URL}"
)

# URLs
HPSA_CSV_URL = "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv"
PBJ_API_URL = "https://data.cms.gov/data-api/v1/dataset/7e0d53ba-8f02-4c66-98a5-14a1c997c50d/data"

# HCRIS: CMS Cost Report fiscal year page
# We use the provider-compliance API for structured access
HCRIS_API_URL = "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report"

_HCRIS_PROVIDER_COLUMNS = (
    "prvdr_num",
    "provider_number",
    "provider_id",
    "ccn",
    "cms_certification_number",
)
_HCRIS_YEAR_COLUMNS = (
    "fiscal_year",
    "fy",
    "report_year",
    "cost_report_year",
    "year",
    "fy_end_dt",
    "fiscal_year_end",
    "fiscal_year_end_date",
)
_HCRIS_REPORT_RECORD_COLUMNS = ("rpt_rec_num", "report_record_number", "rpt_rec")
_HCRIS_VALUE_COLUMNS = ("itm_val_num", "itm_val", "value", "val")
_S3_FTE_COLUMN_MAP = {
    "001": "total_ftes",
    "002": "rn_ftes",
    "003": "lpn_ftes",
    "004": "aide_ftes",
}
_S3_DEPARTMENT_LINES = {
    "001": "Hospital Adults & Pediatrics",
    "002": "Intensive Care Unit",
    "003": "Coronary Care Unit",
    "004": "Burn Intensive Care Unit",
    "005": "Surgical Intensive Care Unit",
    "006": "Other Special Care",
    "007": "Nursery",
    "014": "Total Adults and Pediatrics",
    "016": "Subprovider - IPF",
    "017": "Subprovider - IRF",
    "019": "Skilled Nursing Facility",
    "020": "Nursing Facility",
    "022": "Home Health Agency",
    "023": "ASC",
    "024": "Hospice",
}

_ACGME_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "program_id": (
        "program_id",
        "program id",
        "acgme_program_id",
        "program number",
        "program_no",
        "program code",
        "program_code",
        "program",
        "id",
    ),
    "specialty": (
        "specialty",
        "specialty_name",
        "specialty name",
        "program specialty",
        "discipline",
    ),
    "institution": (
        "institution",
        "institution_name",
        "sponsoring institution",
        "sponsoring_institution",
        "sponsor institution",
        "sponsor_institution",
        "sponsor",
        "sponsor_name",
    ),
    "city": ("city", "institution_city", "program_city"),
    "state": ("state", "st", "institution_state", "program_state"),
    "total_positions": (
        "total_positions",
        "approved_positions",
        "approved positions",
        "program_complement",
        "program complement",
        "resident_complement",
        "positions",
    ),
    "filled_positions": (
        "filled_positions",
        "filled positions",
        "on_duty",
        "on duty",
        "active_trainees",
        "current_filled_positions",
    ),
    "accreditation_status": (
        "accreditation_status",
        "accreditation status",
        "status",
    ),
}

_ACGME_REQUIRED_COLUMNS = ("specialty", "institution", "state")


def _normalize_column_name(name: str) -> str:
    return (
        name.strip()
        .lower()
        .replace("/", " ")
        .replace("-", " ")
        .replace(".", " ")
        .replace("(", " ")
        .replace(")", " ")
        .replace("#", " number ")
    )


def _find_column(columns: pd.Index, aliases: tuple[str, ...]) -> str | None:
    normalized = {_normalize_column_name(column): column for column in columns}
    for alias in aliases:
        match = normalized.get(_normalize_column_name(alias))
        if match:
            return match
    return None


def normalize_acgme_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize an ACGME export into the canonical queryable schema."""
    if df.empty:
        return pd.DataFrame(columns=list(_ACGME_COLUMN_ALIASES))

    selected: dict[str, pd.Series] = {}
    for canonical_name, aliases in _ACGME_COLUMN_ALIASES.items():
        source_column = _find_column(df.columns, aliases)
        if source_column:
            selected[canonical_name] = df[source_column]

    missing_required = [column for column in _ACGME_REQUIRED_COLUMNS if column not in selected]
    if missing_required:
        raise ValueError(
            "Missing required ACGME columns: "
            + ", ".join(missing_required)
            + ". Available columns: "
            + ", ".join(str(column) for column in df.columns)
        )

    normalized = pd.DataFrame(selected)

    for column in _ACGME_COLUMN_ALIASES:
        if column not in normalized.columns:
            normalized[column] = ""

    for column in ("program_id", "specialty", "institution", "city", "state", "accreditation_status"):
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized["program_id"] = normalized["program_id"].map(_normalize_acgme_program_id)
    invalid_program_ids = [
        value for value in normalized["program_id"].tolist()
        if value and not (len(value) == 10 and value.isdigit())
    ]
    if invalid_program_ids:
        raise ValueError("ACGME program_id must be a 10-digit string when present.")

    for column in ("total_positions", "filled_positions"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0).astype(int)

    normalized = normalized[
        (normalized["specialty"] != "")
        & (normalized["institution"] != "")
        & (normalized["state"] != "")
    ].copy()
    normalized["state"] = normalized["state"].str.upper()
    return normalized[list(_ACGME_COLUMN_ALIASES)]


def _normalize_acgme_program_id(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text.zfill(10) if text.isdigit() and len(text) < 10 else text


def acgme_column_map(df: pd.DataFrame) -> dict[str, str]:
    return {
        canonical: source
        for canonical, aliases in _ACGME_COLUMN_ALIASES.items()
        if (source := _find_column(df.columns, aliases))
    }


def write_acgme_import_metadata(
    *,
    input_path: Path,
    output_path: Path,
    raw_df: pd.DataFrame,
    normalized_df: pd.DataFrame,
    source_url: str = _ACGME_SOURCE_URL,
) -> dict[str, object]:
    checksum = hashlib.sha256(output_path.read_bytes()).hexdigest() if output_path.exists() else ""
    optional_missing = [
        column
        for column in ("program_id", "city", "total_positions", "filled_positions", "accreditation_status")
        if column not in acgme_column_map(raw_df)
    ]
    meta = {
        "input_filename": str(input_path),
        "imported_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(normalized_df)),
        "normalized_column_map": acgme_column_map(raw_df),
        "source_url": source_url,
        "source_urls": _ACGME_SOURCE_URLS,
        "checksum_sha256": checksum,
        "optional_missing_columns": optional_missing,
    }
    meta_path = output_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    if output_path == _ACGME_CACHE_CSV and meta_path != _ACGME_CACHE_META:
        _ACGME_CACHE_META.write_text(json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8")
    return meta


def resolve_acgme_csv_path() -> Path | None:
    """Return the first available ACGME CSV path from env, cache, or bundled data."""
    configured_path = os.environ.get(_ACGME_ENV_VAR, "").strip()
    candidates = [
        Path(configured_path).expanduser() if configured_path else None,
        _ACGME_CACHE_CSV,
        _ACGME_CSV,
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    return None


def acgme_missing_data_error() -> str:
    return (
        f"ACGME data file not found. Checked {_ACGME_ENV_VAR}, {_ACGME_CACHE_CSV}, and {_ACGME_CSV}. "
        + _ACGME_IMPORT_HINT
    )


def get_acgme_source_status() -> dict[str, object]:
    acgme_csv = resolve_acgme_csv_path()
    meta_path = acgme_csv.with_suffix(".meta.json") if acgme_csv else _ACGME_CACHE_META
    meta: dict[str, object] = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    if acgme_csv is None:
        status = "import_required"
        row_count = 0
    else:
        try:
            df = pd.read_csv(acgme_csv, dtype=str, keep_default_na=False)
            normalize_acgme_dataframe(df)
            status = "ready"
            row_count = int(len(df))
        except Exception as exc:
            status = "invalid_import"
            row_count = 0
            meta["last_error"] = str(exc)
    return {
        "source_name": "ACGME Program Search public export",
        "source_urls": _ACGME_SOURCE_URLS,
        "cache_path": str(_ACGME_CACHE_CSV),
        "active_csv_path": str(acgme_csv) if acgme_csv else "",
        "metadata_path": str(meta_path),
        "row_count": int(meta.get("row_count") or row_count),
        "last_import_time": str(meta.get("imported_at", "")),
        "required_columns": list(_ACGME_REQUIRED_COLUMNS),
        "optional_columns": [
            column for column in _ACGME_COLUMN_ALIASES if column not in _ACGME_REQUIRED_COLUMNS
        ],
        "status": status,
        "next_step": _ACGME_IMPORT_HINT if status != "ready" else "",
        "source_caveat": (
            "This repo supports import of ACGME public Program Search exports unless/until "
            "ACGME provides a stable documented unauthenticated API."
        ),
        "metadata": meta,
    }


def _acgme_result(row: pd.Series, match_basis: list[str] | None = None, confidence: str = "high") -> dict[str, object]:
    return {
        "program_id": str(row.get("program_id", "")),
        "specialty": str(row.get("specialty", "")),
        "institution": str(row.get("institution", "")),
        "city": str(row.get("city", "")),
        "state": str(row.get("state", "")),
        "total_positions": int(row.get("total_positions", 0) or 0),
        "filled_positions": int(row.get("filled_positions", 0) or 0),
        "accreditation_status": str(row.get("accreditation_status", "")),
        "match_basis": match_basis or [],
        "confidence": confidence,
    }


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    """Check if a cached file exists and is within TTL."""
    return is_cache_valid(path, max_age_days=ttl_days)


def _first_column(columns: list[str], candidates: tuple[str, ...], contains: str = "") -> str | None:
    if contains:
        return next((column for column in columns if contains in column), None)
    return next((column for column in candidates if column in columns), None)


def _hcris_code(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits[:3].zfill(3)


def _hcris_float(value: object) -> float:
    try:
        return float(str(value).replace(",", "").strip()) if str(value).strip() else 0.0
    except (TypeError, ValueError):
        return 0.0


def _filter_hcris_provider_year(
    con: duckdb.DuckDBPyConnection,
    *,
    ccn: str,
    worksheet_prefix: str,
    year: int = 0,
) -> pd.DataFrame:
    cols = [
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='hcris'"
        ).fetchall()
    ]
    wksht_col = _first_column(cols, (), contains="wksht")
    prvdr_col = _first_column(cols, _HCRIS_PROVIDER_COLUMNS)
    if not wksht_col or not prvdr_col:
        return pd.DataFrame()

    where_parts = [f"{prvdr_col} = ?", f"{wksht_col} LIKE ?"]
    params: list[object] = [ccn.strip().zfill(6), f"{worksheet_prefix}%"]
    if year:
        year_col = _first_column(cols, _HCRIS_YEAR_COLUMNS)
        if year_col:
            where_parts.append(f"CAST({year_col} AS VARCHAR) LIKE ?")
            params.append(f"%{int(year)}%")

    where = " AND ".join(where_parts)
    return con.execute(f"SELECT * FROM hcris WHERE {where} LIMIT 1000", params).fetchdf()


# ---------------------------------------------------------------------------
# HRSA HPSA Data
# ---------------------------------------------------------------------------

async def ensure_hpsa_cached() -> bool:
    """Download HRSA HPSA CSV and cache as Parquet."""
    if _is_cache_valid(_HPSA_CACHE):
        return True

    logger.info("Downloading HRSA HPSA data...")
    try:
        resp = await resilient_request("GET", HPSA_CSV_URL, timeout=300.0)

        csv_path = _CACHE_DIR / "hpsa_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_HPSA_CACHE, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("HPSA data cached: %d records", len(df))
        return True

    except Exception as e:
        logger.warning("Failed to cache HPSA data: %s", e)
        return False


def query_hpsas(state: str, discipline: str = "", county_fips: str = "") -> list[dict]:
    """Query cached HPSA data by state, discipline, and optional county."""
    if not _HPSA_CACHE.exists():
        return []

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hpsa AS SELECT * FROM {safe_parquet_sql(_HPSA_CACHE)}")

        # Find relevant columns
        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='hpsa'"
        ).fetchall()]

        state_col = next((c for c in cols if c in (
            "hpsa_state_abbreviation", "state_abbreviation", "state"
        )), None)
        disc_col = next((c for c in cols if c in (
            "hpsa_discipline_class", "discipline_class", "discipline"
        )), None)
        name_col = next((c for c in cols if c in (
            "hpsa_name", "name"
        )), None)
        score_col = next((c for c in cols if c in (
            "hpsa_score", "score"
        )), None)
        id_col = next((c for c in cols if c in (
            "source_id", "hpsa_id", "hpsa_source_id"
        )), None)

        if not state_col:
            con.close()
            return []

        where_parts = [f"{state_col} = ?"]
        params: list = [state.upper()]

        if discipline and disc_col:
            where_parts.append(f"LOWER({disc_col}) LIKE ?")
            params.append(f"%{discipline.lower()}%")

        where = " AND ".join(where_parts)
        rows = con.execute(
            f"SELECT * FROM hpsa WHERE {where} LIMIT 200", params
        ).fetchdf()
        con.close()

        results = []
        for _, row in rows.iterrows():
            results.append({
                "hpsa_name": str(row.get(name_col, "")) if name_col else "",
                "hpsa_id": str(row.get(id_col, "")) if id_col else "",
                "hpsa_score": int(float(row.get(score_col, 0) or 0)) if score_col else 0,
                "discipline": str(row.get(disc_col, "")) if disc_col else "",
                "state": str(row.get(state_col, "")),
            })

        return results

    except Exception as e:
        logger.warning("HPSA query failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# CMS HCRIS Cost Report Data
# ---------------------------------------------------------------------------

async def ensure_hcris_cached() -> bool:
    """Download HCRIS cost report data and cache staffing-relevant rows as Parquet.

    The full nmrc file is >2GB. We filter to Worksheets S-2 and S-3 only
    to keep the cache manageable (~50MB).
    """
    if _is_cache_valid(_HCRIS_CACHE):
        return True

    logger.info("Downloading HCRIS cost report data...")
    try:
        # Try the CMS fiscal year download (most recent year)
        # Pattern: https://downloads.cms.gov/files/hcris/HOSP10FY{year}.zip
        import datetime as dt
        current_year = dt.date.today().year
        zip_path: Path | None = None

        for year in range(current_year, current_year - 3, -1):
            url = f"https://downloads.cms.gov/files/hcris/HOSP10FY{year}.zip"
            try:
                client = get_client()
                resp = await client.get(
                    url,
                    timeout=httpx.Timeout(connect=10.0, read=600.0, write=600.0, pool=10.0),
                )
                if resp.status_code == 200 and len(resp.content) > 10000:
                    zip_path = _CACHE_DIR / f"hcris_fy{year}.zip"
                    zip_path.write_bytes(resp.content)
                    logger.info("Downloaded HCRIS FY%d (%d bytes)", year, len(resp.content))
                    break
            except Exception:
                continue

        if zip_path is None:
            logger.warning("Could not download HCRIS data from CMS")
            return False

        # Extract and filter nmrc file to S-2 and S-3 worksheets
        with zipfile.ZipFile(zip_path) as zf:
            nmrc_files = [f for f in zf.namelist() if "NMRC" in f.upper() or "nmrc" in f.lower()]
            rpt_files = [f for f in zf.namelist() if "RPT" in f.upper() and "NMRC" not in f.upper()]

            if not nmrc_files:
                logger.warning("No NMRC file found in HCRIS ZIP")
                return False

            # Read report file for provider/year metadata. NMRC rows are keyed by report record.
            rpt_df = None
            if rpt_files:
                with zf.open(rpt_files[0]) as f:
                    rpt_df = pd.read_csv(f, dtype=str, keep_default_na=False, low_memory=False)
                    rpt_df.columns = [c.strip().lower().replace(" ", "_") for c in rpt_df.columns]

            # Read nmrc file in chunks, filter to S-2 and S-3
            filtered_chunks = []
            with zf.open(nmrc_files[0]) as f:
                for chunk in pd.read_csv(f, dtype=str, keep_default_na=False, chunksize=500000, low_memory=False):
                    chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
                    wksht_col = next((c for c in chunk.columns if "wksht" in c), None)
                    if wksht_col:
                        mask = chunk[wksht_col].str.startswith(("S2", "S3"))
                        filtered = chunk[mask]
                        if not filtered.empty:
                            filtered_chunks.append(filtered)

        if not filtered_chunks:
            logger.warning("No S-2/S-3 data found in HCRIS NMRC file")
            return False

        result_df = pd.concat(filtered_chunks, ignore_index=True)

        # Join with report file for provider number, fiscal year, and provider names if available.
        if rpt_df is not None:
            rpt_rec_col = _first_column(list(result_df.columns), _HCRIS_REPORT_RECORD_COLUMNS, contains="rpt_rec")
            if rpt_rec_col and rpt_rec_col in rpt_df.columns:
                provider_cols = [c for c in _HCRIS_PROVIDER_COLUMNS if c in rpt_df.columns]
                year_cols = [c for c in _HCRIS_YEAR_COLUMNS if c in rpt_df.columns]
                name_cols = [c for c in rpt_df.columns if "prvdr" in c or "name" in c]
                merge_cols = [rpt_rec_col] + provider_cols[:1] + year_cols[:2] + name_cols[:2]
                merge_cols = list(dict.fromkeys(merge_cols))
                if len(merge_cols) > 1:
                    deduped = pd.DataFrame(rpt_df[merge_cols]).drop_duplicates(subset=rpt_rec_col)
                    result_df = result_df.merge(
                        deduped, on=rpt_rec_col, how="left"
                    )

        result_df.to_parquet(_HCRIS_CACHE, compression="zstd", index=False)
        logger.info("HCRIS staffing data cached: %d rows (S-2 and S-3)", len(result_df))

        # Cleanup
        zip_path.unlink(missing_ok=True)
        return True

    except Exception as e:
        logger.warning("Failed to cache HCRIS data: %s", e)
        return False


def query_hcris_gme(ccn: str, year: int = 0) -> dict | None:
    """Query HCRIS S-2 data for GME/teaching hospital profile."""
    if not _HCRIS_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hcris AS SELECT * FROM {safe_parquet_sql(_HCRIS_CACHE)}")

        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='hcris'"
        ).fetchall()]

        line_col = _first_column(cols, (), contains="line")
        clmn_col = _first_column(cols, (), contains="clmn")
        val_col = _first_column(cols, _HCRIS_VALUE_COLUMNS)

        if not all([line_col, clmn_col, val_col]):
            con.close()
            return None

        rows = _filter_hcris_provider_year(con, ccn=ccn, worksheet_prefix="S2", year=year)
        con.close()

        if rows.empty:
            return None

        # Parse S-2 worksheet values
        result: dict = {"ccn": ccn, "teaching_status": "Non-Teaching"}
        for _, row in rows.iterrows():
            line = _hcris_code(row.get(line_col, ""))
            col = _hcris_code(row.get(clmn_col, ""))
            fval = _hcris_float(row.get(val_col, ""))

            # Resident FTEs (line 66, col 1 = IME FTEs)
            if line == "066" and col == "001":
                result["total_resident_ftes"] = fval
                if fval > 0:
                    result["teaching_status"] = "Teaching"

        return result

    except Exception as e:
        logger.warning("HCRIS GME query failed for CCN %s: %s", ccn, e)
        return None


def query_hcris_staffing(ccn: str, year: int = 0) -> dict | None:
    """Query HCRIS S-3 data for staffing FTEs by department."""
    if not _HCRIS_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hcris AS SELECT * FROM {safe_parquet_sql(_HCRIS_CACHE)}")

        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='hcris'"
        ).fetchall()]

        line_col = _first_column(cols, (), contains="line")
        clmn_col = _first_column(cols, (), contains="clmn")
        val_col = _first_column(cols, _HCRIS_VALUE_COLUMNS)

        if not all([line_col, clmn_col, val_col]):
            con.close()
            return None

        rows = _filter_hcris_provider_year(con, ccn=ccn, worksheet_prefix="S3", year=year)
        con.close()

        if rows.empty:
            return None

        # Parse S-3 worksheet values into departments
        departments: dict[str, dict[str, float | str]] = {}

        for _, row in rows.iterrows():
            line = _hcris_code(row.get(line_col, ""))
            col = _hcris_code(row.get(clmn_col, ""))
            metric = _S3_FTE_COLUMN_MAP.get(col)
            if not line or not metric:
                continue

            fval = _hcris_float(row.get(val_col, ""))
            dept_key = f"line_{line}"
            if dept_key not in departments:
                departments[dept_key] = {
                    "dept_name": _S3_DEPARTMENT_LINES.get(line, f"Cost Center {line}"),
                    "total_ftes": 0.0,
                    "rn_ftes": 0.0,
                    "lpn_ftes": 0.0,
                    "aide_ftes": 0.0,
                }
            departments[dept_key][metric] = fval

        total_ftes = sum(float(d["total_ftes"]) for d in departments.values())

        dept_list = [
            {
                "dept_name": str(d["dept_name"]),
                "total_ftes": float(d["total_ftes"]),
                "rn_ftes": float(d["rn_ftes"]),
                "lpn_ftes": float(d["lpn_ftes"]),
                "aide_ftes": float(d["aide_ftes"]),
            }
            for d in departments.values()
            if any(float(d[key]) > 0 for key in ("total_ftes", "rn_ftes", "lpn_ftes", "aide_ftes"))
        ]

        return {
            "ccn": ccn,
            "departments": dept_list,
            "total_ftes": round(total_ftes, 1),
        }

    except Exception as e:
        logger.warning("HCRIS staffing query failed for CCN %s: %s", ccn, e)
        return None


# ---------------------------------------------------------------------------
# CMS PBJ Nursing Home Staffing
# ---------------------------------------------------------------------------

async def query_pbj_staffing(ccn: str = "", state: str = "") -> list[dict]:
    """Query CMS PBJ API for nursing home staffing data.

    Uses the Socrata-compatible data.cms.gov API.
    """
    params: dict = {"size": 100}

    if ccn:
        params["filter[PROVNUM]"] = ccn.strip()
    elif state:
        params["filter[STATE]"] = state.upper()
    else:
        return []

    try:
        resp = await resilient_request("GET", PBJ_API_URL, params=params, timeout=60.0)
        records = resp.json()

        results = []
        for r in records:
            rn_hrs = float(r.get("Hrs_RN", 0) or 0)
            lpn_hrs = float(r.get("Hrs_LPN", 0) or 0)
            cna_hrs = float(r.get("Hrs_CNA", 0) or 0)
            census = float(r.get("MDScensus", 1) or 1)

            results.append({
                "facility_name": r.get("PROVNAME", ""),
                "ccn": r.get("PROVNUM", ""),
                "state": r.get("STATE", ""),
                "date": r.get("WorkDate", ""),
                "census": int(census),
                "rn_hprd": round(rn_hrs / census, 2) if census > 0 else 0,
                "lpn_hprd": round(lpn_hrs / census, 2) if census > 0 else 0,
                "cna_hprd": round(cna_hrs / census, 2) if census > 0 else 0,
                "total_nurse_hprd": round((rn_hrs + lpn_hrs + cna_hrs) / census, 2) if census > 0 else 0,
            })

        return results

    except Exception as e:
        logger.warning("PBJ query failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# ACGME Static Data
# ---------------------------------------------------------------------------

def query_acgme_programs(
    institution: str = "", specialty: str = "", state: str = ""
) -> list[dict]:
    """Query bundled ACGME program data.

    If the canonical CSV doesn't exist, returns a helpful error message.
    """
    acgme_csv = resolve_acgme_csv_path()
    if acgme_csv is None:
        return [{"error": acgme_missing_data_error()}]

    try:
        df = pd.read_csv(acgme_csv, dtype=str, keep_default_na=False)
        df = normalize_acgme_dataframe(df)

        # Apply filters
        if institution:
            mask = df["institution"].str.lower().str.contains(institution.lower(), na=False)
            df = pd.DataFrame(df[mask])

        if specialty:
            mask = df["specialty"].str.lower().str.contains(specialty.lower(), na=False)
            df = pd.DataFrame(df[mask])

        if state:
            mask = df["state"].str.upper() == state.upper()
            df = pd.DataFrame(df[mask])

        match_basis = []
        if institution:
            match_basis.append("institution_contains")
        if specialty:
            match_basis.append("specialty_contains")
        if state:
            match_basis.append("state_exact")
        results = []
        for _, row in df.head(100).iterrows():
            results.append(_acgme_result(row, match_basis=match_basis, confidence="high" if match_basis else "not_requested"))

        return results

    except Exception as e:
        logger.warning("ACGME query failed: %s", e)
        return [{"error": f"ACGME query failed: {e}. {_ACGME_IMPORT_HINT}"}]


def get_acgme_program(program_id: str) -> dict[str, object] | None:
    normalized = _normalize_acgme_program_id(program_id)
    if not (len(normalized) == 10 and normalized.isdigit()):
        raise ValueError("program_id must be a 10-digit ACGME Program Code.")
    acgme_csv = resolve_acgme_csv_path()
    if acgme_csv is None:
        return None
    df = normalize_acgme_dataframe(pd.read_csv(acgme_csv, dtype=str, keep_default_na=False))
    matches = df[df["program_id"] == normalized]
    if matches.empty:
        return None
    return _acgme_result(matches.iloc[0], match_basis=["program_id_exact"], confidence="high")
