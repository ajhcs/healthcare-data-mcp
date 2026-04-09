"""Workforce data loaders for HRSA, HCRIS, PBJ, and ACGME datasets.

Handles bulk dataset downloads, Parquet caching, and DuckDB queries
for healthcare workforce analysis.
"""

import logging
import os
import zipfile
from pathlib import Path

import duckdb
import httpx

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
_ACGME_ENV_VAR = "ACGME_PROGRAMS_CSV"
_ACGME_SOURCE_URL = "https://acgmecloud.org/analytics/explore-public-data/program-search"
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

    for column in ("total_positions", "filled_positions"):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0).astype(int)

    normalized = normalized[
        (normalized["specialty"] != "")
        & (normalized["institution"] != "")
        & (normalized["state"] != "")
    ].copy()
    normalized["state"] = normalized["state"].str.upper()
    return normalized[list(_ACGME_COLUMN_ALIASES)]


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


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    """Check if a cached file exists and is within TTL."""
    return is_cache_valid(path, max_age_days=ttl_days)


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
        con.execute(f"CREATE VIEW hpsa AS SELECT * FROM read_parquet('{_HPSA_CACHE}')")

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

            # Read report file for provider names
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

        # Join with report file for provider names if available
        if rpt_df is not None:
            rpt_rec_col = next((c for c in result_df.columns if "rpt_rec" in c), None)
            if rpt_rec_col and rpt_rec_col in rpt_df.columns:
                name_cols = [c for c in rpt_df.columns if "prvdr" in c or "name" in c]
                if name_cols:
                    merge_cols = [rpt_rec_col] + name_cols[:2]
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


def query_hcris_gme(ccn: str) -> dict | None:
    """Query HCRIS S-2 data for GME/teaching hospital profile."""
    if not _HCRIS_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hcris AS SELECT * FROM read_parquet('{_HCRIS_CACHE}')")

        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='hcris'"
        ).fetchall()]

        wksht_col = next((c for c in cols if "wksht" in c), None)
        line_col = next((c for c in cols if "line" in c), None)
        clmn_col = next((c for c in cols if "clmn" in c), None)
        val_col = next((c for c in cols if "val" in c or "itm" in c), None)
        prvdr_col = next((c for c in cols if "prvdr" in c and "num" in c), None)

        if not all([wksht_col, line_col, clmn_col, val_col]):
            con.close()
            return None

        # First find the report record for this CCN
        if prvdr_col:
            rows = con.execute(f"""
                SELECT * FROM hcris
                WHERE {prvdr_col} = ? AND {wksht_col} LIKE 'S2%'
                LIMIT 100
            """, [ccn.strip().zfill(6)]).fetchdf()
        else:
            con.close()
            return None

        con.close()

        if rows.empty:
            return None

        # Parse S-2 worksheet values
        result: dict = {"ccn": ccn, "teaching_status": "Non-Teaching"}
        for _, row in rows.iterrows():
            line = str(row.get(line_col, ""))
            col = str(row.get(clmn_col, ""))
            val = str(row.get(val_col, ""))

            try:
                fval = float(val.replace(",", "")) if val.strip() else 0.0
            except ValueError:
                fval = 0.0

            # Resident FTEs (line 66, col 1 = IME FTEs)
            if line.startswith("066") and col.startswith("001"):
                result["total_resident_ftes"] = fval
                if fval > 0:
                    result["teaching_status"] = "Teaching"

        return result

    except Exception as e:
        logger.warning("HCRIS GME query failed for CCN %s: %s", ccn, e)
        return None


def query_hcris_staffing(ccn: str) -> dict | None:
    """Query HCRIS S-3 data for staffing FTEs by department."""
    if not _HCRIS_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hcris AS SELECT * FROM read_parquet('{_HCRIS_CACHE}')")

        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='hcris'"
        ).fetchall()]

        wksht_col = next((c for c in cols if "wksht" in c), None)
        line_col = next((c for c in cols if "line" in c), None)
        clmn_col = next((c for c in cols if "clmn" in c), None)
        val_col = next((c for c in cols if "val" in c or "itm" in c), None)
        prvdr_col = next((c for c in cols if "prvdr" in c and "num" in c), None)

        if not all([wksht_col, line_col, clmn_col, val_col, prvdr_col]):
            con.close()
            return None

        rows = con.execute(f"""
            SELECT * FROM hcris
            WHERE {prvdr_col} = ? AND {wksht_col} LIKE 'S3%'
            LIMIT 500
        """, [ccn.strip().zfill(6)]).fetchdf()
        con.close()

        if rows.empty:
            return None

        # Parse S-3 worksheet values into departments
        departments: dict[str, dict] = {}
        total_ftes = 0.0

        for _, row in rows.iterrows():
            line = str(row.get(line_col, ""))
            col = str(row.get(clmn_col, ""))
            val = str(row.get(val_col, ""))

            try:
                fval = float(val.replace(",", "")) if val.strip() else 0.0
            except ValueError:
                fval = 0.0

            # S-3 Part I: Employee FTEs by cost center line
            if col.startswith("001"):  # Column 1 = employee FTEs
                dept_key = f"line_{line}"
                if dept_key not in departments:
                    departments[dept_key] = {"dept_name": f"Cost Center {line}", "total_ftes": 0.0}
                departments[dept_key]["total_ftes"] = fval
                total_ftes += fval

        dept_list = [
            {"dept_name": d["dept_name"], "total_ftes": d["total_ftes"],
             "rn_ftes": 0.0, "lpn_ftes": 0.0, "aide_ftes": 0.0}
            for d in departments.values() if d["total_ftes"] > 0
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

        results = []
        for _, row in df.head(100).iterrows():
            results.append({
                "program_id": str(row.get("program_id", "")),
                "specialty": str(row.get("specialty", "")),
                "institution": str(row.get("institution", "")),
                "city": str(row.get("city", "")),
                "state": str(row.get("state", "")),
                "total_positions": int(row.get("total_positions", 0) or 0),
                "filled_positions": int(row.get("filled_positions", 0) or 0),
                "accreditation_status": str(row.get("accreditation_status", "")),
            })

        return results

    except Exception as e:
        logger.warning("ACGME query failed: %s", e)
        return [{"error": f"ACGME query failed: {e}. {_ACGME_IMPORT_HINT}"}]
