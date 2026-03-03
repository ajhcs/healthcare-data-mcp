"""Workforce data loaders for HRSA, HCRIS, PBJ, and ACGME datasets.

Handles bulk dataset downloads, Parquet caching, and DuckDB queries
for healthcare workforce analysis.
"""

import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "workforce"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_HPSA_CACHE = _CACHE_DIR / "hpsa.parquet"
_HCRIS_CACHE = _CACHE_DIR / "hcris_staffing.parquet"
_PBJ_CACHE = _CACHE_DIR / "pbj_staffing.parquet"
_CACHE_TTL_DAYS = 30

# ACGME static data (bundled with server)
_ACGME_DATA_DIR = Path(__file__).parent / "data"
_ACGME_CSV = _ACGME_DATA_DIR / "acgme_programs.csv"

# URLs
HPSA_CSV_URL = "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv"
PBJ_API_URL = "https://data.cms.gov/data-api/v1/dataset/7e0d53ba-8f02-4c66-98a5-14a1c997c50d/data"

# HCRIS: CMS Cost Report fiscal year page
# We use the provider-compliance API for structured access
HCRIS_API_URL = "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report"


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    """Check if a cached file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


# ---------------------------------------------------------------------------
# HRSA HPSA Data
# ---------------------------------------------------------------------------

async def ensure_hpsa_cached() -> bool:
    """Download HRSA HPSA CSV and cache as Parquet."""
    if _is_cache_valid(_HPSA_CACHE):
        return True

    logger.info("Downloading HRSA HPSA data...")
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(HPSA_CSV_URL)
            resp.raise_for_status()

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
        downloaded = False

        for year in range(current_year, current_year - 3, -1):
            url = f"https://downloads.cms.gov/files/hcris/HOSP10FY{year}.zip"
            try:
                async with httpx.AsyncClient(timeout=600.0, follow_redirects=True) as client:
                    resp = await client.get(url)
                    if resp.status_code == 200 and len(resp.content) > 10000:
                        zip_path = _CACHE_DIR / f"hcris_fy{year}.zip"
                        zip_path.write_bytes(resp.content)
                        downloaded = True
                        logger.info("Downloaded HCRIS FY%d (%d bytes)", year, len(resp.content))
                        break
            except Exception:
                continue

        if not downloaded:
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
                    result_df = result_df.merge(
                        rpt_df[merge_cols].drop_duplicates(subset=[rpt_rec_col]),
                        on=rpt_rec_col, how="left"
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

        rpt_col = next((c for c in cols if "rpt_rec" in c), None)
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
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(PBJ_API_URL, params=params)
            resp.raise_for_status()
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

    If the static CSV doesn't exist, returns a helpful error message.
    """
    if not _ACGME_CSV.exists():
        return [{"error": f"ACGME data file not found at {_ACGME_CSV}. "
                         "Place acgme_programs.csv in the data/ directory."}]

    try:
        df = pd.read_csv(_ACGME_CSV, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Apply filters
        if institution:
            inst_col = next((c for c in df.columns if "institution" in c or "sponsor" in c), None)
            if inst_col:
                df = df[df[inst_col].str.lower().str.contains(institution.lower(), na=False)]

        if specialty:
            spec_col = next((c for c in df.columns if "specialty" in c), None)
            if spec_col:
                df = df[df[spec_col].str.lower().str.contains(specialty.lower(), na=False)]

        if state:
            state_col = next((c for c in df.columns if c in ("state", "st")), None)
            if state_col:
                df = df[df[state_col].str.upper() == state.upper()]

        results = []
        for _, row in df.head(100).iterrows():
            results.append({
                "program_id": str(row.get("program_id", row.get("id", ""))),
                "specialty": str(row.get("specialty", row.get("specialty_name", ""))),
                "institution": str(row.get("institution", row.get("sponsor_institution", ""))),
                "city": str(row.get("city", "")),
                "state": str(row.get("state", row.get("st", ""))),
                "total_positions": int(float(row.get("total_positions", row.get("approved_positions", 0)) or 0)),
                "filled_positions": int(float(row.get("filled_positions", row.get("on_duty", 0)) or 0)),
                "accreditation_status": str(row.get("accreditation_status", row.get("status", ""))),
            })

        return results

    except Exception as e:
        logger.warning("ACGME query failed: %s", e)
        return []
