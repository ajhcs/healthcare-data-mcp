"""Labor data: NLRB union elections and BLS work stoppages.

Sources:
- labordata/nlrb-data: https://github.com/labordata/nlrb-data
- BLS Work Stoppages: https://www.bls.gov/wsp/
"""

import logging
import sqlite3
import zipfile
from pathlib import Path

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

_NLRB_DB = _CACHE_DIR / "nlrb.db"
_STOPPAGES_CACHE = _CACHE_DIR / "work_stoppages.parquet"
_CACHE_TTL_DAYS = 7  # NLRB updates nightly

NLRB_DB_URL = "https://github.com/labordata/nlrb-data/releases/download/nightly/nlrb.db.zip"
BLS_STOPPAGES_URL = "https://download.bls.gov/pub/time.series/ws/ws.data.1.AllData"

# Healthcare employer name patterns for text-based filtering
HEALTHCARE_PATTERNS = [
    "hospital", "medical center", "health system", "healthcare",
    "health care", "nursing", "clinic", "physician", "ambulance",
    "home health", "hospice", "rehabilitation", "pharmacy",
    "kaiser", "hca ", "ascension", "commonspirit", "tenet",
    "community health", "universal health", "trinity health",
]


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    """Check if a cached file exists and is within TTL."""
    return is_cache_valid(path, max_age_days=ttl_days)


# ---------------------------------------------------------------------------
# NLRB SQLite Database
# ---------------------------------------------------------------------------

async def ensure_nlrb_cached() -> bool:
    """Download NLRB SQLite database from GitHub."""
    if _is_cache_valid(_NLRB_DB):
        return True

    logger.info("Downloading NLRB database...")
    try:
        resp = await resilient_request("GET", NLRB_DB_URL, timeout=300.0)

        zip_path = _CACHE_DIR / "nlrb.db.zip"
        zip_path.write_bytes(resp.content)

        with zipfile.ZipFile(zip_path) as zf:
            db_files = [f for f in zf.namelist() if f.endswith(".db")]
            if db_files:
                zf.extract(db_files[0], _CACHE_DIR)
                extracted = _CACHE_DIR / db_files[0]
                if extracted != _NLRB_DB:
                    extracted.rename(_NLRB_DB)

        zip_path.unlink(missing_ok=True)
        logger.info("NLRB database cached: %s", _NLRB_DB)
        return True

    except Exception as e:
        logger.warning("Failed to download NLRB database: %s", e)
        return False


def _is_healthcare_employer(name: str) -> bool:
    """Check if an employer name looks healthcare-related."""
    lower = name.lower()
    return any(p in lower for p in HEALTHCARE_PATTERNS)


def search_nlrb_elections(
    employer_name: str = "",
    state: str = "",
    year_start: int = 2015,
    year_end: int = 2026,
    limit: int = 50,
) -> list[dict]:
    """Search NLRB election records, filtered to healthcare employers.

    Joins filing (employer info) + election (dates/unit size) + participant (union name).
    """
    if not _NLRB_DB.exists():
        return []

    try:
        con = sqlite3.connect(str(_NLRB_DB))
        con.row_factory = sqlite3.Row

        where_parts = []
        params: list = []

        if employer_name:
            where_parts.append("LOWER(f.name) LIKE ?")
            params.append(f"%{employer_name.lower()}%")

        if state:
            where_parts.append("UPPER(f.state) = ?")
            params.append(state.upper())

        where_parts.append("SUBSTR(f.date_filed, 1, 4) BETWEEN ? AND ?")
        params.extend([str(year_start), str(year_end)])

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        query = f"""
            SELECT
                f.case_number,
                f.name AS employer,
                f.city,
                f.state,
                f.date_filed,
                f.status,
                f.number_of_eligible_voters,
                e.date AS election_date,
                e.unit_size,
                p.name AS union_name
            FROM filing f
            LEFT JOIN election e ON f.case_number = e.case_number
            LEFT JOIN (
                SELECT case_number, participant AS name
                FROM participant
                WHERE type = 'Petitioner' AND subtype = 'Union'
                GROUP BY case_number
            ) p ON f.case_number = p.case_number
            WHERE {where_clause}
            ORDER BY f.date_filed DESC
            LIMIT ?
        """
        # If no specific employer, overfetch for healthcare filtering
        fetch_limit = limit * 3 if not employer_name else limit
        params.append(fetch_limit)

        rows = con.execute(query, params).fetchall()
        con.close()

        results = []
        for row in rows:
            r = dict(row)
            name = r.get("employer", "")

            # Filter to healthcare if no specific employer search
            if not employer_name and not _is_healthcare_employer(name):
                continue

            results.append({
                "case_number": r.get("case_number", ""),
                "employer": name,
                "union": r.get("union_name", "") or "",
                "date": r.get("election_date", "") or r.get("date_filed", ""),
                "result": r.get("status", ""),
                "unit_size": int(r.get("unit_size") or r.get("number_of_eligible_voters") or 0),
                "city": r.get("city", ""),
                "state": r.get("state", ""),
            })

            if len(results) >= limit:
                break

        return results

    except Exception as e:
        logger.warning("NLRB query failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# BLS Work Stoppages
# ---------------------------------------------------------------------------

async def ensure_stoppages_cached() -> bool:
    """Download BLS work stoppage data."""
    if _is_cache_valid(_STOPPAGES_CACHE, ttl_days=30):
        return True

    logger.info("Downloading BLS work stoppage data...")
    try:
        resp = await resilient_request("GET", BLS_STOPPAGES_URL, timeout=120.0)

        # Tab-delimited file
        lines = resp.text.strip().split("\n")
        if len(lines) < 2:
            return False

        # Parse header and data
        header = [h.strip() for h in lines[0].split("\t")]
        data_rows = []
        for line in lines[1:]:
            values = [v.strip() for v in line.split("\t")]
            if len(values) == len(header):
                data_rows.append(dict(zip(header, values)))

        df = pd.DataFrame(data_rows)
        df.to_parquet(_STOPPAGES_CACHE, compression="zstd", index=False)

        logger.info("Work stoppages cached: %d records", len(df))
        return True

    except Exception as e:
        logger.warning("Failed to cache work stoppages: %s", e)
        return False


def query_work_stoppages(year_start: int = 2015, year_end: int = 2026) -> list[dict]:
    """Query cached BLS work stoppage data."""
    if not _STOPPAGES_CACHE.exists():
        return []

    try:
        import duckdb
        from shared.utils.duckdb_safe import safe_parquet_sql
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW ws AS SELECT * FROM {safe_parquet_sql(_STOPPAGES_CACHE)}")

        cols = [r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='ws'"
        ).fetchall()]

        year_col = next((c for c in cols if "year" in c.lower()), None)
        val_col = next((c for c in cols if "value" in c.lower()), None)
        series_col = next((c for c in cols if "series" in c.lower()), None)

        if year_col:
            rows = con.execute(f"""
                SELECT * FROM ws
                WHERE CAST({year_col} AS INTEGER) BETWEEN ? AND ?
                LIMIT 200
            """, [year_start, year_end]).fetchdf()
        else:
            rows = con.execute("SELECT * FROM ws LIMIT 200").fetchdf()

        con.close()

        results = []
        for _, row in rows.iterrows():
            results.append({
                "series_id": str(row.get(series_col, "")) if series_col else "",
                "year": str(row.get(year_col, "")) if year_col else "",
                "value": str(row.get(val_col, "")) if val_col else "",
            })

        return results

    except Exception as e:
        logger.warning("Work stoppages query failed: %s", e)
        return []
