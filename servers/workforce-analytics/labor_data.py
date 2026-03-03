"""Labor data: NLRB union elections and BLS work stoppages.

Sources:
- labordata/nlrb-data: https://github.com/labordata/nlrb-data
- BLS Work Stoppages: https://www.bls.gov/wsp/
"""

import logging
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

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
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


# ---------------------------------------------------------------------------
# NLRB SQLite Database
# ---------------------------------------------------------------------------

async def ensure_nlrb_cached() -> bool:
    """Download NLRB SQLite database from GitHub."""
    if _is_cache_valid(_NLRB_DB):
        return True

    logger.info("Downloading NLRB database...")
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(NLRB_DB_URL)
            resp.raise_for_status()

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
    """Search NLRB election records, filtered to healthcare employers."""
    if not _NLRB_DB.exists():
        return []

    try:
        con = sqlite3.connect(str(_NLRB_DB))
        con.row_factory = sqlite3.Row

        # Discover table names
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]

        # Look for elections table
        election_table = next(
            (t for t in tables if "election" in t.lower() or "case" in t.lower()),
            tables[0] if tables else None,
        )

        if not election_table:
            con.close()
            return []

        # Get column names
        cols = [info[1] for info in con.execute(f"PRAGMA table_info({election_table})").fetchall()]

        name_col = next((c for c in cols if c in ("name", "employer", "employer_name")), None)
        state_col = next((c for c in cols if c == "state"), None)
        date_col = next((c for c in cols if "date" in c and "filed" in c), cols[0] if cols else None)
        case_col = next((c for c in cols if "case" in c), None)
        union_col = next((c for c in cols if "union" in c or "labor_organization" in c), None)
        voters_col = next((c for c in cols if "eligible" in c or "voter" in c), None)
        status_col = next((c for c in cols if "status" in c or "reason" in c), None)

        where_parts = []
        params: list = []

        if employer_name and name_col:
            where_parts.append(f"LOWER({name_col}) LIKE ?")
            params.append(f"%{employer_name.lower()}%")

        if state and state_col:
            where_parts.append(f"UPPER({state_col}) = ?")
            params.append(state.upper())

        if date_col:
            where_parts.append(f"SUBSTR({date_col},1,4) BETWEEN ? AND ?")
            params.extend([str(year_start), str(year_end)])

        where = " AND ".join(where_parts) if where_parts else "1=1"

        rows = con.execute(
            f"SELECT * FROM {election_table} WHERE {where} LIMIT ?",
            params + [limit * 3],  # Overfetch for healthcare filtering
        ).fetchall()
        con.close()

        results = []
        for row in rows:
            r = dict(row)
            name = str(r.get(name_col, "")) if name_col else ""

            # Filter to healthcare if no specific employer search
            if not employer_name and not _is_healthcare_employer(name):
                continue

            results.append({
                "case_number": str(r.get(case_col, "")) if case_col else "",
                "employer": name,
                "union": str(r.get(union_col, "")) if union_col else "",
                "date": str(r.get(date_col, "")) if date_col else "",
                "result": str(r.get(status_col, "")) if status_col else "",
                "unit_size": int(float(r.get(voters_col, 0) or 0)) if voters_col else 0,
                "city": str(r.get("city", "")),
                "state": str(r.get(state_col, "")) if state_col else "",
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
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(BLS_STOPPAGES_URL)
            resp.raise_for_status()

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
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW ws AS SELECT * FROM read_parquet('{_STOPPAGES_CACHE}')")

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
