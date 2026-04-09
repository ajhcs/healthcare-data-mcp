"""Data loaders for CMS Geographic Variation PUF."""

import logging
from pathlib import Path

import duckdb

from shared.utils.http_client import resilient_request
import pandas as pd

import sys as _sys
_project_root = __import__("pathlib").Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in _sys.path:
    _sys.path.insert(0, str(_project_root))

from shared.utils.cache import is_cache_valid  # noqa: E402
from shared.utils.cms_url_resolver import resolve_cms_download_url  # noqa: E402

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "geo-demographics"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_GV_PARQUET = _CACHE_DIR / "geographic_variation.parquet"
_CACHE_TTL_DAYS = 90

GV_CSV_URL = (
    "https://data.cms.gov/sites/default/files/2025-03/"
    "a40ac71d-9f80-4d99-92d2-fd149433d7d8/"
    "2014-2023%20Medicare%20Fee-for-Service%20Geographic%20Variation%20Public%20Use%20File.csv"
)
_GV_DATASET_TITLE = "Medicare Geographic Variation - by National, State & County"


def _is_cache_valid(path: Path) -> bool:
    return is_cache_valid(path, max_age_days=_CACHE_TTL_DAYS)


async def ensure_gv_cached() -> bool:
    """Download GV PUF CSV and convert to Parquet if needed."""
    if _is_cache_valid(_GV_PARQUET):
        return True

    logger.info("Downloading Geographic Variation PUF...")
    try:
        gv_url = await resolve_cms_download_url("gv-puf", "Geographic_Variation")
        if not gv_url:
            raise RuntimeError("Unable to resolve Geographic Variation download URL")

        resp = await resilient_request("GET", gv_url, timeout=600.0)

        csv_path = _CACHE_DIR / "gv_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.to_parquet(_GV_PARQUET, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("GV PUF cached: %d rows", len(df))
        return True
    except Exception as e:
        logger.warning("Failed to cache GV PUF: %s", e)
        return False


def query_gv(geo_level: str, geo_code: str) -> dict | None:
    """Query cached GV Parquet for a geography, returning most recent year.

    Args:
        geo_level: "State" or "County"
        geo_code: State abbreviation (e.g. "PA") or 5-digit county FIPS.
    """
    if not _GV_PARQUET.exists():
        return None

    try:
        from shared.utils.duckdb_safe import safe_parquet_sql
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW gv AS SELECT * FROM {safe_parquet_sql(_GV_PARQUET)}")

        if geo_level == "State" and len(geo_code) == 2 and geo_code.isalpha():
            # State abbreviations are stored in BENE_GEO_DESC, not BENE_GEO_CD
            rows = con.execute("""
                SELECT * FROM gv
                WHERE BENE_GEO_LVL = 'State'
                  AND UPPER(BENE_GEO_DESC) = ?
                ORDER BY CAST(YEAR AS INTEGER) DESC
                LIMIT 1
            """, [geo_code.upper()]).fetchdf()
        else:
            rows = con.execute("""
                SELECT * FROM gv
                WHERE BENE_GEO_LVL = ?
                  AND BENE_GEO_CD = ?
                ORDER BY CAST(YEAR AS INTEGER) DESC
                LIMIT 1
            """, [geo_level, geo_code]).fetchdf()
        con.close()

        if rows.empty:
            return None

        row = rows.iloc[0]

        def _f(col: str) -> float | None:
            v = row.get(col)
            if v is None or str(v).strip() in ("", "*"):
                return None
            try:
                return float(str(v).replace(",", ""))
            except (ValueError, TypeError):
                return None

        def _i(col: str) -> int | None:
            f = _f(col)
            return int(f) if f is not None else None

        return {
            "year": str(row.get("YEAR", "")),
            "geo_level": str(row.get("BENE_GEO_LVL", "")),
            "geo_code": str(row.get("BENE_GEO_CD", "")),
            "geo_desc": str(row.get("BENE_GEO_DESC", "")),
            "total_beneficiaries": _i("BENES_FFS_CNT"),
            "ma_penetration_pct": _f("MA_PRTCPTN_RATE"),
            "avg_age": _f("BENE_AVG_AGE"),
            "pct_female": _f("BENE_FEML_PCT"),
            "pct_dual_eligible": _f("BENE_DUAL_PCT"),
            "per_capita_spending": _f("TOT_MDCR_PYMT_PC"),
            "ip_spending_per_capita": _f("IP_MDCR_PYMT_PC"),
            "op_spending_per_capita": _f("OP_MDCR_PYMT_PC"),
            "physician_spending_per_capita": _f("PHYS_MDCR_PYMT_PC"),
            "snf_spending_per_capita": _f("SNF_MDCR_PYMT_PC"),
            "discharges_per_1000": _f("IP_CVRD_STAYS_PER_1000_BENES"),
            "er_visits_per_1000": _f("ER_VISITS_PER_1000_BENES"),
            "readmission_rate": _f("ACUTE_HOSP_READMSN_PCT"),
        }
    except Exception as e:
        logger.warning("GV query failed: %s", e)
        return None
