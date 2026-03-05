"""Bulk data loaders for CMS Medicare Provider Utilization PUFs.

Downloads inpatient and outpatient PUF CSV files from data.cms.gov,
converts to Parquet with zstd compression, and queries with DuckDB.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import httpx
import pandas as pd

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "claims-analytics"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_CACHE_TTL_DAYS = 90

# CMS data.cms.gov download URLs for Inpatient PUF (by Provider and Service)
INPATIENT_URLS: dict[str, str] = {
    "2023": "https://data.cms.gov/sites/default/files/2025-05/ca1c9013-8c7c-4560-a4a1-28cf7e43ccc8/MUP_INP_RY25_P03_V10_DY23_PrvSvc.CSV",
    "2022": "https://data.cms.gov/sites/default/files/2024-05/0b6cc46f-0264-4a07-b2c4-be3a34fd0498/MUP_INP_RY24_P03_V10_DY22_PrvSvc.CSV",
    "2021": "https://data.cms.gov/sites/default/files/2023-05/3e0e2616-8ff3-4d64-b10b-07273e90d8ff/MUP_INP_RY23_P03_V10_DY21_PrvSvc.CSV",
}

# CMS data.cms.gov download URLs for Outpatient PUF (by Provider and Service)
OUTPATIENT_URLS: dict[str, str] = {
    "2023": "https://data.cms.gov/sites/default/files/2025-08/bceaa5e1-e58c-4109-9f05-832fc5e6bbc8/MUP_OUT_RY25_P04_V10_DY23_Prov_Svc.csv",
    "2022": "https://data.cms.gov/sites/default/files/2024-08/8b607225-c733-4f89-9490-9a84a5687604/MUP_OUT_RY24_P04_V10_DY22_Prov_Svc.csv",
    "2021": "https://data.cms.gov/sites/default/files/2023-08/d42dfca1-844a-4tried-a7e0-bcdafc7a0727/MUP_OUT_RY23_P04_V10_DY21_Prov_Svc.csv",
}

# Available years (most recent first)
AVAILABLE_YEARS = ["2023", "2022", "2021"]
LATEST_YEAR = "2023"


def _cache_path(dataset: str, year: str) -> Path:
    """Get Parquet cache path for a dataset and year."""
    return _CACHE_DIR / f"{dataset}_dy{year[-2:]}.parquet"


def _is_cache_valid(path: Path, ttl_days: int = _CACHE_TTL_DAYS) -> bool:
    """Check if a cached file exists and is within TTL."""
    if not path.exists():
        return False
    age_days = (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86400
    return age_days < ttl_days


async def _download_and_cache_csv(url: str, cache_path: Path, dataset_name: str) -> bool:
    """Download CSV from CMS and cache as Parquet."""
    logger.info("Downloading %s from %s ...", dataset_name, url[:80])
    try:
        async with httpx.AsyncClient(timeout=300.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        csv_path = _CACHE_DIR / f"{cache_path.stem}_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(cache_path, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("%s cached: %d records -> %s", dataset_name, len(df), cache_path.name)
        return True

    except Exception as e:
        logger.warning("Failed to download %s: %s", dataset_name, e)
        return False


async def ensure_inpatient_cached(year: str = LATEST_YEAR) -> bool:
    """Ensure inpatient PUF for a given year is downloaded and cached."""
    path = _cache_path("inpatient", year)
    if _is_cache_valid(path):
        return True

    url = INPATIENT_URLS.get(year)
    if not url:
        logger.warning("No inpatient PUF URL for year %s", year)
        return False

    return await _download_and_cache_csv(url, path, f"Inpatient PUF DY{year}")


async def ensure_outpatient_cached(year: str = LATEST_YEAR) -> bool:
    """Ensure outpatient PUF for a given year is downloaded and cached."""
    path = _cache_path("outpatient", year)
    if _is_cache_valid(path):
        return True

    url = OUTPATIENT_URLS.get(year)
    if not url:
        logger.warning("No outpatient PUF URL for year %s", year)
        return False

    return await _download_and_cache_csv(url, path, f"Outpatient PUF DY{year}")


async def ensure_all_years_cached(include_outpatient: bool = True) -> list[str]:
    """Cache all available years. Returns list of years successfully cached."""
    cached_years = []
    for year in AVAILABLE_YEARS:
        inp_ok = await ensure_inpatient_cached(year)
        if include_outpatient:
            out_ok = await ensure_outpatient_cached(year)
            if inp_ok and out_ok:
                cached_years.append(year)
        elif inp_ok:
            cached_years.append(year)
    return cached_years


def _get_con_with_view(dataset: str, year: str) -> duckdb.DuckDBPyConnection | None:
    """Create DuckDB connection with a view for the cached Parquet file."""
    path = _cache_path(dataset, year)
    if not path.exists():
        return None
    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW data AS SELECT * FROM read_parquet('{path}')")
    return con


def _detect_columns(con: duckdb.DuckDBPyConnection) -> dict[str, str | None]:
    """Detect column names dynamically (CMS data has inconsistent naming)."""
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='data'"
    ).fetchall()]

    return {
        "ccn": next((c for c in cols if c in (
            "rndrng_prvdr_ccn", "prvdr_ccn", "provider_ccn", "ccn"
        )), None),
        "provider_name": next((c for c in cols if c in (
            "rndrng_prvdr_org_name", "prvdr_org_name", "provider_name", "hospital_name"
        )), None),
        "state": next((c for c in cols if c in (
            "rndrng_prvdr_state_abrvtn", "prvdr_state_abrvtn", "state"
        )), None),
        "drg_code": next((c for c in cols if c in (
            "drg_cd", "drg_code", "ms_drg_cd"
        )), None),
        "drg_desc": next((c for c in cols if c in (
            "drg_desc", "drg_description", "ms_drg_desc"
        )), None),
        "discharges": next((c for c in cols if c in (
            "tot_dschrgs", "total_discharges", "discharges"
        )), None),
        "avg_charges": next((c for c in cols if c in (
            "avg_submtd_chrgs", "avg_submitted_charges", "avg_charges"
        )), None),
        "avg_total_payment": next((c for c in cols if c in (
            "avg_tot_pymt_amt", "avg_total_payment", "avg_tot_payment"
        )), None),
        "avg_medicare_payment": next((c for c in cols if c in (
            "avg_mdcr_pymt_amt", "avg_medicare_payment", "avg_mdcr_payment"
        )), None),
        # Outpatient-specific
        "apc_code": next((c for c in cols if c in (
            "apc_cd", "apc_code", "apc"
        )), None),
        "apc_desc": next((c for c in cols if c in (
            "apc_desc", "apc_description"
        )), None),
        "services": next((c for c in cols if c in (
            "outptnt_srvcs", "outpatient_services", "services", "capc_srvcs"
        )), None),
    }


def query_inpatient(
    year: str = LATEST_YEAR,
    ccn: str = "",
    ccns: list[str] | None = None,
    drg_code: str = "",
) -> list[dict]:
    """Query cached inpatient PUF data.

    Args:
        year: Discharge year.
        ccn: Single CCN to filter.
        ccns: List of CCNs (for market analysis).
        drg_code: Filter to specific DRG.
    """
    con = _get_con_with_view("inpatient", year)
    if con is None:
        return []

    try:
        col_map = _detect_columns(con)
        ccn_col = col_map["ccn"]
        if not ccn_col:
            con.close()
            return []

        where_parts: list[str] = []
        params: list[str] = []

        if ccn:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        elif ccns:
            placeholders = ", ".join(["?"] * len(ccns))
            where_parts.append(f"TRIM({ccn_col}) IN ({placeholders})")
            params.extend([c.strip() for c in ccns])

        drg_col = col_map["drg_code"]
        if drg_code and drg_col:
            where_parts.append(f"TRIM({drg_col}) = ?")
            params.append(drg_code.strip())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        rows = con.execute(f"SELECT * FROM data WHERE {where}", params).fetchdf()
        con.close()

        results: list[dict] = []
        name_col = col_map["provider_name"]
        state_col = col_map["state"]
        drg_cd_col = col_map["drg_code"]
        desc_col = col_map["drg_desc"]
        disch_col = col_map["discharges"]
        chrg_col = col_map["avg_charges"]
        tot_col = col_map["avg_total_payment"]
        mcr_col = col_map["avg_medicare_payment"]

        for _, row in rows.iterrows():
            def _str(col: str | None) -> str:
                return str(row.get(col, "")).strip() if col and col in row.index else ""

            def _float(col: str | None) -> float:
                v = _str(col)
                try:
                    return float(v.replace(",", "")) if v else 0.0
                except ValueError:
                    return 0.0

            results.append({
                "ccn": _str(ccn_col),
                "provider_name": _str(name_col),
                "state": _str(state_col),
                "drg_code": _str(drg_cd_col),
                "drg_desc": _str(desc_col),
                "discharges": int(_float(disch_col)),
                "avg_charges": _float(chrg_col),
                "avg_total_payment": _float(tot_col),
                "avg_medicare_payment": _float(mcr_col),
            })

        return results

    except Exception as e:
        logger.warning("Inpatient query failed: %s", e)
        if con:
            con.close()
        return []


def query_outpatient(
    year: str = LATEST_YEAR,
    ccn: str = "",
    ccns: list[str] | None = None,
    apc_code: str = "",
) -> list[dict]:
    """Query cached outpatient PUF data."""
    con = _get_con_with_view("outpatient", year)
    if con is None:
        return []

    try:
        col_map = _detect_columns(con)
        ccn_col = col_map["ccn"]
        if not ccn_col:
            con.close()
            return []

        where_parts: list[str] = []
        params: list[str] = []

        if ccn:
            where_parts.append(f"TRIM({ccn_col}) = ?")
            params.append(ccn.strip())
        elif ccns:
            placeholders = ", ".join(["?"] * len(ccns))
            where_parts.append(f"TRIM({ccn_col}) IN ({placeholders})")
            params.extend([c.strip() for c in ccns])

        apc_col = col_map["apc_code"]
        if apc_code and apc_col:
            where_parts.append(f"TRIM({apc_col}) = ?")
            params.append(apc_code.strip())

        where = " AND ".join(where_parts) if where_parts else "1=1"
        rows = con.execute(f"SELECT * FROM data WHERE {where}", params).fetchdf()
        con.close()

        results: list[dict] = []
        name_col = col_map["provider_name"]
        state_col = col_map["state"]
        apc_cd_col = col_map["apc_code"]
        apc_desc_col = col_map["apc_desc"]
        svc_col = col_map["services"]
        chrg_col = col_map["avg_charges"]
        tot_col = col_map["avg_total_payment"]
        mcr_col = col_map["avg_medicare_payment"]

        for _, row in rows.iterrows():
            def _str(col: str | None) -> str:
                return str(row.get(col, "")).strip() if col and col in row.index else ""

            def _float(col: str | None) -> float:
                v = _str(col)
                try:
                    return float(v.replace(",", "")) if v else 0.0
                except ValueError:
                    return 0.0

            results.append({
                "ccn": _str(ccn_col),
                "provider_name": _str(name_col),
                "state": _str(state_col),
                "apc_code": _str(apc_cd_col),
                "apc_desc": _str(apc_desc_col),
                "services": int(_float(svc_col)),
                "avg_charges": _float(chrg_col),
                "avg_total_payment": _float(tot_col),
                "avg_medicare_payment": _float(mcr_col),
            })

        return results

    except Exception as e:
        logger.warning("Outpatient query failed: %s", e)
        if con:
            con.close()
        return []
