"""Referral network analysis using DocGraph shared patient data.

DocGraph (CareSet) provides directed physician-to-physician shared patient
counts derived from Medicare claims (2014-2020). This module caches the data
as Parquet and provides DuckDB-based graph queries.

Data source: https://careset.com/datasets/
"""

import logging
from pathlib import Path

import duckdb
from shared.utils.duckdb_safe import safe_parquet_sql
import httpx

from shared.utils.http_client import resilient_request, get_client
import pandas as pd

from shared.utils.duckdb_safe import safe_parquet_sql

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "docgraph"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_SHARED_PATIENTS_CACHE = _CACHE_DIR / "shared_patients.parquet"

_DARTMOUTH_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "dartmouth"
_DARTMOUTH_DIR.mkdir(parents=True, exist_ok=True)
_HSA_CROSSWALK_CACHE = _DARTMOUTH_DIR / "zip_hsa_hrr.parquet"

# DocGraph CSV expected columns
DOCGRAPH_COLUMNS = ["npi_from", "npi_to", "shared_count", "transaction_count", "same_day_count"]

# Dartmouth Atlas HSA/HRR crosswalk URL
DARTMOUTH_HSA_URL = "https://data.dartmouthatlas.org/downloads/geography/ZipHsaHrr18.csv"

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"


# ---------------------------------------------------------------------------
# DocGraph Data Management
# ---------------------------------------------------------------------------

def is_docgraph_cached() -> bool:
    """Check if DocGraph shared patient data is cached."""
    return _SHARED_PATIENTS_CACHE.exists()


def load_docgraph_csv(csv_path: str | Path) -> int:
    """Load a DocGraph CSV file and convert to Parquet cache.

    The DocGraph Hop Teaming CSV typically has columns:
    - Column 1: NPI of first provider
    - Column 2: NPI of second provider
    - Column 3: Number of shared patients (or transactions)
    Additional columns vary by release year.

    Args:
        csv_path: Path to the downloaded DocGraph CSV file.

    Returns:
        Number of rows loaded.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"DocGraph CSV not found: {path}")

    logger.info("Loading DocGraph CSV: %s", path)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False)

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Map to standard columns (DocGraph releases vary in column names)
    col_map = {}
    for col in df.columns:
        if "from" in col or col in ("npi1", "npi_1", "referring_npi"):
            col_map[col] = "npi_from"
        elif "to" in col or col in ("npi2", "npi_2", "referred_npi"):
            col_map[col] = "npi_to"
        elif "shared" in col or "patient" in col:
            col_map[col] = "shared_count"
        elif "transaction" in col or "claim" in col:
            col_map[col] = "transaction_count"
        elif "same_day" in col or "sameday" in col:
            col_map[col] = "same_day_count"

    df = df.rename(columns=col_map)

    # Ensure required columns exist
    for req_col in ["npi_from", "npi_to"]:
        if req_col not in df.columns:
            # If we have exactly 2-3 unnamed columns, assume standard order
            if len(df.columns) >= 2:
                orig_cols = list(df.columns)
                df = df.rename(columns={orig_cols[0]: "npi_from", orig_cols[1]: "npi_to"})
                if len(orig_cols) >= 3:
                    df = df.rename(columns={orig_cols[2]: "shared_count"})
            else:
                raise ValueError(f"Cannot identify required column '{req_col}' in DocGraph CSV")

    # Add missing optional columns
    if "shared_count" not in df.columns:
        df["shared_count"] = "0"
    if "transaction_count" not in df.columns:
        df["transaction_count"] = "0"
    if "same_day_count" not in df.columns:
        df["same_day_count"] = "0"

    # Convert numeric columns
    for col in ["shared_count", "transaction_count", "same_day_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)  # type: ignore[union-attr]

    # Keep only standard columns
    df = df[["npi_from", "npi_to", "shared_count", "transaction_count", "same_day_count"]]

    # Write Parquet
    df.to_parquet(_SHARED_PATIENTS_CACHE, compression="zstd", index=False)
    logger.info("DocGraph cached: %d referral pairs", len(df))

    return len(df)


# ---------------------------------------------------------------------------
# Dartmouth Atlas HSA/HRR Crosswalk
# ---------------------------------------------------------------------------

async def ensure_hsa_crosswalk_cached() -> bool:
    """Download Dartmouth Atlas ZIP-to-HSA/HRR crosswalk if not cached."""
    if _HSA_CROSSWALK_CACHE.exists():
        return True

    logger.info("Downloading Dartmouth Atlas HSA/HRR crosswalk...")
    try:
        resp = await resilient_request("GET", DARTMOUTH_HSA_URL, timeout=120.0)

        csv_path = _DARTMOUTH_DIR / "zip_hsa_hrr_raw.csv"
        csv_path.write_bytes(resp.content)

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df.to_parquet(_HSA_CROSSWALK_CACHE, compression="zstd", index=False)

        csv_path.unlink(missing_ok=True)
        logger.info("HSA crosswalk cached: %d ZIP codes", len(df))
        return True

    except Exception as e:
        logger.warning("Failed to download HSA crosswalk: %s", e)
        return False


def get_hsa_for_zip(zip_code: str) -> str | None:
    """Look up HSA number for a ZIP code from cached crosswalk."""
    if not _HSA_CROSSWALK_CACHE.exists():
        return None

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hsa AS SELECT * FROM {safe_parquet_sql(_HSA_CROSSWALK_CACHE)}")

        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='hsa'").fetchall()]
        zip_col = next((c for c in cols if "zip" in c), None)
        hsa_col = next((c for c in cols if "hsa" in c and "hrr" not in c), None)

        if not zip_col or not hsa_col:
            con.close()
            return None

        result = con.execute(f"SELECT {hsa_col} FROM hsa WHERE {zip_col} = ? LIMIT 1", [zip_code.strip()[:5]]).fetchone()
        con.close()
        return result[0] if result else None

    except Exception:
        return None


def get_zips_for_hsa(hsa_number: str) -> list[str]:
    """Get all ZIP codes belonging to an HSA."""
    if not _HSA_CROSSWALK_CACHE.exists():
        return []

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE VIEW hsa AS SELECT * FROM {safe_parquet_sql(_HSA_CROSSWALK_CACHE)}")

        cols = [r[0] for r in con.execute("SELECT column_name FROM information_schema.columns WHERE table_name='hsa'").fetchall()]
        zip_col = next((c for c in cols if "zip" in c), None)
        hsa_col = next((c for c in cols if "hsa" in c and "hrr" not in c), None)

        if not zip_col or not hsa_col:
            con.close()
            return []

        results = con.execute(f"SELECT DISTINCT {zip_col} FROM hsa WHERE {hsa_col} = ?", [hsa_number]).fetchall()
        con.close()
        return [r[0] for r in results]

    except Exception:
        return []


# ---------------------------------------------------------------------------
# Referral Network Queries
# ---------------------------------------------------------------------------

def get_referral_network(npi: str, depth: int = 1, min_shared: int = 11) -> dict:
    """Build referral network graph for a physician.

    Args:
        npi: Center NPI.
        depth: 1 for direct connections, 2 for second-hop.
        min_shared: Minimum shared patient count to include (DocGraph suppresses <11).

    Returns:
        Dict with nodes and edges lists.
    """
    if not is_docgraph_cached():
        return {"error": "DocGraph data not cached. Load with load_docgraph_csv()."}

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW dg AS SELECT * FROM {safe_parquet_sql(_SHARED_PATIENTS_CACHE)}")

    # Direct connections (depth 1)
    edges = con.execute("""
        SELECT npi_from, npi_to, shared_count, transaction_count, same_day_count
        FROM dg
        WHERE (npi_from = ? OR npi_to = ?)
          AND shared_count >= ?
        ORDER BY shared_count DESC
        LIMIT 200
    """, [npi, npi, min_shared]).fetchdf()

    if depth >= 2 and not edges.empty:
        # Get second-hop NPIs
        hop1_npis = set(edges["npi_from"].tolist() + edges["npi_to"].tolist())
        hop1_npis.discard(npi)
        if hop1_npis:
            placeholders = ", ".join(["?" for _ in hop1_npis])
            hop2 = con.execute(f"""
                SELECT npi_from, npi_to, shared_count, transaction_count, same_day_count
                FROM dg
                WHERE (npi_from IN ({placeholders}) OR npi_to IN ({placeholders}))
                  AND shared_count >= ?
                ORDER BY shared_count DESC
                LIMIT 500
            """, list(hop1_npis) + list(hop1_npis) + [min_shared]).fetchdf()
            edges = pd.concat([edges, hop2]).drop_duplicates(subset=["npi_from", "npi_to"])

    con.close()

    # Collect unique NPIs for node list
    all_npis = set()
    edge_list = []
    for _, row in edges.iterrows():
        all_npis.add(row["npi_from"])
        all_npis.add(row["npi_to"])
        edge_list.append({
            "npi_from": row["npi_from"],
            "npi_to": row["npi_to"],
            "shared_count": int(row["shared_count"]),
            "transaction_count": int(row["transaction_count"]),
            "same_day_count": int(row["same_day_count"]),
        })

    # Build minimal node list (NPI only — caller can enrich with NPPES)
    nodes = [{"npi": n, "name": "", "specialty": "", "city": "", "state": ""} for n in all_npis]

    return {
        "center_npi": npi,
        "nodes": nodes,
        "edges": edge_list,
        "total_connections": len(edge_list),
    }


def get_top_referral_pairs(npi: str, direction: str = "both", limit: int = 25) -> list[dict]:
    """Get top referral pairs for a physician, ranked by shared patient count.

    Args:
        npi: Target NPI.
        direction: "outgoing" (npi refers to), "incoming" (referred to npi), or "both".
        limit: Max results.

    Returns:
        List of referral pair dicts.
    """
    if not is_docgraph_cached():
        return []

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW dg AS SELECT * FROM {safe_parquet_sql(_SHARED_PATIENTS_CACHE)}")

    if direction == "outgoing":
        where = "npi_from = ?"
    elif direction == "incoming":
        where = "npi_to = ?"
    else:
        where = "(npi_from = ? OR npi_to = ?)"

    params = [npi, npi] if direction == "both" else [npi]

    rows = con.execute(f"""
        SELECT npi_from, npi_to, shared_count, transaction_count, same_day_count
        FROM dg
        WHERE {where}
        ORDER BY shared_count DESC
        LIMIT ?
    """, params + [limit]).fetchdf()
    con.close()

    results = []
    for _, row in rows.iterrows():
        other_npi = row["npi_to"] if row["npi_from"] == npi else row["npi_from"]
        results.append({
            "npi": other_npi,
            "shared_count": int(row["shared_count"]),
            "transaction_count": int(row["transaction_count"]),
            "same_day_count": int(row["same_day_count"]),
            "direction": "outgoing" if row["npi_from"] == npi else "incoming",
        })

    return results


# ---------------------------------------------------------------------------
# Leakage Detection
# ---------------------------------------------------------------------------

def detect_leakage(
    system_npis: set[str],
    system_zips: set[str],
    min_shared: int = 11,
    limit: int = 50,
) -> dict:
    """Detect out-of-network referral leakage for a health system.

    Args:
        system_npis: Set of NPIs belonging to the health system.
        system_zips: Set of ZIP codes in the system's HSA service area.
        min_shared: Minimum shared count threshold.
        limit: Max leakage destinations to return.

    Returns:
        Dict with leakage statistics and top destinations.
    """
    if not is_docgraph_cached():
        return {"error": "DocGraph data not cached."}

    con = duckdb.connect(":memory:")
    con.execute(f"CREATE VIEW dg AS SELECT * FROM {safe_parquet_sql(_SHARED_PATIENTS_CACHE)}")

    # Get all outbound referrals from system NPIs
    npi_list = list(system_npis)
    if not npi_list:
        con.close()
        return {"error": "No system NPIs provided."}

    placeholders = ", ".join(["?" for _ in npi_list])
    outbound = con.execute(f"""
        SELECT npi_to, SUM(shared_count) as total_shared
        FROM dg
        WHERE npi_from IN ({placeholders})
          AND shared_count >= ?
        GROUP BY npi_to
        ORDER BY total_shared DESC
    """, npi_list + [min_shared]).fetchdf()
    con.close()

    if outbound.empty:
        return {
            "total_referrals": 0,
            "in_network_pct": 0.0,
            "out_of_network_in_area_pct": 0.0,
            "out_of_area_pct": 0.0,
            "top_leakage_destinations": [],
            "specialty_breakdown": [],
        }

    # Classify each destination
    total_shared = int(outbound["total_shared"].sum())
    in_network_shared = 0
    out_network_in_area = 0
    out_of_area = 0
    leakage_destinations = []

    for _, row in outbound.iterrows():
        dest_npi = row["npi_to"]
        shared = int(row["total_shared"])

        if dest_npi in system_npis:
            in_network_shared += shared
        else:
            # Check if destination is in service area (would need NPPES lookup for ZIP)
            # For now, classify all out-of-network as potential leakage
            out_of_area += shared
            leakage_destinations.append({
                "npi": dest_npi,
                "name": "",
                "specialty": "",
                "shared_count": shared,
                "city": "",
                "state": "",
                "classification": "out_of_network",
            })

    in_network_pct = (in_network_shared / total_shared * 100) if total_shared > 0 else 0
    out_pct = ((total_shared - in_network_shared) / total_shared * 100) if total_shared > 0 else 0

    return {
        "total_referrals": total_shared,
        "in_network_pct": round(in_network_pct, 1),
        "out_of_network_in_area_pct": 0.0,  # Requires NPPES ZIP lookup enrichment
        "out_of_area_pct": round(out_pct, 1),
        "top_leakage_destinations": leakage_destinations[:limit],
        "specialty_breakdown": [],  # Requires NPPES taxonomy enrichment
    }
