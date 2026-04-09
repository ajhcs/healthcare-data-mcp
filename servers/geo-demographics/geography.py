"""ZCTA shapefile loading, adjacency computation, and geographic utilities."""

import json
import logging
import zipfile
from pathlib import Path

import geopandas as gpd

from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

TIGER_ZCTA_URL = "https://www2.census.gov/geo/tiger/TIGER2024/ZCTA520/tl_2024_us_zcta520.zip"
ZCTA_SHAPEFILE_CACHE = CACHE_DIR / "tl_2024_us_zcta520"
ADJACENCY_CACHE = CACHE_DIR / "zcta_adjacency.json"

# Module-level caches
_zcta_gdf: gpd.GeoDataFrame | None = None
_adjacency_dict: dict[str, list[str]] | None = None


async def _download_zcta_shapefile() -> Path:
    """Download and extract the TIGER/Line ZCTA shapefile."""
    shp_path = ZCTA_SHAPEFILE_CACHE / "tl_2024_us_zcta520.shp"
    if shp_path.exists():
        logger.info("ZCTA shapefile already cached at %s", shp_path)
        return shp_path

    zip_path = CACHE_DIR / "tl_2024_us_zcta520.zip"
    if not zip_path.exists():
        logger.info("Downloading ZCTA shapefile from %s ...", TIGER_ZCTA_URL)
        resp = await resilient_request("GET", TIGER_ZCTA_URL, timeout=600.0)
        zip_path.write_bytes(resp.content)
        logger.info("Downloaded ZCTA shapefile (%d MB)", zip_path.stat().st_size // (1024 * 1024))

    ZCTA_SHAPEFILE_CACHE.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting ZCTA shapefile...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(ZCTA_SHAPEFILE_CACHE)

    if shp_path.exists():
        return shp_path

    # Fallback: find the .shp in the extracted directory
    shp_files = list(ZCTA_SHAPEFILE_CACHE.glob("*.shp"))
    if shp_files:
        return shp_files[0]

    raise FileNotFoundError(f"No .shp file found in {ZCTA_SHAPEFILE_CACHE}")


async def load_zcta_geodataframe() -> gpd.GeoDataFrame:
    """Load the ZCTA shapefile into a GeoDataFrame (cached in memory)."""
    global _zcta_gdf
    if _zcta_gdf is not None:
        return _zcta_gdf

    shp_path = await _download_zcta_shapefile()
    logger.info("Loading ZCTA shapefile into GeoDataFrame...")
    gdf = gpd.read_file(shp_path)
    # Normalize the ZCTA column name (varies by year: ZCTA5CE20, ZCTA5CE10, etc.)
    zcta_col = None
    for col in gdf.columns:
        if col.upper().startswith("ZCTA5CE") or col.upper().startswith("ZCTA"):
            zcta_col = col
            break
    if zcta_col and zcta_col != "ZCTA5CE20":
        gdf = gdf.rename(columns={zcta_col: "ZCTA5CE20"})

    _zcta_gdf = gdf
    logger.info("Loaded %d ZCTA geometries", len(gdf))
    return gdf


def _build_adjacency(gdf: gpd.GeoDataFrame) -> dict[str, list[str]]:
    """Build adjacency dict from GeoDataFrame using spatial index for efficiency."""
    logger.info("Building ZCTA adjacency graph (this may take a few minutes)...")
    sindex = gdf.sindex
    adjacency: dict[str, list[str]] = {}

    for idx, row in gdf.iterrows():
        zcta = row["ZCTA5CE20"]
        geom = row.geometry
        # Use spatial index to find candidates
        candidates_idx = list(sindex.intersection(geom.bounds))
        neighbors = []
        for cidx in candidates_idx:
            if cidx == idx:
                continue
            candidate = gdf.iloc[cidx]
            if geom.touches(candidate.geometry) or geom.intersects(candidate.geometry):
                neighbor_zcta = candidate["ZCTA5CE20"]
                if neighbor_zcta != zcta:
                    neighbors.append(neighbor_zcta)
        adjacency[zcta] = sorted(set(neighbors))

    return adjacency


async def get_adjacency_dict() -> dict[str, list[str]]:
    """Get the ZCTA adjacency dictionary, computing and caching if needed."""
    global _adjacency_dict
    if _adjacency_dict is not None:
        return _adjacency_dict

    # Try loading from cache file
    if ADJACENCY_CACHE.exists():
        logger.info("Loading adjacency cache from %s", ADJACENCY_CACHE)
        with open(ADJACENCY_CACHE) as f:
            _adjacency_dict = json.load(f)
        return _adjacency_dict

    # Build from shapefile
    gdf = await load_zcta_geodataframe()
    _adjacency_dict = _build_adjacency(gdf)

    # Save to cache
    with open(ADJACENCY_CACHE, "w") as f:
        json.dump(_adjacency_dict, f)
    logger.info("Adjacency cache saved to %s (%d ZCTAs)", ADJACENCY_CACHE, len(_adjacency_dict))

    return _adjacency_dict


async def get_adjacent_zctas(zcta: str) -> list[str]:
    """Get list of ZCTAs adjacent to the given ZCTA."""
    adj = await get_adjacency_dict()
    return adj.get(zcta, [])
