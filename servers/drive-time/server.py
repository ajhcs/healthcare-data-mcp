"""Drive Time & Accessibility MCP Server.

Computes travel times between locations and healthcare facilities, generates
isochrone polygons, and scores spatial accessibility using the Enhanced Two-Step
Floating Catchment Area (E2SFCA) method.

Routing backends:
  - OSRM (route, table): public demo or self-hosted via OSRM_BASE_URL
  - OpenRouteService (isochrones): requires ORS_API_KEY

NOTE: The OSRM public demo server (router.project-osrm.org) is rate-limited.
For production use, deploy a self-hosted OSRM instance and set OSRM_BASE_URL.
"""

import io
import json
import logging
import os
import zipfile

import pandas as pd
from mcp.server.fastmcp import FastMCP
from shared.utils.http_client import resilient_request

from .accessibility import compute_e2sfca, summarize_scores
from .models import (
    AccessibilityResult,
    AccessibilitySummary,
    MatrixEntry,
    NearbyFacility,
    RouteResult,
)
from .routing import ORSRouter, OSRMRouter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OSRM_BASE_URL = os.environ.get("OSRM_BASE_URL", "http://router.project-osrm.org")
ORS_API_KEY = os.environ.get("ORS_API_KEY", "")

METERS_PER_MILE = 1609.344

# Facility data cache
CACHE_DIR = os.path.join(os.path.expanduser("~"), ".healthcare-data-mcp", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
HOSPITAL_INFO_URL = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0/download?format=csv"

# In-memory cache of facility DataFrame
_facility_df: pd.DataFrame | None = None

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------
_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "drive-time"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8004"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_osrm() -> OSRMRouter:
    return OSRMRouter(base_url=OSRM_BASE_URL)


def _get_ors() -> ORSRouter:
    if not ORS_API_KEY:
        raise ValueError("ORS_API_KEY environment variable is required for isochrone generation")
    return ORSRouter(api_key=ORS_API_KEY)


async def _load_facilities() -> pd.DataFrame:
    """Load CMS Hospital General Info, downloading and caching if needed."""
    global _facility_df
    if _facility_df is not None:
        return _facility_df


    cache_path = os.path.join(CACHE_DIR, "hospital_general_info.csv")
    if not os.path.exists(cache_path):
        logger.info("Downloading Hospital General Info from CMS...")
        resp = await resilient_request("GET", HOSPITAL_INFO_URL, timeout=300.0)
        with open(cache_path, "wb") as f:
            f.write(resp.content)
        logger.info("Saved to %s", cache_path)

    df = pd.read_csv(cache_path, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    _facility_df = df
    return df


# Census Gazetteer ZIP centroid file (~1MB ZIP archive, pipe-delimited inside)
GAZETTEER_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_zcta_national.zip"
_ZIP_CENTROIDS: dict[str, tuple[float, float]] | None = None


async def _ensure_zip_centroids() -> dict[str, tuple[float, float]]:
    """Load ZIP → (lat, lon) centroid mapping from Census Gazetteer."""
    global _ZIP_CENTROIDS
    if _ZIP_CENTROIDS is not None:
        return _ZIP_CENTROIDS

    cache_path = os.path.join(CACHE_DIR, "zip_centroids.txt")
    if not os.path.exists(cache_path):
        logger.info("Downloading Census ZIP centroids...")
        resp = await resilient_request("GET", GAZETTEER_URL, timeout=120.0)
        # Extract the text file from the ZIP archive
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            txt_files = [n for n in zf.namelist() if n.endswith(".txt")]
            content = zf.read(txt_files[0]) if txt_files else zf.read(zf.namelist()[0])
        with open(cache_path, "wb") as f:
            f.write(content)

    df = pd.read_csv(cache_path, sep="\t", dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    _ZIP_CENTROIDS = {}
    for _, row in df.iterrows():
        z = str(row.get("GEOID", "")).strip().zfill(5)
        try:
            lat = float(row.get("INTPTLAT", 0))
            lon = float(row.get("INTPTLONG", row.get("INTPTLON", 0)))
            if lat and lon:
                _ZIP_CENTROIDS[z] = (lat, lon)
        except (ValueError, TypeError):
            pass

    logger.info("Loaded %d ZIP centroids", len(_ZIP_CENTROIDS))
    return _ZIP_CENTROIDS


async def _parse_lat_lon(df: pd.DataFrame) -> pd.DataFrame:
    """Extract lat/lon columns from the facility DataFrame.

    Tries direct lat/lon columns first, then JSON 'location' column,
    then falls back to ZIP code centroid geocoding.
    """
    if "latitude" in df.columns and "longitude" in df.columns:
        df = df.copy()
        df["_lat"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["_lon"] = pd.to_numeric(df["longitude"], errors="coerce")
        return df

    if "location" in df.columns:
        df = df.copy()

        def _extract(val: str, key: str) -> float | None:
            try:
                obj = json.loads(val)
                return float(obj[key])
            except Exception:
                return None

        df["_lat"] = df["location"].apply(lambda v: _extract(v, "latitude"))
        df["_lon"] = df["location"].apply(lambda v: _extract(v, "longitude"))
        return df

    # Fallback: geocode via ZIP centroid
    zip_col = next((c for c in df.columns if c in ("zip_code", "zip", "zipcode")), None)
    if zip_col:
        centroids = await _ensure_zip_centroids()
        df = df.copy()
        df["_lat"] = df[zip_col].apply(
            lambda z: centroids.get(str(z).strip().zfill(5), (None, None))[0]
        )
        df["_lon"] = df[zip_col].apply(
            lambda z: centroids.get(str(z).strip().zfill(5), (None, None))[1]
        )
        logger.info("Geocoded %d/%d facilities via ZIP centroids",
                     df["_lat"].notna().sum(), len(df))
        return df

    raise ValueError("Cannot find latitude/longitude columns in facility data")


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def compute_drive_time(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> str:
    """Compute driving time and distance between two geographic points.

    Uses the OSRM routing engine. Returns duration in seconds/minutes
    and distance in meters/miles.
    """
    router = _get_osrm()
    result = await router.route(
        origin=(origin_lon, origin_lat),
        dest=(dest_lon, dest_lat),
    )
    route = RouteResult(
        duration_seconds=result["duration_seconds"],
        duration_minutes=round(result["duration_seconds"] / 60, 2),
        distance_meters=result["distance_meters"],
        distance_miles=round(result["distance_meters"] / METERS_PER_MILE, 2),
    )
    return route.model_dump_json(indent=2)


@mcp.tool()
async def compute_drive_time_matrix(
    origins: list[dict],
    destinations: list[dict],
) -> str:
    """Compute an NxM driving time matrix between origins and destinations.

    Each origin/destination should be a dict with keys: lat, lon, id.
    Uses the OSRM table endpoint for efficient bulk computation.
    Returns a matrix of drive times (minutes) with origin and destination IDs.
    """
    # Validate inputs
    if not origins or not destinations:
        return json.dumps({"error": "origins and destinations must be non-empty"})

    origin_ids = [o.get("id", f"origin_{i}") for i, o in enumerate(origins)]
    dest_ids = [d.get("id", f"dest_{i}") for i, d in enumerate(destinations)]

    # Build combined coord list: origins first, then destinations
    coords: list[tuple[float, float]] = []
    for o in origins:
        coords.append((float(o["lon"]), float(o["lat"])))
    for d in destinations:
        coords.append((float(d["lon"]), float(d["lat"])))

    source_indices = list(range(len(origins)))
    dest_indices = list(range(len(origins), len(origins) + len(destinations)))

    router = _get_osrm()
    data = await router.table(coords, sources=source_indices, destinations=dest_indices)

    # Parse the response into structured entries
    durations = data.get("durations", [])
    distances = data.get("distances", [])

    entries: list[dict] = []
    for i, origin_id in enumerate(origin_ids):
        for j, dest_id in enumerate(dest_ids):
            dur = durations[i][j] if durations and durations[i][j] is not None else None
            dist = distances[i][j] if distances and distances[i][j] is not None else None
            entry = MatrixEntry(
                origin_id=origin_id,
                destination_id=dest_id,
                duration_minutes=round(dur / 60, 2) if dur is not None else None,
                distance_miles=round(dist / METERS_PER_MILE, 2) if dist is not None else None,
            )
            entries.append(entry.model_dump())

    result = {
        "origin_ids": origin_ids,
        "destination_ids": dest_ids,
        "matrix": entries,
    }
    return json.dumps(result, indent=2)


@mcp.tool()
async def generate_isochrone(
    lat: float,
    lon: float,
    minutes: list[int] | None = None,
) -> str:
    """Generate drive-time isochrone polygons around a point.

    Creates GeoJSON polygons showing areas reachable within the specified
    time thresholds. Requires ORS_API_KEY environment variable.

    Args:
        lat: Latitude of the center point.
        lon: Longitude of the center point.
        minutes: Time thresholds in minutes (default: [15, 30, 60]).
    """
    if minutes is None:
        minutes = [15, 30, 60]

    ranges_seconds = [m * 60 for m in minutes]
    ors = _get_ors()
    geojson = await ors.isochrone(lon=lon, lat=lat, ranges=ranges_seconds)
    return json.dumps(geojson, indent=2)


@mcp.tool()
async def find_competing_facilities(
    lat: float,
    lon: float,
    radius_minutes: int = 30,
    facility_type: str = "hospital",
) -> str:
    """Find healthcare facilities within a drive-time radius of a point.

    Loads CMS Hospital General Info data, geocodes facilities, and computes
    driving time from the given point to each facility. Returns facilities
    sorted by drive time.

    Args:
        lat: Latitude of the origin point.
        lon: Longitude of the origin point.
        radius_minutes: Maximum drive time in minutes (default 30).
        facility_type: Type filter — "hospital" uses all hospitals.
    """
    df = await _load_facilities()
    df = await _parse_lat_lon(df)
    df = df.dropna(subset=["_lat", "_lon"])

    if df.empty:
        return json.dumps({"error": "No geocoded facilities found in dataset"})

    # Pre-filter by rough bounding box (~1 degree ≈ 60 miles ≈ ~60 min drive)
    # to avoid sending thousands of points to OSRM
    deg_buffer = max(1.0, radius_minutes / 30)
    nearby = df[
        (df["_lat"].between(lat - deg_buffer, lat + deg_buffer))
        & (df["_lon"].between(lon - deg_buffer, lon + deg_buffer))
    ]

    if nearby.empty:
        return json.dumps({"facilities": [], "count": 0, "message": "No facilities found within bounding box"})

    # Cap at 100 facilities per OSRM table request to avoid overloading
    if len(nearby) > 100:
        # Sort by Euclidean distance and keep closest 100
        nearby = nearby.copy()
        nearby["_euc"] = ((nearby["_lat"] - lat) ** 2 + (nearby["_lon"] - lon) ** 2) ** 0.5
        nearby = nearby.nsmallest(100, "_euc")

    # Build coords: origin at index 0, then all facilities
    coords: list[tuple[float, float]] = [(lon, lat)]
    for _, row in nearby.iterrows():
        coords.append((row["_lon"], row["_lat"]))

    router = _get_osrm()
    data = await router.table(coords, sources=[0], destinations=list(range(1, len(coords))))

    durations = data.get("durations", [[]])[0]
    distances_row = data.get("distances", [[]])[0]

    facilities: list[dict] = []
    for idx, (_, row) in enumerate(nearby.iterrows()):
        dur = durations[idx] if idx < len(durations) else None
        dist = distances_row[idx] if idx < len(distances_row) else None
        if dur is None:
            continue
        drive_min = dur / 60
        if drive_min > radius_minutes:
            continue
        fac = NearbyFacility(
            ccn=row.get("facility_id", row.get("provider_id", row.get("ccn", ""))),
            name=row.get("facility_name", row.get("hospital_name", "")),
            address=row.get("address", ""),
            city=row.get("city", row.get("city/town", "")),
            state=row.get("state", ""),
            drive_time_minutes=round(drive_min, 2),
            distance_miles=round(dist / METERS_PER_MILE, 2) if dist is not None else 0.0,
        )
        facilities.append(fac.model_dump())

    facilities.sort(key=lambda f: f["drive_time_minutes"])

    return json.dumps({"facilities": facilities, "count": len(facilities)}, indent=2)


@mcp.tool()
async def compute_accessibility_score(
    demand_points: list[dict],
    supply_points: list[dict],
    catchment_minutes: int = 30,
) -> str:
    """Compute spatial accessibility scores using Enhanced Two-Step Floating Catchment Area (E2SFCA).

    Measures how accessible healthcare supply is from each demand location,
    accounting for competition from other demand points.

    Args:
        demand_points: List of dicts with keys: lat, lon, population, id (optional).
            E.g., ZCTA centroids with population counts.
        supply_points: List of dicts with keys: lat, lon, capacity, id (optional).
            E.g., hospitals with bed counts.
        catchment_minutes: Max travel time for the catchment area (default 30).
    """
    if not demand_points or not supply_points:
        return json.dumps({"error": "demand_points and supply_points must be non-empty"})

    # Build coordinate list: demand points first, then supply points
    coords: list[tuple[float, float]] = []
    for dp in demand_points:
        coords.append((float(dp["lon"]), float(dp["lat"])))
    for sp in supply_points:
        coords.append((float(sp["lon"]), float(sp["lat"])))

    n_demand = len(demand_points)
    n_supply = len(supply_points)
    source_indices = list(range(n_demand))
    dest_indices = list(range(n_demand, n_demand + n_supply))

    # Get travel time matrix from OSRM
    router = _get_osrm()
    data = await router.table(coords, sources=source_indices, destinations=dest_indices)

    raw_durations = data.get("durations", [])

    # Convert to minutes, handling nulls
    travel_matrix: list[list[float | None]] = []
    for row in raw_durations:
        travel_matrix.append([v / 60 if v is not None else None for v in row])

    populations = [float(dp.get("population", 0)) for dp in demand_points]
    capacities = [float(sp.get("capacity", 0)) for sp in supply_points]

    # Run E2SFCA
    scores = compute_e2sfca(travel_matrix, populations, capacities, catchment_minutes)

    # Build results
    results: list[dict] = []
    for i, dp in enumerate(demand_points):
        ar = AccessibilityResult(
            demand_id=dp.get("id", f"demand_{i}"),
            lat=float(dp["lat"]),
            lon=float(dp["lon"]),
            population=populations[i],
            accessibility_score=round(scores[i], 8),
        )
        results.append(ar.model_dump())

    stats = summarize_scores(scores)
    summary = AccessibilitySummary(
        num_demand_points=n_demand,
        num_supply_points=n_supply,
        catchment_minutes=catchment_minutes,
        mean_score=round(stats["mean"], 8),
        median_score=round(stats["median"], 8),
        min_score=round(stats["min"], 8),
        max_score=round(stats["max"], 8),
        std_score=round(stats["std"], 8),
        points_with_zero_access=stats["points_with_zero_access"],
        results=results,
    )
    return summary.model_dump_json(indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport=_transport)
