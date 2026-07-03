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
from datetime import UTC, datetime
from typing import Any


from shared.utils.http_client import resilient_request
import pandas as pd
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils.source_backed_result import source_claim

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
HOSPITAL_INFO_LANDING_PAGE = "https://data.cms.gov/provider-data/dataset/xubh-q36u"

# In-memory cache of facility DataFrame
_facility_df: pd.DataFrame | None = None

# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------
_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "drive-time"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8004"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "drive-time")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_osrm() -> OSRMRouter:
    return OSRMRouter(base_url=OSRM_BASE_URL)


def _get_ors() -> ORSRouter:
    if not ORS_API_KEY:
        raise ValueError("ORS_API_KEY environment variable is required for isochrone generation")
    return ORSRouter(api_key=ORS_API_KEY)


def _cache_age_days(cache_path: str) -> int | None:
    if not os.path.exists(cache_path):
        return None
    age_seconds = datetime.now(UTC).timestamp() - os.path.getmtime(cache_path)
    return max(0, int(age_seconds // 86400))


def _cache_mtime(cache_path: str) -> str:
    if not os.path.exists(cache_path):
        return ""
    return datetime.fromtimestamp(os.path.getmtime(cache_path), UTC).isoformat()


def _osrm_source_metadata(operation: str) -> dict[str, Any]:
    public_demo = "router.project-osrm.org" in OSRM_BASE_URL
    return {
        "source_name": "Open Source Routing Machine (OSRM) routing engine",
        "source_url": OSRM_BASE_URL,
        "dataset_id": f"osrm_{operation}",
        "source_period": "live routing response at request time",
        "landing_page": "https://project-osrm.org/",
        "cache_status": "live_api",
        "cache_freshness": "queried live via configured OSRM endpoint",
        "source_caveat": (
            "Drive times are routing-engine estimates from the configured OSRM road graph, "
            "not observed traffic, official access standards, or proof of network adequacy."
            + (" The public OSRM demo endpoint is rate-limited and not intended for production workloads." if public_demo else "")
        ),
    }


def _ors_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "OpenRouteService Isochrones API",
        "source_url": "https://api.openrouteservice.org/v2/isochrones/driving-car",
        "dataset_id": "openrouteservice_drive_isochrone",
        "source_period": "live routing response at request time",
        "landing_page": "https://openrouteservice.org/",
        "cache_status": "live_api",
        "cache_freshness": "queried live via OpenRouteService API",
        "source_caveat": (
            "Isochrones are modeled drive-time polygons from OpenRouteService and should be "
            "validated against local routing assumptions before operational use."
        ),
    }


def _hospital_info_source_metadata() -> dict[str, Any]:
    cache_path = os.path.join(CACHE_DIR, "hospital_general_info.csv")
    cache_status = "local_cache_hit" if os.path.exists(cache_path) else "download_on_demand"
    return {
        "source_name": "CMS Provider Data Hospital General Information",
        "source_url": HOSPITAL_INFO_URL,
        "dataset_id": "cms_hospital_general_information",
        "source_period": "CMS Provider Data API export at cache download time",
        "landing_page": HOSPITAL_INFO_LANDING_PAGE,
        "source_modified": _cache_mtime(cache_path),
        "cache_status": cache_status,
        "cache_age_days": _cache_age_days(cache_path),
        "cache_key": cache_path,
        "source_caveat": (
            "Hospital General Information identifies public Medicare facilities and locations; "
            "it is not a complete provider directory, licensure record, or current capacity source."
        ),
    }


def _gazetteer_source_metadata() -> dict[str, Any]:
    cache_path = os.path.join(CACHE_DIR, "zip_centroids.txt")
    cache_status = "local_cache_hit" if os.path.exists(cache_path) else "download_on_demand"
    return {
        "source_name": "U.S. Census Gazetteer ZCTA centroid file",
        "source_url": GAZETTEER_URL,
        "dataset_id": "census_gazetteer_zcta_centroids",
        "source_period": "2023",
        "landing_page": GAZETTEER_LANDING_PAGE,
        "source_modified": _cache_mtime(cache_path),
        "cache_status": cache_status,
        "cache_age_days": _cache_age_days(cache_path),
        "cache_key": cache_path,
        "source_caveat": (
            "ZCTA centroids are geography approximations used only when facility coordinates are absent; "
            "they should not be treated as exact facility coordinates."
        ),
    }


def _combined_source_metadata(*, dataset_id: str, include_gazetteer: bool = False) -> dict[str, Any]:
    sources = [_hospital_info_source_metadata(), _osrm_source_metadata("table_matrix")]
    if include_gazetteer:
        sources.append(_gazetteer_source_metadata())
    return {
        "source_name": "CMS Provider Data Hospital General Information + OSRM routing engine",
        "source_url": HOSPITAL_INFO_URL,
        "dataset_id": dataset_id,
        "source_period": "CMS facility cache plus live routing response at request time",
        "landing_page": HOSPITAL_INFO_LANDING_PAGE,
        "cache_status": ", ".join(str(source.get("cache_status") or "") for source in sources if source.get("cache_status")),
        "cache_freshness": "; ".join(str(source.get("cache_freshness") or "") for source in sources if source.get("cache_freshness")),
        "source_caveat": (
            "Combines public CMS facility location records with modeled OSRM drive times. "
            "Results are market/access context, not a complete provider directory, observed traffic, "
            "network adequacy finding, or referral/leakage conclusion."
        ),
        "sources": sources,
    }


def _drive_time_evidence(
    source_metadata: dict[str, Any],
    *,
    entity_scope: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str = "",
    next_step: str = "",
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=source_metadata,
        entity_scope=entity_scope,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat or str(source_metadata.get("source_caveat") or ""),
        next_step=next_step,
    )


def _drive_time_row_evidence(
    source_metadata: dict[str, Any],
    *,
    entity_scope: str,
    parent_query: dict[str, Any],
    row_query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str = "",
    next_step: str = "",
) -> dict[str, Any]:
    query = {
        **{key: value for key, value in parent_query.items() if value not in ("", None, [])},
        **{key: value for key, value in row_query.items() if value not in ("", None, [])},
    }
    return _drive_time_evidence(
        source_metadata,
        entity_scope=entity_scope,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _drive_time_source_claim(
    source_metadata: dict[str, Any],
    *,
    collection: str = "",
    row_evidence_paths: tuple[str, ...] = (),
    match_policy: str,
) -> dict[str, Any]:
    return source_claim(
        collection=collection or str(source_metadata.get("dataset_id") or "drive_time"),
        source_name=str(source_metadata.get("source_name") or ""),
        source_url=str(source_metadata.get("source_url") or ""),
        evidence_path="evidence",
        source_metadata_path="source_metadata",
        row_evidence_paths=row_evidence_paths,
        match_policy=match_policy,
    )


def _coordinate_identity(*, entity_id: str, lat: float, lon: float, entity_type: str = "coordinate") -> dict[str, Any]:
    return {
        "canonical_name": str(entity_id or ""),
        "entity_type": entity_type,
        "ccn": "",
        "npi": "",
        "pecos_enrollment_id": "",
        "ahrq_system_id": "",
        "owner_id": "",
        "address": "",
        "zip_code": "",
        "lat": lat,
        "lon": lon,
        "aliases": [],
        "match_decisions": [
            {
                "basis": "caller_supplied_coordinate",
                "confidence": "caller_supplied",
                "decided_at": "",
                "notes": "Coordinate identity is scoped to this request and is not a healthcare entity match.",
            }
        ],
        "conflicts": [],
        "unresolved_identifiers": [],
    }


def _facility_identity(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
    get = row.get
    address = get("address", "")
    city = get("city", get("city/town", ""))
    state = get("state", "")
    zip_code = get("zip_code", get("zip", get("zipcode", "")))
    full_address = ", ".join(str(part) for part in (address, city, state) if part)
    return identity_from_public_record(
        name=get("facility_name", get("hospital_name", "")),
        entity_type="facility",
        ccn=get("facility_id", get("provider_id", get("ccn", ""))),
        address=full_address,
        zip_code=zip_code,
        source_name="CMS Provider Data Hospital General Information",
        source_url=HOSPITAL_INFO_URL,
    ).to_dict()


def _location_identity_map(
    origins: list[dict[str, Any]],
    destinations: list[dict[str, Any]],
    *,
    source_metadata: dict[str, Any],
    row_evidence_paths: tuple[str, ...] = (),
    match_policy: str,
) -> dict[str, Any]:
    entities = [
        _coordinate_identity(
            entity_id=str(origin.get("id", f"origin_{index}")),
            lat=float(origin["lat"]),
            lon=float(origin["lon"]),
            entity_type="origin_coordinate",
        )
        for index, origin in enumerate(origins)
    ]
    entities.extend(
        _coordinate_identity(
            entity_id=str(destination.get("id", f"dest_{index}")),
            lat=float(destination["lat"]),
            lon=float(destination["lon"]),
            entity_type="destination_coordinate",
        )
        for index, destination in enumerate(destinations)
    )
    return {
        "entities": entities,
        "source_claims": [
            _drive_time_source_claim(
                source_metadata,
                row_evidence_paths=row_evidence_paths,
                match_policy=match_policy,
            )
        ],
    }


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
GAZETTEER_LANDING_PAGE = "https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html"
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
        df["_coordinate_source"] = "facility_latitude_longitude_columns"
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
        df["_coordinate_source"] = "facility_location_json"
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
        df["_coordinate_source"] = "census_zcta_centroid_fallback"
        logger.info("Geocoded %d/%d facilities via ZIP centroids",
                     df["_lat"].notna().sum(), len(df))
        return df

    raise ValueError("Cannot find latitude/longitude columns in facility data")


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool(structured_output=True)
@observe_tool("drive-time")
async def compute_drive_time(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
) -> dict[str, Any]:
    """Compute driving time and distance between two geographic points.

    Uses the OSRM routing engine. Returns duration in seconds/minutes
    and distance in meters/miles.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compute_drive_time","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
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
    source_metadata = _osrm_source_metadata("route")
    response = route.model_dump()
    response["source_metadata"] = source_metadata
    response["evidence"] = _drive_time_evidence(
        source_metadata,
        entity_scope="point_to_point_route",
        query={
            "origin": {"lat": origin_lat, "lon": origin_lon},
            "destination": {"lat": dest_lat, "lon": dest_lon},
            "mode": "driving-car",
        },
        match_basis="caller_supplied_coordinates_osrm_route",
        confidence="high_for_configured_routing_engine",
        next_step="Use a self-hosted OSRM backend and local validation before treating this as an operational access standard.",
    )
    response["identity_map"] = {
        "entities": [
            _coordinate_identity(entity_id="origin", lat=origin_lat, lon=origin_lon, entity_type="origin_coordinate"),
            _coordinate_identity(entity_id="destination", lat=dest_lat, lon=dest_lon, entity_type="destination_coordinate"),
        ],
        "source_claims": [
            _drive_time_source_claim(
                source_metadata,
                match_policy="caller_supplied_coordinates_osrm_route",
            )
        ],
    }
    return to_structured(response)


@mcp.tool(structured_output=True)
@observe_tool("drive-time")
async def compute_drive_time_matrix(
    origins: list[dict],
    destinations: list[dict],
) -> dict[str, Any]:
    """Compute an NxM driving time matrix between origins and destinations.

    Each origin/destination should be a dict with keys: lat, lon, id.
    Uses the OSRM table endpoint for efficient bulk computation.
    Returns a matrix of drive times (minutes) with origin and destination IDs.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compute_drive_time_matrix","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    # Validate inputs
    if not origins or not destinations:
        return error_response(
            "origins and destinations must be non-empty",
            evidence=_drive_time_evidence(
                _osrm_source_metadata("table_matrix"),
                entity_scope="origin_destination_drive_time_matrix",
                query={"origin_count": len(origins), "destination_count": len(destinations)},
                match_basis="caller_supplied_coordinate_ids_osrm_table",
                confidence="not_run_invalid_input",
                next_step="Provide at least one origin and one destination with lat/lon values.",
            ),
        )

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

    source_metadata = _osrm_source_metadata("table_matrix")
    parent_query = {
        "origin_count": len(origins),
        "destination_count": len(destinations),
        "origin_ids": origin_ids,
        "destination_ids": dest_ids,
        "mode": "driving-car",
    }
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
            entry_payload = entry.model_dump()
            entry_payload["evidence"] = _drive_time_row_evidence(
                source_metadata,
                entity_scope="origin_destination_drive_time_matrix_row",
                parent_query=parent_query,
                row_query={
                    "origin_id": origin_id,
                    "destination_id": dest_id,
                    "duration_minutes": entry_payload.get("duration_minutes"),
                    "distance_miles": entry_payload.get("distance_miles"),
                },
                match_basis="osrm_table_matrix_origin_destination_cell",
                confidence="high_for_configured_routing_engine_cell",
                next_step="Preserve this row receipt before joining the origin/destination IDs to healthcare entities.",
            )
            entries.append(entry_payload)

    result = {
        "origin_ids": origin_ids,
        "destination_ids": dest_ids,
        "matrix": entries,
        "source_metadata": source_metadata,
        "evidence": _drive_time_evidence(
            source_metadata,
            entity_scope="origin_destination_drive_time_matrix",
            query=parent_query,
            match_basis="caller_supplied_coordinate_ids_osrm_table",
            confidence="high_for_configured_routing_engine",
            next_step="Retain caller-supplied IDs with the returned matrix before joining to healthcare entities.",
        ),
        "identity_map": _location_identity_map(
            origins,
            destinations,
            source_metadata=source_metadata,
            row_evidence_paths=("matrix[].evidence",),
            match_policy="caller_supplied_coordinate_ids_osrm_table",
        ),
    }
    return to_structured(result)


@mcp.tool(structured_output=True)
@observe_tool("drive-time")
async def generate_isochrone(
    lat: float,
    lon: float,
    minutes: list[int] | None = None,
) -> dict[str, Any]:
    """Generate drive-time isochrone polygons around a point.

    Creates GeoJSON polygons showing areas reachable within the specified
    time thresholds. Requires ORS_API_KEY environment variable.

    Args:
        lat: Latitude of the center point.
        lon: Longitude of the center point.
        minutes: Time thresholds in minutes (default: [15, 30, 60]).

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"generate_isochrone","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if minutes is None:
        minutes = [15, 30, 60]

    ranges_seconds = [m * 60 for m in minutes]
    ors = _get_ors()
    geojson = await ors.isochrone(lon=lon, lat=lat, ranges=ranges_seconds)
    result = to_structured(geojson)
    if isinstance(result, dict):
        source_metadata = _ors_source_metadata()
        result["source_metadata"] = source_metadata
        result["evidence"] = _drive_time_evidence(
            source_metadata,
            entity_scope="drive_time_isochrone",
            query={"lat": lat, "lon": lon, "minutes": minutes, "mode": "driving-car"},
            match_basis="caller_supplied_coordinate_openrouteservice_isochrone",
            confidence="high_for_configured_routing_engine",
            next_step="Validate polygon boundaries against local routing assumptions before using them for facility access decisions.",
        )
        result["identity"] = _coordinate_identity(entity_id="isochrone_center", lat=lat, lon=lon)
    return to_structured(result)


@mcp.tool(structured_output=True)
@observe_tool("drive-time")
async def find_competing_facilities(
    lat: float,
    lon: float,
    radius_minutes: int = 30,
    facility_type: str = "hospital",
) -> dict[str, Any]:
    """Find healthcare facilities within a drive-time radius of a point.

    Loads CMS Hospital General Info data, geocodes facilities, and computes
    driving time from the given point to each facility. Returns facilities
    sorted by drive time.

    Args:
        lat: Latitude of the origin point.
        lon: Longitude of the origin point.
        radius_minutes: Maximum drive time in minutes (default 30).
        facility_type: Type filter — "hospital" uses all hospitals.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"find_competing_facilities","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    df = await _load_facilities()
    df = await _parse_lat_lon(df)
    df = df.dropna(subset=["_lat", "_lon"])
    include_gazetteer = bool(
        "_coordinate_source" in df.columns and (df["_coordinate_source"] == "census_zcta_centroid_fallback").any()
    )
    source_metadata = _combined_source_metadata(
        dataset_id="drive_time_competing_facilities",
        include_gazetteer=include_gazetteer,
    )
    evidence = _drive_time_evidence(
        source_metadata,
        entity_scope="facilities_within_drive_time_radius",
        query={"lat": lat, "lon": lon, "radius_minutes": radius_minutes, "facility_type": facility_type},
        match_basis="cms_facility_location_candidates_plus_osrm_drive_time_threshold",
        confidence="medium_for_market_context",
        next_step="Inspect returned facility CCNs and source records before using the candidate set in a market or access workflow.",
    )

    if df.empty:
        return error_response(
            "No geocoded facilities found in dataset",
            evidence=evidence,
            source_metadata={"sources": source_metadata["sources"]},
        )

    # Pre-filter by rough bounding box (~1 degree ≈ 60 miles ≈ ~60 min drive)
    # to avoid sending thousands of points to OSRM
    deg_buffer = max(1.0, radius_minutes / 30)
    nearby = df[
        (df["_lat"].between(lat - deg_buffer, lat + deg_buffer))
        & (df["_lon"].between(lon - deg_buffer, lon + deg_buffer))
    ]

    if nearby.empty:
        return to_structured({
            "facilities": [],
            "count": 0,
            "message": "No facilities found within bounding box",
            "evidence": evidence,
            "source_metadata": {"sources": source_metadata["sources"]},
            "identity": _coordinate_identity(entity_id="origin", lat=lat, lon=lon, entity_type="origin_coordinate"),
            "identity_map": {
                "entities": [],
                "source_claims": [
                    _drive_time_source_claim(
                        source_metadata,
                        collection="drive_time_competing_facilities",
                        row_evidence_paths=("facilities[].evidence",),
                        match_policy="cms_facility_location_candidates_plus_osrm_drive_time_threshold",
                    )
                ],
            },
        })

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
    facility_identities: list[dict] = []
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
        facility_payload = fac.model_dump()
        facility_payload["evidence"] = _drive_time_evidence(
            source_metadata,
            entity_scope="facilities_within_drive_time_radius_row",
            query={
                "origin_lat": lat,
                "origin_lon": lon,
                "radius_minutes": radius_minutes,
                "facility_type": facility_type,
                "ccn": facility_payload.get("ccn", ""),
                "facility_name": facility_payload.get("name", ""),
            },
            match_basis="competing_facility_drive_time_row",
            confidence="modeled_drive_time_public_facility_row",
            caveat=(
                "This competing-facility row combines public CMS facility coordinates with modeled OSRM drive time; "
                "verify the facility source record and routing assumptions before citing competitive access context."
            ),
            next_step="Preserve this row receipt with the facility CCN/name and rerun using a production routing source if needed.",
        )
        facilities.append(facility_payload)
        facility_identities.append(_facility_identity(row))

    paired = sorted(zip(facilities, facility_identities, strict=True), key=lambda pair: pair[0]["drive_time_minutes"])
    facilities = [facility for facility, _ in paired]
    facility_identities = [identity for _, identity in paired]

    return to_structured({
        "facilities": facilities,
        "count": len(facilities),
        "evidence": evidence,
        "source_metadata": {"sources": source_metadata["sources"]},
        "identity": _coordinate_identity(entity_id="origin", lat=lat, lon=lon, entity_type="origin_coordinate"),
        "identity_map": {
            "entities": facility_identities,
            "source_claims": [
                _drive_time_source_claim(
                    source_metadata,
                    collection="drive_time_competing_facilities",
                    row_evidence_paths=("facilities[].evidence",),
                    match_policy="cms_facility_location_candidates_plus_osrm_drive_time_threshold",
                )
            ],
        },
    })


@mcp.tool(structured_output=True)
@observe_tool("drive-time")
async def compute_accessibility_score(
    demand_points: list[dict],
    supply_points: list[dict],
    catchment_minutes: int = 30,
) -> dict[str, Any]:
    """Compute spatial accessibility scores using Enhanced Two-Step Floating Catchment Area (E2SFCA).

    Measures how accessible healthcare supply is from each demand location,
    accounting for competition from other demand points.

    Args:
        demand_points: List of dicts with keys: lat, lon, population, id (optional).
            E.g., ZCTA centroids with population counts.
        supply_points: List of dicts with keys: lat, lon, capacity, id (optional).
            E.g., hospitals with bed counts.
        catchment_minutes: Max travel time for the catchment area (default 30).

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compute_accessibility_score","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    source_metadata = {
        "source_name": "OSRM routing engine + E2SFCA accessibility method",
        "source_url": OSRM_BASE_URL,
        "dataset_id": "drive_time_e2sfca_accessibility",
        "source_period": "live routing response at request time",
        "landing_page": "https://project-osrm.org/",
        "cache_status": "live_api",
        "cache_freshness": "queried live via configured OSRM endpoint",
        "source_caveat": (
            "E2SFCA scores are modeled accessibility indexes using caller-supplied demand and supply inputs "
            "plus OSRM travel times; they are not observed utilization, capacity verification, network adequacy, "
            "or patient-level access facts."
        ),
        "sources": [
            _osrm_source_metadata("accessibility_table"),
            {
                "source_name": "Enhanced Two-Step Floating Catchment Area method",
                "source_url": "https://doi.org/10.1016/j.healthplace.2009.06.002",
                "dataset_id": "e2sfca_method_reference",
                "source_period": "2009 method publication",
                "landing_page": "https://www.sciencedirect.com/science/article/pii/S1353829209000642",
                "cache_status": "not_applicable",
                "source_caveat": "Method reference only; input demand and supply values must be independently sourced and cited.",
            },
        ],
    }
    evidence = _drive_time_evidence(
        source_metadata,
        entity_scope="modeled_accessibility_score",
        query={
            "demand_point_count": len(demand_points),
            "supply_point_count": len(supply_points),
            "catchment_minutes": catchment_minutes,
        },
        match_basis="caller_supplied_demand_supply_coordinates_osrm_table_e2sfca",
        confidence="method_estimate_depends_on_inputs",
        next_step="Attach source evidence for the caller-supplied demand populations and supply capacities before citing scores.",
    )
    if not demand_points or not supply_points:
        return error_response(
            "demand_points and supply_points must be non-empty",
            evidence=evidence,
            source_metadata={"sources": source_metadata["sources"]},
        )

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
    parent_query = {
        "demand_point_count": len(demand_points),
        "supply_point_count": len(supply_points),
        "catchment_minutes": catchment_minutes,
    }
    results: list[dict] = []
    for i, dp in enumerate(demand_points):
        reachable_supply_count = sum(1 for value in travel_matrix[i] if value is not None and value <= catchment_minutes)
        ar = AccessibilityResult(
            demand_id=dp.get("id", f"demand_{i}"),
            lat=float(dp["lat"]),
            lon=float(dp["lon"]),
            population=populations[i],
            accessibility_score=round(scores[i], 8),
        )
        result_payload = ar.model_dump()
        result_payload["evidence"] = _drive_time_row_evidence(
            source_metadata,
            entity_scope="modeled_accessibility_score_row",
            parent_query=parent_query,
            row_query={
                "demand_id": result_payload.get("demand_id"),
                "population": result_payload.get("population"),
                "reachable_supply_count": reachable_supply_count,
                "accessibility_score": result_payload.get("accessibility_score"),
            },
            match_basis="e2sfca_demand_point_score_row",
            confidence="method_estimate_depends_on_caller_supplied_inputs",
            next_step=(
                "Attach source evidence for this demand point's population and the supply capacities "
                "before citing the modeled score."
            ),
        )
        results.append(result_payload)

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
    response = summary.model_dump()
    response["results"] = results
    response["evidence"] = evidence
    response["source_metadata"] = {"sources": source_metadata["sources"]}
    response["identity_map"] = {
        "entities": [
            _coordinate_identity(
                entity_id=str(point.get("id", f"demand_{index}")),
                lat=float(point["lat"]),
                lon=float(point["lon"]),
                entity_type="demand_coordinate",
            )
            for index, point in enumerate(demand_points)
        ]
        + [
            _coordinate_identity(
                entity_id=str(point.get("id", f"supply_{index}")),
                lat=float(point["lat"]),
                lon=float(point["lon"]),
                entity_type="supply_coordinate",
            )
            for index, point in enumerate(supply_points)
        ],
        "source_claims": [
            _drive_time_source_claim(
                source_metadata,
                collection="drive_time_e2sfca_accessibility",
                row_evidence_paths=("results[].evidence",),
                match_policy="caller_supplied_demand_supply_coordinates_osrm_table_e2sfca",
            )
        ],
    }
    return to_structured(response)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport=_transport)
