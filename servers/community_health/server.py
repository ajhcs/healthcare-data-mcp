"""Community health MCP server backed by CDC PLACES datasets."""

from __future__ import annotations

import logging
import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from shared.utils.mcp_response import collection_response, error_response, response_envelope

from . import data_loaders
from .models import MarketCommunityProfile, MeasureMetadata, PlacesProfile
from .socrata_client import normalize_geography_type

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict[str, Any] = {"name": "community-health"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8018"))
mcp = FastMCP(**_mcp_kwargs)


@mcp.tool(structured_output=True)
async def list_places_measures(
    geography_type: str = "county",
    state: str = "",
    search: str = "",
    limit: int = 200,
) -> dict[str, Any]:
    """List CDC PLACES community-health measures available for a geography.

    Args:
        geography_type: County, place, tract, or ZCTA. Common aliases such as "zip" are accepted.
        state: Optional two-letter state filter for data-backed measure discovery.
        search: Optional measure/category text filter.
        limit: Maximum source rows to inspect when reading from Socrata or a fixture.
    """
    try:
        geography = normalize_geography_type(geography_type)
        bounded_limit = _bounded_limit(limit, 1000, default=200)
        rows, source = await data_loaders.normalized_places_rows(geography, state=state or None, limit=bounded_limit)
        measures = data_loaders.build_measure_metadata(rows)
        if search:
            token = search.strip().upper()
            measures = [
                measure
                for measure in measures
                if token in f"{measure['measure_id']} {measure['measure']} {measure['category']}".upper()
            ]
        modeled = [MeasureMetadata(**measure) for measure in measures]
        return collection_response(
            [measure.model_dump() for measure in modeled],
            limit=bounded_limit,
            meta={"source": source, "geography_type": geography},
        )
    except Exception as exc:
        logger.exception("list_places_measures failed")
        return error_response(f"list_places_measures failed: {exc}")


@mcp.tool(structured_output=True)
async def search_places(
    query: str,
    geography_type: str = "county",
    state: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search PLACES locations by name or location ID.

    Args:
        query: Location name or FIPS/ZCTA/location ID fragment.
        geography_type: County, place, tract, or ZCTA.
        state: Optional two-letter state filter.
        limit: Maximum locations to return.
    """
    try:
        if not str(query or "").strip():
            return error_response("query is required.", code="invalid_params")
        geography = normalize_geography_type(geography_type)
        bounded_limit = _bounded_limit(limit, 100, default=25)
        rows, source = await data_loaders.normalized_places_rows(
            geography,
            state=state or None,
            search=query,
            limit=min(bounded_limit * 10, 1000),
        )
        locations = data_loaders.summarize_locations(rows)[:bounded_limit]
        return collection_response(
            locations,
            limit=bounded_limit,
            meta={"source": source, "geography_type": geography},
        )
    except Exception as exc:
        logger.exception("search_places failed")
        return error_response(f"search_places failed: {exc}")


@mcp.tool(structured_output=True)
async def get_places_profile(
    location_id: str,
    geography_type: str = "county",
    measure_ids: list[str] | None = None,
    data_value_types: list[str] | None = None,
) -> dict[str, Any]:
    """Get a PLACES community-health profile for one location.

    Args:
        location_id: PLACES location ID such as county FIPS, census place ID, tract GEOID, or ZCTA.
        geography_type: County, place, tract, or ZCTA.
        measure_ids: Optional PLACES measure IDs to include.
        data_value_types: Optional value types, for example "Age-adjusted prevalence".
    """
    try:
        location_ids, error = _clean_location_ids([location_id], field_name="location_id", max_count=1)
        if error:
            return error_response(error, code="invalid_params")
        if not location_ids:
            return error_response("location_id is required.", code="invalid_params")
        geography = normalize_geography_type(geography_type)
        rows, source = await data_loaders.normalized_places_rows(
            geography,
            location_ids=location_ids,
            measure_ids=measure_ids,
            data_value_types=data_value_types,
        )
        profile = PlacesProfile(**data_loaders.group_profile(rows))
        return response_envelope(profile=profile.model_dump(), meta={"source": source, "geography_type": geography})
    except Exception as exc:
        logger.exception("get_places_profile failed")
        return error_response(f"get_places_profile failed: {exc}")


@mcp.tool(structured_output=True)
async def compare_places(
    location_ids: list[str],
    geography_type: str = "county",
    measure_ids: list[str] | None = None,
    data_value_types: list[str] | None = None,
) -> dict[str, Any]:
    """Compare CDC PLACES community-health estimates across locations.

    Args:
        location_ids: Two or more PLACES location IDs for the same geography type.
        geography_type: County, place, tract, or ZCTA.
        measure_ids: Optional PLACES measure IDs to include.
        data_value_types: Optional value types to include.
    """
    try:
        location_ids, error = _clean_location_ids(location_ids, field_name="location_ids", max_count=50)
        if error:
            return error_response(error, code="invalid_params")
        if len(location_ids) < 2:
            return error_response("compare_places requires at least two non-empty location_ids", code="invalid_params")
        geography = normalize_geography_type(geography_type)
        rows, source = await data_loaders.normalized_places_rows(
            geography,
            location_ids=location_ids,
            measure_ids=measure_ids,
            data_value_types=data_value_types,
            limit=min(max(len(location_ids) * 500, 1000), 10000),
        )
        grouped = {location_id: [] for location_id in location_ids}
        for row in rows:
            grouped.setdefault(row["location_id"], []).append(row)

        profiles = {
            location_id: PlacesProfile(**data_loaders.group_profile(records)).model_dump()
            for location_id, records in grouped.items()
        }
        missing = [location_id for location_id, records in grouped.items() if not records]
        return response_envelope(
            comparison={"geography_type": geography, "profiles": profiles, "missing_locations": missing},
            meta={"source": source, "notes": [data_loaders.COMMUNITY_ESTIMATE_NOTE]},
        )
    except Exception as exc:
        logger.exception("compare_places failed")
        return error_response(f"compare_places failed: {exc}")


@mcp.tool(structured_output=True)
async def get_market_community_profile(
    county_fips: list[str] | None = None,
    zctas: list[str] | None = None,
    measure_ids: list[str] | None = None,
    data_value_types: list[str] | None = None,
) -> dict[str, Any]:
    """Aggregate PLACES county/ZCTA community estimates for a service area.

    Args:
        county_fips: County FIPS codes in the market service area.
        zctas: ZIP Code Tabulation Areas in the market service area.
        measure_ids: Optional PLACES measure IDs to include.
        data_value_types: Optional value types to include.
    """
    try:
        county_fips, county_error = _clean_location_ids(county_fips or [], field_name="county_fips", max_count=100)
        zctas, zcta_error = _clean_location_ids(zctas or [], field_name="zctas", max_count=100)
        if county_error:
            return error_response(county_error, code="invalid_params")
        if zcta_error:
            return error_response(zcta_error, code="invalid_params")
        if not county_fips and not zctas:
            return error_response("Provide at least one non-empty county_fips or zctas value", code="invalid_params")

        all_rows: list[dict[str, Any]] = []
        sources: list[dict[str, Any]] = []
        if county_fips:
            rows, source = await data_loaders.normalized_places_rows(
                "county",
                location_ids=county_fips,
                measure_ids=measure_ids,
                data_value_types=data_value_types,
                limit=min(max(len(county_fips) * 500, 5000), 25000),
            )
            all_rows.extend(rows)
            sources.append(source)
        if zctas:
            rows, source = await data_loaders.normalized_places_rows(
                "zcta",
                location_ids=zctas,
                measure_ids=measure_ids,
                data_value_types=data_value_types,
                limit=min(max(len(zctas) * 500, 5000), 25000),
            )
            all_rows.extend(rows)
            sources.append(source)

        profile = MarketCommunityProfile(**data_loaders.aggregate_market_profile(all_rows))
        return response_envelope(
            market_profile=profile.model_dump(),
            meta={"sources": sources, "notes": [data_loaders.COMMUNITY_ESTIMATE_NOTE]},
        )
    except Exception as exc:
        logger.exception("get_market_community_profile failed")
        return error_response(f"get_market_community_profile failed: {exc}")


def _bounded_limit(limit: int, maximum: int, *, default: int) -> int:
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


def _clean_location_ids(values: list[str] | None, *, field_name: str, max_count: int) -> tuple[list[str], str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        token = str(value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        cleaned.append(token)
        if len(cleaned) > max_count:
            return [], f"{field_name} accepts at most {max_count} values."
    return cleaned, ""


if __name__ == "__main__":
    mcp.run(transport=_transport)
