"""Community health MCP server backed by CDC PLACES datasets."""

from __future__ import annotations

import logging
import os
from typing import Any

from mcp.server.fastmcp import FastMCP

from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import collection_response, error_response, evidence_receipt, response_envelope

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


def _places_evidence(
    source: dict[str, Any],
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
    entity_scope: str | None = None,
    dataset_id: str = "",
) -> dict[str, Any]:
    geography = str(source.get("geography_type") or "")
    return evidence_receipt(
        source_name=str(source.get("name") or "CDC PLACES: Local Data for Better Health"),
        source_url=str(source.get("source_url") or ""),
        dataset_id=dataset_id or str(source.get("dataset_id") or ""),
        source_period=str(source.get("release") or ""),
        landing_page=str(source.get("landing_page") or ""),
        source_modified=str(source.get("modified") or ""),
        cache_status="ready" if source.get("dataset_id") or source.get("source_url") else "source_metadata_unresolved",
        cache_freshness=_places_cache_freshness(source),
        entity_scope=entity_scope or f"cdc_places_{geography or 'geography'}",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=(
            "CDC PLACES values are model-based community estimates for geographic areas; "
            "they are not patient-level facts, clinical quality measures, or facility performance facts."
        ),
        next_step=next_step,
    )


def _places_row_evidence(
    source: dict[str, Any],
    *,
    row: dict[str, Any],
    row_kind: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
    entity_scope: str | None = None,
    dataset_id: str = "",
) -> dict[str, Any]:
    row_query = _compact_query(query)
    for key in (
        "geography_type",
        "location_id",
        "location_name",
        "state_abbr",
        "state_name",
        "measure_id",
        "measure",
        "data_value_type",
        "data_value_type_id",
        "year",
        "locations_reporting",
        "weight_basis",
    ):
        if row.get(key) not in (None, "", [], {}):
            row_query[f"row_{key}"] = row[key]
    if row.get("confidence_interval") not in (None, "", [], {}):
        row_query["row_confidence_interval"] = row["confidence_interval"]
    if row.get("data_value") is not None:
        row_query["row_data_value"] = row["data_value"]
    if row.get("weighted_average") is not None:
        row_query["row_weighted_average"] = row["weighted_average"]
    if row.get("simple_average") is not None:
        row_query["row_simple_average"] = row["simple_average"]

    geography = str(row.get("geography_type") or source.get("geography_type") or "geography")
    return _places_evidence(
        source,
        dataset_id=dataset_id or str(source.get("dataset_id") or ""),
        entity_scope=entity_scope or f"cdc_places_{row_kind}_{geography}",
        query=row_query,
        match_basis=match_basis,
        confidence=confidence,
        next_step=next_step,
    )


def _compact_query(query: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in query.items() if value not in (None, "", [], {})}


def _with_location_evidence(
    locations: list[dict[str, Any]],
    source: dict[str, Any],
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
) -> list[dict[str, Any]]:
    return [
        {
            **location,
            "evidence": _places_row_evidence(
                source,
                row=location,
                row_kind="location_row",
                query=query,
                match_basis=match_basis,
                confidence=confidence,
                next_step=next_step,
            ),
        }
        for location in locations
    ]


def _with_measure_row_evidence(
    rows: list[dict[str, Any]],
    source: dict[str, Any],
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
    row_kind: str = "measure_row",
) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "evidence": _places_row_evidence(
                source,
                row=row,
                row_kind=row_kind,
                query=query,
                match_basis=match_basis,
                confidence=confidence,
                next_step=next_step,
            ),
        }
        for row in rows
    ]


def _places_cache_freshness(source: dict[str, Any]) -> str:
    parts = []
    if source.get("modified"):
        parts.append(f"source_modified={source['modified']}")
    if source.get("release"):
        parts.append(f"release={source['release']}")
    if source.get("record_count") not in (None, ""):
        parts.append(f"record_count={source['record_count']}")
    return "; ".join(parts) or "source freshness unavailable"


def _location_identity(location: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    geography = str(location.get("geography_type") or source.get("geography_type") or "geography")
    identity = identity_from_public_record(
        name=location.get("location_name") or "",
        entity_type=f"places_{geography}",
        source_name=str(source.get("name") or "CDC PLACES: Local Data for Better Health"),
        source_url=str(source.get("landing_page") or source.get("source_url") or ""),
    ).to_dict()
    location_id = str(location.get("location_id") or "").strip()
    if location_id:
        identity["unresolved_identifiers"].append({"type": f"places_{geography}_location_id", "value": location_id})
    if location.get("state_abbr"):
        identity["state"] = str(location["state_abbr"]).upper()
    if location.get("state_name"):
        identity["state_name"] = str(location["state_name"])
    return identity


def _places_identity_map(locations: list[dict[str, Any]], source: dict[str, Any], *, match_basis: str) -> dict[str, Any]:
    return {
        "entities": [_location_identity(location, source) for location in locations if location],
        "match_basis": match_basis,
        "conflict_policy": (
            "Join PLACES geographies by exact geography_type plus location_id; names are labels only "
            "and must not be merged across geography levels."
        ),
    }


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
        query_payload = {"geography_type": geography, "state": state, "search": search, "limit": bounded_limit}
        measure_rows = _with_measure_row_evidence(
            [measure.model_dump() for measure in modeled],
            source,
            query=query_payload,
            match_basis="cdc_places_measure_metadata_row",
            confidence="source_backed_measure_metadata_row",
            next_step="Use this exact measure_id and data_value_type when requesting profile facts.",
            row_kind="measure_metadata_row",
        )
        return collection_response(
            measure_rows,
            limit=bounded_limit,
            meta={"source": source, "geography_type": geography},
            source_metadata=source,
            evidence=_places_evidence(
                source,
                query=query_payload,
                match_basis="cdc_places_measure_metadata_from_rows",
                confidence="source_backed_measure_catalog",
                next_step="Use exact measure_id and data_value_type filters when requesting profile facts.",
            ),
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
        query_payload = {"query": query, "geography_type": geography, "state": state, "limit": bounded_limit}
        locations_with_evidence = _with_location_evidence(
            locations,
            source,
            query=query_payload,
            match_basis="cdc_places_location_search_result_row",
            confidence="candidate_geography_match_requires_location_id_review",
            next_step="Use this exact PLACES location_id and geography_type for profile calls.",
        )
        return collection_response(
            locations_with_evidence,
            limit=bounded_limit,
            meta={"source": source, "geography_type": geography},
            source_metadata=source,
            evidence=_places_evidence(
                source,
                query=query_payload,
                match_basis="cdc_places_location_name_or_id_search",
                confidence="candidate_geography_matches_require_location_id_review",
                next_step="Use the exact PLACES location_id and geography_type from the selected row for profile calls.",
            ),
            identity_map=_places_identity_map(
                locations,
                source,
                match_basis="places_geography_candidate_search_results",
            ),
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
        profile_payload = profile.model_dump()
        query_payload = {
            "location_id": location_ids[0],
            "geography_type": geography,
            "measure_ids": measure_ids or [],
            "data_value_types": data_value_types or [],
        }
        if profile_payload.get("location"):
            profile_payload["location"] = _with_location_evidence(
                [profile_payload["location"]],
                source,
                query=query_payload,
                match_basis="cdc_places_profile_location_id_exact_row",
                confidence="high_for_exact_places_geography_id" if rows else "no_matching_places_rows",
                next_step="Preserve this location_id and geography_type when joining community-health rows.",
            )[0]
        profile_payload["measures"] = _with_measure_row_evidence(
            list(profile_payload.get("measures") or []),
            source,
            query=query_payload,
            match_basis="cdc_places_profile_measure_row_exact_location",
            confidence="source_row_for_exact_places_geography_id",
            next_step="Preserve measure_id, data_value_type, year, data_value, and confidence interval when citing this estimate.",
            row_kind="profile_measure_row",
        )
        location = profile_payload.get("location") or {}
        return response_envelope(
            profile=profile_payload,
            meta={"source": source, "geography_type": geography},
            source_metadata=source,
            evidence=_places_evidence(
                source,
                query=query_payload,
                match_basis="cdc_places_location_id_exact",
                confidence="high_for_exact_places_geography_id" if rows else "no_matching_places_rows",
                next_step="Preserve measure_id, data_value_type, year, and confidence interval when citing specific estimates.",
            ),
            identity=_location_identity(location, source) if location else None,
        )
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

        query_payload = {
            "location_ids": location_ids,
            "geography_type": geography,
            "measure_ids": measure_ids or [],
            "data_value_types": data_value_types or [],
        }
        profiles = {}
        for location_id, records in grouped.items():
            profile_payload = PlacesProfile(**data_loaders.group_profile(records)).model_dump()
            if profile_payload.get("location"):
                profile_payload["location"] = _with_location_evidence(
                    [profile_payload["location"]],
                    source,
                    query={**query_payload, "location_id": location_id},
                    match_basis="cdc_places_comparison_location_id_exact_row",
                    confidence="high_for_exact_places_geography_id" if records else "no_matching_places_rows",
                    next_step="Use this location_id as the geography key for comparison rows.",
                )[0]
            profile_payload["measures"] = _with_measure_row_evidence(
                list(profile_payload.get("measures") or []),
                source,
                query={**query_payload, "location_id": location_id},
                match_basis="cdc_places_comparison_measure_row_exact_location",
                confidence="source_row_for_exact_places_geography_id",
                next_step="Keep location_id, measure_id, data_value_type, year, and confidence interval with this comparison value.",
                row_kind="comparison_measure_row",
            )
            profiles[location_id] = profile_payload
        missing = [location_id for location_id, records in grouped.items() if not records]
        locations = [
            profile["location"]
            for profile in profiles.values()
            if profile.get("location")
        ]
        return response_envelope(
            comparison={"geography_type": geography, "profiles": profiles, "missing_locations": missing},
            meta={"source": source, "notes": [data_loaders.COMMUNITY_ESTIMATE_NOTE]},
            source_metadata=source,
            evidence=_places_evidence(
                source,
                query=query_payload,
                match_basis="cdc_places_location_id_exact_comparison",
                confidence="high_for_returned_exact_places_geography_ids",
                next_step="Treat missing_locations as unresolved geographies and do not impute community estimates.",
            ),
            identity_map=_places_identity_map(locations, source, match_basis="places_geography_exact_id_comparison"),
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
        profile_payload = profile.model_dump()
        primary_source = sources[0] if sources else {}
        query_payload = {
            "county_fips": county_fips,
            "zctas": zctas,
            "measure_ids": measure_ids or [],
            "data_value_types": data_value_types or [],
        }
        sources_by_geography = {str(source.get("geography_type") or ""): source for source in sources}
        profile_payload["locations"] = [
            {
                **location,
                "evidence": _places_row_evidence(
                    sources_by_geography.get(str(location.get("geography_type") or ""), primary_source),
                    row=location,
                    row_kind="market_location_row",
                    query=query_payload,
                    match_basis="cdc_places_market_location_exact_geography_id_row",
                    confidence="source_row_for_exact_places_market_geography_id",
                    next_step="Keep geography_type and location_id with this market service-area geography.",
                ),
            }
            for location in list(profile_payload.get("locations") or [])
        ]
        profile_payload["aggregated_measures"] = [
            {
                **measure,
                "evidence": _places_row_evidence(
                    primary_source,
                    row=measure,
                    row_kind="market_aggregated_measure_row",
                    dataset_id="cdc_places_market_community_profile",
                    entity_scope="cdc_places_market_aggregated_measure_row",
                    query=query_payload,
                    match_basis="cdc_places_market_aggregated_measure_row",
                    confidence="derived_from_exact_public_geography_rows",
                    next_step=(
                        "Review source geography coverage, locations_reporting, weight_basis, and row-level location "
                        "receipts before citing this market aggregate."
                    ),
                ),
            }
            for measure in list(profile_payload.get("aggregated_measures") or [])
        ]
        return response_envelope(
            market_profile=profile_payload,
            meta={"sources": sources, "notes": [data_loaders.COMMUNITY_ESTIMATE_NOTE]},
            source_metadata={"sources": sources},
            evidence=_places_evidence(
                primary_source,
                dataset_id="cdc_places_market_community_profile",
                entity_scope="cdc_places_market_geography_set",
                query=query_payload,
                match_basis="cdc_places_exact_geography_id_market_aggregation",
                confidence="aggregate_of_exact_public_geography_rows" if all_rows else "no_matching_places_rows",
                next_step="Review geographic_basis, locations_reporting, and weight_basis before using market averages in reports.",
            ),
            identity_map=_places_identity_map(
                list(profile_payload.get("locations") or []),
                primary_source,
                match_basis="places_market_geography_exact_ids",
            ),
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
