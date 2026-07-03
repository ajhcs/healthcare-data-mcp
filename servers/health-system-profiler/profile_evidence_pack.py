"""Source-backed evidence packs for health-system profile population."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime, timezone
from inspect import isawaitable
import re
from typing import Any

import pandas as pd

from shared.cache_manager import core as cache_core
from shared.utils.bed_resolver import resolve_hospital_bed_source
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.http_client import resilient_request
from shared.utils.identity import normalize_ccn, normalize_name
from shared.utils.mcp_response import evidence_receipt, to_structured
from shared.utils.source_backed_result import values_at_path

MCP_SERVER = "health-system-profiler"
MCP_TOOL = "build_profile_evidence_pack"
ENTITY_SCOPE = "health_system_profile_population"
PROJECT_LANDING_PAGE = "https://github.com/ajhcs/healthcare-data-mcp"

SOURCE_PRECEDENCE = [
    {
        "rank": 1,
        "source_family": "cms_pos_hgi",
        "rule": "Use CMS POS/HGI for facility identity, addresses, CCNs, source-local identifiers, and certified bed candidates.",
    },
    {
        "rank": 2,
        "source_family": "ahrq_compendium",
        "rule": "Use AHRQ as discovery and linkage spine only; do not treat it as final current ownership authority.",
    },
    {
        "rank": 3,
        "source_family": "hcris_state_official_beds",
        "rule": "Use HCRIS, state reports, and official pages for bed-count corroboration and conflicts.",
    },
    {
        "rank": 4,
        "source_family": "census_geocoder",
        "rule": "Use Census Geocoder first for coordinates, county, and county_geoid.",
    },
    {
        "rank": 5,
        "source_family": "osm_nominatim",
        "rule": "Use OSM/Nominatim only as a fallback when match quality is acceptable.",
    },
    {
        "rank": 6,
        "source_family": "official_system_pages_reports",
        "rule": "Use official system pages or reports for exact facility/site counts when the claim is exact.",
    },
    {
        "rank": 7,
        "source_family": "pecos_chow_official_affiliation",
        "rule": "Use PECOS/provider enrollment/CHOW and official pages for current affiliation review.",
    },
]

FIELD_TO_SECTION = {
    "system_identity": "system_identity_aliases",
    "aliases": "system_identity_aliases",
    "current_hospital_roster": "current_hospital_roster",
    "ccns": "source_identifiers",
    "source_local_identifiers": "source_identifiers",
    "addresses": "addresses",
    "county": "geography_candidates",
    "county_geoid": "geography_candidates",
    "latitude": "geography_candidates",
    "longitude": "geography_candidates",
    "hospital_bed_counts": "hospital_bed_counts",
    "system_bed_count": "system_bed_count_candidates",
    "bed_rollup_guidance": "bed_rollup_guidance",
    "affiliation": "affiliation_evidence",
    "facility_site_count": "facility_site_count_evidence",
}

Geocoder = Callable[[str], Awaitable[dict[str, Any] | None]]
ReverseGeocoder = Callable[[float, float], Awaitable[dict[str, Any] | None]]
RowsLoader = Callable[..., Any]


async def build_profile_evidence_pack(
    *,
    state: str,
    system_slug: str = "",
    system_name: str = "",
    ccns: list[str] | None = None,
    required_fields: list[str] | None = None,
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
    pos_df: pd.DataFrame,
    provider_enrollment_rows: list[dict[str, Any]] | None = None,
    hcris_rows: list[dict[str, Any]] | None = None,
    state_bed_rows: list[dict[str, Any]] | None = None,
    official_evidence_rows: list[dict[str, Any]] | None = None,
    provider_enrollment_loader: RowsLoader | None = None,
    hcris_loader: RowsLoader | None = None,
    state_bed_loader: RowsLoader | None = None,
    official_evidence_loader: RowsLoader | None = None,
    cache_root: str | None = None,
    census_geocoder: Geocoder | None = None,
    osm_geocoder: Geocoder | None = None,
    reverse_geocoder: ReverseGeocoder | None = None,
) -> dict[str, Any]:
    """Build a Healthcare Toolkit ingest-ready, read-only public evidence pack."""

    retrieval_time = _now()
    state_norm = str(state or "").strip().upper()
    ccn_values = _normalized_ccns(ccns or [])
    query = {
        "state": state_norm,
        "system_slug": system_slug or "",
        "system_name": system_name or "",
        "ccns": ccn_values,
        "required_fields": required_fields or [],
    }
    cache_preflight = _cache_preflight(cache_root)

    system_rows = _resolve_system_rows(
        systems_df,
        hospitals_df,
        state=state_norm,
        system_slug=system_slug,
        system_name=system_name,
        ccns=ccn_values,
    )
    facility_rows = _resolve_facility_rows(
        hospitals_df,
        pos_df,
        state=state_norm,
        system_rows=system_rows,
        ccns=ccn_values,
    )
    resolved_ccns = _normalized_ccns([*ccn_values, *(facility.get("ccn") for facility in facility_rows)])
    official_query_name = system_name or _first_system_name(system_rows) or system_slug
    provider_rows = _merge_rows(
        provider_enrollment_rows,
        await _call_rows_loader(provider_enrollment_loader, resolved_ccns),
    )
    hcris_bed_rows = _merge_rows(
        hcris_rows,
        await _call_rows_loader(hcris_loader, state_norm, resolved_ccns),
    )
    state_public_bed_rows = _merge_rows(
        state_bed_rows,
        await _call_rows_loader(state_bed_loader, state_norm, resolved_ccns),
    )
    official_rows = _merge_rows(
        official_evidence_rows,
        await _call_rows_loader(official_evidence_loader, official_query_name, state_norm),
    )

    pack: dict[str, Any] = {
        "workflow_id": "profile_evidence_pack",
        "status": "ready" if facility_rows or system_rows else "needs_review",
        "query": query,
        "metadata": {
            "mcp_server": MCP_SERVER,
            "mcp_tool": MCP_TOOL,
            "read_only": True,
            "healthcare_toolkit_write_policy": "MCP returns evidence only; Healthcare Toolkit decides what to persist or review.",
            "generated_at": retrieval_time,
        },
        "resolved_identifiers": {"ccns": resolved_ccns},
        "source_precedence": SOURCE_PRECEDENCE,
        "cache_preflight": cache_preflight,
        "system_identity_aliases": [],
        "current_hospital_roster": [],
        "source_identifiers": [],
        "addresses": [],
        "geography_candidates": [],
        "hospital_bed_counts": [],
        "system_bed_count_candidates": [],
        "bed_rollup_guidance": [],
        "affiliation_evidence": [],
        "facility_site_count_evidence": [],
        "conflicts": [],
        "unavailable_public_findings": [],
        "recovery_hints": [],
        "suggested_next_calls": [],
    }

    pack["evidence"] = _receipt(
        source_family="profile_evidence_pack_workflow",
        source_name="Healthcare Data MCP profile evidence pack workflow",
        dataset_id="profile_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_preflight",
        cache_freshness=_readiness_summary(cache_preflight),
        query=query,
        match_basis="workflow_input_with_public_source_resolution",
        confidence="source_scoped_candidate_pack",
        caveat="This MCP workflow is read-only and returns public evidence candidates. It does not write to Healthcare Toolkit and does not estimate missing facts.",
        next_step="Persist only source-backed fact rows with their evidence receipts; send conflicts and unavailable_public findings to manual review.",
        retrieved_at=retrieval_time,
    )
    pack["source_metadata"] = _source_metadata(pack["evidence"], "profile_evidence_pack_workflow")

    _add_system_identity(pack, system_rows, query=query, retrieved_at=retrieval_time)
    _add_facility_identity(pack, facility_rows, query=query, retrieved_at=retrieval_time)
    await _add_geography(
        pack,
        facility_rows,
        query=query,
        retrieved_at=retrieval_time,
        census_geocoder=census_geocoder or census_geocode_address,
        osm_geocoder=osm_geocoder or osm_geocode_address,
        reverse_geocoder=reverse_geocoder or reverse_geocode_coordinates,
    )
    _add_beds(
        pack,
        facility_rows,
        hcris_rows=hcris_bed_rows,
        state_bed_rows=state_public_bed_rows,
        query=query,
        retrieved_at=retrieval_time,
    )
    _add_affiliation(
        pack,
        facility_rows,
        system_rows,
        provider_rows=provider_rows,
        official_rows=official_rows,
        query=query,
        retrieved_at=retrieval_time,
    )
    _add_facility_counts(
        pack,
        system_rows,
        official_rows=official_rows,
        roster_count=len(facility_rows),
        query=query,
        retrieved_at=retrieval_time,
    )
    _add_required_field_findings(pack, required_fields or [], query=query, retrieved_at=retrieval_time)
    _add_recovery_hints(pack)

    pack["identity"] = identity_from_public_record(
        name=system_name or _first_system_name(system_rows) or system_slug,
        entity_type="health_system",
        ahrq_system_id=str(system_rows[0].get("health_sys_id") or "") if system_rows else "",
        source_name="profile_evidence_pack_input",
    ).to_dict()
    pack["identity_map"] = _identity_map(pack, query)

    return to_structured(pack)  # type: ignore[return-value]


async def census_geocode_address(address: str) -> dict[str, Any] | None:
    """Return Census geocoder match details for one address."""

    if not address.strip():
        return None
    url = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "vintage": "Current_Current",
        "format": "json",
    }
    response = await resilient_request("GET", url, params=params, timeout=20.0, max_retries=1)
    matches = response.json().get("result", {}).get("addressMatches", [])
    if not matches:
        return None
    match = matches[0]
    counties = match.get("geographies", {}).get("Counties", [])
    county = counties[0] if counties else {}
    coordinates = match.get("coordinates", {})
    return {
        "source_family": "census_geocoder",
        "status": "matched",
        "match_quality": "exact_or_candidate",
        "matched_address": match.get("matchedAddress", ""),
        "latitude": coordinates.get("y"),
        "longitude": coordinates.get("x"),
        "county": county.get("NAME", ""),
        "county_geoid": county.get("GEOID", ""),
        "source_url": url,
        "source_period": "Census Geocoder current benchmark/vintage",
    }


async def osm_geocode_address(address: str) -> dict[str, Any] | None:
    """Return acceptable OSM/Nominatim fallback details for one address."""

    if not address.strip():
        return None
    url = "https://nominatim.openstreetmap.org/search"
    response = await resilient_request(
        "GET",
        url,
        params={"q": address, "format": "jsonv2", "addressdetails": 1, "limit": 1},
        headers={"User-Agent": "healthcare-data-mcp/0.3 profile-evidence-pack"},
        timeout=20.0,
        max_retries=1,
    )
    rows = response.json()
    if not rows:
        return None
    row = rows[0]
    importance = _float_or_none(row.get("importance"))
    if importance is not None and importance < 0.2:
        return {
            "source_family": "osm_nominatim",
            "status": "rejected",
            "match_quality": "approximate_rejected",
            "caveat": "Nominatim result importance was below the acceptance threshold.",
            "source_url": url,
            "source_period": "Nominatim live lookup",
        }
    address_payload = row.get("address") if isinstance(row.get("address"), dict) else {}
    return {
        "source_family": "osm_nominatim",
        "status": "matched",
        "match_quality": "fallback_acceptable",
        "matched_address": row.get("display_name", ""),
        "latitude": row.get("lat"),
        "longitude": row.get("lon"),
        "county": address_payload.get("county", ""),
        "county_geoid": "",
        "source_url": url,
        "source_period": "Nominatim live lookup",
        "caveat": "OSM/Nominatim is fallback geography evidence and should be reviewed against Census when possible.",
    }


async def reverse_geocode_coordinates(latitude: float, longitude: float) -> dict[str, Any] | None:
    """Return Census reverse-geography details for coordinates."""

    url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
    response = await resilient_request(
        "GET",
        url,
        params={
            "x": longitude,
            "y": latitude,
            "benchmark": "Public_AR_Current",
            "vintage": "Current_Current",
            "format": "json",
        },
        timeout=20.0,
        max_retries=1,
    )
    counties = response.json().get("result", {}).get("geographies", {}).get("Counties", [])
    if not counties:
        return None
    county = counties[0]
    return {
        "source_family": "census_geocoder",
        "status": "matched",
        "match_quality": "reverse_geocode",
        "latitude": latitude,
        "longitude": longitude,
        "county": county.get("NAME", ""),
        "county_geoid": county.get("GEOID", ""),
        "source_url": url,
        "source_period": "Census Geocoder current benchmark/vintage",
    }


def _add_system_identity(
    pack: dict[str, Any],
    system_rows: list[dict[str, Any]],
    *,
    query: dict[str, Any],
    retrieved_at: str,
) -> None:
    for row in system_rows:
        name = str(row.get("health_sys_name") or row.get("system_name") or "")
        system_id = str(row.get("health_sys_id") or row.get("system_id") or "")
        pack["system_identity_aliases"].append(
            _value(
                field="system_identity",
                value={"system_id": system_id, "name": name, "hq_city": row.get("health_sys_city", ""), "hq_state": row.get("health_sys_state", "")},
                status="supported",
                source_family="ahrq_compendium",
                source_name="AHRQ Compendium of U.S. Health Systems",
                dataset_id="ahrq_health_system_compendium",
                source_period="AHRQ Compendium 2023",
                source_url="https://www.ahrq.gov/chsp/data-resources/compendium.html",
                landing_page="https://www.ahrq.gov/chsp/data-resources/compendium.html",
                cache_status="mixed_public_cache",
                cache_freshness="Check cache_preflight for ahrq_health_system_compendium readiness.",
                query={**query, "system_id": system_id, "system_name": name},
                match_basis="ahrq_system_id_or_name_resolution",
                confidence="high_for_source_identity_not_current_operator",
                caveat="AHRQ is a discovery/linkage source and may lag current ownership or operating names.",
                next_step="Use CCN, PECOS, CHOW, and official-page evidence before persisting current affiliation.",
                retrieved_at=retrieved_at,
            )
        )


def _add_facility_identity(
    pack: dict[str, Any],
    facility_rows: list[dict[str, Any]],
    *,
    query: dict[str, Any],
    retrieved_at: str,
) -> None:
    seen_addresses: dict[str, str] = {}
    for facility in facility_rows:
        ccn = str(facility.get("ccn") or "")
        name = str(facility.get("name") or "")
        address = _address_string(facility)
        source_local_ids = {
            "ccn": ccn,
            "prvdr_num": facility.get("PRVDR_NUM") or ccn,
            "ahrq_system_id": facility.get("health_sys_id", ""),
        }
        pack["current_hospital_roster"].append(
            _value(
                field="current_hospital_roster",
                value={"ccn": ccn, "name": name, "state": facility.get("state", ""), "address": address},
                status="supported",
                source_family="cms_pos_hgi",
                source_name="CMS Provider of Services / Hospital General Information",
                dataset_id="cms_provider_of_services",
                source_period="CMS POS Q4 2025 or configured local cache period",
                source_url="https://data.cms.gov/",
                landing_page="https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities",
                cache_status="mixed_public_cache",
                cache_freshness="Check cache_preflight for cms_provider_of_services and cms_hospital_general_info readiness.",
                query={**query, "ccn": ccn},
                match_basis="ccn_exact" if ccn else "ahrq_linked_facility_row",
                confidence="high_when_ccn_matches_cms_pos",
                caveat="Roster rows are public-source candidates for Healthcare Toolkit review, not a write operation.",
                next_step="Persist as profile_sources/profile_knowledge_objects only with this evidence and identity map.",
                retrieved_at=retrieved_at,
            )
        )
        pack["source_identifiers"].append(
            _value(
                field="source_local_identifiers",
                value=source_local_ids,
                status="supported" if ccn else "needs_review",
                source_family="cms_pos_hgi",
                source_name="CMS Provider of Services",
                dataset_id="cms_provider_of_services",
                source_period="CMS POS Q4 2025 or configured local cache period",
                source_url="https://data.cms.gov/",
                landing_page="https://data.cms.gov/",
                cache_status="mixed_public_cache",
                cache_freshness="Check cache_preflight for cms_provider_of_services readiness.",
                query={**query, "ccn": ccn},
                match_basis="source_local_identifier_row",
                confidence="high_for_ccn_when_present",
                caveat="Source-local identifiers must remain source-scoped.",
                next_step="Use CCN for facility joins and AHRQ system ID only for system-linkage context.",
                retrieved_at=retrieved_at,
            )
        )
        if address:
            pack["addresses"].append(
                _value(
                    field="addresses",
                    value={"ccn": ccn, "address": address, "county": facility.get("county", "")},
                    status="supported",
                    source_family="cms_pos_hgi",
                    source_name="CMS Provider of Services",
                    dataset_id="cms_provider_of_services",
                    source_period="CMS POS Q4 2025 or configured local cache period",
                    source_url="https://data.cms.gov/",
                    landing_page="https://data.cms.gov/",
                    cache_status="mixed_public_cache",
                    cache_freshness="Check cache_preflight for cms_provider_of_services readiness.",
                    query={**query, "ccn": ccn},
                    match_basis="ccn_exact_address_row",
                    confidence="high_for_source_address",
                    caveat="Address is source-scoped and should be conflict-checked against HGI and official pages.",
                    next_step="Use Census Geocoder output for county_geoid and coordinates.",
                    retrieved_at=retrieved_at,
                )
            )
            address_key = normalize_name(address)
            if address_key and address_key in seen_addresses and seen_addresses[address_key] != ccn:
                pack["conflicts"].append(
                    _finding(
                        field="duplicate_campus",
                        status="needs_review",
                        detail={"ccn": ccn, "other_ccn": seen_addresses[address_key], "address": address},
                        source_family="cms_pos_hgi",
                        source_name="CMS Provider of Services",
                        dataset_id="cms_provider_of_services",
                        source_period="CMS POS Q4 2025 or configured local cache period",
                        query={**query, "ccn": ccn},
                        match_basis="duplicate_normalized_address",
                        confidence="needs_review_duplicate_campus",
                        caveat="Multiple CCNs share the same normalized address; additive system-bed rollups may double count a campus.",
                        next_step="Review campus/licensure relationship before summing bed counts.",
                        retrieved_at=retrieved_at,
                    )
                )
            seen_addresses.setdefault(address_key, ccn)


async def _add_geography(
    pack: dict[str, Any],
    facility_rows: list[dict[str, Any]],
    *,
    query: dict[str, Any],
    retrieved_at: str,
    census_geocoder: Geocoder,
    osm_geocoder: Geocoder,
    reverse_geocoder: ReverseGeocoder,
) -> None:
    for facility in facility_rows:
        ccn = str(facility.get("ccn") or "")
        address = _address_string(facility)
        geocode = None
        source_family = "census_geocoder"
        if address:
            geocode = await _safe_geocode(census_geocoder, address)
            if not geocode:
                geocode = await _safe_geocode(osm_geocoder, address)
                source_family = str((geocode or {}).get("source_family") or "osm_nominatim")
        lat = _float_or_none(facility.get("latitude") or facility.get("LATITUDE") or facility.get("lat"))
        lon = _float_or_none(facility.get("longitude") or facility.get("LONGITUDE") or facility.get("lon"))
        if not geocode and lat is not None and lon is not None:
            geocode = await _safe_reverse_geocode(reverse_geocoder, lat, lon)
            source_family = str((geocode or {}).get("source_family") or "census_geocoder")
        if not geocode:
            pack["unavailable_public_findings"].append(
                _finding(
                    field="geography",
                    status="unavailable_public",
                    detail={"ccn": ccn, "address": address, "searched": ["census_geocoder", "osm_nominatim_fallback", "census_reverse_geocode_when_coordinates_present"]},
                    source_family="census_geocoder",
                    source_name="U.S. Census Geocoder",
                    dataset_id="census_geocoder",
                    source_period="request_time_lookup",
                    query={**query, "ccn": ccn, "address": address},
                    match_basis="no_acceptable_public_geocode_match",
                    confidence="not_available",
                    caveat="No coordinate/county candidate is returned without acceptable source evidence.",
                    next_step="Retry with a more complete street address or inspect CMS HGI address fields.",
                    retrieved_at=retrieved_at,
                )
            )
            continue
        status = str(geocode.get("status") or "matched")
        if status == "rejected":
            pack["unavailable_public_findings"].append(
                _finding(
                    field="geography",
                    status="needs_review",
                    detail={"ccn": ccn, **geocode},
                    source_family=source_family,
                    source_name=_geo_source_name(source_family),
                    dataset_id=source_family,
                    source_period=str(geocode.get("source_period") or "request_time_lookup"),
                    source_url=str(geocode.get("source_url") or ""),
                    query={**query, "ccn": ccn, "address": address},
                    match_basis=str(geocode.get("match_quality") or "approximate_rejected"),
                    confidence="rejected_approximate_match",
                    caveat=str(geocode.get("caveat") or "Approximate geography match was rejected."),
                    next_step="Retry Census Geocoder with a cleaner address or review manually.",
                    retrieved_at=retrieved_at,
                )
            )
            continue
        pack["geography_candidates"].append(
            _value(
                field="geography",
                value={
                    "ccn": ccn,
                    "latitude": _float_or_none(geocode.get("latitude")),
                    "longitude": _float_or_none(geocode.get("longitude")),
                    "county": geocode.get("county") or facility.get("county", ""),
                    "county_geoid": geocode.get("county_geoid", ""),
                    "matched_address": geocode.get("matched_address", ""),
                },
                status="supported",
                source_family=source_family,
                source_name=_geo_source_name(source_family),
                dataset_id=source_family,
                source_period=str(geocode.get("source_period") or "request_time_lookup"),
                source_url=str(geocode.get("source_url") or ""),
                landing_page=str(geocode.get("source_url") or ""),
                cache_status="live_api",
                cache_freshness="Live public geocoder lookup at request time.",
                query={**query, "ccn": ccn, "address": address},
                match_basis=str(geocode.get("match_quality") or "geocoder_match"),
                confidence="high_for_census_match" if source_family == "census_geocoder" else "medium_fallback_osm_match",
                caveat=str(geocode.get("caveat") or "Coordinates/counties are source geocoder output and should remain source-scoped."),
                next_step="Persist county_geoid/coordinates only with geocoder evidence and review OSM fallback rows before use.",
                retrieved_at=retrieved_at,
            )
        )


def _add_beds(
    pack: dict[str, Any],
    facility_rows: list[dict[str, Any]],
    *,
    hcris_rows: list[dict[str, Any]],
    state_bed_rows: list[dict[str, Any]],
    query: dict[str, Any],
    retrieved_at: str,
) -> None:
    included_values: list[int | float] = []
    duplicate_conflict = any(item.get("field") == "duplicate_campus" for item in pack["conflicts"])
    for facility in facility_rows:
        ccn = str(facility.get("ccn") or "")
        resolution = resolve_hospital_bed_source(
            ccn=ccn,
            state=str(facility.get("state") or query.get("state") or ""),
            year=_current_year(),
            target_scope="ccn",
            pos_row=facility.get("_pos_row") or {},
            hcris_row=_first_row_for_ccn(hcris_rows, ccn),
            ahrq_row=facility.get("_ahrq_row") or {},
            pa_rows=[row for row in state_bed_rows if not ccn or normalize_ccn(row.get("ccn")) == ccn],
        )
        resolution["candidates"] = [
            _bed_candidate_with_evidence(candidate, ccn=ccn, query=query, retrieved_at=retrieved_at)
            for candidate in resolution.get("candidates", [])
        ]
        resolution["rejected_candidates"] = [
            _bed_candidate_with_evidence(candidate, ccn=ccn, query=query, retrieved_at=retrieved_at, rejected=True)
            for candidate in resolution.get("rejected_candidates", [])
        ]
        selected = resolution.get("selected_bed_count")
        status = "supported" if selected not in (None, "") else "unavailable_public"
        if resolution.get("warnings"):
            status = "source_conflict"
            for warning in resolution["warnings"]:
                pack["conflicts"].append(
                    _finding(
                        field="hospital_bed_count",
                        status="source_conflict",
                        detail={"ccn": ccn, "warning": warning},
                        source_family="hcris_state_official_beds",
                        source_name="Hospital bed resolver",
                        dataset_id="hospital_bed_resolver",
                        source_period=str(_current_year()),
                        query={**query, "ccn": ccn},
                        match_basis="material_bed_source_variance",
                        confidence="needs_review",
                        caveat="Material variance across public bed sources must be reviewed before persistence.",
                        next_step="Compare candidate source periods and row scopes before selecting a Healthcare Toolkit metric value.",
                        retrieved_at=retrieved_at,
                    )
                )
        elif selected not in (None, "") and not duplicate_conflict:
            included_values.append(selected)  # type: ignore[arg-type]
        pack["hospital_bed_counts"].append(
            _value(
                field="hospital_bed_counts",
                value={"ccn": ccn, "selected_bed_count": selected, "resolution": resolution},
                status=status,
                source_family=_bed_source_family(str(resolution.get("selected_source") or "")),
                source_name=str(resolution.get("selected_source") or "Hospital bed resolver"),
                dataset_id=_bed_dataset_id(str(resolution.get("selected_source") or "")),
                source_period=str(resolution.get("source_period") or _current_year()),
                source_url="https://data.cms.gov/",
                landing_page="https://data.cms.gov/",
                cache_status="mixed_public_cache",
                cache_freshness="Check cache_preflight for POS, HCRIS, and state report readiness.",
                query={**query, "ccn": ccn},
                match_basis="bed_resolver_selected_candidate" if selected not in (None, "") else "no_valid_bed_candidate",
                confidence=str(resolution.get("confidence") or "not_available"),
                caveat="Bed values are source-scoped candidates. Do not sum duplicate campuses or incompatible scopes.",
                next_step="Persist selected source-backed metric only after reviewing rejected candidates, warnings, and rollup guidance.",
                retrieved_at=retrieved_at,
            )
        )
    if included_values:
        pack["system_bed_count_candidates"].append(
            _value(
                field="system_bed_count",
                value={"candidate_bed_count": int(sum(float(value) for value in included_values)), "included_facility_count": len(included_values)},
                status="supported" if not duplicate_conflict else "needs_review",
                source_family="bed_rollup_guidance",
                source_name="Healthcare Data MCP bed rollup guidance",
                dataset_id="profile_evidence_pack",
                source_period="request_time_rollup",
                source_url=PROJECT_LANDING_PAGE,
                landing_page=PROJECT_LANDING_PAGE,
                cache_status="derived_from_source_backed_candidates",
                cache_freshness="Uses source-backed facility bed candidates from this evidence pack.",
                query=query,
                match_basis="additive_ccn_scope_sum",
                confidence="candidate_rollup_not_final_authority",
                caveat="This is an additive candidate over included CCN-scope rows, not an estimate and not final current licensed-bed authority.",
                next_step="Exclude duplicate campuses and non-additive rows before persisting a system-level metric.",
                retrieved_at=retrieved_at,
            )
        )
    pack["bed_rollup_guidance"].append(
        _value(
            field="bed_rollup_guidance",
            value={
                "additive_when": "CCN-scope hospital rows represent distinct campuses/providers and source periods are comparable.",
                "non_additive_when": "Rows are campus/license/system scope, duplicate campuses, sub-units, specialty units, or materially conflicting source periods.",
                "duplicate_campus_conflict": duplicate_conflict,
                "system_rollup_status": "needs_review" if duplicate_conflict else "candidate_available" if included_values else "unavailable_public",
            },
            status="needs_review" if duplicate_conflict else "supported",
            source_family="bed_rollup_guidance",
            source_name="Healthcare Data MCP bed rollup guidance",
            dataset_id="profile_evidence_pack",
            source_period="request_time_rollup",
            source_url=PROJECT_LANDING_PAGE,
            landing_page=PROJECT_LANDING_PAGE,
            cache_status="derived_from_source_backed_candidates",
            cache_freshness="Uses source-backed facility bed candidates from this evidence pack.",
            query=query,
            match_basis="source_scope_rollup_policy",
            confidence="policy_guidance",
            caveat="Rollup guidance is not a source value; it explains how to review source-backed bed rows.",
            next_step="Healthcare Toolkit should persist guidance as a review object, not as a numeric metric value.",
            retrieved_at=retrieved_at,
        )
    )


def _add_affiliation(
    pack: dict[str, Any],
    facility_rows: list[dict[str, Any]],
    system_rows: list[dict[str, Any]],
    *,
    provider_rows: list[dict[str, Any]],
    official_rows: list[dict[str, Any]],
    query: dict[str, Any],
    retrieved_at: str,
) -> None:
    system_name = str(query.get("system_name") or _first_system_name(system_rows) or query.get("system_slug") or "")
    system_norm = normalize_name(system_name, remove_legal_suffixes=True)
    for facility in facility_rows:
        ccn = str(facility.get("ccn") or "")
        pack["affiliation_evidence"].append(
            _value(
                field="affiliation",
                value={"ccn": ccn, "system_name": _first_system_name(system_rows), "basis": "ahrq_hospital_linkage"},
                status="supported",
                source_family="ahrq_compendium",
                source_name="AHRQ Compendium hospital linkage",
                dataset_id="ahrq_health_system_compendium",
                source_period="AHRQ Compendium 2023",
                source_url="https://www.ahrq.gov/chsp/data-resources/compendium.html",
                landing_page="https://www.ahrq.gov/chsp/data-resources/compendium.html",
                cache_status="mixed_public_cache",
                cache_freshness="Check cache_preflight for ahrq_health_system_compendium readiness.",
                query={**query, "ccn": ccn},
                match_basis="ahrq_system_hospital_linkage",
                confidence="medium_for_historical_affiliation",
                caveat="AHRQ linkage may be stale and is not final current operator authority.",
                next_step="Cross-check current operator through PECOS, CHOW, and official system pages.",
                retrieved_at=retrieved_at,
            )
        )
    for row in provider_rows:
        pack["affiliation_evidence"].append(
            _value(
                field="affiliation",
                value=row,
                status="supported",
                source_family="pecos_chow_official_affiliation",
                source_name=str(row.get("source_name") or "CMS PECOS provider enrollment/ownership/CHOW"),
                dataset_id=str(row.get("dataset_id") or row.get("source_dataset_key") or "cms_pecos_public_provider_enrollment"),
                source_period=str(row.get("source_period") or row.get("source_date") or "configured public cache period"),
                source_url=str(row.get("source_url") or "https://data.cms.gov/provider-enrollment"),
                landing_page=str(row.get("landing_page") or "https://data.cms.gov/provider-enrollment"),
                cache_status=str(row.get("cache_status") or "mixed_public_cache"),
                cache_freshness=str(row.get("cache_freshness") or "Check provider-enrollment cache readiness."),
                query={**query, "ccn": normalize_ccn(row.get("ccn")) or ""},
                match_basis="pecos_provider_enrollment_or_chow_row",
                confidence=str(row.get("confidence") or "source_row"),
                caveat="PECOS/CHOW rows support enrollment or control review but should not be over-generalized beyond the source row.",
                next_step="Use current effective dates and active flags before persisting current operator evidence.",
                retrieved_at=retrieved_at,
            )
        )
    for row in official_rows:
        if not _is_affiliation_row(row):
            continue
        operator = str(row.get("current_operator") or row.get("operator") or row.get("system_name") or "")
        status = "supported"
        if system_norm and operator and normalize_name(operator, remove_legal_suffixes=True) != system_norm:
            status = "source_conflict"
            pack["conflicts"].append(
                _finding(
                    field="affiliation",
                    status="source_conflict",
                    detail={"ccn": normalize_ccn(row.get("ccn")) or "", "ahrq_system_name": system_name, "official_operator": operator},
                    source_family="pecos_chow_official_affiliation",
                    source_name=str(row.get("source_name") or "Official health-system page/report"),
                    dataset_id=str(row.get("dataset_id") or "official_system_page"),
                    source_period=str(row.get("source_period") or row.get("source_date") or "official page retrieval period"),
                    source_url=str(row.get("source_url") or ""),
                    query=query,
                    match_basis="current_official_operator_mismatch",
                    confidence="needs_review",
                    caveat="Official current-operator evidence conflicts with AHRQ/system query context.",
                    next_step="Route to manual review; do not overwrite current affiliation automatically.",
                    retrieved_at=retrieved_at,
                )
            )
        pack["affiliation_evidence"].append(
            _value(
                field="affiliation",
                value=row,
                status=status,
                source_family="pecos_chow_official_affiliation",
                source_name=str(row.get("source_name") or "Official health-system page/report"),
                dataset_id=str(row.get("dataset_id") or "official_system_page"),
                source_period=str(row.get("source_period") or row.get("source_date") or "official page retrieval period"),
                source_url=str(row.get("source_url") or ""),
                landing_page=str(row.get("landing_page") or row.get("source_url") or ""),
                cache_status=str(row.get("cache_status") or "live_or_reviewed_public_page"),
                cache_freshness=str(row.get("cache_freshness") or "Official page freshness depends on retrieval timestamp."),
                query=query,
                match_basis="official_current_operator_claim",
                confidence=str(row.get("confidence") or "candidate_official_page_claim"),
                caveat="Official page claims should be preserved with URL, retrieved date, and exact claim text.",
                next_step="Use with PECOS/CHOW where current operator is operationally material.",
                retrieved_at=retrieved_at,
            )
        )


def _add_facility_counts(
    pack: dict[str, Any],
    system_rows: list[dict[str, Any]],
    *,
    official_rows: list[dict[str, Any]],
    roster_count: int,
    query: dict[str, Any],
    retrieved_at: str,
) -> None:
    for row in system_rows:
        count = _int_or_none(row.get("hosp_count") or row.get("hospital_count"))
        if count is None:
            continue
        pack["facility_site_count_evidence"].append(
            _value(
                field="facility_site_count",
                value={"count": count, "count_type": "hospital_count", "claim_precision": "exact_source_field"},
                status="supported",
                source_family="ahrq_compendium",
                source_name="AHRQ Compendium of U.S. Health Systems",
                dataset_id="ahrq_health_system_compendium",
                source_period="AHRQ Compendium 2023",
                source_url="https://www.ahrq.gov/chsp/data-resources/compendium.html",
                landing_page="https://www.ahrq.gov/chsp/data-resources/compendium.html",
                cache_status="mixed_public_cache",
                cache_freshness="Check cache_preflight for ahrq_health_system_compendium readiness.",
                query=query,
                match_basis="ahrq_system_hospital_count_field",
                confidence="medium_for_source_period_count",
                caveat="AHRQ count is source-period system context and may lag current facility rosters.",
                next_step="Use official exact facility/site counts when available and record conflicts.",
                retrieved_at=retrieved_at,
            )
        )
    official_count_seen = False
    for row in official_rows:
        if not _is_count_row(row):
            continue
        official_count_seen = True
        claim_text = str(row.get("claim_text") or "")
        count = _int_or_none(row.get("count_value"))
        vague = _is_vague_count_claim(claim_text, row)
        status = "needs_review" if vague or count is None else "supported"
        if count is not None and roster_count and count != roster_count:
            pack["conflicts"].append(
                _finding(
                    field="facility_site_count",
                    status="source_conflict",
                    detail={"official_count": count, "roster_count": roster_count, "claim_text": claim_text},
                    source_family="official_system_pages_reports",
                    source_name=str(row.get("source_name") or "Official health-system page/report"),
                    dataset_id=str(row.get("dataset_id") or "official_system_page"),
                    source_period=str(row.get("source_period") or row.get("source_date") or "official page retrieval period"),
                    source_url=str(row.get("source_url") or ""),
                    query=query,
                    match_basis="official_count_roster_count_mismatch",
                    confidence="needs_review",
                    caveat="Exact official count differs from current roster candidate count.",
                    next_step="Review count semantics: hospitals, campuses, sites, facilities, clinics, and source periods may differ.",
                    retrieved_at=retrieved_at,
                )
            )
        pack["facility_site_count_evidence"].append(
            _value(
                field="facility_site_count",
                value={"count": count, "claim_text": claim_text, "claim_precision": "vague" if vague else "exact"},
                status=status,
                source_family="official_system_pages_reports",
                source_name=str(row.get("source_name") or "Official health-system page/report"),
                dataset_id=str(row.get("dataset_id") or "official_system_page"),
                source_period=str(row.get("source_period") or row.get("source_date") or "official page retrieval period"),
                source_url=str(row.get("source_url") or ""),
                landing_page=str(row.get("landing_page") or row.get("source_url") or ""),
                cache_status=str(row.get("cache_status") or "live_or_reviewed_public_page"),
                cache_freshness=str(row.get("cache_freshness") or "Official page freshness depends on retrieval timestamp."),
                query=query,
                match_basis="official_facility_site_count_claim",
                confidence="high_exact_official_claim" if status == "supported" else "needs_review_vague_count_claim",
                caveat="Persist exact counts only. Vague claims such as 'more than' or 'over' should become review objects, not metric values.",
                next_step="Healthcare Toolkit should persist exact supported counts and route vague claims to manual review.",
                retrieved_at=retrieved_at,
            )
        )
    if not official_count_seen:
        pack["unavailable_public_findings"].append(
            _finding(
                field="facility_site_count",
                status="unavailable_public",
                detail={"searched": ["official_system_pages_reports"], "system_name": query.get("system_name") or query.get("system_slug") or ""},
                source_family="official_system_pages_reports",
                source_name="Official health-system pages/reports",
                dataset_id="official_system_page",
                source_period="request_time_search_scope",
                query=query,
                match_basis="no_exact_official_facility_site_count_available",
                confidence="not_available",
                caveat="No exact official facility/site count was provided by configured public evidence inputs.",
                next_step="Call web-intelligence.scrape_system_profile or provide reviewed official page rows before persisting facility/site count facts.",
                retrieved_at=retrieved_at,
            )
        )


def _add_required_field_findings(
    pack: dict[str, Any],
    required_fields: list[str],
    *,
    query: dict[str, Any],
    retrieved_at: str,
) -> None:
    for field in required_fields:
        section = FIELD_TO_SECTION.get(str(field).strip(), str(field).strip())
        values = pack.get(section)
        if values:
            continue
        pack["unavailable_public_findings"].append(
            _finding(
                field=str(field),
                status="unavailable_public",
                detail={"required_field": field, "searched_section": section},
                source_family="profile_evidence_pack_workflow",
                source_name="Healthcare Data MCP profile evidence pack workflow",
                dataset_id="profile_evidence_pack",
                source_period="request_time_public_source_pack",
                query=query,
                match_basis="required_field_no_source_backed_candidate",
                confidence="not_available",
                caveat="Required field was not populated because no sufficient public evidence candidate was available.",
                next_step="Use suggested_next_calls to retrieve a stronger source or send the field to manual review.",
                retrieved_at=retrieved_at,
            )
        )


def _add_recovery_hints(pack: dict[str, Any]) -> None:
    pack["suggested_next_calls"] = [
        {
            "server": "cache-manager",
            "tool": "get_workflow_cache_readiness",
            "arguments": {"workflow_id": "profile_evidence_pack", "inputs": pack["query"]},
            "reason": "Inspect cache blockers before treating missing public rows as unavailable.",
        },
        {
            "server": "web-intelligence",
            "tool": "scrape_system_profile",
            "arguments": {"system_name": pack["query"].get("system_name") or pack["query"].get("system_slug") or ""},
            "reason": "Collect official/public page count and affiliation claims for review.",
        },
        {
            "server": "provider-enrollment",
            "tool": "profile_provider_control",
            "arguments": {"ccn": "<ccn from current_hospital_roster>"},
            "reason": "Cross-check current enrollment, ownership, and CHOW signals by exact CCN.",
        },
    ]
    if pack["conflicts"]:
        pack["recovery_hints"].append("Do not auto-persist conflicting fields. Preserve all evidence rows and route the field to manual review.")
    if pack["unavailable_public_findings"]:
        pack["recovery_hints"].append("Unavailable public findings show searched source families; they are not negative factual claims.")
    pack["recovery_hints"].append("Healthcare Toolkit should persist supported source rows into profile_sources and exact supported metrics into profile_metric_values; needs_review/source_conflict/unavailable_public rows belong in review queues or profile_knowledge_objects with caveats.")


def _resolve_system_rows(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
    *,
    state: str,
    system_slug: str,
    system_name: str,
    ccns: list[str],
) -> list[dict[str, Any]]:
    if systems_df.empty:
        return []
    candidates = systems_df.copy()
    if state and "health_sys_state" in candidates.columns:
        state_rows = candidates[candidates["health_sys_state"].astype(str).str.upper() == state]
        if not state_rows.empty:
            candidates = state_rows
    ids_from_ccn: set[str] = set()
    if ccns and not hospitals_df.empty and {"ccn", "health_sys_id"} <= set(hospitals_df.columns):
        ids_from_ccn = set(
            hospitals_df[hospitals_df["ccn"].astype(str).str.zfill(6).isin(ccns)]["health_sys_id"].astype(str)
        )
    if ids_from_ccn and "health_sys_id" in candidates.columns:
        matched = candidates[candidates["health_sys_id"].astype(str).isin(ids_from_ccn)]
        if not matched.empty:
            return _records(matched)
    wanted = normalize_name(system_name or system_slug.replace("-", " "), remove_legal_suffixes=True)
    if wanted and "health_sys_name" in candidates.columns:
        exact = candidates[
            candidates["health_sys_name"].map(lambda value: normalize_name(value, remove_legal_suffixes=True) == wanted)
        ]
        if not exact.empty:
            return _records(exact.head(3))
        contains = candidates[
            candidates["health_sys_name"].map(lambda value: wanted in normalize_name(value, remove_legal_suffixes=True))
        ]
        if not contains.empty:
            return _records(contains.head(3))
    return []


def _resolve_facility_rows(
    hospitals_df: pd.DataFrame,
    pos_df: pd.DataFrame,
    *,
    state: str,
    system_rows: list[dict[str, Any]],
    ccns: list[str],
) -> list[dict[str, Any]]:
    ahrq_rows: list[dict[str, Any]] = []
    system_ids = {str(row.get("health_sys_id") or row.get("system_id") or "") for row in system_rows}
    if not hospitals_df.empty:
        frame = hospitals_df.copy()
        if ccns and "ccn" in frame.columns:
            frame = frame[frame["ccn"].astype(str).str.zfill(6).isin(ccns)]
        elif system_ids and "health_sys_id" in frame.columns:
            frame = frame[frame["health_sys_id"].astype(str).isin(system_ids)]
        if state and "hosp_state" in frame.columns:
            frame = frame[frame["hosp_state"].astype(str).str.upper() == state]
        ahrq_rows = _records(frame)
    if not ahrq_rows and ccns:
        ahrq_rows = [{"ccn": ccn} for ccn in ccns]
    rows = []
    for ahrq in ahrq_rows:
        ccn = normalize_ccn(ahrq.get("ccn")) or ""
        pos = _pos_row(pos_df, ccn)
        rows.append(_facility_from_rows(ahrq, pos))
    return rows


def _facility_from_rows(ahrq: dict[str, Any], pos: dict[str, Any]) -> dict[str, Any]:
    return {
        "ccn": normalize_ccn(pos.get("PRVDR_NUM") or ahrq.get("ccn")) or "",
        "name": str(pos.get("FAC_NAME") or ahrq.get("hospital_name") or ""),
        "address": str(pos.get("ST_ADR") or ahrq.get("address") or ""),
        "city": str(pos.get("CITY_NAME") or ahrq.get("hosp_city") or ""),
        "state": str(pos.get("STATE_CD") or ahrq.get("hosp_state") or ""),
        "zip_code": str(pos.get("ZIP_CD") or ahrq.get("hosp_zip") or "")[:5],
        "county": str(pos.get("COUNTY_NAME") or ahrq.get("county") or ""),
        "health_sys_id": str(ahrq.get("health_sys_id") or ""),
        "_ahrq_row": ahrq,
        "_pos_row": pos,
        **{key: value for key, value in pos.items() if key not in {"_ahrq_row", "_pos_row"}},
    }


def _pos_row(pos_df: pd.DataFrame, ccn: str) -> dict[str, Any]:
    if pos_df.empty or not ccn or "PRVDR_NUM" not in pos_df.columns:
        return {}
    matched = pos_df[pos_df["PRVDR_NUM"].astype(str).str.zfill(6) == ccn]
    if matched.empty:
        return {}
    return dict(matched.iloc[0])


def _cache_preflight(cache_root: str | None) -> dict[str, Any]:
    try:
        return cache_core.list_cache_sources(cache_root=cache_root, workflow="profile_evidence_pack")
    except Exception as exc:
        return {
            "summary": {"error": 1},
            "sources": [],
            "status": "needs_review",
            "error": str(exc),
            "next_actions": ["Run cache-manager.get_workflow_cache_readiness for profile_evidence_pack."],
        }


def _value(
    *,
    field: str,
    value: Any,
    status: str,
    source_family: str,
    source_name: str,
    dataset_id: str,
    source_period: str,
    source_url: str,
    landing_page: str,
    cache_status: str,
    cache_freshness: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    retrieved_at: str,
) -> dict[str, Any]:
    receipt = _receipt(
        source_family=source_family,
        source_name=source_name,
        dataset_id=dataset_id,
        source_period=source_period,
        source_url=source_url,
        landing_page=landing_page,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
        retrieved_at=retrieved_at,
    )
    return {
        "field": field,
        "value": value,
        "status": status,
        "dataset_id": dataset_id,
        "source_family": source_family,
        "source_period": source_period,
        "retrieval_access_date": retrieved_at,
        "cache_status": cache_status,
        "confidence": confidence,
        "match_basis": match_basis,
        "caveat": caveat,
        "evidence": receipt,
        "source_metadata": _source_metadata(receipt, source_family),
        "metadata": {"mcp_server": MCP_SERVER, "mcp_tool": MCP_TOOL},
    }


def _finding(
    *,
    field: str,
    status: str,
    detail: Any,
    source_family: str,
    source_name: str,
    dataset_id: str,
    source_period: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    retrieved_at: str,
    source_url: str = "",
) -> dict[str, Any]:
    return _value(
        field=field,
        value=detail,
        status=status,
        source_family=source_family,
        source_name=source_name,
        dataset_id=dataset_id,
        source_period=source_period,
        source_url=source_url,
        landing_page=source_url,
        cache_status="searched_public_source",
        cache_freshness="Search/access date is this evidence pack retrieval time unless source row states otherwise.",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
        retrieved_at=retrieved_at,
    )


def _receipt(
    *,
    source_family: str,
    source_name: str,
    dataset_id: str,
    source_period: str,
    source_url: str,
    landing_page: str,
    cache_status: str,
    cache_freshness: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    retrieved_at: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url or PROJECT_LANDING_PAGE,
        dataset_id=dataset_id or source_family,
        source_period=source_period,
        landing_page=landing_page or source_url or PROJECT_LANDING_PAGE,
        retrieved_at=retrieved_at,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope=ENTITY_SCOPE,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _source_metadata(receipt: dict[str, Any], source_family: str) -> dict[str, Any]:
    return {
        "source_name": receipt.get("source_name", ""),
        "source_url": receipt.get("source_url", ""),
        "dataset_id": receipt.get("dataset_id", ""),
        "source_family": source_family,
        "source_period": receipt.get("source_period", ""),
        "landing_page": receipt.get("landing_page", ""),
        "retrieved_at": receipt.get("retrieved_at", ""),
        "source_modified": receipt.get("source_modified", ""),
        "cache_status": receipt.get("cache_status", ""),
        "cache_freshness": receipt.get("cache_freshness", ""),
        "entity_scope": receipt.get("entity_scope", ENTITY_SCOPE),
        "query": receipt.get("query", {}),
        "cache_key": receipt.get("cache_key", ""),
        "match_basis": receipt.get("match_basis", ""),
        "confidence": receipt.get("confidence", ""),
        "caveat": receipt.get("caveat", ""),
        "next_step": receipt.get("next_step", ""),
        "mcp_server": MCP_SERVER,
        "mcp_tool": MCP_TOOL,
    }


def _bed_candidate_with_evidence(
    candidate: dict[str, Any],
    *,
    ccn: str,
    query: dict[str, Any],
    retrieved_at: str,
    rejected: bool = False,
) -> dict[str, Any]:
    source = str(candidate.get("source") or "Hospital bed resolver")
    source_family = _bed_source_family(source)
    payload = dict(candidate)
    payload.update(
        _value(
            field="hospital_bed_count_candidate",
            value={"ccn": ccn, "selected_bed_count": candidate.get("selected_bed_count"), "source_field": candidate.get("source_field")},
            status="needs_review" if rejected else "supported",
            source_family=source_family,
            source_name=source,
            dataset_id=_bed_dataset_id(source),
            source_period=str(candidate.get("source_period") or _current_year()),
            source_url=str(candidate.get("source_artifact") or "https://data.cms.gov/"),
            landing_page=str(candidate.get("source_artifact") or "https://data.cms.gov/"),
            cache_status="mixed_public_cache",
            cache_freshness="Check cache_preflight for source readiness.",
            query={**query, "ccn": ccn, "source_field": candidate.get("source_field", "")},
            match_basis="bed_candidate_rejected" if rejected else "bed_candidate_source_row",
            confidence=str(candidate.get("confidence") or "source_row"),
            caveat=str(candidate.get("rejection_reason") or "Bed candidate must be reviewed for scope and source period before use."),
            next_step="Use selected resolver output and rejected candidates to decide Healthcare Toolkit metric persistence.",
            retrieved_at=retrieved_at,
        )
    )
    return payload


def _identity_map(pack: dict[str, Any], query: dict[str, Any]) -> dict[str, Any]:
    ccns = sorted(
        {
            str(item.get("value", {}).get("ccn") or "")
            for item in pack.get("current_hospital_roster", [])
            if isinstance(item.get("value"), dict) and item.get("value", {}).get("ccn")
        }
    )
    return {
        "entity_scope": ENTITY_SCOPE,
        "join_keys": [
            {"field": "state", "values": [query.get("state")] if query.get("state") else [], "status": "provided" if query.get("state") else "missing", "used_by": [MCP_TOOL]},
            {"field": "system_slug", "values": [query.get("system_slug")] if query.get("system_slug") else [], "status": "provided" if query.get("system_slug") else "missing", "used_by": [MCP_TOOL]},
            {"field": "canonical_name", "values": [normalize_name(query.get("system_name"), remove_legal_suffixes=True)] if query.get("system_name") else [], "status": "provided" if query.get("system_name") else "missing", "used_by": [MCP_TOOL]},
            {"field": "ccn", "values": ccns or query.get("ccns", []), "status": "provided" if (ccns or query.get("ccns")) else "missing", "used_by": [MCP_TOOL]},
        ],
        "source_claims": [
            {
                "collection": "profile_evidence_pack",
                "identity_paths": ["evidence.query"],
                "evidence_path": "evidence",
                "source_metadata_path": "source_metadata",
                "row_evidence_paths": [path for path in _row_evidence_paths() if values_at_path(pack, path)],
                "match_policy": "exact_identifiers_before_names",
            },
        ],
        "conflict_policy": [
            "Do not overwrite Healthcare Toolkit profile fields from source_conflict, unavailable_public, or needs_review rows.",
            "Use CCN and source-local identifiers for facility facts; use AHRQ system IDs only as linkage context.",
            "Preserve all source evidence rows and route current-operator conflicts to manual review.",
        ],
    }


def _row_evidence_paths() -> list[str]:
    return [
        "system_identity_aliases[].evidence",
        "current_hospital_roster[].evidence",
        "source_identifiers[].evidence",
        "addresses[].evidence",
        "geography_candidates[].evidence",
        "hospital_bed_counts[].evidence",
        "hospital_bed_counts[].value.resolution.candidates[].evidence",
        "hospital_bed_counts[].value.resolution.rejected_candidates[].evidence",
        "system_bed_count_candidates[].evidence",
        "bed_rollup_guidance[].evidence",
        "affiliation_evidence[].evidence",
        "facility_site_count_evidence[].evidence",
        "conflicts[].evidence",
        "unavailable_public_findings[].evidence",
    ]


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [dict(row) for row in frame.to_dict(orient="records")]


async def _call_rows_loader(loader: RowsLoader | None, *args: Any) -> list[dict[str, Any]]:
    if loader is None:
        return []
    result = loader(*args)
    if isawaitable(result):
        result = await result
    return [dict(row) for row in result or [] if isinstance(row, dict)]


def _merge_rows(*row_groups: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for group in row_groups:
        for row in group or []:
            key = repr(sorted((str(k), str(v)) for k, v in row.items()))
            if key in seen:
                continue
            seen.add(key)
            rows.append(dict(row))
    return rows


def _normalized_ccns(values: Iterable[Any]) -> list[str]:
    return sorted({normalized for value in values if (normalized := normalize_ccn(value))})


def _address_string(facility: dict[str, Any]) -> str:
    street = facility.get("address") or facility.get("ST_ADR")
    if not str(street or "").strip():
        return ""
    parts = [
        street,
        facility.get("city") or facility.get("CITY_NAME"),
        facility.get("state") or facility.get("STATE_CD"),
        facility.get("zip_code") or facility.get("ZIP_CD"),
    ]
    return ", ".join(str(part).strip() for part in parts if str(part or "").strip())


def _first_system_name(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        name = str(row.get("health_sys_name") or row.get("system_name") or "")
        if name:
            return name
    return ""


def _first_row_for_ccn(rows: list[dict[str, Any]], ccn: str) -> dict[str, Any]:
    wanted = normalize_ccn(ccn)
    for row in rows:
        if normalize_ccn(row.get("ccn") or row.get("provider_ccn") or row.get("prvdr_num")) == wanted:
            return row
    return {}


def _bed_source_family(source: str) -> str:
    lower = source.lower()
    if "provider of services" in lower or "pos" in lower:
        return "cms_pos_hgi"
    if "hcris" in lower or "cost report" in lower:
        return "hcris_state_official_beds"
    if "pennsylvania" in lower or "state" in lower or "official" in lower:
        return "hcris_state_official_beds"
    if "ahrq" in lower:
        return "ahrq_compendium"
    return "hcris_state_official_beds"


def _bed_dataset_id(source: str) -> str:
    lower = source.lower()
    if "provider of services" in lower or "pos" in lower:
        return "cms_provider_of_services"
    if "hcris" in lower or "cost report" in lower:
        return "cms_cost_report"
    if "pennsylvania" in lower:
        return "pa_hospital_reports"
    if "ahrq" in lower:
        return "ahrq_health_system_compendium"
    if "official" in lower:
        return "official_system_page"
    return "hospital_bed_resolver"


def _geo_source_name(source_family: str) -> str:
    return "OpenStreetMap Nominatim" if source_family == "osm_nominatim" else "U.S. Census Geocoder"


def _is_affiliation_row(row: dict[str, Any]) -> bool:
    return bool(row.get("current_operator") or row.get("operator") or row.get("affiliation_status") or row.get("affiliation"))


def _is_count_row(row: dict[str, Any]) -> bool:
    return bool(row.get("count_value") not in (None, "") or row.get("claim_text") or row.get("count_type"))


def _is_vague_count_claim(claim_text: str, row: dict[str, Any]) -> bool:
    if str(row.get("claim_precision") or "").lower() in {"exact", "precise"}:
        return False
    text = claim_text.lower()
    return bool(re.search(r"\b(more than|over|nearly|approximately|about|around|at least)\b|\+", text))


def _safe_geocode(geocoder: Geocoder, address: str) -> AwaitableResult:
    return AwaitableResult(geocoder(address))


def _safe_reverse_geocode(geocoder: ReverseGeocoder, latitude: float, longitude: float) -> AwaitableResult:
    return AwaitableResult(geocoder(latitude, longitude))


class AwaitableResult:
    """Small awaitable wrapper that converts geocoder exceptions to None."""

    def __init__(self, awaitable: Awaitable[dict[str, Any] | None]):
        self.awaitable = awaitable

    def __await__(self):
        async def _run() -> dict[str, Any] | None:
            try:
                return await self.awaitable
            except Exception:
                return None

        return _run().__await__()


def _readiness_summary(cache_preflight: dict[str, Any]) -> str:
    summary = cache_preflight.get("summary")
    return f"cache_preflight={summary}" if summary else "cache_preflight_unavailable"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _current_year() -> int:
    return datetime.now(timezone.utc).year


def _float_or_none(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    number = _float_or_none(value)
    if number is None:
        return None
    return int(number)
