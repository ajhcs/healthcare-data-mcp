"""Tests for drive-time MCP tool provenance wrappers."""

from __future__ import annotations

import pandas as pd
import pytest

from servers.drive_time import server
from shared.utils.mcp_response import validate_evidence_receipt
from shared.utils.source_backed_result import validate_source_claim_paths
from tests.helpers import parse_tool_result


class FakeOSRM:
    async def route(self, origin: tuple[float, float], dest: tuple[float, float]) -> dict:
        assert origin == (-75.16, 39.95)
        assert dest == (-75.17, 39.96)
        return {"duration_seconds": 600.0, "distance_meters": 3218.688}

    async def table(
        self,
        coords: list[tuple[float, float]],
        sources: list[int] | None = None,
        destinations: list[int] | None = None,
    ) -> dict:
        assert coords
        assert sources is not None
        assert destinations is not None
        return {
            "durations": [[600.0 for _ in destinations] for _ in sources],
            "distances": [[3218.688 for _ in destinations] for _ in sources],
        }


class FakeORS:
    async def isochrone(self, lon: float, lat: float, ranges: list[int]) -> dict:
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"value": ranges[0]},
                    "geometry": {"type": "Polygon", "coordinates": []},
                }
            ],
        }


def _assert_receipt(result: dict, *, dataset_id: str, match_basis: str) -> None:
    assert result["evidence"]["dataset_id"] == dataset_id
    assert result["evidence"]["match_basis"] == match_basis
    validate_evidence_receipt(result["evidence"], require_content=True)


def _assert_boundary_traceability(result: dict) -> None:
    assert result["identity_map"]["source_claims"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_compute_drive_time_returns_evidence_and_coordinate_identity(monkeypatch) -> None:
    monkeypatch.setattr(server, "_get_osrm", lambda: FakeOSRM())

    result = parse_tool_result(await server.compute_drive_time(39.95, -75.16, 39.96, -75.17))

    assert result["duration_minutes"] == 10.0
    _assert_receipt(result, dataset_id="osrm_route", match_basis="caller_supplied_coordinates_osrm_route")
    assert result["source_metadata"]["dataset_id"] == "osrm_route"
    assert {entity["entity_type"] for entity in result["identity_map"]["entities"]} == {
        "origin_coordinate",
        "destination_coordinate",
    }
    _assert_boundary_traceability(result)


@pytest.mark.asyncio
async def test_compute_drive_time_matrix_returns_evidence_and_location_map(monkeypatch) -> None:
    monkeypatch.setattr(server, "_get_osrm", lambda: FakeOSRM())
    origins = [{"id": "market_zip_19107", "lat": 39.95, "lon": -75.16}]
    destinations = [{"id": "hospital_390001", "lat": 39.96, "lon": -75.17}]

    result = parse_tool_result(await server.compute_drive_time_matrix(origins, destinations))

    assert result["matrix"][0]["duration_minutes"] == 10.0
    assert result["source_metadata"]["dataset_id"] == "osrm_table_matrix"
    validate_evidence_receipt(result["matrix"][0]["evidence"], require_content=True)
    assert result["matrix"][0]["evidence"]["dataset_id"] == "osrm_table_matrix"
    assert result["matrix"][0]["evidence"]["match_basis"] == "osrm_table_matrix_origin_destination_cell"
    assert result["matrix"][0]["evidence"]["query"]["origin_id"] == "market_zip_19107"
    assert result["matrix"][0]["evidence"]["query"]["destination_id"] == "hospital_390001"
    _assert_receipt(result, dataset_id="osrm_table_matrix", match_basis="caller_supplied_coordinate_ids_osrm_table")
    assert {entity["canonical_name"] for entity in result["identity_map"]["entities"]} == {
        "market_zip_19107",
        "hospital_390001",
    }
    _assert_boundary_traceability(result)


@pytest.mark.asyncio
async def test_generate_isochrone_returns_evidence(monkeypatch) -> None:
    monkeypatch.setattr(server, "_get_ors", lambda: FakeORS())

    result = parse_tool_result(await server.generate_isochrone(39.95, -75.16, minutes=[15]))

    assert result["type"] == "FeatureCollection"
    assert result["source_metadata"]["dataset_id"] == "openrouteservice_drive_isochrone"
    _assert_receipt(
        result,
        dataset_id="openrouteservice_drive_isochrone",
        match_basis="caller_supplied_coordinate_openrouteservice_isochrone",
    )
    assert result["identity"]["entity_type"] == "coordinate"


@pytest.mark.asyncio
async def test_find_competing_facilities_returns_evidence_and_facility_identity(monkeypatch) -> None:
    monkeypatch.setattr(server, "_get_osrm", lambda: FakeOSRM())

    async def fake_load_facilities() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "facility_id": "390001",
                    "facility_name": "Example Hospital",
                    "address": "100 Market St",
                    "city": "Philadelphia",
                    "state": "PA",
                    "zip_code": "19107",
                    "latitude": "39.96",
                    "longitude": "-75.17",
                }
            ]
        )

    monkeypatch.setattr(server, "_load_facilities", fake_load_facilities)

    result = parse_tool_result(await server.find_competing_facilities(39.95, -75.16, radius_minutes=30))

    assert result["facilities"][0]["ccn"] == "390001"
    validate_evidence_receipt(result["facilities"][0]["evidence"], require_content=True)
    assert result["facilities"][0]["evidence"]["dataset_id"] == "drive_time_competing_facilities"
    assert result["facilities"][0]["evidence"]["match_basis"] == "competing_facility_drive_time_row"
    assert result["facilities"][0]["evidence"]["query"]["ccn"] == "390001"
    _assert_receipt(
        result,
        dataset_id="drive_time_competing_facilities",
        match_basis="cms_facility_location_candidates_plus_osrm_drive_time_threshold",
    )
    assert result["identity_map"]["entities"][0]["ccn"] == "390001"
    assert result["source_metadata"]["sources"][0]["dataset_id"] == "cms_hospital_general_information"
    _assert_boundary_traceability(result)


@pytest.mark.asyncio
async def test_find_competing_facilities_empty_bbox_still_returns_evidence(monkeypatch) -> None:
    async def fake_load_facilities() -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "facility_id": "390001",
                    "facility_name": "Example Hospital",
                    "latitude": "30.0",
                    "longitude": "-90.0",
                }
            ]
        )

    monkeypatch.setattr(server, "_load_facilities", fake_load_facilities)

    result = parse_tool_result(await server.find_competing_facilities(39.95, -75.16, radius_minutes=15))

    assert result["count"] == 0
    _assert_receipt(
        result,
        dataset_id="drive_time_competing_facilities",
        match_basis="cms_facility_location_candidates_plus_osrm_drive_time_threshold",
    )


@pytest.mark.asyncio
async def test_compute_accessibility_score_returns_evidence_and_input_identity_map(monkeypatch) -> None:
    monkeypatch.setattr(server, "_get_osrm", lambda: FakeOSRM())
    demand_points = [{"id": "19107", "lat": 39.95, "lon": -75.16, "population": 1000}]
    supply_points = [{"id": "390001", "lat": 39.96, "lon": -75.17, "capacity": 100}]

    result = parse_tool_result(await server.compute_accessibility_score(demand_points, supply_points, catchment_minutes=30))

    assert result["results"][0]["accessibility_score"] == 0.1
    validate_evidence_receipt(result["results"][0]["evidence"], require_content=True)
    assert result["results"][0]["evidence"]["dataset_id"] == "drive_time_e2sfca_accessibility"
    assert result["results"][0]["evidence"]["match_basis"] == "e2sfca_demand_point_score_row"
    assert result["results"][0]["evidence"]["query"]["demand_id"] == "19107"
    assert result["results"][0]["evidence"]["query"]["reachable_supply_count"] == 1
    _assert_receipt(
        result,
        dataset_id="drive_time_e2sfca_accessibility",
        match_basis="caller_supplied_demand_supply_coordinates_osrm_table_e2sfca",
    )
    assert {entity["entity_type"] for entity in result["identity_map"]["entities"]} == {
        "demand_coordinate",
        "supply_coordinate",
    }
    _assert_boundary_traceability(result)


@pytest.mark.asyncio
async def test_compute_accessibility_score_empty_inputs_returns_evidence() -> None:
    result = parse_tool_result(await server.compute_accessibility_score([], [], catchment_minutes=30))

    assert result["ok"] is False
    _assert_receipt(
        result,
        dataset_id="drive_time_e2sfca_accessibility",
        match_basis="caller_supplied_demand_supply_coordinates_osrm_table_e2sfca",
    )
