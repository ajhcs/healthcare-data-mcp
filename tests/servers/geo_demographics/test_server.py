"""Tests for geo-demographics MCP tool wrappers."""

from __future__ import annotations

import pytest

from servers.geo_demographics import server
from shared.utils.mcp_response import validate_evidence_receipt
from tests.helpers import parse_tool_result


def _sample_demographics(zcta: str, year: int = 2023) -> dict:
    return {
        "zcta": zcta,
        "year": year,
        "total_population": 1000,
        "median_age": 40.0,
        "male_population": 480,
        "female_population": 520,
        "age_distribution": {"under_18": 200, "age_18_to_64": 600, "age_65_plus": 200},
        "median_household_income": 75000,
        "insurance": {
            "private": 500,
            "public_medicare": 200,
            "public_medicaid": 150,
            "uninsured": 50,
            "uninsured_pct": 5.0,
        },
    }


def _gv_row() -> dict:
    return {
        "year": "2023",
        "geo_level": "County",
        "geo_code": "42101",
        "geo_desc": "Philadelphia County, PA",
        "total_beneficiaries": 100000,
        "ma_penetration_pct": 45.1,
        "avg_age": 72.4,
        "pct_female": 55.0,
        "pct_dual_eligible": 22.2,
        "per_capita_spending": 12000.0,
        "ip_spending_per_capita": 2500.0,
        "op_spending_per_capita": 1800.0,
        "physician_spending_per_capita": 2100.0,
        "snf_spending_per_capita": 900.0,
        "discharges_per_1000": 230.0,
        "er_visits_per_1000": 510.0,
        "readmission_rate": 14.2,
    }


def _assert_receipt(result: dict, *, dataset_id: str, match_basis: str) -> None:
    assert result["evidence"]["dataset_id"] == dataset_id
    assert result["evidence"]["match_basis"] == match_basis
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_zcta_demographics_returns_evidence_and_identity(monkeypatch) -> None:
    async def fake_get(zcta: str, year: int = 2023) -> dict:
        return _sample_demographics(zcta, year)

    monkeypatch.setattr(server, "get_demographics_for_zcta", fake_get)

    result = parse_tool_result(await server.get_zcta_demographics("19107", year=2023))

    assert result["zcta"] == "19107"
    _assert_receipt(result, dataset_id="census_acs5_zcta_demographics", match_basis="zcta_exact_acs5_api_row")
    assert result["identity"]["zip_code"] == "19107"


@pytest.mark.asyncio
async def test_get_zcta_demographics_batch_returns_collection_evidence(monkeypatch) -> None:
    async def fake_batch(zctas: list[str], year: int = 2023) -> list[dict]:
        return [_sample_demographics(zcta, year) for zcta in zctas]

    monkeypatch.setattr(server, "get_demographics_batch", fake_batch)

    result = parse_tool_result(await server.get_zcta_demographics_batch(["19107", "19104"], year=2023))

    assert result["count"] == 2
    assert [row["zcta"] for row in result["results"]] == ["19107", "19104"]
    validate_evidence_receipt(result["results"][0]["evidence"], require_content=True)
    assert result["results"][0]["evidence"]["dataset_id"] == "census_acs5_zcta_demographics"
    assert result["results"][0]["evidence"]["match_basis"] == "zcta_exact_acs5_batch_row"
    assert result["results"][0]["evidence"]["query"]["zcta"] == "19107"
    _assert_receipt(result, dataset_id="census_acs5_zcta_demographics", match_basis="zcta_exact_batch_acs5_api_rows")
    assert {entity["zip_code"] for entity in result["identity_map"]["entities"]} == {"19107", "19104"}


@pytest.mark.asyncio
async def test_get_zcta_adjacency_returns_tiger_evidence(monkeypatch) -> None:
    async def fake_adjacent(zcta: str) -> list[str]:
        assert zcta == "19107"
        return ["19103", "19106"]

    monkeypatch.setattr(server, "get_adjacent_zctas", fake_adjacent)

    result = parse_tool_result(await server.get_zcta_adjacency("19107"))

    assert result["adjacent_zctas"] == ["19103", "19106"]
    assert result["adjacent_zcta_rows"][0]["adjacent_zcta"] == "19103"
    validate_evidence_receipt(result["adjacent_zcta_rows"][0]["evidence"], require_content=True)
    assert result["adjacent_zcta_rows"][0]["evidence"]["dataset_id"] == "census_tiger_zcta_adjacency"
    assert result["adjacent_zcta_rows"][0]["evidence"]["match_basis"] == "tiger_zcta_adjacency_neighbor_row"
    assert result["adjacent_zcta_rows"][0]["evidence"]["query"]["adjacent_zcta"] == "19103"
    _assert_receipt(result, dataset_id="census_tiger_zcta_adjacency", match_basis="zcta_exact_tiger_adjacency_cache")
    assert {entity["zip_code"] for entity in result["identity_map"]["entities"]} == {"19103", "19106"}


@pytest.mark.asyncio
async def test_get_medicare_enrollment_returns_gv_evidence(monkeypatch) -> None:
    async def fake_cache() -> bool:
        return True

    monkeypatch.setattr(server.gv_loaders, "ensure_gv_cached", fake_cache)
    monkeypatch.setattr(server.gv_loaders, "query_gv", lambda level, code: _gv_row())

    result = parse_tool_result(await server.get_medicare_enrollment(county_fips="42101"))

    assert result["geography_code"] == "42101"
    _assert_receipt(result, dataset_id="cms_medicare_geographic_variation_puf", match_basis="geography_code_exact_cms_gv_latest_year")
    assert result["identity"]["unresolved_identifiers"][0] == {"type": "county", "value": "42101"}


@pytest.mark.asyncio
async def test_get_geographic_variation_returns_gv_evidence(monkeypatch) -> None:
    async def fake_cache() -> bool:
        return True

    monkeypatch.setattr(server.gv_loaders, "ensure_gv_cached", fake_cache)
    monkeypatch.setattr(server.gv_loaders, "query_gv", lambda level, code: _gv_row())

    result = parse_tool_result(await server.get_geographic_variation("county", "42101"))

    assert result["readmission_rate"] == 14.2
    _assert_receipt(result, dataset_id="cms_medicare_geographic_variation_puf", match_basis="geography_code_exact_cms_gv_latest_year")
    assert result["identity"]["entity_type"] == "county_geography"


@pytest.mark.asyncio
async def test_crosswalk_zip_returns_hud_evidence_and_identity_map(monkeypatch) -> None:
    class FakeResponse:
        def json(self) -> dict:
            return {
                "data": {
                    "results": [
                        {"county": "42101", "res_ratio": "0.8", "bus_ratio": "0.7", "oth_ratio": "0.6", "tot_ratio": "0.75"}
                    ]
                }
            }

    async def fake_request(*args, **kwargs) -> FakeResponse:
        return FakeResponse()

    monkeypatch.setenv("HUD_API_TOKEN", "test-token")
    monkeypatch.setattr(server, "resilient_request", fake_request)

    result = parse_tool_result(await server.crosswalk_zip("19107", target="county"))

    assert result["results"][0]["target_code"] == "42101"
    validate_evidence_receipt(result["results"][0]["evidence"], require_content=True)
    assert result["results"][0]["evidence"]["dataset_id"] == "hud_usps_zip_crosswalk"
    assert result["results"][0]["evidence"]["match_basis"] == "hud_zip_crosswalk_allocation_row"
    assert result["results"][0]["evidence"]["query"]["target_code"] == "42101"
    _assert_receipt(result, dataset_id="hud_usps_zip_crosswalk", match_basis="zip_exact_hud_usps_crosswalk")
    assert result["identity"]["zip_code"] == "19107"
    assert result["identity_map"]["entities"][0]["unresolved_identifiers"][0] == {"type": "county", "value": "42101"}
