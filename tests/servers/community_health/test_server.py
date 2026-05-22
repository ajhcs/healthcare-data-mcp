from __future__ import annotations

import pytest

from servers.community_health import data_loaders, server
from shared.utils.mcp_response import validate_evidence_receipt


SOURCE = {
    "name": "CDC PLACES: Local Data for Better Health",
    "dataset_title": "PLACES: Local Data for Better Health, County Data, 2025 release",
    "dataset_id": "fixture-county",
    "geography_type": "county",
    "release": "2025 release",
    "source_url": "https://data.cdc.gov/resource/fixture-county.json",
    "landing_page": "https://data.cdc.gov/d/fixture-county",
    "modified": "2026-01-01T00:00:00Z",
    "record_count": 2,
    "domain": "data.cdc.gov",
    "interpretation": "PLACES values are model-based community estimates for geographic areas, not patient-level facts.",
}


RAW_ROWS = [
    {
        "year": "2022",
        "stateabbr": "PA",
        "statedesc": "Pennsylvania",
        "locationname": "Allegheny",
        "datasource": "BRFSS",
        "category": "Health Outcomes",
        "measure": "High blood pressure among adults aged >=18 years",
        "data_value_unit": "%",
        "data_value_type": "Age-adjusted prevalence",
        "data_value": "33.4",
        "low_confidence_limit": "31.2",
        "high_confidence_limit": "35.6",
        "totalpop18plus": "955000",
        "totalpopulation": "1230000",
        "locationid": "42003",
        "categoryid": "HLTHOUT",
        "measureid": "BPHIGH",
        "datavaluetypeid": "AgeAdjPrv",
        "short_question_text": "High Blood Pressure",
    },
    {
        "year": "2022",
        "stateabbr": "PA",
        "statedesc": "Pennsylvania",
        "locationname": "Philadelphia",
        "datasource": "BRFSS",
        "category": "Health Outcomes",
        "measure": "High blood pressure among adults aged >=18 years",
        "data_value_unit": "%",
        "data_value_type": "Age-adjusted prevalence",
        "data_value": "35.0",
        "low_confidence_limit": "33.0",
        "high_confidence_limit": "37.0",
        "totalpop18plus": "1200000",
        "totalpopulation": "1600000",
        "locationid": "42101",
        "categoryid": "HLTHOUT",
        "measureid": "BPHIGH",
        "datavaluetypeid": "AgeAdjPrv",
        "short_question_text": "High Blood Pressure",
    },
]


def _assert_places_row_receipt(
    receipt: dict,
    *,
    expected_match_basis: str,
    expected_dataset_id: str = "fixture-county",
) -> None:
    validate_evidence_receipt(receipt, require_content=True)
    assert receipt["source_name"] == "CDC PLACES: Local Data for Better Health"
    assert receipt["dataset_id"] == expected_dataset_id
    assert receipt["match_basis"] == expected_match_basis
    assert "model-based community estimates" in receipt["caveat"]
    assert receipt["next_step"]


@pytest.fixture(autouse=True)
def patch_places_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_normalized_places_rows(geography_type: str, **filters):
        raw = data_loaders.filter_rows(
            RAW_ROWS,
            location_ids=filters.get("location_ids"),
            state=filters.get("state"),
            measure_ids=filters.get("measure_ids"),
            data_value_types=filters.get("data_value_types"),
            search=filters.get("search"),
            limit=filters.get("limit", 5000),
        )
        source = {**SOURCE, "geography_type": geography_type}
        rows = [data_loaders.normalize_places_record(row, geography_type=geography_type, source=source) for row in raw]
        return rows, source

    monkeypatch.setattr(server.data_loaders, "normalized_places_rows", fake_normalized_places_rows)


@pytest.mark.asyncio
async def test_list_places_measures_returns_source_metadata() -> None:
    result = await server.list_places_measures(geography_type="county", state="PA")

    assert result["ok"] is True
    assert result["count"] == 1
    assert result["results"][0]["measure_id"] == "BPHIGH"
    _assert_places_row_receipt(
        result["results"][0]["evidence"],
        expected_match_basis="cdc_places_measure_metadata_row",
    )
    assert result["results"][0]["evidence"]["query"]["row_measure_id"] == "BPHIGH"
    assert result["meta"]["source"]["dataset_id"] == "fixture-county"
    assert result["evidence"]["dataset_id"] == "fixture-county"
    assert result["evidence"]["match_basis"] == "cdc_places_measure_metadata_from_rows"
    assert result["source_metadata"]["dataset_id"] == "fixture-county"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_search_places_returns_locations() -> None:
    result = await server.search_places("Allegheny", geography_type="county", state="PA")

    assert result["ok"] is True
    assert result["results"][0]["location_id"] == "42003"
    assert result["results"][0]["location_name"] == "Allegheny"
    _assert_places_row_receipt(
        result["results"][0]["evidence"],
        expected_match_basis="cdc_places_location_search_result_row",
    )
    assert result["results"][0]["evidence"]["query"]["row_location_id"] == "42003"
    assert result["evidence"]["match_basis"] == "cdc_places_location_name_or_id_search"
    assert result["identity_map"]["entities"][0]["unresolved_identifiers"][0] == {
        "type": "places_county_location_id",
        "value": "42003",
    }
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_search_places_rejects_blank_query() -> None:
    result = await server.search_places(" ", geography_type="county")

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_get_places_profile_labels_community_estimates() -> None:
    result = await server.get_places_profile("42003", geography_type="county", measure_ids=["BPHIGH"])

    profile = result["profile"]
    assert profile["location"]["location_id"] == "42003"
    assert profile["measures"][0]["data_value"] == 33.4
    _assert_places_row_receipt(
        profile["location"]["evidence"],
        expected_match_basis="cdc_places_profile_location_id_exact_row",
    )
    _assert_places_row_receipt(
        profile["measures"][0]["evidence"],
        expected_match_basis="cdc_places_profile_measure_row_exact_location",
    )
    assert profile["measures"][0]["evidence"]["query"]["row_data_value"] == 33.4
    assert "not patient-level facts" in profile["interpretation"]
    assert result["evidence"]["match_basis"] == "cdc_places_location_id_exact"
    assert result["identity"]["canonical_name"] == "ALLEGHENY"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_places_profile_rejects_blank_location_id() -> None:
    result = await server.get_places_profile("", geography_type="county", measure_ids=["BPHIGH"])

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_compare_places_reports_missing_locations() -> None:
    result = await server.compare_places(["42003", "42101", "42091"], geography_type="county", measure_ids=["BPHIGH"])

    assert result["ok"] is True
    assert result["comparison"]["missing_locations"] == ["42091"]
    assert result["comparison"]["profiles"]["42101"]["location"]["location_name"] == "Philadelphia"
    _assert_places_row_receipt(
        result["comparison"]["profiles"]["42101"]["location"]["evidence"],
        expected_match_basis="cdc_places_comparison_location_id_exact_row",
    )
    _assert_places_row_receipt(
        result["comparison"]["profiles"]["42101"]["measures"][0]["evidence"],
        expected_match_basis="cdc_places_comparison_measure_row_exact_location",
    )
    assert result["evidence"]["match_basis"] == "cdc_places_location_id_exact_comparison"
    assert {entity["canonical_name"] for entity in result["identity_map"]["entities"]} == {"ALLEGHENY", "PHILADELPHIA"}
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_compare_places_rejects_blank_location_lists() -> None:
    result = await server.compare_places(["", "  "], geography_type="county", measure_ids=["BPHIGH"])

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_get_market_community_profile_aggregates_counties() -> None:
    result = await server.get_market_community_profile(county_fips=["42003", "42101"], measure_ids=["BPHIGH"])

    profile = result["market_profile"]
    measure = profile["aggregated_measures"][0]
    assert profile["geographic_basis"] == ["county"]
    assert measure["measure_id"] == "BPHIGH"
    assert measure["locations_reporting"] == 2
    assert measure["weighted_average"] == 34.291
    _assert_places_row_receipt(
        profile["locations"][0]["evidence"],
        expected_match_basis="cdc_places_market_location_exact_geography_id_row",
    )
    _assert_places_row_receipt(
        measure["evidence"],
        expected_match_basis="cdc_places_market_aggregated_measure_row",
        expected_dataset_id="cdc_places_market_community_profile",
    )
    assert measure["evidence"]["query"]["row_locations_reporting"] == 2
    assert "not patient-level facts" in profile["interpretation"]
    assert result["evidence"]["dataset_id"] == "cdc_places_market_community_profile"
    assert result["evidence"]["match_basis"] == "cdc_places_exact_geography_id_market_aggregation"
    assert {entity["unresolved_identifiers"][0]["value"] for entity in result["identity_map"]["entities"]} == {
        "42003",
        "42101",
    }
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_market_community_profile_rejects_blank_service_area() -> None:
    result = await server.get_market_community_profile(county_fips=[""], zctas=[" "], measure_ids=["BPHIGH"])

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
