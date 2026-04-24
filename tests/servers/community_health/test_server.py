from __future__ import annotations

import pytest

from servers.community_health import data_loaders, server


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
    assert result["meta"]["source"]["dataset_id"] == "fixture-county"


@pytest.mark.asyncio
async def test_search_places_returns_locations() -> None:
    result = await server.search_places("Allegheny", geography_type="county", state="PA")

    assert result["ok"] is True
    assert result["results"][0]["location_id"] == "42003"
    assert result["results"][0]["location_name"] == "Allegheny"


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
    assert "not patient-level facts" in profile["interpretation"]


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
    assert "not patient-level facts" in profile["interpretation"]


@pytest.mark.asyncio
async def test_get_market_community_profile_rejects_blank_service_area() -> None:
    result = await server.get_market_community_profile(county_fips=[""], zctas=[" "], measure_ids=["BPHIGH"])

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
