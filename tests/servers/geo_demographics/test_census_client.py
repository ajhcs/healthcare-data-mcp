"""Tests for Census ACS client batching behavior."""

import pytest

from servers.geo_demographics import census_client


def test_chunked_splits_into_fixed_size_groups():
    values = [str(i).zfill(5) for i in range(23)]
    chunks = census_client._chunked(values, size=10)

    assert [len(chunk) for chunk in chunks] == [10, 10, 3]
    assert chunks[0][0] == "00000"
    assert chunks[-1][-1] == "00022"


def test_all_variables_include_public_alpha_race_ethnicity_fields():
    variables = census_client._all_variable_codes()

    assert "B02001_002E" in variables
    assert "B02001_003E" in variables
    assert "B03003_003E" in variables
    assert len(variables) > census_client.CENSUS_MAX_VARIABLES_PER_REQUEST


def test_parse_demographics_includes_race_ethnicity_and_density_inputs():
    row = {
        "B01003_001E": "1000",
        "B01002_001E": "40",
        "B01001_002E": "480",
        "B01001_026E": "520",
        "B19013_001E": "75000",
        "B27010_001E": "1000",
        "B02001_002E": "600",
        "B02001_003E": "200",
        "B02001_004E": "10",
        "B02001_005E": "100",
        "B02001_006E": "5",
        "B02001_007E": "20",
        "B02001_008E": "65",
        "B03003_002E": "850",
        "B03003_003E": "150",
        **{var: "1" for var in census_client.UNDER_18_VARS},
        **{var: "1" for var in census_client.OVER_65_VARS},
        **{var: "1" for var in census_client.INSURANCE_VARS.values()},
    }

    parsed = census_client.parse_demographics(
        row,
        "19107",
        2023,
        land_area_square_meters=census_client.SQUARE_METERS_PER_SQUARE_MILE,
    )

    assert parsed["race_ethnicity"]["white_alone"] == 600
    assert parsed["race_ethnicity"]["hispanic_latino"] == 150
    assert parsed["land_area"]["land_area_square_miles"] == 1.0
    assert parsed["population_density"]["people_per_square_mile"] == 1000.0
    assert parsed["population_density"]["population_input"] == 1000


def test_parse_gazetteer_text_returns_land_area_by_zcta():
    text = "GEOID\tALAND\tINTPTLAT\tINTPTLONG\n19107\t2589988.110336\t0\t0\n"

    assert census_client._parse_gazetteer_text(text) == {"19107": 2589988.110336}


@pytest.mark.asyncio
async def test_query_acs_merged_combines_variable_chunks_by_zcta(monkeypatch):
    async def fake_query_acs(variables, zcta="*", year=2023, api_key=None):
        assert len(variables) <= census_client.CENSUS_MAX_VARIABLES_PER_REQUEST
        return [
            {
                "NAME": "ZCTA5 19107",
                "zip code tabulation area": "19107",
                **{variable: f"value-{variable}" for variable in variables},
            }
        ]

    monkeypatch.setattr(census_client, "query_acs", fake_query_acs)

    rows = await census_client.query_acs_merged(
        [f"VAR_{index}" for index in range(census_client.CENSUS_MAX_VARIABLES_PER_REQUEST + 2)],
        zcta="19107",
        year=2023,
    )

    assert len(rows) == 1
    assert rows[0]["VAR_0"] == "value-VAR_0"
    assert rows[0][f"VAR_{census_client.CENSUS_MAX_VARIABLES_PER_REQUEST + 1}"] == (
        f"value-VAR_{census_client.CENSUS_MAX_VARIABLES_PER_REQUEST + 1}"
    )


@pytest.mark.asyncio
async def test_get_demographics_batch_chunks_large_requests(monkeypatch):
    calls: list[tuple[str, tuple[str, ...]]] = []

    async def fake_query_acs(variables, zcta="*", year=2023, api_key=None):
        assert len(variables) <= census_client.CENSUS_MAX_VARIABLES_PER_REQUEST
        calls.append((zcta, tuple(variables)))
        requested = zcta.split(",")
        return [
            {
                "zip code tabulation area": code,
                "B01003_001E": "1000",
                "B01002_001E": "40",
                "B01001_002E": "480",
                "B01001_026E": "520",
                "B19013_001E": "75000",
                "B27010_001E": "1000",
                "B02001_002E": "600",
                "B02001_003E": "200",
                "B02001_004E": "10",
                "B02001_005E": "100",
                "B02001_006E": "5",
                "B02001_007E": "20",
                "B02001_008E": "65",
                "B03003_002E": "850",
                "B03003_003E": "150",
                **{var: "1" for var in census_client.UNDER_18_VARS},
                **{var: "1" for var in census_client.OVER_65_VARS},
                **{var: "1" for var in census_client.INSURANCE_VARS.values()},
            }
            for code in requested
        ]

    async def fake_land_areas(zctas):
        return {"00000": 100.0}

    monkeypatch.setattr(census_client, "query_acs", fake_query_acs)
    monkeypatch.setattr(census_client, "get_zcta_land_areas", fake_land_areas)

    zctas = [str(i).zfill(5) for i in range(25)]
    results = await census_client.get_demographics_batch(zctas, year=2023)

    zcta_calls = [zcta for zcta, _variables in calls]
    assert len(calls) == 6
    assert zcta_calls == [
        "00000,00001,00002,00003,00004,00005,00006,00007,00008,00009",
        "00000,00001,00002,00003,00004,00005,00006,00007,00008,00009",
        "00010,00011,00012,00013,00014,00015,00016,00017,00018,00019",
        "00010,00011,00012,00013,00014,00015,00016,00017,00018,00019",
        "00020,00021,00022,00023,00024",
        "00020,00021,00022,00023,00024",
    ]
    assert len(results) == 25
    assert results[0]["zcta"] == "00000"
    assert results[-1]["zcta"] == "00024"
    assert "*" not in zcta_calls


@pytest.mark.asyncio
async def test_get_demographics_batch_dedupes_and_preserves_input_order(monkeypatch):
    async def fake_query_acs(variables, zcta="*", year=2023, api_key=None):
        return [
            {
                "zip code tabulation area": code,
                "B01003_001E": "1000",
                "B01002_001E": "40",
                "B01001_002E": "480",
                "B01001_026E": "520",
                "B19013_001E": "75000",
                "B27010_001E": "1000",
                "B02001_002E": "600",
                "B02001_003E": "200",
                "B02001_004E": "10",
                "B02001_005E": "100",
                "B02001_006E": "5",
                "B02001_007E": "20",
                "B02001_008E": "65",
                "B03003_002E": "850",
                "B03003_003E": "150",
                **{var: "1" for var in census_client.UNDER_18_VARS},
                **{var: "1" for var in census_client.OVER_65_VARS},
                **{var: "1" for var in census_client.INSURANCE_VARS.values()},
            }
            for code in zcta.split(",")
        ]

    async def fake_land_areas(zctas):
        return {}

    monkeypatch.setattr(census_client, "query_acs", fake_query_acs)
    monkeypatch.setattr(census_client, "get_zcta_land_areas", fake_land_areas)

    results = await census_client.get_demographics_batch(
        ["19107", "90210", "19107", "10001"],
        year=2023,
    )

    assert [row["zcta"] for row in results] == ["19107", "90210", "10001"]


@pytest.mark.asyncio
async def test_get_demographics_batch_returns_no_data_rows_for_missing_zctas(monkeypatch):
    async def fake_query_acs(variables, zcta="*", year=2023, api_key=None):
        return [
            {
                "zip code tabulation area": "19107",
                "B01003_001E": "1000",
                "B01002_001E": "40",
                "B01001_002E": "480",
                "B01001_026E": "520",
                "B19013_001E": "75000",
                "B27010_001E": "1000",
                **{var: "1" for var in census_client.UNDER_18_VARS},
                **{var: "1" for var in census_client.OVER_65_VARS},
                **{var: "1" for var in census_client.INSURANCE_VARS.values()},
            }
        ]

    async def fake_land_areas(zctas):
        return {}

    monkeypatch.setattr(census_client, "query_acs", fake_query_acs)
    monkeypatch.setattr(census_client, "get_zcta_land_areas", fake_land_areas)

    results = await census_client.get_demographics_batch(["19107", "00000"], year=2023)

    assert [row["zcta"] for row in results] == ["19107", "00000"]
    assert results[1]["status"] == "no_data"
    assert results[1]["missingness_state"] == "unavailable_public"
