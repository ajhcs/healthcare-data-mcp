"""Tests for Census ACS client batching behavior."""

import pytest

from servers.geo_demographics import census_client


def test_chunked_splits_into_fixed_size_groups():
    values = [str(i).zfill(5) for i in range(23)]
    chunks = census_client._chunked(values, size=10)

    assert [len(chunk) for chunk in chunks] == [10, 10, 3]
    assert chunks[0][0] == "00000"
    assert chunks[-1][-1] == "00022"


@pytest.mark.asyncio
async def test_get_demographics_batch_chunks_large_requests(monkeypatch):
    calls: list[str] = []

    async def fake_query_acs(variables, zcta="*", year=2023, api_key=None):
        calls.append(zcta)
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
                **{var: "1" for var in census_client.UNDER_18_VARS},
                **{var: "1" for var in census_client.OVER_65_VARS},
                **{var: "1" for var in census_client.INSURANCE_VARS.values()},
            }
            for code in requested
        ]

    monkeypatch.setattr(census_client, "query_acs", fake_query_acs)

    zctas = [str(i).zfill(5) for i in range(25)]
    results = await census_client.get_demographics_batch(zctas, year=2023)

    assert len(calls) == 3
    assert calls == [
        "00000,00001,00002,00003,00004,00005,00006,00007,00008,00009",
        "00010,00011,00012,00013,00014,00015,00016,00017,00018,00019",
        "00020,00021,00022,00023,00024",
    ]
    assert len(results) == 25
    assert results[0]["zcta"] == "00000"
    assert results[-1]["zcta"] == "00024"
    assert "*" not in calls


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
                **{var: "1" for var in census_client.UNDER_18_VARS},
                **{var: "1" for var in census_client.OVER_65_VARS},
                **{var: "1" for var in census_client.INSURANCE_VARS.values()},
            }
            for code in zcta.split(",")
        ]

    monkeypatch.setattr(census_client, "query_acs", fake_query_acs)

    results = await census_client.get_demographics_batch(
        ["19107", "90210", "19107", "10001"],
        year=2023,
    )

    assert [row["zcta"] for row in results] == ["19107", "90210", "10001"]
