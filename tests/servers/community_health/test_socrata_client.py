from __future__ import annotations

import pytest

from servers.community_health.socrata_client import build_places_query, resolve_places_dataset, source_metadata


@pytest.fixture
def places_catalog_fixture() -> dict:
    return {
        "results": [
            {
                "resource": {
                    "id": "old1-aaaa",
                    "name": "PLACES: Local Data for Better Health, County Data, 2024 release",
                    "domain": "data.cdc.gov",
                    "updatedAt": "2025-01-01T00:00:00Z",
                    "row_count": "10",
                },
                "permalink": "https://data.cdc.gov/d/old1-aaaa",
            },
            {
                "resource": {
                    "id": "new1-bbbb",
                    "name": "PLACES: Local Data for Better Health, County Data, 2025 release",
                    "domain": "data.cdc.gov",
                    "updatedAt": "2026-01-01T00:00:00Z",
                    "row_count": "200",
                },
                "permalink": "https://data.cdc.gov/d/new1-bbbb",
            },
            {
                "resource": {
                    "id": "zcta-2025",
                    "name": "PLACES: Local Data for Better Health, ZCTA Data, 2025 release",
                    "domain": "data.cdc.gov",
                    "updatedAt": "2026-01-01T00:00:00Z",
                },
                "permalink": "https://data.cdc.gov/d/zcta-2025",
            },
        ]
    }


def test_resolve_places_dataset_uses_catalog_release_not_hardcoded_id(places_catalog_fixture: dict) -> None:
    manifest = resolve_places_dataset("county", catalog_data=places_catalog_fixture)

    assert manifest.dataset_id == "new1-bbbb"
    assert manifest.source_url == "https://data.cdc.gov/resource/new1-bbbb.json"
    assert manifest.record_count == 200
    assert source_metadata(manifest, geography_type="county")["interpretation"].startswith("PLACES values")


def test_build_places_query_filters_and_escapes_values() -> None:
    query = build_places_query(
        location_ids=["42003", "42091"],
        state="pa",
        measure_ids=["bphigh", "csmoking"],
        data_value_types=["Age-adjusted prevalence"],
        search="Allegheny",
        limit=100,
        offset=50,
    )

    assert query["$limit"] == 100
    assert query["$offset"] == 50
    assert "locationid in('42003', '42091')" in query["$where"]
    assert "upper(measureid) in('BPHIGH', 'CSMOKING')" in query["$where"]
    assert "upper(stateabbr) = 'PA'" in query["$where"]
    assert "Age-adjusted prevalence" in query["$where"]
    assert "upper(locationname) like '%ALLEGHENY%'" in query["$where"]


def test_build_places_query_bounds_invalid_limit_and_offset() -> None:
    query = build_places_query(limit="bad", offset="bad")  # type: ignore[arg-type]

    assert query["$limit"] == 5000
    assert query["$offset"] == 0


def test_resolve_places_dataset_rejects_unsupported_geography(places_catalog_fixture: dict) -> None:
    with pytest.raises(ValueError, match="Unsupported PLACES geography_type"):
        resolve_places_dataset("msa", catalog_data=places_catalog_fixture)
