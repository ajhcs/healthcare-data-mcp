from __future__ import annotations

import json

import pytest

from shared.utils.source_catalog import (
    PublicSourceCatalog,
    SourceManifest,
    read_source_manifest,
    resolve_cms_dataset,
    resolve_socrata_dataset,
    write_source_manifest,
)


def test_resolve_cms_dataset_prefers_matching_landing_page_slug() -> None:
    catalog = {
        "dataset": [
            {
                "title": "Hospital All Owners",
                "identifier": "old-owner-id",
                "modified": "2024-10-01",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-all-owners-archive",
                "distribution": [{"downloadURL": "https://example.test/old.xlsx", "format": "XLSX"}],
            },
            {
                "title": "Hospital All Owners",
                "identifier": "new-owner-id",
                "modified": "2026-04-20",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-all-owners",
                "distribution": [
                    {"downloadURL": "https://example.test/owners.csv", "mediaType": "text/csv", "checksum": "abc123"}
                ],
            },
        ]
    }

    manifest = resolve_cms_dataset(
        "hospital all owners",
        landing_page_slug="hospital-all-owners",
        catalog_data=catalog,
    )

    assert manifest.dataset_id == "new-owner-id"
    assert manifest.source_url == "https://example.test/owners.csv"
    assert manifest.landing_page.endswith("/hospital-all-owners")
    assert manifest.modified == "2026-04-20"
    assert manifest.checksum == "abc123"


def test_resolve_cms_dataset_does_not_treat_archive_suffix_as_slug_match() -> None:
    catalog = {
        "dataset": [
            {
                "title": "Hospital All Owners",
                "identifier": "archive",
                "modified": "2026-05-01",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-all-owners-archive",
                "distribution": [{"downloadURL": "https://example.test/archive.csv", "format": "CSV"}],
            },
            {
                "title": "Hospital All Owners",
                "identifier": "current",
                "modified": "2026-04-20",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-all-owners",
                "distribution": [{"downloadURL": "https://example.test/current.csv", "format": "CSV"}],
            },
        ]
    }

    manifest = resolve_cms_dataset(
        "Hospital All Owners",
        landing_page_slug="hospital-all-owners",
        catalog_data=catalog,
    )

    assert manifest.dataset_id == "current"
    assert manifest.source_url == "https://example.test/current.csv"


def test_resolve_cms_dataset_uses_modified_as_deterministic_tiebreaker() -> None:
    catalog = {
        "dataset": [
            {
                "title": "Hospital Enrollments",
                "identifier": "older",
                "modified": "2025-01-01",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-enrollments",
                "distribution": [{"downloadURL": "https://example.test/older.csv", "format": "CSV"}],
            },
            {
                "title": "Hospital Enrollments",
                "identifier": "newer",
                "modified": "2026-04-20",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-enrollments",
                "distribution": [{"downloadURL": "https://example.test/newer.csv", "format": "CSV"}],
            },
        ]
    }

    manifest = resolve_cms_dataset("Hospital Enrollments", catalog_data=catalog)

    assert manifest.dataset_id == "newer"
    assert manifest.source_url == "https://example.test/newer.csv"


def test_resolve_socrata_dataset_by_title_release_and_domain() -> None:
    catalog = {
        "results": [
            {
                "resource": {
                    "id": "eav7-hnsx",
                    "name": "PLACES: Local Data for Better Health, Place Data, 2025 release",
                    "domain": "data.cdc.gov",
                    "updatedAt": "2025-08-01T00:00:00Z",
                    "row_count": "222",
                },
                "permalink": "https://data.cdc.gov/d/eav7-hnsx",
            },
            {
                "resource": {
                    "id": "swc5-untb",
                    "name": "PLACES: Local Data for Better Health, County Data, 2025 release",
                    "domain": "data.cdc.gov",
                    "updatedAt": "2025-08-15T00:00:00Z",
                    "row_count": 1000,
                },
                "permalink": "https://data.cdc.gov/d/swc5-untb",
            },
        ]
    }

    manifest = resolve_socrata_dataset(
        "PLACES: Local Data for Better Health, County Data",
        release="2025 release",
        domain="data.cdc.gov",
        catalog_data=catalog,
    )

    assert manifest.dataset_id == "swc5-untb"
    assert manifest.source_url == "https://data.cdc.gov/resource/swc5-untb.json"
    assert manifest.landing_page == "https://data.cdc.gov/d/swc5-untb"
    assert manifest.record_count == 1000
    assert manifest.extra["domain"] == "data.cdc.gov"


def test_source_manifest_json_roundtrip(tmp_path) -> None:
    path = tmp_path / "dataset.meta.json"
    manifest = SourceManifest(
        source_url="https://example.test/data.csv",
        landing_page="https://example.test/landing",
        dataset_id="abc",
        title="Example",
        modified="2026-04-20",
        fetched_at="2026-04-23T00:00:00Z",
        etag="etag",
        last_modified="Wed, 22 Apr 2026 00:00:00 GMT",
        record_count=12,
        checksum="sha256:abc",
    )

    write_source_manifest(manifest, path)
    loaded = read_source_manifest(path)

    assert loaded == manifest
    assert json.loads(path.read_text(encoding="utf-8"))["source_url"] == "https://example.test/data.csv"


def test_public_source_catalog_resolves_cms_and_socrata_from_loaded_catalogs() -> None:
    catalog = PublicSourceCatalog(
        cms_catalog={
            "dataset": [
                {
                    "title": "Hospital Enrollments",
                    "identifier": "cms-current",
                    "modified": "2026-04-20",
                    "landingPage": "https://data.cms.gov/provider-enrollment/hospital-enrollments",
                    "distribution": [{"downloadURL": "https://example.test/enrollments.csv", "format": "CSV"}],
                }
            ]
        },
        socrata_catalog={
            "results": [
                {
                    "resource": {
                        "id": "abcd-1234",
                        "name": "PLACES County Data 2025 release",
                        "domain": "data.cdc.gov",
                    },
                    "permalink": "https://data.cdc.gov/d/abcd-1234",
                }
            ]
        },
    )

    cms = catalog.resolve_cms_dataset("Hospital Enrollments")
    socrata = catalog.resolve_socrata_dataset("PLACES County Data", release="2025 release")

    assert cms.dataset_id == "cms-current"
    assert socrata.source_url == "https://data.cdc.gov/resource/abcd-1234.json"


def test_resolvers_raise_for_missing_fixture_matches() -> None:
    with pytest.raises(LookupError):
        resolve_cms_dataset("Missing", catalog_data={"dataset": []})

    with pytest.raises(LookupError):
        resolve_socrata_dataset("Missing", catalog_data={"results": []})
