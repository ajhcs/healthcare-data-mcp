from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from shared.utils.source_catalog import SourceManifest

from servers.provider_enrollment import data_loaders


def test_resolve_dataset_manifest_uses_shared_cms_catalog_fixture() -> None:
    catalog = {
        "dataset": [
            {
                "title": "Hospital Enrollments",
                "identifier": "hospital-enrollment-id",
                "modified": "2026-04-20",
                "landingPage": "https://data.cms.gov/provider-enrollment/hospital-enrollments",
                "distribution": [{"downloadURL": "https://example.test/hospital.csv", "format": "CSV"}],
            }
        ]
    }

    manifest = data_loaders.resolve_dataset_manifest("hospital_enrollments", catalog_data=catalog)

    assert manifest.dataset_id == "hospital-enrollment-id"
    assert manifest.source_url == "https://example.test/hospital.csv"
    assert manifest.extra["dataset_key"] == "hospital_enrollments"


def test_cache_csv_preserves_original_columns_and_adds_normalized_aliases(tmp_path) -> None:
    csv_path = tmp_path / "ffs.csv"
    csv_path.write_text(
        "NPI,PAC ID,Enrollment ID,State,Provider Name,Provider Type\n"
        "1234567893,PAC-77,ENR-1,Pennsylvania,Jefferson Medical Group,Hospitalist\n",
        encoding="utf-8",
    )
    manifest = SourceManifest(
        source_url="https://example.test/ffs.csv",
        landing_page="https://data.cms.gov/provider-enrollment/ffs",
        dataset_id="ffs",
        title="Medicare Fee-For-Service Public Provider Enrollment",
        modified="2026-04-20",
    )

    written = data_loaders.cache_csv(
        "medicare_ffs_public_provider_enrollment",
        csv_path,
        manifest,
        cache_dir=tmp_path,
    )
    frame = data_loaders.load_cached_frame("medicare_ffs_public_provider_enrollment", cache_dir=tmp_path)

    assert written.record_count == 1
    assert (tmp_path / "medicare_ffs_public_provider_enrollment.parquet").exists()
    assert (tmp_path / "medicare_ffs_public_provider_enrollment.meta.json").exists()
    assert "PAC ID" in frame.columns
    assert "pac_id" in frame.columns
    assert frame.iloc[0]["NPI"] == "1234567893"
    assert frame.iloc[0]["npi"] == "1234567893"
    assert frame.iloc[0]["enrollment_id"] == "ENR1"
    assert frame.iloc[0]["state"] == "PA"


def test_cache_records_supports_api_style_rows_and_ownership_queries(tmp_path) -> None:
    data_loaders.cache_records(
        "hospital_all_owners",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Owner Organization Name": "Jefferson Parent LLC",
                "Owner Associate ID": "OWN-1",
                "Association Role Text": "5 percent or greater direct owner",
                "Ownership Percentage": "100",
            },
            {
                "Enrollment ID": "ENR-2",
                "CCN": "390002",
                "Facility Name": "Other Hospital",
                "State": "NJ",
                "Owner Organization Name": "Other Parent LLC",
                "Owner Associate ID": "OWN-2",
            },
        ],
        cache_dir=tmp_path,
    )

    rows = data_loaders.query_ownership(ccn="390001", cache_dir=tmp_path)

    assert len(rows) == 1
    assert rows[0]["ccn"] == "390001"
    assert rows[0]["owner_name"] == "Jefferson Parent LLC"
    assert rows[0]["owner_associate_id"] == "OWN1"


def test_chow_query_uses_same_normalization_path_for_snf(tmp_path) -> None:
    data_loaders.cache_records(
        "snf_chow",
        [
            {
                "Enrollment ID": "SNF-1",
                "Provider Number": "395555",
                "Facility Name": "Jefferson SNF",
                "State": "PA",
                "Transaction Date": "2026-04-01",
                "Change Type": "Change of ownership",
            }
        ],
        cache_dir=tmp_path,
    )

    rows = data_loaders.query_chow(ccn="395555", provider_category="snf", cache_dir=tmp_path)

    assert len(rows) == 1
    assert rows[0]["provider_category"] == "snf"
    assert rows[0]["transaction_date"] == "2026-04-01"


def test_search_enrollments_filters_npi_state_and_name(tmp_path) -> None:
    data_loaders.cache_records(
        "medicare_ffs_public_provider_enrollment",
        [
            {
                "NPI": "1234567893",
                "Enrollment ID": "ENR-1",
                "State": "PA",
                "Provider Name": "Jefferson Medical Group",
                "Provider Type": "Group Practice",
            },
            {
                "NPI": "1111111111",
                "Enrollment ID": "ENR-2",
                "State": "NJ",
                "Provider Name": "Unrelated Group",
                "Provider Type": "Group Practice",
            },
        ],
        cache_dir=tmp_path,
    )

    rows = data_loaders.search_enrollments(
        npi="1234567893",
        provider_name="Jefferson",
        state="Pennsylvania",
        cache_dir=tmp_path,
    )

    assert len(rows) == 1
    assert rows[0]["provider_name"] == "Jefferson Medical Group"


@pytest.mark.asyncio
async def test_ensure_all_datasets_cached_writes_nine_parquets_and_manifests(tmp_path, monkeypatch) -> None:
    catalog = {
        "dataset": [
            {
                "title": dataset.title,
                "identifier": f"{dataset.key}-id",
                "modified": "2026-04-20",
                "landingPage": f"https://data.cms.gov/provider-enrollment/{dataset.landing_page_slug}",
                "distribution": [
                    {
                        "downloadURL": f"https://example.test/{dataset.key}.csv",
                        "format": "CSV",
                    }
                ],
            }
            for dataset in data_loaders.DATASETS.values()
        ]
    }

    async def fake_request(method: str, url: str, **_kwargs):
        assert method == "GET"
        key = url.rsplit("/", 1)[-1].removesuffix(".csv")
        return SimpleNamespace(
            content=(
                "NPI,Enrollment ID,CCN,Facility Name,State,Owner Organization Name,Transaction Date\n"
                f"1234567893,{key}-ENR,390001,Jefferson Hospital,PA,{key} Owner,2026-04-01\n"
            ).encode(),
            headers={"etag": f'"{key}-etag"', "last-modified": "Mon, 20 Apr 2026 00:00:00 GMT"},
        )

    monkeypatch.setattr(data_loaders, "resilient_request", fake_request)

    manifests = await data_loaders.ensure_all_datasets_cached(cache_dir=tmp_path, catalog_data=catalog)

    assert len(manifests) == 9
    for dataset_key in data_loaders.DATASETS:
        parquet_path = tmp_path / f"{dataset_key}.parquet"
        meta_path = tmp_path / f"{dataset_key}.meta.json"
        assert parquet_path.exists()
        assert meta_path.exists()
        metadata = json.loads(meta_path.read_text(encoding="utf-8"))
        assert metadata["source_url"] == f"https://example.test/{dataset_key}.csv"
        assert metadata["modified"] == "2026-04-20"
        assert metadata["fetched_at"]
        assert metadata["record_count"] > 0
        assert metadata["checksum"]
        assert metadata["etag"] == f'"{dataset_key}-etag"'
