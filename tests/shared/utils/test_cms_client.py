"""Tests for CMS catalog-driven URL discovery helpers."""

import json
import os
import time
from types import SimpleNamespace

import pytest

from shared.utils import cms_client


@pytest.fixture
def sample_catalog():
    return {
        "dataset": [
            {
                "title": "Hospital Service Area",
                "landingPage": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/hospital-service-area",
                "distribution": [
                    {
                        "title": "Hospital Service Area : 2024-01-01",
                        "mediaType": "text/csv",
                        "downloadURL": "https://example.test/hsaf-2024.csv",
                    },
                    {
                        "title": "Hospital Service Area : 2023-01-01",
                        "mediaType": "text/csv",
                        "downloadURL": "https://example.test/hsaf-2023.csv",
                    },
                ],
            },
            {
                "title": "Medicare Inpatient Hospitals - by Provider and Service",
                "landingPage": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals/medicare-inpatient-hospitals-by-provider-and-service",
                "distribution": [
                    {
                        "title": "Medicare Inpatient Hospitals - by Provider and Service : 2023-12-31",
                        "mediaType": "text/csv",
                        "downloadURL": "https://example.test/inpatient-2023.csv",
                    },
                    {
                        "title": "Medicare Inpatient Hospitals - by Provider and Service : 2022-12-01",
                        "mediaType": "text/csv",
                        "downloadURL": "https://example.test/inpatient-2022.csv",
                    },
                ],
            },
        ]
    }


def test_find_cms_dataset_exact_title(sample_catalog):
    dataset = cms_client.find_cms_dataset(sample_catalog, title="Hospital Service Area")
    assert dataset is not None
    assert dataset["title"] == "Hospital Service Area"


def test_select_cms_distribution_by_release_year(sample_catalog):
    dataset = cms_client.find_cms_dataset(
        sample_catalog,
        title="Medicare Inpatient Hospitals - by Provider and Service",
    )
    distribution = cms_client.select_cms_distribution(
        dataset,
        media_type="text/csv",
        release_year="2022",
    )
    assert distribution is not None
    assert distribution["downloadURL"] == "https://example.test/inpatient-2022.csv"


@pytest.mark.asyncio
async def test_cms_discover_download_url_uses_contains_match(monkeypatch, sample_catalog):
    async def fake_catalog():
        return sample_catalog

    monkeypatch.setattr(cms_client, "get_cms_data_catalog", fake_catalog)

    url = await cms_client.cms_discover_download_url(
        title_contains="Inpatient Hospitals - by Provider and Service",
        landing_page_contains="/medicare-inpatient-hospitals/",
    )

    assert url == "https://example.test/inpatient-2023.csv"


@pytest.mark.asyncio
async def test_cms_discover_download_url_falls_back_when_missing(monkeypatch):
    async def fake_catalog():
        return {"dataset": []}

    monkeypatch.setattr(cms_client, "get_cms_data_catalog", fake_catalog)

    url = await cms_client.cms_discover_download_url(
        title="Missing Dataset",
        fallback_url="https://fallback.test/file.csv",
    )

    assert url == "https://fallback.test/file.csv"


@pytest.mark.asyncio
async def test_cms_download_csv_refreshes_stale_cache_and_writes_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(cms_client, "DATA_DIR", tmp_path)
    calls = 0

    async def fake_request(method: str, url: str, **kwargs):  # noqa: ARG001
        nonlocal calls
        calls += 1
        return SimpleNamespace(content=f"col\n{calls}\n".encode())

    monkeypatch.setattr(cms_client, "resilient_request", fake_request)

    path = await cms_client.cms_download_csv("https://example.test/source.csv", cache_key="friendly_cache", ttl_days=1)
    assert path == tmp_path / "8d7a777cdf3f9048.csv"
    assert path.read_text(encoding="utf-8") == "col\n1\n"
    metadata = json.loads((tmp_path / "8d7a777cdf3f9048.csv.meta.json").read_text(encoding="utf-8"))
    assert metadata["source_url"] == "https://example.test/source.csv"
    assert metadata["cache_key"] == "friendly_cache"

    fresh = await cms_client.cms_download_csv("https://example.test/source.csv", cache_key="friendly_cache", ttl_days=1)
    assert fresh == path
    assert calls == 1

    stale_mtime = time.time() - (2 * 86_400)
    os.utime(path, (stale_mtime, stale_mtime))
    refreshed = await cms_client.cms_download_csv("https://example.test/source.csv", cache_key="friendly_cache", ttl_days=1)

    assert refreshed == path
    assert calls == 2
    assert path.read_text(encoding="utf-8") == "col\n2\n"


@pytest.mark.asyncio
async def test_cms_download_csv_uses_stale_cache_when_refresh_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(cms_client, "DATA_DIR", tmp_path)
    path = cms_client.get_cache_path("friendly_cache", suffix=".csv")
    path.write_text("col\nstale\n", encoding="utf-8")
    stale_mtime = time.time() - (2 * 86_400)
    os.utime(path, (stale_mtime, stale_mtime))

    async def fake_request(method: str, url: str, **kwargs):  # noqa: ARG001
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(cms_client, "resilient_request", fake_request)

    result = await cms_client.cms_download_csv("https://example.test/source.csv", cache_key="friendly_cache", ttl_days=1)

    assert result == path
    assert result.read_text(encoding="utf-8") == "col\nstale\n"


@pytest.mark.asyncio
async def test_hospital_general_info_uses_stale_cache_when_refresh_fails(tmp_path, monkeypatch):
    cache_path = tmp_path / "hospital_general_info.csv"
    cache_path.write_text("Facility ID,Facility Name\n390001,Example Hospital\n", encoding="utf-8")
    stale_mtime = time.time() - (91 * 86_400)
    os.utime(cache_path, (stale_mtime, stale_mtime))

    monkeypatch.setattr(cms_client, "_HOSPITAL_INFO_CACHE_PATH", cache_path)
    monkeypatch.setattr(cms_client, "_hospital_info_raw", None)
    monkeypatch.setattr(cms_client, "_hospital_info_normalized", None)

    async def fake_request(method: str, url: str, **kwargs):  # noqa: ARG001
        raise RuntimeError("network unavailable")

    monkeypatch.setattr(cms_client, "resilient_request", fake_request)

    df = await cms_client.load_hospital_general_info(normalize_columns=False)

    assert df.to_dict(orient="records") == [{"Facility ID": "390001", "Facility Name": "Example Hospital"}]
