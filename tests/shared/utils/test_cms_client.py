"""Tests for CMS catalog-driven URL discovery helpers."""

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
