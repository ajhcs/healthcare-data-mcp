"""Tests for public-records regulatory provenance receipts."""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.public_records import server
from shared.utils.mcp_response import validate_evidence_receipt


@pytest.mark.asyncio
async def test_get_accreditation_returns_evidence_and_identity_map(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_ensure_pos_cached() -> bool:
        return True

    cache_path = tmp_path / "pos.parquet"
    cache_path.write_text("fixture", encoding="utf-8")
    monkeypatch.setattr(server.data_loaders, "_POS_PARQUET", cache_path)
    monkeypatch.setattr(server.data_loaders, "ensure_pos_cached", fake_ensure_pos_cached)
    monkeypatch.setattr(
        server.data_loaders,
        "query_pos",
        lambda **kwargs: [
            {
                "ccn": "390001",
                "provider_name": "Example Hospital",
                "state": "PA",
                "city": "Pittsburgh",
                "accreditation_type_code": "1",
                "accreditation_effective_date": "2025-01-01",
                "accreditation_expiration_date": "2027-01-01",
                "certification_date": "2020-01-01",
                "ownership_type": "Non-profit",
                "bed_count": 100,
                "medicare_medicaid": "Both",
                "compliance_status": "Active",
            }
        ],
    )

    response = await server.get_accreditation(ccn="390001")

    assert response["total_results"] == 1
    assert response["evidence"]["dataset_id"] == "cms_provider_of_services"
    assert response["evidence"]["match_basis"] == "ccn_exact"
    assert response["evidence"]["cache_status"] == "ready"
    assert response["source_metadata"]["dataset_id"] == response["evidence"]["dataset_id"]
    assert response["source_metadata"]["source_name"] == response["evidence"]["source_name"]
    assert response["source_metadata"]["source_url"] == response["evidence"]["source_url"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    provider = response["providers"][0]
    validate_evidence_receipt(provider["evidence"], require_content=True)
    assert provider["evidence"]["dataset_id"] == "cms_provider_of_services"
    assert provider["evidence"]["match_basis"] == "cms_provider_of_services_accreditation_row"
    assert provider["evidence"]["query"]["ccn"] == "390001"
    assert "CCN" in provider["evidence"]["caveat"]
    assert response["identity"]["ccn"] == "390001"
    by_field = {entry["field"]: entry for entry in response["identity_map"]["join_keys"]}
    assert by_field["ccn"]["values"] == ["390001"]
    assert by_field["canonical_name"]["values"] == ["EXAMPLE HOSPITAL"]
    assert response["identity_map"]["source_claims"][0]["dataset_id"] == "cms_provider_of_services"


@pytest.mark.asyncio
async def test_get_interop_status_returns_evidence_identity_map_and_cyber_caveat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_ensure_pi_cached() -> bool:
        return True

    cache_path = tmp_path / "pi.parquet"
    cache_path.write_text("fixture", encoding="utf-8")
    monkeypatch.setattr(server.data_loaders, "_PI_PARQUET", cache_path)
    monkeypatch.setattr(server.data_loaders, "ensure_pi_cached", fake_ensure_pi_cached)
    monkeypatch.setattr(
        server.data_loaders,
        "query_pi",
        lambda **kwargs: [
            {
                "facility_name": "Example Hospital",
                "ccn": "390001",
                "state": "PA",
                "city": "Pittsburgh",
                "meets_pi_criteria": "Y",
                "cehrt_id": "ABC123",
                "reporting_period_start": "2025-01-01",
                "reporting_period_end": "2025-12-31",
                "ehr_product_name": "Example EHR",
                "ehr_developer": "Example Vendor",
            }
        ],
    )

    response = await server.get_interop_status(ccn="390001")

    assert response["total_results"] == 1
    assert response["can_assert_cybersecurity_attestation"] is False
    assert response["evidence"]["dataset_id"] == "cms_promoting_interoperability_hospital"
    assert response["evidence"]["match_basis"] == "ccn_exact"
    assert "do not establish broad cybersecurity attestation" in response["evidence"]["caveat"]
    assert response["source_metadata"]["dataset_id"] == response["evidence"]["dataset_id"]
    assert response["source_metadata"]["source_name"] == response["evidence"]["source_name"]
    assert response["source_metadata"]["source_url"] == response["evidence"]["source_url"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    record = response["records"][0]
    validate_evidence_receipt(record["evidence"], require_content=True)
    assert record["evidence"]["dataset_id"] == "cms_promoting_interoperability_hospital"
    assert record["evidence"]["match_basis"] == "cms_promoting_interoperability_hospital_row"
    assert record["evidence"]["query"]["ccn"] == "390001"
    assert "do not establish broad cybersecurity attestation" in record["evidence"]["caveat"]
    assert response["identity"]["ccn"] == "390001"
    by_field = {entry["field"]: entry for entry in response["identity_map"]["join_keys"]}
    assert by_field["ccn"]["values"] == ["390001"]
    assert response["identity_map"]["source_claims"][0]["collection"] == "records"


@pytest.mark.asyncio
async def test_get_interop_status_no_match_keeps_source_scoped_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_ensure_pi_cached() -> bool:
        return True

    cache_path = tmp_path / "pi.parquet"
    cache_path.write_text("fixture", encoding="utf-8")
    monkeypatch.setattr(server.data_loaders, "_PI_PARQUET", cache_path)
    monkeypatch.setattr(server.data_loaders, "ensure_pi_cached", fake_ensure_pi_cached)
    monkeypatch.setattr(server.data_loaders, "query_pi", lambda **kwargs: [])

    response = await server.get_interop_status(ccn="390999")

    assert response["total_results"] == 0
    assert response["evidence"]["match_basis"] == "ccn_exact_no_match"
    assert response["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_promoting_interoperability_cache"
    assert response["source_metadata"]["dataset_id"] == response["evidence"]["dataset_id"]
    assert response["identity"]["ccn"] == "390999"
    assert response["identity_map"]["missing_data_policy"].startswith("No-match public-record regulatory responses")
