"""Tests for PHC4 public-report provenance wrappers in public-records."""

from __future__ import annotations

import pytest

from shared import state_health_data
from shared.utils.mcp_response import validate_evidence_receipt
from servers.public_records import server


def _assert_phc4_identity_map(identity_map: dict, *, expected_name: str = "", expected_procedure: str = "") -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "phc4_public_report"
    assert identity_map["source_claims"][0]["collection"] == "phc4_public_reports"
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No PHC4 report or table-row match")
    assert state_health_data.PHC4_REPORT_LIBRARY_URL in by_field["source_url"]["values"]
    if expected_name:
        assert expected_name in by_field["canonical_name"]["values"]
    if expected_procedure:
        assert expected_procedure in by_field["procedure"]["values"]


@pytest.mark.asyncio
async def test_search_phc4_public_reports_adds_evidence_and_identity_map(monkeypatch) -> None:
    async def fake_search(query: str, year: str = "", report_type: str = ""):
        return {
            "query": query,
            "year": year,
            "report_type": report_type,
            "total_results": 1,
            "reports": [
                {
                    "title": "Hospital Performance Report 2024",
                    "url": "https://www.phc4.org/example.pdf",
                    "year": 2024,
                    "report_type": "hospital_performance",
                }
            ],
        }

    monkeypatch.setattr(server.state_health_data, "search_phc4_reports", fake_search)

    response = await server.search_phc4_public_reports("Example Hospital", year="2024", report_type="hospital_performance")

    assert response["total_results"] == 1
    assert response["source_metadata"]["dataset_id"] == "phc4_public_reports"
    assert response["source_metadata"]["record_count"] == 1
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["evidence"]["source_name"] == "PHC4 Public Reports Library"
    assert response["evidence"]["source_url"] == "https://www.phc4.org/example.pdf"
    assert response["evidence"]["dataset_id"] == "phc4_public_reports"
    assert response["evidence"]["source_period"] == "2024"
    assert response["evidence"]["cache_status"] == "public_report_index"
    assert response["evidence"]["match_basis"] == "phc4_public_report_index_search"
    assert response["evidence"]["confidence"] == "public_report_index_match"
    assert "not paid PHC4 discharge datasets" in response["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    _assert_phc4_identity_map(response["identity_map"], expected_name="EXAMPLE HOSPITAL")


@pytest.mark.asyncio
async def test_search_phc4_public_reports_zero_results_are_scoped_no_match(monkeypatch) -> None:
    async def fake_search(query: str, year: str = "", report_type: str = ""):
        return {
            "query": query,
            "year": year,
            "report_type": report_type,
            "total_results": 0,
            "reports": [],
        }

    monkeypatch.setattr(server.state_health_data, "search_phc4_reports", fake_search)

    response = await server.search_phc4_public_reports("No Such Hospital")

    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["evidence"]["match_basis"] == "phc4_public_report_index_search_no_match"
    assert response["evidence"]["confidence"] == "no_indexed_phc4_public_report_match"
    assert response["evidence"]["source_period"] == "latest indexed PHC4 public reports"
    assert "not proof" in response["identity_map"]["missing_data_policy"]


@pytest.mark.asyncio
async def test_phc4_hospital_performance_profile_adds_report_evidence(monkeypatch) -> None:
    async def fake_profile(*, hospital_name: str = "", year: int = 0, report_type: str = ""):
        return {
            "hospital_name": hospital_name,
            "year": year,
            "report_type": report_type,
            "source_status": "public_report_index",
            "confidence": "public_report_profile_match",
            "note": "Public PHC4 reports are indexed and cached; paid PHC4 discharge files are not used.",
            "reports": [
                {
                    "title": "Hospital Performance Report 2024",
                    "url": "https://www.phc4.org/hospital-performance.pdf",
                    "year": year,
                    "report_type": report_type,
                }
            ],
        }

    monkeypatch.setattr(server.state_health_data, "phc4_report_profile", fake_profile)

    response = await server.get_phc4_hospital_performance(hospital_name="Example Hospital", year=2024)

    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["evidence"]["match_basis"] == "phc4_public_hospital_performance_profile"
    assert response["evidence"]["confidence"] == "public_report_profile_match"
    assert response["source_metadata"]["dataset_id"] == "phc4_public_reports"
    assert response["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    _assert_phc4_identity_map(response["identity_map"], expected_name="EXAMPLE HOSPITAL")


@pytest.mark.asyncio
async def test_phc4_financial_analysis_profile_adds_report_evidence(monkeypatch) -> None:
    async def fake_profile(*, hospital_name: str = "", fiscal_year: int = 0, report_type: str = ""):
        return {
            "hospital_name": hospital_name,
            "fiscal_year": fiscal_year,
            "report_type": report_type,
            "source_status": "public_report_index",
            "confidence": "public_report_profile_match",
            "note": "Public PHC4 reports are indexed and cached; paid PHC4 discharge files are not used.",
            "reports": [
                {
                    "title": "Financial Analysis 2024",
                    "url": "https://www.phc4.org/financial-analysis.pdf",
                    "year": fiscal_year,
                    "report_type": report_type,
                }
            ],
        }

    monkeypatch.setattr(server.state_health_data, "phc4_report_profile", fake_profile)

    response = await server.get_phc4_financial_analysis(hospital_name="Example Hospital", fiscal_year=2024)

    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["evidence"]["match_basis"] == "phc4_public_financial_analysis_profile"
    assert response["evidence"]["confidence"] == "public_report_profile_match"
    assert response["source_metadata"]["dataset_id"] == "phc4_public_reports"
    assert response["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    _assert_phc4_identity_map(response["identity_map"], expected_name="EXAMPLE HOSPITAL")


@pytest.mark.asyncio
async def test_phc4_common_procedure_profile_adds_report_identity_boundaries(monkeypatch) -> None:
    async def fake_profile(*, hospital_name: str = "", procedure: str = "", year: int = 0, report_type: str = ""):
        return {
            "hospital_name": hospital_name,
            "procedure": procedure,
            "year": year,
            "report_type": report_type,
            "source_status": "public_report_index",
            "confidence": "high_extracted_table_row",
            "note": "Public PHC4 reports are indexed and cached; paid PHC4 discharge files are not used.",
            "table_rows": [
                {
                    "hospital_name": hospital_name,
                    "procedure": procedure,
                    "measure_name": "volume",
                    "measure_value": "42",
                    "report_year": year,
                    "source_artifact": "https://www.phc4.org/common-procedures.csv",
                }
            ],
            "reports": [
                {
                    "title": "Common Procedures Fiscal Year 2024",
                    "url": "https://www.phc4.org/common-procedures.csv",
                    "year": year,
                    "report_type": report_type,
                }
            ],
        }

    monkeypatch.setattr(server.state_health_data, "phc4_report_profile", fake_profile)

    response = await server.get_phc4_common_procedure_profile(
        hospital_name="Example Hospital",
        procedure="Knee Replacement",
        year=2024,
    )

    assert response["table_rows"][0]["measure_value"] == "42"
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["evidence"]["match_basis"] == "phc4_public_common_procedure_profile"
    assert response["evidence"]["confidence"] == "high_extracted_table_row"
    assert response["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    assert {"type": "procedure", "value": "Knee Replacement"} in response["identity"]["unresolved_identifiers"]
    _assert_phc4_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HOSPITAL",
        expected_procedure="Knee Replacement",
    )
