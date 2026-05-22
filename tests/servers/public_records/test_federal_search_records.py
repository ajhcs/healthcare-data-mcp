"""Tests for public-records federal search provenance receipts."""

from __future__ import annotations

from typing import Any

import pytest

from servers.public_records import server
from shared.utils.mcp_response import validate_evidence_receipt


@pytest.mark.asyncio
async def test_search_usaspending_returns_evidence_and_candidate_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_search_awards(
        recipient_name: str,
        award_type: str = "",
        fiscal_year: str = "",
        limit: int = 25,
    ) -> dict[str, Any]:
        assert recipient_name == "Example Health"
        return {
            "page_metadata": {"total": 1},
            "results": [
                {
                    "Award ID": "AWD-1",
                    "Recipient Name": "Example Health System",
                    "Awarding Agency": "HHS",
                    "Award Type": "Grant",
                    "Award Amount": 125000,
                    "Description": "Public health award",
                    "Start Date": "2025-01-01",
                    "End Date": "2025-12-31",
                    "NAICS Code": "622110",
                    "NAICS Description": "General Medical and Surgical Hospitals",
                }
            ],
        }

    monkeypatch.setattr(server.data_loaders, "load_cached_api_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_api_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.usaspending_client, "search_awards", fake_search_awards)

    response = await server.search_usaspending("Example Health", fiscal_year="2025")

    assert response["total_awards"] == 1
    assert response["evidence"]["dataset_id"] == "usaspending_awards"
    assert response["evidence"]["match_basis"] == "recipient_name_fiscal_year_search"
    assert response["evidence"]["cache_status"] == "written"
    validate_evidence_receipt(response["evidence"], require_content=True)
    award = response["awards"][0]
    validate_evidence_receipt(award["evidence"], require_content=True)
    assert award["evidence"]["dataset_id"] == "usaspending_awards"
    assert award["evidence"]["match_basis"] == "usaspending_award_row"
    assert award["evidence"]["query"]["row_award_id"] == "AWD-1"
    assert award["evidence"]["query"]["recipient_name"] == "Example Health"
    assert "candidate aliases" in award["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH SYSTEM"
    by_field = {entry["field"]: entry for entry in response["identity_map"]["join_keys"]}
    assert by_field["canonical_name"]["status"] == "candidate"
    assert "EXAMPLE HEALTH" in by_field["canonical_name"]["values"]
    assert "EXAMPLE HEALTH SYSTEM" in by_field["canonical_name"]["values"]
    assert response["identity_map"]["source_claims"][0]["match_policy"] == "candidate_public_records_search_not_identity_proof"


@pytest.mark.asyncio
async def test_search_sam_gov_returns_evidence_without_treating_keyword_as_exclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_search_opportunities(
        keyword: str,
        posted_from: str = "",
        posted_to: str = "",
        ptype: str = "",
        limit: int = 25,
    ) -> dict[str, Any]:
        assert keyword == "Example Health"
        return {
            "totalRecords": 1,
            "opportunitiesData": [
                {
                    "noticeId": "NOTICE-1",
                    "title": "Hospital services",
                    "solicitationNumber": "SOL-1",
                    "department": "HHS",
                    "subTier": "CMS",
                    "postedDate": "05/01/2026",
                    "responseDeadLine": "06/01/2026",
                    "naicsCode": "622110",
                    "typeOfSetAsideDescription": "",
                    "description": "Public opportunity",
                    "active": True,
                }
            ],
        }

    monkeypatch.setattr(server.data_loaders, "load_cached_api_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_api_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.sam_client, "search_opportunities", fake_search_opportunities)

    response = await server.search_sam_gov("Example Health", posted_from="05/01/2026", posted_to="05/31/2026")

    assert response["total_results"] == 1
    assert response["evidence"]["dataset_id"] == "sam_gov_opportunities"
    assert response["evidence"]["match_basis"] == "keyword_posted_date_search"
    assert "not SAM.gov Exclusions" in response["evidence"]["caveat"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    opportunity = response["opportunities"][0]
    validate_evidence_receipt(opportunity["evidence"], require_content=True)
    assert opportunity["evidence"]["dataset_id"] == "sam_gov_opportunities"
    assert opportunity["evidence"]["match_basis"] == "sam_gov_opportunity_row"
    assert opportunity["evidence"]["query"]["row_notice_id"] == "NOTICE-1"
    assert opportunity["evidence"]["query"]["row_solicitation_number"] == "SOL-1"
    assert opportunity["evidence"]["query"]["keyword"] == "Example Health"
    assert "not SAM.gov Exclusions" in opportunity["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    assert response["identity_map"]["source_claims"][0]["identity_paths"] == [
        "keyword",
        "opportunities[].notice_id",
        "opportunities[].solicitation_number",
    ]


@pytest.mark.asyncio
async def test_search_sam_gov_missing_key_returns_error_with_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_search_opportunities(
        keyword: str,
        posted_from: str = "",
        posted_to: str = "",
        ptype: str = "",
        limit: int = 25,
    ) -> dict[str, Any]:
        return {
            "error": "SAM_GOV_API_KEY not set",
            "instructions": "Set SAM_GOV_API_KEY.",
        }

    monkeypatch.setattr(server.data_loaders, "load_cached_api_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.sam_client, "search_opportunities", fake_search_opportunities)

    response = await server.search_sam_gov("Example Health")

    assert response["ok"] is False
    assert response["error"]["code"] == "missing_api_key"
    assert response["evidence"]["dataset_id"] == "sam_gov_opportunities"
    assert response["evidence"]["match_basis"] == "keyword_search_not_evaluated"
    validate_evidence_receipt(response["evidence"], require_content=True)
