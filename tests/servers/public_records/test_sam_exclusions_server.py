"""Tool tests for public-records SAM.gov Exclusions responses."""

from __future__ import annotations

from typing import Any

import pytest

from servers.public_records import server
from shared.utils.mcp_response import validate_evidence_receipt


SAM_METADATA = {
    "source_name": "SAM.gov Exclusions",
    "source_url": "https://api.sam.gov/entity-information/v4/exclusions",
    "docs_url": "https://open.gsa.gov/api/exclusions-api/",
    "api_version": "v4",
    "queried_at": "2026-04-23T00:00:00+00:00",
    "query": {"ueiSAM": "ABC123ABC123", "recordStatus": "active"},
    "total_records": 1,
    "returned_records": 1,
    "limit": 10,
    "page_count": 1,
    "has_more": False,
    "api_key_configured": True,
    "last_error": "",
}

SAM_RECORD = {
    "exclusionDetails": {
        "classificationType": "Firm",
        "exclusionType": "Ineligible (Proceedings Completed)",
        "exclusionProgram": "Reciprocal",
        "excludingAgencyCode": "HHS",
        "excludingAgencyName": "DEPARTMENT OF HEALTH AND HUMAN SERVICES",
    },
    "exclusionIdentification": {
        "ueiSAM": "ABC123ABC123",
        "cageCode": "1CM51",
        "npi": "1234567893",
        "entityName": "Acme Health LLC",
    },
    "exclusionActions": {
        "listOfActions": [
            {
                "createDate": "01-01-2026",
                "updateDate": "01-02-2026",
                "activateDate": "01-03-2026",
                "terminationDate": None,
                "terminationType": "Indefinite",
                "recordStatus": "Active",
            }
        ]
    },
    "exclusionPrimaryAddress": {
        "addressLine1": "123 Main St",
        "city": "Pittsburgh",
        "stateOrProvinceCode": "PA",
        "zipCode": "15213",
        "countryCode": "USA",
    },
    "exclusionOtherInformation": {
        "isFASCSAOrder": "No",
        "ctCode": "A",
        "additionalComments": "Fixture comment",
        "references": {
            "referencesList": [
                {"exclusionName": "Acme Cross Reference", "type": "Cross-Reference"}
            ]
        },
    },
}


@pytest.mark.asyncio
async def test_search_sam_exclusions_requires_query() -> None:
    response = await server.search_sam_exclusions()

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_search_sam_exclusions_rejects_invalid_npi() -> None:
    response = await server.search_sam_exclusions(npi="0000000000")

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_search_sam_exclusions_returns_structured_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_search(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["uei"] == "ABC123ABC123"
        return {
            "totalRecords": 1,
            "excludedEntity": [SAM_RECORD],
            "source_metadata": SAM_METADATA,
        }

    monkeypatch.setattr(server.sam_exclusions_client, "search_exclusions", fake_search)

    response = await server.search_sam_exclusions(uei="ABC123ABC123")

    assert response["status"] == "strong_potential_match"
    assert response["total_results"] == 1
    assert response["records"][0]["display_name"] == "Acme Health LLC"
    assert response["records"][0]["match_basis"] == "uei_exact"
    assert response["records"][0]["match_score"] == 100
    assert response["records"][0]["references"][0]["type"] == "Cross-Reference"
    assert "SAM.gov Exclusions" in response["sam_verification_caveat"]
    assert response["evidence"]["dataset_id"] == "sam_gov_exclusions"
    assert response["evidence"]["match_basis"] == "uei_exact"
    assert response["evidence"]["retrieved_at"] == "2026-04-23T00:00:00+00:00"
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["identity"]["canonical_name"] == "ACME HEALTH"
    assert response["identity"]["npi"] == "1234567893"
    assert response["identity"]["zip_code"] == "15213"
    assert response["identity"]["match_decisions"][0]["basis"] == "uei_exact"
    assert {"type": "uei", "value": "ABC123ABC123"} in response["identity"]["unresolved_identifiers"]
    assert {"type": "cage_code", "value": "1CM51"} in response["identity"]["unresolved_identifiers"]


@pytest.mark.asyncio
async def test_check_sam_exclusion_identifier_returns_evidence_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_check(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["uei"] == "ABC123ABC123"
        return {
            "totalRecords": 1,
            "excludedEntity": [SAM_RECORD],
            "source_metadata": SAM_METADATA,
        }

    monkeypatch.setattr(server.sam_exclusions_client, "check_identifier", fake_check)

    response = await server.check_sam_exclusion_identifier(uei="ABC123ABC123")

    assert response["status"] == "strong_potential_match"
    assert response["records"][0]["match_basis"] == "uei_exact"
    assert response["evidence"]["dataset_id"] == "sam_gov_exclusions"
    assert response["evidence"]["match_basis"] == "uei_exact"
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["identity"]["canonical_name"] == "ACME HEALTH"


@pytest.mark.asyncio
async def test_check_sam_exclusion_identifier_handles_missing_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_check(**kwargs: Any) -> dict[str, Any]:
        return {
            "error": "SAM_GOV_API_KEY is not set.",
            "code": "missing_api_key",
            "retryable": False,
            "instructions": "Set SAM_GOV_API_KEY.",
            "source_metadata": {
                **SAM_METADATA,
                "api_key_configured": False,
                "last_error": "SAM_GOV_API_KEY is not set.",
            },
        }

    monkeypatch.setattr(server.sam_exclusions_client, "check_identifier", fake_check)

    response = await server.check_sam_exclusion_identifier(uei="ABC123ABC123")

    assert response["ok"] is False
    assert response["error"]["code"] == "missing_api_key"
    assert response["source_metadata"]["api_key_configured"] is False


@pytest.mark.asyncio
async def test_screen_sam_exclusions_batch_caps_batch_size() -> None:
    response = await server.screen_sam_exclusions_batch(
        [{"candidate_id": str(i), "entity_name": "Acme"} for i in range(101)]
    )

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_screen_sam_exclusions_batch_rejects_sensitive_identifiers() -> None:
    response = await server.screen_sam_exclusions_batch(
        [{"candidate_id": "1", "entity_name": "Acme", "ein": "12-3456789"}]
    )

    assert response["ok"] is False
    assert "does not accept" in response["error"]["message"]


@pytest.mark.asyncio
async def test_screen_sam_exclusions_batch_rejects_sensitive_identifier_alias() -> None:
    response = await server.screen_sam_exclusions_batch(
        [{"candidate_id": "1", "entity_name": "Acme", "fein": "12-3456789"}]
    )

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"
    assert "does not accept" in response["error"]["message"]


@pytest.mark.asyncio
async def test_screen_sam_exclusions_batch_rejects_invalid_candidate_npi() -> None:
    response = await server.screen_sam_exclusions_batch(
        [{"candidate_id": "1", "npi": "0000000000"}]
    )

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"
    assert "npi must be a valid" in response["error"]["message"]


@pytest.mark.asyncio
async def test_screen_sam_exclusions_batch_returns_per_candidate_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_search(**kwargs: Any) -> dict[str, Any]:
        assert kwargs["entity_name"] == "Acme Health LLC"
        return {
            "totalRecords": 1,
            "excludedEntity": [SAM_RECORD],
            "source_metadata": {
                **SAM_METADATA,
                "query": {"exclusionName": "Acme Health LLC", "recordStatus": "active"},
            },
        }

    monkeypatch.setattr(server.sam_exclusions_client, "search_exclusions", fake_search)

    response = await server.screen_sam_exclusions_batch(
        [{"candidate_id": "a", "entity_name": "Acme Health LLC"}]
    )

    assert response["total_candidates"] == 1
    assert response["results"][0]["status"] == "potential_match"
    assert response["results"][0]["match_basis"] == "name_search"
    assert response["results"][0]["best_match_score"] == 70
    assert response["results"][0]["matches"][0]["match_score"] == 70
    assert response["results"][0]["matches"][0]["entity_name"] == "Acme Health LLC"
    assert response["results"][0]["identity"]["canonical_name"] == "ACME HEALTH"
    assert response["results"][0]["identity"]["match_decisions"][0]["basis"] == "name_search"
    assert response["identity_map"][0]["npi"] == "1234567893"
    assert response["evidence"]["match_basis"] == "batch_candidate_screening"
    validate_evidence_receipt(response["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_sam_exclusions_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server.sam_exclusions_client, "source_metadata", lambda: SAM_METADATA)

    response = await server.get_sam_exclusions_metadata()

    assert response["source_name"] == "SAM.gov Exclusions"
    assert response["api_version"] == "v4"
    assert response["source_metadata"]["source_name"] == "SAM.gov Exclusions"
    assert response["evidence"]["dataset_id"] == "sam_gov_exclusions"
    assert response["evidence"]["match_basis"] == "source_metadata_lookup"
    assert response["evidence"]["confidence"] == "api_metadata"
    assert response["evidence"]["retrieved_at"] == "2026-04-23T00:00:00+00:00"
    assert response["evidence"]["cache_status"] == "live_api"
    validate_evidence_receipt(response["evidence"], require_content=True)
