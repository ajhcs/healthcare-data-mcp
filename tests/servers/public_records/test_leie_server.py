"""Tool tests for public-records LEIE screening responses."""

from __future__ import annotations

import pytest

from servers.public_records import server
from shared.utils.mcp_response import validate_evidence_receipt


METADATA = {
    "source_name": "HHS OIG LEIE",
    "source_url": "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv",
    "landing_page_url": "https://oig.hhs.gov/exclusions/exclusions_list.asp",
    "record_layout_url": "https://www.oig.hhs.gov/exclusions/files/leie_record_layout.pdf",
    "downloaded_at": "2026-04-10T11:00:39+00:00",
    "source_last_modified": "Fri, 10 Apr 2026 11:00:39 GMT",
    "source_etag": '"fixture"',
    "record_count": 2,
    "cache_path": "/tmp/leie_current.parquet",
    "csv_path": "/tmp/leie_current.csv",
    "cache_status": "fresh",
    "cache_age_days": 1.0,
    "layout_columns": ["LASTNAME", "FIRSTNAME", "NPI"],
    "last_error": "",
}

RECORD = {
    "entity_type": "individual",
    "display_name": "Jane Q Smith",
    "last_name": "Smith",
    "first_name": "Jane",
    "middle_name": "Q",
    "business_name": "",
    "general_category": "BUSOWNER",
    "specialty": "NURSING",
    "upin": "",
    "npi": "1234567893",
    "dob": "1970-01-31",
    "address": "123 Main St",
    "city": "Pittsburgh",
    "state": "PA",
    "zip_code": "15213",
    "exclusion_type": "1128b4",
    "exclusion_date": "2024-01-15",
    "reinstatement_date": "",
    "waiver_date": "",
    "waiver_state": "",
    "match_basis": "npi_exact",
    "match_score": 100,
    "verification_status": "strong_potential_match",
}

ENTITY_RECORD = {
    **RECORD,
    "entity_type": "business",
    "display_name": "Acme Health LLC",
    "last_name": "",
    "first_name": "",
    "middle_name": "",
    "business_name": "Acme Health LLC",
    "npi": "",
    "dob": "",
    "match_basis": "entity_name_state",
    "match_score": 85,
    "verification_status": "potential_match",
}


@pytest.mark.asyncio
async def test_check_leie_npi_rejects_invalid_npi() -> None:
    response = await server.check_leie_npi("0000000000")

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_check_leie_npi_returns_structured_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_ensure(force_refresh: bool = False) -> dict:
        return METADATA

    monkeypatch.setattr(server.data_loaders, "ensure_leie_cached", fake_ensure)
    monkeypatch.setattr(server.data_loaders, "query_leie_by_npi", lambda npi: [RECORD])

    response = await server.check_leie_npi("1234567893")

    assert response["status"] == "strong_potential_match"
    assert response["total_results"] == 1
    assert response["records"][0]["match_basis"] == "npi_exact"
    assert "SSN/EIN" in response["oig_verification_caveat"]
    assert response["evidence"]["dataset_id"] == "hhs_oig_leie"
    assert response["evidence"]["match_basis"] == "npi_exact"
    assert response["evidence"]["cache_freshness"] == "fresh; age_days=1.0"
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["identity"]["canonical_name"] == "JANE Q SMITH"
    assert response["identity"]["npi"] == "1234567893"
    assert response["identity"]["zip_code"] == "15213"
    assert response["identity"]["aliases"][0]["source_name"] == "HHS OIG LEIE"
    assert response["identity"]["match_decisions"][0]["basis"] == "npi_exact"


@pytest.mark.asyncio
async def test_search_leie_individual_requires_last_name() -> None:
    response = await server.search_leie_individual("")

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_search_leie_individual_returns_evidence_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_ensure(force_refresh: bool = False) -> dict:
        return METADATA

    def fake_query(
        *,
        last_name: str,
        first_name: str,
        state: str,
        dob: str,
        limit: int,
    ) -> list[dict]:
        assert last_name == "Smith"
        assert first_name == "Jane"
        assert state == "PA"
        assert dob == ""
        assert limit == 25
        return [RECORD]

    monkeypatch.setattr(server.data_loaders, "ensure_leie_cached", fake_ensure)
    monkeypatch.setattr(server.data_loaders, "query_leie_by_individual", fake_query)

    response = await server.search_leie_individual("Smith", first_name="Jane", state="PA")

    assert response["status"] == "strong_potential_match"
    assert response["records"][0]["display_name"] == "Jane Q Smith"
    assert response["evidence"]["dataset_id"] == "hhs_oig_leie"
    assert response["evidence"]["match_basis"] == "npi_exact"
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["identity"]["canonical_name"] == "JANE Q SMITH"


@pytest.mark.asyncio
async def test_search_leie_entity_requires_name_or_npi() -> None:
    response = await server.search_leie_entity()

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_search_leie_entity_returns_evidence_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_ensure(force_refresh: bool = False) -> dict:
        return METADATA

    def fake_query(
        *,
        entity_name: str,
        state: str,
        npi: str,
        limit: int,
    ) -> list[dict]:
        assert entity_name == "Acme Health LLC"
        assert state == "PA"
        assert npi == ""
        assert limit == 25
        return [ENTITY_RECORD]

    monkeypatch.setattr(server.data_loaders, "ensure_leie_cached", fake_ensure)
    monkeypatch.setattr(server.data_loaders, "query_leie_by_entity", fake_query)

    response = await server.search_leie_entity(entity_name="Acme Health LLC", state="PA")

    assert response["status"] == "potential_match"
    assert response["records"][0]["display_name"] == "Acme Health LLC"
    assert response["evidence"]["dataset_id"] == "hhs_oig_leie"
    assert response["evidence"]["match_basis"] == "entity_name_state"
    validate_evidence_receipt(response["evidence"], require_content=True)
    assert response["identity"]["canonical_name"] == "ACME HEALTH"


@pytest.mark.asyncio
async def test_screen_leie_batch_caps_batch_size() -> None:
    response = await server.screen_leie_batch([{"candidate_id": str(i)} for i in range(101)])

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_screen_leie_batch_rejects_sensitive_identifiers() -> None:
    response = await server.screen_leie_batch([{"candidate_id": "1", "ssn": "123-45-6789"}])

    assert response["ok"] is False
    assert "does not accept" in response["error"]["message"]


@pytest.mark.asyncio
async def test_screen_leie_batch_rejects_tax_identifier_alias() -> None:
    response = await server.screen_leie_batch(
        [{"candidate_id": "1", "taxpayer_identification_number": "12-3456789"}],
    )

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"
    assert "does not accept" in response["error"]["message"]


@pytest.mark.asyncio
async def test_screen_leie_batch_prioritizes_npi_exact_match(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_ensure(force_refresh: bool = False) -> dict:
        return METADATA

    monkeypatch.setattr(server.data_loaders, "ensure_leie_cached", fake_ensure)
    monkeypatch.setattr(
        server.data_loaders,
        "screen_leie_candidates",
        lambda candidates, limit_per_candidate=5: [
            {
                "candidate": candidates[0],
                "status": "strong_potential_match",
                "match_count": 1,
                "best_match_score": 100,
                "matches": [RECORD],
                "screened_at": "2026-04-23T00:00:00+00:00",
                "source_metadata": METADATA,
            }
        ],
    )

    response = await server.screen_leie_batch([{"candidate_id": "a", "npi": "1234567893"}])

    assert response["total_candidates"] == 1
    assert response["results"][0]["status"] == "strong_potential_match"
    assert response["results"][0]["matches"][0]["match_basis"] == "npi_exact"
    assert response["results"][0]["identity"]["npi"] == "1234567893"
    assert response["results"][0]["identity"]["match_decisions"][0]["basis"] == "npi_exact"
    assert response["identity_map"][0]["canonical_name"] == "JANE Q SMITH"
    assert response["evidence"]["match_basis"] == "batch_candidate_screening"
    validate_evidence_receipt(response["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_leie_metadata_does_not_require_real_download(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server.data_loaders, "get_leie_source_metadata", lambda: METADATA)

    response = await server.get_leie_metadata()

    assert response["source_name"] == "HHS OIG LEIE"
    assert response["source_url"].endswith("/UPDATED.csv")
    assert response["record_count"] == 2
    assert response["source_metadata"]["cache_status"] == "fresh"
    assert response["evidence"]["dataset_id"] == "hhs_oig_leie"
    assert response["evidence"]["match_basis"] == "source_metadata_lookup"
    assert response["evidence"]["confidence"] == "source_cache_metadata"
    assert response["evidence"]["cache_freshness"] == "fresh; age_days=1.0"
    validate_evidence_receipt(response["evidence"], require_content=True)
