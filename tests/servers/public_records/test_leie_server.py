"""Tool tests for public-records LEIE screening responses."""

from __future__ import annotations

import pytest

from servers.public_records import server


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


@pytest.mark.asyncio
async def test_search_leie_individual_requires_last_name() -> None:
    response = await server.search_leie_individual("")

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_search_leie_entity_requires_name_or_npi() -> None:
    response = await server.search_leie_entity()

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_params"


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


@pytest.mark.asyncio
async def test_get_leie_metadata_does_not_require_real_download(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server.data_loaders, "get_leie_source_metadata", lambda: METADATA)

    response = await server.get_leie_metadata()

    assert response["source_name"] == "HHS OIG LEIE"
    assert response["source_url"].endswith("/UPDATED.csv")
    assert response["record_count"] == 2
