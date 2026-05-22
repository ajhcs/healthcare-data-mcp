from __future__ import annotations

from pathlib import Path

import pytest

from servers.public_records import data_loaders, server
from shared.utils.mcp_response import validate_evidence_receipt


def _assert_cyber_identity_map(
    identity_map: dict,
    *,
    expected_name: str = "",
    expected_state: str = "",
    expected_cik: str = "",
    expected_sources: set[str] | None = None,
) -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "public_cyber_breach_records"
    assert identity_map["source_claims"]
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No-hit or not-evaluated cyber/breach responses")
    if expected_name:
        assert expected_name in by_field["canonical_name"]["values"]
    if expected_state:
        assert expected_state in by_field["state"]["values"]
    if expected_cik:
        assert expected_cik in by_field["cik"]["values"]
    if expected_sources:
        assert {claim["collection"] for claim in identity_map["source_claims"]} >= expected_sources


def _cyber_source_claim(identity_map: dict, collection: str) -> dict:
    claims = {claim["collection"]: claim for claim in identity_map["source_claims"]}
    return claims[collection]


def _assert_cyber_source_metadata(response: dict) -> None:
    metadata = response["source_metadata"]
    evidence = response["evidence"]

    assert metadata["source_name"] == evidence["source_name"]
    assert metadata["source_url"] == evidence["source_url"]
    assert metadata["dataset_id"] == evidence["dataset_id"]
    assert metadata["source_period"] == evidence["source_period"]
    assert metadata["landing_page"] == evidence["landing_page"]
    assert metadata["retrieved_at"] == evidence["retrieved_at"]
    assert metadata["source_modified"] == evidence["source_modified"]
    assert metadata["cache_status"] == evidence["cache_status"]
    assert metadata["cache_freshness"] == evidence["cache_freshness"]
    assert metadata["entity_scope"] == evidence["entity_scope"]
    assert metadata["query"] == evidence["query"]
    assert metadata["cache_key"] == evidence["cache_key"]
    assert metadata["source_type"] == "public_cyber_breach_record"


@pytest.fixture
def cyber_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    ocr_dir = tmp_path / "ocr_enforcement_actions"
    sec_dir = tmp_path / "sec_cyber_disclosures"
    ocr_dir.mkdir()
    sec_dir.mkdir()

    monkeypatch.setattr(data_loaders, "_OCR_ENFORCEMENT_DIR", ocr_dir)
    monkeypatch.setattr(data_loaders, "_OCR_ENFORCEMENT_PARQUET", tmp_path / "ocr_enforcement_actions.parquet")
    monkeypatch.setattr(data_loaders, "_SEC_CYBER_DISCLOSURES_DIR", sec_dir)
    monkeypatch.setattr(data_loaders, "_SEC_CYBER_DISCLOSURES_PARQUET", tmp_path / "sec_cyber_disclosures.parquet")
    return tmp_path


def test_ocr_enforcement_public_html_pages_are_indexed(cyber_cache: Path) -> None:
    source_dir = cyber_cache / "ocr_enforcement_actions"
    (source_dir / "example.html").write_text(
        """
        <html>
          <head><title>Example Health OCR Resolution Agreement</title></head>
          <body>
            <h1>Example Health</h1>
            <p>OCR announced a ransomware data security incident resolution.</p>
          </body>
        </html>
        """,
        encoding="utf-8",
    )

    result = data_loaders.search_ocr_enforcement_actions(entity_name="Example Health")

    assert result["source_status"]["status"] == "ready"
    assert result["source_status"]["record_count"] == 1
    assert result["records"][0]["source_type"] == "ocr_enforcement_action"
    assert result["records"][0]["incident_type"] == "ransomware"
    assert result["records"][0]["entity_match_confidence"] == "medium"
    assert result["records"][0]["timeline_inferred"] is False


@pytest.mark.asyncio
async def test_ocr_enforcement_search_records_include_row_evidence(cyber_cache: Path) -> None:
    source_dir = cyber_cache / "ocr_enforcement_actions"
    (source_dir / "example.html").write_text(
        """
        <html>
          <head><title>Example Health OCR Resolution Agreement</title></head>
          <body>
            <h1>Example Health</h1>
            <p>OCR announced a ransomware data security incident resolution.</p>
          </body>
        </html>
        """,
        encoding="utf-8",
    )

    response = await server.search_ocr_enforcement_actions(entity_name="Example Health")

    record = response["records"][0]
    validate_evidence_receipt(record["evidence"], require_content=True)
    assert record["evidence"]["dataset_id"] == "hhs_ocr_enforcement_actions"
    assert record["evidence"]["match_basis"] == "ocr_enforcement_action_row"
    assert record["evidence"]["source_url"] == (
        "https://www.hhs.gov/hipaa/for-professionals/compliance-enforcement/agreements/index.html"
    )
    assert record["evidence"]["query"]["row_source_file"].endswith("example.html")
    assert record["evidence"]["query"]["row_source_type"] == "ocr_enforcement_action"
    assert record["evidence"]["query"]["row_incident_type"] == "ransomware"
    assert record["evidence"]["query"]["row_entity_match_confidence"] == "medium"
    claim = _cyber_source_claim(response["identity_map"], "hhs_ocr_enforcement_actions")
    assert claim["row_evidence_paths"] == ["records[].evidence"]


@pytest.mark.asyncio
async def test_sec_cyber_disclosure_search_requires_sec_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    response = await server.search_sec_cyber_disclosures(entity_name="Example Health")

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_config"
    assert response["evidence"]["match_basis"] == "sec_user_agent_missing"
    assert response["evidence"]["confidence"] == "not_evaluated_source_not_searchable"
    assert "not proof" in response["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_sources={"sec_cyber_disclosures"},
    )
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_sec_cyber_disclosure_search_returns_accession_date_and_confidence(
    cyber_cache: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = cyber_cache / "sec_cyber_disclosures"
    (source_dir / "sec.jsonl").write_text(
        (
            '{"entity_name":"Example Health Inc","cik":"0000123456",'
            '"accession_number":"0000123456-26-000010","filing_date":"2026-02-03",'
            '"summary":"Item 1.05 Cybersecurity incident involving unauthorized access",'
            '"source_url":"https://www.sec.gov/Archives/edgar/data/123456/fixture.txt"}\n'
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    response = await server.search_sec_cyber_disclosures(entity_name="Example Health", cik="123456")

    assert response["total_results"] == 1
    record = response["records"][0]
    assert record["accession_number"] == "0000123456-26-000010"
    assert record["accession"] == "0000123456-26-000010"
    assert record["disclosure_date"] == "2026-02-03"
    assert record["date"] == "2026-02-03"
    assert record["confidence"] == "high"
    assert record["entity_match_confidence"] == "high"
    assert record["incident_type_confidence"] == "high"
    assert record["source_type"] == "sec_cyber_disclosure"
    validate_evidence_receipt(record["evidence"], require_content=True)
    assert record["evidence"]["dataset_id"] == "sec_cyber_disclosures"
    assert record["evidence"]["match_basis"] == "sec_cyber_disclosure_row"
    assert record["evidence"]["source_url"] == "https://www.sec.gov/Archives/edgar/data/123456/fixture.txt"
    assert record["evidence"]["query"]["row_source_type"] == "sec_cyber_disclosure"
    assert record["evidence"]["query"]["row_accession"] == "0000123456-26-000010"
    assert record["evidence"]["query"]["row_incident_type_confidence"] == "high"
    assert response["evidence"]["dataset_id"] == "sec_cyber_disclosures"
    assert response["evidence"]["match_basis"] == "imported_sec_disclosure_search"
    assert response["evidence"]["source_period"] == "unbounded to latest indexed filing"
    assert response["evidence"]["cache_status"] == "ready"
    assert response["evidence"]["cache_freshness"]
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_cik="123456",
        expected_sources={"sec_cyber_disclosures"},
    )
    claim = _cyber_source_claim(response["identity_map"], "sec_cyber_disclosures")
    assert claim["row_evidence_paths"] == ["records[].evidence"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_sec_cyber_disclosure_zero_result_is_scoped_no_match(
    cyber_cache: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_dir = cyber_cache / "sec_cyber_disclosures"
    (source_dir / "sec.jsonl").write_text(
        (
            '{"entity_name":"Other Health Inc","cik":"0000999999",'
            '"accession_number":"0000999999-26-000010","filing_date":"2026-02-03",'
            '"summary":"Item 1.05 Cybersecurity incident",'
            '"source_url":"https://www.sec.gov/Archives/edgar/data/999999/fixture.txt"}\n'
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SEC_USER_AGENT", "healthcare-data-mcp tests@example.com")

    response = await server.search_sec_cyber_disclosures(entity_name="Example Health", cik="123456")

    assert response["total_results"] == 0
    assert response["evidence"]["match_basis"] == "imported_sec_disclosure_search_no_match"
    assert response["evidence"]["confidence"] == "no_indexed_sec_cyber_disclosure_match"
    assert "not proof" in response["evidence"]["caveat"]
    assert response["identity"]["unresolved_identifiers"] == [{"type": "cik", "value": "123456"}]
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_cik="123456",
        expected_sources={"sec_cyber_disclosures"},
    )
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_breach_history_records_include_row_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server.data_loaders, "ensure_breach_loaded", lambda: True)
    monkeypatch.setattr(
        server.data_loaders,
        "query_breaches",
        lambda **kwargs: [
            {
                "entity_name": "Example Health",
                "state": "PA",
                "covered_entity_type": "Healthcare Provider",
                "individuals_affected": 5000,
                "breach_submission_date": "2026-01-15",
                "breach_type": "Hacking/IT Incident",
                "location_of_breached_info": "Network Server",
                "business_associate_present": "No",
                "web_description": "Example Health disclosed a ransomware incident.",
            }
        ],
    )

    response = await server.get_breach_history(entity_name="Example Health", state="PA")

    breach = response["breaches"][0]
    validate_evidence_receipt(breach["evidence"], require_content=True)
    assert breach["evidence"]["dataset_id"] == "hhs_ocr_breach_portal"
    assert breach["evidence"]["match_basis"] == "hhs_ocr_breach_row"
    assert breach["evidence"]["source_url"] == "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf"
    assert breach["evidence"]["query"]["row_source_type"] == "hhs_ocr_breach_portal"
    assert breach["evidence"]["query"]["row_individuals_affected"] == 5000
    assert breach["evidence"]["query"]["row_incident_type"] == "Hacking/IT Incident"
    claim = _cyber_source_claim(response["identity_map"], "hhs_ocr_breach_portal")
    assert claim["row_evidence_paths"] == ["breaches[].evidence"]


@pytest.mark.asyncio
async def test_ocr_enforcement_missing_index_returns_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_loaders, "_OCR_ENFORCEMENT_DIR", tmp_path / "ocr_enforcement_actions")
    monkeypatch.setattr(data_loaders, "_OCR_ENFORCEMENT_PARQUET", tmp_path / "ocr_enforcement_actions.parquet")

    response = await server.search_ocr_enforcement_actions(entity_name="Example Health")

    assert response["total_results"] == 0
    assert response["source_status"]["status"] == "not_searchable"
    assert response["evidence"]["match_basis"] == "imported_public_record_search_source_not_searchable"
    assert response["evidence"]["confidence"] == "not_evaluated_source_not_searchable"
    assert "not proof" in response["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_sources={"hhs_ocr_enforcement_actions"},
    )
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_state_ag_statuses_include_pa_nj_de_reasons() -> None:
    response = await server.get_state_ag_breach_notice_sources()

    assert response["sources"]["PA"]["status"] == "ready"
    assert response["sources"]["NJ"]["status"] == "not_searchable"
    assert response["sources"]["DE"]["status"] == "not_automatable"
    assert all(response["sources"][state]["reason"] for state in ("PA", "NJ", "DE"))
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)
    _assert_cyber_identity_map(response["identity_map"], expected_sources={"state_ag_breach_notices"})


@pytest.mark.asyncio
async def test_cisa_kev_context_is_not_attribution() -> None:
    response = await server.get_cisa_kev_context_status()

    assert response["status"] == "context_only"
    assert response["attribution_used"] is False
    assert "must not be used to attribute" in response["reason"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)
    _assert_cyber_identity_map(response["identity_map"], expected_sources={"cisa_kev_context"})


@pytest.mark.asyncio
async def test_cyber_incident_profile_includes_required_confidence_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_breach_history(entity_name: str, state: str = "", min_individuals: int = 0) -> dict:
        return {
            "search_entity": entity_name,
            "total_breaches": 1,
            "total_individuals_affected": 5000,
            "breaches": [
                {
                    "entity_name": "Example Health",
                    "state": state,
                    "individuals_affected": 5000,
                    "breach_submission_date": "2026-01-15",
                    "breach_type": "Hacking/IT Incident",
                    "web_description": "Ransomware incident disclosed to OCR.",
                }
            ],
        }

    monkeypatch.setattr(server, "get_breach_history", fake_breach_history)
    monkeypatch.setattr(
        server.data_loaders,
        "search_ocr_enforcement_actions",
        lambda **kwargs: {"source_status": {"status": "not_searchable"}, "records": []},
    )
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    response = await server.get_cyber_incident_profile(entity_name="Example Health", state="PA")

    incident = response["incidents"][0]
    assert incident["source_type"] == "hhs_ocr_breach_portal"
    assert incident["entity_match_confidence"] == "high"
    assert incident["incident_type_confidence"] == "high"
    assert incident["timeline_disclosed"] is True
    assert incident["timeline_inferred"] is False
    validate_evidence_receipt(incident["evidence"], require_content=True)
    assert incident["evidence"]["dataset_id"] == "hhs_ocr_breach_portal"
    assert incident["evidence"]["match_basis"] == "public_cyber_incident_row"
    assert incident["evidence"]["query"]["row_source_type"] == "hhs_ocr_breach_portal"
    assert incident["evidence"]["query"]["row_incident_type"] == "ransomware"
    assert incident["evidence"]["query"]["row_individuals_affected"] == 5000
    assert response["sources"]["cisa_kev"]["attribution_used"] is False
    assert response["evidence"]["dataset_id"] == "public_cyber_incident_profile"
    assert response["evidence"]["match_basis"] == "multi_source_public_record_profile"
    assert response["evidence"]["source_period"]
    assert response["evidence"]["cache_status"] == "aggregated"
    assert response["evidence"]["cache_freshness"]
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_state="PA",
        expected_sources={"public_cyber_incident_profile"},
    )
    claim = _cyber_source_claim(response["identity_map"], "public_cyber_incident_profile")
    assert claim["row_evidence_paths"] == ["incidents[].evidence"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_cyber_incident_profile_zero_result_is_not_assurance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_breach_history(entity_name: str, state: str = "", min_individuals: int = 0) -> dict:
        return {
            "search_entity": entity_name,
            "total_breaches": 0,
            "total_individuals_affected": 0,
            "breaches": [],
        }

    monkeypatch.setattr(server, "get_breach_history", fake_breach_history)
    monkeypatch.setattr(
        server.data_loaders,
        "search_ocr_enforcement_actions",
        lambda **kwargs: {"source_status": {"status": "not_searchable"}, "records": []},
    )
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    response = await server.get_cyber_incident_profile(entity_name="Example Health", state="PA")

    assert response["incident_count"] == 0
    assert response["evidence"]["match_basis"] == "multi_source_public_record_profile_no_match"
    assert response["evidence"]["confidence"] == "no_configured_public_cyber_incident_source_match"
    assert "not proof of no incident" in response["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_state="PA",
        expected_sources={"public_cyber_incident_profile"},
    )
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


def test_blank_breach_entity_name_is_not_high_confidence_match() -> None:
    confidence = server._breach_entity_match_confidence(
        "Example Health",
        {"entity_name": "", "web_description": "Example Health disclosed a cyber incident."},
    )

    assert confidence == "medium"


@pytest.mark.asyncio
async def test_breach_history_missing_cache_returns_evidence_and_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_loaders, "_BREACH_PARQUET", tmp_path / "hipaa_breaches.parquet")
    monkeypatch.setattr(data_loaders, "_BREACH_CSV", tmp_path / "hipaa_breaches.csv")

    response = await server.get_breach_history(entity_name="Example Health", state="PA")

    assert response["ok"] is False
    assert response["error"]["code"] == "source_unavailable"
    assert response["evidence"]["match_basis"] == "hhs_ocr_breach_cache_missing"
    assert response["evidence"]["confidence"] == "not_evaluated_source_missing"
    assert response["evidence"]["cache_status"] == "missing"
    assert "not proof" in response["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    assert response["identity"]["unresolved_identifiers"] == [{"type": "state", "value": "PA"}]
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_state="PA",
        expected_sources={"hhs_ocr_breach_portal"},
    )
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_cyber_attestation_source_status_is_unsupported() -> None:
    response = await server.get_cyber_attestation_source_status()

    assert response["status"] == "not_publicly_available"
    assert response["can_assert_attestation_status"] is False
    assert response["evidence"]["confidence"] == "unsupported_assertion"
    assert response["evidence"]["source_period"]
    assert response["evidence"]["cache_status"] == "not_applicable"
    _assert_cyber_identity_map(response["identity_map"], expected_sources={"unsupported_cybersecurity_attestation"})
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)
    assert any(source["source_type"] == "cms_promoting_interoperability" for source in response["supported_adjacent_sources"])


@pytest.mark.asyncio
async def test_interop_status_does_not_assert_cybersecurity_attestation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_ensure_pi_cached():
        return True

    monkeypatch.setattr(server.data_loaders, "ensure_pi_cached", fake_ensure_pi_cached)
    monkeypatch.setattr(
        server.data_loaders,
        "query_pi",
        lambda **kwargs: [
            {
                "facility_name": "Example Hospital",
                "ccn": "390001",
                "state": "PA",
                "meets_pi_criteria": "Y",
                "cehrt_id": "ABC123",
            }
        ],
    )

    response = await server.get_interop_status(ccn="390001")

    assert response["total_results"] == 1
    assert response["can_assert_cybersecurity_attestation"] is False
    assert "does not establish a general cybersecurity attestation" in response["source_note"]


def test_state_breach_notice_import_and_search_requires_source_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_path = tmp_path / "state_breach_notices.parquet"
    csv_path = tmp_path / "pa_notices.csv"
    csv_path.write_text(
        "entity_name,state,date,title\nExample Health,PA,2026-01-01,Example Notice\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(data_loaders, "_STATE_BREACH_NOTICES_PARQUET", cache_path)

    with pytest.raises(ValueError, match="source_url"):
        data_loaders.import_state_breach_notices("PA", csv_path)

    imported = data_loaders.import_state_breach_notices("PA", csv_path, source_url="https://example.test/notice")
    result = data_loaders.search_state_breach_notices(entity_name="Example Health", state="PA")

    assert imported["rows_imported"] == 1
    assert result["source_status"]["status"] == "ready"
    assert result["records"][0]["entity_match_confidence"] == "high"
    assert result["records"][0]["confidence"] == "high"


@pytest.mark.asyncio
async def test_state_breach_notice_search_evidence_includes_cache_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "state_breach_notices.parquet"
    csv_path = tmp_path / "pa_notices.csv"
    csv_path.write_text(
        "entity_name,state,date,source_url,title\n"
        "Example Health,PA,2026-01-01,https://example.test/notice,Example Notice\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(data_loaders, "_STATE_BREACH_NOTICES_PARQUET", cache_path)
    data_loaders.import_state_breach_notices("PA", csv_path)

    response = await server.search_state_breach_notices(entity_name="Example Health", state="PA")

    assert response["evidence"]["dataset_id"] == "state_ag_breach_notices"
    assert response["evidence"]["source_period"] == "unbounded to latest reviewed import"
    assert response["evidence"]["cache_status"] == "ready"
    assert response["evidence"]["cache_freshness"]
    validate_evidence_receipt(response["records"][0]["evidence"], require_content=True)
    assert response["records"][0]["evidence"]["dataset_id"] == "state_ag_breach_notices"
    assert response["records"][0]["evidence"]["match_basis"] == "state_breach_notice_row"
    assert response["records"][0]["evidence"]["source_url"] == "https://example.test/notice"
    assert response["records"][0]["evidence"]["query"]["row_source_type"] == "state_ag_breach_notice"
    assert response["records"][0]["evidence"]["query"]["row_title"] == "Example Notice"
    assert response["records"][0]["evidence"]["query"]["row_confidence"] == "high"
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_state="PA",
        expected_sources={"state_ag_breach_notices"},
    )
    claim = _cyber_source_claim(response["identity_map"], "state_ag_breach_notices")
    assert claim["row_evidence_paths"] == ["records[].evidence"]
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


@pytest.mark.asyncio
async def test_state_breach_notice_missing_import_returns_scoped_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(data_loaders, "_STATE_BREACH_NOTICES_PARQUET", tmp_path / "state_breach_notices.parquet")

    response = await server.search_state_breach_notices(entity_name="Example Health", state="PA")

    assert response["total_results"] == 0
    assert response["source_status"]["status"] == "import_required"
    assert response["evidence"]["match_basis"] == "reviewed_imported_state_notice_search_source_import_required"
    assert response["evidence"]["confidence"] == "not_evaluated_source_import_required"
    assert "not proof" in response["evidence"]["caveat"]
    assert response["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_cyber_identity_map(
        response["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_state="PA",
        expected_sources={"state_ag_breach_notices"},
    )
    validate_evidence_receipt(response["evidence"], require_content=True)
    _assert_cyber_source_metadata(response)


def test_state_breach_notice_blank_entity_downgrades_confidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_path = tmp_path / "state_breach_notices.parquet"
    csv_path = tmp_path / "pa_notices.csv"
    csv_path.write_text(
        "entity_name,state,date,source_url,title\n,PA,2026-01-01,https://example.test/notice,Example Notice\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(data_loaders, "_STATE_BREACH_NOTICES_PARQUET", cache_path)

    data_loaders.import_state_breach_notices("PA", csv_path)
    result = data_loaders.search_state_breach_notices(state="PA")

    assert result["records"][0]["entity_match_confidence"] == "not_requested"
    assert result["records"][0]["confidence"] == "low"
