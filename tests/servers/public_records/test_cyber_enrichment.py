from __future__ import annotations

from pathlib import Path

import pytest

from servers.public_records import data_loaders, server


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
async def test_sec_cyber_disclosure_search_requires_sec_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    response = await server.search_sec_cyber_disclosures(entity_name="Example Health")

    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_config"


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


@pytest.mark.asyncio
async def test_state_ag_statuses_include_pa_nj_de_reasons() -> None:
    response = await server.get_state_ag_breach_notice_sources()

    assert response["sources"]["PA"]["status"] == "ready"
    assert response["sources"]["NJ"]["status"] == "not_searchable"
    assert response["sources"]["DE"]["status"] == "not_automatable"
    assert all(response["sources"][state]["reason"] for state in ("PA", "NJ", "DE"))


@pytest.mark.asyncio
async def test_cisa_kev_context_is_not_attribution() -> None:
    response = await server.get_cisa_kev_context_status()

    assert response["status"] == "context_only"
    assert response["attribution_used"] is False
    assert "must not be used to attribute" in response["reason"]


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
    assert response["sources"]["cisa_kev"]["attribution_used"] is False


def test_blank_breach_entity_name_is_not_high_confidence_match() -> None:
    confidence = server._breach_entity_match_confidence(
        "Example Health",
        {"entity_name": "", "web_description": "Example Health disclosed a cyber incident."},
    )

    assert confidence == "medium"
