from __future__ import annotations

import os

import pandas as pd

from shared.setup_wizard import (
    acquire_public_caches,
    import_manual_caches,
    print_agent_cache_instructions,
    print_cache_guide,
    print_cache_status,
    validate_env,
)
from shared.utils.env_file import load_env_file, parse_env_text, read_env_file, write_env_file


def test_parse_env_text_handles_comments_quotes_and_blanks() -> None:
    lines = parse_env_text('FOO=bar\n# comment\nEMPTY=\nQUOTED="hello world"\n')

    values = {line.key: line.value for line in lines if line.kind == "assignment"}

    assert values == {"FOO": "bar", "EMPTY": "", "QUOTED": "hello world"}


def test_write_env_file_preserves_template_and_quotes_spaces(tmp_path) -> None:
    template = tmp_path / ".env.example"
    target = tmp_path / ".env"
    template.write_text("# Header\nSEC_USER_AGENT=\nSAM_GOV_API_KEY=\n", encoding="utf-8")

    write_env_file(
        target,
        {"SEC_USER_AGENT": "HealthcareData contact@example.org", "SAM_GOV_API_KEY": "abc123"},
        template_path=template,
    )

    assert read_env_file(target) == {
        "SEC_USER_AGENT": "HealthcareData contact@example.org",
        "SAM_GOV_API_KEY": "abc123",
    }
    assert "# Header" in target.read_text(encoding="utf-8")
    assert 'SEC_USER_AGENT="HealthcareData contact@example.org"' in target.read_text(encoding="utf-8")


def test_load_env_file_does_not_override_existing_values(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("SAM_GOV_API_KEY=from_file\nCHPL_API_KEY=from_file\n", encoding="utf-8")
    monkeypatch.setenv("SAM_GOV_API_KEY", "from_process")

    loaded = load_env_file(env_file)

    assert loaded == env_file
    assert os.environ["SAM_GOV_API_KEY"] == "from_process"
    assert os.environ["CHPL_API_KEY"] == "from_file"


def test_validate_env_reports_important_configuration_gaps() -> None:
    messages = validate_env(
        {
            "SEC_USER_AGENT": "Healthcare support@example.com",
            "GOOGLE_CSE_API_KEY": "key",
            "MCP_GATEWAY_BEARER_TOKEN_SHA256": "not-a-hash",
        }
    )

    assert any("SEC_USER_AGENT" in message for message in messages)
    assert any("SAM_GOV_API_KEY" in message for message in messages)
    assert any("GOOGLE_CSE_API_KEY" in message for message in messages)
    assert any("MCP_GATEWAY_BEARER_TOKEN_SHA256" in message for message in messages)


def test_import_manual_caches_copies_public_record_seeds(tmp_path) -> None:
    source_breach = tmp_path / "breaches.csv"
    cache_root = tmp_path / "cache"
    source_breach.write_text("name,state\nExample Hospital,PA\n", encoding="utf-8")

    results = import_manual_caches(
        cache_root=cache_root,
        breach_csv=source_breach,
    )

    assert len(results) == 1
    assert (cache_root / "public-records" / "hipaa_breaches.csv").read_text(
        encoding="utf-8"
    ) == source_breach.read_text(encoding="utf-8")


def test_import_docgraph_csv_converts_to_parquet(tmp_path) -> None:
    source = tmp_path / "docgraph.csv"
    cache_root = tmp_path / "cache"
    source.write_text("npi1,npi2,shared_patients\n1111111111,2222222222,7\n", encoding="utf-8")

    results = import_manual_caches(cache_root=cache_root, docgraph_csv=source)

    assert "1 rows" in results[0]
    assert (cache_root / "docgraph" / "shared_patients.parquet").exists()


def test_acquire_public_caches_fetches_hipaa_breach_table(tmp_path, monkeypatch) -> None:
    def fake_read_html(url: str):
        assert url == "https://ocrportal.hhs.gov/ocr/breach/breach_report_hip.jsf"
        return [
            pd.DataFrame({"not": ["it"]}),
            pd.DataFrame(
                {
                    "Name of Covered Entity": ["Example Hospital"],
                    "State": ["PA"],
                    "Covered Entity Type": ["Healthcare Provider"],
                    "Individuals Affected": [1234],
                    "Breach Submission Date": ["04/01/2026"],
                    "Type of Breach": ["Hacking/IT Incident"],
                    "Location of Breached Information": ["Email"],
                }
            ),
        ]

    monkeypatch.setattr(pd, "read_html", fake_read_html)

    results = acquire_public_caches(cache_root=tmp_path / "cache", hipaa_breaches=True)

    target = tmp_path / "cache" / "public-records" / "hipaa_breaches.csv"
    assert "1 rows" in results[0]
    assert "Example Hospital" in target.read_text(encoding="utf-8")


def test_cache_status_reports_missing_manual_data(tmp_path, capsys) -> None:
    print_cache_status(tmp_path / "cache")

    output = capsys.readouterr().out
    assert "340B covered entities" not in output
    assert "DocGraph shared patients: UNAVAILABLE" in output
    assert "data_unavailable: licensed_source_missing" in output
    assert "hc-mcp-setup --acquire-hipaa-breaches" in output
    assert "PHC4 public reports: MISSING" in output
    assert "hc-mcp-setup --acquire-phc4-public-reports" in output


def test_cache_status_requires_docgraph_parquet_for_ready(tmp_path, capsys) -> None:
    cache_root = tmp_path / "cache"
    seed = cache_root / "docgraph" / "shared_patients.csv"
    seed.parent.mkdir(parents=True)
    seed.write_text("npi1,npi2,shared_patients\n1111111111,2222222222,7\n", encoding="utf-8")

    print_cache_status(cache_root)

    output = capsys.readouterr().out
    assert "DocGraph shared patients: UNAVAILABLE" in output
    assert "DocGraph shared patients: READY" not in output


def test_cache_guide_prints_source_and_import_commands(tmp_path, capsys) -> None:
    print_cache_guide(tmp_path / "cache")

    output = capsys.readouterr().out
    assert "Data acquisition guide" in output
    assert "hc-mcp-setup --acquire-hipaa-breaches" in output
    assert "hc-mcp-setup --acquire-ahrq-hfmd" in output


def test_agent_cache_instructions_skip_ready_cache(tmp_path, capsys) -> None:
    cache_root = tmp_path / "cache"
    ready = cache_root / "public-records" / "hipaa_breaches.csv"
    ready.parent.mkdir(parents=True)
    ready.write_text("name,state\nExample Hospital,PA\n", encoding="utf-8")

    print_agent_cache_instructions(cache_root)

    output = capsys.readouterr().out
    assert "HIPAA breach reports" not in output
    assert "PHC4 public reports" in output
    assert "After imports" in output
