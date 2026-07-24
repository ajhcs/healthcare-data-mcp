from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from servers.form_990_facts.fact_store import Form990FactStore, validate_lan_bind_host


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "8.8.8.8", "example.com"])
def test_query_server_rejects_public_wildcard_or_hostname_binds(host: str) -> None:
    with pytest.raises(ValueError, match="LAN-only|literal"):
        validate_lan_bind_host(host)


@pytest.mark.parametrize(
    ("host", "expected"),
    [("localhost", "127.0.0.1"), ("127.0.0.1", "127.0.0.1"), ("192.168.1.60", "192.168.1.60")],
)
def test_query_server_accepts_only_loopback_or_private_lan_binds(host: str, expected: str) -> None:
    assert validate_lan_bind_host(host) == expected


def test_lan_query_tool_reads_preingested_facts_without_exposing_ingestion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "facts.sqlite3"
    store = Form990FactStore(database_path)
    store.ingest_xml(
        (FIXTURES / "health_system_2024.xml").read_bytes(),
        source_url="https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_11C.zip",
        source_member="202523169349306307_public.xml",
        object_id="202523169349306307",
        filing_year=2025,
    )
    monkeypatch.setenv("HC_990_FACTS_DB", str(database_path))
    server = importlib.import_module("servers.form_990_facts.server")

    result = server.get_form_990_facts(ein="811244422", tax_year=2024)

    assert result["status"] == "ready"
    assert result["facts"]["total_revenue"]["value"] == 32_100_123_456
    assert not hasattr(server, "ingest_form_990")
