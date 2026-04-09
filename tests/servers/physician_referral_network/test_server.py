"""Tests for physician referral network MCP tool wrappers."""

import json

import pytest

from servers.physician_referral_network import server


@pytest.mark.asyncio
async def test_load_docgraph_cache_uses_explicit_csv_path(monkeypatch):
    monkeypatch.delenv("DOCGRAPH_CSV_PATH", raising=False)
    monkeypatch.setattr(server.referral_network, "load_docgraph_csv", lambda path: 42)
    monkeypatch.setattr(
        server.referral_network,
        "get_docgraph_cache_path",
        lambda: "/tmp/shared_patients.parquet",
    )

    result = json.loads(await server.load_docgraph_cache("/tmp/docgraph.csv"))

    assert result == {
        "status": "loaded",
        "csv_path": "/tmp/docgraph.csv",
        "cache_path": "/tmp/shared_patients.parquet",
        "rows_loaded": 42,
    }


@pytest.mark.asyncio
async def test_load_docgraph_cache_uses_env_fallback(monkeypatch):
    seen: dict[str, str] = {}

    def fake_loader(path: str) -> int:
        seen["path"] = path
        return 7

    monkeypatch.setenv("DOCGRAPH_CSV_PATH", "/data/docgraph.csv")
    monkeypatch.setattr(server.referral_network, "load_docgraph_csv", fake_loader)
    monkeypatch.setattr(
        server.referral_network,
        "get_docgraph_cache_path",
        lambda: "/tmp/shared_patients.parquet",
    )

    result = json.loads(await server.load_docgraph_cache())

    assert seen["path"] == "/data/docgraph.csv"
    assert result["rows_loaded"] == 7


@pytest.mark.asyncio
async def test_docgraph_backed_tools_return_loader_guidance_when_cache_missing(monkeypatch):
    monkeypatch.setattr(server.referral_network, "is_docgraph_cached", lambda: False)

    network_result = json.loads(await server.map_referral_network("1234567890"))
    leakage_result = json.loads(await server.detect_leakage("Example Health"))

    assert "load_docgraph_cache" in network_result["error"]
    assert "DOCGRAPH_CSV_PATH" in network_result["error"]
    assert "load_docgraph_cache" in leakage_result["error"]
