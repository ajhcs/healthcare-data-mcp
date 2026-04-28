"""Tests for deterministic hospital MRF registry discovery."""

import pytest

from servers.price_transparency import mrf_registry, server
from tests.helpers import parse_tool_result


def test_builtin_registry_finds_jefferson_pa_mrfs() -> None:
    results = mrf_registry.search_registry("Jefferson", state="PA")

    names = {result["name"] for result in results}
    assert "Thomas Jefferson University Hospital" in names
    assert "Jefferson Einstein Philadelphia Hospital" in names

    first_url = results[0]["mrf_urls"][0]
    assert first_url["machine_readable_url"].startswith("https://")
    assert first_url["last_updated"] == "2026-04-28"
    assert first_url["version"] == "cms-hpt.txt"


def test_builtin_registry_finds_lvhn_pa_mrfs() -> None:
    results = mrf_registry.search_registry("Lehigh Valley Hospital", state="PA")

    ccns = {result["ccn"] for result in results}
    assert {"390133", "390185", "390201", "390338", "390430"}.issubset(ccns)
    assert all(result["mrf_urls"] for result in results)


@pytest.mark.asyncio
async def test_search_mrf_index_returns_urls_and_cache_status(monkeypatch) -> None:
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: hospital_id == "390174")
    monkeypatch.setattr(
        server.mrf_processor,
        "get_cache_metadata",
        lambda hospital_id: {"cached_at": "2026-04-28T00:00:00+00:00", "row_count": 123},
    )

    result = parse_tool_result(await server.search_mrf_index("Jefferson", state="PA"))

    assert result["total_results"] >= 2
    tjuh = next(h for h in result["hospitals"] if h["ccn"] == "390174")
    assert tjuh["hospital_id"] == "390174"
    assert tjuh["cache_status"] == "ready"
    assert tjuh["row_count"] == 123
    assert tjuh["mrf_urls"][0]["machine_readable_url"].startswith("https://")
    assert tjuh["mrf_urls"][0]["last_updated"] == "2026-04-28"
