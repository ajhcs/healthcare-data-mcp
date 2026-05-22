"""Tests for price-transparency MCP tool wrappers."""

from __future__ import annotations

import pytest

from servers.price_transparency import server
from shared.utils.mcp_response import validate_evidence_receipt
from tests.helpers import parse_tool_result


def _assert_receipt(result: dict, *, dataset_id: str, match_basis: str) -> None:
    assert result["evidence"]["dataset_id"] == dataset_id
    assert result["evidence"]["match_basis"] == match_basis
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_search_mrf_index_returns_evidence_and_identity(monkeypatch) -> None:
    async def fake_discover(query: str, state: str = "") -> list[dict]:
        return [
            {
                "name": "Example Hospital",
                "ccn": "390174",
                "ein": "123456789",
                "city": "Philadelphia",
                "state": "PA",
                "mrf_urls": [{"url": "https://example.org/mrf.json", "source_page_url": "https://example.org/cms-hpt.txt"}],
            }
        ]

    monkeypatch.setattr(server.mrf_registry, "discover_mrf_urls", fake_discover)
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: hospital_id == "390174")
    monkeypatch.setattr(server.mrf_processor, "get_cache_metadata", lambda hospital_id: {"cached_at": "2026-04-28T00:00:00+00:00", "row_count": 10})

    result = parse_tool_result(await server.search_mrf_index("Example", state="PA"))

    assert result["hospitals"][0]["ccn"] == "390174"
    _assert_receipt(result, dataset_id="cms_hospital_price_transparency_mrf_discovery", match_basis="ccn_ein_or_name_mrf_registry_search")
    assert result["identity_map"]["entities"][0]["ccn"] == "390174"


@pytest.mark.asyncio
async def test_get_negotiated_rates_returns_mrf_evidence_and_identity(monkeypatch) -> None:
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: True)
    monkeypatch.setattr(
        server.mrf_processor,
        "get_cache_metadata",
        lambda hospital_id: {"hospital_name": "Example Hospital", "cached_at": "2026-04-28T00:00:00+00:00", "row_count": 2},
    )
    monkeypatch.setattr(
        server.mrf_processor,
        "get_rates",
        lambda hospital_id, cpt_codes, payer="": [
            {
                "cpt_code": "99213",
                "description": "Office visit",
                "payer_name": "Example Payer",
                "plan_name": "PPO",
                "negotiated_dollar": 100.0,
                "methodology": "negotiated",
            }
        ],
    )

    result = parse_tool_result(await server.get_negotiated_rates("390174", ["99213"], payer="Example"))

    assert result["hospital_name"] == "Example Hospital"
    assert result["rates"][0]["negotiated_dollar"] == 100.0
    _assert_receipt(result, dataset_id="hospital_price_transparency_mrf_cache", match_basis="hospital_id_exact_mrf_cache_cpt_filter")
    validate_evidence_receipt(result["rates"][0]["evidence"], require_content=True)
    assert result["rates"][0]["evidence"]["dataset_id"] == "hospital_price_transparency_mrf_cache"
    assert result["rates"][0]["evidence"]["match_basis"] == "hospital_mrf_negotiated_rate_row"
    assert result["rates"][0]["evidence"]["query"]["hospital_id"] == "390174"
    assert result["rates"][0]["evidence"]["query"]["cpt_code"] == "99213"
    assert result["rates"][0]["evidence"]["query"]["payer_name"] == "Example Payer"
    assert result["identity"]["ccn"] == "390174"


@pytest.mark.asyncio
async def test_get_negotiated_rates_cache_missing_returns_evidence(monkeypatch) -> None:
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: False)
    monkeypatch.setattr(server.mrf_processor, "get_cache_metadata", lambda hospital_id: {})

    result = parse_tool_result(await server.get_negotiated_rates("390174", ["99213"]))

    assert result["ok"] is False
    _assert_receipt(result, dataset_id="hospital_price_transparency_mrf_cache", match_basis="mrf_cache_readiness_check")
    assert "mrf_url" in result["evidence"]["next_step"]


@pytest.mark.asyncio
async def test_compute_rate_dispersion_returns_evidence(monkeypatch) -> None:
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: True)
    monkeypatch.setattr(server.mrf_processor, "get_cache_metadata", lambda hospital_id: {"hospital_name": "Example Hospital"})
    monkeypatch.setattr(
        server.mrf_processor,
        "get_rate_stats",
        lambda hospital_id, cpt_codes: [
            {
                "cpt_code": "99213",
                "description": "Office visit",
                "payer_count": 2,
                "min_rate": 90.0,
                "max_rate": 130.0,
                "median_rate": 110.0,
            }
        ],
    )

    result = parse_tool_result(await server.compute_rate_dispersion("390174", ["99213"]))

    assert result["dispersion"][0]["median_rate"] == 110.0
    _assert_receipt(result, dataset_id="hospital_price_transparency_mrf_cache", match_basis="hospital_id_exact_mrf_cache_rate_distribution")
    validate_evidence_receipt(result["dispersion"][0]["evidence"], require_content=True)
    assert result["dispersion"][0]["evidence"]["match_basis"] == "hospital_mrf_rate_dispersion_row"
    assert result["dispersion"][0]["evidence"]["query"]["hospital_id"] == "390174"
    assert result["dispersion"][0]["evidence"]["query"]["cpt_code"] == "99213"
    assert result["identity"]["ccn"] == "390174"


@pytest.mark.asyncio
async def test_compare_rates_system_returns_evidence_and_identity_map(monkeypatch) -> None:
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: True)
    monkeypatch.setattr(server.mrf_processor, "get_cache_metadata", lambda hospital_id: {"hospital_name": hospital_id})
    monkeypatch.setattr(
        server.mrf_processor,
        "get_all_cached_hospitals",
        lambda: [
            {"hospital_id": "390174", "hospital_name": "Example Health Hospital"},
            {"hospital_id": "390175", "hospital_name": "Other Hospital"},
        ],
    )
    monkeypatch.setattr(
        server.mrf_processor,
        "get_rates",
        lambda hospital_id, cpt_codes: [
            {"cpt_code": "99213", "description": "Office visit", "payer_name": "Payer", "negotiated_dollar": 100.0}
        ],
    )

    result = parse_tool_result(await server.compare_rates_system("Example Health", ["99213"]))

    assert len(result["hospitals"]) == 1
    _assert_receipt(result, dataset_id="hospital_price_transparency_system_comparison", match_basis="cached_hospital_name_contains_system_name")
    validate_evidence_receipt(result["hospitals"][0]["evidence"], require_content=True)
    validate_evidence_receipt(result["hospitals"][0]["rates"][0]["evidence"], require_content=True)
    assert result["hospitals"][0]["evidence"]["match_basis"] == "cached_mrf_system_comparison_hospital_row"
    assert result["hospitals"][0]["rates"][0]["evidence"]["match_basis"] == "cached_mrf_system_comparison_rate_row"
    assert result["hospitals"][0]["rates"][0]["evidence"]["query"]["hospital_id"] == "390174"
    assert result["hospitals"][0]["rates"][0]["evidence"]["query"]["cpt_code"] == "99213"
    assert result["identity_map"]["entities"][0]["ccn"] == "390174"


@pytest.mark.asyncio
async def test_benchmark_rates_returns_benchmark_evidence(monkeypatch) -> None:
    monkeypatch.setattr(server.mrf_processor, "is_cached", lambda hospital_id: True)
    monkeypatch.setattr(server.mrf_processor, "get_cache_metadata", lambda hospital_id: {"hospital_name": "Example Hospital"})
    monkeypatch.setattr(
        server.mrf_processor,
        "get_rate_stats",
        lambda hospital_id, cpt_codes: [{"cpt_code": "99213", "description": "Office visit", "median_rate": 120.0}],
    )
    monkeypatch.setattr(
        server.mrf_processor,
        "get_cross_hospital_rates",
        lambda cpt_codes: [
            {"hospital_id": "390174", "cpt_code": "99213", "negotiated_dollar": 100.0},
            {"hospital_id": "390175", "cpt_code": "99213", "negotiated_dollar": 150.0},
        ],
    )

    async def fake_gpci(locality: str | None = None) -> dict:
        return {"locality": locality or "0000000", "locality_name": "National", "gpci_work": 1.0, "gpci_pe": 1.0, "gpci_mp": 1.0}

    async def fake_pfs(code: str) -> dict:
        return {"hcpc": code, "rvu_work": 1.0, "rvu_mp": 0.1, "full_nfac_pe": 1.0, "conv_fact": 33.4009}

    async def fake_utilization(code: str) -> dict:
        return {"avg_medicare_payment": 80.0}

    monkeypatch.setattr(server.benchmark_client, "get_locality_gpci", fake_gpci)
    monkeypatch.setattr(server.benchmark_client, "get_pfs_rate", fake_pfs)
    monkeypatch.setattr(server.benchmark_client, "get_utilization_data", fake_utilization)

    result = parse_tool_result(await server.benchmark_rates("390174", ["99213"], locality="0000000"))

    assert result["benchmarks"][0]["pct_of_medicare"] is not None
    _assert_receipt(result, dataset_id="price_transparency_medicare_benchmark_workflow", match_basis="hospital_mrf_median_rates_plus_medicare_pfs_and_peer_cache")
    validate_evidence_receipt(result["benchmarks"][0]["evidence"], require_content=True)
    assert result["benchmarks"][0]["evidence"]["dataset_id"] == "price_transparency_medicare_benchmark_workflow"
    assert result["benchmarks"][0]["evidence"]["match_basis"] == "hospital_mrf_medicare_peer_benchmark_row"
    assert result["benchmarks"][0]["evidence"]["query"]["hospital_id"] == "390174"
    assert result["benchmarks"][0]["evidence"]["query"]["cpt_code"] == "99213"
    assert result["benchmarks"][0]["evidence"]["query"]["locality"] == "0000000"
    assert result["identity"]["ccn"] == "390174"
