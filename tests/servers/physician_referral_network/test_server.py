"""Tests for physician referral network MCP tool wrappers."""

from tests.helpers import parse_tool_result

import pytest

from servers.physician_referral_network import server
from shared.utils.mcp_response import validate_evidence_receipt
from shared.utils.source_backed_result import validate_source_claim_paths


def _assert_receipt(result: dict, *, dataset_id: str, match_basis: str) -> None:
    assert result["evidence"]["dataset_id"] == dataset_id
    assert result["evidence"]["match_basis"] == match_basis
    validate_evidence_receipt(result["evidence"], require_content=True)


def _assert_row_receipt(receipt: dict, *, dataset_id: str, match_basis: str, row_kind: str) -> None:
    validate_evidence_receipt(receipt, require_content=True)
    assert receipt["dataset_id"] == dataset_id
    assert receipt["match_basis"] == match_basis
    assert receipt["query"]["row_kind"] == row_kind
    assert receipt["confidence"]
    assert receipt["caveat"]
    assert receipt["next_step"]


@pytest.mark.asyncio
async def test_search_physicians_returns_evidence_and_identity_map(monkeypatch):
    async def fake_search(query: str, specialty: str = "", state: str = "", limit: int = 25):
        return [
            {
                "npi": "1234567893",
                "first_name": "Jane",
                "last_name": "Smith",
                "credential": "MD",
                "specialty": "Cardiovascular Disease",
                "city": "Pittsburgh",
                "state": "PA",
                "zip_code": "15213",
                "org_name": "Example Health",
            }
        ]

    monkeypatch.setattr(server.nppes_client, "search_physicians", fake_search)

    result = parse_tool_result(await server.search_physicians("Jane Smith", specialty="cardiology", state="PA"))

    assert result["physicians"][0]["npi"] == "1234567893"
    _assert_receipt(result, dataset_id="nppes_npi_registry", match_basis="nppes_name_taxonomy_state_search")
    _assert_row_receipt(
        result["physicians"][0]["evidence"],
        dataset_id="nppes_npi_registry",
        match_basis="nppes_physician_search_result_row",
        row_kind="nppes_physician_search_result",
    )
    assert result["physicians"][0]["evidence"]["query"]["row_npi"] == "1234567893"
    assert result["identity_map"]["entities"][0]["npi"] == "1234567893"
    assert result["identity_map"]["entities"][0]["state"] == "PA"
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_get_physician_profile_returns_combined_evidence_and_identity(monkeypatch):
    async def no_cache() -> bool:
        return False

    async def fake_detail(npi: str):
        return {
            "npi": npi,
            "first_name": "Jane",
            "last_name": "Smith",
            "credential": "MD",
            "specialties": ["Cardiovascular Disease"],
            "practice_locations": [{"address_1": "1 Main St", "city": "Pittsburgh", "state": "PA", "postal_code": "15213"}],
            "org_affiliations": ["Example Health"],
        }

    monkeypatch.setattr(server.nppes_client, "ensure_physician_compare_cached", no_cache)
    monkeypatch.setattr(server.nppes_client, "ensure_utilization_cached", no_cache)
    monkeypatch.setattr(server.nppes_client, "get_physician_detail", fake_detail)

    result = parse_tool_result(await server.get_physician_profile("1234567893"))

    assert result["npi"] == "1234567893"
    _assert_receipt(result, dataset_id="public_physician_profile", match_basis="npi_exact_public_profile")
    assert result["identity"]["npi"] == "1234567893"
    assert result["source_metadata"]["sources"][0]["dataset_id"] == "nppes_npi_registry"


@pytest.mark.asyncio
async def test_load_docgraph_cache_uses_explicit_csv_path(monkeypatch):
    monkeypatch.delenv("DOCGRAPH_CSV_PATH", raising=False)
    monkeypatch.setattr(server.referral_network, "load_docgraph_csv", lambda path: 42)
    monkeypatch.setattr(
        server.referral_network,
        "get_docgraph_cache_path",
        lambda: "/tmp/shared_patients.parquet",
    )

    result = parse_tool_result(await server.load_docgraph_cache("/tmp/docgraph.csv"))

    assert result["status"] == "loaded"
    assert result["csv_path"] == "/tmp/docgraph.csv"
    assert result["cache_path"] == "/tmp/shared_patients.parquet"
    assert result["rows_loaded"] == 42
    _assert_receipt(
        result,
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="operator_supplied_docgraph_csv_import",
    )


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

    result = parse_tool_result(await server.load_docgraph_cache())

    assert seen["path"] == "/data/docgraph.csv"
    assert result["rows_loaded"] == 7


@pytest.mark.asyncio
async def test_docgraph_backed_tools_return_loader_guidance_when_cache_missing(monkeypatch):
    monkeypatch.setattr(server.referral_network, "is_docgraph_cached", lambda: False)

    network_result = parse_tool_result(await server.map_referral_network("1234567890"))
    leakage_result = parse_tool_result(await server.detect_leakage("Example Health"))

    assert "load_docgraph_cache" in network_result["error"]
    assert "DOCGRAPH_CSV_PATH" in network_result["error"]
    assert network_result["data_unavailable"] == "licensed_source_missing"
    _assert_receipt(
        network_result,
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="docgraph_cache_readiness_check",
    )
    assert "load_docgraph_cache" in leakage_result["error"]
    assert leakage_result["data_unavailable"] == "licensed_source_missing"


@pytest.mark.asyncio
async def test_map_referral_network_returns_docgraph_evidence_and_identities(monkeypatch):
    monkeypatch.setattr(server.referral_network, "is_docgraph_cached", lambda: True)
    monkeypatch.setattr(
        server.referral_network,
        "get_referral_network",
        lambda npi, depth=1, min_shared=11: {
            "nodes": [{"npi": "1234567893"}, {"npi": "2000000002"}],
            "edges": [{"npi_from": "1234567893", "npi_to": "2000000002", "shared_count": 14}],
            "total_connections": 1,
        },
    )

    async def fake_search(query: str, specialty: str = "", state: str = "", limit: int = 25):
        if query == "1234567893":
            return [{"npi": query, "first_name": "Jane", "last_name": "Smith", "specialty": "Cardiology", "city": "Pittsburgh", "state": "PA"}]
        return [{"npi": query, "first_name": "John", "last_name": "Jones", "specialty": "Internal Medicine", "city": "Erie", "state": "PA"}]

    monkeypatch.setattr(server.nppes_client, "search_physicians", fake_search)

    result = parse_tool_result(await server.map_referral_network("1234567893", depth=1, min_shared=11))

    assert result["center_npi"] == "1234567893"
    _assert_receipt(
        result,
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="docgraph_exact_center_npi_shared_patient_edges",
    )
    _assert_row_receipt(
        result["nodes"][0]["evidence"],
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="docgraph_referral_network_node_row",
        row_kind="docgraph_referral_network_node",
    )
    _assert_row_receipt(
        result["edges"][0]["evidence"],
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="docgraph_shared_patient_edge_row",
        row_kind="docgraph_shared_patient_edge",
    )
    assert result["edges"][0]["evidence"]["query"]["row_npi_from"] == "1234567893"
    assert result["edges"][0]["evidence"]["query"]["row_npi_to"] == "2000000002"
    assert {entity["npi"] for entity in result["identity_map"]["entities"]} == {"1234567893", "2000000002"}


@pytest.mark.asyncio
async def test_analyze_physician_mix_returns_public_workflow_evidence(monkeypatch):
    async def fake_mix(system_name: str, state: str = ""):
        return {
            "system_name": "Example Health",
            "total_physicians": 1,
            "employed": 1,
            "affiliated": 0,
            "independent": 0,
            "employed_pct": 100.0,
            "affiliated_pct": 0.0,
            "independent_pct": 0.0,
            "by_specialty": [],
            "sample_physicians": [
                {
                    "npi": "1234567893",
                    "name": "Jane Smith",
                    "specialty": "Cardiology",
                    "status": "employed",
                    "confidence": 0.9,
                    "evidence": ["Org name matched"],
                }
            ],
        }

    monkeypatch.setattr(server.physician_mix, "analyze_system_mix", fake_mix)

    result = parse_tool_result(await server.analyze_physician_mix("Example", state="PA"))

    assert result["system_name"] == "Example Health"
    _assert_receipt(
        result,
        dataset_id="physician_mix_public_workflow",
        match_basis="ahrq_system_name_resolution_plus_nppes_organization_search",
    )
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_row_receipt(
        result["sample_physicians"][0]["evidence"],
        dataset_id="physician_mix_public_workflow",
        match_basis="physician_mix_sample_classification_row",
        row_kind="physician_mix_sample_classification",
    )
    assert result["sample_physicians"][0]["classification_evidence"] == ["Org name matched"]
    assert result["identity_map"]["entities"][0]["npi"] == "1234567893"


@pytest.mark.asyncio
async def test_detect_leakage_returns_readiness_evidence_and_destination_identities(monkeypatch):
    monkeypatch.setattr(server.referral_network, "is_docgraph_cached", lambda: True)

    async def fake_mix(system_name: str, state: str = ""):
        return {
            "system_name": "Example Health",
            "sample_physicians": [{"npi": "1234567893", "status": "employed"}],
        }

    def fake_leakage(system_npis, system_zips, min_shared=11):
        return {
            "total_referrals": 20,
            "in_network_pct": 0.0,
            "out_of_network_in_area_pct": 0.0,
            "out_of_area_pct": 100.0,
            "top_leakage_destinations": [{"npi": "2000000002", "shared_count": 20, "classification": "out_of_area"}],
            "specialty_breakdown": [
                {
                    "specialty": "Internal Medicine",
                    "total_referrals": 20,
                    "in_network": 0,
                    "out_of_network": 20,
                    "leakage_pct": 100.0,
                }
            ],
        }

    async def fake_search(query: str, specialty: str = "", state: str = "", limit: int = 25):
        return [{"npi": query, "first_name": "John", "last_name": "Jones", "specialty": "Internal Medicine", "city": "Erie", "state": "PA"}]

    monkeypatch.setattr(server.physician_mix, "analyze_system_mix", fake_mix)
    monkeypatch.setattr(server.referral_network, "detect_leakage", fake_leakage)
    monkeypatch.setattr(server.nppes_client, "search_physicians", fake_search)

    result = parse_tool_result(await server.detect_leakage("Example", state="PA"))

    assert result["top_leakage_destinations"][0]["npi"] == "2000000002"
    _assert_receipt(
        result,
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="physician_mix_system_npis_plus_docgraph_outbound_shared_patient_counts",
    )
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_row_receipt(
        result["top_leakage_destinations"][0]["evidence"],
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="docgraph_leakage_destination_row",
        row_kind="docgraph_leakage_destination",
    )
    _assert_row_receipt(
        result["specialty_breakdown"][0]["evidence"],
        dataset_id="careset_docgraph_shared_patient_counts",
        match_basis="docgraph_leakage_specialty_breakdown_row",
        row_kind="docgraph_leakage_specialty_breakdown",
    )
    assert result["identity_map"]["entities"][0]["npi"] == "2000000002"
