from __future__ import annotations

import pytest

from servers.provider_enrollment import data_loaders, server
from shared.utils.mcp_response import validate_evidence_receipt


def assert_provider_source_metadata(result: dict) -> None:
    metadata = result["source_metadata"]
    evidence = result["evidence"]

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
    assert metadata["source_type"] == "cms_pecos_provider_enrollment_public_file"


@pytest.fixture
def provider_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(data_loaders, "_CACHE_DIR", tmp_path)
    data_loaders.cache_records(
        "medicare_ffs_public_provider_enrollment",
        [
            {
                "NPI": "1234567893",
                "PAC ID": "PAC-77",
                "Enrollment ID": "ENR-1",
                "State": "PA",
                "Provider Name": "Jefferson Medical Group",
                "Provider Type": "Group Practice",
            }
        ],
    )
    data_loaders.cache_records(
        "hospital_enrollments",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Provider Type": "Hospital",
            }
        ],
    )
    data_loaders.cache_records(
        "hospital_all_owners",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Owner Organization Name": "Jefferson Parent LLC",
                "Owner Associate ID": "OWN-1",
                "Owner PAC ID": "PAC-OWNER",
                "Association Role Text": "Direct owner",
                "Ownership Percentage": "100",
            }
        ],
    )
    data_loaders.cache_records(
        "hospital_chow",
        [
            {
                "Enrollment ID": "ENR-1",
                "CCN": "390001",
                "Facility Name": "Jefferson Hospital",
                "State": "PA",
                "Transaction Date": "2026-04-01",
                "Change Type": "Change of ownership",
            }
        ],
    )
    return tmp_path


@pytest.mark.asyncio
async def test_search_provider_enrollment(provider_cache) -> None:
    result = await server.search_provider_enrollment(npi="1234567893")

    assert result["total_results"] == 1
    enrollment = result["enrollments"][0]
    assert enrollment["npi"] == "1234567893"
    assert enrollment["raw"]["PAC ID"] == "PAC-77"
    assert enrollment["source_name"] == "CMS Provider Enrollment"
    assert enrollment["source_url"] == "records"
    assert enrollment["landing_page"] == "fixture://medicare_ffs_public_provider_enrollment"
    assert enrollment["retrieved_at"]
    assert enrollment["source_modified"] == ""
    assert enrollment["entity_scope"] == "medicare_ffs:enrollment"
    assert enrollment["query"] == {"dataset_key": "medicare_ffs_public_provider_enrollment"}
    assert enrollment["cache_key"] == "medicare_ffs_public_provider_enrollment"
    assert enrollment["confidence"] == "source_row"
    assert result["metadata"]
    assert result["metadata"][0]["source_modified"] == ""
    assert result["metadata"][0]["source_period"]
    assert result["metadata"][0]["cache_status"] == "ready"
    assert result["metadata"][0]["cache_freshness"]
    assert result["metadata"][0]["entity_scope"]
    assert result["metadata"][0]["cache_key"]
    assert result["evidence"]["dataset_id"] == "medicare_ffs_public_provider_enrollment"
    assert result["evidence"]["match_basis"] == "npi_exact"
    assert result["evidence"]["source_period"]
    assert result["evidence"]["cache_status"] == "ready"
    assert result["evidence"]["cache_freshness"]
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert_provider_source_metadata(result)
    validate_evidence_receipt(enrollment["evidence"], require_content=True)
    assert enrollment["evidence"]["dataset_id"] == "medicare_ffs_public_provider_enrollment"
    assert enrollment["evidence"]["match_basis"] == "cms_provider_enrollment_row"
    assert enrollment["evidence"]["query"]["npi"] == "1234567893"
    assert enrollment["evidence"]["confidence"] == "source_row"
    assert enrollment["source_period"]
    assert enrollment["cache_status"] == "ready"
    assert enrollment["cache_freshness"]
    assert result["identity"]["npi"] == "1234567893"
    identity_map = result["identity_map"]
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}
    assert by_field["npi"]["values"] == ["1234567893"]
    assert by_field["pecos_enrollment_id"]["values"] == ["ENR1"]
    assert "enrollments" in by_field["npi"]["used_by"]
    assert identity_map["source_claims"][0]["match_policy"] == "exact_identifier_required_for_report_fact"
    assert identity_map["source_claims"][0]["row_evidence_paths"] == ["enrollments[].evidence"]
    assert identity_map["conflict_policy"]


@pytest.mark.asyncio
async def test_search_provider_enrollment_no_match_has_zero_result_evidence(provider_cache) -> None:
    result = await server.search_provider_enrollment(npi="1999999984")

    assert result["total_results"] == 0
    assert result["enrollments"] == []
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert result["evidence"]["match_basis"] == "npi_exact_no_match"
    assert result["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
    assert result["evidence"]["query"]["npi"] == "1999999984"
    assert "Verify the identifier" in result["evidence"]["next_step"]
    assert_provider_source_metadata(result)
    assert result["identity"]["npi"] == "1999999984"
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert by_field["npi"]["values"] == ["1999999984"]
    assert by_field["ccn"]["status"] == "missing"
    assert result["identity_map"]["missing_data_policy"].startswith("No-match provider-enrollment responses")


@pytest.mark.asyncio
async def test_get_provider_enrollment_detail_links_owners_and_chow(provider_cache) -> None:
    result = await server.get_provider_enrollment_detail(npi="1234567893")

    assert result["enrollments"][0]["enrollment_id"] == "ENR1"
    assert result["ownership"][0]["owner_name"] == "Jefferson Parent LLC"
    assert result["chow_history"][0]["change_type"] == "Change of ownership"
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["enrollments"][0]["evidence"], require_content=True)
    validate_evidence_receipt(result["ownership"][0]["evidence"], require_content=True)
    validate_evidence_receipt(result["chow_history"][0]["evidence"], require_content=True)
    assert_provider_source_metadata(result)


@pytest.mark.asyncio
async def test_get_provider_enrollment_detail_no_match_has_zero_result_evidence(provider_cache) -> None:
    result = await server.get_provider_enrollment_detail(npi="1999999984")

    assert result["enrollments"] == []
    assert result["ownership"] == []
    assert result["chow_history"] == []
    assert result["evidence"]["match_basis"] == "exact_public_identifier_no_match"
    assert result["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert_provider_source_metadata(result)
    assert result["identity"]["npi"] == "1999999984"


@pytest.mark.asyncio
async def test_get_facility_ownership(provider_cache) -> None:
    result = await server.get_facility_ownership(ccn="390001")

    assert result["total_results"] == 1
    assert result["owners"][0]["owner_associate_id"] == "OWN1"
    assert result["owners"][0]["is_active"] is True
    validate_evidence_receipt(result["owners"][0]["evidence"], require_content=True)
    assert result["owners"][0]["evidence"]["dataset_id"] == "hospital_all_owners"
    assert result["owners"][0]["evidence"]["match_basis"] == "cms_provider_ownership_row"
    assert result["owners"][0]["evidence"]["query"]["owner_associate_id"] == "OWN1"
    assert result["evidence"]["match_basis"] == "ccn_exact"
    assert_provider_source_metadata(result)
    assert result["identity"]["ccn"] == "390001"
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert by_field["ccn"]["values"] == ["390001"]
    assert by_field["pecos_enrollment_id"]["values"] == ["ENR1"]
    assert by_field["owner_id"]["values"] == ["OWN1"]
    assert "owners" in by_field["owner_id"]["used_by"]
    owner_claim = next(claim for claim in result["identity_map"]["source_claims"] if claim["collection"] == "owners")
    assert owner_claim["row_evidence_paths"] == ["owners[].evidence"]


@pytest.mark.asyncio
async def test_get_facility_ownership_no_match_has_zero_result_evidence(provider_cache) -> None:
    result = await server.get_facility_ownership(ccn="390999")

    assert result["total_results"] == 0
    assert result["owners"] == []
    assert result["evidence"]["match_basis"] == "ccn_exact_no_match"
    assert result["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
    assert_provider_source_metadata(result)
    assert result["identity"]["ccn"] == "390999"


@pytest.mark.asyncio
async def test_trace_owner_network(provider_cache) -> None:
    result = await server.trace_owner_network(owner_name="Jefferson Parent", depth=3)

    assert {node["kind"] for node in result["nodes"]} == {"owner", "facility"}
    for node in result["nodes"]:
        validate_evidence_receipt(node["evidence"], require_content=True)
        assert node["evidence"]["dataset_id"] == "hospital_all_owners"
        assert node["evidence"]["match_basis"].startswith("cms_provider_owner_graph_")
        assert node["evidence"]["query"]["graph_node_id"] == node["id"]
        assert node["evidence"]["query"]["graph_node_kind"] == node["kind"]
    assert result["edges"][0]["relationship"] == "owns_or_controls"
    validate_evidence_receipt(result["edges"][0]["evidence"], require_content=True)
    assert result["edges"][0]["evidence"]["dataset_id"] == "hospital_all_owners"
    assert result["edges"][0]["evidence"]["match_basis"] == "cms_provider_owner_graph_edge_row"
    assert result["edges"][0]["evidence"]["query"]["graph_edge_relationship"] == "owns_or_controls"
    assert_provider_source_metadata(result)


@pytest.mark.asyncio
async def test_trace_owner_network_no_match_has_zero_result_evidence(provider_cache) -> None:
    result = await server.trace_owner_network(owner_name="No Such Owner")

    assert result["nodes"] == []
    assert result["edges"] == []
    assert result["evidence"]["match_basis"] == "owner_name_seed_no_match"
    assert result["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
    assert_provider_source_metadata(result)
    assert result["identity"]["canonical_name"] == "NO SUCH OWNER"
    assert result["identity_map"]["source_claims"][0]["collection"] == "owner_network"
    assert result["identity_map"]["source_claims"][0]["row_evidence_paths"] == [
        "nodes[].evidence",
        "edges[].evidence",
    ]


@pytest.mark.asyncio
async def test_trace_owner_network_defaults_invalid_depth(provider_cache) -> None:
    result = await server.trace_owner_network(owner_name="Jefferson Parent", depth="bad")  # type: ignore[arg-type]

    assert result["depth"] == 1
    assert result["nodes"]


@pytest.mark.asyncio
async def test_search_change_of_ownership(provider_cache) -> None:
    result = await server.search_change_of_ownership(ccn="390001", start_date="2026-01-01")

    assert result["total_results"] == 1
    assert result["events"][0]["transaction_date"] == "2026-04-01"
    assert_provider_source_metadata(result)
    validate_evidence_receipt(result["events"][0]["evidence"], require_content=True)
    assert result["events"][0]["evidence"]["dataset_id"] == "hospital_chow"
    assert result["events"][0]["evidence"]["match_basis"] == "cms_provider_chow_row"
    assert result["events"][0]["evidence"]["query"]["transaction_date"] == "2026-04-01"


@pytest.mark.asyncio
async def test_search_change_of_ownership_no_match_has_zero_result_evidence(provider_cache) -> None:
    result = await server.search_change_of_ownership(ccn="390999")

    assert result["total_results"] == 0
    assert result["events"] == []
    assert result["evidence"]["match_basis"] == "ccn_exact_no_match"
    assert result["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
    assert_provider_source_metadata(result)
    assert result["identity"]["ccn"] == "390999"


@pytest.mark.asyncio
async def test_profile_provider_control(provider_cache) -> None:
    result = await server.profile_provider_control(ccn="390001")

    assert result["ownership"][0]["owner_name"] == "Jefferson Parent LLC"
    assert result["join_keys"]["ccn"] == ["390001"]
    assert result["owner_network"]["nodes"]
    validate_evidence_receipt(result["owner_network"]["nodes"][0]["evidence"], require_content=True)
    validate_evidence_receipt(result["owner_network"]["edges"][0]["evidence"], require_content=True)
    assert result["owner_network"]["edges"][0]["evidence"]["match_basis"] == "cms_provider_owner_graph_edge_row"
    validate_evidence_receipt(result["ownership"][0]["evidence"], require_content=True)
    validate_evidence_receipt(result["chow_history"][0]["evidence"], require_content=True)
    assert result["ownership"][0]["evidence"]["match_basis"] == "cms_provider_ownership_row"
    assert result["chow_history"][0]["evidence"]["match_basis"] == "cms_provider_chow_row"
    assert result["evidence"]["match_basis"] == "ccn_exact"
    assert_provider_source_metadata(result)
    assert result["identity"]["ccn"] == "390001"
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert by_field["ccn"]["values"] == ["390001"]
    assert by_field["owner_id"]["values"] == ["OWN1"]
    assert {claim["collection"] for claim in result["identity_map"]["source_claims"]} >= {
        "enrollment",
        "ownership",
        "chow_history",
        "owner_network",
    }
    claims = {claim["collection"]: claim for claim in result["identity_map"]["source_claims"]}
    assert claims["enrollment"]["row_evidence_paths"] == ["enrollment[].evidence"]
    assert claims["ownership"]["row_evidence_paths"] == ["ownership[].evidence"]
    assert claims["chow_history"]["row_evidence_paths"] == ["chow_history[].evidence"]
    assert claims["owner_network"]["row_evidence_paths"] == [
        "owner_network.nodes[].evidence",
        "owner_network.edges[].evidence",
    ]


@pytest.mark.asyncio
async def test_profile_provider_control_no_match_has_zero_result_evidence(provider_cache) -> None:
    result = await server.profile_provider_control(ccn="390999")

    assert result["enrollment"] == []
    assert result["ownership"] == []
    assert result["chow_history"] == []
    assert result["evidence"]["match_basis"] == "ccn_exact_no_match"
    assert result["evidence"]["confidence"] == "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
    assert_provider_source_metadata(result)
    assert result["identity"]["ccn"] == "390999"


@pytest.mark.asyncio
async def test_invalid_npi_returns_structured_error(provider_cache) -> None:
    result = await server.search_provider_enrollment(npi="1111111111")

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
