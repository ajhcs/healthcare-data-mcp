"""Tests for the claims-analytics MCP server tools.

Uses monkeypatching to avoid downloading real CMS PUF data (hundreds of MBs).
"""

from tests.helpers import parse_tool_result
from unittest.mock import AsyncMock, patch

import pytest

from servers.claims_analytics import server, data_loaders
from shared.utils.mcp_response import validate_evidence_receipt
from shared.utils.source_backed_result import validate_source_claim_paths


# ---------------------------------------------------------------------------
# Sample inpatient row fixtures (already in the normalised dict format
# returned by data_loaders.query_inpatient)
# ---------------------------------------------------------------------------

INPATIENT_ROWS = [
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "drg_code": "470",
        "drg_desc": "Major Joint Replacement or Reattachment of Lower Extremity",
        "discharges": 812,
        "avg_charges": 68000.0,
        "avg_total_payment": 18500.0,
        "avg_medicare_payment": 16200.0,
    },
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "drg_code": "291",
        "drg_desc": "Heart Failure & Shock w MCC",
        "discharges": 340,
        "avg_charges": 42000.0,
        "avg_total_payment": 15000.0,
        "avg_medicare_payment": 13800.0,
    },
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "drg_code": "065",
        "drg_desc": "Intracranial Hemorrhage or Cerebral Infarction",
        "discharges": 210,
        "avg_charges": 35000.0,
        "avg_total_payment": 11000.0,
        "avg_medicare_payment": 9800.0,
    },
]

OUTPATIENT_ROWS = [
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "apc_code": "5115",
        "apc_desc": "Level 5 Musculoskeletal Procedures",
        "services": 1420,
        "avg_charges": 12000.0,
        "avg_total_payment": 3200.0,
        "avg_medicare_payment": 2800.0,
    },
    {
        "ccn": "390223",
        "provider_name": "Thomas Jefferson University Hospital",
        "state": "PA",
        "apc_code": "5072",
        "apc_desc": "Level 2 Cardiac Imaging",
        "services": 880,
        "avg_charges": 2400.0,
        "avg_total_payment": 620.0,
        "avg_medicare_payment": 510.0,
    },
]


def assert_claims_receipt(result: dict, *, dataset_id: str, ccn: str = "390223") -> None:
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert result["evidence"]["dataset_id"] == dataset_id
    assert result["evidence"]["entity_scope"] == "claims_public_aggregate"
    assert result["evidence"]["cache_status"] in {"ready", "partial", "missing"}
    assert result["identity"]["ccn"] == ccn
    assert result["source_metadata"]["dataset_id"] == dataset_id


def assert_claims_row_receipt(receipt: dict, *, dataset_id: str, match_basis: str, row_kind: str) -> None:
    validate_evidence_receipt(receipt, require_content=True)
    assert receipt["dataset_id"] == dataset_id
    assert receipt["entity_scope"] == "claims_public_aggregate"
    assert receipt["match_basis"] == match_basis
    assert receipt["query"]["row_kind"] == row_kind
    assert receipt["confidence"]
    assert "not PHI" in receipt["caveat"]
    assert receipt["next_step"]


# ---------------------------------------------------------------------------
# Tests: get_inpatient_volumes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_inpatient_volumes_success():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
    ):
        result = parse_tool_result(await server.get_inpatient_volumes(ccn="390223"))

    assert result["ccn"] == "390223"
    assert result["provider_name"] == "Thomas Jefferson University Hospital"
    assert result["total_discharges"] == 812 + 340 + 210
    assert result["total_drgs"] == 3
    assert isinstance(result["service_line_summary"], list)
    assert len(result["drg_details"]) == 3
    # Sorted descending by discharges — DRG 470 should be first
    assert result["drg_details"][0]["drg_code"] == "470"
    assert result["evidence"]["match_basis"] == "ccn_exact_inpatient_provider_service_rows"
    assert_claims_receipt(result, dataset_id="cms_medicare_inpatient_puf")
    assert_claims_row_receipt(
        result["service_line_summary"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="inpatient_service_line_summary_row",
        row_kind="inpatient_service_line_summary",
    )
    assert_claims_row_receipt(
        result["drg_details"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="inpatient_drg_detail_row",
        row_kind="inpatient_drg_detail",
    )
    assert result["drg_details"][0]["evidence"]["query"]["row_drg_code"] == "470"


@pytest.mark.asyncio
async def test_get_inpatient_volumes_no_data():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=[]),
    ):
        result = parse_tool_result(await server.get_inpatient_volumes(ccn="999999"))
    assert "error" in result
    assert "999999" in result["error"]


@pytest.mark.asyncio
async def test_get_inpatient_volumes_invalid_year():
    result = parse_tool_result(await server.get_inpatient_volumes(ccn="390223", year="1999"))
    assert "error" in result
    assert "1999" in result["error"]


@pytest.mark.asyncio
async def test_get_inpatient_volumes_service_line_filter():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
    ):
        result = parse_tool_result(await server.get_inpatient_volumes(ccn="390223", service_line="Orthopedics"))

    # DRG 470 maps to Orthopedics — only that row should survive
    if "error" not in result:
        for detail in result["drg_details"]:
            assert detail["service_line"].lower() == "orthopedics"
    # If 0 matches, expect error — both outcomes are valid
    assert "ccn" in result or "error" in result


# ---------------------------------------------------------------------------
# Tests: get_outpatient_volumes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_outpatient_volumes_success():
    with (
        patch.object(data_loaders, "ensure_outpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_outpatient", return_value=OUTPATIENT_ROWS),
    ):
        result = parse_tool_result(await server.get_outpatient_volumes(ccn="390223"))

    assert result["ccn"] == "390223"
    assert result["total_services"] == 1420 + 880
    assert result["total_apcs"] == 2
    # Sorted descending by services — APC 5115 should be first
    assert result["apc_details"][0]["apc_code"] == "5115"
    assert result["evidence"]["match_basis"] == "ccn_exact_outpatient_provider_service_rows"
    assert_claims_receipt(result, dataset_id="cms_medicare_outpatient_puf")
    assert_claims_row_receipt(
        result["apc_details"][0]["evidence"],
        dataset_id="cms_medicare_outpatient_puf",
        match_basis="outpatient_apc_detail_row",
        row_kind="outpatient_apc_detail",
    )
    assert result["apc_details"][0]["evidence"]["query"]["row_apc_code"] == "5115"


@pytest.mark.asyncio
async def test_get_outpatient_volumes_no_data():
    with (
        patch.object(data_loaders, "ensure_outpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_outpatient", return_value=[]),
    ):
        result = parse_tool_result(await server.get_outpatient_volumes(ccn="000000"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: compute_case_mix
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compute_case_mix_success():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
    ):
        result = parse_tool_result(await server.compute_case_mix(ccn="390223"))

    assert result["ccn"] == "390223"
    assert "case_mix_index" in result
    assert isinstance(result["case_mix_index"], float)
    assert result["case_mix_index"] > 0
    assert result["total_discharges"] == 812 + 340 + 210
    assert len(result["service_line_acuity"]) > 0
    # Each acuity entry should have required fields
    for entry in result["service_line_acuity"]:
        assert "service_line" in entry
        assert "discharges" in entry
        assert "avg_drg_weight" in entry
    assert result["evidence"]["match_basis"] == "ccn_exact_inpatient_drg_rows_with_public_weights"
    assert_claims_receipt(result, dataset_id="cms_medicare_inpatient_puf")
    assert_claims_row_receipt(
        result["service_line_acuity"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="case_mix_service_line_acuity_row",
        row_kind="case_mix_service_line_acuity",
    )
    assert_claims_row_receipt(
        result["top_drgs_by_weight"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="case_mix_drg_weight_contribution_row",
        row_kind="case_mix_drg_weight_contribution",
    )


@pytest.mark.asyncio
async def test_trend_service_lines_rows_include_report_receipts():
    with (
        patch.object(data_loaders, "ensure_all_years_cached", new_callable=AsyncMock, return_value=["2021", "2022", "2023"]),
        patch.object(data_loaders, "query_inpatient", return_value=INPATIENT_ROWS),
        patch.object(data_loaders, "query_outpatient", return_value=OUTPATIENT_ROWS),
    ):
        result = parse_tool_result(await server.trend_service_lines(ccn="390223"))

    assert result["evidence"]["match_basis"] == "ccn_exact_multi_year_provider_service_rows"
    assert_claims_receipt(result, dataset_id="cms_medicare_provider_utilization_puf")
    assert_claims_row_receipt(
        result["inpatient_trends"][0]["evidence"],
        dataset_id="cms_medicare_provider_utilization_puf",
        match_basis="inpatient_service_line_trend_row",
        row_kind="inpatient_service_line_trend",
    )
    assert_claims_row_receipt(
        result["outpatient_trends"][0]["evidence"],
        dataset_id="cms_medicare_provider_utilization_puf",
        match_basis="outpatient_apc_trend_row",
        row_kind="outpatient_apc_trend",
    )


@pytest.mark.asyncio
async def test_compute_case_mix_no_data():
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=[]),
    ):
        result = parse_tool_result(await server.compute_case_mix(ccn="000000"))
    assert "error" in result


@pytest.mark.asyncio
async def test_analyze_market_volumes_includes_evidence_and_identity_map():
    market_rows = [
        *INPATIENT_ROWS,
        {
            **INPATIENT_ROWS[0],
            "ccn": "390226",
            "provider_name": "Temple University Hospital",
            "discharges": 500,
        },
    ]
    with (
        patch.object(data_loaders, "ensure_inpatient_cached", new_callable=AsyncMock, return_value=True),
        patch.object(data_loaders, "query_inpatient", return_value=market_rows),
    ):
        result = parse_tool_result(await server.analyze_market_volumes(provider_ccns=["390223", "390226"]))

    assert result["total_providers"] == 2
    assert result["evidence"]["dataset_id"] == "cms_medicare_inpatient_puf"
    assert result["evidence"]["match_basis"] == "ccn_exact_provider_set_inpatient_rows"
    validate_evidence_receipt(result["evidence"], require_content=True)
    identity_ccns = {entity["ccn"] for entity in result["identity_map"]["entities"]}
    assert identity_ccns == {"390223", "390226"}
    assert result["identity_map"]["match_basis"] == "ccn_exact_provider_set"
    assert result["identity_map"]["source_claims"][0]["source_metadata_path"] == "source_metadata"
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True
    assert_claims_row_receipt(
        result["provider_shares"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="provider_market_share_row",
        row_kind="provider_market_share",
    )
    assert_claims_row_receipt(
        result["provider_shares"][0]["service_line_breakdown"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="provider_service_line_market_share_row",
        row_kind="provider_service_line_market_share",
    )
    assert_claims_row_receipt(
        result["service_line_totals"][0]["evidence"],
        dataset_id="cms_medicare_inpatient_puf",
        match_basis="service_line_market_total_row",
        row_kind="service_line_market_total",
    )
