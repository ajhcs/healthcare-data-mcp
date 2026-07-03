"""Composite source-input evidence-pack contract tests."""

from __future__ import annotations

import importlib

import pytest

from servers.health_system_profiler import server
from shared.utils.source_backed_result import validate_source_claim_paths


def test_health_system_profiler_imports_composite_source_input_evidence_pack_tool() -> None:
    imported = importlib.import_module("servers.health_system_profiler.server")

    assert hasattr(imported, "build_composite_source_input_evidence_pack")


@pytest.mark.asyncio
async def test_composite_source_input_evidence_pack_normalizes_fsi_and_scale_rows() -> None:
    result = await server.build_composite_source_input_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        source_rows=[*_fsi_rows(), *_scale_rows()],
    )

    assert result["workflow_id"] == "composite_source_input_evidence_pack"
    assert result["public_alpha_metric_keys"] == [
        "finance.ushso_financial_strength_index",
        "system.health_system_scale_score",
    ]
    assert result["metadata"]["read_only"] is True
    assert "Toolkit owns FSI and Scale Score formulas" in result["metadata"]["formula_policy"]
    assert result["status"] == "source_candidates_ready"
    assert result["source_hierarchy"][0]["retrieval_owner"] == "financial-intelligence"
    assert result["identity_join_policy"]["peer_join"].startswith("Join peer benchmark rows")
    assert result["confidence_inputs"]["source_period"]
    assert result["coverage"]["coverage_tier"] == "complete_required_fields"
    assert result["coverage"]["missing_fields_by_metric"]["finance.ushso_financial_strength_index"] == []
    assert result["coverage"]["missing_fields_by_metric"]["system.health_system_scale_score"] == []

    revenue = result["composite_source_input_rows"][0]
    assert revenue["status"] == "supported"
    assert revenue["metric_key"] == "finance.ushso_financial_strength_index"
    assert revenue["field"] == "total_operating_revenue_usd"
    assert revenue["value"]["input_value"] == 1200000000
    assert revenue["value"]["retrieval_owner"] == "financial-intelligence"
    assert revenue["value"]["confidence_inputs"]["source_rank"] == 1
    assert revenue["evidence"]["source_url"] == "https://example.org/audited-financials"
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_composite_source_input_evidence_pack_reports_missing_required_fields() -> None:
    result = await server.build_composite_source_input_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        required_metric_keys=["finance.ushso_financial_strength_index"],
        source_rows=[_financial_row("total_operating_revenue_usd", 1200000000)],
    )

    assert result["status"] == "needs_review"
    assert result["coverage"]["coverage_tier"] == "partial_with_blockers"
    assert "operating_margin_pct" in result["coverage"]["missing_fields_by_metric"]["finance.ushso_financial_strength_index"]
    assert result["blockers"][0]["status"] == "unavailable_public"
    assert "MCP must not calculate FSI or Scale Score" in result["identity_map"]["conflict_policy"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_composite_source_input_evidence_pack_no_rows_is_not_yet_researched() -> None:
    result = await server.build_composite_source_input_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
    )

    assert result["status"] == "not_yet_researched"
    assert result["composite_source_input_rows"] == []
    assert result["blockers"][0]["status"] == "not_yet_researched"
    assert result["missingness_states"] == [
        "not_yet_researched",
        "unavailable_public",
        "not_applicable",
        "blocked_source_conflict",
    ]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_composite_source_input_evidence_pack_blocks_same_period_value_conflict() -> None:
    result = await server.build_composite_source_input_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        required_metric_keys=["finance.ushso_financial_strength_index"],
        source_rows=[
            _financial_row("total_operating_revenue_usd", 1200000000),
            {**_financial_row("total_operating_revenue_usd", 1300000000), "source_row_id": "row-2"},
        ],
    )

    assert result["status"] == "blocked_source_conflict"
    assert result["conflicts"][0]["status"] == "blocked_source_conflict"
    assert result["conflicts"][0]["detail"]["input_field"] == "total_operating_revenue_usd"
    assert result["conflicts"][0]["detail"]["candidate_values"] == ["1200000000", "1300000000"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_composite_source_input_evidence_pack_requires_source_row_receipts_and_allowed_source_field_pair() -> None:
    result = await server.build_composite_source_input_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        source_rows=[
            {
                **_financial_row("total_operating_revenue_usd", 1200000000),
                "source_family": "approved_profile_facility_roster",
                "source_name": "",
                "dataset_id": "",
                "source_period": "",
                "source_url": "",
                "identity_join_strength": "",
            }
        ],
    )

    row = result["composite_source_input_rows"][0]
    assert row["status"] == "needs_review"
    assert "input_field_not_allowed_for_source_family" in row["value"]["missing_reasons"]
    assert "source_name" in row["value"]["missing_reasons"]
    assert "dataset_id" in row["value"]["missing_reasons"]
    assert "source_period" in row["value"]["missing_reasons"]
    assert "source_url_or_landing_page" in row["value"]["missing_reasons"]
    assert "identity_join_strength" in row["value"]["missing_reasons"]
    assert row["value"]["confidence_inputs"]["row_receipt_complete"] is False
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


def _financial_row(input_field: str, value: int | float) -> dict[str, object]:
    return {
        "metric_key": "finance.ushso_financial_strength_index",
        "input_field": input_field,
        "value": value,
        "unit": "USD" if input_field.endswith("_usd") else "ratio",
        "source_family": "audited_consolidated_financial_statement",
        "source_name": "Example Health Audited Financial Statements",
        "dataset_id": "example_health_audited_financials",
        "source_period": "FY 2025",
        "source_url": "https://example.org/audited-financials",
        "source_row_id": f"financial-{input_field}",
        "definition_basis": "audited_consolidated",
        "identity_join_strength": "exact_ein",
        "identity_join_keys": {"ein": "12-3456789"},
    }


def _peer_row(input_field: str, value: int | float) -> dict[str, object]:
    return {
        "metric_key": "finance.ushso_financial_strength_index",
        "input_field": input_field,
        "value": value,
        "unit": "ratio",
        "source_family": "approved_public_peer_benchmark_packet",
        "source_name": "Public Alpha Peer Benchmark Packet",
        "dataset_id": "public_alpha_peer_benchmarks",
        "source_period": "FY 2025",
        "source_url": "https://example.org/peer-benchmarks",
        "source_row_id": f"peer-{input_field}",
        "definition_basis": "public_alpha_peer_class",
        "identity_join_strength": "benchmark_class_match",
        "identity_join_keys": {"benchmark_class": "large_regional_nonprofit"},
    }


def _scale_row(input_field: str, value: int | float, source_family: str, owner_hint: str) -> dict[str, object]:
    return {
        "metric_key": "system.health_system_scale_score",
        "input_field": input_field,
        "value": value,
        "unit": "count",
        "source_family": source_family,
        "source_name": f"Example {owner_hint} Source",
        "dataset_id": f"example_{owner_hint}_source",
        "source_period": "FY 2025",
        "source_url": f"https://example.org/{owner_hint}",
        "source_row_id": f"scale-{input_field}",
        "definition_basis": owner_hint,
        "identity_join_strength": "approved_roster_match",
        "identity_join_keys": {"system_slug": "example-health"},
    }


def _fsi_rows() -> list[dict[str, object]]:
    return [
        _financial_row("total_operating_revenue_usd", 1200000000),
        _financial_row("operating_margin_pct", 3.5),
        _financial_row("days_cash_on_hand", 180),
        _financial_row("debt_to_capitalization_pct", 42),
        _financial_row("cash_to_debt_ratio", 1.4),
        _financial_row("net_assets_usd", 700000000),
        _peer_row("peer_operating_margin_pct", 2.5),
        _peer_row("peer_days_cash_on_hand", 150),
        _peer_row("peer_debt_to_capitalization_pct", 45),
        _peer_row("peer_cash_to_debt_ratio", 1.1),
    ]


def _scale_rows() -> list[dict[str, object]]:
    return [
        _scale_row("operating_revenue_usd", 1200000000, "audited_consolidated_financial_statement", "audited_financials"),
        _scale_row("hospital_count", 4, "approved_profile_facility_roster", "profile_roster"),
        _scale_row("bed_count", 900, "approved_profile_facility_roster", "profile_roster"),
        _scale_row("annual_discharges", 45000, "public_utilization_or_claims_context", "utilization"),
        _scale_row("physician_count", 1200, "physician_platform_evidence_pack", "physician_platform"),
        _scale_row("service_line_count", 16, "public_utilization_or_claims_context", "service_line"),
        _scale_row("safety_net_patient_mix_pct", 22.5, "public_safety_net_or_community_benefit_source", "safety_net"),
        _scale_row("emergency_department_count", 4, "approved_profile_facility_roster", "emergency_access"),
        _scale_row("essential_service_designation_count", 3, "public_records_or_web_intelligence_essentiality_source", "essentiality"),
    ]
