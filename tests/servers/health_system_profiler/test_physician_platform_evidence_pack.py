"""Physician-platform evidence-pack contract tests."""

from __future__ import annotations

import importlib

import pytest

from servers.health_system_profiler import server
from shared.utils.source_backed_result import validate_source_claim_paths


def test_health_system_profiler_imports_physician_platform_evidence_pack_tool() -> None:
    imported = importlib.import_module("servers.health_system_profiler.server")

    assert hasattr(imported, "build_physician_platform_evidence_pack")


@pytest.mark.asyncio
async def test_physician_platform_evidence_pack_normalizes_rows_and_boundary_receipts() -> None:
    result = await server.build_physician_platform_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        required_definition_bases=["employed"],
        source_rows=[
            {
                "source_family": "official_system_physician_enterprise_disclosure",
                "source_name": "Example Health 2025 Annual Report",
                "dataset_id": "example_health_annual_report",
                "source_period": "FY 2025",
                "source_url": "https://example.org/annual-report",
                "count_value": "1,234",
                "definition_basis": "employed",
                "source_claim_text": "Example Health employs 1,234 physicians.",
                "identity_join_strength": "official_name_match",
                "deduplication_basis": "source_roster_unique",
            }
        ],
    )

    assert result["workflow_id"] == "physician_platform_evidence_pack"
    assert result["public_alpha_metric_key"] == "system.physician_count"
    assert result["metadata"]["read_only"] is True
    assert "profile_metric_values" in result["metadata"]["profile_write_policy"]
    assert result["status"] == "source_candidates_ready"
    assert result["source_hierarchy"][0]["source_family"] == "official_system_physician_enterprise_disclosure"
    assert result["identity_join_policy"]["physician_join"].startswith("Join individual physicians by exact NPI")
    assert result["confidence_inputs"]["source_period"]
    row = result["physician_platform_evidence_rows"][0]
    assert row["status"] == "supported"
    assert row["value"]["count_value"] == 1234
    assert row["value"]["definition_basis"] == "employed"
    assert row["value"]["confidence_inputs"]["source_rank"] == 1
    assert row["evidence"]["source_url"] == "https://example.org/annual-report"
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_physician_platform_evidence_pack_reports_same_basis_conflict() -> None:
    result = await server.build_physician_platform_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        source_rows=[
            _source_row(count_value=1000, source_url="https://example.org/a"),
            _source_row(count_value=1200, source_url="https://example.org/b"),
        ],
    )

    assert result["status"] == "blocked_source_conflict"
    assert result["conflicts"][0]["status"] == "blocked_source_conflict"
    assert result["conflicts"][0]["detail"]["candidate_counts"] == [1000, 1200]
    assert "must not select" in result["identity_map"]["conflict_policy"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_physician_platform_evidence_pack_no_rows_is_not_yet_researched() -> None:
    result = await server.build_physician_platform_evidence_pack(
        system_slug="example-health",
        system_name="Example Health",
        state="PA",
        required_definition_bases=["affiliated"],
    )

    assert result["status"] == "not_yet_researched"
    assert result["physician_platform_evidence_rows"] == []
    assert result["unavailable_public_findings"][0]["status"] == "not_yet_researched"
    assert result["missingness_states"] == [
        "not_yet_researched",
        "unavailable_public",
        "not_applicable",
        "blocked_source_conflict",
    ]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


def _source_row(*, count_value: int, source_url: str) -> dict[str, object]:
    return {
        "source_family": "official_system_physician_enterprise_disclosure",
        "source_name": "Example Health Physician Disclosure",
        "dataset_id": "example_health_physician_disclosure",
        "source_period": "2025",
        "source_url": source_url,
        "count_value": count_value,
        "definition_basis": "employed",
        "identity_join_strength": "official_name_match",
        "deduplication_basis": "source_roster_unique",
    }
