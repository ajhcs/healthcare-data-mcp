"""Payer discovery evidence-pack contract tests."""

from __future__ import annotations

from servers.health_system_profiler.payer_discovery_evidence_pack import build_payer_discovery_evidence_pack


def _row(payer_type: str, family: str) -> dict[str, object]:
    return {"payer_type": payer_type, "source_family": family, "source_period": "2024", "source_url": "https://data.cms.gov/example", "denominator_value": 100, "source_row_id": payer_type}


def test_official_ma_part_d_marketplace_candidates_have_explicit_toc_and_coverage() -> None:
    result = build_payer_discovery_evidence_pack(
        system_slug="example-health", system_name="Example Health", state="PA",
        required_payer_types=["medicare_advantage", "medicare_part_d", "marketplace"],
        source_rows=[_row("medicare_advantage", "cms_ma_state_county_enrollment"), _row("medicare_part_d", "cms_part_d_state_county_enrollment"), _row("marketplace", "cms_marketplace_effectuated_enrollment")],
    )
    assert result["status"] == "source_candidates_ready"
    assert result["coverage"]["coverage_state"] == "complete"
    assert result["coverage"]["missing_payer_types"] == []
    assert {row["value"]["type_of_coverage"] for row in result["payer_coverage_evidence_rows"]} == {"medicare_advantage", "medicare_part_d", "marketplace"}


def test_census_population_is_blocked_as_payer_denominator() -> None:
    result = build_payer_discovery_evidence_pack(system_slug="example-health", system_name="Example Health", source_rows=[_row("marketplace", "census_population")])
    assert result["status"] == "blocked_source_conflict"
    assert result["payer_coverage_evidence_rows"][0]["status"] == "needs_review"
    assert "census_not_payer_denominator" in result["payer_coverage_evidence_rows"][0]["value"]["missing_reasons"]


def test_acs_insurance_is_also_rejected_and_never_reported_as_supported() -> None:
    result = build_payer_discovery_evidence_pack(
        system_slug="example-health", system_name="Example Health",
        source_rows=[_row("medicare_part_d", "acs_population")],
    )
    row = result["payer_coverage_evidence_rows"][0]
    assert result["status"] == "blocked_source_conflict"
    assert row["status"] == "needs_review"
    assert row["value"]["denominator_value"] == 100
    assert "census_not_payer_denominator" in row["value"]["missing_reasons"]


def test_empty_rows_are_not_yet_researched_and_missing_types_are_unresolved() -> None:
    result = build_payer_discovery_evidence_pack(system_slug="example-health", system_name="Example Health", required_payer_types=["medicare_advantage"])
    assert result["status"] == "not_yet_researched"
    assert result["coverage"]["coverage_state"] == "not_evaluated"
    assert result["blockers"][0]["status"] == "not_yet_researched"
