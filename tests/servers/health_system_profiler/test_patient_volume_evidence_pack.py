"""Patient-volume evidence-pack contract tests."""

from __future__ import annotations

import importlib

import pytest

from servers.health_system_profiler import server
from shared.utils.source_backed_result import validate_source_claim_paths

LAUNCH_SYSTEMS = [
    "christianacare",
    "jefferson-health",
    "temple-health",
    "penn-medicine",
    "cooper-university-health-care",
    "main-line-health",
]


def test_health_system_profiler_imports_patient_volume_evidence_pack_tool() -> None:
    imported = importlib.import_module("servers.health_system_profiler.server")

    assert hasattr(imported, "build_patient_volume_evidence_pack")


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_normalizes_all_required_input_rows() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        required_system_slugs=LAUNCH_SYSTEMS,
        source_rows=[
            *[_zip_demand_row(system_slug=slug, zip_code=f"1910{index}") for index, slug in enumerate(LAUNCH_SYSTEMS)],
            _access_row(),
            _distance_row(),
            _attractiveness_row(),
        ],
    )

    assert result["workflow_id"] == "patient_volume_evidence_pack"
    assert result["public_alpha_metric_keys"] == [
        "geography.primary_service_area",
        "market.effective_local_market_share",
    ]
    assert result["metadata"]["read_only"] is True
    assert "hc-metrics owns PSA, ELMS" in result["metadata"]["formula_policy"]
    assert result["status"] == "source_candidates_ready"
    assert result["source_hierarchy"][0]["source_family"] == "state_all_payer_discharge_zip_origin"
    assert result["denominator_scope_policy"]["public_metric_requirement"].startswith("One approved denominator scope")
    assert result["coverage"]["coverage_tier"] == "complete_all_six"
    assert result["coverage"]["missing_system_slugs"] == []
    assert result["coverage"]["missing_row_types"] == []
    demand = result["patient_volume_input_rows"][0]
    assert demand["status"] == "supported"
    assert demand["value"]["zip_code"] == "19100"
    assert demand["value"]["geography_basis"] == "zip_code"
    assert demand["value"]["zip_demand"] == 1234.0
    assert demand["value"]["denominator_scope"] == "medicare_inpatient"
    assert demand["value"]["confidence_inputs"]["source_rank"] == 2
    assert demand["evidence"]["source_url"] == "https://data.cms.gov/example/hsaf"
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_reports_all_six_and_row_type_blockers() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        required_system_slugs=LAUNCH_SYSTEMS,
        source_rows=[_zip_demand_row(system_slug="christianacare", zip_code="19713")],
    )

    assert result["status"] == "needs_review"
    assert result["coverage"]["coverage_tier"] == "partial_with_blockers"
    assert result["coverage"]["missing_system_slugs"] == [
        "cooper-university-health-care",
        "jefferson-health",
        "main-line-health",
        "penn-medicine",
        "temple-health",
    ]
    assert result["coverage"]["missing_row_types"] == [
        "competitor_access_point",
        "distance_friction",
        "attractiveness_input",
    ]
    assert result["blockers"][0]["status"] == "unavailable_public"
    assert "must not calculate" in result["identity_map"]["conflict_policy"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_no_rows_is_not_yet_researched() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        required_system_slugs=LAUNCH_SYSTEMS,
    )

    assert result["status"] == "not_yet_researched"
    assert result["patient_volume_input_rows"] == []
    assert result["blockers"][0]["status"] == "not_yet_researched"
    assert result["missingness_states"] == [
        "not_yet_researched",
        "unavailable_public",
        "not_applicable",
        "blocked_source_conflict",
    ]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_blocks_mixed_denominator_scopes() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        required_system_slugs=["christianacare"],
        source_rows=[
            _zip_demand_row(system_slug="christianacare", zip_code="19713"),
            {
                **_zip_demand_row(system_slug="christianacare", zip_code="19713"),
                "source_family": "state_all_payer_discharge_zip_origin",
                "denominator_scope": "all_payer_inpatient",
                "source_url": "https://example.org/all-payer",
            },
            _access_row(system_slug="christianacare"),
            _distance_row(system_slug="christianacare"),
            _attractiveness_row(system_slug="christianacare"),
        ],
    )

    assert result["status"] == "blocked_source_conflict"
    assert result["blockers"][0]["status"] == "blocked_source_conflict"
    assert result["blockers"][0]["detail"]["denominator_scopes"] == [
        "all_payer_inpatient",
        "medicare_inpatient",
    ]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_blocks_disallowed_source_scope_pair() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        source_rows=[
            {
                **_zip_demand_row(system_slug="christianacare", zip_code="19713"),
                "source_family": "state_all_payer_discharge_zip_origin",
                "denominator_scope": "medicare_inpatient",
            }
        ],
    )

    row = result["patient_volume_input_rows"][0]
    assert row["status"] == "needs_review"
    assert "denominator_scope_not_allowed_for_source_family" in row["value"]["missing_reasons"]
    assert row["value"]["confidence_inputs"]["allowed_denominator_scopes"] == ["all_payer_inpatient"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_requires_row_type_specific_fields() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        required_system_slugs=["christianacare"],
        source_rows=[
            {
                **_zip_demand_row(system_slug="christianacare", zip_code="19713"),
                "row_type": "distance_friction",
                "zip_demand": "",
                "competitor_id": "000001",
                "competitor_name": "Example Hospital",
                "source_family": "public_facility_and_routing_context",
                "distance_miles": "",
                "friction_basis": "",
            },
            {
                **_zip_demand_row(system_slug="christianacare", zip_code="19713"),
                "row_type": "attractiveness_input",
                "zip_code": "",
                "zip_demand": "",
                "competitor_id": "000001",
                "source_family": "public_facility_and_routing_context",
                "attractiveness": "",
                "attractiveness_basis": "",
            },
        ],
    )

    distance, attractiveness = result["patient_volume_input_rows"]
    assert distance["status"] == "needs_review"
    assert "distance_miles_or_friction_value" in distance["value"]["missing_reasons"]
    assert "friction_basis" in distance["value"]["missing_reasons"]
    assert attractiveness["status"] == "needs_review"
    assert "attractiveness" in attractiveness["value"]["missing_reasons"]
    assert "attractiveness_basis" in attractiveness["value"]["missing_reasons"]
    assert result["coverage"]["missing_row_types"] == [
        "zip_demand",
        "competitor_access_point",
        "distance_friction",
        "attractiveness_input",
    ]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_requires_source_family_and_denominator_scope() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        source_rows=[
            {
                **_zip_demand_row(system_slug="christianacare", zip_code="19713"),
                "source_family": "",
                "denominator_scope": "",
            }
        ],
    )

    row = result["patient_volume_input_rows"][0]
    assert row["status"] == "needs_review"
    assert "source_family" in row["value"]["missing_reasons"]
    assert "denominator_scope" in row["value"]["missing_reasons"]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


@pytest.mark.asyncio
async def test_patient_volume_evidence_pack_blocks_conflicting_zip_demand_values() -> None:
    result = await server.build_patient_volume_evidence_pack(
        region_slug="philadelphia-public-alpha",
        required_system_slugs=["christianacare"],
        source_rows=[
            _zip_demand_row(system_slug="christianacare", zip_code="19713"),
            {
                **_zip_demand_row(system_slug="christianacare", zip_code="19713"),
                "zip_demand": "1,999",
            },
            _access_row(system_slug="christianacare"),
            _distance_row(system_slug="christianacare"),
            _attractiveness_row(system_slug="christianacare"),
        ],
    )

    conflict = result["blockers"][0]
    assert result["status"] == "blocked_source_conflict"
    assert conflict["status"] == "blocked_source_conflict"
    assert conflict["detail"]["reason"] == "conflicting_zip_demand"
    assert conflict["detail"]["zip_demand_values"] == [1234.0, 1999.0]
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


def _zip_demand_row(*, system_slug: str, zip_code: str) -> dict[str, object]:
    return {
        "row_type": "zip_demand",
        "system_slug": system_slug,
        "zip_code": zip_code,
        "year": "2024",
        "source_family": "cms_hospital_service_area_file",
        "source_name": "CMS Hospital Service Area File",
        "dataset_id": "cms_hospital_service_area_file",
        "source_period": "2024",
        "source_url": "https://data.cms.gov/example/hsaf",
        "denominator_scope": "medicare_inpatient",
        "zip_demand": "1,234",
        "bias_notes": "Medicare inpatient only; age and payer bias require Toolkit review.",
    }


def _access_row(system_slug: str = "christianacare") -> dict[str, object]:
    return {
        **_zip_demand_row(system_slug=system_slug, zip_code="19713"),
        "row_type": "competitor_access_point",
        "zip_demand": "",
        "competitor_id": "000001",
        "competitor_name": "Example Hospital",
        "source_family": "public_facility_and_routing_context",
    }


def _distance_row(system_slug: str = "christianacare") -> dict[str, object]:
    return {
        **_access_row(system_slug=system_slug),
        "row_type": "distance_friction",
        "distance_miles": 8.5,
        "friction_basis": "OSRM drive distance from ZCTA centroid",
    }


def _attractiveness_row(system_slug: str = "christianacare") -> dict[str, object]:
    return {
        **_access_row(system_slug=system_slug),
        "row_type": "attractiveness_input",
        "attractiveness": 1.0,
        "attractiveness_basis": "ELMS v1 default pending approved capacity override",
    }
