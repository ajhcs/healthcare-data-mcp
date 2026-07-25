import json
from pathlib import Path

import jsonschema

from perimeter_registry import Registry


def test_penn_and_upmc_fixtures_conform_to_schema() -> None:
    root = Path(__file__).parents[1]
    schema = json.loads((root / "schema" / "registry.schema.json").read_text())

    for fixture_name in ("penn", "upmc"):
        fixture = json.loads(
            (root / "src" / "perimeter_registry" / "data" / f"{fixture_name}.json").read_text()
        )
        jsonschema.Draft202012Validator(schema).validate(fixture)


def test_penn_brand_ein_remains_ambiguous() -> None:
    result = Registry.penn().resolve("What is Penn Medicine's EIN?")

    assert result.status == "needs_clarification"
    assert result.identifiers == ()
    assert "not assigned a universal EIN" in result.flags[0]


def test_penn_hup_facility_keeps_operator_ein_and_ccn_distinct() -> None:
    result = Registry.penn().resolve(
        "What CCN should I use for Hospital of the University of Pennsylvania?"
    )

    assert result.entity_ids == ("trustees_upenn",)
    assert result.identifiers == (("EIN", "23-1352685"), ("CCN", "390111"))
    assert "not the enterprise perimeter" in result.flags[0]
    assert "upenn_ein" in result.evidence_ids


def test_doylestown_membership_is_not_back_projected() -> None:
    result = Registry.penn().resolve("As of 2025-03-31, was Doylestown Health part of UPHS?")

    assert result.as_of == "2025-03-31"
    assert result.flags[:2] == (
        "not in the documented perimeter as of 2025-03-31",
        "documented entry date is 2025-04-01",
    )


def test_upmc_enterprise_includes_insurance_but_excludes_pitt() -> None:
    result = Registry.upmc().resolve(
        "For UPMC as a whole, should I include hospitals, Insurance Services, "
        "and the University of Pittsburgh?"
    )

    assert result.options[0].scope_id == "upmc_consolidated_cy2025"
    assert "Insurance Services" in result.options[0].includes
    assert result.options[0].excludes == ("University of Pittsburgh",)


def test_upmc_health_plan_brand_does_not_resolve_to_one_ein() -> None:
    result = Registry.upmc().resolve("What EIN should I use for UPMC Health Plan?")

    assert result.status == "needs_clarification"
    assert result.identifiers == ()
    assert "no universal brand EIN is asserted" in result.flags


def test_upmc_parent_and_combined_hospital_filer_eins_are_not_group_ein() -> None:
    parent = Registry.upmc().resolve(
        "I want UPMC's parent-organization Form 990 for FY2023. Which exact EIN?"
    )
    hospital = Registry.upmc().resolve("Which EIN and CCN apply to UPMC Presbyterian Shadyside?")

    assert parent.identifiers == (("EIN", "25-1423657"),)
    assert hospital.identifiers == (("EIN", "25-0965480"), ("CCN", "390164"))
    assert ("EIN", "20-8295721") not in hospital.identifiers
    assert any("current continuity is not reverified" in flag for flag in hospital.flags)
