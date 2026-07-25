import json
from pathlib import Path

import jsonschema

from perimeter_registry import Registry


def test_adversarial_fixtures_conform_to_schema() -> None:
    root = Path(__file__).parents[1]
    schema = json.loads((root / "schema" / "registry.schema.json").read_text())

    for fixture_name in ("nebraska", "kaiser"):
        fixture = json.loads(
            (root / "src" / "perimeter_registry" / "data" / f"{fixture_name}.json").read_text()
        )
        jsonschema.Draft202012Validator(schema).validate(fixture)


def test_nebraska_clinical_enterprise_keeps_unmc_outside_owned_core() -> None:
    result = Registry.from_fixture("nebraska").resolve(
        "For Nebraska Medicine as a whole, what belongs in the lean clinical enterprise?"
    )

    assert result.options[0].scope_id == "nebraska_medicine_clinical_enterprise_2026"
    assert any("UNMC Physicians" in item for item in result.options[0].includes)
    assert any(
        "University of Nebraska Medical Center" in item for item in result.options[0].excludes
    )


def test_nebraska_facility_operator_identifiers_remain_typed() -> None:
    result = Registry.from_fixture("nebraska").resolve(
        "Which EIN and CCN apply to Bellevue Medical Center?"
    )

    assert result.entity_ids == ("bellevue_medical_center_llc",)
    assert result.identifiers == (("EIN", "20-4305186"), ("CCN", "280132"))
    assert "not the enterprise perimeter" in result.flags[0]


def test_kaiser_brand_ein_remains_ambiguous() -> None:
    result = Registry.from_fixture("kaiser").resolve("What is Kaiser Permanente's EIN?")

    assert result.status == "needs_clarification"
    assert result.identifiers == ()
    assert "not assigned a universal EIN" in result.flags[0]


def test_kaiser_consolidated_result_routes_to_period_specific_perimeter() -> None:
    result = Registry.from_fixture("kaiser").resolve(
        "Can I assign Kaiser Permanente's 2024 operating revenue to one EIN?"
    )

    assert result.options[0].scope_id == "kfhp_h_risant_2024_financial"
    assert result.options[0].amount_usd == 115_750_000_000
    assert "Risant Health" in result.options[0].includes
    assert any("one EIN" in item for item in result.options[0].excludes)


def test_kaiser_unresolved_pmg_ein_is_preserved_without_crashing_form_990_menu() -> None:
    registry = Registry.from_fixture("kaiser")
    medical_group = registry.lookup_entity("Colorado Permanente Medical Group")
    result = registry.resolve("Which Form 990 filer should I use for Kaiser Permanente?")

    assert medical_group.identifiers == ()
    assert result.status == "needs_clarification"
    assert all("Colorado Permanente Medical Group" not in option.label for option in result.options)


def test_kaiser_santa_clara_keeps_entity_ein_and_facility_ccn_distinct() -> None:
    result = Registry.from_fixture("kaiser").resolve(
        "Which EIN and CCN apply to Kaiser Foundation Hospital - Santa Clara?"
    )

    assert result.entity_ids == ("kfh",)
    assert result.identifiers == (("EIN", "94-1105628"), ("CCN", "050071"))


def test_nebraska_parent_ein_does_not_inherit_to_affiliates() -> None:
    result = Registry.from_fixture("nebraska").resolve(
        "Which EIN belongs to the Nebraska Medicine parent, and may UNMC Physicians or UNMC "
        "inherit it?"
    )

    assert result.entity_ids == ("nebraska_medicine_parent",)
    assert result.identifiers == (("EIN", "81-3158267"),)


def test_nebraska_university_ein_stays_on_board_record_not_unmc_campus() -> None:
    registry = Registry.from_fixture("nebraska")
    board = registry.lookup_entity("University of Nebraska Board of Regents")
    unmc = registry.lookup_entity("UNMC")
    result = registry.resolve("What is UNMC EIN?")

    assert board.identifier_pairs() == (("EIN", "47-0049123"),)
    assert unmc.identifiers == ()
    assert result.status == "needs_clarification"
    assert result.identifiers == ()
    assert "no EIN is supported for this exact record" in result.flags[0]
