import json
from pathlib import Path

import jsonschema

from perimeter_registry import Registry


def test_minimal_fixture_has_six_concepts_or_entities_and_four_facilities() -> None:
    registry = Registry.jefferson()

    assert registry.system.name == "Jefferson"
    assert registry.system.legal_entity is False
    assert len(registry.entities) == 5
    assert {facility.ccn for facility in registry.facilities} == {
        "390174",
        "390231",
        "390142",
        "390329",
    }


def test_document_specific_perimeters_are_four_distinct_records() -> None:
    registry = Registry.jefferson()

    assert {item.perimeter_id for item in registry.reporting_perimeters} == {
        "enterprise_fy2025",
        "system_excluding_insurance_fy2025",
        "insurance_fy2025",
        "obligated_group_2025",
    }


def test_relationships_preserve_types_dates_evidence_and_unresolved_start() -> None:
    registry = Registry.jefferson()
    relationships = {item.relationship_id: item for item in registry.relationships}

    assert "affiliate" not in {item.relationship_type for item in registry.relationships}
    assert relationships["jhc_member_of_lvhn"].effective_start == "2024-08-01"
    assert relationships["jhc_member_of_lvhn"].effective_end == "2025-06-29"
    assert relationships["lvhn_merged_into_jhc"].effective_start == "2025-06-30"
    assert relationships["tju_member_of_jhc"].effective_start is None
    assert relationships["tju_member_of_jhc"].status == "current_as_of_source_period"
    assert relationships["tju_member_of_jhc"].evidence_ids == ("fy25_audit",)
    assert relationships["hpp_marketed_as_jhp"].effective_start is None
    assert "lvhn_obligated_group_member" not in relationships
    assert relationships["jhc_controls_abington_operator"].effective_start == "2024-06-30"
    assert (
        relationships["jhc_controls_einstein_montgomery_operator"].relationship_type
        == "controls_operator_of_facility"
    )


def test_evidence_preserves_exact_url_locator_period_and_confidence() -> None:
    audit = Registry.jefferson().evidence("fy25_audit")

    assert audit.url == (
        "https://www.jeffersonhealth.org/content/dam/health2021/documents/financial/"
        "tjuh-financial-statements/tjuh-audited-financial-statements-2025.pdf"
    )
    assert audit.locator == "Note 1 pp7–8; Note 2 pp18–19; pp45–48; Note 13 / MTI"
    assert audit.source_period == "FY ended 2025-06-30"
    assert audit.confidence == "high"


def test_fixture_conforms_to_schema_and_schema_forbids_affiliate() -> None:
    schema_path = Path(__file__).parents[1] / "schema" / "registry.schema.json"
    fixture_path = (
        Path(__file__).parents[1] / "src" / "perimeter_registry" / "data" / "jefferson.json"
    )
    with schema_path.open(encoding="utf-8") as handle:
        schema = json.load(handle)
    with fixture_path.open(encoding="utf-8") as handle:
        fixture = json.load(handle)

    jsonschema.Draft202012Validator(schema).validate(fixture)
    relationship_types = schema["$defs"]["relationship"]["properties"]["relationship_type"]["enum"]
    assert "affiliate" not in relationship_types


def test_exact_ccn_lookup_does_not_infer_a_financial_perimeter() -> None:
    facility = Registry.jefferson().lookup_facility("390174")

    assert facility.name == "Thomas Jefferson University Hospital"
    assert facility.operator_entity_id == "tjuh"
    assert not hasattr(facility, "revenue")
