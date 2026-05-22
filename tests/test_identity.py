from __future__ import annotations

from shared.utils.identity import (
    conservative_fuzzy_match,
    conservative_fuzzy_score,
    is_valid_npi,
    normalize_address,
    normalize_ccn,
    normalize_enrollment_id,
    normalize_name,
    normalize_npi,
    normalize_pac_id,
    normalize_state,
    normalize_uei,
    normalize_zip,
)
from shared.utils.healthcare_identity import (
    coerce_healthcare_identity,
    identity_from_public_record,
    merge_healthcare_identities,
    record_identity_conflict,
)


def test_npi_validation_and_placeholder_rejection() -> None:
    assert normalize_npi("123-456-7893") == "1234567893"
    assert is_valid_npi("1999999984")
    assert normalize_npi("0000000000") is None
    assert normalize_npi("1234567890") is None


def test_provider_identifier_normalization() -> None:
    assert normalize_ccn("3901") == "003901"
    assert normalize_ccn("39a001") == "39A001"
    assert normalize_ccn("000000") is None
    assert normalize_ccn("1234567") is None

    assert normalize_uei(" abcd-1234-ef56 ") == "ABCD1234EF56"
    assert normalize_uei("999999999999") is None
    assert normalize_uei("short") is None

    assert normalize_pac_id(" PAC 123-456 ") == "123456"
    assert normalize_pac_id("000000") is None
    assert normalize_enrollment_id(" en-abc 123 ") == "ENABC123"
    assert normalize_enrollment_id("0000") is None


def test_geographic_and_address_normalization() -> None:
    assert normalize_state("Pennsylvania") == "PA"
    assert normalize_state(" pa ") == "PA"
    assert normalize_state("Not A State") is None
    assert normalize_zip("19104-1234") == "19104"
    assert normalize_zip("00000") is None
    assert normalize_address("123 Saint Mary Street, Suite 4") == "123 ST MARY ST STE 4"


def test_name_normalization() -> None:
    assert normalize_name("  Johns-Hopkins, LLC  ") == "JOHNS HOPKINS LLC"
    assert normalize_name("  Johns-Hopkins, LLC  ", remove_legal_suffixes=True) == "JOHNS HOPKINS"
    assert normalize_name("José A. García") == "JOSE A GARCIA"


def test_conservative_fuzzy_matching() -> None:
    assert conservative_fuzzy_score("Johns Hopkins Hospital", "JOHNS HOPKINS HOSPITAL LLC") == 100
    assert conservative_fuzzy_match("Jefferson Health", "Jefferson Hlth", threshold=85)

    assert conservative_fuzzy_score("Saint Mary", "Saint Mary Regional Medical Center") < 90
    assert not conservative_fuzzy_match("ABCD", "ABCE", threshold=80)
    assert conservative_fuzzy_score("", "Jefferson Health") == 0


def test_healthcare_identity_map_normalizes_public_record_identifiers() -> None:
    identity = identity_from_public_record(
        name="Thomas Jefferson University Hospitals, Inc.",
        entity_type="hospital",
        ccn="390223",
        npi="1234567893",
        pecos_enrollment_id=" en abc 123 ",
        address="111 South 11th Street",
        zip_code="19107-5097",
        source_name="CMS Provider Enrollment",
        source_url="https://data.cms.gov/",
    )

    assert identity.canonical_name == "THOMAS JEFFERSON UNIVERSITY HOSPITALS"
    assert identity.ccn == "390223"
    assert identity.npi == "1234567893"
    assert identity.pecos_enrollment_id == "ENABC123"
    assert identity.address == "111 SOUTH 11TH ST"
    assert identity.zip_code == "19107"
    assert identity.aliases[0].identifier_type == "ccn"

    record_identity_conflict(identity, field="canonical_name", left="Jefferson", right="TJUH", source="fixture")
    assert identity.conflicts[0]["field"] == "canonical_name"


def test_healthcare_identity_map_tracks_unresolved_identifiers() -> None:
    identity = identity_from_public_record(name="Example", ccn="1234567", npi="0000000000")

    assert {"type": "ccn", "value": "1234567"} in identity.unresolved_identifiers
    assert {"type": "npi", "value": "0000000000"} in identity.unresolved_identifiers


def test_healthcare_identity_map_does_not_create_empty_alias_for_blank_seed() -> None:
    identity = identity_from_public_record(source_name="workflow_input")

    assert identity.aliases == []


def test_merge_healthcare_identities_combines_non_conflicting_public_identifiers() -> None:
    seed = identity_from_public_record(
        name="Example Hospital",
        entity_type="hospital",
        ccn="390223",
        source_name="CMS Provider of Services",
        source_url="https://data.cms.gov/provider-of-services",
    )
    enrollment = identity_from_public_record(
        name="Example Regional Hospital",
        entity_type="hospital",
        ccn="390223",
        npi="1234567893",
        pecos_enrollment_id="EN-ABC-123",
        source_name="CMS Provider Enrollment",
        source_url="https://data.cms.gov/provider-enrollment",
    )

    merged = merge_healthcare_identities(seed, enrollment)

    assert seed.npi == ""
    assert merged.ccn == "390223"
    assert merged.npi == "1234567893"
    assert merged.pecos_enrollment_id == "ENABC123"
    assert {alias.source_name for alias in merged.aliases} == {
        "CMS Provider of Services",
        "CMS Provider Enrollment",
    }
    assert any(decision.basis == "conservative_public_identifier_merge" for decision in merged.match_decisions)
    assert merged.conflicts == [
        {
            "field": "canonical_name",
            "left": "EXAMPLE HOSPITAL",
            "right": "EXAMPLE REGIONAL HOSPITAL",
            "source": "CMS Provider Enrollment",
        }
    ]


def test_merge_healthcare_identities_preserves_exact_identifier_conflicts() -> None:
    seed = identity_from_public_record(
        name="Example Hospital",
        ccn="390223",
        npi="1234567893",
        source_name="CMS Hospital General Information",
    )
    conflicting = identity_from_public_record(
        name="Example Hospital",
        ccn="390224",
        npi="1234567893",
        source_name="AHRQ Compendium",
    )

    merged = merge_healthcare_identities(seed.to_dict(), conflicting.to_dict())

    assert merged.ccn == "390223"
    assert {
        "field": "ccn",
        "left": "390223",
        "right": "390224",
        "source": "AHRQ Compendium",
    } in merged.conflicts
    assert coerce_healthcare_identity(merged.to_dict()).ccn == "390223"
