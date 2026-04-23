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
