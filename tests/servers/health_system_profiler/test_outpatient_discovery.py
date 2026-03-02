"""Tests for NPPES-based outpatient site discovery."""

import pytest

from servers.health_system_profiler.outpatient_discovery import (
    categorize_taxonomy,
    build_search_patterns,
    parse_nppes_results,
)


def test_categorize_taxonomy():
    assert categorize_taxonomy("207Q00000X") == "Family Medicine"
    assert categorize_taxonomy("261QP2300X") == "Clinic/Center"
    assert categorize_taxonomy("332B00000X") == "Pharmacy"
    assert categorize_taxonomy("225100000X") == "Rehabilitation"
    assert categorize_taxonomy("999Z00000X") == "Other"


def test_build_search_patterns():
    patterns = build_search_patterns("Jefferson Health", "PA")
    assert len(patterns) >= 1
    assert any("Jefferson" in p["organization_name"] for p in patterns)
    assert all(p.get("state") == "PA" for p in patterns)


def test_build_search_patterns_multi_word():
    patterns = build_search_patterns("Lehigh Valley Health Network", "PA")
    assert len(patterns) >= 1
    assert any("Lehigh Valley" in p["organization_name"] for p in patterns)


def test_parse_nppes_results():
    raw = [
        {
            "number": "1234567890",
            "enumeration_type": "NPI-2",
            "basic": {
                "organization_name": "Jefferson Family Medicine",
                "status": "A",
            },
            "addresses": [
                {
                    "address_purpose": "LOCATION",
                    "address_1": "123 Main St",
                    "city": "Philadelphia",
                    "state": "PA",
                    "postal_code": "191070000",
                    "telephone_number": "215-555-1234",
                }
            ],
            "taxonomies": [
                {
                    "code": "207Q00000X",
                    "desc": "Family Medicine",
                    "primary": True,
                }
            ],
        }
    ]
    sites = parse_nppes_results(raw)
    assert len(sites) == 1
    assert sites[0].npi == "1234567890"
    assert sites[0].name == "Jefferson Family Medicine"
    assert sites[0].city == "Philadelphia"
    assert sites[0].taxonomy_code == "207Q00000X"
    assert sites[0].category == "Family Medicine"


def test_parse_nppes_results_skips_inactive():
    raw = [
        {
            "number": "9999999999",
            "basic": {"organization_name": "Inactive Clinic", "status": "D"},
            "addresses": [],
            "taxonomies": [],
        }
    ]
    sites = parse_nppes_results(raw)
    assert len(sites) == 0
