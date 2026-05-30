"""Shared public-healthcare identifier normalization for MCP tools."""

from __future__ import annotations

import re
from typing import Any

from shared.utils.mistake_detection import Mistake, detect_name_used_for_exact_id, detect_placeholder

US_STATES = frozenset(
    {
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "PR",
        "VI",
        "GU",
        "AS",
        "MP",
    }
)


def normalize_digits(value: Any) -> str:
    """Return only digit characters from a value."""

    return re.sub(r"\D+", "", str(value or ""))


def normalize_ccn(value: Any) -> tuple[str, Mistake | None]:
    """Normalize a CMS Certification Number and report common caller mistakes."""

    placeholder = detect_placeholder(value, parameter="ccn")
    if placeholder:
        return "", placeholder
    name_mistake = detect_name_used_for_exact_id(value, parameter="ccn", expected="a 6-character CCN")
    if name_mistake:
        return "", name_mistake
    cleaned = str(value or "").strip().upper()
    digits = normalize_digits(cleaned)
    if digits and len(digits) <= 6 and cleaned == digits:
        cleaned = digits.zfill(6)
    if not cleaned or not re.fullmatch(r"[A-Z0-9]{6}", cleaned):
        return cleaned, Mistake(
            "INVALID_IDENTIFIER_FORMAT",
            "ccn must be a 6-character CMS Certification Number.",
            "Use search_facilities with name/state to discover a CCN, then retry the exact lookup.",
            {"parameter": "ccn", "value": str(value or ""), "expected_format": "6 uppercase letters/digits"},
        )
    return cleaned, None


def normalize_npi(value: Any) -> tuple[str, Mistake | None]:
    """Normalize an NPI and report common caller mistakes."""

    placeholder = detect_placeholder(value, parameter="npi")
    if placeholder:
        return "", placeholder
    name_mistake = detect_name_used_for_exact_id(value, parameter="npi", expected="a 10-digit NPI")
    if name_mistake:
        return "", name_mistake
    cleaned = normalize_digits(value)
    if not re.fullmatch(r"\d{10}", cleaned):
        return cleaned, Mistake(
            "INVALID_IDENTIFIER_FORMAT",
            "npi must be exactly 10 digits.",
            "Use an NPI search by organization/name/state before retrying exact NPI screening.",
            {"parameter": "npi", "value": str(value or ""), "expected_format": "10 digits"},
        )
    return cleaned, None


def normalize_state(value: Any) -> tuple[str, Mistake | None]:
    """Normalize a US state or territory abbreviation."""

    placeholder = detect_placeholder(value, parameter="state")
    if placeholder:
        return "", placeholder
    cleaned = str(value or "").strip().upper()
    if not cleaned:
        return "", None
    if cleaned not in US_STATES:
        return cleaned, Mistake(
            "INVALID_IDENTIFIER_FORMAT",
            "state must be a two-letter US state or territory abbreviation.",
            "Use a USPS-style state abbreviation such as PA, CA, NY, or TX.",
            {"parameter": "state", "value": str(value or ""), "available_options": sorted(US_STATES)},
        )
    return cleaned, None


def normalize_zcta(value: Any) -> tuple[str, Mistake | None]:
    """Normalize a ZIP Code Tabulation Area code."""

    placeholder = detect_placeholder(value, parameter="zcta")
    if placeholder:
        return "", placeholder
    cleaned = normalize_digits(value)
    if not re.fullmatch(r"\d{1,5}", cleaned):
        return cleaned, Mistake(
            "INVALID_IDENTIFIER_FORMAT",
            "zcta must contain 1-5 digits.",
            "Use a five-digit ZIP/ZCTA code such as 19104.",
            {"parameter": "zcta", "value": str(value or ""), "expected_format": "5 digits"},
        )
    return cleaned.zfill(5), None


def normalize_fips(value: Any, *, parameter: str = "fips") -> tuple[str, Mistake | None]:
    """Normalize a county/state FIPS code."""

    placeholder = detect_placeholder(value, parameter=parameter)
    if placeholder:
        return "", placeholder
    cleaned = normalize_digits(value)
    if len(cleaned) not in {2, 5}:
        return cleaned, Mistake(
            "INVALID_IDENTIFIER_FORMAT",
            f"{parameter} must be a 2-digit state FIPS or 5-digit county FIPS code.",
            "Use Census FIPS codes, preserving leading zeroes.",
            {"parameter": parameter, "value": str(value or ""), "expected_format": "2 or 5 digits"},
        )
    return cleaned, None


def normalize_catalog_id(value: Any, *, parameter: str = "id") -> tuple[str, Mistake | None]:
    """Normalize dataset, workflow, preset, server, or tool IDs."""

    placeholder = detect_placeholder(value, parameter=parameter)
    if placeholder:
        return "", placeholder
    return str(value or "").strip().lower().replace("_", "-"), None

