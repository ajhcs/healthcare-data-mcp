"""Shared identity normalization and conservative fuzzy matching helpers."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

from rapidfuzz import fuzz

_US_STATES = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}
_STATE_CODES = set(_US_STATES.values())
_LEGAL_SUFFIXES = {
    "CO",
    "COMPANY",
    "CORP",
    "CORPORATION",
    "INC",
    "INCORPORATED",
    "LLC",
    "LLP",
    "LP",
    "LTD",
    "LIMITED",
    "PA",
    "PC",
    "PLLC",
}
_ADDRESS_ABBREVIATIONS = {
    "AVENUE": "AVE",
    "BOULEVARD": "BLVD",
    "CENTER": "CTR",
    "CIRCLE": "CIR",
    "COURT": "CT",
    "DRIVE": "DR",
    "HIGHWAY": "HWY",
    "LANE": "LN",
    "PARKWAY": "PKWY",
    "PLACE": "PL",
    "ROAD": "RD",
    "SAINT": "ST",
    "SQUARE": "SQ",
    "STREET": "ST",
    "SUITE": "STE",
    "TERRACE": "TER",
}


def normalize_npi(value: Any) -> str | None:
    """Return a valid 10-digit NPI, or ``None`` for invalid/placeholders."""
    digits = _digits(value)
    if len(digits) != 10 or _is_placeholder(digits):
        return None
    return digits if is_valid_npi(digits) else None


def is_valid_npi(value: Any) -> bool:
    """Validate an NPI with the CMS check digit algorithm."""
    digits = _digits(value)
    if len(digits) != 10 or _is_placeholder(digits):
        return False
    return _luhn_check_digit("80840" + digits[:9]) == int(digits[-1])


def normalize_ccn(value: Any) -> str | None:
    """Normalize a CMS Certification Number to its six-character form."""
    token = _alnum(value)
    if not token or len(token) > 6:
        return None
    if token.isdigit():
        token = token.zfill(6)
    if len(token) != 6 or _is_placeholder(token):
        return None
    return token


def normalize_uei(value: Any) -> str | None:
    """Normalize a SAM.gov Unique Entity ID."""
    token = _alnum(value)
    if len(token) != 12 or _is_placeholder(token):
        return None
    return token


def normalize_pac_id(value: Any) -> str | None:
    """Normalize a PECOS PAC ID or owner PAC-style identifier."""
    digits = _digits(value)
    if not digits or _is_placeholder(digits):
        return None
    return digits


def normalize_enrollment_id(value: Any) -> str | None:
    """Normalize a PECOS enrollment identifier for exact joins."""
    token = _alnum(value)
    if not token or _is_placeholder(token):
        return None
    return token


def normalize_name(value: Any, *, remove_legal_suffixes: bool = False) -> str:
    """Normalize organization or person names for indexing and matching."""
    text = _ascii_upper(value)
    text = re.sub(r"['&.,()/+-]", " ", text)
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    parts = [part for part in text.split() if part]
    if remove_legal_suffixes:
        while parts and parts[-1] in _LEGAL_SUFFIXES:
            parts.pop()
    return " ".join(parts)


def normalize_state(value: Any) -> str | None:
    """Normalize a USPS state abbreviation or full state name."""
    text = normalize_name(value)
    if len(text) == 2 and text in _STATE_CODES:
        return text
    return _US_STATES.get(text)


def normalize_zip(value: Any) -> str | None:
    """Normalize ZIP or ZIP+4 input to a five-digit ZIP code."""
    digits = _digits(value)
    if len(digits) < 5:
        return None
    zip5 = digits[:5]
    return None if _is_placeholder(zip5) else zip5


def normalize_address(value: Any) -> str:
    """Normalize a street address without inventing missing components."""
    text = normalize_name(value)
    parts = [_ADDRESS_ABBREVIATIONS.get(part, part) for part in text.split()]
    return " ".join(parts)


def conservative_fuzzy_score(left: Any, right: Any) -> int:
    """Return a conservative 0-100 similarity score for normalized names."""
    left_norm = normalize_name(left, remove_legal_suffixes=True)
    right_norm = normalize_name(right, remove_legal_suffixes=True)
    if not left_norm or not right_norm:
        return 0
    if left_norm == right_norm:
        return 100
    if min(len(left_norm), len(right_norm)) <= 4:
        return 0

    ratio = fuzz.ratio(left_norm, right_norm)
    token_sort = fuzz.token_sort_ratio(left_norm, right_norm)
    score = max(ratio, token_sort)

    # Avoid treating a short contained name as a confident entity match.
    shorter, longer = sorted((left_norm, right_norm), key=len)
    if shorter in longer and len(longer) - len(shorter) >= 5:
        score = min(score, 88)

    return int(round(score))


def conservative_fuzzy_match(left: Any, right: Any, *, threshold: int = 90) -> bool:
    """Return whether two names clear a conservative fuzzy threshold."""
    return conservative_fuzzy_score(left, right) >= threshold


def _digits(value: Any) -> str:
    return re.sub(r"\D+", "", "" if value is None else str(value))


def _alnum(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", "", _ascii_upper(value))


def _ascii_upper(value: Any) -> str:
    text = "" if value is None else str(value)
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", "ignore").decode("ascii").upper()


def _is_placeholder(value: str) -> bool:
    return bool(value) and len(set(value)) == 1 and value[0] in {"0", "9"}


def _luhn_check_digit(body: str) -> int:
    total = 0
    for index, char in enumerate(reversed(body + "0")):
        digit = int(char)
        if index % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return (10 - (total % 10)) % 10
