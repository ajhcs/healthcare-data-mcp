"""Input validation for health data API parameters."""

import re


def validate_facility_id(facility_id: str) -> str:
    """Validate CMS Facility ID (CCN). Must be 6-digit numeric."""
    cleaned = facility_id.strip()
    if not re.match(r"^\d{6}$", cleaned):
        raise ValueError(
            f"Invalid CMS Facility ID: '{facility_id}'. Expected 6-digit numeric (e.g., '390174')."
        )
    return cleaned


def validate_ein(ein: str) -> str:
    """Validate EIN. Must be 9-digit numeric (no dashes)."""
    cleaned = ein.strip().replace("-", "")
    if not re.match(r"^\d{9}$", cleaned):
        raise ValueError(
            f"Invalid EIN: '{ein}'. Expected 9-digit numeric without dashes (e.g., '232829095')."
        )
    return cleaned


def validate_npi(npi: str) -> str:
    """Validate NPI number. Must be 10-digit numeric."""
    cleaned = npi.strip()
    if not re.match(r"^\d{10}$", cleaned):
        raise ValueError(
            f"Invalid NPI: '{npi}'. Expected 10-digit numeric (e.g., '1215916002')."
        )
    return cleaned


def validate_system_key(system_key: str) -> str:
    """Validate system key. Must be alphanumeric with underscores only."""
    cleaned = system_key.strip()
    if not re.match(r"^[a-z][a-z0-9_]{1,50}$", cleaned):
        raise ValueError(
            f"Invalid system key: '{system_key}'. Expected lowercase alphanumeric with underscores."
        )
    return cleaned
