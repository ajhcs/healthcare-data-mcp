"""NPPES-based outpatient site discovery and taxonomy categorization."""

import logging

from .models import OutpatientSite

logger = logging.getLogger(__name__)

# Taxonomy code prefix -> human-readable category
TAXONOMY_CATEGORIES = {
    "207Q": "Family Medicine",
    "207R": "Internal Medicine",
    "207X": "Orthopedic Surgery",
    "207Y": "Ophthalmology",
    "2084": "Psychiatry",
    "2085": "Radiology",
    "2086": "Surgery",
    "208C": "Cardiology",
    "208D": "Dermatology",
    "208G": "Gastroenterology",
    "208M": "Nephrology",
    "208U": "Neurology",
    "261Q": "Clinic/Center",
    "225": "Rehabilitation",
    "332B": "Pharmacy",
    "332": "Pharmacy",
    "363": "Nurse Practitioner",
    "367": "Physician Assistant",
    "174": "Dentist",
    "122": "Optometrist",
    "111": "Chiropractor",
    "133": "Psychologist",
    "341": "Home Health",
    "281": "Hospital",
    "282": "Hospital",
    "283": "Hospital",
    "291": "Laboratory",
    "302": "Nursing Facility",
    "311": "Hospice",
    "314": "Skilled Nursing",
    "324": "Behavioral Health",
}


def categorize_taxonomy(code: str) -> str:
    """Map a taxonomy code to a human-readable category.

    Checks longest prefix first (4 chars) down to 3 chars.
    """
    code = str(code).strip()
    for length in (4, 3):
        prefix = code[:length]
        if prefix in TAXONOMY_CATEGORIES:
            return TAXONOMY_CATEGORIES[prefix]
    return "Other"


def build_search_patterns(system_name: str, state: str) -> list[dict]:
    """Generate NPPES search patterns from a system name.

    Creates wildcard-friendly search parameters. NPPES API supports
    partial name matching with trailing wildcard behavior.

    Args:
        system_name: Health system name (e.g. "Jefferson Health").
        state: Two-letter state code.

    Returns:
        List of param dicts for NPPES queries.
    """
    patterns = []
    name = system_name.strip()
    for suffix in ["Health System", "Health Network", "Health", "Medical Center", "Medicine"]:
        if name.lower().endswith(suffix.lower()):
            name = name[: -len(suffix)].strip()
            break

    # Primary pattern: the distinctive name part + wildcard
    if name:
        patterns.append({
            "organization_name": f"{name}*",
            "state": state,
            "enumeration_type": "NPI-2",
        })

    # Full system name pattern
    patterns.append({
        "organization_name": f"{system_name.strip()}*",
        "state": state,
        "enumeration_type": "NPI-2",
    })

    return patterns


def parse_nppes_results(raw_results: list[dict]) -> list[OutpatientSite]:
    """Parse NPPES API results into OutpatientSite models.

    Extracts location address, primary taxonomy, and categorizes.
    """
    sites = []
    for r in raw_results:
        basic = r.get("basic", {})
        if basic.get("status", "").upper() != "A":
            continue

        name = basic.get("organization_name", "")
        npi = str(r.get("number", ""))

        # Get location address (not mailing)
        address = ""
        city = ""
        state = ""
        zip_code = ""
        phone = ""
        for addr in r.get("addresses", []):
            if addr.get("address_purpose", "").upper() == "LOCATION":
                address = addr.get("address_1", "")
                city = addr.get("city", "")
                state = addr.get("state", "")
                raw_zip = addr.get("postal_code", "")
                zip_code = raw_zip[:5] if len(raw_zip) >= 5 else raw_zip
                phone = addr.get("telephone_number", "")
                break

        # Get primary taxonomy
        taxonomy_code = ""
        taxonomy_desc = ""
        for tax in r.get("taxonomies", []):
            if tax.get("primary", False):
                taxonomy_code = tax.get("code", "")
                taxonomy_desc = tax.get("desc", "")
                break
        if not taxonomy_code and r.get("taxonomies"):
            taxonomy_code = r["taxonomies"][0].get("code", "")
            taxonomy_desc = r["taxonomies"][0].get("desc", "")

        category = categorize_taxonomy(taxonomy_code)

        sites.append(OutpatientSite(
            npi=npi,
            name=name,
            address=address,
            city=city,
            state=state,
            zip_code=zip_code,
            phone=phone,
            taxonomy_code=taxonomy_code,
            taxonomy_description=taxonomy_desc,
            category=category,
        ))

    return sites
