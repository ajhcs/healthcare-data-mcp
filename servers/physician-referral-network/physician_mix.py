"""Physician employment mix analysis.

Classifies physicians as employed, affiliated, or independent relative to
a health system by cross-referencing NPPES, AHRQ Compendium, and CMS POS data.
"""

import logging

import httpx
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"


# ---------------------------------------------------------------------------
# NPPES physician search (NPI-1 individuals)
# ---------------------------------------------------------------------------

async def _search_nppes_physicians(
    organization_name: str = "",
    state: str = "",
    taxonomy: str = "",
    limit: int = 200,
) -> list[dict]:
    """Search NPPES for individual physicians."""
    params: dict = {
        "version": "2.1",
        "enumeration_type": "NPI-1",
        "limit": min(limit, 200),
    }
    if organization_name:
        params["organization_name"] = organization_name
    if state:
        params["state"] = state.upper()
    if taxonomy:
        params["taxonomy_description"] = taxonomy

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(NPPES_API_URL, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def _extract_physician_info(npi_result: dict) -> dict:
    """Extract key fields from an NPPES result for classification."""
    basic = npi_result.get("basic", {})
    taxonomies = npi_result.get("taxonomies", [])
    addresses = npi_result.get("addresses", [])

    practice_addr = next(
        (a for a in addresses if a.get("address_purpose") == "LOCATION"),
        addresses[0] if addresses else {},
    )
    primary_tax = next(
        (t for t in taxonomies if t.get("primary")),
        taxonomies[0] if taxonomies else {},
    )

    return {
        "npi": npi_result.get("number", ""),
        "first_name": basic.get("first_name", ""),
        "last_name": basic.get("last_name", ""),
        "org_name": basic.get("organization_name", ""),
        "specialty": primary_tax.get("desc", ""),
        "practice_address": practice_addr.get("address_1", ""),
        "practice_city": practice_addr.get("city", ""),
        "practice_state": practice_addr.get("state", ""),
        "practice_zip": (practice_addr.get("postal_code") or "")[:5],
    }


def classify_physician(
    physician: dict,
    system_name: str,
    facility_addresses: list[dict],
    facility_zips: set[str],
) -> dict:
    """Classify a single physician relative to a health system.

    Args:
        physician: Dict from _extract_physician_info().
        system_name: Health system name.
        facility_addresses: List of {address, city, state, zip} for system facilities.
        facility_zips: Set of ZIP codes where system has facilities.

    Returns:
        Dict with status, confidence, evidence.
    """
    evidence = []
    status = "independent"
    confidence = 0.3

    org_name = physician.get("org_name", "")
    practice_zip = physician.get("practice_zip", "")
    practice_city = physician.get("practice_city", "")

    # Check 1: Organization name matches system name (fuzzy)
    if org_name:
        name_score = fuzz.token_set_ratio(org_name.lower(), system_name.lower())
        if name_score >= 80:
            status = "affiliated"
            confidence = 0.7 + (name_score - 80) * 0.01  # 0.7-0.9
            evidence.append(f"Org name '{org_name}' matches system (score={name_score})")

    # Check 2: Practice address matches a facility address
    for facility in facility_addresses:
        if (
            practice_zip == facility.get("zip", "")
            and practice_city.lower() == facility.get("city", "").lower()
        ):
            # Same ZIP + city as a system facility
            if status == "affiliated":
                status = "employed"
                confidence = min(confidence + 0.15, 0.95)
                evidence.append(
                    f"Practice ZIP {practice_zip} matches facility in {facility.get('city', '')}"
                )
            else:
                status = "affiliated"
                confidence = 0.6
                evidence.append(
                    f"Practice in same ZIP as facility ({practice_zip})"
                )
            break

    # Check 3: Practice in same general area (facility ZIP set)
    if status == "independent" and practice_zip in facility_zips:
        confidence = 0.4
        evidence.append(f"Practice ZIP {practice_zip} is near system facilities")

    if not evidence:
        evidence.append("No organizational or geographic match found")

    return {
        "npi": physician["npi"],
        "name": f"{physician.get('first_name', '')} {physician.get('last_name', '')}".strip(),
        "specialty": physician.get("specialty", ""),
        "status": status,
        "confidence": round(confidence, 2),
        "evidence": evidence,
    }


# ---------------------------------------------------------------------------
# System-level analysis
# ---------------------------------------------------------------------------

async def analyze_system_mix(
    system_name: str,
    state: str = "",
) -> dict:
    """Analyze physician employment mix for a health system.

    Queries NPPES for physicians associated with the system name,
    cross-references with AHRQ facility data, and classifies each.

    Args:
        system_name: Health system name (e.g. "Penn Medicine").
        state: State filter.

    Returns:
        Dict with employed/affiliated/independent counts and percentages.
    """
    # Load facility data from shared AHRQ data loaders
    try:
        from shared.utils.ahrq_data import (
            load_ahrq_hospital_linkage,
            load_ahrq_systems,
            load_pos,
        )
    except ImportError:
        return {"error": "shared AHRQ data loaders not available (shared/utils/ahrq_data.py)"}

    # Find system in AHRQ Compendium
    systems_df = await load_ahrq_systems()
    name_col = "health_sys_name"
    if name_col not in systems_df.columns:
        return {"error": "AHRQ system data missing health_sys_name column"}

    matches = systems_df[
        systems_df[name_col].str.lower().str.contains(system_name.lower(), na=False)
    ]

    if matches.empty:
        return {"error": f"System '{system_name}' not found in AHRQ Compendium"}

    system_id = matches.iloc[0].get("health_sys_id", "")
    resolved_name = matches.iloc[0].get(name_col, system_name)

    # Get system's hospitals from linkage file
    linkage_df = await load_ahrq_hospital_linkage()
    system_hospitals = linkage_df[linkage_df.get("health_sys_id", linkage_df.columns[0]) == system_id]

    # Get facility addresses from POS
    facility_addresses: list[dict] = []
    facility_zips: set[str] = set()

    if not system_hospitals.empty:
        pos_df = await load_pos()
        for _, hosp in system_hospitals.iterrows():
            ccn = str(hosp.get("ccn", "")).strip().zfill(6)
            # Look up in POS
            pos_match = pos_df[pos_df.iloc[:, 0].astype(str).str.strip().str.zfill(6) == ccn]
            if not pos_match.empty:
                pos_row = pos_match.iloc[0]
                addr = {
                    "city": str(pos_row.get("CITY", pos_row.get("city", ""))),
                    "state": str(pos_row.get("STATE", pos_row.get("state", ""))),
                    "zip": str(pos_row.get("ZIP_CD", pos_row.get("zip", "")))[:5],
                }
                facility_addresses.append(addr)
                if addr["zip"]:
                    facility_zips.add(addr["zip"])

    # Search NPPES for physicians matching system name
    physicians_raw = await _search_nppes_physicians(
        organization_name=system_name,
        state=state,
        limit=200,
    )

    # Also search with resolved AHRQ name if different
    if resolved_name.lower() != system_name.lower():
        more = await _search_nppes_physicians(
            organization_name=resolved_name,
            state=state,
            limit=200,
        )
        seen_npis = {r.get("number") for r in physicians_raw}
        for r in more:
            if r.get("number") not in seen_npis:
                physicians_raw.append(r)

    # Classify each physician
    classifications = []
    for raw in physicians_raw:
        info = _extract_physician_info(raw)
        if state and info.get("practice_state", "").upper() != state.upper():
            continue
        result = classify_physician(info, system_name, facility_addresses, facility_zips)
        classifications.append(result)

    # Aggregate
    employed = sum(1 for c in classifications if c["status"] == "employed")
    affiliated = sum(1 for c in classifications if c["status"] == "affiliated")
    independent = sum(1 for c in classifications if c["status"] == "independent")
    total = len(classifications)

    # Specialty breakdown
    specialty_counts: dict[str, dict[str, int]] = {}
    for c in classifications:
        spec = c.get("specialty", "Other") or "Other"
        if spec not in specialty_counts:
            specialty_counts[spec] = {"employed": 0, "affiliated": 0, "independent": 0}
        specialty_counts[spec][c["status"]] += 1

    by_specialty = [
        {"specialty": k, **v, "total": sum(v.values())}
        for k, v in sorted(specialty_counts.items(), key=lambda x: -sum(x[1].values()))
    ]

    return {
        "system_name": resolved_name,
        "total_physicians": total,
        "employed": employed,
        "affiliated": affiliated,
        "independent": independent,
        "employed_pct": round(employed / total * 100, 1) if total else 0,
        "affiliated_pct": round(affiliated / total * 100, 1) if total else 0,
        "independent_pct": round(independent / total * 100, 1) if total else 0,
        "by_specialty": by_specialty,
        "sample_physicians": classifications[:10],
    }
