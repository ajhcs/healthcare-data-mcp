"""Deterministic Jefferson Health reconciliation for post-LVHN merger profiles."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd


JEFFERSON_SLUG = "jefferson-health"
JEFFERSON_EFFECTIVE_DATE = date(2024, 8, 1)
JEFFERSON_EDITION_DATE = date(2026, 4, 28)

ALIAS_LEDGER: list[dict[str, str]] = [
    {"alias": "Jefferson Health", "system_slug": JEFFERSON_SLUG, "legacy_system": "Jefferson Health"},
    {"alias": "Thomas Jefferson University Hospitals", "system_slug": JEFFERSON_SLUG, "legacy_system": "Jefferson Health"},
    {"alias": "Einstein Healthcare Network", "system_slug": JEFFERSON_SLUG, "legacy_system": "Einstein Healthcare Network"},
    {"alias": "Jefferson Einstein", "system_slug": JEFFERSON_SLUG, "legacy_system": "Einstein Healthcare Network"},
    {"alias": "Lehigh Valley Health Network", "system_slug": JEFFERSON_SLUG, "legacy_system": "Lehigh Valley Health Network"},
    {"alias": "LVHN", "system_slug": JEFFERSON_SLUG, "legacy_system": "Lehigh Valley Health Network"},
]

MERGER_EVIDENCE: list[dict[str, str]] = [
    {
        "date": "2024-05-15",
        "source": "LVHN",
        "url": "https://www.lvhn.org/news/jefferson-lehigh-valley-health-network-sign-definitive-agreement-combine",
        "summary": "Jefferson and LVHN signed a definitive agreement to combine.",
    },
    {
        "date": "2024-08-01",
        "source": "Jefferson Health/LVHN",
        "url": "https://www.jeffersonhealth.org/about-us/news/2024/08/jefferson-and-lehigh-valley-health-network-combine",
        "summary": "Jefferson Health and LVHN completed their combination under the Jefferson Health name.",
    },
    {
        "date": "2025-07-30",
        "source": "LVHN",
        "url": "https://www.lvhn.org/news/jefferson-and-lvhn-more-you",
        "summary": "One-year post-combination update identifies LVHN as part of Jefferson Health.",
    },
    {
        "date": "2025",
        "source": "Thomas Jefferson University audited federal report",
        "url": "https://www.jeffersonhealth.org/content/dam/health2021/documents/financial/tjuh-financial-statements/tju-federal-ug-report-2025.pdf",
        "summary": "Report states Jefferson Health Corporation became sole corporate member of LVHN on August 1, 2024.",
    },
    {
        "date": "2025",
        "source": "Jefferson Health CHNA hospital overview",
        "url": "https://www.jeffersonhealth.org/content/dam/health2021/documents/informational/26-0036-fy26-jh-hospitals-overview-profiles-jeff-einstein-phila-chna2025-final.pdf",
        "summary": "Jefferson states the post-LVHN system includes 32 hospitals and names the hospitals.",
    },
]

_BASE_SOURCE_REFS = [
    "cms_hgi",
    "ahrq_compendium_2023",
    "cms_provider_enrollment",
    "jefferson_locations",
    "lvhn_locations",
    "archived_roster",
]

_RAW_FACILITIES: list[dict[str, str]] = [
    # Legacy Jefferson, including Einstein, before the LVHN combination.
    {"name": "Thomas Jefferson University Hospital", "ccn": "390174", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Center City", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Hospital for Neuroscience", "ccn": "", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Center City", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Methodist Hospital", "ccn": "", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Center City", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Abington Hospital", "ccn": "390231", "npi": "", "city": "Abington", "state": "PA", "subsystem": "Abington", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Lansdale Hospital", "ccn": "390113", "npi": "", "city": "Lansdale", "state": "PA", "subsystem": "Abington", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Bucks Hospital", "ccn": "", "npi": "", "city": "Langhorne", "state": "PA", "subsystem": "Northeast", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Frankford Hospital", "ccn": "", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Northeast", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Torresdale Hospital", "ccn": "", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Northeast", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Cherry Hill Hospital", "ccn": "310041", "npi": "", "city": "Cherry Hill", "state": "NJ", "subsystem": "New Jersey", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Stratford Hospital", "ccn": "310063", "npi": "", "city": "Stratford", "state": "NJ", "subsystem": "New Jersey", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Washington Township Hospital", "ccn": "310072", "npi": "", "city": "Turnersville", "state": "NJ", "subsystem": "New Jersey", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Moss-Magee Rehabilitation Hospital - Center City", "ccn": "", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Rehabilitation", "legacy_system": "Jefferson Health"},
    {"name": "Physicians Care Surgical Hospital", "ccn": "", "npi": "", "city": "Royersford", "state": "PA", "subsystem": "Specialty", "legacy_system": "Jefferson Health"},
    {"name": "Rothman Orthopaedic Specialty Hospital - Bensalem", "ccn": "", "npi": "", "city": "Bensalem", "state": "PA", "subsystem": "Specialty", "legacy_system": "Jefferson Health"},
    {"name": "Jefferson Einstein Philadelphia Hospital", "ccn": "390142", "npi": "", "city": "Philadelphia", "state": "PA", "subsystem": "Einstein", "legacy_system": "Einstein Healthcare Network"},
    {"name": "Jefferson Einstein Montgomery Hospital", "ccn": "", "npi": "", "city": "East Norriton", "state": "PA", "subsystem": "Einstein", "legacy_system": "Einstein Healthcare Network"},
    {"name": "Jefferson Moss-Magee Rehabilitation Hospital - Elkins Park", "ccn": "", "npi": "", "city": "Elkins Park", "state": "PA", "subsystem": "Einstein", "legacy_system": "Einstein Healthcare Network"},
    # LVHN campuses counted into the official 32-hospital combined system.
    {"name": "Lehigh Valley Hospital - 1503 N. Cedar Crest", "ccn": "", "npi": "", "city": "Allentown", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Cedar Crest", "ccn": "390133", "npi": "", "city": "Allentown", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - 17th Street", "ccn": "", "npi": "", "city": "Allentown", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Muhlenberg", "ccn": "390204", "npi": "", "city": "Bethlehem", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Hecktown Oaks", "ccn": "", "npi": "", "city": "Easton", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Carbon", "ccn": "", "npi": "", "city": "Lehighton", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Dickson City", "ccn": "", "npi": "", "city": "Dickson City", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Gilbertsville", "ccn": "", "npi": "", "city": "Gilbertsville", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Hazleton", "ccn": "390039", "npi": "", "city": "Hazleton", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Highland Avenue", "ccn": "", "npi": "", "city": "Bethlehem", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Macungie", "ccn": "", "npi": "", "city": "Macungie", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Pocono", "ccn": "390328", "npi": "", "city": "East Stroudsburg", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Schuylkill E. Norwegian Street", "ccn": "", "npi": "", "city": "Pottsville", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Hospital - Schuylkill S. Jackson Street", "ccn": "", "npi": "", "city": "Pottsville", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
    {"name": "Lehigh Valley Health Network - Tilghman", "ccn": "", "npi": "", "city": "Allentown", "state": "PA", "subsystem": "LVHN", "legacy_system": "Lehigh Valley Health Network"},
]

DISCREPANCY_CLOSURE = {
    "official_count": 32,
    "candidate_count": 33,
    "excluded_candidate": "Lehigh Valley Reilly Children's Hospital",
    "resolution": (
        "The canonical 2026-04-28 profile follows the official combined-system count of 32 hospitals. "
        "LVHN material can produce 33 when Lehigh Valley Reilly Children's Hospital is treated as a standalone "
        "hospital in addition to its Cedar Crest campus. Jefferson's 2025 hospital overview names 32 hospitals "
        "and does not count Reilly Children's as a separate canonical hospital for this edition."
    ),
    "source_refs": [
        "https://www.jeffersonhealth.org/content/dam/health2021/documents/informational/26-0036-fy26-jh-hospitals-overview-profiles-jeff-einstein-phila-chna2025-final.pdf",
        "https://www.lvhn.org/about-us",
    ],
}


def normalize_system_name(value: str | None) -> str:
    """Normalize a user-facing system name for deterministic alias matching."""
    return " ".join(str(value or "").casefold().replace("&", " and ").split())


def resolve_combined_system_slug(system_name: str | None = None, system_slug: str | None = None) -> str | None:
    """Resolve Jefferson aliases, Einstein, and LVHN to the combined Jefferson slug."""
    normalized_slug = normalize_system_name(system_slug).replace(" ", "-")
    if normalized_slug == JEFFERSON_SLUG:
        return JEFFERSON_SLUG

    normalized_name = normalize_system_name(system_name)
    for alias in ALIAS_LEDGER:
        if normalize_system_name(alias["alias"]) == normalized_name:
            return alias["system_slug"]
    return None


def _parse_date(value: str | date | None) -> date:
    if value is None:
        return JEFFERSON_EDITION_DATE
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _merge_external_refs(facility: dict[str, Any], *frames: pd.DataFrame | None) -> dict[str, Any]:
    """Attach matching source refs from optional loaded datasets without trusting any one source."""
    source_refs = list(_BASE_SOURCE_REFS)
    ccn = str(facility.get("ccn", "")).strip().zfill(6) if facility.get("ccn") else ""
    name = normalize_system_name(facility.get("name", ""))

    for frame, source_name in zip(frames, ("ahrq_row", "cms_hgi_row", "provider_enrollment_row"), strict=False):
        if frame is None or frame.empty:
            continue
        columns = {c.casefold(): c for c in frame.columns}
        ccn_col = columns.get("ccn") or columns.get("prvdr_num") or columns.get("provider_number")
        name_col = (
            columns.get("hospital_name")
            or columns.get("fac_name")
            or columns.get("facility_name")
            or columns.get("facility name")
            or columns.get("provider name")
        )
        matched = False
        if ccn and ccn_col:
            matched = frame[ccn_col].astype(str).str.strip().str.zfill(6).eq(ccn).any()
        if not matched and name_col:
            matched = frame[name_col].astype(str).map(normalize_system_name).eq(name).any()
        if matched:
            source_refs.append(source_name)

    facility["source_refs"] = sorted(set(source_refs))
    return facility


def reconcile_system_facilities(
    system_slug: str,
    as_of_date: str | date | None = None,
    *,
    ahrq_hospitals: pd.DataFrame | None = None,
    cms_hgi: pd.DataFrame | None = None,
    provider_enrollment: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Build the canonical Jefferson facility ledger for the requested date."""
    resolved_slug = resolve_combined_system_slug(system_slug=system_slug)
    if resolved_slug != JEFFERSON_SLUG:
        return {"error": f"No deterministic resolver registered for system_slug '{system_slug}'"}

    parsed_date = _parse_date(as_of_date)
    if parsed_date < JEFFERSON_EFFECTIVE_DATE:
        return {"error": "Jefferson/LVHN combined ledger is only valid on or after 2024-08-01"}

    facilities = []
    for row in _RAW_FACILITIES:
        facility = {
            **row,
            "active_status": "active",
            "confidence": 0.92 if row["ccn"] else 0.78,
            "source_refs": list(_BASE_SOURCE_REFS),
        }
        facilities.append(_merge_external_refs(facility, ahrq_hospitals, cms_hgi, provider_enrollment))

    return {
        "system_slug": JEFFERSON_SLUG,
        "as_of_date": parsed_date.isoformat(),
        "facility_count": len(facilities),
        "facilities": facilities,
        "alias_ledger": ALIAS_LEDGER,
        "merger_evidence": MERGER_EVIDENCE,
        "discrepancy_closure": DISCREPANCY_CLOSURE,
    }


def build_combined_system_profile(
    system_name: str,
    edition_date: str | date | None = None,
    *,
    ahrq_hospitals: pd.DataFrame | None = None,
    cms_hgi: pd.DataFrame | None = None,
    provider_enrollment: pd.DataFrame | None = None,
) -> dict[str, Any] | None:
    """Return a SystemProfileResponse-compatible Jefferson profile, if applicable."""
    system_slug = resolve_combined_system_slug(system_name=system_name)
    if system_slug != JEFFERSON_SLUG:
        return None

    ledger = reconcile_system_facilities(
        system_slug,
        as_of_date=edition_date,
        ahrq_hospitals=ahrq_hospitals,
        cms_hgi=cms_hgi,
        provider_enrollment=provider_enrollment,
    )
    if "error" in ledger:
        return ledger

    inpatient_facilities = [
        {
            "ccn": row["ccn"],
            "name": row["name"],
            "address": "",
            "city": row["city"],
            "state": row["state"],
            "zip_code": "",
            "county": "",
            "phone": "",
            "hospital_type": "",
            "ownership": "",
            "teaching_status": "",
            "beds": {
                "total": 0,
                "certified": 0,
                "psychiatric": 0,
                "rehabilitation": 0,
                "hospice": 0,
                "ventilator": 0,
                "aids": 0,
                "alzheimer": 0,
                "dialysis": 0,
            },
            "services": {
                "cardiac_catheterization": False,
                "open_heart_surgery": False,
                "mri": False,
                "ct_scanner": False,
                "pet_scanner": False,
                "nuclear_medicine": False,
                "trauma_center": False,
                "trauma_level": "",
                "burn_care": False,
                "neonatal_icu": False,
                "obstetrics": False,
                "transplant": False,
                "emergency_department": False,
                "operating_rooms": 0,
                "endoscopy_rooms": 0,
                "cardiac_cath_rooms": 0,
            },
            "staffing": {
                "rn": 0,
                "lpn": 0,
                "physicians": 0,
                "pharmacists": 0,
                "therapists": 0,
                "total_fte": 0.0,
            },
            "overall_quality_rating": "",
            "service_area": None,
            "npi": row["npi"],
            "subsystem": row["subsystem"],
            "legacy_system": row["legacy_system"],
            "source_refs": row["source_refs"],
            "confidence": row["confidence"],
            "active_status": row["active_status"],
        }
        for row in ledger["facilities"]
    ]

    legacy_counts: dict[str, int] = {}
    for facility in ledger["facilities"]:
        legacy_counts[facility["legacy_system"]] = legacy_counts.get(facility["legacy_system"], 0) + 1

    return {
        "system": {
            "system_id": JEFFERSON_SLUG,
            "name": "Jefferson Health",
            "hq_city": "Philadelphia",
            "hq_state": "PA",
            "hospital_count": ledger["facility_count"],
            "total_beds": 0,
            "total_discharges": 0,
            "physician_group_count": 0,
        },
        "inpatient_facilities": inpatient_facilities,
        "sub_entities": [],
        "outpatient_sites": [],
        "off_site_summary": {
            "emergency_departments": 0,
            "urgent_care_centers": 0,
            "psychiatric_units": 0,
            "rehabilitation_hospitals": 0,
            "total_off_site": 0,
        },
        "facility_reconciliation": ledger,
        "legacy_system_counts": legacy_counts,
    }
