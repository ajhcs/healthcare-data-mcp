"""Generic AHRQ/CMS facility reconciliation for health systems."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

try:
    from .facility_enrichment import enrich_facility
    from .jefferson_resolver import normalize_system_name
    from .system_discovery import fuzzy_search_systems
except ImportError:
    from facility_enrichment import enrich_facility
    from jefferson_resolver import normalize_system_name
    from system_discovery import fuzzy_search_systems


GENERIC_METHOD_NOTE = (
    "Generic AHRQ/CMS reconciliation using AHRQ Compendium system-hospital linkage "
    "with CMS Provider of Services enrichment and optional CMS provider enrollment "
    "cross-references. This is not a curated merger ledger."
)


def system_slug_from_name(value: str) -> str:
    """Build a stable human-readable slug from an AHRQ system name."""
    normalized = normalize_system_name(value)
    return "-".join(token for token in normalized.replace("/", " ").split() if token)


def resolve_generic_system(
    system_slug: str,
    systems_df: pd.DataFrame,
) -> dict[str, Any] | None:
    """Resolve an AHRQ system ID, normalized name, or slug to a system row."""
    if systems_df.empty or "health_sys_id" not in systems_df.columns:
        return None

    query = str(system_slug or "").strip()
    if not query:
        return None

    ids = systems_df["health_sys_id"].astype(str)
    id_match = systems_df[ids.str.casefold() == query.casefold()]
    if not id_match.empty:
        return id_match.iloc[0].to_dict()

    query_norm = normalize_system_name(query)
    query_slug = query_norm.replace(" ", "-")
    for _, row in systems_df.iterrows():
        name = str(row.get("health_sys_name", "") or "")
        if not name:
            continue
        if normalize_system_name(name) == query_norm or system_slug_from_name(name) == query_slug:
            return row.to_dict()

    matches = fuzzy_search_systems(query, systems_df, limit=1, score_cutoff=90.0)
    if not matches:
        return None
    system_id = matches[0]["system_id"]
    match = systems_df[systems_df["health_sys_id"].astype(str) == system_id]
    return match.iloc[0].to_dict() if not match.empty else None


def reconcile_generic_system_facilities(
    system_slug: str,
    as_of_date: str | date | None,
    *,
    systems_df: pd.DataFrame,
    ahrq_hospitals: pd.DataFrame,
    cms_hgi: pd.DataFrame | None = None,
    provider_enrollment: pd.DataFrame | None = None,
    resolved_system: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a generic AHRQ/CMS facility ledger for one health system."""
    system = resolved_system or resolve_generic_system(system_slug, systems_df)
    if system is None:
        return {"error": f"System '{system_slug}' not found in AHRQ Compendium"}

    system_id = str(system.get("health_sys_id", "") or "")
    system_name = str(system.get("health_sys_name", "") or "")
    parsed_date = _parse_date(as_of_date)

    if ahrq_hospitals.empty or "health_sys_id" not in ahrq_hospitals.columns:
        hospitals = pd.DataFrame()
    else:
        hospitals = ahrq_hospitals[ahrq_hospitals["health_sys_id"].astype(str) == system_id]

    pos_frame = cms_hgi if cms_hgi is not None else pd.DataFrame()
    enrollment_frame = provider_enrollment if provider_enrollment is not None else pd.DataFrame()
    facilities = [_facility_from_ahrq_row(row, pos_frame, enrollment_frame) for _, row in hospitals.iterrows()]

    return {
        "system_slug": system_slug_from_name(system_name) or system_id,
        "system_id": system_id,
        "system_name": system_name,
        "as_of_date": parsed_date.isoformat(),
        "facility_count": len(facilities),
        "facilities": facilities,
        "alias_ledger": [],
        "merger_evidence": [],
        "discrepancy_closure": None,
        "source_metadata": [
            {
                "source_name": "AHRQ Compendium of U.S. Health Systems",
                "source_ref": "ahrq_compendium_2023",
                "role": "system_hospital_linkage",
            },
            {
                "source_name": "CMS Provider of Services",
                "source_ref": "cms_provider_of_services",
                "role": "facility_enrichment",
            },
            {
                "source_name": "CMS Provider Enrollment",
                "source_ref": "cms_provider_enrollment",
                "role": "optional_cross_reference",
            },
        ],
        "source_evidence": {
            "method": "generic_ahrq_cms_reconciliation",
            "note": GENERIC_METHOD_NOTE,
            "query": {"input": system_slug, "resolved_system_id": system_id, "resolved_system_name": system_name},
        },
        "method_note": GENERIC_METHOD_NOTE,
    }


def _parse_date(value: str | date | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _facility_from_ahrq_row(
    row: pd.Series,
    cms_hgi: pd.DataFrame,
    provider_enrollment: pd.DataFrame,
) -> dict[str, Any]:
    raw_ccn = str(row.get("ccn", "") or "").strip()
    ccn = raw_ccn.zfill(6) if raw_ccn else ""
    facility = _ahrq_facility(row, ccn)
    source_refs = {"ahrq_compendium_2023", "ahrq_hospital_linkage_row"}
    confidence = 0.76

    if ccn and not cms_hgi.empty:
        enriched = enrich_facility(ccn, cms_hgi)
        if enriched is not None:
            payload = enriched.model_dump()
            facility.update(
                {
                    "name": payload.get("name") or facility["name"],
                    "address": payload.get("address") or facility["address"],
                    "city": payload.get("city") or facility["city"],
                    "state": payload.get("state") or facility["state"],
                    "zip_code": payload.get("zip_code") or facility["zip_code"],
                    "county": payload.get("county") or "",
                    "phone": payload.get("phone") or "",
                    "hospital_type": payload.get("hospital_type") or "",
                    "ownership": payload.get("ownership") or facility["ownership"],
                    "teaching_status": payload.get("teaching_status") or facility["teaching_status"],
                    "beds": payload.get("beds") or facility["beds"],
                    "services": payload.get("services") or facility["services"],
                    "staffing": payload.get("staffing") or facility["staffing"],
                }
            )
            source_refs.add("cms_pos_row")
            confidence = 0.88

    if _matches_provider_enrollment(facility, provider_enrollment):
        source_refs.add("provider_enrollment_row")
        confidence = max(confidence, 0.9 if ccn else 0.8)

    facility["source_refs"] = sorted(source_refs)
    facility["confidence"] = confidence
    facility["active_status"] = "active"
    if not ccn:
        facility["no_ccn_reason"] = "AHRQ hospital linkage row did not include a CCN"
    return facility


def _ahrq_facility(row: pd.Series, ccn: str) -> dict[str, Any]:
    return {
        "ccn": ccn,
        "name": str(row.get("hospital_name", "") or "").strip(),
        "address": str(row.get("hosp_addr", "") or "").strip(),
        "city": str(row.get("hosp_city", "") or "").strip(),
        "state": str(row.get("hosp_state", "") or "").strip(),
        "zip_code": str(row.get("hosp_zip", "") or "").strip(),
        "county": "",
        "phone": "",
        "hospital_type": "",
        "ownership": str(row.get("ownership", "") or "").strip(),
        "teaching_status": str(row.get("teaching", "") or "").strip(),
        "beds": {
            "total": _safe_int(row.get("hos_beds", 0)),
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
    }


def _safe_int(value: Any) -> int:
    try:
        return int(float(str(value or "0").strip()))
    except (TypeError, ValueError):
        return 0


def _matches_provider_enrollment(facility: dict[str, Any], frame: pd.DataFrame) -> bool:
    if frame.empty:
        return False
    columns = {str(c).casefold(): c for c in frame.columns}
    ccn_col = columns.get("ccn") or columns.get("provider_number") or columns.get("cms_certification_number")
    name_col = columns.get("facility_name") or columns.get("hospital_name") or columns.get("provider_name")

    ccn = str(facility.get("ccn", "") or "").strip().zfill(6)
    if ccn and ccn_col:
        if bool(frame[ccn_col].astype(str).str.strip().str.zfill(6).eq(ccn).any()):
            return True

    name = normalize_system_name(facility.get("name", ""))
    if name and name_col:
        return bool(frame[name_col].astype(str).map(normalize_system_name).eq(name).any())
    return False
