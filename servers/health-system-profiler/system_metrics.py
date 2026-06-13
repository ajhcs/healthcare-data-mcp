"""Source-disciplined AHRQ health-system metrics assembly."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any, Literal

import pandas as pd
from rapidfuzz import fuzz, process


UniverseMode = Literal["compendium_snapshot", "latest_public_overlay"]
StateScope = Literal["headquarters", "facility_presence"]
SortKey = Literal["health_sys_id", "health_sys_name", "state", "hospital_count", "bed_count"]

UNIVERSE = "ahrq_compendium_2023"
UNIVERSE_DEFINITION = "AHRQ CHSP health-system definition"
SNAPSHOT_YEAR = 2023
SOURCE_RELEASE = "2023 Compendium; September 2025 revised system file when available"
UNIVERSE_CAVEAT = (
    "AHRQ Compendium 2023 is a public health-system snapshot aggregated to the highest ownership "
    "level. Subsidiary systems and individual campuses may not appear as separate systems."
)
PHYSICIAN_CAVEAT = (
    "System-level AHRQ/CHSP physician count; not a public roster of individually verified active "
    "physicians. AHRQ notes physician counts vary across source data and may include some double counting."
)
SHARED_CCN_CAVEAT = (
    "CCN is a join key, not guaranteed campus-level identity. AHRQ documents that multiple facilities "
    "or campuses sharing one CCN appear as a single linkage entry."
)

VALID_MODES = {"compendium_snapshot", "latest_public_overlay"}
VALID_STATE_SCOPES = {"headquarters", "facility_presence"}
VALID_SORTS = {"health_sys_id", "health_sys_name", "state", "hospital_count", "bed_count"}
MAX_PAGE_SIZE = 100

_HGI_DATASET = "cms_hospital_general_info"
_POS_DATASET = "cms_provider_of_services"
_AHRQ_DATASET = "ahrq_health_system_compendium"
_CLINICIAN_DATASET = "cms_doctors_clinicians_national_downloadable_file"


def list_health_system_metric_rows(
    *,
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
    cursor: str | None = None,
    page_size: int = 50,
    sort: str = "health_sys_id",
    state: str | None = None,
    state_scope: str = "headquarters",
    as_of_mode: str = "compendium_snapshot",
    include_facilities: bool = False,
    include_medicare_public_clinician_roster_estimate: bool = False,
    hgi_df: pd.DataFrame | None = None,
    pos_df: pd.DataFrame | None = None,
    clinicians_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Return a cursor-paged national metrics payload."""

    mode = _normalize_mode(as_of_mode)
    scope = _normalize_state_scope(state_scope)
    sort_key = _normalize_sort(sort)
    bounded_size = max(1, min(int(page_size or 50), MAX_PAGE_SIZE))
    snapshot_id = build_snapshot_id(systems_df, hospitals_df)
    decoded = _decode_cursor(cursor)
    if decoded and decoded.get("snapshot_id") != snapshot_id:
        return _error_payload(
            "cursor_snapshot_mismatch",
            "Cursor belongs to a different AHRQ snapshot/cache state.",
            mode=mode,
            snapshot_id=snapshot_id,
            data={"cursor_snapshot_id": decoded.get("snapshot_id"), "expected_snapshot_id": snapshot_id},
        )

    filtered_systems = _filter_systems(systems_df, hospitals_df, state=state, state_scope=scope)
    ordered = _sort_systems(filtered_systems, sort_key)
    start = int(decoded.get("offset", 0)) if decoded else 0
    end = start + bounded_size
    page = ordered.iloc[start:end]

    hgi_index = _frame_by_ccn(hgi_df)
    pos_index = _frame_by_ccn(pos_df)
    systems = [
        build_system_metric(
            row.to_dict(),
            hospitals_df,
            mode=mode,
            include_facilities=include_facilities,
            include_medicare_public_clinician_roster_estimate=include_medicare_public_clinician_roster_estimate,
            hgi_index=hgi_index,
            pos_index=pos_index,
            clinicians_df=clinicians_df,
        )
        for _, row in page.iterrows()
    ]

    next_cursor = None
    if end < len(ordered):
        next_cursor = _encode_cursor(
            {
                "snapshot_id": snapshot_id,
                "offset": end,
                "sort": sort_key,
                "state": _state(state),
                "state_scope": scope,
                "as_of_mode": mode,
            }
        )

    query = {
        "cursor": cursor or "",
        "page_size": bounded_size,
        "sort": sort_key,
        "state": _state(state),
        "state_scope": scope,
        "as_of_mode": mode,
        "include_facilities": include_facilities,
        "include_medicare_public_clinician_roster_estimate": include_medicare_public_clinician_roster_estimate,
    }
    payload = {
        **universe_metadata(mode, snapshot_id),
        "pagination": {
            "cursor": cursor,
            "next_cursor": next_cursor,
            "page_size": bounded_size,
            "systems_returned": len(systems),
            "sort": sort_key,
        },
        "systems": systems,
        "coverage": coverage_summary(
            systems_df,
            hospitals_df,
            systems_returned=len(systems),
            hgi_df=hgi_df,
            pos_df=pos_df,
        ),
        "evidence": _evidence(query=query, match_basis="ahrq_compendium_system_metric_page", mode=mode),
        "source_metadata": source_metadata(
            mode=mode,
            include_overlay=mode == "latest_public_overlay",
            include_clinician_roster=include_medicare_public_clinician_roster_estimate,
        ),
        "identity_map": _identity_map(systems),
        "next_actions": [
            "Use get_health_system_metrics with an exact AHRQ health_sys_id before citing one system in detail.",
            "Keep compendium_snapshot values separate from latest_public_overlay candidates in downstream reports.",
        ],
    }
    return payload


def get_health_system_metric(
    *,
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
    system_id: str | None = None,
    system_name: str | None = None,
    as_of_mode: str = "compendium_snapshot",
    include_facilities: bool = True,
    include_medicare_public_clinician_roster_estimate: bool = False,
    hgi_df: pd.DataFrame | None = None,
    pos_df: pd.DataFrame | None = None,
    clinicians_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Return one system metrics payload or a bounded candidate list."""

    mode = _normalize_mode(as_of_mode)
    snapshot_id = build_snapshot_id(systems_df, hospitals_df)
    query = {
        "system_id": str(system_id or ""),
        "system_name": str(system_name or ""),
        "as_of_mode": mode,
        "include_facilities": include_facilities,
        "include_medicare_public_clinician_roster_estimate": include_medicare_public_clinician_roster_estimate,
    }
    row = _resolve_system_row(systems_df, system_id=system_id, system_name=system_name)
    if row.get("status") == "candidates":
        return {
            **universe_metadata(mode, snapshot_id),
            "error": {
                "code": "ambiguous_system_name",
                "message": "System name did not resolve above the confidence threshold; retry with exact AHRQ system_id.",
                "recoverable": True,
                "data": {"candidates": row["candidates"]},
            },
            "candidates": row["candidates"],
            "coverage": coverage_summary(systems_df, hospitals_df, systems_returned=0, hgi_df=hgi_df, pos_df=pos_df),
            "evidence": _evidence(query=query, match_basis="low_confidence_system_name_candidates", mode=mode),
            "source_metadata": source_metadata(
                mode=mode,
                include_overlay=mode == "latest_public_overlay",
                include_clinician_roster=include_medicare_public_clinician_roster_estimate,
            ),
            "next_actions": ["Retry with one candidate's exact system_id."],
        }
    if row.get("status") == "not_found":
        return _error_payload(
            "not_found",
            "No AHRQ Compendium 2023 health system matched the provided identifier.",
            mode=mode,
            snapshot_id=snapshot_id,
            data={"query": query},
        )

    hgi_index = _frame_by_ccn(hgi_df)
    pos_index = _frame_by_ccn(pos_df)
    system = build_system_metric(
        row["row"],
        hospitals_df,
        mode=mode,
        include_facilities=include_facilities,
        include_medicare_public_clinician_roster_estimate=include_medicare_public_clinician_roster_estimate,
        hgi_index=hgi_index,
        pos_index=pos_index,
        clinicians_df=clinicians_df,
    )
    return {
        **universe_metadata(mode, snapshot_id),
        "system": system,
        "coverage": coverage_summary(systems_df, hospitals_df, systems_returned=1, hgi_df=hgi_df, pos_df=pos_df),
        "evidence": _evidence(query=query, match_basis="ahrq_system_id_exact" if system_id else "system_name_resolved", mode=mode),
        "source_metadata": source_metadata(
            mode=mode,
            include_overlay=mode == "latest_public_overlay",
            include_clinician_roster=include_medicare_public_clinician_roster_estimate,
        ),
        "identity_map": _identity_map([system]),
        "next_actions": [
            "Use compendium_snapshot values for AHRQ 2023 reports.",
            "Use latest_public_overlay candidates only when a current public-data caveat is acceptable.",
        ],
    }


def build_system_metric(
    system: dict[str, Any],
    hospitals_df: pd.DataFrame,
    *,
    mode: UniverseMode,
    include_facilities: bool,
    include_medicare_public_clinician_roster_estimate: bool,
    hgi_index: dict[str, dict[str, Any]] | None = None,
    pos_index: dict[str, dict[str, Any]] | None = None,
    clinicians_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Build one source-disciplined system metric object."""

    system_id = str(system.get("health_sys_id") or "")
    linked = _linked_hospitals(hospitals_df, system_id)
    acute_rows = [row for row in linked if _int_or_none(row.get("acutehosp_flag")) == 1]
    hospital_count_total = _int_or_none(system.get("hosp_cnt"))
    acute_count = _int_or_none(system.get("acutehosp_cnt"))
    linked_count = len(linked)
    acute_linked_count = len(acute_rows)
    warnings = _system_warnings(
        hospital_count_total=hospital_count_total,
        acute_count=acute_count,
        linked_count=linked_count,
        acute_linked_count=acute_linked_count,
        system=system,
        acute_rows=acute_rows,
    )
    facilities = [
        build_hospital_metric(row, mode=mode, hgi_row=(hgi_index or {}).get(str(row.get("ccn") or "")), pos_row=(pos_index or {}).get(str(row.get("ccn") or "")))
        for row in linked
    ]
    payload = {
        "system_id": system_id,
        "system_name": str(system.get("health_sys_name") or ""),
        "headquarters": {
            "city": str(system.get("health_sys_city") or ""),
            "state": str(system.get("health_sys_state") or ""),
        },
        "counts": {
            "hospital_count_total": _metric_value(hospital_count_total, "AHRQ hosp_cnt", "ahrq_system_file"),
            "hospital_count_nonfederal_general_acute": _metric_value(acute_count, "AHRQ acutehosp_cnt", "ahrq_system_file"),
            "linked_hospital_rows_count": _metric_value(linked_count, "AHRQ hospital linkage grouped rows", "ahrq_hospital_linkage"),
            "linked_nonfederal_general_acute_rows_count": _metric_value(acute_linked_count, "AHRQ hospital linkage acutehosp_flag grouped rows", "ahrq_hospital_linkage"),
            "system_bed_count_nonfederal_general_acute": _metric_value(
                _int_or_none(system.get("sys_beds")),
                "AHRQ sys_beds",
                "ahrq_system_file",
                caveat="AHRQ defines system beds as beds in non-federal general acute care hospitals.",
            ),
            "facility_rollup_candidate": _facility_rollup_candidate(acute_rows),
            "physician_count": {
                "value": _int_or_none(system.get("total_mds")),
                "label": "AHRQ Compendium physician count",
                "source_field": "total_mds",
                "source": "AHRQ system file",
                "dataset_id": _AHRQ_DATASET,
                "confidence": "compendium_snapshot_count",
                "caveat": PHYSICIAN_CAVEAT,
            },
            "primary_care_physician_count": _metric_value(_int_or_none(system.get("prim_care_mds")), "AHRQ prim_care_mds", "ahrq_system_file"),
            "nurse_practitioner_count": _metric_value(_int_or_none(system.get("total_nps")), "AHRQ total_nps", "ahrq_system_file"),
            "physician_assistant_count": _metric_value(_int_or_none(system.get("total_pas")), "AHRQ total_pas", "ahrq_system_file"),
            "physician_group_count": _metric_value(_int_or_none(system.get("grp_cnt") or system.get("phys_grp_count")), "AHRQ grp_cnt", "ahrq_system_file"),
        },
        "warnings": warnings,
        "source_vintage_policy": _vintage_policy(mode),
        "evidence": _evidence(
            query={"system_id": system_id, "system_name": str(system.get("health_sys_name") or "")},
            match_basis="ahrq_system_metric_row",
            mode=mode,
        ),
    }
    if include_medicare_public_clinician_roster_estimate:
        payload["medicare_public_clinician_roster_estimate"] = _clinician_roster_estimate(system, facilities, clinicians_df)
    if include_facilities:
        payload["hospitals"] = facilities
    return payload


def build_hospital_metric(
    row: dict[str, Any],
    *,
    mode: UniverseMode,
    hgi_row: dict[str, Any] | None = None,
    pos_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one hospital-level metric row with candidates."""

    ccn = str(row.get("ccn") or "")
    address = _hospital_address(row, mode=mode, hgi_row=hgi_row, pos_row=pos_row)
    hospital_type = _hospital_type(row, mode=mode, hgi_row=hgi_row, pos_row=pos_row)
    warnings = []
    if not ccn:
        warnings.append({"code": "missing_ccn", "message": "AHRQ linkage row has no CCN; use compendium_hospital_id as row identity."})
    warnings.append({"code": "ccn_not_campus_identity", "message": SHARED_CCN_CAVEAT})
    return {
        "compendium_hospital_id": str(row.get("compendium_hospital_id") or ""),
        "ccn": ccn,
        "ccn_role": "join_key_not_guaranteed_campus_identity",
        "hospital_name": str(row.get("hospital_name") or ""),
        "system_id": str(row.get("health_sys_id") or ""),
        "system_name": str(row.get("health_sys_name") or ""),
        "hospital_bed_count": {
            "primary": _int_or_none(row.get("hos_beds")),
            "primary_basis": "compendium_snapshot",
            "source": "AHRQ hospital linkage hos_beds",
            "source_field": "hos_beds",
            "dataset_id": _AHRQ_DATASET,
            "candidates": _hospital_bed_candidates(row, pos_row=pos_row),
            "caveat": "In compendium_snapshot mode, AHRQ hospital linkage hos_beds is the primary hospital bed count.",
        },
        "hospital_address": address,
        "hospital_type": hospital_type,
        "discharges": _metric_value(_int_or_none(row.get("hos_dsch")), "AHRQ hos_dsch", "ahrq_hospital_linkage"),
        "warnings": warnings,
    }


def coverage_summary(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
    *,
    systems_returned: int,
    hgi_df: pd.DataFrame | None = None,
    pos_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Return concrete source coverage fields."""

    linked = hospitals_df[hospitals_df.get("health_sys_id", pd.Series(dtype=str)).astype(str).str.strip() != ""] if "health_sys_id" in hospitals_df.columns else pd.DataFrame()
    acute = linked[pd.to_numeric(linked.get("acutehosp_flag", pd.Series(dtype=str)), errors="coerce").fillna(0).astype(int) == 1] if not linked.empty and "acutehosp_flag" in linked.columns else pd.DataFrame()
    count_fields = ("hosp_cnt", "acutehosp_cnt", "sys_beds", "total_mds")
    complete = 0
    for _, row in systems_df.iterrows():
        if all(_int_or_none(row.get(field)) is not None for field in count_fields):
            complete += 1
    hgi_ccns = set(_frame_by_ccn(hgi_df))
    pos_ccns = set(_frame_by_ccn(pos_df))
    linked_ccns = {str(value) for value in linked.get("ccn", []) if str(value)}
    return {
        "total_systems_in_universe": len(systems_df),
        "systems_returned": systems_returned,
        "total_hospital_linkage_rows": len(hospitals_df),
        "linked_hospitals_to_systems": len(linked),
        "linked_nonfederal_general_acute_hospitals": len(acute),
        "systems_with_all_required_counts": complete,
        "hospitals_with_hos_beds": int(pd.to_numeric(linked.get("hos_beds", pd.Series(dtype=str)), errors="coerce").notna().sum()) if not linked.empty else 0,
        "hospitals_missing_hcris_bed_data": None,
        "hospitals_matched_to_cms_hgi": len(linked_ccns & hgi_ccns),
        "hospitals_matched_to_pos": len(linked_ccns & pos_ccns),
        "coverage_note": UNIVERSE_CAVEAT,
    }


def universe_metadata(mode: UniverseMode, snapshot_id: str) -> dict[str, Any]:
    return {
        "universe": UNIVERSE,
        "universe_definition": UNIVERSE_DEFINITION,
        "snapshot_year": SNAPSHOT_YEAR,
        "source_release": SOURCE_RELEASE,
        "data_mode": mode,
        "snapshot_id": snapshot_id,
        "universe_caveat": UNIVERSE_CAVEAT,
    }


def source_metadata(*, mode: UniverseMode, include_overlay: bool, include_clinician_roster: bool = False) -> list[dict[str, Any]]:
    sources = [
        {
            "source_name": "AHRQ Compendium of U.S. Health Systems, 2023",
            "dataset_id": _AHRQ_DATASET,
            "source_period": SOURCE_RELEASE,
            "landing_page": "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
            "role": "canonical_health_system_universe_and_snapshot_metrics",
        }
    ]
    if include_overlay:
        sources.extend(
            [
                {
                    "source_name": "CMS Hospital General Information",
                    "dataset_id": _HGI_DATASET,
                    "source_period": "latest configured CMS Provider Data export",
                    "landing_page": "https://data.cms.gov/provider-data/dataset/xubh-q36u",
                    "role": "latest_public_overlay_address_type_candidate",
                },
                {
                    "source_name": "CMS Provider of Services",
                    "dataset_id": _POS_DATASET,
                    "source_period": "latest configured CMS POS cache",
                    "landing_page": "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-internet-quality-improvement-and-evaluation-system",
                    "role": "latest_public_overlay_facility_candidate",
                },
            ]
        )
    if include_clinician_roster:
        sources.append(
            {
                "source_name": "CMS Doctors and Clinicians National Downloadable File",
                "dataset_id": _CLINICIAN_DATASET,
                "source_period": "latest configured CMS Provider Data export when loaded",
                "landing_page": "https://data.cms.gov/provider-data/dataset/mj5m-pzi6",
                "role": "experimental_medicare_public_clinician_roster_estimate",
            }
        )
    return sources


def build_snapshot_id(systems_df: pd.DataFrame, hospitals_df: pd.DataFrame) -> str:
    payload = {
        "universe": UNIVERSE,
        "systems_rows": len(systems_df),
        "hospitals_rows": len(hospitals_df),
        "systems_columns": sorted(str(col) for col in systems_df.columns),
        "hospitals_columns": sorted(str(col) for col in hospitals_df.columns),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _hospital_address(
    row: dict[str, Any],
    *,
    mode: UniverseMode,
    hgi_row: dict[str, Any] | None,
    pos_row: dict[str, Any] | None,
) -> dict[str, Any]:
    candidates = [
        {
            "source": "AHRQ hospital linkage",
            "dataset_id": _AHRQ_DATASET,
            "data_mode": "compendium_snapshot",
            "address": _address_from_ahrq(row),
        }
    ]
    if hgi_row:
        candidates.append({"source": "CMS Hospital General Information", "dataset_id": _HGI_DATASET, "data_mode": "latest_public_overlay", "address": _address_from_hgi(hgi_row)})
    if pos_row:
        candidates.append({"source": "CMS Provider of Services", "dataset_id": _POS_DATASET, "data_mode": "latest_public_overlay", "address": _address_from_pos(pos_row)})
    preferred_mode = "latest_public_overlay" if mode == "latest_public_overlay" else "compendium_snapshot"
    primary = next((candidate for candidate in candidates if candidate["data_mode"] == preferred_mode and any(candidate["address"].values())), candidates[0])
    return {
        "primary": primary["address"],
        "primary_basis": preferred_mode,
        "candidates": candidates,
        "conflicts": _address_conflicts(candidates),
    }


def _hospital_type(
    row: dict[str, Any],
    *,
    mode: UniverseMode,
    hgi_row: dict[str, Any] | None,
    pos_row: dict[str, Any] | None,
) -> dict[str, Any]:
    hgi_raw = _first_value(hgi_row or {}, "hospital_type", "Hospital Type", "facility_type")
    pos_raw = _first_value(pos_row or {}, "PRVDR_CTGRY_CD", "PRVDR_CTGRY_SBTYP_CD", "GNRL_FAC_TYPE_CD")
    ahrq_acute = _int_or_none(row.get("acutehosp_flag"))
    inferred = "nonfederal_general_acute" if ahrq_acute == 1 else "non_acute_or_unknown" if ahrq_acute == 0 else "unknown"
    primary_raw = (hgi_raw or pos_raw or inferred) if mode == "latest_public_overlay" else inferred
    normalized = _normalize_hospital_type(str(primary_raw or inferred), ahrq_acute=ahrq_acute)
    return {
        "normalized_type": normalized,
        "primary_basis": "latest_public_overlay" if mode == "latest_public_overlay" and (hgi_raw or pos_raw) else "compendium_snapshot",
        "ahrq_acutehosp_flag": ahrq_acute,
        "cms_hgi_hospital_type_raw": hgi_raw or None,
        "cms_pos_provider_type_raw": pos_raw or None,
        "ccn_type_inferred": inferred,
        "conflicts": _type_conflicts(ahrq_acute, hgi_raw, pos_raw),
    }


def _hospital_bed_candidates(row: dict[str, Any], *, pos_row: dict[str, Any] | None) -> list[dict[str, Any]]:
    candidates = [
        {
            "source": "AHRQ hospital linkage",
            "dataset_id": _AHRQ_DATASET,
            "source_field": "hos_beds",
            "data_mode": "compendium_snapshot",
            "value": _int_or_none(row.get("hos_beds")),
            "selected_for_compendium_snapshot": True,
        }
    ]
    if pos_row:
        for field in ("BED_CNT", "CRTFD_BED_CNT"):
            value = _int_or_none(pos_row.get(field))
            if value is not None:
                candidates.append(
                    {
                        "source": "CMS Provider of Services",
                        "dataset_id": _POS_DATASET,
                        "source_field": field,
                        "data_mode": "latest_public_overlay",
                        "value": value,
                        "selected_for_compendium_snapshot": False,
                    }
                )
    return candidates


def _facility_rollup_candidate(acute_rows: list[dict[str, Any]]) -> dict[str, Any]:
    values = [_int_or_none(row.get("hos_beds")) for row in acute_rows]
    included = [value for value in values if value is not None]
    return {
        "value": sum(included) if included else None,
        "included_facility_count": len(included),
        "source": "AHRQ hospital linkage hos_beds acute-row rollup",
        "confidence": "candidate_rollup_not_replacement",
        "caveat": "Compare to AHRQ sys_beds; differences are warnings, not silent replacements.",
    }


def _clinician_roster_estimate(
    system: dict[str, Any],
    facilities: list[dict[str, Any]],
    clinicians_df: pd.DataFrame | None,
) -> dict[str, Any]:
    if clinicians_df is None or clinicians_df.empty:
        return {
            "status": "unavailable_public_cache",
            "value": None,
            "label": "Experimental Medicare public clinician roster estimate",
            "dataset_id": _CLINICIAN_DATASET,
            "caveat": "Doctors and Clinicians rows are clinician/enrollment/group/address-level; load a cache before estimating.",
        }
    system_name = _norm(system.get("health_sys_name"))
    states = {str(system.get("health_sys_state") or "").upper()}
    states.update(str(f.get("hospital_address", {}).get("primary", {}).get("state") or "").upper() for f in facilities)
    facility_names = {_norm(f.get("hospital_name")) for f in facilities}
    npi_col = _find_col(clinicians_df, "npi")
    spec_col = _find_col(clinicians_df, "pri_spec", "primary_specialty", "specialty")
    facility_col = _find_col(clinicians_df, "facility_name", "organization_name", "org_name")
    state_col = _find_col(clinicians_df, "state")
    cred_col = _find_col(clinicians_df, "cred", "credential")
    if not npi_col:
        return {"status": "unavailable_public_cache", "value": None, "caveat": "Clinician file did not include an NPI column."}
    npis: set[str] = set()
    for _, row in clinicians_df.iterrows():
        npi = str(row.get(npi_col) or "").strip()
        if not npi:
            continue
        state = str(row.get(state_col) or "").upper() if state_col else ""
        if states and state and state not in states:
            continue
        if not _looks_like_physician(row.get(spec_col) if spec_col else "", row.get(cred_col) if cred_col else ""):
            continue
        org_norm = _norm(row.get(facility_col) if facility_col else "")
        if org_norm and (system_name in org_norm or org_norm in facility_names):
            npis.add(npi)
    return {
        "status": "experimental_candidate",
        "value": len(npis),
        "dedupe_key": "npi",
        "label": "Experimental Medicare public clinician roster estimate",
        "dataset_id": _CLINICIAN_DATASET,
        "caveat": "Deduped by NPI after physician credential/specialty and organization/geography filters; not a full system physician roster.",
    }


def _looks_like_physician(specialty: Any, credential: Any) -> bool:
    text = f"{specialty or ''} {credential or ''}".upper()
    physician_markers = ("MD", "DO", "M.D", "D.O", "PHYSICIAN", "SURGERY", "CARDIOLOGY", "RADIOLOGY", "ANESTHESIOLOGY", "INTERNAL MEDICINE", "FAMILY PRACTICE", "GENERAL PRACTICE")
    excluded = ("NURSE", "PHYSICIAN ASSISTANT", "SOCIAL WORKER", "DIETITIAN", "PSYCHOLOGIST")
    return any(marker in text for marker in physician_markers) and not any(marker in text for marker in excluded)


def _system_warnings(
    *,
    hospital_count_total: int | None,
    acute_count: int | None,
    linked_count: int,
    acute_linked_count: int,
    system: dict[str, Any],
    acute_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    if hospital_count_total is not None and hospital_count_total != linked_count:
        warnings.append({"code": "hosp_cnt_linkage_count_mismatch", "source_value": hospital_count_total, "linked_row_count": linked_count})
    if acute_count is not None and acute_count != acute_linked_count:
        warnings.append({"code": "acutehosp_cnt_linkage_count_mismatch", "source_value": acute_count, "linked_acute_row_count": acute_linked_count})
    sys_beds = _int_or_none(system.get("sys_beds"))
    rollup = _facility_rollup_candidate(acute_rows)["value"]
    if sys_beds is not None and rollup is not None and sys_beds != rollup:
        warnings.append({"code": "sys_beds_acute_hos_beds_rollup_difference", "source_value": sys_beds, "rollup_candidate": rollup})
    return warnings


def _filter_systems(systems_df: pd.DataFrame, hospitals_df: pd.DataFrame, *, state: str | None, state_scope: StateScope) -> pd.DataFrame:
    if not state:
        return systems_df.copy()
    state_norm = _state(state)
    if state_scope == "headquarters":
        if "health_sys_state" not in systems_df.columns:
            return systems_df.iloc[0:0].copy()
        return systems_df[systems_df["health_sys_state"].astype(str).str.upper() == state_norm].copy()
    if "hospital_state" not in hospitals_df.columns:
        return systems_df.iloc[0:0].copy()
    ids = set(hospitals_df[hospitals_df["hospital_state"].astype(str).str.upper() == state_norm]["health_sys_id"].astype(str))
    return systems_df[systems_df["health_sys_id"].astype(str).isin(ids)].copy()


def _sort_systems(systems_df: pd.DataFrame, sort: SortKey) -> pd.DataFrame:
    frame = systems_df.copy()
    if sort == "state":
        return frame.sort_values(["health_sys_state", "health_sys_id"], kind="stable")
    if sort == "hospital_count":
        frame["_sort"] = pd.to_numeric(frame.get("hosp_cnt"), errors="coerce").fillna(-1)
        return frame.sort_values(["_sort", "health_sys_id"], ascending=[False, True], kind="stable").drop(columns=["_sort"])
    if sort == "bed_count":
        frame["_sort"] = pd.to_numeric(frame.get("sys_beds"), errors="coerce").fillna(-1)
        return frame.sort_values(["_sort", "health_sys_id"], ascending=[False, True], kind="stable").drop(columns=["_sort"])
    return frame.sort_values([sort], kind="stable")


def _resolve_system_row(systems_df: pd.DataFrame, *, system_id: str | None, system_name: str | None) -> dict[str, Any]:
    if system_id:
        matches = systems_df[systems_df.get("health_sys_id", pd.Series(dtype=str)).astype(str).str.casefold() == str(system_id).casefold()]
        if not matches.empty:
            return {"status": "ok", "row": matches.iloc[0].to_dict()}
    if not system_name:
        return {"status": "not_found"}
    if "health_sys_name" not in systems_df.columns:
        return {"status": "not_found"}
    names = systems_df["health_sys_name"].astype(str).tolist()
    matches = process.extract(
        system_name,
        names,
        scorer=fuzz.token_set_ratio,
        limit=5,
        processor=lambda value: value.lower() if isinstance(value, str) else value,
    )
    if not matches:
        return {"status": "not_found"}
    best_name, score, idx = matches[0]
    if score < 90:
        return {
            "status": "candidates",
            "candidates": [_candidate(systems_df.iloc[item[2]], item[1]) for item in matches],
        }
    return {"status": "ok", "row": systems_df.iloc[idx].to_dict(), "match_score": score, "matched_name": best_name}


def _candidate(row: pd.Series, score: float) -> dict[str, Any]:
    return {
        "system_id": str(row.get("health_sys_id") or ""),
        "system_name": str(row.get("health_sys_name") or ""),
        "hq_city": str(row.get("health_sys_city") or ""),
        "hq_state": str(row.get("health_sys_state") or ""),
        "match_score": round(float(score), 1),
    }


def _linked_hospitals(hospitals_df: pd.DataFrame, system_id: str) -> list[dict[str, Any]]:
    if hospitals_df.empty or "health_sys_id" not in hospitals_df.columns:
        return []
    return hospitals_df[hospitals_df["health_sys_id"].astype(str) == str(system_id)].to_dict("records")


def _frame_by_ccn(frame: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty:
        return {}
    ccn_col = _find_col(frame, "ccn", "facility_id", "prvdr_num", "provider_number", "cms_certification_number")
    if not ccn_col:
        return {}
    index: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        ccn = str(row.get(ccn_col) or "").strip()
        if ccn:
            index[ccn.zfill(6)] = row.to_dict()
    return index


def _address_from_ahrq(row: dict[str, Any]) -> dict[str, str]:
    return {
        "line1": str(row.get("hospital_street") or ""),
        "city": str(row.get("hospital_city") or ""),
        "state": str(row.get("hospital_state") or ""),
        "zip_code": str(row.get("hospital_zip") or ""),
    }


def _address_from_hgi(row: dict[str, Any]) -> dict[str, str]:
    return {
        "line1": _first_value(row, "address", "Address"),
        "city": _first_value(row, "city/town", "city", "City/Town"),
        "state": _first_value(row, "state", "State"),
        "zip_code": _first_value(row, "zip_code", "ZIP Code", "zip"),
    }


def _address_from_pos(row: dict[str, Any]) -> dict[str, str]:
    return {
        "line1": _first_value(row, "ST_ADR", "street_address", "address"),
        "city": _first_value(row, "CITY_NAME", "city"),
        "state": _first_value(row, "STATE_CD", "state"),
        "zip_code": _first_value(row, "ZIP_CD", "zip_code", "zip"),
    }


def _address_conflicts(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: dict[str, list[str]] = {}
    for candidate in candidates:
        address = candidate.get("address") or {}
        key = _norm(" ".join(str(address.get(part) or "") for part in ("line1", "city", "state", "zip_code")))
        if key:
            seen.setdefault(key, []).append(candidate["source"])
    if len(seen) <= 1:
        return []
    return [{"code": "address_source_conflict", "sources_by_normalized_address": seen}]


def _type_conflicts(ahrq_acute: int | None, hgi_raw: str, pos_raw: str) -> list[dict[str, Any]]:
    conflicts = []
    if ahrq_acute == 1 and hgi_raw and "acute" not in hgi_raw.lower() and "critical access" not in hgi_raw.lower():
        conflicts.append({"code": "ahrq_acute_flag_cms_type_difference", "ahrq_acutehosp_flag": ahrq_acute, "cms_hgi_hospital_type_raw": hgi_raw})
    if hgi_raw and pos_raw and _normalize_hospital_type(hgi_raw, ahrq_acute=None) != _normalize_hospital_type(pos_raw, ahrq_acute=None):
        conflicts.append({"code": "cms_hgi_pos_type_difference", "cms_hgi_hospital_type_raw": hgi_raw, "cms_pos_provider_type_raw": pos_raw})
    return conflicts


def _normalize_hospital_type(value: str, *, ahrq_acute: int | None) -> str:
    text = value.casefold()
    if "critical access" in text:
        return "critical_access"
    if "children" in text:
        return "childrens"
    if "psychiatric" in text or "psych" in text:
        return "psychiatric"
    if "rehab" in text:
        return "rehab"
    if "long term" in text or "ltach" in text:
        return "ltach"
    if "acute" in text or ahrq_acute == 1:
        return "acute_care"
    if ahrq_acute == 0:
        return "other"
    return "unknown"


def _metric_value(value: int | None, label: str, source_field: str, *, caveat: str = "") -> dict[str, Any]:
    return {
        "value": value,
        "label": label,
        "source_field": source_field,
        "dataset_id": _AHRQ_DATASET,
        "confidence": "compendium_snapshot_count",
        "caveat": caveat or UNIVERSE_CAVEAT,
    }


def _vintage_policy(mode: UniverseMode) -> dict[str, Any]:
    return {
        "mode": mode,
        "default_rule": "Do not silently mix AHRQ 2023 snapshot values with later CMS public overlays.",
        "compendium_snapshot": "Primary values come from AHRQ 2023 system and hospital linkage files.",
        "latest_public_overlay": "CMS HGI/POS/HCRIS/state values are dated candidates and not replacements for AHRQ snapshot values.",
    }


def _evidence(*, query: dict[str, Any], match_basis: str, mode: UniverseMode) -> dict[str, Any]:
    return {
        "source_name": "AHRQ Compendium of U.S. Health Systems, 2023",
        "source_url": "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
        "dataset_id": _AHRQ_DATASET,
        "source_period": SOURCE_RELEASE,
        "landing_page": "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
        "cache_status": "local_public_cache",
        "entity_scope": "ahrq_compendium_2023_health_system_metrics",
        "query": query,
        "match_basis": match_basis,
        "confidence": "source_disciplined_snapshot" if mode == "compendium_snapshot" else "snapshot_with_public_overlay_candidates",
        "caveat": UNIVERSE_CAVEAT,
        "next_step": "Preserve data_mode and source_metadata with every cited count, address, type, or roster estimate.",
    }


def _identity_map(systems: list[dict[str, Any]]) -> dict[str, Any]:
    ids = [str(system.get("system_id") or "") for system in systems if system.get("system_id")]
    return {
        "entity_scope": "ahrq_compendium_2023_health_system_metrics",
        "join_keys": [
            {"field": "health_sys_id", "values": ids, "status": "provided" if ids else "missing", "used_by": [_AHRQ_DATASET]},
            {"field": "compendium_hospital_id", "values": [], "status": "row_level_in_hospitals", "used_by": [_AHRQ_DATASET]},
            {"field": "ccn", "values": [], "status": "facility_join_key_not_campus_identity", "used_by": [_AHRQ_DATASET, _HGI_DATASET, _POS_DATASET]},
        ],
        "conflict_policy": [
            "Use health_sys_id for AHRQ system joins.",
            "Use compendium_hospital_id as the AHRQ hospital linkage row identity.",
            "Use CCN as a facility join key, not guaranteed campus-level identity.",
        ],
    }


def _error_payload(code: str, message: str, *, mode: UniverseMode, snapshot_id: str, data: dict[str, Any]) -> dict[str, Any]:
    return {
        **universe_metadata(mode, snapshot_id),
        "error": {"code": code, "message": message, "recoverable": True, "data": data},
        "source_metadata": source_metadata(mode=mode, include_overlay=mode == "latest_public_overlay"),
    }


def _encode_cursor(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: str | None) -> dict[str, Any]:
    if not cursor:
        return {}
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        return json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception:
        return {}


def _normalize_mode(value: str) -> UniverseMode:
    if value not in VALID_MODES:
        return "compendium_snapshot"
    return value  # type: ignore[return-value]


def _normalize_state_scope(value: str) -> StateScope:
    if value not in VALID_STATE_SCOPES:
        return "headquarters"
    return value  # type: ignore[return-value]


def _normalize_sort(value: str) -> SortKey:
    if value not in VALID_SORTS:
        return "health_sys_id"
    return value  # type: ignore[return-value]


def _state(value: str | None) -> str:
    return str(value or "").strip().upper()


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return None


def _find_col(frame: pd.DataFrame, *candidates: str) -> str:
    lower = {str(col).casefold(): str(col) for col in frame.columns}
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
        found = lower.get(candidate.casefold())
        if found:
            return found
    return ""


def _first_value(row: dict[str, Any], *candidates: str) -> str:
    lower = {str(key).casefold(): key for key in row}
    for candidate in candidates:
        key = candidate if candidate in row else lower.get(candidate.casefold())
        if key is None:
            continue
        value = row.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").casefold())).strip()
