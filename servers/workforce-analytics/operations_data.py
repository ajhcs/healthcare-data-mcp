"""Public hospital operations and throughput metric extraction helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from importlib import import_module
import json
from pathlib import Path
from typing import Any

from shared.utils.cost_report import cr_safe_float


HospitalRowLoader = Callable[[str], Awaitable[dict[str, Any]]]
CostReportRowLoader = Callable[[str, int], Awaitable[Any | None]]


def ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den in (None, 0):
        return None
    return round(float(num) / float(den), 4)


def dict_float(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        raw = row.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(str(raw).replace(",", ""))
        except (TypeError, ValueError):
            continue
    return None


def series_float(row: Any, *keys: str) -> float | None:
    if row is None:
        return None
    return cr_safe_float(row, *keys)


def _dict_value(row: dict[str, Any], source: str, confidence: str, *keys: str) -> tuple[float | None, dict[str, str]]:
    for key in keys:
        value = dict_float(row, key)
        if value is not None:
            return value, {"source": source, "source_field": key, "confidence": confidence}
    return None, {}


def _series_value(row: Any, source: str, confidence: str, *keys: str) -> tuple[float | None, dict[str, str]]:
    for key in keys:
        value = series_float(row, key)
        if value is not None:
            return value, {"source": source, "source_field": key, "confidence": confidence}
    return None, {}


def _first_metric(*candidates: tuple[float | None, dict[str, str]]) -> tuple[float | None, dict[str, str]]:
    for value, metadata in candidates:
        if value is not None:
            return value, metadata
    return None, {}


def _metric_confidence(metric: str, metadata: dict[str, str]) -> dict[str, str]:
    if metadata:
        return {"metric": metric, **metadata}
    return {"metric": metric, "source": "", "source_field": "", "confidence": "not_available"}


def _source_rankings() -> list[dict[str, Any]]:
    return [
        {
            "rank": 1,
            "source": "CMS Hospital Cost Report PUF / HCRIS-derived public file",
            "confidence": "high_for_reported_provider_year_fields",
            "fields": ["beds", "bed_days_available", "discharges", "patient_days"],
        },
        {
            "rank": 2,
            "source": "AHRQ Compendium hospital linkage",
            "confidence": "medium_high_for_linked_facility_attributes",
            "fields": ["beds", "discharges", "hospital_name", "state"],
        },
        {
            "rank": 3,
            "source": "public state health normalized extracts",
            "confidence": "high_when_structured_table_row_matches_facility",
            "fields": ["inpatient_admissions_from_ed", "ed_visits", "procedure_volumes"],
        },
    ]


async def throughput_profile(
    *,
    ccn: str = "",
    state_facility_id: str = "",
    state: str = "",
    year: int = 0,
    hospital_row_loader: HospitalRowLoader,
    cost_report_row_loader: CostReportRowLoader,
) -> dict[str, Any]:
    """Build public throughput metrics with source provenance and confidence metadata."""
    selected_ccn = ccn or state_facility_id
    row = await hospital_row_loader(selected_ccn)
    cost_row = await cost_report_row_loader(selected_ccn, year)

    bed_days_available, bed_days_available_meta = _series_value(
        cost_row,
        "CMS Hospital Cost Report PUF / HCRIS-derived public file",
        "high_for_reported_provider_year_field",
        "bed_days_available",
        "total_bed_days_available",
    )
    beds, beds_meta = _first_metric(
        _series_value(
            cost_row,
            "CMS Hospital Cost Report PUF / HCRIS-derived public file",
            "high_for_reported_provider_year_field",
            "beds",
            "total_beds",
        ),
        _dict_value(row, "AHRQ Compendium hospital linkage", "medium_high_for_linked_attribute", "hos_beds", "beds"),
    )
    if beds is None and bed_days_available is not None:
        beds = ratio(bed_days_available, 365)
        beds_meta = {
            "source": "CMS Hospital Cost Report PUF / HCRIS-derived public file",
            "source_field": bed_days_available_meta.get("source_field", "bed_days_available"),
            "confidence": "medium_derived_from_bed_days_available",
        }

    discharges, discharges_meta = _first_metric(
        _series_value(
            cost_row,
            "CMS Hospital Cost Report PUF / HCRIS-derived public file",
            "high_for_reported_provider_year_field",
            "total_discharges",
            "discharges",
            "total_hospital_discharges",
            "medicare_discharges",
        ),
        _dict_value(row, "AHRQ Compendium hospital linkage", "medium_high_for_linked_attribute", "hos_dsch", "discharges"),
    )
    patient_days, patient_days_meta = _first_metric(
        _series_value(
            cost_row,
            "CMS Hospital Cost Report PUF / HCRIS-derived public file",
            "high_for_reported_provider_year_field",
            "total_inpatient_days",
            "inpatient_days",
            "days_of_care",
            "total_patient_days",
        ),
        _dict_value(row, "AHRQ Compendium hospital linkage", "medium_when_available", "patient_days", "days_of_care", "inpatient_days"),
    )
    bed_days = bed_days_available or (beds * 365 if beds else None)

    state_code = state.upper() if state else str(row.get("hosp_state", "") or row.get("state", "")).upper()
    admissions_enhancement = await _pa_admissions_enhancement(
        state=state_code,
        hospital_name=str(row.get("hospital_name", "")),
        year=year,
    )
    inpatient_admissions_from_ed = dict_float(row, "inpatient_admissions_from_ed")
    admissions_meta = _metric_confidence(
        "inpatient_admissions_from_ed",
        {"source": "AHRQ Compendium hospital linkage", "source_field": "inpatient_admissions_from_ed", "confidence": "medium_when_available"}
        if inpatient_admissions_from_ed is not None
        else {},
    )
    if admissions_enhancement and admissions_enhancement.get("inpatient_admissions") is not None:
        inpatient_admissions_from_ed = admissions_enhancement["inpatient_admissions"]
        admissions_meta = _metric_confidence(
            "inpatient_admissions_from_ed",
            {
                "source": "PHC4 normalized public report table",
                "source_field": admissions_enhancement.get("source_measure", ""),
                "confidence": admissions_enhancement.get("confidence", "medium_normalized_state_match"),
            },
        )

    ed_visits, ed_visits_meta = _dict_value(
        row,
        "AHRQ Compendium hospital linkage",
        "medium_when_available",
        "ed_visits",
        "emergency_department_visits",
    )
    or_procedure_volumes, or_procedure_meta = _dict_value(
        row,
        "AHRQ Compendium hospital linkage",
        "medium_when_available",
        "or_procedure_volumes",
        "surgical_procedures",
    )
    ct_scans, ct_meta = _dict_value(row, "AHRQ Compendium hospital linkage", "medium_when_available", "ct_scans")
    mri_scans, mri_meta = _dict_value(row, "AHRQ Compendium hospital linkage", "medium_when_available", "mri_scans")
    cath_volumes, cath_meta = _dict_value(
        row,
        "AHRQ Compendium hospital linkage",
        "medium_when_available",
        "cardiac_catheterizations",
    )
    open_heart_volumes, open_heart_meta = _dict_value(
        row,
        "AHRQ Compendium hospital linkage",
        "medium_when_available",
        "open_heart_procedures",
    )

    metric_confidence = {
        "beds": _metric_confidence("beds", beds_meta),
        "bed_days_available": _metric_confidence("bed_days_available", bed_days_available_meta),
        "discharges": _metric_confidence("discharges", discharges_meta),
        "patient_days": _metric_confidence("patient_days", patient_days_meta),
        "occupancy_rate": _derived_metric_confidence("occupancy_rate", patient_days_meta, bed_days_available_meta or beds_meta),
        "average_length_of_stay": _derived_metric_confidence("average_length_of_stay", patient_days_meta, discharges_meta),
        "bed_turnover_rate": _derived_metric_confidence("bed_turnover_rate", discharges_meta, beds_meta),
        "discharges_per_staffed_bed": _derived_metric_confidence("discharges_per_staffed_bed", discharges_meta, beds_meta),
        "ed_visits": _metric_confidence("ed_visits", ed_visits_meta),
        "inpatient_admissions_from_ed": admissions_meta,
        "or_procedure_volumes": _metric_confidence("or_procedure_volumes", or_procedure_meta),
        "ct_scans": _metric_confidence("ct_scans", ct_meta),
        "mri_scans": _metric_confidence("mri_scans", mri_meta),
        "cardiac_catheterizations": _metric_confidence("cardiac_catheterizations", cath_meta),
        "open_heart_procedures": _metric_confidence("open_heart_procedures", open_heart_meta),
    }
    return {
        "ccn": ccn,
        "year": year or 0,
        "state_facility_id": state_facility_id,
        "state": state_code,
        "hospital_name": str(row.get("hospital_name", "")),
        "source": "CMS Cost Report PUF/HCRIS-derived fields, AHRQ linkage, and cached public state extracts",
        "source_rankings": _source_rankings(),
        "occupancy_rate": ratio(patient_days, bed_days),
        "average_length_of_stay": ratio(patient_days, discharges),
        "bed_turnover_rate": ratio(discharges, beds),
        "discharges_per_staffed_bed": ratio(discharges, beds),
        "ed_visits": ed_visits,
        "inpatient_admissions_from_ed": inpatient_admissions_from_ed,
        "or_procedure_volumes": or_procedure_volumes,
        "ct_mri_cath_open_heart_volumes": {
            "ct": ct_scans,
            "mri": mri_scans,
            "cath": cath_volumes,
            "open_heart": open_heart_volumes,
        },
        "metric_confidence": metric_confidence,
        "pa_admissions_enhancement": admissions_enhancement,
        "confidence": "high_when_reported_cost_report_field_present",
    }


def _derived_metric_confidence(metric: str, *inputs: dict[str, str]) -> dict[str, str]:
    if all(metadata for metadata in inputs):
        confidence = "high_derived_from_reported_fields"
        if any(str(metadata.get("confidence", "")).startswith("medium") for metadata in inputs):
            confidence = "medium_derived_from_public_fields"
        sources = sorted({metadata.get("source", "") for metadata in inputs if metadata.get("source")})
        fields = sorted({metadata.get("source_field", "") for metadata in inputs if metadata.get("source_field")})
        return {
            "metric": metric,
            "source": "; ".join(sources),
            "source_field": "; ".join(fields),
            "confidence": confidence,
        }
    return _metric_confidence(metric, {})


async def _pa_admissions_enhancement(*, state: str, hospital_name: str, year: int) -> dict[str, Any] | None:
    if state != "PA":
        return None
    state_health_data = _load_state_health_data()
    if state_health_data is None or not _phc4_normalized_tables_exist(state_health_data):
        return None

    profile = await state_health_data.phc4_report_profile(
        hospital_name=hospital_name,
        year=year,
        report_type="hospital_performance",
    )
    rows = profile.get("table_rows") or []
    admission_row = _find_admissions_row(rows)
    if not admission_row:
        return {
            "source": "PHC4 normalized public report table",
            "confidence": profile.get("confidence", "no_public_report_match"),
            "matched_rows": 0,
        }
    return {
        "source": "PHC4 normalized public report table",
        "confidence": admission_row.get("confidence", profile.get("confidence", "medium_normalized_state_match")),
        "source_measure": admission_row.get("measure_name", ""),
        "inpatient_admissions": _parse_number(admission_row.get("measure_value")),
        "matched_rows": len(rows),
        "report_year": admission_row.get("report_year"),
        "source_artifact": admission_row.get("source_artifact", ""),
    }


def _load_state_health_data() -> Any | None:
    try:
        return import_module("shared.state_health_data")
    except Exception:
        return None


def _phc4_normalized_tables_exist(state_health_data: Any) -> bool:
    cache = Path(getattr(state_health_data, "PHC4_CACHE", ""))
    index = cache / "report_index.json"
    if not index.exists():
        return False
    try:
        records = json.loads(index.read_text(encoding="utf-8"))
    except Exception:
        return False
    return any(record.get("table_references") for record in records if isinstance(record, dict))


def _find_admissions_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        measure = str(row.get("measure_name", "")).lower()
        if "admission" in measure or "admitted" in measure:
            return row
    return None


def _parse_number(value: object) -> float | None:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
