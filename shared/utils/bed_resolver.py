"""Hospital bed-source resolution with provenance and scope controls."""

from __future__ import annotations

from math import isfinite
from typing import Any, Literal, NotRequired, TypedDict

import pandas as pd


RowScope = Literal["ccn", "campus", "license", "system"]


class BedSourceCandidate(TypedDict):
    source: str
    source_field: str
    raw_value: Any
    value_kind: str
    row_scope: RowScope
    ccn: str
    state_facility_id: str
    source_period: str
    fiscal_year_end: str
    confidence: str
    exact_ccn_match: bool
    source_artifact: str
    selected_bed_count: float | int | None
    rejection_reason: NotRequired[str]


class BedResolutionPayload(TypedDict):
    ccn: str
    state_facility_id: str
    state: str
    year: int
    target_scope: RowScope
    selected_bed_count: float | int | None
    selected_source: str
    selected_source_field: str
    source_period: str
    fiscal_year_end: str
    row_scope: str
    confidence: str
    candidates: list[BedSourceCandidate]
    rejected_candidates: list[BedSourceCandidate]
    warnings: list[str]
    scope_policy: dict[str, Any]


SUPPORTED_ROW_SCOPES = {"ccn", "campus", "license", "system"}
BED_COUNT_CEILINGS = {
    "ccn": 5_000,
    "campus": 5_000,
    "license": 10_000,
    "system": 100_000,
}
MATERIAL_DIFFERENCE_RATIO = 0.20


def resolve_hospital_bed_source(
    *,
    ccn: str = "",
    state_facility_id: str = "",
    state: str = "",
    year: int = 0,
    target_scope: str = "ccn",
    pos_row: Any | None = None,
    hcris_row: Any | None = None,
    ahrq_row: dict[str, Any] | None = None,
    pa_rows: list[dict[str, Any]] | None = None,
) -> BedResolutionPayload:
    """Resolve the best available bed count and retain rejected candidates."""

    scope = _normalize_scope(target_scope)
    candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    _add_pos_candidates(candidates, rejected, pos_row, ccn=ccn, year=year)
    _add_hcris_candidates(candidates, rejected, hcris_row, ccn=ccn, year=year)
    _add_pa_candidates(candidates, rejected, pa_rows or [], ccn=ccn, state_facility_id=state_facility_id, state=state, year=year)
    _add_ahrq_fallback(candidates, rejected, ahrq_row or {}, ccn=ccn, year=year)

    scoped_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        if _scope_compatible(candidate, scope):
            scoped_candidates.append(candidate)
        else:
            rejected.append({**candidate, "rejection_reason": f"row_scope_{candidate['row_scope']}_not_compatible_with_target_{scope}"})

    scoped_candidates.sort(key=lambda item: (item["rank"], -float(item["selected_bed_count"] or 0)))
    selected = scoped_candidates[0] if scoped_candidates else None
    warnings = _build_warnings(scoped_candidates, selected)

    return {
        "ccn": _normalize_ccn(ccn),
        "state_facility_id": state_facility_id,
        "state": state.upper() if state else "",
        "year": year or 0,
        "target_scope": scope,
        "selected_bed_count": selected["selected_bed_count"] if selected else None,
        "selected_source": selected["source"] if selected else "",
        "selected_source_field": selected["source_field"] if selected else "",
        "source_period": selected["source_period"] if selected else "",
        "fiscal_year_end": selected["fiscal_year_end"] if selected else "",
        "row_scope": selected["row_scope"] if selected else "",
        "confidence": selected["confidence"] if selected else "not_available",
        "candidates": [_public_candidate(candidate) for candidate in scoped_candidates],
        "rejected_candidates": [_public_candidate(candidate) for candidate in rejected],
        "warnings": warnings,
        "scope_policy": {
            "supported_scopes": sorted(SUPPORTED_ROW_SCOPES),
            "target_scope": scope,
            "rule": "Rows are selected only when row scope matches the target scope. CCN targets require CCN-scope rows; campus/license substitutions require an exact CCN mapping and remain non-system scope.",
        },
    }


def _add_pos_candidates(
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    row: Any | None,
    *,
    ccn: str,
    year: int,
) -> None:
    if _row_empty(row):
        return
    for field, label, rank in (
        ("BED_CNT", "CMS Provider of Services", 30),
        ("CRTFD_BED_CNT", "CMS Provider of Services", 35),
    ):
        _append_candidate(
            candidates,
            rejected,
            raw_value=_value(row, field),
            source=label,
            source_field=field,
            row_scope="ccn",
            rank=rank,
            ccn=ccn,
            source_period=str(year or ""),
            fiscal_year_end="",
            confidence="high_reported_pos_field",
        )


def _add_hcris_candidates(
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    row: Any | None,
    *,
    ccn: str,
    year: int,
) -> None:
    if _row_empty(row):
        return
    fiscal_year_end = _first_text(row, "fiscal_year_end", "fy_end", "fy_end_dt", "fiscal_year_end_date", "fiscal_year_end_dt")
    source_period = str(year or _first_text(row, "fiscal_year", "fy", "report_year", "cost_report_year", "year"))
    for field in ("beds", "total_beds", "bed_count", "hospital_beds", "number_of_beds"):
        _append_candidate(
            candidates,
            rejected,
            raw_value=_value(row, field),
            source="CMS Hospital Cost Report PUF / HCRIS-derived public file",
            source_field=field,
            row_scope="ccn",
            rank=10,
            ccn=ccn,
            source_period=source_period,
            fiscal_year_end=fiscal_year_end,
            confidence="high_reported_hcris_field",
        )
    for field in ("bed_days_available", "total_bed_days_available", "available_bed_days", "total_available_bed_days"):
        _append_candidate(
            candidates,
            rejected,
            raw_value=_value(row, field),
            source="CMS Hospital Cost Report PUF / HCRIS-derived public file",
            source_field=field,
            row_scope="ccn",
            rank=20,
            ccn=ccn,
            source_period=source_period,
            fiscal_year_end=fiscal_year_end,
            confidence="medium_derived_from_bed_days_available",
            value_kind="bed_days",
        )


def _add_pa_candidates(
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    rows: list[dict[str, Any]],
    *,
    ccn: str,
    state_facility_id: str,
    state: str,
    year: int,
) -> None:
    for row in rows:
        row_scope = _normalize_scope(str(row.get("row_scope") or "license"))
        row_ccn = _normalize_ccn(row.get("ccn"))
        exact_ccn = bool(row_ccn and ccn and row_ccn == _normalize_ccn(ccn))
        if ccn and row_ccn and not exact_ccn:
            continue
        row_state_id = str(row.get("state_facility_id") or row.get("license_id") or "").strip()
        if state_facility_id and row_state_id and row_state_id != state_facility_id:
            continue
        row_state = str(row.get("state") or "").upper()
        if state and row_state and row_state != state.upper():
            continue
        row_year = _int_or_none(row.get("report_year") or row.get("year") or row.get("fiscal_year"))
        if year and row_year and row_year != int(year):
            continue
        _append_candidate(
            candidates,
            rejected,
            raw_value=row.get("metric_value"),
            source=str(row.get("source") or "Pennsylvania Department of Health Hospital Reports"),
            source_field=str(row.get("raw_column") or row.get("source_field") or row.get("metric_name") or ""),
            row_scope=row_scope,
            rank=50,
            ccn=row_ccn or ccn,
            state_facility_id=row_state_id,
            source_period=str(row_year or year or ""),
            fiscal_year_end=str(row.get("fiscal_year_end") or ""),
            confidence=str(row.get("confidence") or "medium_structured_state_extract"),
            value_kind="bed_days" if str(row.get("metric_name") or "") == "bed_days_available" else "bed_count",
            exact_ccn_match=exact_ccn,
            source_artifact=str(row.get("source_artifact") or row.get("artifact_url") or ""),
        )


def _add_ahrq_fallback(
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    ccn: str,
    year: int,
) -> None:
    if not row:
        return
    _append_candidate(
        candidates,
        rejected,
        raw_value=row.get("hos_beds") or row.get("beds"),
        source="AHRQ Compendium hospital linkage",
        source_field="hos_beds" if row.get("hos_beds") not in (None, "") else "beds",
        row_scope="ccn",
        rank=70,
        ccn=ccn,
        source_period=str(year or ""),
        fiscal_year_end="",
        confidence="medium_high_for_linked_attribute",
    )


def _append_candidate(
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    *,
    raw_value: Any,
    source: str,
    source_field: str,
    row_scope: str,
    rank: int,
    ccn: str = "",
    state_facility_id: str = "",
    source_period: str = "",
    fiscal_year_end: str = "",
    confidence: str,
    value_kind: str = "bed_count",
    exact_ccn_match: bool = True,
    source_artifact: str = "",
) -> None:
    if raw_value in (None, ""):
        return
    parsed = _float_or_none(raw_value)
    candidate = {
        "source": source,
        "source_field": source_field,
        "raw_value": raw_value,
        "value_kind": value_kind,
        "row_scope": _normalize_scope(row_scope),
        "rank": rank,
        "ccn": _normalize_ccn(ccn),
        "state_facility_id": state_facility_id,
        "source_period": source_period,
        "fiscal_year_end": fiscal_year_end,
        "confidence": confidence,
        "exact_ccn_match": exact_ccn_match,
        "source_artifact": source_artifact,
    }
    if parsed is None:
        rejected.append({**candidate, "selected_bed_count": None, "rejection_reason": "non_numeric_bed_value"})
        return
    if value_kind == "bed_days":
        selected_value = parsed / 365
    else:
        selected_value = parsed
    reason = _bed_rejection_reason(selected_value, candidate["row_scope"], integer_expected=value_kind != "bed_days")
    normalized_value = round(float(selected_value), 4) if value_kind == "bed_days" else int(selected_value)
    payload = {**candidate, "selected_bed_count": normalized_value}
    if reason:
        rejected.append({**payload, "rejection_reason": reason})
        return
    candidates.append(payload)


def _bed_rejection_reason(value: float, row_scope: str, *, integer_expected: bool) -> str:
    if not isfinite(value):
        return "non_finite_bed_value"
    if value < 0:
        return "negative_bed_value"
    if value == 0:
        return "zero_bed_value"
    if integer_expected and not float(value).is_integer():
        return "fractional_direct_bed_count"
    ceiling = BED_COUNT_CEILINGS.get(row_scope, BED_COUNT_CEILINGS["ccn"])
    if value > ceiling:
        return f"bed_value_above_{row_scope}_ceiling_{ceiling}"
    return ""


def _scope_compatible(candidate: dict[str, Any], target_scope: str) -> bool:
    row_scope = candidate.get("row_scope", "")
    if row_scope == target_scope:
        return True
    if target_scope == "ccn":
        return bool(candidate.get("exact_ccn_match") and row_scope == "ccn")
    return bool(candidate.get("exact_ccn_match") and target_scope in {"campus", "license"} and row_scope in {"campus", "license"})


def _build_warnings(candidates: list[dict[str, Any]], selected: dict[str, Any] | None) -> list[str]:
    if not selected:
        return ["No valid bed-source candidate matched the requested row scope."]
    warnings: list[str] = []
    selected_value = float(selected["selected_bed_count"])
    for candidate in candidates:
        if candidate is selected:
            continue
        other_value = float(candidate["selected_bed_count"])
        if _sources_family(candidate["source"]) == _sources_family(selected["source"]):
            continue
        diff_ratio = abs(selected_value - other_value) / max(selected_value, other_value)
        if diff_ratio >= MATERIAL_DIFFERENCE_RATIO:
            warnings.append(
                f"Material bed-source variance: selected {selected['source']} {selected_value:g} vs "
                f"{candidate['source']} {other_value:g}."
            )
    return warnings


def _sources_family(source: str) -> str:
    lower = source.lower()
    if "provider of services" in lower:
        return "pos"
    if "hcris" in lower or "cost report" in lower:
        return "hcris"
    if "pennsylvania" in lower:
        return "pa"
    if "ahrq" in lower:
        return "ahrq"
    return lower


def _public_candidate(candidate: dict[str, Any]) -> BedSourceCandidate:
    return {key: value for key, value in candidate.items() if key != "rank"}


def _row_empty(row: Any | None) -> bool:
    if row is None:
        return True
    if isinstance(row, pd.Series):
        return row.empty
    if isinstance(row, dict):
        return not row
    return False


def _value(row: Any, key: str) -> Any:
    if row is None:
        return None
    if hasattr(row, "get"):
        return row.get(key)
    try:
        return row[key]
    except (KeyError, TypeError):
        return None


def _first_text(row: Any, *keys: str) -> str:
    for key in keys:
        raw = _value(row, key)
        if raw not in (None, ""):
            return str(raw).strip()
    return ""


def _float_or_none(value: Any) -> float | None:
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    number = _float_or_none(value)
    if number is None:
        return None
    return int(number)


def _normalize_scope(scope: str) -> str:
    normalized = str(scope or "ccn").strip().lower().replace("-", "_")
    return normalized if normalized in SUPPORTED_ROW_SCOPES else "ccn"


def _normalize_ccn(value: Any) -> str:
    text = str(value or "").strip()
    return text.zfill(6) if text else ""
