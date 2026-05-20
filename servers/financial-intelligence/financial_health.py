"""Public financial-health normalization helpers.

The helpers in this module only surface public source fields. They intentionally
avoid deriving HFMA MAP revenue-cycle KPIs from HCRIS or HFMD fields.
"""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any
import zipfile

import pandas as pd
from shared.utils.bed_resolver import resolve_hospital_bed_source


DEFAULT_CACHE_ROOT = Path.home() / ".healthcare-data-mcp" / "cache"
AHRQ_HFMD_URL = "https://www.ahrq.gov/data/innovations/hfmd.html"

PROHIBITED_MAP_KPI_FIELDS = frozenset(
    {
        "clean_claim_rate",
        "denial_rate",
        "net_days_in_ar",
        "cost_to_collect",
        "dnfb",
        "aged_ar",
    }
)

PROHIBITED_MAP_KPI_ALIASES = PROHIBITED_MAP_KPI_FIELDS | {
    "clean_claims_rate",
    "clean_claim",
    "denials_rate",
    "days_in_accounts_receivable",
    "days_in_ar",
    "ar_days",
    "accounts_receivable_days",
    "net_ar_days",
    "cost_to_collect_rate",
    "discharged_not_final_billed",
    "discharged_not_final_bill",
    "aged_accounts_receivable",
    "aged_receivables",
}

HCRIS_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "beds": (
        "beds",
        "total_beds",
        "bed_count",
        "hospital_beds",
        "number_of_beds",
    ),
    "bed_days_available": (
        "bed_days_available",
        "total_bed_days_available",
        "available_bed_days",
        "total_available_bed_days",
    ),
    "discharges": (
        "total_discharges",
        "discharges",
        "medicare_discharges",
        "total_hospital_discharges",
        "total_discharges_all_patients",
    ),
    "inpatient_days": (
        "total_inpatient_days",
        "inpatient_days",
        "days_of_care",
        "total_patient_days",
        "adult_and_pediatric_days",
    ),
    "net_patient_revenue": (
        "net_patient_revenue",
        "net_pat_rev",
        "net_revenue",
        "net_patient_service_revenue",
        "net_patient_revenue_amount",
    ),
    "total_margin": (
        "total_margin",
        "tot_margin",
        "total_margin_percent",
        "total_margin_percentage",
    ),
    "operating_margin": (
        "operating_margin",
        "op_margin",
        "operating_margin_percent",
        "operating_margin_percentage",
    ),
    "uncompensated_care_cost": (
        "uncompensated_care_cost",
        "total_uncompensated_care_cost",
        "worksheet_s_10_uncompensated_care_cost",
        "worksheet_s_10_total_uncompensated_care_cost",
        "worksheet_s10_uncompensated_care_cost",
        "worksheet_s10_total_uncompensated_care_cost",
        "s_10_uncompensated_care_cost",
        "s10_uncompensated_care_cost",
        "s10_total_uncompensated_care_cost",
        "cost_of_uncompensated_care",
    ),
    "charity_care_cost": (
        "charity_care_cost",
        "cost_of_charity_care",
        "s_10_charity_care_cost",
        "s10_charity_care",
        "s10_charity_care_cost",
        "worksheet_s_10_charity_care_cost",
        "worksheet_s10_charity_care_cost",
    ),
    "bad_debt_expense": (
        "bad_debt_expense",
        "bad_debt",
        "medicare_bad_debt",
        "s_10_bad_debt_expense",
        "s10_bad_debt",
        "s10_bad_debt_expense",
        "worksheet_s_10_bad_debt_expense",
        "worksheet_s10_bad_debt_expense",
    ),
    "medicaid_shortfall": (
        "medicaid_shortfall",
        "medicaid_deficit",
        "medicaid_shortfall_or_surplus",
        "s10_medicaid_shortfall",
    ),
    "medicare_shortfall": (
        "medicare_shortfall",
        "medicare_deficit",
        "medicare_shortfall_or_surplus",
        "s10_medicare_shortfall",
    ),
}

HFMD_METRIC_ALIASES: dict[str, tuple[str, ...]] = {
    "operating_margin": (
        "operating_margin",
        "operating_margin_percent",
        "operating_margin_percentage",
    ),
    "total_margin": (
        "total_margin",
        "total_margin_percent",
        "total_margin_percentage",
        "hfmd05",
    ),
    "net_patient_revenue": (
        "net_patient_revenue",
        "net_patient_service_revenue",
        "net_patient_revenue_amount",
    ),
    "total_revenue": (
        "total_revenue",
        "total_operating_revenue",
        "total_operating_revenues",
        "operating_revenue",
    ),
    "total_expenses": (
        "total_expenses",
        "total_operating_expense",
        "total_operating_expenses",
        "operating_expense",
    ),
    "total_assets": ("total_assets",),
    "total_liabilities": ("total_liabilities",),
    "quick_ratio": ("quick_ratio", "hfmd09"),
    "days_cash_on_hand": ("days_cash_on_hand", "cash_on_hand_days"),
    "debt_service_coverage_ratio": (
        "debt_service_coverage_ratio",
        "debt_service_coverage",
    ),
    "net_revenue_margin_to_total_cost": ("hfmd01",),
    "net_revenue_margin_to_patient_revenue": ("hfmd02",),
    "net_income_to_equity": ("hfmd03",),
    "net_income_to_total_fixed_assets": ("hfmd04",),
    "average_age_of_plant": ("hfmd06",),
    "debt_burden_to_total_assets": ("hfmd07",),
    "current_ratio": ("current_ratio", "hfmd08"),
    "uncompensated_care_burden_to_total_cost": ("hfmd10",),
    "unreimbursed_and_uncompensated_care_burden_to_total_cost": ("hfmd11",),
}

CCN_ALIASES = (
    "facility_id",
    "ccn",
    "provider_ccn",
    "provider_id",
    "provider_number",
    "cms_certification_number",
    "medicare_provider_number",
    "medicare_provider_id",
    "prvdr_num",
)
FY_END_ALIASES = (
    "fiscal_year_end",
    "fy_end",
    "fy_end_date",
    "fy_end_dt",
    "fiscal_year_end_date",
    "fiscal_year_end_dt",
)
YEAR_ALIASES = ("fiscal_year", "fy", "report_year", "cost_report_year", "year")
FACILITY_NAME_ALIASES = ("facility_name", "hospital_name", "provider_name", "name")
STATE_ALIASES = ("state", "provider_state", "hospital_state")


def normalize_hcris_public_metrics(row: Any, requested_ccn: str = "") -> dict[str, Any]:
    """Normalize public HCRIS/S-10 fields with per-metric confidence."""
    normalized = _normalized_row(row)
    metrics = {
        metric_name: _metric_payload(normalized, aliases, "high_reported_hcris_field")
        for metric_name, aliases in HCRIS_METRIC_ALIASES.items()
    }

    bed_source = resolve_hospital_bed_source(
        ccn=_first_text(normalized, CCN_ALIASES) or requested_ccn,
        hcris_row=normalized,
        target_scope="ccn",
    )
    if bed_source["selected_bed_count"] is not None:
        metrics["beds"] = {
            "value": bed_source["selected_bed_count"],
            "confidence": bed_source["confidence"],
            "source_field": bed_source["selected_source_field"],
        }

    fiscal_year_end = _first_text(normalized, FY_END_ALIASES)
    ccn = _first_text(normalized, CCN_ALIASES) or requested_ccn
    profile: dict[str, Any] = {
        "ccn": ccn,
        "source": "CMS Hospital Cost Report PUF / HCRIS-derived public file",
        "source_status": "ready",
        "fiscal_year_end": fiscal_year_end,
        "metrics": metrics,
        "bed_source": bed_source,
        "metric_confidence": {name: metric["confidence"] for name, metric in metrics.items()},
    }
    for name, metric in metrics.items():
        profile[name] = metric["value"]
    return _strip_prohibited(profile)


def load_ahrq_hfmd_profile(
    *,
    ccn: str = "",
    state: str = "",
    cache_root: Path | None = None,
    limit: int = 25,
) -> dict[str, Any]:
    """Load and normalize cached AHRQ HFMD CSV/ZIP artifacts without acquiring data."""
    cache_dir = _hfmd_cache_dir(cache_root)
    artifacts = _hfmd_artifacts(cache_dir)
    if not artifacts:
        return {
            "source_id": "ahrq_hfmd",
            "source_name": "AHRQ Hospital Financial Measures Database",
            "source_url": AHRQ_HFMD_URL,
            "source_status": "cache_not_found",
            "cache_path": str(cache_dir),
            "record_count": 0,
            "matched_count": 0,
            "metrics": {},
            "metric_confidence": {},
            "records": [],
        }

    frames = _read_hfmd_artifacts(artifacts)
    if not frames:
        return {
            "source_id": "ahrq_hfmd",
            "source_name": "AHRQ Hospital Financial Measures Database",
            "source_url": AHRQ_HFMD_URL,
            "source_status": "unparseable_cache",
            "cache_path": str(cache_dir),
            "record_count": 0,
            "matched_count": 0,
            "metrics": {},
            "metric_confidence": {},
            "records": [],
        }

    df = pd.concat(frames, ignore_index=True)
    source_columns = [column for column in df.columns if column.startswith("_hfmd_source")]
    dedupe_columns = [column for column in df.columns if column not in source_columns]
    if dedupe_columns:
        df = df.drop_duplicates(subset=dedupe_columns, keep="first")
    matches = _filter_hfmd_rows(df, ccn=ccn, state=state)
    records = [
        _normalize_hfmd_row(row, requested_ccn=ccn, state=state)
        for _, row in matches.head(limit).iterrows()
    ]
    selected = records[0] if records else {}
    metrics = selected.get("metrics", {}) if selected else {}
    matched_on = selected.get("matched_on", "") if selected else ""
    return _strip_prohibited(
        {
            "source_id": "ahrq_hfmd",
            "source_name": "AHRQ Hospital Financial Measures Database",
            "source_url": AHRQ_HFMD_URL,
            "source_status": "ready" if records else "no_match",
            "cache_path": str(cache_dir),
            "record_count": int(len(df)),
            "matched_count": int(len(matches)),
            "ccn": selected.get("ccn", ccn) if selected else ccn,
            "state": selected.get("state", state.upper()) if selected else state.upper(),
            "matched_on": matched_on,
            "join_keys": {
                "ccn": ccn,
                "hfmd_provider_id": selected.get("provider_id", "") if selected else "",
                "matched_on": matched_on,
            },
            "metrics": metrics,
            "metric_confidence": {name: metric["confidence"] for name, metric in metrics.items()},
            "records": records,
        }
    )


def _hfmd_cache_dir(cache_root: Path | None) -> Path:
    return (cache_root or DEFAULT_CACHE_ROOT).expanduser() / "state-health-data" / "ahrq-hfmd"


def _hfmd_artifacts(cache_dir: Path) -> list[Path]:
    if not cache_dir.exists():
        return []
    artifacts = [
        path
        for path in cache_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in {".csv", ".zip"}
    ]
    return sorted(artifacts)


def _read_hfmd_artifacts(artifacts: list[Path]) -> list[pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    for artifact in artifacts:
        try:
            if artifact.suffix.lower() == ".csv":
                frame = pd.read_csv(artifact, dtype=str, keep_default_na=False)
                frame["_hfmd_source_file"] = str(artifact)
                frames.append(frame)
                continue
            with zipfile.ZipFile(artifact) as zf:
                for member in zf.namelist():
                    if not member.lower().endswith(".csv"):
                        continue
                    with zf.open(member) as fp:
                        frame = pd.read_csv(fp, dtype=str, keep_default_na=False)
                    frame["_hfmd_source_file"] = str(artifact)
                    frame["_hfmd_source_member"] = member
                    frames.append(frame)
        except Exception:
            continue
    return frames


def _filter_hfmd_rows(df: pd.DataFrame, *, ccn: str = "", state: str = "") -> pd.DataFrame:
    matches = df
    provider_cols = _candidate_columns(df, CCN_ALIASES, provider_like_fallback=True)
    if ccn and provider_cols:
        wanted = _normalize_provider_id(ccn)
        mask = pd.Series(False, index=df.index)
        for col in provider_cols:
            mask = mask | df[col].astype(str).map(_normalize_provider_id).eq(wanted)
        matches = df[mask]
        if not matches.empty:
            return matches

    if state:
        state_cols = _candidate_columns(df, STATE_ALIASES)
        if state_cols:
            wanted_state = state.strip().upper()
            mask = pd.Series(False, index=matches.index)
            for col in state_cols:
                mask = mask | matches[col].astype(str).str.strip().str.upper().eq(wanted_state)
            matches = matches[mask]
    return matches


def _normalize_hfmd_row(row: Any, *, requested_ccn: str = "", state: str = "") -> dict[str, Any]:
    normalized = _normalized_row(row)
    provider_id = _first_text(normalized, CCN_ALIASES)
    matched_on = "ccn" if requested_ccn and _normalize_provider_id(provider_id) == _normalize_provider_id(requested_ccn) else ""
    if not matched_on and state and _first_text(normalized, STATE_ALIASES).upper() == state.upper():
        matched_on = "state"
    confidence = "high_reported_hfmd_ccn_match" if matched_on == "ccn" else "medium_reported_hfmd_field"
    metrics = {
        metric_name: _metric_payload(normalized, aliases, confidence)
        for metric_name, aliases in HFMD_METRIC_ALIASES.items()
    }
    metrics = {name: metric for name, metric in metrics.items() if metric["value"] is not None}
    source_file = _first_text(normalized, ("_hfmd_source_file",))
    record = {
        "ccn": provider_id,
        "provider_id": provider_id,
        "facility_name": _first_text(normalized, FACILITY_NAME_ALIASES),
        "state": _first_text(normalized, STATE_ALIASES),
        "fiscal_year": _first_text(normalized, YEAR_ALIASES),
        "source_file": source_file,
        "source_member": _first_text(normalized, ("_hfmd_source_member",)),
        "matched_on": matched_on,
        "metrics": metrics,
        "metric_confidence": {name: metric["confidence"] for name, metric in metrics.items()},
    }
    return _strip_prohibited(record)


def _metric_payload(normalized: dict[str, tuple[str, Any]], aliases: tuple[str, ...], confidence: str) -> dict[str, Any]:
    source_field, raw = _first_value(normalized, aliases)
    value = _safe_float(raw)
    return {
        "value": value,
        "confidence": confidence if value is not None else "not_available",
        "source_field": source_field,
    }


def _normalized_row(row: Any) -> dict[str, tuple[str, Any]]:
    if hasattr(row, "items"):
        items = row.items()
    elif hasattr(row, "to_dict"):
        items = row.to_dict().items()
    else:
        items = []
    normalized: dict[str, tuple[str, Any]] = {}
    for key, value in items:
        normalized_key = _normalize_key(str(key))
        normalized.setdefault(normalized_key, (str(key), value))
    return normalized


def _candidate_columns(df: pd.DataFrame, aliases: tuple[str, ...], *, provider_like_fallback: bool = False) -> list[str]:
    alias_set = {_normalize_key(alias) for alias in aliases}
    exact = [col for col in df.columns if _normalize_key(str(col)) in alias_set]
    if exact or not provider_like_fallback:
        return exact
    provider_like = []
    for col in df.columns:
        normalized = _normalize_key(str(col))
        if "provider" in normalized and ("id" in normalized or "number" in normalized or "ccn" in normalized):
            provider_like.append(col)
    return provider_like


def _first_text(normalized: dict[str, tuple[str, Any]], aliases: tuple[str, ...]) -> str:
    _, value = _first_value(normalized, aliases)
    return "" if value is None else str(value).strip()


def _first_value(normalized: dict[str, tuple[str, Any]], aliases: tuple[str, ...]) -> tuple[str, Any]:
    for alias in aliases:
        key = _normalize_key(alias)
        if key not in normalized:
            continue
        source_field, value = normalized[key]
        if value is None or str(value).strip() == "":
            continue
        return source_field, value
    return "", None


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _normalize_provider_id(value: Any) -> str:
    text = re.sub(r"\D", "", "" if value is None else str(value))
    return text.lstrip("0") or text


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"not available", "n/a", "na", "too few", "null", "none"}:
        return None
    is_negative = text.startswith("(") and text.endswith(")")
    text = text.strip("()").replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    return -parsed if is_negative else parsed


def _strip_prohibited(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned = {}
        for key, child in value.items():
            if _normalize_key(str(key)) in PROHIBITED_MAP_KPI_ALIASES:
                continue
            cleaned[key] = _strip_prohibited(child)
        return cleaned
    if isinstance(value, list):
        return [_strip_prohibited(item) for item in value]
    return value
