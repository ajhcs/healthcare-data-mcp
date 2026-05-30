"""Hospital Quality & Performance MCP Server.

Provides tools for hospital quality metrics, readmission data, safety scores,
patient experience, and financial profiling from public CMS data.
"""

from typing import Any
import logging
import os

from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_name
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured

from . import data_loaders
from .models import (
    ConditionReadmission,
    DomainScores,
    ExperienceDomain,
    FinancialProfile,
    PatientExperience,
    QualityScores,
    ReadmissionData,
    SafetyScores,
)

logger = logging.getLogger(__name__)

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "hospital-quality"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8005"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "hospital-quality")


def _col(df, *candidates, default=""):
    """Find the first matching column name in a DataFrame."""
    for c in candidates:
        if c in df.columns:
            return c
    return default


def _safe_float(val) -> float | None:
    """Parse a string to float, returning None on failure."""
    if not val or str(val).strip().lower() in ("", "not available", "n/a", "too few"):
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Parse a string to int, returning None on failure."""
    f = _safe_float(val)
    return int(f) if f is not None else None


def _find_ccn_col(df):
    """Find the CCN/facility_id column in a DataFrame."""
    return _col(df, "facility_id", "ccn", "provider_id", "provider_ccn",
                "cms_certification_number", "provider_number", "prvdr_num")


def _filter_by_ccn(df, ccn: str):
    """Filter a DataFrame to rows matching the given CCN."""
    ccn_col = _find_ccn_col(df)
    if not ccn_col:
        return df.head(0)
    return df[df[ccn_col].str.strip() == ccn.strip()]


_QUALITY_MEASURE_ALIASES: dict[str, dict[str, Any]] = {
    "hcahps_communication_nurses": {
        "dataset": "hcahps",
        "measure_ids": ("H_COMP_1", "H-COMP-1"),
        "description": "HCAHPS communication with nurses",
        "match_prefix": True,
    },
    "ami_30_day_mortality": {
        "dataset": "complications",
        "measure_ids": ("MORT_30_AMI", "MORT-30-AMI"),
        "description": "CMS 30-day AMI mortality",
    },
    "hospital_wide_readmission": {
        "dataset": "unplanned_visits",
        "measure_ids": ("READM_30_HOSP_WIDE", "READM-30-HOSP-WIDE"),
        "description": "CMS hospital-wide 30-day readmission",
        "adjacent_tool": "get_readmission_data",
        "adjacent_dataset": "hrrp",
    },
    "clabsi_sir": {
        "dataset": "hai",
        "measure_ids": ("HAI_1_SIR", "HAI-1"),
        "description": "CMS CLABSI standardized infection ratio",
        "adjacent_tool": "get_safety_scores",
        "adjacent_dataset": "hac",
    },
}

_QUALITY_DATASET_LOADERS = {
    "hospital_info": "load_hospital_info",
    "hrrp": "load_hrrp",
    "hac": "load_hac",
    "hcahps": "load_hcahps",
    "complications": "load_complications",
    "hai": "load_hai",
    "unplanned_visits": "load_unplanned_visits",
}

_QUALITY_DATASET_NAMES = {
    "hospital_info": "CMS Hospital General Information",
    "hrrp": "CMS Hospital Readmissions Reduction Program",
    "hac": "CMS Hospital-Acquired Condition Reduction Program",
    "hcahps": "CMS HCAHPS - Hospital",
    "complications": "CMS Complications and Deaths - Hospital",
    "hai": "CMS Healthcare-Associated Infections - Hospital",
    "unplanned_visits": "CMS Unplanned Hospital Visits - Hospital",
    "cost_report": "CMS Hospital Cost Report",
}

_QUALITY_DATASET_IDS = {
    dataset: data_loaders.DATASETS.get(dataset, "")
    for dataset in _QUALITY_DATASET_LOADERS
}
_QUALITY_DATASET_IDS["cost_report"] = "cms_cost_report"


def _clean_measure_token(value: Any) -> str:
    return str(value or "").strip().upper().replace("-", "_")


def _find_measure_col(df):
    return _col(
        df,
        "measure_id",
        "hcahps_measure_id",
        "measure_name",
        "hrrp_measure_id",
        "measure",
    )


def _row_value(row, *candidates: str) -> str:
    for key in candidates:
        if key in row.index:
            value = str(row.get(key, "")).strip()
            if value:
                return value
    return ""


def _quality_dataset_source_url(dataset: str) -> str:
    if dataset == "cost_report":
        return "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report"
    dataset_id = _QUALITY_DATASET_IDS.get(dataset, "")
    if not dataset_id:
        return ""
    return f"https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0/download?format=csv"


def _quality_summary_evidence(
    *,
    dataset: str,
    ccn: str,
    match_basis: str,
    confidence: str,
    source_period: str = "",
) -> dict[str, Any]:
    source_metadata = data_loaders.dataset_cache_metadata(dataset)
    return evidence_receipt(
        source_metadata=source_metadata,
        source_name=_QUALITY_DATASET_NAMES.get(dataset, dataset),
        source_url=_quality_dataset_source_url(dataset),
        dataset_id=_QUALITY_DATASET_IDS.get(dataset, ""),
        source_period=source_period or str(source_metadata.get("source_period") or ""),
        cache_status=str(source_metadata.get("cache_status") or ""),
        cache_freshness=str(source_metadata.get("cache_freshness") or ""),
        entity_scope="ccn",
        query={"ccn": ccn},
        match_basis=match_basis,
        confidence=confidence,
        caveat=(
            "CMS summary quality program data is public source context. "
            "Use get_quality_measure_rows for exact named CMS measure assertions."
        ),
        next_step="For reportable measure-specific facts, fetch the exact CMS measure row and preserve its evidence receipt.",
    )


def _quality_source_metadata(evidence: dict[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a hospital-quality evidence receipt."""

    return {
        "source_name": evidence.get("source_name", ""),
        "source_url": evidence.get("source_url", ""),
        "dataset_id": evidence.get("dataset_id", ""),
        "source_period": evidence.get("source_period", ""),
        "retrieved_at": evidence.get("retrieved_at", ""),
        "source_modified": evidence.get("source_modified", ""),
        "cache_status": evidence.get("cache_status", ""),
        "cache_freshness": evidence.get("cache_freshness", ""),
        "entity_scope": evidence.get("entity_scope", "hospital_quality_ccn"),
        "source_type": "cms_hospital_quality_public_file",
    }


def _quality_summary_row_evidence(
    row: Any,
    *,
    dataset: str,
    ccn: str,
    row_kind: str,
    match_basis: str,
    confidence: str,
    extra_query: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return provenance for a summary-derived row separated from its parent result."""

    source_metadata = data_loaders.dataset_cache_metadata(dataset)
    query = {
        "ccn": ccn,
        "row_kind": row_kind,
        "facility_name": _row_value(row, "facility_name", "hospital_name", "provider_name"),
        "measure_id": _row_value(row, "measure_id", "hcahps_measure_id", "hrrp_measure_id", "measure"),
    }
    query.update(extra_query or {})
    return evidence_receipt(
        source_metadata=source_metadata,
        source_name=_QUALITY_DATASET_NAMES.get(dataset, dataset),
        source_url=_quality_dataset_source_url(dataset),
        dataset_id=_QUALITY_DATASET_IDS.get(dataset, ""),
        source_period=_quality_source_period(row) or str(source_metadata.get("source_period") or ""),
        cache_status=str(source_metadata.get("cache_status") or ""),
        cache_freshness=str(source_metadata.get("cache_freshness") or ""),
        entity_scope="hospital_quality_ccn",
        query={key: value for key, value in query.items() if value not in ("", [], None)},
        match_basis=match_basis,
        confidence=confidence,
        caveat=(
            "CMS summary rows are source-backed public context, not substitutes for exact named measure rows. "
            "Use get_quality_measure_rows before citing a specific CMS measure assertion."
        ),
        next_step="Preserve this row receipt with the parent evidence and identity_map; fetch exact measure rows for reportable named quality facts.",
    )


def _quality_identity(row: Any, *, ccn: str, dataset: str) -> dict[str, Any]:
    source_metadata = data_loaders.dataset_cache_metadata(dataset)
    return identity_from_public_record(
        name=_row_value(row, "facility_name", "hospital_name", "provider_name"),
        entity_type="hospital",
        ccn=_row_value(row, "facility_id", "ccn", "provider_id", "provider_ccn") or ccn,
        source_name=_QUALITY_DATASET_NAMES.get(dataset, dataset),
        source_url=str(source_metadata.get("source_url") or _quality_dataset_source_url(dataset)),
    ).to_dict()


def _quality_seed_identity(*, ccn: str, dataset: str) -> dict[str, Any]:
    source_metadata = data_loaders.dataset_cache_metadata(dataset)
    return identity_from_public_record(
        entity_type="hospital",
        ccn=ccn,
        source_name=_QUALITY_DATASET_NAMES.get(dataset, dataset),
        source_url=str(source_metadata.get("source_url") or _quality_dataset_source_url(dataset)),
    ).to_dict()


def _quality_identity_map(
    *,
    dataset: str,
    ccn: str,
    rows: Any = None,
    evidence: dict[str, Any] | None = None,
    measure_ids: tuple[str, ...] = (),
    row_evidence_paths: tuple[str, ...] = (),
) -> dict[str, Any]:
    records = _quality_records(rows)
    observed_ccns = _quality_identity_values(
        "ccn",
        ccn,
        *(_quality_record_value(row, "facility_id", "ccn", "provider_id", "provider_ccn", "provider_number") for row in records),
    )
    observed_names = _quality_identity_values(
        "canonical_name",
        *(_quality_record_value(row, "facility_name", "hospital_name", "provider_name", "name") for row in records),
    )
    observed_measure_ids = _quality_identity_values(
        "measure_id",
        *measure_ids,
        *(_quality_record_value(row, "measure_id", "hcahps_measure_id", "hrrp_measure_id", "measure") for row in records),
    )
    source_claims = _quality_source_claims(dataset, evidence=evidence, row_evidence_paths=row_evidence_paths)
    return {
        "entity_scope": "hospital_quality_ccn",
        "join_keys": [
            {
                "field": "ccn",
                "values": observed_ccns,
                "status": "provided" if observed_ccns else "missing",
                "used_by": [claim["collection"] for claim in source_claims],
            },
            {
                "field": "canonical_name",
                "values": observed_names,
                "status": "provided" if observed_names else "missing",
                "used_by": [claim["collection"] for claim in source_claims if "name" in " ".join(claim["identity_paths"])],
            },
            {
                "field": "measure_id",
                "values": observed_measure_ids,
                "status": "provided" if observed_measure_ids else "not_applicable",
                "used_by": [claim["collection"] for claim in source_claims if "measure" in " ".join(claim["identity_paths"])],
            },
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Use CCN as the hospital identity anchor for CMS quality summary and exact-measure joins.",
            "Use measure_id only for row-level CMS measure facts; summary domains are adjacent context unless their receipt supports the claim.",
            "Keep CMS facility names as source-specific aliases and do not merge hospitals by name alone.",
        ],
        "missing_data_policy": (
            "No-match or missing hospital-quality responses identify the searched CMS public-source scope; "
            "they are not proof of no quality measures, no readmissions, no safety events, or no patient-experience results."
        ),
    }


def _quality_records(rows: Any) -> list[Any]:
    if rows is None:
        return []
    if isinstance(rows, list):
        return rows
    if isinstance(rows, tuple):
        return list(rows)
    if hasattr(rows, "iterrows"):
        return [row for _, row in rows.iterrows()]
    return [rows]


def _quality_record_value(record: Any, *keys: str) -> str:
    for key in keys:
        if isinstance(record, dict):
            value = record.get(key, "")
        elif hasattr(record, "index") and key in record.index:
            value = record.get(key, "")
        else:
            continue
        value_text = str(value or "").strip()
        if value_text and value_text.lower() not in {"nan", "none", "not available", "n/a"}:
            return value_text
    return ""


def _quality_identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        if value in ("", None):
            continue
        if field == "ccn":
            normalized = normalize_ccn(value) or ""
        elif field == "canonical_name":
            normalized = normalize_name(value, remove_legal_suffixes=True)
        elif field == "measure_id":
            normalized = _clean_measure_token(value)
        else:
            normalized = str(value).strip()
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _quality_source_claims(
    dataset: str,
    *,
    evidence: dict[str, Any] | None,
    row_evidence_paths: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    dataset_id = str((evidence or {}).get("dataset_id") or _QUALITY_DATASET_IDS.get(dataset, "") or dataset)
    collection = dataset_id or dataset
    claim = {
        "collection": collection,
        "source_name": _QUALITY_DATASET_NAMES.get(dataset, dataset),
        "identity_paths": [
            "query.ccn",
            f"{dataset}.facility_id",
            f"{dataset}.provider_id",
            f"{dataset}.facility_name",
            f"{dataset}.measure_id",
        ],
        "evidence_path": "evidence",
        "match_policy": "ccn_required_for_hospital_quality_join_measure_id_required_for_named_measure_fact",
    }
    if row_evidence_paths:
        claim["row_evidence_paths"] = list(row_evidence_paths)
    return [claim]


def _quality_no_data_response(
    message: str,
    *,
    dataset: str,
    ccn: str,
    match_basis: str,
    confidence: str,
    code: str = "not_found",
    next_step: str,
) -> dict[str, Any]:
    evidence = _quality_summary_evidence(
        dataset=dataset,
        ccn=ccn,
        match_basis=match_basis,
        confidence=confidence,
    )
    evidence["next_step"] = next_step
    return error_response(
        message,
        code=code,
        evidence=evidence,
        source_metadata=_quality_source_metadata(evidence),
        identity=_quality_seed_identity(ccn=ccn, dataset=dataset),
        identity_map=_quality_identity_map(dataset=dataset, ccn=ccn, evidence=evidence),
    )


def _quality_comparison_evidence(*, ccns: list[str], matched_count: int) -> dict[str, Any]:
    datasets = ("hospital_info", "hac", "hrrp", "hcahps")
    metadata_by_dataset = {dataset: data_loaders.dataset_cache_metadata(dataset) for dataset in datasets}
    cache_statuses = {
        dataset: str(metadata.get("cache_status") or "")
        for dataset, metadata in metadata_by_dataset.items()
    }
    cache_freshness = "; ".join(
        f"{dataset}={metadata.get('cache_freshness') or metadata.get('cache_status') or 'unknown'}"
        for dataset, metadata in metadata_by_dataset.items()
    )
    return evidence_receipt(
        source_name="CMS Hospital Quality comparison bundle",
        source_url="https://data.cms.gov/provider-data/topics/hospitals",
        dataset_id="cms_hospital_quality_comparison",
        source_period="latest cached CMS public files at query time",
        cache_status="composite",
        cache_freshness=cache_freshness,
        entity_scope="ccn_list",
        query={"ccns": ccns},
        match_basis="ccn_exact_multi_source_summary" if matched_count else "no_ccn_matches_in_quality_sources",
        confidence="source_specific_nested_receipts_required",
        caveat=(
            "Comparison output combines multiple CMS public quality files. "
            "Cite nested per-domain evidence receipts for source-specific facts."
        ),
        next_step="Use each hospital domain's nested evidence receipt before citing a quality, safety, readmission, or HCAHPS fact.",
        source_metadata={
            "cache_status": "composite",
            "cache_freshness": cache_freshness,
            "query": {"ccns": ccns, "cache_statuses": cache_statuses},
        },
    )


def _quality_comparison_error(message: str, *, ccns: list[str], code: str = "invalid_params") -> dict[str, Any]:
    evidence = evidence_receipt(
        source_name="CMS Hospital Quality comparison bundle",
        source_url="https://data.cms.gov/provider-data/topics/hospitals",
        dataset_id="cms_hospital_quality_comparison",
        source_period="latest cached CMS public files at query time",
        cache_status="not_evaluated",
        cache_freshness="not evaluated because comparison parameters were invalid",
        entity_scope="ccn_list",
        query={"ccns": ccns},
        match_basis="invalid_comparison_parameters",
        confidence="not_evaluated",
        caveat="No CMS source rows were evaluated because the comparison request did not meet tool bounds.",
        next_step="Provide 2 to 10 CMS Certification Numbers and rerun the comparison.",
    )
    return error_response(
        message,
        code=code,
        evidence=evidence,
        source_metadata=_quality_source_metadata(evidence),
        identity_map={
            "join_key": "ccn",
            "provided_ccns": ccns,
            "facilities": [_quality_seed_identity(ccn=ccn, dataset="hospital_info") for ccn in ccns],
            "conflict_policy": "Do not merge hospitals across CCNs; each CCN remains a separate comparison entity.",
        },
    )


def _quality_comparison_identity_map(hospitals: list[dict[str, Any]], *, requested_ccns: list[str]) -> dict[str, Any]:
    facilities = []
    for hospital in hospitals:
        identity = None
        for domain in ("quality", "safety", "readmission", "patient_experience"):
            domain_payload = hospital.get(domain)
            if isinstance(domain_payload, dict) and isinstance(domain_payload.get("identity"), dict):
                identity = domain_payload["identity"]
                break
        if identity is None:
            identity = _quality_seed_identity(ccn=str(hospital.get("ccn") or ""), dataset="hospital_info")
        facilities.append(
            {
                "ccn": str(hospital.get("ccn") or ""),
                "identity": identity,
                "available_domains": [
                    domain
                    for domain in ("quality", "safety", "readmission", "patient_experience")
                    if isinstance(hospital.get(domain), dict) and "error" not in hospital[domain]
                ],
                "missing_domains": [
                    domain
                    for domain in ("quality", "safety", "readmission", "patient_experience")
                    if not isinstance(hospital.get(domain), dict) or "error" in hospital[domain]
                ],
            }
        )
    return {
        "join_key": "ccn",
        "requested_ccns": requested_ccns,
        "facilities": facilities,
        "conflict_policy": (
            "Do not merge hospitals across CCNs; preserve source-specific names as aliases and cite nested "
            "domain evidence for each compared fact."
        ),
    }


def _quality_source_period(row: Any) -> str:
    start_date = _row_value(row, "start_date", "measure_start_date", "reporting_period_start")
    end_date = _row_value(row, "end_date", "measure_end_date", "reporting_period_end")
    fiscal_year = _row_value(row, "fiscal_year", "fy", "cost_report_year", "year", "fiscal_year_end", "fy_end")
    if start_date or end_date:
        return " - ".join(part for part in (start_date, end_date) if part)
    if fiscal_year:
        return fiscal_year
    return ""


async def _load_quality_dataset(dataset: str):
    loader_name = _QUALITY_DATASET_LOADERS.get(dataset)
    if not loader_name:
        return None
    loader = getattr(data_loaders, loader_name)
    return await loader()


async def _quality_measure_rows_from_dataset(
    ccn: str,
    dataset: str,
    measure_ids: tuple[str, ...],
    *,
    match_prefix: bool = False,
) -> list[dict[str, Any]]:
    df = await _load_quality_dataset(dataset)
    if df is None or df.empty:
        return []
    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return []

    wanted = tuple(_clean_measure_token(measure_id) for measure_id in measure_ids)
    measure_col = _find_measure_col(matches)
    if measure_col:
        rows = []
        for _, row in matches.iterrows():
            token = _clean_measure_token(row.get(measure_col, ""))
            if token in wanted or (match_prefix and any(token.startswith(f"{candidate}_") for candidate in wanted)):
                rows.append(_quality_measure_result_row(row, dataset, measure_col))
        return rows

    # Some CMS extracts are wide. If a direct measure column exists, expose it
    # with the full source row rather than silently returning an aggregate.
    rows = []
    normalized_columns = {_clean_measure_token(column): column for column in matches.columns}
    for candidate in wanted:
        column = normalized_columns.get(candidate)
        if column:
            row = matches.iloc[0]
            result = _quality_measure_result_row(row, dataset, "")
            result.update({"measure_id": candidate, "score": _row_value(row, column)})
            rows.append(result)
    return rows


def _quality_measure_result_row(row, dataset: str, measure_col: str) -> dict[str, Any]:
    raw = {str(key): str(value).strip() for key, value in row.to_dict().items()}
    dataset_id = _QUALITY_DATASET_IDS.get(dataset, "")
    source_url = (
        f"https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0/download?format=csv"
        if dataset_id
        else ""
    )
    start_date = _row_value(row, "start_date", "measure_start_date")
    end_date = _row_value(row, "end_date", "measure_end_date")
    measure_id = _row_value(row, measure_col) if measure_col else ""
    source_metadata = data_loaders.dataset_cache_metadata(dataset)
    receipt = evidence_receipt(
        source_metadata=source_metadata,
        source_name=_QUALITY_DATASET_NAMES.get(dataset, dataset),
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=" - ".join(part for part in (start_date, end_date) if part)
        or str(source_metadata.get("source_period") or ""),
        cache_status=str(source_metadata.get("cache_status") or ""),
        cache_freshness=str(source_metadata.get("cache_freshness") or ""),
        entity_scope="ccn",
        query={"ccn": _row_value(row, "facility_id", "ccn", "provider_id", "provider_ccn"), "measure_id": measure_id},
        match_basis="ccn_exact_measure_id" if measure_id else "ccn_exact_dataset_row",
        confidence="high_for_exact_cms_measure_rows",
        caveat="Exact row-level CMS Provider Data Catalog measure row; adjacent summaries are not substituted.",
        next_step="If this row is missing, verify the CCN and measure ID against the CMS Provider Data Catalog file.",
    )
    return {
        "ccn": _row_value(row, "facility_id", "ccn", "provider_id", "provider_ccn"),
        "facility_name": _row_value(row, "facility_name", "hospital_name", "provider_name"),
        "dataset": dataset,
        "dataset_id": dataset_id,
        "source_name": receipt["source_name"],
        "source_url": receipt["source_url"],
        "source_period": receipt["source_period"],
        "retrieved_at": receipt["retrieved_at"],
        "source_modified": receipt["source_modified"],
        "cache_status": receipt["cache_status"],
        "cache_freshness": receipt["cache_freshness"],
        "entity_scope": receipt["entity_scope"],
        "query": receipt["query"],
        "cache_key": receipt["cache_key"],
        "confidence": receipt["confidence"],
        "measure_id": measure_id,
        "measure_name": _row_value(row, "measure_name", "hcahps_question", "compared_to_national"),
        "score": _row_value(
            row,
            "score",
            "denominator",
            "patient_survey_star_rating",
            "hcahps_answer_percent",
            "rate",
            "compared_to_national",
            "excess_readmission_ratio",
        ),
        "start_date": start_date,
        "end_date": end_date,
        "evidence": receipt,
        "raw": raw,
    }


async def _quality_dataset_shape(dataset: str) -> dict[str, Any]:
    df = await _load_quality_dataset(dataset)
    dataset_id = _QUALITY_DATASET_IDS.get(dataset, "")
    columns = [str(column) for column in getattr(df, "columns", [])]
    return {
        "dataset": dataset,
        "dataset_id": dataset_id,
        "source_name": _QUALITY_DATASET_NAMES.get(dataset, dataset),
        "source_url": (
            f"https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0/download?format=csv"
            if dataset_id
            else ""
        ),
        "row_count": 0 if df is None else int(len(df)),
        "columns_sample": columns[:25],
        "has_measure_column": bool(_find_measure_col(df)) if df is not None and not df.empty else False,
    }


def _error_message(response: dict[str, Any]) -> str | None:
    """Extract a user-facing message from structured helper error responses."""
    error = response.get("error")
    if isinstance(error, dict):
        return str(error.get("message") or "")
    if isinstance(error, str):
        return error
    return None


def _comparison_domain_payload(response: dict[str, Any]) -> dict[str, Any]:
    """Preserve nested provenance for comparison domains, including errors."""

    message = _error_message(response)
    if not message:
        return response
    payload = {"error": message}
    for key in ("ok", "status", "evidence", "identity", "identity_map", "source_metadata"):
        if key in response:
            payload[key] = response[key]
    error = response.get("error")
    if isinstance(error, dict) and error.get("code"):
        payload["error_code"] = error["code"]
    return payload


# ---------------------------------------------------------------------------
# Tool: get_quality_scores
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def get_quality_scores(ccn: str) -> dict[str, Any]:
    """Get overall quality ratings for a hospital from CMS Hospital General Info.

    Returns star ratings (1-5) and national comparison ratings for mortality,
    safety, readmission, patient experience, and timeliness of care.

    Args:
        ccn: The 6-character CMS Certification Number (e.g. "050454").

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_quality_scores","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    df = await data_loaders.load_hospital_info()
    if df.empty:
        return _quality_no_data_response(
            "Hospital General Info data not available",
            dataset="hospital_info",
            ccn=ccn,
            match_basis="source_cache_unavailable",
            confidence="not_evaluated",
            code="source_unavailable",
            next_step="Refresh or load the CMS Hospital General Information cache before citing hospital quality ratings.",
        )

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return _quality_no_data_response(
            f"No hospital found with CCN: {ccn}",
            dataset="hospital_info",
            ccn=ccn,
            match_basis="ccn_no_match_in_hospital_general_info",
            confidence="no_match_in_loaded_cms_hospital_general_info",
            next_step="Verify the CCN against CMS Provider of Services or Hospital General Information before reporting no quality rating.",
        )

    row = matches.iloc[0]

    def val(*keys):
        for k in keys:
            if k in row.index and row[k]:
                return str(row[k]).strip()
        return ""

    result = QualityScores(
        ccn=ccn,
        facility_name=val("facility_name", "hospital_name", "provider_name"),
        overall_rating=val("hospital_overall_rating", "overall_rating", "overall_quality_star_rating"),
        mortality_national_comparison=val("mortality_national_comparison", "mortality_rating"),
        safety_national_comparison=val("safety_of_care_national_comparison", "safety_rating"),
        readmission_national_comparison=val("readmission_national_comparison", "readmission_rating"),
        patient_experience_national_comparison=val("patient_experience_national_comparison", "patient_experience_rating"),
        timeliness_national_comparison=val("timeliness_of_care_national_comparison", "timeliness_rating"),
    )
    payload = result.model_dump()
    payload["identity"] = _quality_identity(row, ccn=ccn, dataset="hospital_info")
    payload["evidence"] = _quality_summary_evidence(
        dataset="hospital_info",
        ccn=ccn,
        match_basis="ccn_exact",
        confidence="high_for_cms_hospital_general_info_row",
        source_period=_quality_source_period(row),
    )
    payload["source_metadata"] = _quality_source_metadata(payload["evidence"])
    payload["identity_map"] = _quality_identity_map(
        dataset="hospital_info",
        ccn=ccn,
        rows=row,
        evidence=payload["evidence"],
    )
    return to_structured(payload)


# ---------------------------------------------------------------------------
# Tool: get_readmission_data
# ---------------------------------------------------------------------------

# HRRP measure IDs map to condition abbreviations
_HRRP_MEASURES = {
    "READM-30-AMI-HRRP": "AMI",
    "READM-30-HF-HRRP": "HF",
    "READM-30-PN-HRRP": "PN",
    "READM-30-COPD-HRRP": "COPD",
    "READM-30-HIP-KNEE-HRRP": "HIP_KNEE",
    "READM-30-CABG-HRRP": "CABG",
}


@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def get_readmission_data(ccn: str) -> dict[str, Any]:
    """Get Hospital Readmissions Reduction Program (HRRP) data for a hospital.

    Returns excess readmission ratios, predicted/expected readmission rates,
    discharge and readmission counts per condition (AMI, HF, PN, COPD, HIP_KNEE,
    CABG), and the payment reduction percentage.

    Args:
        ccn: The 6-character CMS Certification Number.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_readmission_data","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    df = await data_loaders.load_hrrp()
    if df.empty:
        return _quality_no_data_response(
            "HRRP data not available",
            dataset="hrrp",
            ccn=ccn,
            match_basis="source_cache_unavailable",
            confidence="not_evaluated",
            code="source_unavailable",
            next_step="Refresh or load the CMS HRRP cache before citing readmission reduction program fields.",
        )

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return _quality_no_data_response(
            f"No HRRP data found for CCN: {ccn}",
            dataset="hrrp",
            ccn=ccn,
            match_basis="ccn_no_match_in_hrrp",
            confidence="no_match_in_loaded_cms_hrrp",
            next_step="Use get_quality_measure_rows for exact named readmission measures; do not substitute HRRP no-match for hospital-wide readmission facts.",
        )

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(matches.iloc[0][name_col]).strip()

    # Parse per-condition readmission rows
    measure_col = _col(matches, "measure_id", "measure_name", "hrrp_measure_id")
    conditions = []
    condition_rows: dict[str, Any] = {}

    if measure_col:
        for _, row in matches.iterrows():
            measure_raw = str(row.get(measure_col, "")).strip().upper()
            condition = _HRRP_MEASURES.get(measure_raw, "")
            if not condition:
                # Try partial matching
                for key, abbr in _HRRP_MEASURES.items():
                    if abbr in measure_raw or key in measure_raw:
                        condition = abbr
                        break
                if not condition:
                    condition = measure_raw

            conditions.append(ConditionReadmission(
                measure=condition,
                excess_readmission_ratio=_safe_float(row.get("excess_readmission_ratio", "")),
                predicted_readmission_rate=_safe_float(row.get("predicted_readmission_rate", "")),
                expected_readmission_rate=_safe_float(row.get("expected_readmission_rate", "")),
                number_of_discharges=_safe_int(row.get("number_of_discharges", "")),
                number_of_readmissions=_safe_int(row.get("number_of_readmissions", "")),
            ))
            condition_rows[condition] = row
    else:
        # Flat layout: columns per condition
        for abbr in ("AMI", "HF", "PN", "COPD", "HIP_KNEE", "CABG"):
            prefix = abbr.lower()
            conditions.append(ConditionReadmission(
                measure=abbr,
                excess_readmission_ratio=_safe_float(
                    matches.iloc[0].get(f"{prefix}_excess_readmission_ratio",
                                        matches.iloc[0].get(f"excess_readmission_ratio_{prefix}", ""))
                ),
                number_of_discharges=_safe_int(
                    matches.iloc[0].get(f"{prefix}_number_of_discharges",
                                        matches.iloc[0].get(f"number_of_discharges_{prefix}", ""))
                ),
            ))
            condition_rows[abbr] = matches.iloc[0]

    # Payment reduction: derived from payment_adjustment_factor or payment_reduction columns
    payment_reduction = None
    paf_col = _col(matches, "payment_adjustment_factor", "payment_reduction_percentage",
                   "peer_group_value", "payment_reduction")
    if paf_col:
        paf_val = _safe_float(matches.iloc[0].get(paf_col, ""))
        if paf_val is not None:
            if paf_val <= 1.0 and "factor" in paf_col:
                # Payment adjustment factor: 0.9970 means 0.30% reduction
                payment_reduction = round((1.0 - paf_val) * 100, 4)
            else:
                payment_reduction = paf_val

    result = ReadmissionData(
        ccn=ccn,
        facility_name=facility_name,
        conditions=conditions,
        payment_reduction_percentage=payment_reduction,
    )
    payload = result.model_dump()
    for condition in payload["conditions"]:
        measure = str(condition.get("measure") or "")
        row = condition_rows.get(measure, matches.iloc[0])
        condition["evidence"] = _quality_summary_row_evidence(
            row,
            dataset="hrrp",
            ccn=ccn,
            row_kind="hrrp_condition",
            match_basis="hrrp_condition_summary_row",
            confidence="high_for_cms_hrrp_summary_row",
            extra_query={"condition": measure},
        )
    payload["identity"] = _quality_identity(matches.iloc[0], ccn=ccn, dataset="hrrp")
    payload["evidence"] = _quality_summary_evidence(
        dataset="hrrp",
        ccn=ccn,
        match_basis="ccn_exact_hrrp_condition_rows",
        confidence="high_for_cms_hrrp_summary_rows",
        source_period=_quality_source_period(matches.iloc[0]),
    )
    payload["source_metadata"] = _quality_source_metadata(payload["evidence"])
    payload["identity_map"] = _quality_identity_map(
        dataset="hrrp",
        ccn=ccn,
        rows=matches,
        evidence=payload["evidence"],
        row_evidence_paths=("conditions[].evidence",),
    )
    return to_structured(payload)


# ---------------------------------------------------------------------------
# Tool: get_safety_scores
# ---------------------------------------------------------------------------

# HAC domain column name mappings (try multiple naming conventions)
_HAC_DOMAINS = {
    "psi90": ("psi_90_safety", "psi90", "psi_90", "cms_psi_90"),
    "clabsi": ("clabsi", "hai_1_clabsi", "central_line_associated_bloodstream_infection"),
    "cauti": ("cauti", "hai_2_cauti", "catheter_associated_urinary_tract_infection"),
    "ssi_colon": ("ssi_colon", "hai_3_ssi_colon", "ssi_abdominal"),
    "ssi_hyst": ("ssi_hyst", "hai_4_ssi_hyst", "ssi_hysterectomy"),
    "mrsa": ("mrsa", "hai_5_mrsa", "mrsa_bacteremia"),
    "cdi": ("cdi", "hai_6_cdi", "c_diff", "clostridium_difficile"),
}


@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def get_safety_scores(ccn: str) -> dict[str, Any]:
    """Get Hospital-Acquired Condition (HAC) Reduction Program safety scores.

    Returns total HAC score, payment reduction status, and domain scores
    for PSI-90, CLABSI, CAUTI, SSI (colon/hyst), MRSA, and CDI.

    Args:
        ccn: The 6-character CMS Certification Number.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_safety_scores","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    df = await data_loaders.load_hac()
    if df.empty:
        return _quality_no_data_response(
            "HAC Reduction Program data not available",
            dataset="hac",
            ccn=ccn,
            match_basis="source_cache_unavailable",
            confidence="not_evaluated",
            code="source_unavailable",
            next_step="Refresh or load the CMS HAC cache before citing HAC Reduction Program scores.",
        )

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return _quality_no_data_response(
            f"No HAC data found for CCN: {ccn}",
            dataset="hac",
            ccn=ccn,
            match_basis="ccn_no_match_in_hac",
            confidence="no_match_in_loaded_cms_hac",
            next_step="Use get_quality_measure_rows for exact HAI measure assertions; do not substitute HAC no-match for CLABSI or other HAI facts.",
        )

    row = matches.iloc[0]

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(row[name_col]).strip()

    total_col = _col(matches, "total_hac_score", "total_score", "hac_score")
    reduction_col = _col(matches, "payment_reduction", "payment_reduction_indicator",
                         "hac_payment_reduction")

    # Build domain scores by searching for matching columns
    domain_kwargs = {}
    domain_evidence_rows = []
    for domain_key, candidates in _HAC_DOMAINS.items():
        val = None
        matched_column = ""
        for candidate in candidates:
            # Try exact and with common suffixes
            for col_try in (candidate, f"{candidate}_score", f"{candidate}_measure"):
                if col_try in row.index:
                    val = _safe_float(row[col_try])
                    if val is not None:
                        matched_column = col_try
                        break
            if val is not None:
                break
        domain_kwargs[domain_key] = val
        if val is not None:
            domain_evidence_rows.append(
                {
                    "domain": domain_key,
                    "value": val,
                    "source_column": matched_column,
                    "evidence": _quality_summary_row_evidence(
                        row,
                        dataset="hac",
                        ccn=ccn,
                        row_kind="hac_domain",
                        match_basis="hac_domain_summary_field",
                        confidence="high_for_cms_hac_domain_field",
                        extra_query={"domain": domain_key, "source_column": matched_column},
                    ),
                }
            )

    result = SafetyScores(
        ccn=ccn,
        facility_name=facility_name,
        total_hac_score=_safe_float(row.get(total_col, "")) if total_col else None,
        payment_reduction=str(row.get(reduction_col, "")).strip() if reduction_col else "",
        domain_scores=DomainScores(**domain_kwargs),
    )
    payload = result.model_dump()
    payload["domain_evidence"] = domain_evidence_rows
    payload["identity"] = _quality_identity(row, ccn=ccn, dataset="hac")
    payload["evidence"] = _quality_summary_evidence(
        dataset="hac",
        ccn=ccn,
        match_basis="ccn_exact_hac_summary_row",
        confidence="high_for_cms_hac_summary_row",
        source_period=_quality_source_period(row),
    )
    payload["source_metadata"] = _quality_source_metadata(payload["evidence"])
    payload["identity_map"] = _quality_identity_map(
        dataset="hac",
        ccn=ccn,
        rows=row,
        evidence=payload["evidence"],
        row_evidence_paths=("domain_evidence[].evidence",),
    )
    return to_structured(payload)


# ---------------------------------------------------------------------------
# Tool: get_patient_experience
# ---------------------------------------------------------------------------

# HCAHPS measure ID prefix to domain name mapping
_HCAHPS_DOMAINS = {
    "H_COMP_1": "nurse_communication",
    "H_COMP_2": "doctor_communication",
    "H_COMP_3": "staff_responsiveness",
    "H_COMP_4": "pain_management",
    "H_COMP_5": "medicine_communication",
    "H_COMP_6": "discharge_info",
    "H_COMP_7": "care_transition",
    "H_CLEAN": "cleanliness",
    "H_QUIET": "quietness",
    "H_HSP_RATING": "overall_rating",
    "H_RECMND": "recommend",
}

# Star rating measure suffixes
_STAR_SUFFIX = "_STAR_RATING"
# Answer percent measure suffixes for top/middle/bottom box
_LINEAR_SCORE = "_LINEAR_SCORE"


@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def get_patient_experience(ccn: str) -> dict[str, Any]:
    """Get HCAHPS patient experience survey scores for a hospital.

    Returns star ratings and response percentages for domains: nurse/doctor
    communication, staff responsiveness, pain management, medicine communication,
    discharge info, care transition, cleanliness, quietness, overall rating,
    and recommendation.

    Args:
        ccn: The 6-character CMS Certification Number.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_patient_experience","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    df = await data_loaders.load_hcahps()
    if df.empty:
        return _quality_no_data_response(
            "HCAHPS data not available",
            dataset="hcahps",
            ccn=ccn,
            match_basis="source_cache_unavailable",
            confidence="not_evaluated",
            code="source_unavailable",
            next_step="Refresh or load the CMS HCAHPS cache before citing patient-experience survey fields.",
        )

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return _quality_no_data_response(
            f"No HCAHPS data found for CCN: {ccn}",
            dataset="hcahps",
            ccn=ccn,
            match_basis="ccn_no_match_in_hcahps",
            confidence="no_match_in_loaded_cms_hcahps",
            next_step="Verify the CCN and CMS HCAHPS source period before reporting absence of patient-experience rows.",
        )

    facility_name = ""
    name_col = _col(matches, "facility_name", "hospital_name", "provider_name")
    if name_col:
        facility_name = str(matches.iloc[0][name_col]).strip()

    measure_col = _col(matches, "hcahps_measure_id", "measure_id", "measure_name")
    star_col = _col(matches, "patient_survey_star_rating", "star_rating", "hcahps_star_rating")
    answer_pct_col = _col(matches, "hcahps_answer_percent", "answer_percent", "percent")
    answer_desc_col = _col(matches, "hcahps_answer_description", "answer_description")
    response_rate_col = _col(matches, "survey_response_rate_percent", "response_rate")
    num_surveys_col = _col(matches, "number_of_completed_surveys", "completed_surveys",
                           "num_completed_surveys")

    # Get survey response rate and completed surveys from any row
    survey_response_rate = ""
    num_completed_surveys = ""
    if response_rate_col:
        val = str(matches.iloc[0].get(response_rate_col, "")).strip()
        if val and val.lower() not in ("not available", "n/a"):
            survey_response_rate = val
    if num_surveys_col:
        val = str(matches.iloc[0].get(num_surveys_col, "")).strip()
        if val and val.lower() not in ("not available", "n/a"):
            num_completed_surveys = val

    # Aggregate measures by domain
    domain_data: dict[str, ExperienceDomain] = {}
    domain_rows: dict[str, Any] = {}
    domain_measure_ids: dict[str, list[str]] = {}

    if measure_col:
        for _, row in matches.iterrows():
            measure_id = str(row.get(measure_col, "")).strip().upper()

            # Match measure to domain
            matched_domain = None
            for prefix, domain_name in _HCAHPS_DOMAINS.items():
                if measure_id.startswith(prefix):
                    matched_domain = domain_name
                    break

            if not matched_domain:
                continue

            if matched_domain not in domain_data:
                domain_data[matched_domain] = ExperienceDomain(domain=matched_domain)
                domain_rows[matched_domain] = row
            domain_measure_ids.setdefault(matched_domain, []).append(measure_id)

            domain = domain_data[matched_domain]

            # Star rating measure
            if _STAR_SUFFIX in measure_id or _LINEAR_SCORE in measure_id:
                if star_col:
                    domain.star_rating = str(row.get(star_col, "")).strip()

            # Answer percent measures — categorize by top/middle/bottom box
            if answer_pct_col and answer_desc_col:
                desc = str(row.get(answer_desc_col, "")).strip().lower()
                pct = str(row.get(answer_pct_col, "")).strip()
                if any(kw in desc for kw in ("always", "strongly agree", "9", "10", "yes", "definitely")):
                    domain.top_box_percent = pct
                elif any(kw in desc for kw in ("never", "strongly disagree", "0", "1", "2", "3", "4", "5", "6")):
                    domain.bottom_box_percent = pct
                elif any(kw in desc for kw in ("sometimes", "usually", "somewhat", "7", "8")):
                    domain.middle_box_percent = pct

            # If star_col has data on any row for this domain
            if star_col and not domain.star_rating:
                val = str(row.get(star_col, "")).strip()
                if val and val.lower() not in ("not available", "n/a", "not applicable"):
                    domain.star_rating = val

    result = PatientExperience(
        ccn=ccn,
        facility_name=facility_name,
        survey_response_rate=survey_response_rate,
        num_completed_surveys=num_completed_surveys,
        domains=list(domain_data.values()),
    )
    payload = result.model_dump()
    for domain in payload["domains"]:
        domain_name = str(domain.get("domain") or "")
        row = domain_rows.get(domain_name, matches.iloc[0])
        source_measure_ids = sorted(set(domain_measure_ids.get(domain_name, [])))
        domain["evidence"] = _quality_summary_row_evidence(
            row,
            dataset="hcahps",
            ccn=ccn,
            row_kind="hcahps_domain",
            match_basis="hcahps_domain_summary_rows",
            confidence="high_for_cms_hcahps_summary_rows",
            extra_query={"domain": domain_name, "source_measure_ids": source_measure_ids},
        )
    payload["identity"] = _quality_identity(matches.iloc[0], ccn=ccn, dataset="hcahps")
    payload["evidence"] = _quality_summary_evidence(
        dataset="hcahps",
        ccn=ccn,
        match_basis="ccn_exact_hcahps_rows",
        confidence="high_for_cms_hcahps_summary_rows",
        source_period=_quality_source_period(matches.iloc[0]),
    )
    payload["source_metadata"] = _quality_source_metadata(payload["evidence"])
    payload["identity_map"] = _quality_identity_map(
        dataset="hcahps",
        ccn=ccn,
        rows=matches,
        evidence=payload["evidence"],
        row_evidence_paths=("domains[].evidence",),
    )
    return to_structured(payload)


# ---------------------------------------------------------------------------
# Tool: get_financial_profile
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def get_financial_profile(ccn: str) -> dict[str, Any]:
    """Get financial profile for a hospital from CMS Cost Report data.

    Returns case mix index, discharge/bed counts, teaching status,
    DSH percentage, wage index, and urban/rural classification.

    Args:
        ccn: The 6-character CMS Certification Number.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_financial_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    df = await data_loaders.load_cost_report()
    if df.empty:
        return _quality_no_data_response(
            "Cost report data not available",
            dataset="cost_report",
            ccn=ccn,
            match_basis="source_cache_unavailable",
            confidence="not_evaluated",
            code="source_unavailable",
            next_step="Refresh or load CMS Hospital Cost Report data before citing financial profile fields.",
        )

    matches = _filter_by_ccn(df, ccn)
    if matches.empty:
        return _quality_no_data_response(
            f"No cost report data found for CCN: {ccn}",
            dataset="cost_report",
            ccn=ccn,
            match_basis="ccn_no_match_in_cost_report",
            confidence="no_match_in_loaded_cms_cost_report",
            next_step="Verify the CCN and fiscal-year coverage before reporting no public cost-report profile.",
        )

    # Take the most recent row if multiple years exist
    fy_col = _col(matches, "fiscal_year_end", "fy_end", "fiscal_year_end_date", "fy_end_dt",
                  "fiscal_year_end_dt")
    if fy_col and fy_col in matches.columns:
        matches = matches.sort_values(fy_col, ascending=False)

    row = matches.iloc[0]

    def val(*keys):
        for k in keys:
            if k in row.index and row[k]:
                return str(row[k]).strip()
        return ""

    facility_name = val("facility_name", "hospital_name", "provider_name", "name")

    # Case mix index
    cmi = _safe_float(val("case_mix_index", "cmi", "casemix_index", "case_mix"))

    # Discharges and beds
    total_discharges = _safe_int(val("total_discharges", "discharges", "tot_dschrgs"))
    total_beds = _safe_int(val("total_bed_days_available", "beds", "total_beds",
                               "bed_size", "number_of_beds", "hospital_bed_count"))

    # Teaching: resident-to-bed ratio
    rtb_raw = _safe_float(val("resident_to_bed_ratio", "rtb_ratio", "teaching_ratio",
                              "resident_to_adb_ratio", "residents_to_beds"))
    teaching_status = ""
    if rtb_raw is not None:
        if rtb_raw == 0:
            teaching_status = "Non-teaching"
        elif rtb_raw < 0.25:
            teaching_status = "Minor teaching"
        else:
            teaching_status = "Major teaching"

    # DSH
    dsh = _safe_float(val("dsh_pct", "dsh_percent", "disproportionate_share",
                          "dsh_adjustment_percent", "dsh_patient_percent"))

    # Wage index
    wage = _safe_float(val("wage_index", "area_wage_index", "cbsa_wage_index"))

    # Urban/Rural
    geo = val("urban_rural", "urban_rural_indicator", "geographic_location",
              "urban_or_rural", "cbsa_urban_rural")
    if not geo:
        # Derive from other columns if possible
        provider_type = val("provider_type", "hospital_type", "facility_type").lower()
        if "rural" in provider_type:
            geo = "Rural"
        elif "urban" in provider_type:
            geo = "Urban"

    result = FinancialProfile(
        ccn=ccn,
        facility_name=facility_name,
        case_mix_index=cmi,
        total_discharges=total_discharges,
        total_beds=total_beds,
        teaching_status=teaching_status,
        resident_to_bed_ratio=rtb_raw,
        dsh_pct=dsh,
        wage_index=wage,
        geographic_location=geo,
    )
    payload = result.model_dump()
    payload["identity"] = _quality_identity(row, ccn=ccn, dataset="cost_report")
    payload["evidence"] = _quality_summary_evidence(
        dataset="cost_report",
        ccn=ccn,
        match_basis="ccn_exact_cost_report_row",
        confidence="high_when_public_cost_report_fields_present",
        source_period=_quality_source_period(row),
    )
    payload["source_metadata"] = _quality_source_metadata(payload["evidence"])
    payload["identity_map"] = _quality_identity_map(
        dataset="cost_report",
        ccn=ccn,
        rows=row,
        evidence=payload["evidence"],
    )
    return to_structured(payload)


@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def get_quality_measure_rows(ccn: str, measure: str = "", measure_id: str = "") -> dict[str, Any]:
    """Return exact CMS quality measure rows for a hospital.

    This is the row-level companion to the summary quality tools. It is intended
    for report ledgers that must promote a specific CMS measure, such as HCAHPS
    nurse communication, AMI mortality, hospital-wide readmission, or CLABSI SIR.

    Args:
        ccn: The 6-character CMS Certification Number.
        measure: Canonical alias, for example ``hcahps_communication_nurses``,
            ``ami_30_day_mortality``, ``hospital_wide_readmission``, or
            ``clabsi_sir``.
        measure_id: Raw CMS measure ID/prefix when a canonical alias is not
            available.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_quality_measure_rows","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not ccn:
            return error_response("ccn is required.", code="invalid_params")
        lookup = (measure or measure_id or "").strip()
        if not lookup:
            return error_response("measure or measure_id is required.", code="invalid_params")

        alias = _QUALITY_MEASURE_ALIASES.get(lookup.lower())
        datasets: list[str]
        measure_ids: tuple[str, ...]
        description = ""
        if alias:
            datasets = [str(alias["dataset"])]
            measure_ids = tuple(str(value) for value in alias["measure_ids"])
            description = str(alias.get("description", ""))
            match_prefix = bool(alias.get("match_prefix"))
        else:
            datasets = ["hcahps", "complications", "hrrp", "hac", "hospital_info"]
            measure_ids = (lookup,)
            match_prefix = False

        rows: list[dict[str, Any]] = []
        checked: list[str] = []
        for dataset in datasets:
            checked.append(dataset)
            rows.extend(
                await _quality_measure_rows_from_dataset(
                    ccn,
                    dataset,
                    measure_ids,
                    match_prefix=match_prefix,
                )
            )
            if rows and alias:
                break

        if not rows:
            shapes = [await _quality_dataset_shape(dataset) for dataset in checked]
            primary_dataset = checked[0] if checked else "hospital_info"
            source_metadata = data_loaders.dataset_cache_metadata(primary_dataset)
            adjacent_dataset = str(alias.get("adjacent_dataset", "")) if alias else ""
            adjacent_available = False
            if adjacent_dataset:
                adjacent_df = await _load_quality_dataset(adjacent_dataset)
                adjacent_available = bool(adjacent_df is not None and not adjacent_df.empty)
            measure_shape_problem = any(shape["row_count"] > 0 and not shape["has_measure_column"] for shape in shapes)
            evidence = evidence_receipt(
                source_metadata=source_metadata,
                source_name="CMS Provider Data Catalog",
                source_url="https://data.cms.gov/provider-data/",
                dataset_id=str(source_metadata.get("dataset_id") or ""),
                source_period=str(source_metadata.get("source_period") or ""),
                cache_status=str(source_metadata.get("cache_status") or ""),
                cache_freshness=str(source_metadata.get("cache_freshness") or ""),
                entity_scope="ccn",
                query={"ccn": ccn, "measure": lookup, "measure_ids": list(measure_ids)},
                match_basis="no_exact_measure_row",
                confidence="no_exact_public_source_match",
                caveat=(
                    "No adjacent public record has been promoted as the requested CMS measure. "
                    "Use adjacent_tool only for separate summary context."
                ),
                next_step=(
                    "Verify the CCN and measure ID against the CMS Provider Data Catalog source file "
                    "for the reporting period."
                ),
            )
            payload = {
                "status": "source_shape_error" if measure_shape_problem else "exact_measure_not_found",
                "ccn": ccn,
                "exact_measure_found": False,
                "measure": lookup,
                "measure_ids": list(measure_ids),
                "identity": identity_from_public_record(
                    entity_type="hospital",
                    ccn=ccn,
                    source_name="CMS Provider Data Catalog",
                    source_url="https://data.cms.gov/provider-data/",
                ).to_dict(),
                "datasets_checked": checked,
                "dataset_shapes": shapes,
                "adjacent_available": adjacent_available,
                "adjacent_tool": str(alias.get("adjacent_tool", "")) if alias else "",
                "source_caveat": (
                    "No adjacent public record has been promoted as the requested CMS measure. "
                    "Use adjacent_tool only for separate summary context."
                ),
                "evidence": evidence,
                "source_metadata": _quality_source_metadata(evidence),
                "next_step": (
                    "Verify the CCN and measure ID against the CMS Provider Data Catalog source file "
                    "for the reporting period."
                ),
            }
            payload["identity_map"] = _quality_identity_map(
                dataset=primary_dataset,
                ccn=ccn,
                evidence=evidence,
                measure_ids=measure_ids,
            )
            return to_structured(payload)

        payload = {
                "ccn": ccn,
                "status": "ready",
                "exact_measure_found": True,
                "measure": measure or "",
                "measure_id": measure_id or "",
                "description": description,
                "measure_ids": list(measure_ids),
                "identity": identity_from_public_record(
                    name=rows[0].get("facility_name", "") if rows else "",
                    entity_type="hospital",
                    ccn=ccn,
                    source_name=rows[0].get("source_name", "CMS Provider Data Catalog") if rows else "CMS Provider Data Catalog",
                    source_url=rows[0].get("source_url", "https://data.cms.gov/provider-data/") if rows else "https://data.cms.gov/provider-data/",
                ).to_dict(),
                "datasets_checked": checked,
                "total_rows": len(rows),
                "rows": rows,
                "confidence": "high_for_exact_cms_measure_rows",
                "source_caveat": "Exact row-level CMS Provider Data Catalog measure rows only; adjacent summaries are not substituted.",
                "evidence": rows[0]["evidence"] if rows else {},
        }
        payload["source_metadata"] = _quality_source_metadata(payload["evidence"])
        payload["identity_map"] = _quality_identity_map(
            dataset=checked[0] if checked else "hospital_info",
            ccn=ccn,
            rows=rows,
            evidence=payload["evidence"],
            measure_ids=measure_ids,
            row_evidence_paths=("rows[].evidence",),
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("get_quality_measure_rows failed")
        return error_response(f"get_quality_measure_rows failed: {e}")


# ---------------------------------------------------------------------------
# Tool: compare_hospitals
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("hospital-quality")
async def compare_hospitals(ccns: list[str]) -> dict[str, Any]:
    """Compare quality, safety, readmission, and experience data across hospitals.

    Pulls all available metrics for each hospital and returns a side-by-side
    comparison as JSON. Useful for benchmarking hospitals against each other.

    Args:
        ccns: List of CMS Certification Numbers to compare (e.g. ["050454", "050755"]).

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compare_hospitals","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if not ccns or len(ccns) < 2:
        return _quality_comparison_error("Provide at least 2 CCNs to compare", ccns=ccns or [])
    if len(ccns) > 10:
        return _quality_comparison_error("Maximum 10 hospitals for comparison", ccns=ccns)

    comparisons = []
    for ccn in ccns:
        hospital = {"ccn": ccn}

        quality = await get_quality_scores(ccn)
        hospital["quality"] = _comparison_domain_payload(quality)

        safety = await get_safety_scores(ccn)
        hospital["safety"] = _comparison_domain_payload(safety)

        readmission = await get_readmission_data(ccn)
        hospital["readmission"] = _comparison_domain_payload(readmission)

        experience = await get_patient_experience(ccn)
        hospital["patient_experience"] = _comparison_domain_payload(experience)

        comparisons.append(hospital)

    matched_count = sum(
        1
        for hospital in comparisons
        if any(
            isinstance(hospital.get(domain), dict) and "error" not in hospital[domain]
            for domain in ("quality", "safety", "readmission", "patient_experience")
        )
    )
    evidence = _quality_comparison_evidence(ccns=ccns, matched_count=matched_count)
    return to_structured(
        {
            "hospital_count": len(comparisons),
            "matched_hospital_count": matched_count,
            "hospitals": comparisons,
            "identity_map": _quality_comparison_identity_map(comparisons, requested_ccns=ccns),
            "evidence": evidence,
            "source_metadata": _quality_source_metadata(evidence),
        }
    )


if __name__ == "__main__":
    mcp.run(transport=_transport)
