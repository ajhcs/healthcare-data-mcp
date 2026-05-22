"""Workforce & Labor Analytics MCP Server.

Provides tools for BLS employment data, HRSA shortage areas, CMS GME profiles,
ACGME residency programs, NLRB union activity, staffing benchmarks, and
HCRIS cost report staffing analysis.
"""

from typing import Any
from datetime import datetime, timezone
import logging
import os as _os
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils import ahrq_data
from shared.utils.bed_resolver import resolve_hospital_bed_source
from shared.utils.cost_report import load_cost_report_row
from shared.utils.healthcare_identity import MatchDecision, identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_name

from . import bls_client, labor_data, operations_data, workforce_data  # pyright: ignore[reportAttributeAccessIssue]
from servers.hospital_quality import data_loaders as hospital_quality_data_loaders
from .models import (
    BLSEmploymentResponse,
    CostReportStaffingResponse,
    DepartmentStaffing,
    GMEProfileResponse,
    HPSARecord,
    HRSAWorkforceResponse,
    NLRBElection,
    ResidencyProgram,
    ResidencyProgramsResponse,
    StaffingBenchmarksResponse,
    UnionActivityResponse,
    WorkStoppage,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "workforce-analytics"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8011"))
mcp = FastMCP(**_mcp_kwargs)


def _workforce_evidence(
    *,
    query: dict[str, Any],
    dataset_id: str,
    match_basis: str,
    confidence: str,
    source_name: str = "CMS HCRIS, AHRQ, PBJ, and public workforce sources",
    source_url: str = "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
    source_period: str = "latest available public source period at request time",
    cache_status: str = "live_or_configured_public_source",
    cache_freshness: str = "source freshness depends on public API response or configured local cache",
) -> dict[str, Any]:
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope="workforce_operations",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat="Public workforce and throughput metrics are source-field dependent; missing fields are not zero values.",
        next_step="Preserve metric_confidence, bed_source, and source rows before citing operational facts.",
    )


def _workforce_source_metadata(evidence: dict[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a workforce evidence receipt."""

    return {
        "source_name": evidence.get("source_name", ""),
        "source_url": evidence.get("source_url", ""),
        "dataset_id": evidence.get("dataset_id", ""),
        "source_period": evidence.get("source_period", ""),
        "landing_page": evidence.get("landing_page", ""),
        "retrieved_at": evidence.get("retrieved_at", ""),
        "source_modified": evidence.get("source_modified", ""),
        "cache_status": evidence.get("cache_status", ""),
        "cache_freshness": evidence.get("cache_freshness", ""),
        "entity_scope": evidence.get("entity_scope", "workforce_operations"),
        "query": evidence.get("query", {}),
        "cache_key": evidence.get("cache_key", ""),
        "source_type": "public_workforce_operations_source",
    }


def _attach_workforce_source_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        payload["source_metadata"] = _workforce_source_metadata(evidence)
    return payload


def _workforce_row_evidence(
    row: dict[str, Any],
    *,
    query: dict[str, Any],
    dataset_id: str,
    row_kind: str,
    match_basis: str,
    source_name: str,
    source_url: str,
    source_period: str = "latest available public source period at request time",
    cache_status: str = "configured_public_cache",
    cache_freshness: str = "source freshness depends on configured local cache or public source response",
    confidence: str | None = None,
) -> dict[str, Any]:
    row_query = {
        "row_kind": row_kind,
        "ccn": row.get("ccn") or query.get("ccn") or "",
        "state": row.get("state") or query.get("state") or "",
        "facility_name": row.get("facility_name") or row.get("hospital_name") or "",
        "program_id": row.get("program_id") or query.get("program_id") or "",
        "institution": row.get("institution") or query.get("institution") or "",
        "specialty": row.get("specialty") or query.get("specialty") or "",
        "discipline": row.get("discipline") or query.get("discipline") or "",
        "designation_id": row.get("designation_id") or row.get("hpsa_id") or "",
        "case_number": row.get("case_number") or "",
        "employer": row.get("employer") or query.get("employer_name") or "",
        "date": row.get("date") or row.get("start_date") or row.get("fy_end_dt") or "",
        "data_period": row.get("data_period") or row.get("date") or query.get("quarter") or query.get("year") or "",
        "department": row.get("dept_name") or "",
    }
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope="workforce_operations",
        query={key: value for key, value in row_query.items() if value not in ("", None)},
        match_basis=match_basis,
        confidence=confidence or str(row.get("confidence") or row.get("match_confidence") or "source_row"),
        caveat="Workforce rows are source-scoped public records; candidate names, employers, and program search hits are not facility identity proof without exact identifiers.",
        next_step="Preserve this row receipt with the parent response evidence and identity_map before citing workforce, staffing, teaching, or labor facts.",
    )


def _attach_workforce_row_evidence(
    rows: list[dict[str, Any]],
    *,
    query: dict[str, Any],
    dataset_id: str,
    row_kind: str,
    match_basis: str,
    source_name: str,
    source_url: str,
    source_period: str = "latest available public source period at request time",
    cache_status: str = "configured_public_cache",
    cache_freshness: str = "source freshness depends on configured local cache or public source response",
    confidence: str | None = None,
) -> list[dict[str, Any]]:
    for row in rows:
        if isinstance(row, dict):
            row["evidence"] = _workforce_row_evidence(
                row,
                query=query,
                dataset_id=dataset_id,
                row_kind=row_kind,
                match_basis=match_basis,
                source_name=source_name,
                source_url=source_url,
                source_period=source_period,
                cache_status=cache_status,
                cache_freshness=cache_freshness,
                confidence=confidence,
            )
    return rows


def _attach_bed_source_evidence(bed_source: dict[str, Any], *, query: dict[str, Any]) -> dict[str, Any]:
    """Attach row-level receipts to bed-source candidates and selected source."""

    for key, match_basis in (
        ("candidates", "hospital_bed_source_candidate_row"),
        ("rejected_candidates", "hospital_bed_source_rejected_candidate_row"),
    ):
        rows = bed_source.get(key)
        if isinstance(rows, list):
            _attach_workforce_row_evidence(
                rows,
                query=query,
                dataset_id="hospital_bed_identity_resolution",
                row_kind=key.rstrip("s"),
                match_basis=match_basis,
                source_name="CMS POS, HCRIS, AHRQ, and public state bed sources",
                source_url=_bed_source_url(str(bed_source.get("selected_source") or "")),
                source_period=str(bed_source.get("source_period") or query.get("year") or "latest available public period"),
                cache_status="mixed_public_cache",
                cache_freshness="depends on configured POS/HCRIS/AHRQ/state cache freshness",
                confidence=None,
            )

    selected = _selected_bed_candidate(bed_source)
    if selected:
        bed_source["selected_candidate_evidence"] = _workforce_row_evidence(
            selected,
            query=query,
            dataset_id="hospital_bed_identity_resolution",
            row_kind="selected_bed_source_candidate",
            match_basis="hospital_bed_source_selected_candidate_row",
            source_name=str(selected.get("source") or "CMS POS, HCRIS, AHRQ, and public state bed sources"),
            source_url=_bed_source_url(str(selected.get("source") or "")),
            source_period=str(selected.get("source_period") or bed_source.get("source_period") or query.get("year") or "latest available public period"),
            cache_status="mixed_public_cache",
            cache_freshness="depends on configured POS/HCRIS/AHRQ/state cache freshness",
            confidence=str(selected.get("confidence") or bed_source.get("confidence") or "bed_source_ranked_resolution"),
        )
    return bed_source


def _selected_bed_candidate(bed_source: dict[str, Any]) -> dict[str, Any] | None:
    selected_source = str(bed_source.get("selected_source") or "")
    selected_field = str(bed_source.get("selected_source_field") or "")
    selected_count = bed_source.get("selected_bed_count")
    for candidate in bed_source.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        if str(candidate.get("source") or "") != selected_source:
            continue
        if str(candidate.get("source_field") or "") != selected_field:
            continue
        if candidate.get("selected_bed_count") == selected_count:
            return candidate
    return None


def _bed_source_url(source: str) -> str:
    source_lower = source.lower()
    if "provider of services" in source_lower:
        return "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities"
    if "hcris" in source_lower or "cost report" in source_lower:
        return "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report"
    if "ahrq" in source_lower:
        return "https://www.ahrq.gov/chsp/data-resources/compendium.html"
    if "pennsylvania" in source_lower or "phc4" in source_lower:
        return "https://www.phc4.org/reports/"
    return "https://data.cms.gov/"


def _workforce_metric_source_url(source: str) -> str:
    source_lower = source.lower()
    if "bls" in source_lower:
        return "https://www.bls.gov/"
    if "hrsa" in source_lower or "hpsa" in source_lower:
        return "https://data.hrsa.gov/"
    return _bed_source_url(source)


def _workforce_metric_evidence(
    *,
    metric_confidence: dict[str, Any],
    payload: dict[str, Any],
    query: dict[str, Any],
    dataset_id: str,
    match_basis_prefix: str,
) -> dict[str, dict[str, Any]]:
    metric_receipts: dict[str, dict[str, Any]] = {}
    for metric_name, metadata in metric_confidence.items():
        if not isinstance(metadata, dict):
            metadata = {"confidence": str(metadata)}
        source = str(metadata.get("source") or payload.get("source") or "Public workforce operations source")
        confidence = str(metadata.get("confidence") or "not_available")
        metric_query = {
            **query,
            "metric_name": str(metric_name),
            "metric_value_present": _metric_value_present(payload, str(metric_name)),
            "metric_confidence": confidence,
            "source": source,
            "source_field": metadata.get("source_field") or "",
            "ccn": payload.get("ccn") or query.get("ccn") or "",
            "state_facility_id": payload.get("state_facility_id") or query.get("state_facility_id") or "",
            "state": payload.get("state") or query.get("state") or "",
            "year": payload.get("year") or query.get("year") or "",
        }
        metric_receipts[str(metric_name)] = _workforce_evidence(
            query={key: value for key, value in metric_query.items() if value not in ("", None, [], {})},
            dataset_id=dataset_id,
            source_name=source,
            source_url=_workforce_metric_source_url(source),
            source_period=str(payload.get("year") or query.get("year") or "latest available public source period at request time"),
            cache_status="mixed_public_cache",
            cache_freshness="source freshness depends on configured POS/HCRIS/AHRQ/state cache freshness",
            match_basis=f"{match_basis_prefix}_metric_{metric_name}",
            confidence=confidence,
        ) | {
            "caveat": (
                "This metric is source-field-specific public workforce/operations context; missing fields are not zero values "
                "and derived rates require preserving their input source fields."
            ),
            "next_step": (
                "Preserve this metric receipt, source_field, confidence, and parent identity_map before citing the value."
            ),
        }
    return metric_receipts


def _metric_value_present(payload: dict[str, Any], metric_name: str) -> bool:
    if metric_name in payload:
        return payload.get(metric_name) not in (None, "")
    if metric_name in {"ct_scans", "mri_scans", "cardiac_catheterizations", "open_heart_procedures"}:
        nested_key = {
            "ct_scans": "ct",
            "mri_scans": "mri",
            "cardiac_catheterizations": "cath",
            "open_heart_procedures": "open_heart",
        }[metric_name]
        values = payload.get("ct_mri_cath_open_heart_volumes")
        if isinstance(values, dict):
            return values.get(nested_key) not in (None, "")
    return False


def _workforce_identity(
    *,
    ccn: str = "",
    facility_name: str = "",
    address: str = "",
    zip_code: str = "",
    match_basis: str = "",
    confidence: str = "",
) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=facility_name,
        entity_type="workforce_operations",
        ccn=ccn,
        address=address,
        zip_code=zip_code,
        source_name="workforce-analytics public operations workflow",
        source_url="https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
    )
    if match_basis or confidence:
        identity.match_decisions.append(
            MatchDecision(
                basis=match_basis,
                confidence=confidence,
                decided_at=datetime.now(timezone.utc).isoformat(),
                notes="Workforce identity is anchored by public CCN when present; operational metrics remain source-field dependent.",
            )
        )
    return identity.to_dict()


def _workforce_identity_map(
    *,
    query: dict[str, Any],
    payload: dict[str, Any] | None = None,
    dataset_id: str = "",
    ccn: str = "",
    state_facility_id: str = "",
    state: str = "",
    year: int | str = "",
    facility_name: str = "",
    facility_type: str = "",
    program_id: str = "",
    occupation: str = "",
    area_code: str = "",
    discipline: str = "",
    employer_name: str = "",
) -> dict[str, Any]:
    """Return workforce-specific join keys and source-claim boundaries."""

    data = payload or {}
    identity = data.get("identity") if isinstance(data.get("identity"), dict) else {}
    evidence = data.get("evidence") if isinstance(data.get("evidence"), dict) else {}
    effective_dataset_id = dataset_id or str(evidence.get("dataset_id") or "")
    join_values = {
        "ccn": _workforce_identity_values("ccn", ccn, query.get("ccn"), data.get("ccn"), identity.get("ccn")),
        "state_facility_id": _workforce_identity_values(
            "state_facility_id",
            state_facility_id,
            query.get("state_facility_id"),
            data.get("state_facility_id"),
        ),
        "canonical_name": _workforce_identity_values(
            "canonical_name",
            facility_name,
            query.get("hospital_name"),
            data.get("hospital_name"),
            data.get("facility_name"),
            identity.get("canonical_name"),
        ),
        "state": _workforce_identity_values("state", state, query.get("state"), data.get("state")),
        "year": _workforce_identity_values("year", year, query.get("year"), data.get("year"), data.get("fiscal_year")),
        "facility_type": _workforce_identity_values(
            "facility_type",
            facility_type,
            query.get("facility_type"),
            data.get("facility_type"),
        ),
        "program_id": _workforce_identity_values(
            "program_id",
            program_id,
            query.get("program_id"),
            data.get("program_id"),
            (data.get("program") or {}).get("program_id") if isinstance(data.get("program"), dict) else "",
        ),
        "occupation": _workforce_identity_values(
            "occupation",
            occupation,
            query.get("occupation"),
            data.get("occupation_title"),
            data.get("soc_code"),
        ),
        "area_code": _workforce_identity_values("area_code", area_code, query.get("area_code"), data.get("area_code")),
        "discipline": _workforce_identity_values("discipline", discipline, query.get("discipline")),
        "employer_name": _workforce_identity_values("employer_name", employer_name, query.get("employer_name")),
    }
    source_claims = _workforce_source_claims(dataset_id=effective_dataset_id, payload=data)
    return {
        "entity_scope": "workforce_operations",
        "join_keys": [
            {
                "field": field,
                "values": values,
                "status": "provided" if values else "missing",
                "used_by": _workforce_join_key_usage(field, source_claims),
            }
            for field, values in join_values.items()
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Use CCN for hospital HCRIS, AHRQ, POS, and most public throughput joins when present.",
            "Use state_facility_id only for state source rows that explicitly expose that identifier; keep it separate from CCN.",
            "Treat facility names, state filters, facility types, and program/institution names as aliases or candidate filters unless exact identifiers support the join.",
            "Preserve bed_source, metric_confidence, source_rankings, source period, and evidence caveats before citing public operations facts.",
        ],
        "missing_data_policy": (
            "No-match or missing workforce/operations responses identify the searched public-source scope; "
            "they are not proof of zero staffing, zero volume, no teaching activity, no programs, no shortage area, or current operational capacity."
        ),
    }


def _workforce_identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        normalized = _normalize_workforce_identity_value(field, value)
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _normalize_workforce_identity_value(field: str, value: Any) -> str:
    if value in ("", None, 0, "0"):
        return ""
    if field == "ccn":
        return normalize_ccn(value) or ""
    if field == "canonical_name":
        return normalize_name(value, remove_legal_suffixes=True)
    if field == "state":
        return str(value).strip().upper()
    if field == "facility_type":
        return str(value).strip().lower()
    if field in {"state_facility_id", "program_id"}:
        return str(value).strip().upper()
    if field in {"occupation", "discipline", "employer_name"}:
        return normalize_name(value, remove_legal_suffixes=True)
    if field == "area_code":
        return str(value).strip().upper()
    if field == "year":
        return str(value).strip()
    return str(value).strip()


def _workforce_source_claims(*, dataset_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    collections = {
        "bls_oes_employment": [
            {
                "collection": "bls_oes_employment",
                "identity_paths": [
                    "query.occupation",
                    "occupation_title",
                    "soc_code",
                    "query.area_code",
                    "query.state",
                    "area_name",
                    "data_year",
                ],
                "evidence_path": "evidence",
                "match_policy": "occupation_area_state_required_for_bls_oes_fact",
            }
        ],
        "hrsa_hpsa_workforce": [
            {
                "collection": "hrsa_hpsa_workforce",
                "identity_paths": [
                    "query.state",
                    "query.county_fips",
                    "query.discipline",
                    "hpsas[].designation_id",
                    "hpsas[].discipline",
                    "hpsas[].state",
                ],
                "evidence_path": "evidence",
                "row_evidence_paths": ["hpsas[].evidence"],
                "match_policy": "state_county_discipline_scope_required_for_hrsa_hpsa_fact",
            }
        ],
        "nlrb_bls_labor_activity": [
            {
                "collection": "nlrb_bls_labor_activity",
                "identity_paths": [
                    "query.employer_name",
                    "query.state",
                    "query.year_start",
                    "query.year_end",
                    "elections[].employer",
                    "work_stoppages[].employer",
                ],
                "evidence_path": "evidence",
                "row_evidence_paths": ["elections[].evidence", "work_stoppages[].evidence"],
                "match_policy": "employer_state_year_search_returns_candidate_labor_activity",
            }
        ],
        "cms_hcris_workforce_productivity": [
            {
                "collection": "cms_hcris_workforce_productivity",
                "identity_paths": ["query.ccn", "ccn", "identity.ccn", "departments", "peer_group_metadata"],
                "evidence_path": "evidence",
                "row_evidence_paths": [
                    "departments[].evidence",
                    "bed_source.selected_candidate_evidence",
                    "bed_source.candidates[].evidence",
                    "bed_source.rejected_candidates[].evidence",
                ],
                "match_policy": "ccn_required_for_hcris_staffing_productivity_facts",
            },
            {
                "collection": "ahrq_hospital_linkage",
                "identity_paths": ["query.ccn", "hospital_name", "state", "peer_group_metadata.attributes.state"],
                "evidence_path": "evidence",
                "match_policy": "ccn_exact_or_source_row_context_only",
            },
        ],
        "cms_hcris_workforce_staffing": [
            {
                "collection": "cms_hcris_staffing",
                "identity_paths": ["query.ccn", "ccn", "identity.ccn", "fiscal_year", "departments"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["departments[].evidence"],
                "match_policy": "ccn_required_for_hospital_cost_report_staffing_facts",
            }
        ],
        "cms_hcris_gme": [
            {
                "collection": "cms_hcris_gme",
                "identity_paths": ["query.ccn", "ccn", "identity.ccn", "year", "resident_fte"],
                "evidence_path": "evidence",
                "match_policy": "ccn_required_for_hcris_teaching_and_gme_facts",
            }
        ],
        "cms_pbj_nursing_staffing": [
            {
                "collection": "cms_pbj_nursing_staffing",
                "identity_paths": ["query.ccn", "query.state", "ccn", "facility_name", "facility_type", "data_period"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["records[].evidence"],
                "match_policy": "ccn_or_state_filter_required_for_pbj_rows",
            }
        ],
        "hospital_bed_identity_resolution": [
            {
                "collection": "hospital_bed_resolution",
                "identity_paths": [
                    "query.ccn",
                    "query.state_facility_id",
                    "selected_source",
                    "selected_source_field",
                    "source_rankings",
                    "bed_source",
                ],
                "evidence_path": "evidence",
                "row_evidence_paths": [
                    "selected_candidate_evidence",
                    "candidates[].evidence",
                    "rejected_candidates[].evidence",
                ],
                "match_policy": "ranked_public_bed_source_resolution_keeps_source_identifier_boundaries",
            }
        ],
        "public_hospital_throughput": [
            {
                "collection": "public_hospital_throughput",
                "identity_paths": [
                    "query.ccn",
                    "query.state_facility_id",
                    "ccn",
                    "state_facility_id",
                    "hospital_name",
                    "state",
                    "source_rankings",
                    "metric_confidence",
                ],
                "evidence_path": "evidence",
                "row_evidence_paths": [
                    "bed_source.selected_candidate_evidence",
                    "bed_source.candidates[].evidence",
                    "bed_source.rejected_candidates[].evidence",
                ],
                "metric_evidence_paths": ["metric_evidence.*"],
                "match_policy": "ccn_or_state_facility_id_required_for_source_backed_throughput_facts",
            }
        ],
        "acgme_program_search_public_export": [
            {
                "collection": "acgme_program_search_public_export",
                "identity_paths": ["query.program_id", "program.program_id", "program.institution", "program.state", "program.specialty"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["programs[].evidence"],
                "match_policy": "program_id_exact_for_program_facts; institution_specialty_state_search_returns_candidates",
            }
        ],
    }
    claims = list(collections.get(dataset_id, []))
    if not claims and ("bed_source" in payload or "source_rankings" in payload):
        claims = list(collections["public_hospital_throughput"])
    if not claims:
        claims = [
            {
                "collection": "workforce_source_query",
                "identity_paths": ["query.ccn", "query.state_facility_id", "query.state", "query.hospital_name", "query.program_id"],
                "evidence_path": "evidence",
                "match_policy": "exact_public_identifier_required_for_workforce_operations_fact",
            }
        ]
    return claims


def _workforce_join_key_usage(field: str, source_claims: list[dict[str, Any]]) -> list[str]:
    path_tokens = {
        "ccn": ("ccn",),
        "state_facility_id": ("state_facility_id",),
        "canonical_name": ("hospital_name", "facility_name", "institution"),
        "state": ("state",),
        "year": ("year", "fiscal_year", "data_period"),
        "facility_type": ("facility_type",),
        "program_id": ("program_id",),
        "occupation": ("occupation", "occupation_title", "soc_code"),
        "area_code": ("area_code",),
        "discipline": ("discipline",),
        "employer_name": ("employer_name", "employer"),
    }[field]
    used_by = []
    for claim in source_claims:
        paths = " ".join(str(path) for path in claim.get("identity_paths", []))
        if any(token in paths for token in path_tokens):
            used_by.append(str(claim.get("collection") or ""))
    return sorted(item for item in used_by if item)


def _workforce_error_response(
    message: str,
    *,
    query: dict[str, Any],
    dataset_id: str,
    match_basis: str,
    confidence: str,
    code: str = "not_found",
    source_name: str = "CMS HCRIS, AHRQ, PBJ, and public workforce sources",
    source_url: str = "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
    source_period: str = "latest available public source period at request time",
    cache_status: str = "live_or_configured_public_source",
    cache_freshness: str = "source freshness depends on public API response or configured local cache",
) -> dict[str, Any]:
    evidence = _workforce_evidence(
        query=query,
        dataset_id=dataset_id,
        source_name=source_name,
        source_url=source_url,
        source_period=source_period,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        match_basis=match_basis,
        confidence=confidence,
    )
    return error_response(
        message,
        code=code,
        evidence=evidence,
        source_metadata=_workforce_source_metadata(evidence),
        identity=_workforce_identity(
            ccn=str(query.get("ccn") or ""),
            facility_name=str(query.get("hospital_name") or ""),
            match_basis=match_basis,
            confidence=confidence,
        ),
        identity_map=_workforce_identity_map(
            query=query,
            dataset_id=dataset_id,
            ccn=str(query.get("ccn") or ""),
            state_facility_id=str(query.get("state_facility_id") or ""),
            state=str(query.get("state") or ""),
            year=str(query.get("year") or ""),
            facility_name=str(query.get("hospital_name") or ""),
            facility_type=str(query.get("facility_type") or ""),
            program_id=str(query.get("program_id") or ""),
        ),
    )


def _facility_name_from_row(row: dict[str, Any]) -> str:
    for key in ("hospital_name", "facility_name", "provider_name", "name"):
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


async def _ahrq_hospital_row(ccn: str) -> dict[str, Any]:
    if not ccn:
        return {}
    try:
        df = await ahrq_data.load_ahrq_hospital_linkage()
        if df.empty or "ccn" not in df.columns:
            return {}
        matches = df[df["ccn"].astype(str).str.zfill(6) == ccn.strip().zfill(6)]
        if matches.empty:
            return {}
        return {str(k): v for k, v in matches.iloc[0].to_dict().items()}
    except Exception:
        logger.debug("AHRQ hospital linkage lookup failed", exc_info=True)
        return {}


async def _cost_report_row(ccn: str, year: int = 0) -> Any | None:
    if not ccn:
        return None
    row, error = await load_cost_report_row(hospital_quality_data_loaders, ccn, year=year)
    if error:
        logger.debug("Cost report row unavailable for %s: %s", ccn, error)
        return None
    return row


async def _pos_row(ccn: str) -> Any | None:
    if not ccn:
        return None
    try:
        df = await ahrq_data.load_pos()
        if df.empty:
            return None
        ccn_col = next((col for col in ("PRVDR_NUM", "PROVIDER_NUMBER", "CCN") if col in df.columns), "")
        if not ccn_col:
            return None
        matches = df[df[ccn_col].astype(str).str.strip().str.zfill(6) == ccn.strip().zfill(6)]
        if matches.empty:
            return None
        return matches.iloc[0]
    except Exception:
        logger.debug("POS lookup failed for %s", ccn, exc_info=True)
        return None


async def _productivity_profile(ccn: str, year: int = 0) -> dict[str, Any]:
    await workforce_data.ensure_hcris_cached()
    staffing = workforce_data.query_hcris_staffing(ccn, year=year) or {}
    gme = workforce_data.query_hcris_gme(ccn, year=year) or {}
    ahrq_row = await _ahrq_hospital_row(ccn)
    cost_row = await _cost_report_row(ccn, year=year)
    total_ftes = operations_data.dict_float(staffing, "total_ftes")
    state_code = str(ahrq_row.get("hosp_state", "") or ahrq_row.get("state", "")).upper()
    bed_source = resolve_hospital_bed_source(
        ccn=ccn,
        state=state_code,
        year=year,
        target_scope="ccn",
        pos_row=await _pos_row(ccn),
        hcris_row=cost_row,
        ahrq_row=ahrq_row,
        pa_rows=operations_data._pa_bed_rows(
            state=state_code,
            ccn=ccn,
            state_facility_id="",
            hospital_name=str(ahrq_row.get("hospital_name", "")),
            year=year,
        ),
    )
    _attach_bed_source_evidence(bed_source, query={"ccn": ccn, "state": state_code, "year": year, "target_scope": "ccn"})
    beds = bed_source.get("selected_bed_count")
    discharges = operations_data.dict_float(ahrq_row, "hos_dsch", "discharges") or operations_data.series_float(
        cost_row,
        "total_discharges",
        "discharges",
        "total_hospital_discharges",
        "medicare_discharges",
    )
    patient_days = operations_data.series_float(
        cost_row,
        "total_inpatient_days",
        "inpatient_days",
        "days_of_care",
        "total_patient_days",
    )
    occupied_beds = operations_data.ratio(patient_days, 365) if patient_days is not None else None
    adjusted_patient_days = operations_data.series_float(cost_row, "adjusted_patient_days", "adj_patient_days")
    cmi = operations_data.series_float(cost_row, "case_mix_index", "cmi", "casemix_index")
    resident_fte = operations_data.dict_float(gme, "total_resident_ftes")
    case_mix_adjusted_discharges = discharges * cmi if discharges is not None and cmi is not None else None
    peer_group_metadata = _peer_group_metadata(ahrq_row, beds=beds, resident_fte=resident_fte)
    evidence = _workforce_evidence(
        query={"ccn": ccn, "year": year},
        dataset_id="cms_hcris_workforce_productivity",
        match_basis="ccn_exact_public_cost_report_and_ahrq_linkage",
        confidence="high_for_reported_public_fields",
    )
    facility_name = _facility_name_from_row(ahrq_row)
    payload = {
        "ccn": ccn,
        "year": year or 0,
        "source": "CMS HCRIS Worksheet S-3 with AHRQ hospital linkage where available",
        "source_confidence": "high_for_reported_hcris_fields",
        "total_ftes": total_ftes,
        "beds": beds,
        "bed_source": bed_source,
        "discharges": discharges,
        "patient_days": patient_days,
        "occupied_beds": occupied_beds,
        "case_mix_index": cmi,
        "fte_per_occupied_bed": operations_data.ratio(total_ftes, occupied_beds),
        "fte_per_adjusted_patient_day": operations_data.ratio(total_ftes, adjusted_patient_days),
        "fte_per_bed": operations_data.ratio(total_ftes, beds),
        "fte_per_discharge": operations_data.ratio(total_ftes, discharges),
        "resident_fte": resident_fte,
        "resident_to_bed_ratio": operations_data.ratio(resident_fte, beds),
        "case_mix_adjusted_discharges_per_fte": operations_data.ratio(case_mix_adjusted_discharges, total_ftes),
        "optional_metric_caveat": "case_mix_adjusted_discharges_per_fte is only populated when public CMI and discharge fields are present.",
        "departments": _attach_workforce_row_evidence(
            list(staffing.get("departments", [])),
            query={"ccn": ccn, "year": year},
            dataset_id="cms_hcris_workforce_productivity",
            row_kind="hcris_department_staffing",
            match_basis="hcris_department_staffing_row",
            source_name="CMS HCRIS Worksheet S-3",
            source_url="https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
            source_period=str(year or "latest cached HCRIS fiscal period"),
            cache_status="configured_hcris_cache",
            cache_freshness="managed by HCRIS cache import/update process",
            confidence="high_for_reported_hcris_fields",
        ),
        "peer_group_metadata": peer_group_metadata,
        "identity": _workforce_identity(
            ccn=ccn,
            facility_name=facility_name,
            match_basis=evidence["match_basis"],
            confidence=evidence["confidence"],
        ),
        "evidence": evidence,
    }
    payload["identity_map"] = _workforce_identity_map(
        query={"ccn": ccn, "year": year},
        payload=payload,
        dataset_id="cms_hcris_workforce_productivity",
        ccn=ccn,
        year=year,
        facility_name=facility_name,
    )
    return _attach_workforce_source_metadata(payload)


async def _throughput_profile(ccn: str = "", state_facility_id: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    payload = await operations_data.throughput_profile(
        ccn=ccn,
        state_facility_id=state_facility_id,
        state=state,
        year=year,
        hospital_row_loader=_ahrq_hospital_row,
        cost_report_row_loader=_cost_report_row,
        pos_row_loader=_pos_row,
    )
    if isinstance(payload.get("bed_source"), dict):
        _attach_bed_source_evidence(
            payload["bed_source"],
            query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year},
        )
    payload["identity"] = _workforce_identity(
        ccn=ccn,
        facility_name=str(payload.get("hospital_name", "")),
        match_basis="ccn_or_state_facility_id_public_source_lookup",
        confidence=str(payload.get("confidence") or "metric_level_confidence"),
    )
    payload["identity_map"] = _workforce_identity_map(
        query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year},
        payload=payload,
        dataset_id="public_hospital_throughput",
        ccn=ccn,
        state_facility_id=state_facility_id,
        state=state,
        year=year,
        facility_name=str(payload.get("hospital_name", "")),
    )
    payload["evidence"] = _workforce_evidence(
        query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year},
        dataset_id="public_hospital_throughput",
        match_basis="ccn_or_state_facility_id_public_source_lookup",
        confidence=str(payload.get("confidence") or "metric_level_confidence"),
    )
    payload["metric_evidence"] = _workforce_metric_evidence(
        metric_confidence=payload.get("metric_confidence") or {},
        payload=payload,
        query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year},
        dataset_id="public_hospital_throughput",
        match_basis_prefix="ccn_or_state_facility_id_public_source_lookup",
    )
    return _attach_workforce_source_metadata(payload)


def _peer_group_metadata(row: dict[str, Any], *, beds: float | None, resident_fte: float | None) -> dict[str, Any]:
    state = str(row.get("hosp_state", "") or row.get("state", "")).upper()
    rural_urban = _rural_urban_value(row)
    attributes = {
        "state": state,
        "bed_size": _bed_size_group(beds),
        "teaching": "teaching" if (resident_fte or 0) > 0 else "non_teaching",
        "rural_urban": rural_urban,
    }
    available = [key for key, value in attributes.items() if value not in ("", None)]
    return {
        "attributes": attributes,
        "available_dimensions": available,
        "logic": "Peer grouping can combine state, bed_size, teaching, and rural_urban when those attributes are present.",
    }


def _bed_size_group(beds: float | None) -> str:
    if beds is None:
        return ""
    if beds < 25:
        return "under_25"
    if beds < 100:
        return "25_99"
    if beds < 300:
        return "100_299"
    if beds < 500:
        return "300_499"
    return "500_plus"


def _rural_urban_value(row: dict[str, Any]) -> str:
    for key in ("urban_rural", "urban_rural_indicator", "rural_urban", "cbsa_urban_rural", "ruralurban"):
        value = str(row.get(key, "")).strip().lower()
        if not value:
            continue
        if "rural" in value:
            return "rural"
        if "urban" in value:
            return "urban"
        return value
    return ""


# ---------------------------------------------------------------------------
# Tool 1: get_bls_employment
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_bls_employment(
    occupation: str, area_code: str = "", state: str = "",
    include_projections: bool = True,  # noqa: ARG001 — exposed in MCP schema
) -> dict[str, Any]:
    """Get occupation-level employment counts, wages, and projections by MSA or state.

    Uses BLS OES (Occupational Employment and Wage Statistics) API v2.

    Args:
        occupation: Occupation name (e.g. "Registered Nurses") or SOC code (e.g. "29-1141").
        area_code: BLS area code (MSA FIPS). Leave empty for state or national.
        state: Two-letter state code (e.g. "PA"). Leave empty for national.
        include_projections: Include 10-year employment projections.
    """
    try:
        result = await bls_client.get_oes_data(occupation, area_code, state)
        if not result:
            return _workforce_error_response(
                "No data returned from BLS API",
                query={"occupation": occupation, "area_code": area_code, "state": state},
                dataset_id="bls_oes_employment",
                source_name="BLS Occupational Employment and Wage Statistics",
                source_url="https://www.bls.gov/oes/",
                source_period="latest BLS OEWS release available at request time",
                match_basis="occupation_area_state_public_api_lookup_no_match",
                confidence="no_matching_bls_oes_record_returned",
            )
        if "error" in result:
            return to_structured(result)

        response = BLSEmploymentResponse(
            occupation_title=result.get("occupation_title", ""),
            soc_code=result.get("soc_code", ""),
            area_name=result.get("area_name", state or "National"),
            employment=result.get("employment", 0),
            mean_wage=result.get("mean_wage", 0),
            median_wage=result.get("median_wage", 0),
            pct_10_wage=result.get("pct_10_wage", 0),
            pct_90_wage=result.get("pct_90_wage", 0),
            data_year=result.get("data_year", ""),
        )
        payload = response.model_dump()
        payload["evidence"] = _workforce_evidence(
            query={"occupation": occupation, "area_code": area_code, "state": state},
            dataset_id="bls_oes_employment",
            source_name="BLS Occupational Employment and Wage Statistics",
            source_url="https://www.bls.gov/oes/",
            source_period=str(result.get("data_year", "")) or "latest BLS OEWS release available at request time",
            match_basis="occupation_area_state_public_api_lookup",
            confidence="high_for_reported_bls_oes_fields",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"occupation": occupation, "area_code": area_code, "state": state},
            payload=payload,
            dataset_id="bls_oes_employment",
            state=state,
            year=str(result.get("data_year", "")),
            occupation=occupation,
            area_code=area_code,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_bls_employment failed")
        return error_response(f"get_bls_employment failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_hrsa_workforce
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_hrsa_workforce(
    state: str, county_fips: str = "", discipline: str = "",
) -> dict[str, Any]:
    """Get health workforce shortage areas (HPSAs) and supply data for a state.

    Uses HRSA Data Warehouse HPSA data and Area Health Resource File.

    Args:
        state: Two-letter state code (e.g. "PA").
        county_fips: 5-digit county FIPS code for county-level detail.
        discipline: Filter by discipline ("Primary Care", "Dental", "Mental Health").
    """
    try:
        await workforce_data.ensure_hpsa_cached()

        hpsas = workforce_data.query_hpsas(state, discipline, county_fips)

        response = HRSAWorkforceResponse(
            state=state.upper(),
            total_hpsas=len(hpsas),
            hpsas=[HPSARecord(**h) for h in hpsas if "error" not in h],
        )
        payload = response.model_dump()
        _attach_workforce_row_evidence(
            payload["hpsas"],
            query={"state": state, "county_fips": county_fips, "discipline": discipline},
            dataset_id="hrsa_hpsa_workforce",
            row_kind="hrsa_hpsa",
            match_basis="hrsa_hpsa_source_row",
            source_name="HRSA Data Warehouse HPSA public data",
            source_url="https://data.hrsa.gov/",
            source_period="latest cached HRSA HPSA extract",
            cache_status="configured_hrsa_cache",
            cache_freshness="managed by workforce cache import/update process",
            confidence="high_for_reported_hrsa_hpsa_rows",
        )
        payload["evidence"] = _workforce_evidence(
            query={"state": state, "county_fips": county_fips, "discipline": discipline},
            dataset_id="hrsa_hpsa_workforce",
            source_name="HRSA Data Warehouse HPSA public data",
            source_url="https://data.hrsa.gov/",
            source_period="latest cached HRSA HPSA extract",
            cache_status="configured_hrsa_cache",
            cache_freshness="managed by workforce cache import/update process",
            match_basis="state_county_discipline_filter",
            confidence="high_for_reported_hrsa_hpsa_rows",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"state": state, "county_fips": county_fips, "discipline": discipline},
            payload=payload,
            dataset_id="hrsa_hpsa_workforce",
            state=state,
            discipline=discipline,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_hrsa_workforce failed")
        return error_response(f"get_hrsa_workforce failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: get_gme_profile
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_gme_profile(
    hospital_name: str = "", ccn: str = "",
) -> dict[str, Any]:
    """Get graduate medical education profile for a teaching hospital.

    Uses CMS HCRIS Worksheet S-2 for resident FTEs, IME/DGME payments,
    teaching status, and bed count.

    Args:
        hospital_name: Hospital name (fuzzy search).
        ccn: 6-digit CMS Certification Number (preferred, exact match).
    """
    try:
        await workforce_data.ensure_hcris_cached()

        if not ccn and hospital_name:
            return error_response("CCN required for HCRIS lookup. Use hospital_name with CMS facility search to find the CCN first.")

        result = workforce_data.query_hcris_gme(ccn)
        if not result:
            return _workforce_error_response(
                f"No GME data found for CCN: {ccn}",
                query={"ccn": ccn, "hospital_name": hospital_name},
                dataset_id="cms_hcris_gme",
                source_name="CMS HCRIS Worksheet S-2",
                source_period="latest cached HCRIS fiscal period unless year is specified",
                cache_status="configured_hcris_cache",
                cache_freshness="managed by HCRIS cache import/update process",
                match_basis="ccn_exact_hcris_gme_no_match",
                confidence="no_matching_hcris_gme_row_in_loaded_cache",
            )

        response = GMEProfileResponse(**result)
        payload = response.model_dump()
        payload["evidence"] = _workforce_evidence(
            query={"ccn": ccn, "hospital_name": hospital_name},
            dataset_id="cms_hcris_gme",
            source_name="CMS HCRIS Worksheet S-2",
            source_period="latest cached HCRIS fiscal period unless year is specified",
            cache_status="configured_hcris_cache",
            cache_freshness="managed by HCRIS cache import/update process",
            match_basis="ccn_exact_hcris_gme_row",
            confidence="high_for_reported_hcris_fields",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "hospital_name": hospital_name},
            payload=payload,
            dataset_id="cms_hcris_gme",
            ccn=ccn,
            facility_name=hospital_name,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_gme_profile failed")
        return error_response(f"get_gme_profile failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: get_residency_programs
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_acgme_source_status() -> dict[str, Any]:
    """Return ACGME public export/import status for residency program inventory."""
    try:
        payload = workforce_data.get_acgme_source_status()
        payload["evidence"] = _workforce_evidence(
            query={},
            dataset_id="acgme_program_search_public_export",
            source_name="ACGME Program Search public export",
            source_url="https://apps.acgme.org/ads/Public/Programs/Search",
            source_period=str(payload.get("source_period") or "current imported ACGME public export status"),
            cache_status=str(payload.get("status") or ""),
            cache_freshness=str(payload.get("cache_freshness") or payload.get("source_caveat") or "local import status reported by source_status"),
            match_basis="local_acgme_import_status",
            confidence="source_status_only",
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_acgme_source_status failed")
        return error_response(f"get_acgme_source_status failed: {e}")


@mcp.tool(structured_output=True)
async def get_acgme_program(program_id: str) -> dict[str, Any]:
    """Return one exact ACGME program by 10-digit Program Code."""
    try:
        status = workforce_data.get_acgme_source_status()
        if status["status"] != "ready":
            status["evidence"] = _workforce_evidence(
                query={"program_id": program_id},
                dataset_id="acgme_program_search_public_export",
                source_name="ACGME Program Search public export",
                source_url="https://apps.acgme.org/ads/Public/Programs/Search",
                source_period=str(status.get("source_period") or "current imported ACGME public export status"),
                cache_status=str(status.get("status") or ""),
                cache_freshness=str(status.get("source_caveat") or "local import is not ready"),
                match_basis="local_acgme_import_status",
                confidence="source_not_ready",
            )
            status["identity_map"] = _workforce_identity_map(
                query={"program_id": program_id},
                payload=status,
                dataset_id="acgme_program_search_public_export",
                program_id=program_id,
            )
            return to_structured(_attach_workforce_source_metadata(status))
        result = workforce_data.get_acgme_program(program_id)
        if result is None:
            payload = {
                "status": "exact_program_not_found",
                "program_id": program_id,
                "source_status": status,
                "next_step": "Verify the 10-digit ACGME Program Code against the imported public export.",
                "evidence": _workforce_evidence(
                    query={"program_id": program_id},
                    dataset_id="acgme_program_search_public_export",
                    source_name="ACGME Program Search public export",
                    source_url="https://apps.acgme.org/ads/Public/Programs/Search",
                    source_period=str(status.get("source_period") or "current imported ACGME public export"),
                    cache_status=str(status.get("status") or ""),
                    cache_freshness=str(status.get("source_caveat") or "local import ready"),
                    match_basis="program_id_exact_no_match",
                    confidence="no_exact_program_match",
                ),
            }
            payload["identity_map"] = _workforce_identity_map(
                query={"program_id": program_id},
                payload=payload,
                dataset_id="acgme_program_search_public_export",
                program_id=program_id,
            )
            return to_structured(_attach_workforce_source_metadata(payload))
        payload = {
            "status": "ready",
            "source_status": status,
            "program": _attach_workforce_row_evidence(
                [dict(result)],
                query={"program_id": program_id},
                dataset_id="acgme_program_search_public_export",
                row_kind="acgme_program",
                match_basis="acgme_program_id_exact_row",
                source_name="ACGME Program Search public export",
                source_url="https://apps.acgme.org/ads/Public/Programs/Search",
                source_period=str(status.get("source_period") or "current imported ACGME public export"),
                cache_status=str(status.get("status") or ""),
                cache_freshness=str(status.get("source_caveat") or "local import ready"),
                confidence="high_for_imported_public_export_row",
            )[0],
            "evidence": _workforce_evidence(
                query={"program_id": program_id},
                dataset_id="acgme_program_search_public_export",
                source_name="ACGME Program Search public export",
                source_url="https://apps.acgme.org/ads/Public/Programs/Search",
                source_period=str(status.get("source_period") or "current imported ACGME public export"),
                cache_status=str(status.get("status") or ""),
                cache_freshness=str(status.get("source_caveat") or "local import ready"),
                match_basis="program_id_exact",
                confidence="high_for_imported_public_export_row",
            ),
        }
        payload["identity_map"] = _workforce_identity_map(
            query={"program_id": program_id},
            payload=payload,
            dataset_id="acgme_program_search_public_export",
            program_id=program_id,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except ValueError as e:
        return error_response(str(e), code="invalid_params")
    except Exception as e:
        logger.exception("get_acgme_program failed")
        return error_response(f"get_acgme_program failed: {e}")


@mcp.tool(structured_output=True)
async def search_acgme_programs(
    institution: str = "", specialty: str = "", state: str = "",
) -> dict[str, Any]:
    """Search imported ACGME program inventory with explicit match-basis fields."""
    try:
        status = workforce_data.get_acgme_source_status()
        if status["status"] != "ready":
            status["evidence"] = _workforce_evidence(
                query={"institution": institution, "specialty": specialty, "state": state},
                dataset_id="acgme_program_search_public_export",
                source_name="ACGME Program Search public export",
                source_url="https://apps.acgme.org/ads/Public/Programs/Search",
                source_period=str(status.get("source_period") or "current imported ACGME public export status"),
                cache_status=str(status.get("status") or ""),
                cache_freshness=str(status.get("source_caveat") or "local import is not ready"),
                match_basis="local_acgme_import_status",
                confidence="source_not_ready",
            )
            return to_structured(_attach_workforce_source_metadata(status))
        programs = workforce_data.query_acgme_programs(institution, specialty, state)
        programs = _attach_workforce_row_evidence(
            [dict(program) for program in programs],
            query={"institution": institution, "specialty": specialty, "state": state},
            dataset_id="acgme_program_search_public_export",
            row_kind="acgme_program",
            match_basis="acgme_program_search_result_row",
            source_name="ACGME Program Search public export",
            source_url="https://apps.acgme.org/ads/Public/Programs/Search",
            source_period=str(status.get("source_period") or "current imported ACGME public export"),
            cache_status=str(status.get("status") or ""),
            cache_freshness=str(status.get("source_caveat") or "local import ready"),
            confidence="candidate_program_match",
        )
        payload = {
            "status": "ready",
            "source_status": status,
            "total_programs": len(programs),
            "programs": programs,
            "evidence": _workforce_evidence(
                query={"institution": institution, "specialty": specialty, "state": state},
                dataset_id="acgme_program_search_public_export",
                source_name="ACGME Program Search public export",
                source_url="https://apps.acgme.org/ads/Public/Programs/Search",
                source_period=str(status.get("source_period") or "current imported ACGME public export"),
                cache_status=str(status.get("status") or ""),
                cache_freshness=str(status.get("source_caveat") or "local import ready"),
                match_basis="institution_specialty_state_search",
                confidence="candidate_program_matches",
            ),
        }
        payload["identity_map"] = _workforce_identity_map(
            query={"institution": institution, "specialty": specialty, "state": state},
            payload=payload,
            dataset_id="acgme_program_search_public_export",
            state=state,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("search_acgme_programs failed")
        return error_response(f"search_acgme_programs failed: {e}")


@mcp.tool(structured_output=True)
async def get_residency_programs(
    institution: str = "", specialty: str = "", state: str = "",
) -> dict[str, Any]:
    """Search residency and fellowship programs from ACGME data.

    Uses a static extract of the ACGME Data Resource Book with program-level
    data including specialty, positions, and accreditation status.

    Args:
        institution: Institution name to search (e.g. "Johns Hopkins").
        specialty: Specialty filter (e.g. "Internal Medicine", "Surgery").
        state: Two-letter state code.
    """
    try:
        programs = workforce_data.query_acgme_programs(institution, specialty, state)

        if programs and "error" in programs[0]:
            return to_structured(programs[0])

        response = ResidencyProgramsResponse(
            total_programs=len(programs),
            programs=[ResidencyProgram(**p) for p in programs],
        )
        payload = response.model_dump()
        _attach_workforce_row_evidence(
            payload["programs"],
            query={"institution": institution, "specialty": specialty, "state": state},
            dataset_id="acgme_program_search_public_export",
            row_kind="acgme_program",
            match_basis="acgme_program_search_result_row",
            source_name="ACGME Program Search public export",
            source_url="https://apps.acgme.org/ads/Public/Programs/Search",
            cache_status="configured_acgme_cache",
            cache_freshness="managed by ACGME public export import process",
            confidence="candidate_program_match",
        )
        payload["evidence"] = _workforce_evidence(
            query={"institution": institution, "specialty": specialty, "state": state},
            dataset_id="acgme_program_search_public_export",
            source_name="ACGME Program Search public export",
            source_url="https://apps.acgme.org/ads/Public/Programs/Search",
            cache_status="configured_acgme_cache",
            cache_freshness="managed by ACGME public export import process",
            match_basis="institution_specialty_state_search",
            confidence="candidate_program_matches",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"institution": institution, "specialty": specialty, "state": state},
            payload=payload,
            dataset_id="acgme_program_search_public_export",
            state=state,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_residency_programs failed")
        return error_response(f"get_residency_programs failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: search_union_activity
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_union_activity(
    employer_name: str = "", state: str = "",
    year_start: int = 2015, year_end: int = 2026,
) -> dict[str, Any]:
    """Search NLRB union election records and BLS work stoppages for healthcare employers.

    Uses the labordata/nlrb-data database (daily refreshed from NLRB.gov)
    and BLS work stoppage data for strikes and lockouts.

    Args:
        employer_name: Employer or health system name to search.
        state: Two-letter state code filter.
        year_start: Start year (default 2015).
        year_end: End year (default 2026).
    """
    try:
        await labor_data.ensure_nlrb_cached()
        await labor_data.ensure_stoppages_cached()

        elections = labor_data.search_nlrb_elections(
            employer_name, state, year_start, year_end
        )
        stoppages = labor_data.query_work_stoppages(year_start, year_end)

        response = UnionActivityResponse(
            total_elections=len(elections),
            total_stoppages=len(stoppages),
            elections=[NLRBElection(**e) for e in elections],
            work_stoppages=[WorkStoppage(**s) for s in stoppages if isinstance(s, dict) and "employer" in s],
        )
        payload = response.model_dump()
        _attach_workforce_row_evidence(
            payload["elections"],
            query={"employer_name": employer_name, "state": state, "year_start": year_start, "year_end": year_end},
            dataset_id="nlrb_bls_labor_activity",
            row_kind="nlrb_election",
            match_basis="nlrb_election_source_row",
            source_name="NLRB election public data",
            source_url="https://www.nlrb.gov/reports/nlrb-case-activity-reports/representation-cases/election-reports",
            source_period=f"{year_start}-{year_end}",
            cache_status="configured_labor_cache",
            cache_freshness="managed by NLRB labor cache import/update process",
            confidence="candidate_labor_activity_match",
        )
        _attach_workforce_row_evidence(
            payload["work_stoppages"],
            query={"employer_name": employer_name, "state": state, "year_start": year_start, "year_end": year_end},
            dataset_id="nlrb_bls_labor_activity",
            row_kind="bls_work_stoppage",
            match_basis="bls_work_stoppage_source_row",
            source_name="BLS work stoppage public data",
            source_url="https://www.bls.gov/wsp/",
            source_period=f"{year_start}-{year_end}",
            cache_status="configured_labor_cache",
            cache_freshness="managed by BLS labor cache import/update process",
            confidence="candidate_labor_activity_match",
        )
        payload["evidence"] = _workforce_evidence(
            query={"employer_name": employer_name, "state": state, "year_start": year_start, "year_end": year_end},
            dataset_id="nlrb_bls_labor_activity",
            source_name="NLRB election data and BLS work stoppage public data",
            source_url="https://www.nlrb.gov/reports/nlrb-case-activity-reports/representation-cases/election-reports",
            source_period=f"{year_start}-{year_end}",
            cache_status="configured_labor_cache",
            cache_freshness="managed by NLRB/BLS labor cache import/update process",
            match_basis="employer_state_year_search",
            confidence="candidate_labor_activity_matches",
        )
        payload["identity"] = _workforce_identity(
            facility_name=employer_name,
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"employer_name": employer_name, "state": state, "year_start": year_start, "year_end": year_end},
            payload=payload,
            dataset_id="nlrb_bls_labor_activity",
            state=state,
            employer_name=employer_name,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("search_union_activity failed")
        return error_response(f"search_union_activity failed: {e}")


# ---------------------------------------------------------------------------
# Tool 6: get_staffing_benchmarks
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_staffing_benchmarks(
    ccn: str = "", state: str = "", facility_type: str = "hospital",
) -> dict[str, Any]:
    """Get staffing benchmarks for a hospital or nursing home.

    Uses CMS PBJ (Payroll-Based Journal) for nursing homes and CMS HCRIS
    Worksheet S-3 for hospitals. Computes peer percentile rankings.

    Args:
        ccn: CMS Certification Number for a specific facility.
        state: State code for state-level benchmarks.
        facility_type: "hospital" (uses HCRIS) or "nursing_home" (uses PBJ).
    """
    try:
        if facility_type == "nursing_home":
            records = await workforce_data.query_pbj_staffing(ccn=ccn, state=state)
            if not records:
                return _workforce_error_response(
                    "No PBJ staffing data found",
                    query={"ccn": ccn, "state": state, "facility_type": facility_type},
                    dataset_id="cms_pbj_nursing_staffing",
                    source_name="CMS Payroll-Based Journal Daily Nurse Staffing",
                    source_url="https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing",
                    source_period="latest PBJ public period in configured cache",
                    cache_status="configured_pbj_cache",
                    cache_freshness="managed by PBJ cache import/update process",
                    match_basis="ccn_or_state_pbj_lookup_no_match",
                    confidence="no_matching_pbj_staffing_rows_in_loaded_cache",
                )

            # Average across dates for the facility
            if ccn and len(records) > 1:
                avg_rn = sum(r["rn_hprd"] for r in records) / len(records)
                avg_lpn = sum(r["lpn_hprd"] for r in records) / len(records)
                avg_cna = sum(r["cna_hprd"] for r in records) / len(records)
                avg_total = sum(r["total_nurse_hprd"] for r in records) / len(records)
                response = StaffingBenchmarksResponse(
                    facility_name=records[0]["facility_name"],
                    ccn=ccn,
                    facility_type="nursing_home",
                    rn_hprd=round(avg_rn, 2),
                    lpn_hprd=round(avg_lpn, 2),
                    cna_hprd=round(avg_cna, 2),
                    total_nurse_hprd=round(avg_total, 2),
                    data_source="CMS_PBJ",
                    data_period=records[0].get("date", ""),
                )
            else:
                r = records[0]
                response = StaffingBenchmarksResponse(
                    facility_name=r["facility_name"],
                    ccn=r.get("ccn", ccn),
                    facility_type="nursing_home",
                    rn_hprd=r["rn_hprd"],
                    lpn_hprd=r["lpn_hprd"],
                    cna_hprd=r["cna_hprd"],
                    total_nurse_hprd=r["total_nurse_hprd"],
                    data_source="CMS_PBJ",
                    data_period=r.get("date", ""),
                )
            payload = response.model_dump()
            payload["evidence"] = _workforce_evidence(
                query={"ccn": ccn, "state": state, "facility_type": facility_type},
                dataset_id="cms_pbj_nursing_staffing",
                source_name="CMS Payroll-Based Journal Daily Nurse Staffing",
                source_url="https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing",
                source_period=payload.get("data_period") or "latest PBJ public period in configured cache",
                cache_status="configured_pbj_cache",
                cache_freshness="managed by PBJ cache import/update process",
                match_basis="ccn_or_state_pbj_lookup",
                confidence="high_for_pbj_reported_hours_and_census",
            )
            payload["identity_map"] = _workforce_identity_map(
                query={"ccn": ccn, "state": state, "facility_type": facility_type},
                payload=payload,
                dataset_id="cms_pbj_nursing_staffing",
                ccn=str(payload.get("ccn") or ccn),
                state=state,
                facility_name=str(payload.get("facility_name") or ""),
                facility_type=facility_type,
            )
            return to_structured(_attach_workforce_source_metadata(payload))

        else:  # hospital
            await workforce_data.ensure_hcris_cached()
            result = workforce_data.query_hcris_staffing(ccn)
            if not result:
                return _workforce_error_response(
                    f"No HCRIS staffing data found for CCN: {ccn}",
                    query={"ccn": ccn, "state": state, "facility_type": facility_type},
                    dataset_id="cms_hcris_workforce_staffing",
                    source_name="CMS HCRIS Worksheet S-3",
                    source_period="latest cached HCRIS fiscal period",
                    cache_status="configured_hcris_cache",
                    cache_freshness="managed by HCRIS cache import/update process",
                    match_basis="ccn_exact_hcris_staffing_no_match",
                    confidence="no_matching_hcris_staffing_row_in_loaded_cache",
                )

            response = StaffingBenchmarksResponse(
                facility_name="",
                ccn=ccn,
                facility_type="hospital",
                data_source="CMS_HCRIS",
                total_nurse_hprd=None,
            )
            payload = response.model_dump()
            payload["evidence"] = _workforce_evidence(
                query={"ccn": ccn, "state": state, "facility_type": facility_type},
                dataset_id="cms_hcris_workforce_staffing",
                source_name="CMS HCRIS Worksheet S-3",
                source_period="latest cached HCRIS fiscal period",
                cache_status="configured_hcris_cache",
                cache_freshness="managed by HCRIS cache import/update process",
                match_basis="ccn_exact_hcris_staffing_row",
                confidence="high_for_reported_hcris_fields",
            )
            payload["identity_map"] = _workforce_identity_map(
                query={"ccn": ccn, "state": state, "facility_type": facility_type},
                payload=payload,
                dataset_id="cms_hcris_workforce_staffing",
                ccn=ccn,
                state=state,
                facility_type=facility_type,
            )
            return to_structured(_attach_workforce_source_metadata(payload))

    except Exception as e:
        logger.exception("get_staffing_benchmarks failed")
        return error_response(f"get_staffing_benchmarks failed: {e}")


# ---------------------------------------------------------------------------
# Tool 7: get_cost_report_staffing
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_cost_report_staffing(ccn: str, year: int = 0) -> dict[str, Any]:
    """Get FTE breakdowns by department from CMS Cost Reports (Worksheet S-3).

    Extracts staffing data from the Healthcare Cost Report Information System
    (HCRIS) for a specific hospital.

    Args:
        ccn: 6-digit CMS Certification Number.
        year: Fiscal year (0 for most recent available).
    """
    try:
        await workforce_data.ensure_hcris_cached()

        result = workforce_data.query_hcris_staffing(ccn, year=year)
        if not result:
            return _workforce_error_response(
                f"No cost report staffing data found for CCN: {ccn}",
                query={"ccn": ccn, "year": year},
                dataset_id="cms_hcris_workforce_staffing",
                source_name="CMS HCRIS Worksheet S-3",
                source_period=str(year) if year else "most_recent",
                cache_status="configured_hcris_cache",
                cache_freshness="managed by HCRIS cache import/update process",
                match_basis="ccn_exact_hcris_staffing_no_match",
                confidence="no_matching_hcris_staffing_row_in_loaded_cache",
            )

        response = CostReportStaffingResponse(
            hospital_name="",
            ccn=ccn,
            fiscal_year=str(year) if year else "most_recent",
            departments=[DepartmentStaffing(**d) for d in result.get("departments", [])],
            total_ftes=result.get("total_ftes", 0),
        )
        payload = response.model_dump()
        _attach_workforce_row_evidence(
            payload["departments"],
            query={"ccn": ccn, "year": year},
            dataset_id="cms_hcris_workforce_staffing",
            row_kind="hcris_department_staffing",
            match_basis="hcris_department_staffing_row",
            source_name="CMS HCRIS Worksheet S-3",
            source_url="https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
            source_period=payload["fiscal_year"],
            cache_status="configured_hcris_cache",
            cache_freshness="managed by HCRIS cache import/update process",
            confidence="high_for_reported_hcris_fields",
        )
        payload["evidence"] = _workforce_evidence(
            query={"ccn": ccn, "year": year},
            dataset_id="cms_hcris_workforce_staffing",
            source_name="CMS HCRIS Worksheet S-3",
            source_period=payload["fiscal_year"],
            cache_status="configured_hcris_cache",
            cache_freshness="managed by HCRIS cache import/update process",
            match_basis="ccn_exact_hcris_staffing_row",
            confidence="high_for_reported_hcris_fields",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "year": year},
            payload=payload,
            dataset_id="cms_hcris_workforce_staffing",
            ccn=ccn,
            year=year,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_cost_report_staffing failed")
        return error_response(f"get_cost_report_staffing failed: {e}")


@mcp.tool(structured_output=True)
async def resolve_hospital_beds(
    ccn: str = "",
    state_facility_id: str = "",
    state: str = "",
    year: int = 0,
    target_scope: str = "ccn",
) -> dict[str, Any]:
    """Resolve hospital bed count from POS, HCRIS, and public state sources with provenance."""
    try:
        if not ccn and not state_facility_id:
            return error_response("ccn or state_facility_id is required.", code="invalid_params")
        selected_ccn = ccn or state_facility_id
        ahrq_row = await _ahrq_hospital_row(selected_ccn)
        state_code = state.upper() if state else str(ahrq_row.get("hosp_state", "") or ahrq_row.get("state", "")).upper()
        cost_row = await _cost_report_row(selected_ccn, year) if ccn else None
        pos_row = await _pos_row(ccn) if ccn else None
        pa_rows = operations_data._pa_bed_rows(
            state=state_code,
            ccn=ccn,
            state_facility_id=state_facility_id,
            hospital_name=str(ahrq_row.get("hospital_name", "")),
            year=year,
        )
        payload = resolve_hospital_bed_source(
            ccn=ccn,
            state_facility_id=state_facility_id,
            state=state_code,
            year=year,
            target_scope=target_scope,
            pos_row=pos_row,
            hcris_row=cost_row,
            ahrq_row=ahrq_row,
            pa_rows=pa_rows,
        )
        _attach_bed_source_evidence(
            payload,
            query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year, "target_scope": target_scope},
        )
        payload["evidence"] = _workforce_evidence(
            query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year, "target_scope": target_scope},
            dataset_id="hospital_bed_identity_resolution",
            source_name="CMS POS, HCRIS, AHRQ, and public state bed sources",
            source_period=str(year or "latest available public period"),
            cache_status="mixed_public_cache",
            cache_freshness="depends on configured POS/HCRIS/AHRQ/state cache freshness",
            match_basis="ccn_or_state_facility_bed_source_resolution",
            confidence=str(payload.get("confidence") or "bed_source_ranked_resolution"),
        )
        payload["identity"] = _workforce_identity(
            ccn=ccn,
            facility_name=_facility_name_from_row(ahrq_row),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "state_facility_id": state_facility_id, "state": state, "year": year, "target_scope": target_scope},
            payload=payload,
            dataset_id="hospital_bed_identity_resolution",
            ccn=ccn,
            state_facility_id=state_facility_id,
            state=state_code,
            year=year,
            facility_name=_facility_name_from_row(ahrq_row),
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("resolve_hospital_beds failed")
        return error_response(f"resolve_hospital_beds failed: {e}")


@mcp.tool(structured_output=True)
async def get_hospital_staffing_productivity(ccn: str, year: int = 0) -> dict[str, Any]:
    """Get high-confidence hospital staffing productivity metrics from public HCRIS/PBJ-adjacent sources."""
    try:
        if not ccn:
            return error_response("ccn is required.", code="invalid_params")
        return to_structured(await _productivity_profile(ccn, year=year))
    except Exception as e:
        logger.exception("get_hospital_staffing_productivity failed")
        return error_response(f"get_hospital_staffing_productivity failed: {e}")


@mcp.tool(structured_output=True)
async def compare_hospital_staffing_productivity(state: str, year: int = 0, peer_group: str = "") -> dict[str, Any]:
    """Compare public staffing productivity profiles for hospitals in a state."""
    try:
        df = await ahrq_data.load_ahrq_hospital_linkage()
        if df.empty:
            return error_response("AHRQ hospital linkage data not available")
        state_col = "hosp_state" if "hosp_state" in df.columns else "state"
        ccn_col = "ccn" if "ccn" in df.columns else ""
        if not state_col or not ccn_col:
            return error_response("Cannot identify state/CCN columns in AHRQ hospital linkage data")
        matches = df[df[state_col].astype(str).str.upper() == state.upper()].head(50)
        profiles = [await _productivity_profile(str(row[ccn_col]).zfill(6), year=year) for _, row in matches.iterrows()]
        requested_peer_dimensions = _requested_peer_dimensions(peer_group)
        payload = {
                "state": state.upper(),
                "year": year or 0,
                "peer_group": peer_group,
                "peer_group_logic": {
                    "requested_dimensions": requested_peer_dimensions,
                    "supported_dimensions": ["state", "bed_size", "teaching", "rural_urban"],
                    "note": "Profiles include peer_group_metadata attributes; callers can bucket peers by any available requested dimension.",
                },
                "total_results": len(profiles),
                "profiles": profiles,
                "confidence": "high_for_reported_public_fields",
        }
        payload["evidence"] = _workforce_evidence(
            query={"state": state, "year": year, "peer_group": peer_group},
            dataset_id="cms_hcris_workforce_productivity",
            match_basis="state_filtered_public_profiles",
            confidence="profile_level_confidence",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"state": state, "year": year, "peer_group": peer_group},
            payload=payload,
            dataset_id="cms_hcris_workforce_productivity",
            state=state,
            year=year,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("compare_hospital_staffing_productivity failed")
        return error_response(f"compare_hospital_staffing_productivity failed: {e}")


def _requested_peer_dimensions(peer_group: str) -> list[str]:
    supported = {"state", "bed_size", "teaching", "rural_urban"}
    requested = [
        token.strip().lower().replace("-", "_")
        for token in peer_group.replace(";", ",").split(",")
        if token.strip()
    ]
    return [token for token in requested if token in supported] or ["state"]


@mcp.tool(structured_output=True)
async def get_snf_nursing_hprd(ccn: str = "", state: str = "", quarter: str = "") -> dict[str, Any]:
    """Get SNF nursing hours per resident day from CMS PBJ public data."""
    try:
        records = await workforce_data.query_pbj_staffing(ccn=ccn, state=state)
        if quarter:
            records = [record for record in records if quarter.lower() in str(record.get("date", "")).lower()]
        records = _attach_workforce_row_evidence(
            [dict(record) for record in records],
            query={"ccn": ccn, "state": state, "quarter": quarter},
            dataset_id="cms_pbj_nursing_staffing",
            row_kind="pbj_daily_staffing",
            match_basis="pbj_daily_staffing_source_row",
            source_name="CMS Payroll-Based Journal Daily Nurse Staffing",
            source_url="https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing",
            source_period=quarter or "latest PBJ public period in configured cache",
            cache_status="configured_pbj_cache",
            cache_freshness="managed by PBJ cache import/update process",
            confidence="high_for_pbj_reported_hours_and_census",
        )
        payload = {
            "ccn": ccn,
            "state": state.upper() if state else "",
            "quarter": quarter,
            "source": "CMS Payroll-Based Journal Daily Nurse Staffing",
            "total_results": len(records),
            "records": records[:200],
            "confidence": "high_for_pbj_reported_hours_and_census",
            "evidence": _workforce_evidence(
                query={"ccn": ccn, "state": state, "quarter": quarter},
                dataset_id="cms_pbj_nursing_staffing",
                source_name="CMS Payroll-Based Journal Daily Nurse Staffing",
                source_url="https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing",
                source_period=quarter or "latest PBJ public period in configured cache",
                cache_status="configured_pbj_cache",
                cache_freshness="managed by PBJ cache import/update process",
                match_basis="ccn_state_quarter_pbj_lookup",
                confidence="high_for_pbj_reported_hours_and_census",
            ),
        }
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "state": state, "quarter": quarter},
            payload=payload,
            dataset_id="cms_pbj_nursing_staffing",
            ccn=ccn,
            state=state,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_snf_nursing_hprd failed")
        return error_response(f"get_snf_nursing_hprd failed: {e}")


@mcp.tool(structured_output=True)
async def get_teaching_intensity(ccn: str, year: int = 0) -> dict[str, Any]:
    """Get resident FTE, resident-to-bed ratio, and teaching status from HCRIS."""
    try:
        if not ccn:
            return error_response("ccn is required.", code="invalid_params")
        profile = await _productivity_profile(ccn, year=year)
        payload = {
            "ccn": ccn,
            "year": year or 0,
            "teaching_status": "Teaching" if (profile.get("resident_fte") or 0) > 0 else "Non-Teaching",
            "resident_fte": profile.get("resident_fte"),
            "beds": profile.get("beds"),
            "bed_source": profile.get("bed_source"),
            "resident_to_bed_ratio": profile.get("resident_to_bed_ratio"),
            "source": "CMS HCRIS Worksheet S-2",
            "confidence": "high_for_reported_hcris_fields",
        }
        payload["evidence"] = _workforce_evidence(
            query={"ccn": ccn, "year": year},
            dataset_id="cms_hcris_gme",
            source_name="CMS HCRIS Worksheet S-2",
            source_period=str(year or "latest cached HCRIS fiscal period"),
            cache_status="configured_hcris_cache",
            cache_freshness="managed by HCRIS cache import/update process",
            match_basis="ccn_exact_hcris_teaching_intensity",
            confidence="high_for_reported_hcris_fields",
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "year": year},
            payload=payload,
            dataset_id="cms_hcris_gme",
            ccn=ccn,
            year=year,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_teaching_intensity failed")
        return error_response(f"get_teaching_intensity failed: {e}")


@mcp.tool(structured_output=True)
async def get_public_throughput_profile(ccn: str = "", state_facility_id: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    """Get public hospital throughput metrics where public source fields exist."""
    try:
        if not ccn and not state_facility_id:
            return error_response("ccn or state_facility_id is required.", code="invalid_params")
        payload = await _throughput_profile(ccn=ccn, state_facility_id=state_facility_id, state=state, year=year)
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_public_throughput_profile failed")
        return error_response(f"get_public_throughput_profile failed: {e}")


@mcp.tool(structured_output=True)
async def compare_public_throughput(state: str, year: int = 0) -> dict[str, Any]:
    """Compare public throughput metrics for hospitals in a state."""
    try:
        df = await ahrq_data.load_ahrq_hospital_linkage()
        if df.empty:
            return error_response("AHRQ hospital linkage data not available")
        state_col = "hosp_state" if "hosp_state" in df.columns else "state"
        ccn_col = "ccn" if "ccn" in df.columns else ""
        matches = df[df[state_col].astype(str).str.upper() == state.upper()].head(100)
        profiles = [await _throughput_profile(ccn=str(row[ccn_col]).zfill(6), state=state, year=year) for _, row in matches.iterrows()]
        payload = {
            "state": state.upper(),
            "year": year or 0,
            "total_results": len(profiles),
            "profiles": profiles,
            "evidence": _workforce_evidence(
                query={"state": state, "year": year},
                dataset_id="public_hospital_throughput",
                match_basis="state_filtered_public_throughput_profiles",
                confidence="profile_level_confidence",
            ),
        }
        payload["identity_map"] = _workforce_identity_map(
            query={"state": state, "year": year},
            payload=payload,
            dataset_id="public_hospital_throughput",
            state=state,
            year=year,
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("compare_public_throughput failed")
        return error_response(f"compare_public_throughput failed: {e}")


@mcp.tool(structured_output=True)
async def get_ed_volume_profile(ccn: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    """Return ED visit and admissions-from-ED fields where public sources provide them."""
    try:
        profile = await _throughput_profile(ccn=ccn, state=state, year=year)
        payload = {
            "ccn": ccn,
            "year": year or 0,
            "state": state.upper() if state else profile.get("state", ""),
            "ed_visits": profile.get("ed_visits"),
            "inpatient_admissions_from_ed": profile.get("inpatient_admissions_from_ed"),
            "source": profile.get("source"),
            "confidence": profile.get("confidence"),
            "metric_confidence": {
                key: value
                for key, value in (profile.get("metric_confidence") or {}).items()
                if key in {"ed_visits", "inpatient_admissions_from_ed"}
            },
            "metric_evidence": {
                key: value
                for key, value in (profile.get("metric_evidence") or {}).items()
                if key in {"ed_visits", "inpatient_admissions_from_ed"}
            },
            "source_profile_evidence": profile.get("evidence"),
        }
        payload["evidence"] = _workforce_evidence(
            query={"ccn": ccn, "state": state, "year": year},
            dataset_id="public_hospital_throughput",
            match_basis="ccn_or_state_public_ed_volume_lookup",
            confidence=str(profile.get("confidence") or "metric_level_confidence"),
        )
        payload["identity"] = _workforce_identity(
            ccn=ccn,
            facility_name=str(profile.get("hospital_name", "")),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "state": state, "year": year},
            payload={**profile, **payload},
            dataset_id="public_hospital_throughput",
            ccn=ccn,
            state=state,
            year=year,
            facility_name=str(profile.get("hospital_name", "")),
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_ed_volume_profile failed")
        return error_response(f"get_ed_volume_profile failed: {e}")


@mcp.tool(structured_output=True)
async def get_or_procedure_volume_profile(ccn: str = "", state: str = "", year: int = 0) -> dict[str, Any]:
    """Return OR/procedure volume fields where public state sources provide them."""
    try:
        profile = await _throughput_profile(ccn=ccn, state=state, year=year)
        payload = {
            "ccn": ccn,
            "year": year or 0,
            "state": state.upper() if state else profile.get("state", ""),
            "or_procedure_volumes": profile.get("or_procedure_volumes"),
            "ct_mri_cath_open_heart_volumes": profile.get("ct_mri_cath_open_heart_volumes"),
            "source": profile.get("source"),
            "confidence": profile.get("confidence"),
            "metric_confidence": {
                key: value
                for key, value in (profile.get("metric_confidence") or {}).items()
                if key in {
                    "or_procedure_volumes",
                    "ct_scans",
                    "mri_scans",
                    "cardiac_catheterizations",
                    "open_heart_procedures",
                }
            },
            "metric_evidence": {
                key: value
                for key, value in (profile.get("metric_evidence") or {}).items()
                if key in {
                    "or_procedure_volumes",
                    "ct_scans",
                    "mri_scans",
                    "cardiac_catheterizations",
                    "open_heart_procedures",
                }
            },
            "source_profile_evidence": profile.get("evidence"),
        }
        payload["evidence"] = _workforce_evidence(
            query={"ccn": ccn, "state": state, "year": year},
            dataset_id="public_hospital_throughput",
            match_basis="ccn_or_state_public_or_procedure_lookup",
            confidence=str(profile.get("confidence") or "metric_level_confidence"),
        )
        payload["identity"] = _workforce_identity(
            ccn=ccn,
            facility_name=str(profile.get("hospital_name", "")),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _workforce_identity_map(
            query={"ccn": ccn, "state": state, "year": year},
            payload={**profile, **payload},
            dataset_id="public_hospital_throughput",
            ccn=ccn,
            state=state,
            year=year,
            facility_name=str(profile.get("hospital_name", "")),
        )
        return to_structured(_attach_workforce_source_metadata(payload))
    except Exception as e:
        logger.exception("get_or_procedure_volume_profile failed")
        return error_response(f"get_or_procedure_volume_profile failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
