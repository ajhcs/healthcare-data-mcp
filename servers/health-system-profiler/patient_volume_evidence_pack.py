"""Patient-volume evidence contract for Public Alpha PSA and ELMS inputs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import evidence_receipt, to_structured
from shared.utils.source_backed_result import source_claim

try:
    from .patient_volume_contract import DENOMINATOR_SCOPES, METRIC_KEYS, MISSINGNESS_STATES, ROW_TYPES, SOURCE_HIERARCHY
    from .patient_volume_contract import allowed_scopes as _allowed_scopes
    from .patient_volume_contract import denominator_scope as _denominator_scope
    from .patient_volume_contract import source_rank as _source_rank
except ImportError:
    from patient_volume_contract import DENOMINATOR_SCOPES, METRIC_KEYS, MISSINGNESS_STATES, ROW_TYPES, SOURCE_HIERARCHY
    from patient_volume_contract import allowed_scopes as _allowed_scopes
    from patient_volume_contract import denominator_scope as _denominator_scope
    from patient_volume_contract import source_rank as _source_rank

MCP_SERVER = "health-system-profiler"
MCP_TOOL = "build_patient_volume_evidence_pack"
PROJECT_LANDING_PAGE = "https://github.com/ajhcs/healthcare-data-mcp"


def build_patient_volume_evidence_pack(
    *,
    region_slug: str,
    systems: list[dict[str, Any]] | None = None,
    source_rows: list[dict[str, Any]] | None = None,
    required_system_slugs: list[str] | None = None,
    denominator_scope: str = "medicare_inpatient",
) -> dict[str, Any]:
    """Build read-only patient-volume input rows with receipts."""

    retrieved_at = datetime.now(timezone.utc).isoformat()
    query = {
        "region_slug": region_slug,
        "denominator_scope": _denominator_scope(denominator_scope),
        "required_system_slugs": _clean_slugs(required_system_slugs or []),
    }
    normalized_rows = [
        _candidate(row, query=query, retrieved_at=retrieved_at)
        for row in source_rows or []
        if isinstance(row, dict)
    ]
    coverage = _coverage(normalized_rows, systems or [], query=query, retrieved_at=retrieved_at)
    blockers = _blockers(normalized_rows, coverage, query=query, retrieved_at=retrieved_at)
    status = _pack_status(normalized_rows, blockers)
    evidence = _receipt(
        source_family="patient_volume_evidence_workflow",
        source_name="Healthcare Data MCP patient-volume evidence workflow",
        dataset_id="patient_volume_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_input",
        cache_freshness="Uses caller-supplied public source rows and explicit searched-source findings.",
        query=query,
        match_basis="workflow_input_normalization",
        confidence="source_scoped_candidate_pack",
        caveat="Read-only evidence pack. It does not calculate PSA, ELMS, capture probabilities, HHI, or profile metric values.",
        next_step="Toolkit methodology review must approve denominator scope, confidence, and publication readiness before any PSA/ELMS calculation.",
        retrieved_at=retrieved_at,
    )
    pack = {
        "workflow_id": "patient_volume_evidence_pack",
        "public_alpha_metric_keys": METRIC_KEYS,
        "status": status,
        "query": query,
        "metadata": {
            "mcp_server": MCP_SERVER,
            "mcp_tool": MCP_TOOL,
            "read_only": True,
            "formula_policy": "MCP returns normalized evidence inputs only; Healthcare Toolkit hc-metrics owns PSA, ELMS, capture probability, distance-decay, HHI, approval, and profile writes.",
            "generated_at": retrieved_at,
        },
        "source_hierarchy": SOURCE_HIERARCHY,
        "denominator_scope_policy": {
            "allowed_values": list(DENOMINATOR_SCOPES),
            "default_review_scope": "medicare_inpatient",
            "public_metric_requirement": "One approved denominator scope must be fair for all six launch systems or PSA/ELMS readiness fails.",
        },
        "required_row_types": list(ROW_TYPES),
        "confidence_inputs": {
            "source_rank": "Rank from source_hierarchy.",
            "denominator_scope": "all_payer_inpatient, medicare_inpatient, or modeled_population_utilization.",
            "coverage_tier": "complete_all_six, partial_with_blockers, or not_evaluated.",
            "row_type": "zip_demand, competitor_access_point, distance_friction, or attractiveness_input.",
            "source_period": "Required for supported rows.",
            "bias_notes": "Required for Medicare-only, modeled, or partial-discharge rows.",
        },
        "patient_volume_input_rows": normalized_rows,
        "coverage": coverage,
        "blockers": blockers,
        "missingness_states": list(MISSINGNESS_STATES),
        "suggested_next_calls": _suggested_next_calls(region_slug),
        "evidence": evidence,
        "source_metadata": _source_metadata(evidence, "patient_volume_evidence_workflow"),
        "identity": _identity(region_slug),
    }
    pack["identity_map"] = _identity_map(pack)
    return to_structured(pack)  # type: ignore[return-value]


def _candidate(row: dict[str, Any], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    row_type = _row_type(row.get("row_type"))
    denominator_scope = _denominator_scope(row.get("denominator_scope") or query["denominator_scope"])
    source_family = str(row.get("source_family") or "cms_hospital_service_area_file")
    source_rank = _source_rank(source_family)
    allowed_scopes = _allowed_scopes(source_family)
    source_period = str(row.get("source_period") or row.get("year") or "")
    source_url = str(row.get("source_url") or row.get("url") or "")
    missingness_state = str(row.get("missingness_state") or "")
    missing_reasons = []
    if row_type not in ROW_TYPES:
        missing_reasons.append("row_type")
    if denominator_scope not in DENOMINATOR_SCOPES:
        missing_reasons.append("denominator_scope")
    elif allowed_scopes and denominator_scope not in allowed_scopes:
        missing_reasons.append("denominator_scope_not_allowed_for_source_family")
    if source_rank is None:
        missing_reasons.append("source_family")
    if not source_period:
        missing_reasons.append("source_period")
    if not (source_url or row.get("landing_page")):
        missing_reasons.append("source_url_or_landing_page")
    if row_type == "zip_demand" and _number(row.get("zip_demand")) is None and not missingness_state:
        missing_reasons.append("zip_demand")
    if row_type in {"competitor_access_point", "distance_friction"} and not (row.get("competitor_id") or row.get("ccn")):
        missing_reasons.append("competitor_id")
    status = "supported"
    if missingness_state in MISSINGNESS_STATES:
        status = missingness_state
    elif missing_reasons:
        status = "needs_review"
    value = {
        "region_slug": row.get("region_slug") or query["region_slug"],
        "system_slug": row.get("system_slug") or "",
        "zip_code": _zip(row.get("zip_code") or row.get("zcta")),
        "zcta": _zip(row.get("zcta") or row.get("zip_code")),
        "year": str(row.get("year") or source_period),
        "row_type": row_type,
        "denominator_scope": denominator_scope,
        "zip_demand": _number(row.get("zip_demand")),
        "competitor_id": row.get("competitor_id") or row.get("ccn") or "",
        "competitor_name": row.get("competitor_name") or row.get("facility_name") or "",
        "distance_miles": _number(row.get("distance_miles")),
        "friction_basis": row.get("friction_basis") or "",
        "attractiveness": _number(row.get("attractiveness")),
        "attractiveness_basis": row.get("attractiveness_basis") or "",
        "bias_notes": row.get("bias_notes") or row.get("coverage_notes") or "",
        "source_row_id": row.get("source_row_id") or row.get("id") or "",
        "confidence_inputs": {
            "source_rank": source_rank,
            "allowed_denominator_scopes": allowed_scopes,
            "denominator_scope": denominator_scope,
            "source_period_present": bool(source_period),
            "source_url_present": bool(source_url or row.get("landing_page")),
            "bias_notes_present": bool(row.get("bias_notes") or row.get("coverage_notes")),
        },
        "missing_reasons": missing_reasons,
    }
    receipt = _receipt(
        source_family=source_family,
        source_name=str(row.get("source_name") or "Public patient-volume source"),
        dataset_id=str(row.get("dataset_id") or source_family),
        source_period=source_period or "missing_source_period",
        source_url=source_url,
        landing_page=str(row.get("landing_page") or source_url or PROJECT_LANDING_PAGE),
        cache_status=str(row.get("cache_status") or "caller_supplied_public_row"),
        cache_freshness=str(row.get("cache_freshness") or "Review source retrieval timestamp before citing."),
        query={**query, "source_row_id": value["source_row_id"], "row_type": row_type, "zip_code": value["zip_code"]},
        match_basis=str(row.get("match_basis") or "public_patient_volume_input_row"),
        confidence=str(row.get("confidence") or ("needs_review" if status == "needs_review" else "source_row")),
        caveat=str(row.get("caveat") or "Candidate PSA/ELMS input row; Toolkit must approve denominator scope and confidence before calculation."),
        next_step=str(row.get("next_step") or "Review denominator coverage, row receipts, bias notes, and all-six fairness before PSA/ELMS calculation."),
        retrieved_at=retrieved_at,
    )
    return {
        "field": "patient_volume_input",
        "value": value,
        "status": status,
        "row_type": row_type,
        "source_family": source_family,
        "source_period": source_period,
        "confidence": receipt["confidence"],
        "evidence": receipt,
        "source_metadata": _source_metadata(receipt, source_family),
        "metadata": {"mcp_server": MCP_SERVER, "mcp_tool": MCP_TOOL},
    }


def _coverage(rows: list[dict[str, Any]], systems: list[dict[str, Any]], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    required = query["required_system_slugs"] or _clean_slugs([str(system.get("system_slug") or "") for system in systems])
    supported = [row for row in rows if row.get("status") == "supported"]
    row_types = {str(row.get("row_type") or "") for row in supported}
    covered_systems = _clean_slugs([str(row.get("value", {}).get("system_slug") or "") for row in supported])
    missing_systems = [slug for slug in required if slug not in covered_systems]
    missing_row_types = [row_type for row_type in ROW_TYPES if row_type not in row_types]
    coverage_tier = "complete_all_six" if required and not missing_systems and not missing_row_types else "partial_with_blockers"
    if not rows:
        coverage_tier = "not_evaluated"
    return {
        "required_system_slugs": required,
        "covered_system_slugs": covered_systems,
        "missing_system_slugs": missing_systems,
        "supported_row_types": sorted(row_types),
        "missing_row_types": missing_row_types,
        "coverage_tier": coverage_tier,
        "evidence": _finding_receipt(
            status=coverage_tier,
            detail={"missing_system_slugs": missing_systems, "missing_row_types": missing_row_types},
            query=query,
            match_basis="all_six_patient_volume_coverage_review",
            confidence=coverage_tier,
            retrieved_at=retrieved_at,
        ),
    }


def _blockers(rows: list[dict[str, Any]], coverage: dict[str, Any], *, query: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    blockers = []
    if not rows:
        blockers.append(_finding("not_yet_researched", {"reason": "no_source_rows_supplied"}, query, retrieved_at))
    if coverage.get("missing_system_slugs") or coverage.get("missing_row_types"):
        blockers.append(_finding("unavailable_public", {"coverage": coverage}, query, retrieved_at))
    scopes = {str(row.get("value", {}).get("denominator_scope") or "") for row in rows if row.get("status") == "supported"}
    if len(scopes) > 1:
        blockers.append(_finding("blocked_source_conflict", {"denominator_scopes": sorted(scopes)}, query, retrieved_at))
    return blockers


def _pack_status(rows: list[dict[str, Any]], blockers: list[dict[str, Any]]) -> str:
    if any(blocker.get("status") == "blocked_source_conflict" for blocker in blockers):
        return "blocked_source_conflict"
    if not rows:
        return "not_yet_researched"
    if blockers:
        return "needs_review"
    if any(row.get("status") == "supported" for row in rows):
        return "source_candidates_ready"
    return "needs_review"


def _finding(status: str, detail: dict[str, Any], query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    return {
        "field": "patient_volume_input",
        "status": status,
        "detail": detail,
        "evidence": _finding_receipt(
            status=status,
            detail=detail,
            query=query,
            match_basis=f"patient_volume_{status}",
            confidence=status,
            retrieved_at=retrieved_at,
        ),
    }


def _finding_receipt(*, status: str, detail: dict[str, Any], query: dict[str, Any], match_basis: str, confidence: str, retrieved_at: str) -> dict[str, Any]:
    return _receipt(
        source_family="patient_volume_evidence_workflow",
        source_name="Healthcare Data MCP patient-volume evidence workflow",
        dataset_id="patient_volume_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_input",
        cache_freshness="Uses current workflow inputs.",
        query={**query, **detail},
        match_basis=match_basis,
        confidence=confidence,
        caveat="Patient-volume evidence adequacy finding; Toolkit decides approval and public readiness.",
        next_step="Resolve blockers before calculating PSA or ELMS.",
        retrieved_at=retrieved_at,
    )


def _identity(region_slug: str) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=region_slug.replace("-", " "),
        entity_type="public_alpha_region",
        source_name="patient_volume_evidence_pack_input",
    ).to_dict()
    identity["region_slug"] = region_slug
    return identity


def _identity_map(pack: dict[str, Any]) -> dict[str, Any]:
    row_paths = ["coverage.evidence"]
    if pack["patient_volume_input_rows"]:
        row_paths.append("patient_volume_input_rows[].evidence")
    if pack["blockers"]:
        row_paths.append("blockers[].evidence")
    return {
        "entity_scope": "public_alpha_patient_volume_evidence",
        "metric_keys": METRIC_KEYS,
        "join_keys": [
            {"field": "region_slug", "policy": "exact Toolkit Public Alpha region slug"},
            {"field": "system_slug", "policy": "exact Toolkit launch-system slug"},
            {"field": "zip_code", "policy": "exact five-digit ZIP/ZCTA key with metadata preserving ZCTA basis"},
            {"field": "competitor_id", "policy": "exact CCN, facility ID, or approved access-point ID when available"},
        ],
        "source_claims": [
            source_claim(
                collection="patient_volume_evidence_pack",
                source_name="Healthcare Data MCP patient-volume evidence workflow",
                source_url=PROJECT_LANDING_PAGE,
                evidence_path="evidence",
                source_metadata_path="source_metadata",
                identity_paths=("identity",),
                row_evidence_paths=row_paths,
                match_policy="read_only_normalization_preserve_source_rows_no_psa_elms_formula",
            )
        ],
        "conflict_policy": "Coverage gaps, denominator-scope conflicts, and missing row types must route to Toolkit methodology review; MCP must not calculate PSA or ELMS.",
        "missing_data_policy": "Use explicit missingness states only: not_yet_researched, unavailable_public, not_applicable, or blocked_source_conflict.",
        "protected_write_policy": "Toolkit/profile workflow owns profile_sources, confidence calibration, approval, hc-metrics calculation, and profile_metric_values writes.",
    }


def _receipt(**kwargs: Any) -> dict[str, Any]:
    return evidence_receipt(
        source_name=kwargs["source_name"],
        source_url=kwargs["source_url"],
        dataset_id=kwargs["dataset_id"],
        source_period=kwargs["source_period"],
        landing_page=kwargs["landing_page"],
        retrieved_at=kwargs["retrieved_at"],
        cache_status=kwargs["cache_status"],
        cache_freshness=kwargs["cache_freshness"],
        entity_scope="public_alpha_patient_volume_evidence",
        query={**kwargs["query"], "source_family": kwargs["source_family"]},
        match_basis=kwargs["match_basis"],
        confidence=kwargs["confidence"],
        caveat=kwargs["caveat"],
        next_step=kwargs["next_step"],
    )


def _source_metadata(receipt: dict[str, Any], source_family: str) -> dict[str, Any]:
    metadata = {
        key: receipt.get(key, "")
        for key in (
            "source_name",
            "source_url",
            "dataset_id",
            "source_period",
            "landing_page",
            "retrieved_at",
            "cache_status",
            "cache_freshness",
        )
    }
    metadata["entity_scope"] = receipt.get("entity_scope", "public_alpha_patient_volume_evidence")
    metadata["query"] = receipt.get("query", {})
    metadata["source_family"] = source_family
    metadata["source_type"] = "public_alpha_patient_volume_evidence"
    return metadata


def _suggested_next_calls(region_slug: str) -> list[dict[str, Any]]:
    return [
        {"server": "service-area", "tool": "compute_service_area", "reason": f"Retrieve CMS HSAF Medicare inpatient ZIP-origin rows for {region_slug} facilities."},
        {"server": "geo-demographics", "tool": "get_zcta_demographics_batch", "reason": "Retrieve source-backed ZCTA population context for modeled demand fallback."},
        {"server": "drive-time", "tool": "compute_drive_time_matrix", "reason": "Retrieve reproducible ZIP/access-point distance or friction rows."},
        {"server": "claims-analytics", "tool": "get_market_volumes", "reason": "Retrieve CMS Medicare utilization context where service-line demand is needed."},
    ]


def _row_type(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _number(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _zip(value: Any) -> str:
    clean = str(value or "").strip()
    return clean.zfill(5) if clean else ""


def _clean_slugs(values: list[str]) -> list[str]:
    return sorted({value.strip().lower() for value in values if value and value.strip()})
