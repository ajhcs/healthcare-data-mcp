"""Composite source-input evidence contract for Public Alpha FSI and Scale Score."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import evidence_receipt, to_structured
from shared.utils.source_backed_result import source_claim

MCP_SERVER = "health-system-profiler"
MCP_TOOL = "build_composite_source_input_evidence_pack"
PROJECT_LANDING_PAGE = "https://github.com/ajhcs/healthcare-data-mcp"

FSI_METRIC_KEY = "finance.ushso_financial_strength_index"
SCALE_METRIC_KEY = "system.health_system_scale_score"
METRIC_KEYS = [FSI_METRIC_KEY, SCALE_METRIC_KEY]

FSI_INPUT_FIELDS = [
    "total_operating_revenue_usd",
    "operating_margin_pct",
    "days_cash_on_hand",
    "debt_to_capitalization_pct",
    "cash_to_debt_ratio",
    "net_assets_usd",
    "peer_operating_margin_pct",
    "peer_days_cash_on_hand",
    "peer_debt_to_capitalization_pct",
    "peer_cash_to_debt_ratio",
]

SCALE_INPUT_FIELDS = [
    "operating_revenue_usd",
    "hospital_count",
    "bed_count",
    "annual_discharges",
    "physician_count",
    "service_line_count",
    "safety_net_patient_mix_pct",
    "emergency_department_count",
    "essential_service_designation_count",
]

FIELD_TO_METRIC = {field: FSI_METRIC_KEY for field in FSI_INPUT_FIELDS}
FIELD_TO_METRIC.update({field: SCALE_METRIC_KEY for field in SCALE_INPUT_FIELDS})

MISSINGNESS_STATES = ("not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict")

SOURCE_HIERARCHY = [
    {
        "rank": 1,
        "source_family": "audited_consolidated_financial_statement",
        "retrieval_owner": "financial-intelligence",
        "input_fields": [
            "total_operating_revenue_usd",
            "operating_revenue_usd",
            "operating_margin_pct",
            "days_cash_on_hand",
            "debt_to_capitalization_pct",
            "cash_to_debt_ratio",
            "net_assets_usd",
        ],
        "rule": "Preferred source for system financial fields when consolidation, audit basis, debt, cash, and net asset definitions are preserved.",
    },
    {
        "rank": 2,
        "source_family": "annual_report_or_public_financial_disclosure",
        "retrieval_owner": "financial-intelligence",
        "input_fields": [
            "total_operating_revenue_usd",
            "operating_revenue_usd",
            "operating_margin_pct",
            "days_cash_on_hand",
            "debt_to_capitalization_pct",
            "cash_to_debt_ratio",
            "net_assets_usd",
        ],
        "rule": "Use public annual reports or disclosure packets when audited consolidated statements are unavailable or incomplete.",
    },
    {
        "rank": 3,
        "source_family": "form_990_or_cost_report_crosswalk",
        "retrieval_owner": "financial-intelligence",
        "input_fields": [
            "total_operating_revenue_usd",
            "operating_revenue_usd",
            "operating_margin_pct",
            "days_cash_on_hand",
            "debt_to_capitalization_pct",
            "cash_to_debt_ratio",
            "net_assets_usd",
        ],
        "rule": "Use IRS Form 990, HCRIS, or HFMD-style crosswalks only when consolidated public financial statements cannot supply the field.",
    },
    {
        "rank": 4,
        "source_family": "approved_public_peer_benchmark_packet",
        "retrieval_owner": "financial-intelligence",
        "input_fields": [
            "peer_operating_margin_pct",
            "peer_days_cash_on_hand",
            "peer_debt_to_capitalization_pct",
            "peer_cash_to_debt_ratio",
        ],
        "rule": "Peer rows must come from an approved public benchmark packet or approved public-data calibration file; MCP must not derive peer adjustment formulas.",
    },
    {
        "rank": 5,
        "source_family": "approved_profile_facility_roster",
        "retrieval_owner": "health-system-profiler",
        "input_fields": ["hospital_count", "bed_count", "emergency_department_count"],
        "rule": "Use approved source-scoped facility roster and bed/access-point rows with CCN or source-native facility IDs where available.",
    },
    {
        "rank": 6,
        "source_family": "public_utilization_or_claims_context",
        "retrieval_owner": "claims-analytics",
        "input_fields": ["annual_discharges", "service_line_count"],
        "rule": "Use public utilization, HSAF, Medicare, state, or service-line rows as denominator inputs only; Scale Score calculation remains Toolkit-owned.",
    },
    {
        "rank": 7,
        "source_family": "physician_platform_evidence_pack",
        "retrieval_owner": "health-system-profiler",
        "input_fields": ["physician_count"],
        "rule": "Use the physician-platform evidence pack output after Toolkit review of definition basis and deduplication.",
    },
    {
        "rank": 8,
        "source_family": "public_safety_net_or_community_benefit_source",
        "retrieval_owner": "financial-intelligence",
        "input_fields": ["safety_net_patient_mix_pct"],
        "rule": "Use Medicaid, uninsured, charity-care, uncompensated-care, or approved safety-net denominator rows with denominator definition preserved.",
    },
    {
        "rank": 9,
        "source_family": "public_records_or_web_intelligence_essentiality_source",
        "retrieval_owner": "public-records/web-intelligence",
        "input_fields": ["essential_service_designation_count", "service_line_count", "emergency_department_count"],
        "rule": "Use structured trauma, burn, transplant, children-specialty, teaching, public mission, or other approved essential-service designation evidence.",
    },
]


def build_composite_source_input_evidence_pack(
    *,
    system_slug: str,
    system_name: str,
    state: str = "",
    source_rows: list[dict[str, Any]] | None = None,
    required_metric_keys: list[str] | None = None,
) -> dict[str, Any]:
    """Build read-only FSI and Scale Score source-input rows with receipts."""

    retrieved_at = datetime.now(timezone.utc).isoformat()
    metric_keys = _valid_metric_keys(required_metric_keys or METRIC_KEYS)
    query = {
        "system_slug": system_slug,
        "system_name": system_name,
        "state": state.strip().upper(),
        "required_metric_keys": metric_keys,
    }
    normalized_rows = [_candidate(row, query=query, retrieved_at=retrieved_at) for row in source_rows or [] if isinstance(row, dict)]
    coverage = _coverage(normalized_rows, metric_keys, query=query, retrieved_at=retrieved_at)
    conflicts = _conflicts(normalized_rows, query=query, retrieved_at=retrieved_at)
    blockers = _blockers(normalized_rows, coverage, conflicts, query=query, retrieved_at=retrieved_at)
    status = _pack_status(normalized_rows, blockers)
    evidence = _receipt(
        source_family="composite_source_input_evidence_workflow",
        source_name="Healthcare Data MCP composite source-input evidence workflow",
        dataset_id="composite_source_input_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_input",
        cache_freshness="Uses caller-supplied public source rows and explicit searched-source findings.",
        query=query,
        match_basis="workflow_input_normalization",
        confidence="source_scoped_candidate_pack",
        caveat="Read-only evidence pack. It does not calculate FSI, Scale Score, components, confidence tiers, approvals, or profile metric values.",
        next_step="Healthcare Toolkit must review sources, approve methods, calibrate confidence, run hc-metrics, and write protected profile values.",
        retrieved_at=retrieved_at,
    )
    pack = {
        "workflow_id": "composite_source_input_evidence_pack",
        "public_alpha_metric_keys": metric_keys,
        "status": status,
        "query": query,
        "metadata": {
            "mcp_server": MCP_SERVER,
            "mcp_tool": MCP_TOOL,
            "read_only": True,
            "formula_policy": "MCP returns normalized source-input rows only; Healthcare Toolkit owns FSI and Scale Score formulas, approval, confidence calibration, hc-metrics execution, and profile_metric_values writes.",
            "generated_at": retrieved_at,
        },
        "source_hierarchy": SOURCE_HIERARCHY,
        "required_input_fields": {FSI_METRIC_KEY: FSI_INPUT_FIELDS, SCALE_METRIC_KEY: SCALE_INPUT_FIELDS},
        "identity_join_policy": {
            "system_join": "Prefer exact Toolkit system_slug plus source-native system ID, EIN, obligated-group ID, CCN, or approved facility roster ID when available.",
            "financial_join": "Treat EIN, CIK, issuer, obligated group, facility, and system names as candidate links until consolidation/audit basis is reviewed.",
            "facility_join": "Join facility-derived Scale inputs by exact CCN or source-native facility ID before name/address matching.",
            "peer_join": "Join peer benchmark rows by approved benchmark class, vintage, and source packet ID; do not infer peer classes in MCP.",
        },
        "confidence_inputs": {
            "source_rank": "Rank from source_hierarchy.",
            "retrieval_owner": "Owning MCP surface for the source family.",
            "source_period": "Required for supported rows.",
            "identity_join_strength": "exact_system_id, exact_ein, exact_ccn, approved_roster_match, official_name_match, benchmark_class_match, or candidate_name_match.",
            "definition_basis": "Required where the input depends on debt, cash, bed, physician, utilization, safety-net, service-line, emergency-access, or essentiality definitions.",
            "coverage_tier": "complete_required_fields, partial_with_blockers, or not_evaluated.",
            "row_receipt_complete": "True only when source name, dataset ID, source period, URL or landing page, match basis, and evidence receipt are present.",
        },
        "composite_source_input_rows": normalized_rows,
        "coverage": coverage,
        "conflicts": conflicts,
        "blockers": blockers,
        "missingness_states": list(MISSINGNESS_STATES),
        "suggested_next_calls": _suggested_next_calls(system_name=system_name, state=query["state"]),
        "evidence": evidence,
        "source_metadata": _source_metadata(evidence, "composite_source_input_evidence_workflow"),
        "identity": _identity(system_slug=system_slug, system_name=system_name, state=query["state"]),
    }
    pack["identity_map"] = _identity_map(pack)
    return to_structured(pack)  # type: ignore[return-value]


def _candidate(row: dict[str, Any], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    input_field = _input_field(row.get("input_field") or row.get("field"))
    metric_key = _metric_key(row.get("metric_key") or FIELD_TO_METRIC.get(input_field, ""))
    source_family = str(row.get("source_family") or "")
    source_rank = _source_rank(source_family)
    retrieval_owner = _retrieval_owner(source_family)
    allowed_fields = _allowed_fields(source_family)
    source_period = str(row.get("source_period") or row.get("year") or row.get("period") or "")
    source_url = str(row.get("source_url") or row.get("url") or "")
    source_row_id = str(row.get("source_row_id") or row.get("id") or "")
    definition_basis = str(row.get("definition_basis") or "")
    missingness_state = str(row.get("missingness_state") or "")
    row_value = _value(row.get("value", row.get("input_value")))
    missing_reasons = []
    if metric_key not in METRIC_KEYS:
        missing_reasons.append("metric_key")
    if input_field not in FIELD_TO_METRIC:
        missing_reasons.append("input_field")
    elif metric_key and FIELD_TO_METRIC.get(input_field) != metric_key:
        missing_reasons.append("metric_input_field_mismatch")
    if not source_family or source_rank is None:
        missing_reasons.append("source_family")
    elif allowed_fields and input_field not in allowed_fields:
        missing_reasons.append("input_field_not_allowed_for_source_family")
    if missingness_state and missingness_state not in MISSINGNESS_STATES:
        missing_reasons.append("missingness_state")
    if row_value is None and not missingness_state:
        missing_reasons.append("value")
    if not row.get("source_name"):
        missing_reasons.append("source_name")
    if not row.get("dataset_id"):
        missing_reasons.append("dataset_id")
    if not source_period:
        missing_reasons.append("source_period")
    if not (source_url or row.get("landing_page")):
        missing_reasons.append("source_url_or_landing_page")
    if not source_row_id:
        missing_reasons.append("source_row_id")
    if not row.get("identity_join_strength"):
        missing_reasons.append("identity_join_strength")
    if not definition_basis:
        missing_reasons.append("definition_basis")

    status = "supported"
    if missingness_state in MISSINGNESS_STATES:
        status = missingness_state
    elif missing_reasons:
        status = "needs_review"

    value = {
        "system_slug": row.get("system_slug") or query["system_slug"],
        "system_name": row.get("system_name") or query["system_name"],
        "metric_key": metric_key,
        "input_field": input_field,
        "input_value": row_value,
        "unit": row.get("unit") or "",
        "source_period": source_period,
        "source_row_id": source_row_id,
        "definition_basis": definition_basis,
        "identity_join_strength": row.get("identity_join_strength") or "",
        "identity_join_keys": row.get("identity_join_keys") or {},
        "retrieval_owner": retrieval_owner,
        "confidence_inputs": {
            "source_rank": source_rank,
            "retrieval_owner": retrieval_owner,
            "allowed_input_fields": allowed_fields,
            "source_period_present": bool(source_period),
            "source_url_present": bool(source_url or row.get("landing_page")),
            "identity_join_strength": row.get("identity_join_strength") or "",
            "definition_basis_present": bool(row.get("definition_basis")),
            "row_receipt_complete": not any(reason in missing_reasons for reason in ROW_RECEIPT_REASONS),
        },
        "missing_reasons": missing_reasons,
    }
    receipt = _receipt(
        source_family=source_family,
        source_name=str(row.get("source_name") or "Public composite source-input source"),
        dataset_id=str(row.get("dataset_id") or source_family),
        source_period=source_period or "missing_source_period",
        source_url=source_url,
        landing_page=str(row.get("landing_page") or source_url or PROJECT_LANDING_PAGE),
        cache_status=str(row.get("cache_status") or "caller_supplied_public_row"),
        cache_freshness=str(row.get("cache_freshness") or "Review source retrieval timestamp before citing."),
        query={**query, "metric_key": metric_key, "input_field": input_field, "source_row_id": value["source_row_id"]},
        match_basis=str(row.get("match_basis") or "public_composite_source_input_row"),
        confidence=str(row.get("confidence") or ("needs_review" if status == "needs_review" else "source_row")),
        caveat=str(row.get("caveat") or "Candidate composite source-input row; Toolkit must approve sources, confidence, formulas, and profile writes."),
        next_step=str(row.get("next_step") or "Review source hierarchy, period, identity join, definition basis, and conflicts before hc-metrics calculation."),
        retrieved_at=retrieved_at,
    )
    return {
        "field": input_field,
        "input_field": input_field,
        "metric_key": metric_key,
        "value": value,
        "status": status,
        "source_family": source_family,
        "source_period": source_period,
        "confidence": receipt["confidence"],
        "match_basis": receipt["match_basis"],
        "evidence": receipt,
        "source_metadata": _source_metadata(receipt, source_family),
        "metadata": {"mcp_server": MCP_SERVER, "mcp_tool": MCP_TOOL},
    }


ROW_RECEIPT_REASONS = {
    "source_name",
    "dataset_id",
    "source_period",
    "source_url_or_landing_page",
    "source_row_id",
    "identity_join_strength",
    "definition_basis",
}


def _coverage(rows: list[dict[str, Any]], metric_keys: list[str], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    supported_fields_by_metric = {
        metric_key: sorted(
            {
                str(row.get("field") or "")
                for row in rows
                if row.get("status") == "supported" and row.get("metric_key") == metric_key
            }
        )
        for metric_key in metric_keys
    }
    required = {FSI_METRIC_KEY: FSI_INPUT_FIELDS, SCALE_METRIC_KEY: SCALE_INPUT_FIELDS}
    missing_fields_by_metric = {
        metric_key: [field for field in required[metric_key] if field not in supported_fields_by_metric.get(metric_key, [])]
        for metric_key in metric_keys
    }
    coverage_tier = "complete_required_fields"
    if not rows:
        coverage_tier = "not_evaluated"
    elif any(missing_fields_by_metric.values()):
        coverage_tier = "partial_with_blockers"
    detail = {"supported_fields_by_metric": supported_fields_by_metric, "missing_fields_by_metric": missing_fields_by_metric}
    return {
        **detail,
        "coverage_tier": coverage_tier,
        "evidence": _finding_receipt(
            status=coverage_tier,
            detail=detail,
            query=query,
            match_basis="composite_required_input_field_coverage_review",
            confidence=coverage_tier,
            retrieved_at=retrieved_at,
        ),
    }


def _conflicts(rows: list[dict[str, Any]], *, query: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    conflicts = []
    values_by_field_period: dict[tuple[str, str, str], set[str]] = {}
    for row in rows:
        if row.get("status") != "supported":
            continue
        value = row.get("value", {})
        key = (str(row.get("metric_key") or ""), str(row.get("field") or ""), str(row.get("source_period") or ""))
        values_by_field_period.setdefault(key, set()).add(str(value.get("input_value")))
    for (metric_key, field, source_period), values in sorted(values_by_field_period.items()):
        if len(values) > 1:
            conflicts.append(
                _finding(
                    "blocked_source_conflict",
                    {
                        "metric_key": metric_key,
                        "input_field": field,
                        "source_period": source_period,
                        "candidate_values": sorted(values),
                    },
                    query,
                    retrieved_at,
                )
            )
    return conflicts


def _blockers(
    rows: list[dict[str, Any]],
    coverage: dict[str, Any],
    conflicts: list[dict[str, Any]],
    *,
    query: dict[str, Any],
    retrieved_at: str,
) -> list[dict[str, Any]]:
    blockers = list(conflicts)
    if not rows:
        blockers.append(_finding("not_yet_researched", {"reason": "no_source_rows_supplied"}, query, retrieved_at))
    if coverage.get("coverage_tier") == "partial_with_blockers":
        blockers.append(_finding("unavailable_public", {"coverage": coverage}, query, retrieved_at))
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
        "field": "composite_source_input",
        "status": status,
        "detail": detail,
        "evidence": _finding_receipt(
            status=status,
            detail=detail,
            query=query,
            match_basis=f"composite_source_input_{status}",
            confidence=status,
            retrieved_at=retrieved_at,
        ),
    }


def _finding_receipt(*, status: str, detail: dict[str, Any], query: dict[str, Any], match_basis: str, confidence: str, retrieved_at: str) -> dict[str, Any]:
    return _receipt(
        source_family="composite_source_input_evidence_workflow",
        source_name="Healthcare Data MCP composite source-input evidence workflow",
        dataset_id="composite_source_input_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_input",
        cache_freshness="Uses current workflow inputs.",
        query={**query, **detail},
        match_basis=match_basis,
        confidence=confidence,
        caveat="Composite source-input evidence finding; Toolkit decides approval, confidence, formulas, and public readiness.",
        next_step="Resolve blockers before running FSI or Scale Score calculations.",
        retrieved_at=retrieved_at,
    )


def _identity(*, system_slug: str, system_name: str, state: str) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=system_name or system_slug.replace("-", " "),
        entity_type="health_system",
        source_name="composite_source_input_evidence_pack_input",
    ).to_dict()
    identity["system_slug"] = system_slug
    if state:
        identity["state"] = state
    return identity


def _identity_map(pack: dict[str, Any]) -> dict[str, Any]:
    row_paths = ["coverage.evidence"]
    if pack["composite_source_input_rows"]:
        row_paths.append("composite_source_input_rows[].evidence")
    if pack["blockers"]:
        row_paths.append("blockers[].evidence")
    return {
        "entity_scope": "public_alpha_composite_source_input_evidence",
        "metric_keys": pack["public_alpha_metric_keys"],
        "join_keys": [
            {"field": "system_slug", "policy": "exact Toolkit launch-system slug"},
            {"field": "source_native_system_id", "policy": "source-scoped system, EIN, obligated-group, issuer, or benchmark packet identifier"},
            {"field": "ccn", "policy": "exact facility join for roster, utilization, bed, emergency access, and service-line rows"},
            {"field": "benchmark_class", "policy": "approved peer benchmark class and source vintage only"},
        ],
        "source_claims": [
            source_claim(
                collection="composite_source_input_evidence_pack",
                source_name="Healthcare Data MCP composite source-input evidence workflow",
                source_url=PROJECT_LANDING_PAGE,
                evidence_path="evidence",
                source_metadata_path="source_metadata",
                identity_paths=("identity",),
                row_evidence_paths=row_paths,
                match_policy="read_only_normalization_preserve_source_rows_no_fsi_scale_formula_no_metric_write",
            )
        ],
        "conflict_policy": "Input value conflicts, missing required fields, and definition mismatches must route to Toolkit review; MCP must not calculate FSI or Scale Score.",
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
        entity_scope="public_alpha_composite_source_input_evidence",
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
    metadata["entity_scope"] = receipt.get("entity_scope", "public_alpha_composite_source_input_evidence")
    metadata["query"] = receipt.get("query", {})
    metadata["source_family"] = source_family
    metadata["source_type"] = "public_alpha_composite_source_input_evidence"
    return metadata


def _suggested_next_calls(*, system_name: str, state: str) -> list[dict[str, Any]]:
    return [
        {"server": "financial-intelligence", "tool": "get_public_financial_health_profile", "arguments": {"state": state}, "reason": f"Retrieve public financial fields and safety-net finance context for {system_name}."},
        {"server": "health-system-profiler", "tool": "build_profile_evidence_pack", "arguments": {"system_name": system_name, "state": state}, "reason": "Retrieve roster, bed, affiliation, and emergency access-point evidence."},
        {"server": "health-system-profiler", "tool": "build_physician_platform_evidence_pack", "arguments": {"system_name": system_name, "state": state}, "reason": "Retrieve physician-platform input rows only."},
        {"server": "claims-analytics", "tool": "get_market_volumes", "arguments": {"state": state}, "reason": "Retrieve public utilization and service-line denominator inputs."},
        {"server": "public-records/web-intelligence", "tool": "search_public_records", "arguments": {"query": f"{system_name} trauma burn transplant safety net teaching designation"}, "reason": "Retrieve essential-service designation and public-record evidence."},
    ]


def _valid_metric_keys(values: list[str]) -> list[str]:
    keys = [str(value) for value in values if str(value) in METRIC_KEYS]
    return keys or METRIC_KEYS


def _metric_key(value: Any) -> str:
    return str(value or "").strip()


def _input_field(value: Any) -> str:
    return str(value or "").strip().lower()


def _value(value: Any) -> int | float | str | None:
    if value in ("", None):
        return None
    if isinstance(value, int | float):
        return value
    clean = str(value).replace(",", "").strip()
    try:
        numeric = float(clean)
    except ValueError:
        return str(value)
    return int(numeric) if numeric.is_integer() else numeric


def _source_rank(source_family: str) -> int | None:
    for source in SOURCE_HIERARCHY:
        if source["source_family"] == source_family:
            return int(source["rank"])
    return None


def _retrieval_owner(source_family: str) -> str:
    for source in SOURCE_HIERARCHY:
        if source["source_family"] == source_family:
            return str(source["retrieval_owner"])
    return ""


def _allowed_fields(source_family: str) -> list[str]:
    for source in SOURCE_HIERARCHY:
        if source["source_family"] == source_family:
            return [str(field) for field in source["input_fields"]]
    return []
