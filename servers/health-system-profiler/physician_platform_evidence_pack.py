"""Physician-platform evidence contract for Public Alpha profile population."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import evidence_receipt, to_structured
from shared.utils.source_backed_result import source_claim

MCP_SERVER = "health-system-profiler"
MCP_TOOL = "build_physician_platform_evidence_pack"
PROJECT_LANDING_PAGE = "https://github.com/ajhcs/healthcare-data-mcp"
METRIC_KEY = "system.physician_count"

SOURCE_HIERARCHY = [
    {
        "rank": 1,
        "source_family": "official_system_physician_enterprise_disclosure",
        "definition_basis_allowed": ["employed", "affiliated", "total", "normalized"],
        "rule": "Prefer exact public system physician-enterprise disclosures when count definition and source period are stated or recoverable.",
    },
    {
        "rank": 2,
        "source_family": "ahrq_chsp_compendium",
        "definition_basis_allowed": ["affiliated", "total"],
        "rule": "Use AHRQ/CHSP-style compendium evidence as source-period context; do not treat it as current operating authority without corroboration.",
    },
    {
        "rank": 3,
        "source_family": "public_affiliated_group_roster",
        "definition_basis_allowed": ["employed", "affiliated", "total"],
        "rule": "Use public medical-group, faculty-practice, or find-a-doctor roster evidence only when identity joins are explicit and deduplication policy is recorded.",
    },
    {
        "rank": 4,
        "source_family": "nppes_individual_provider_registry",
        "definition_basis_allowed": ["normalized"],
        "rule": "Use NPPES rows as individual-provider identity evidence or denominator inputs; NPPES rows alone are not a system physician-count metric.",
    },
    {
        "rank": 5,
        "source_family": "cms_provider_enrollment",
        "definition_basis_allowed": ["normalized"],
        "rule": "Use CMS enrollment rows as provider identity/control context, not as a standalone public physician-platform count.",
    },
]

DEFINITION_BASES = ("employed", "affiliated", "total", "normalized")
MISSINGNESS_STATES = ("not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict")


def build_physician_platform_evidence_pack(
    *,
    system_slug: str,
    system_name: str,
    state: str = "",
    source_rows: list[dict[str, Any]] | None = None,
    required_definition_bases: list[str] | None = None,
) -> dict[str, Any]:
    """Build read-only physician-platform evidence candidates with receipts."""

    retrieved_at = datetime.now(timezone.utc).isoformat()
    query = {
        "system_slug": system_slug,
        "system_name": system_name,
        "state": state.strip().upper(),
        "required_definition_bases": _valid_definition_bases(required_definition_bases or []),
    }
    normalized_rows = [
        _candidate(row, query=query, retrieved_at=retrieved_at)
        for row in source_rows or []
        if isinstance(row, dict)
    ]
    conflicts = _conflicts(normalized_rows, query=query, retrieved_at=retrieved_at)
    unavailable = _unavailable_findings(
        normalized_rows,
        query=query,
        retrieved_at=retrieved_at,
        required_definition_bases=query["required_definition_bases"],
    )
    status = "needs_review"
    if any(row.get("status") == "blocked_source_conflict" for row in conflicts):
        status = "blocked_source_conflict"
    elif normalized_rows:
        status = "source_candidates_ready"
    elif unavailable:
        status = str(unavailable[0].get("status") or "unavailable_public")

    evidence = _receipt(
        source_family="physician_platform_evidence_workflow",
        source_name="Healthcare Data MCP physician-platform evidence workflow",
        dataset_id="physician_platform_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_input",
        cache_freshness="Uses caller-supplied public source rows and explicit searched-source findings.",
        query=query,
        match_basis="workflow_input_normalization",
        confidence="source_scoped_candidate_pack",
        caveat="Read-only evidence pack. It does not calculate, approve, or write the Healthcare Toolkit public physician-count metric.",
        next_step="Use Healthcare Toolkit protected profile workflow to review source rows, choose definition basis, calibrate confidence, and write approved values.",
        retrieved_at=retrieved_at,
    )
    pack = {
        "workflow_id": "physician_platform_evidence_pack",
        "public_alpha_metric_key": METRIC_KEY,
        "status": status,
        "query": query,
        "metadata": {
            "mcp_server": MCP_SERVER,
            "mcp_tool": MCP_TOOL,
            "read_only": True,
            "profile_write_policy": "MCP returns normalized evidence rows only; Healthcare Toolkit owns profile_sources, profile_metric_values, confidence calibration, and protected writes.",
            "generated_at": retrieved_at,
        },
        "source_hierarchy": SOURCE_HIERARCHY,
        "definition_basis_policy": {
            "allowed_values": list(DEFINITION_BASES),
            "public_metric_requirement": "Approved value must state employed, affiliated, total, or normalized basis.",
            "comparison_policy": "Incomparable definition bases block public pairwise deltas until methodology approval resolves normalization.",
        },
        "identity_join_policy": {
            "system_join": "Prefer exact health-system slug/name plus source-native system ID when available.",
            "physician_join": "Join individual physicians by exact NPI first; names, specialties, locations, or organization names are candidate context only.",
            "group_join": "Join physician groups by exact public legal/DBA names, NPIs, TIN/EIN only when public, source URL, and source period are preserved.",
            "deduplication_policy": "Caller must preserve source-native row IDs and NPI/group identifiers so Toolkit can review duplicate physicians across rosters.",
        },
        "confidence_inputs": {
            "source_rank": "Rank from source_hierarchy.",
            "definition_basis": "employed, affiliated, total, or normalized.",
            "source_period": "Required for any populated candidate row.",
            "identity_join_strength": "exact_system_id, exact_npi, exact_group_identifier, official_name_match, or candidate_name_match.",
            "deduplication_basis": "npi_exact, source_roster_unique, group_roster_unique, unresolved, or not_applicable.",
            "coverage_notes": "Required when source does not represent the full physician platform.",
        },
        "missingness_states": list(MISSINGNESS_STATES),
        "physician_platform_evidence_rows": normalized_rows,
        "conflicts": conflicts,
        "unavailable_public_findings": unavailable,
        "suggested_next_calls": _suggested_next_calls(system_name=system_name, state=query["state"]),
        "evidence": evidence,
        "source_metadata": _source_metadata(evidence, "physician_platform_evidence_workflow"),
        "identity": _identity(system_slug=system_slug, system_name=system_name, state=query["state"]),
    }
    pack["identity_map"] = _identity_map(pack)
    return to_structured(pack)  # type: ignore[return-value]


def _candidate(row: dict[str, Any], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    definition_basis = _definition_basis(row.get("definition_basis"))
    count_value = _int_or_none(row.get("count_value"))
    source_family = str(row.get("source_family") or "official_system_physician_enterprise_disclosure")
    source_name = str(row.get("source_name") or "Public physician-platform source")
    dataset_id = str(row.get("dataset_id") or source_family)
    source_period = str(row.get("source_period") or row.get("period") or "")
    source_url = str(row.get("source_url") or row.get("url") or "")
    identity_join_strength = str(row.get("identity_join_strength") or "candidate_name_match")
    missingness_state = str(row.get("missingness_state") or "")
    status = "supported"
    missing_reasons = []
    if definition_basis not in DEFINITION_BASES:
        missing_reasons.append("definition_basis")
    if count_value is None and not missingness_state:
        missing_reasons.append("count_value")
    if not source_period:
        missing_reasons.append("source_period")
    if not (source_url or row.get("landing_page")):
        missing_reasons.append("source_url_or_landing_page")
    if missingness_state in MISSINGNESS_STATES:
        status = missingness_state
    elif missing_reasons:
        status = "needs_review"
    value = {
        "system_slug": row.get("system_slug") or query["system_slug"],
        "system_name": row.get("system_name") or query["system_name"],
        "count_value": count_value,
        "definition_basis": definition_basis,
        "source_period": source_period,
        "source_claim_text": row.get("source_claim_text") or row.get("claim_text") or "",
        "source_row_id": row.get("source_row_id") or row.get("id") or "",
        "physician_group_name": row.get("physician_group_name") or row.get("group_name") or "",
        "source_native_system_id": row.get("source_native_system_id") or "",
        "identity_join_strength": identity_join_strength,
        "deduplication_basis": row.get("deduplication_basis") or "unresolved",
        "coverage_notes": row.get("coverage_notes") or "",
        "confidence_inputs": {
            "source_rank": _source_rank(source_family),
            "identity_join_strength": identity_join_strength,
            "definition_basis": definition_basis,
            "deduplication_basis": row.get("deduplication_basis") or "unresolved",
            "source_period_present": bool(source_period),
            "source_url_present": bool(source_url or row.get("landing_page")),
        },
        "missing_reasons": missing_reasons,
    }
    receipt = _receipt(
        source_family=source_family,
        source_name=source_name,
        dataset_id=dataset_id,
        source_period=source_period or "missing_source_period",
        source_url=source_url,
        landing_page=str(row.get("landing_page") or source_url or PROJECT_LANDING_PAGE),
        cache_status=str(row.get("cache_status") or "caller_supplied_public_row"),
        cache_freshness=str(row.get("cache_freshness") or "Review source retrieval timestamp before citing."),
        query={**query, "source_row_id": value["source_row_id"], "definition_basis": definition_basis},
        match_basis=str(row.get("match_basis") or "public_physician_platform_count_claim"),
        confidence=str(row.get("confidence") or ("needs_review" if status == "needs_review" else "source_row")),
        caveat=str(row.get("caveat") or "Candidate physician-platform evidence row; Toolkit must approve metric definition and confidence before public use."),
        next_step=str(row.get("next_step") or "Review definition, source period, identity join, deduplication, and conflicts before any profile_metric_values write."),
        retrieved_at=retrieved_at,
    )
    return {
        "field": METRIC_KEY,
        "value": value,
        "status": status,
        "definition_basis": definition_basis,
        "source_family": source_family,
        "source_period": source_period,
        "confidence": receipt["confidence"],
        "match_basis": receipt["match_basis"],
        "evidence": receipt,
        "source_metadata": _source_metadata(receipt, source_family),
        "metadata": {"mcp_server": MCP_SERVER, "mcp_tool": MCP_TOOL},
    }


def _conflicts(rows: list[dict[str, Any]], *, query: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    supported = [row for row in rows if row.get("status") == "supported"]
    by_basis: dict[str, set[int]] = {}
    for row in supported:
        basis = str(row.get("definition_basis") or "")
        count = row.get("value", {}).get("count_value")
        if isinstance(count, int):
            by_basis.setdefault(basis, set()).add(count)
    for basis, counts in sorted(by_basis.items()):
        if len(counts) > 1:
            conflicts.append(
                _finding(
                    status="blocked_source_conflict",
                    detail={"definition_basis": basis, "candidate_counts": sorted(counts)},
                    query=query,
                    match_basis="same_definition_basis_count_conflict",
                    confidence="needs_review",
                    caveat="Multiple supported public rows use the same physician definition but disagree on count.",
                    next_step="Route to Toolkit profile review; do not select a metric value in MCP.",
                    retrieved_at=retrieved_at,
                )
            )
    bases = {str(row.get("definition_basis") or "") for row in supported if row.get("definition_basis")}
    if len(bases) > 1:
        conflicts.append(
            _finding(
                status="needs_review",
                detail={"definition_bases": sorted(bases)},
                query=query,
                match_basis="mixed_physician_count_definition_bases",
                confidence="definition_basis_conflict",
                caveat="Rows use different physician-platform definitions and cannot be compared without approved normalization.",
                next_step="Toolkit methodology review must choose or normalize a definition basis before public display.",
                retrieved_at=retrieved_at,
            )
        )
    return conflicts


def _unavailable_findings(
    rows: list[dict[str, Any]],
    *,
    query: dict[str, Any],
    retrieved_at: str,
    required_definition_bases: list[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if not rows:
        findings.append(
            _finding(
                status="not_yet_researched",
                detail={"searched_source_families": [], "required_definition_bases": required_definition_bases},
                query=query,
                match_basis="no_physician_platform_source_rows_supplied",
                confidence="not_evaluated",
                caveat="No source rows were supplied to this normalization workflow.",
                next_step="Retrieve official physician-enterprise disclosures, AHRQ/CHSP context, and public group/NPPES evidence before deciding missingness.",
                retrieved_at=retrieved_at,
            )
        )
        return findings
    supported_bases = {str(row.get("definition_basis") or "") for row in rows if row.get("status") == "supported"}
    for basis in required_definition_bases:
        if basis not in supported_bases:
            findings.append(
                _finding(
                    status="unavailable_public",
                    detail={"definition_basis": basis, "searched_rows": len(rows)},
                    query=query,
                    match_basis="required_definition_basis_no_supported_row",
                    confidence="not_available",
                    caveat="Rows were supplied, but none produced a supported candidate for the required definition basis.",
                    next_step="Record explicit missingness or retrieve stronger source rows through Toolkit review.",
                    retrieved_at=retrieved_at,
                )
            )
    return findings


def _finding(
    *,
    status: str,
    detail: dict[str, Any],
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    retrieved_at: str,
) -> dict[str, Any]:
    receipt = _receipt(
        source_family="physician_platform_evidence_workflow",
        source_name="Healthcare Data MCP physician-platform evidence workflow",
        dataset_id="physician_platform_evidence_pack",
        source_period="request_time_public_source_pack",
        source_url=PROJECT_LANDING_PAGE,
        landing_page=PROJECT_LANDING_PAGE,
        cache_status="workflow_input",
        cache_freshness="Uses current workflow inputs.",
        query={**query, **detail},
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
        retrieved_at=retrieved_at,
    )
    return {
        "field": METRIC_KEY,
        "status": status,
        "detail": detail,
        "evidence": receipt,
        "source_metadata": _source_metadata(receipt, "physician_platform_evidence_workflow"),
    }


def _identity(*, system_slug: str, system_name: str, state: str) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=system_name or system_slug.replace("-", " "),
        entity_type="health_system",
        source_name="physician_platform_evidence_pack_input",
    ).to_dict()
    identity["system_slug"] = system_slug
    if state:
        identity["state"] = state
    return identity


def _identity_map(pack: dict[str, Any]) -> dict[str, Any]:
    row_paths = []
    if pack["physician_platform_evidence_rows"]:
        row_paths.append("physician_platform_evidence_rows[].evidence")
    if pack["conflicts"]:
        row_paths.append("conflicts[].evidence")
    if pack["unavailable_public_findings"]:
        row_paths.append("unavailable_public_findings[].evidence")
    return {
        "entity_scope": "public_alpha_physician_platform_evidence",
        "metric_key": METRIC_KEY,
        "join_keys": [
            {"field": "system_slug", "policy": "exact Toolkit system slug when available"},
            {"field": "source_native_system_id", "policy": "source-scoped exact system/group identifier when supplied"},
            {"field": "npi", "policy": "exact individual physician join only; names are labels"},
        ],
        "source_claims": [
            source_claim(
                collection="physician_platform_evidence_pack",
                source_name="Healthcare Data MCP physician-platform evidence workflow",
                source_url=PROJECT_LANDING_PAGE,
                evidence_path="evidence",
                source_metadata_path="source_metadata",
                identity_paths=("identity",),
                row_evidence_paths=row_paths,
                match_policy="read_only_normalization_preserve_source_rows_no_metric_write",
            )
        ],
        "conflict_policy": "Same-basis count conflicts or mixed definition bases must route to Toolkit review; MCP must not select the public value.",
        "missing_data_policy": "Use explicit missingness states only: not_yet_researched, unavailable_public, not_applicable, or blocked_source_conflict.",
        "protected_write_policy": "Toolkit/profile workflow owns profile_sources, confidence calibration, approval, and profile_metric_values writes.",
    }


def _receipt(
    *,
    source_family: str,
    source_name: str,
    dataset_id: str,
    source_period: str,
    source_url: str,
    landing_page: str,
    cache_status: str,
    cache_freshness: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    retrieved_at: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        landing_page=landing_page,
        retrieved_at=retrieved_at,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope="public_alpha_physician_platform_evidence",
        query={**query, "source_family": source_family},
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _source_metadata(receipt: dict[str, Any], source_family: str) -> dict[str, Any]:
    return {
        "source_name": receipt.get("source_name", ""),
        "source_url": receipt.get("source_url", ""),
        "dataset_id": receipt.get("dataset_id", ""),
        "source_period": receipt.get("source_period", ""),
        "landing_page": receipt.get("landing_page", ""),
        "retrieved_at": receipt.get("retrieved_at", ""),
        "cache_status": receipt.get("cache_status", ""),
        "cache_freshness": receipt.get("cache_freshness", ""),
        "entity_scope": receipt.get("entity_scope", "public_alpha_physician_platform_evidence"),
        "query": receipt.get("query", {}),
        "source_family": source_family,
        "source_type": "public_alpha_physician_platform_evidence",
    }


def _suggested_next_calls(*, system_name: str, state: str) -> list[dict[str, Any]]:
    return [
        {
            "server": "web-intelligence",
            "tool": "search_web",
            "arguments": {"query": f"{system_name} physicians employed affiliated medical group annual report"},
            "reason": "Find official public physician-enterprise disclosures and annual-report count claims.",
        },
        {
            "server": "physician-referral-network",
            "tool": "search_physicians",
            "arguments": {"query": system_name, "state": state, "limit": 25},
            "reason": "Collect NPPES candidate rows for identity/deduplication context only.",
        },
        {
            "server": "provider-enrollment",
            "tool": "search_provider_enrollment",
            "arguments": {"provider_name": system_name, "state": state, "provider_type": "physician", "limit": 25},
            "reason": "Collect CMS enrollment identity/control context when public rows exist.",
        },
    ]


def _valid_definition_bases(values: list[str]) -> list[str]:
    return [basis for basis in (_definition_basis(value) for value in values) if basis in DEFINITION_BASES]


def _definition_basis(value: Any) -> str:
    basis = str(value or "").strip().lower().replace("-", "_")
    if basis in {"employee", "employed_physicians"}:
        return "employed"
    if basis in {"affiliate", "aligned", "associated"}:
        return "affiliated"
    if basis in {"all", "overall"}:
        return "total"
    if basis in {"modelled", "modeled", "normalized_count"}:
        return "normalized"
    return basis


def _source_rank(source_family: str) -> int | None:
    for source in SOURCE_HIERARCHY:
        if source["source_family"] == source_family:
            return int(source["rank"])
    return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None
