"""Payer type-of-coverage evidence candidates for Public Alpha profiles."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from shared.utils.mcp_response import evidence_receipt, to_structured

MCP_SERVER = "health-system-profiler"
MCP_TOOL = "build_payer_discovery_evidence_pack"
PROJECT_LANDING_PAGE = "https://github.com/ajhcs/healthcare-data-mcp"
METRIC_KEY = "system.payer_coverage_mix"
PAYER_TYPES = ("medicare_advantage", "medicare_part_d", "marketplace")
MISSINGNESS_STATES = ("not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict")

SOURCE_HIERARCHY = [
    {"rank": 1, "source_family": "cms_ma_state_county_enrollment", "payer_types": ["medicare_advantage"], "denominator": "CMS Medicare Advantage enrollment by state/county/plan."},
    {"rank": 2, "source_family": "cms_part_d_state_county_enrollment", "payer_types": ["medicare_part_d"], "denominator": "CMS Part D enrollment by state/county/plan."},
    {"rank": 3, "source_family": "cms_marketplace_effectuated_enrollment", "payer_types": ["marketplace"], "denominator": "CMS Marketplace effectuated enrollment by plan/geography."},
]
_SOURCE_BY_FAMILY = {str(row["source_family"]): row for row in SOURCE_HIERARCHY}
REJECTED_DENOMINATOR_FAMILIES = ("census_population", "acs_population", "census_insurance", "modeled_population")


def build_payer_discovery_evidence_pack(*, system_slug: str, system_name: str, state: str = "", source_rows: list[dict[str, Any]] | None = None, required_payer_types: list[str] | None = None) -> dict[str, Any]:
    """Normalize payer TOC/reference candidates; never calculate payer mix."""
    retrieved_at = datetime.now(timezone.utc).isoformat()
    required = _valid_types(required_payer_types or [])
    query = {"system_slug": system_slug, "system_name": system_name, "state": state.strip().upper(), "required_payer_types": required}
    rows = [_candidate(row, query=query, retrieved_at=retrieved_at) for row in source_rows or [] if isinstance(row, dict)]
    coverage = _coverage(rows, required, query=query, retrieved_at=retrieved_at)
    blockers = _blockers(rows, coverage, query=query, retrieved_at=retrieved_at)
    status = _pack_status(rows, blockers)
    evidence = _receipt("payer_discovery_evidence_workflow", "payer_discovery_evidence_pack", query, "workflow_input_normalization", retrieved_at, "Read-only payer TOC/reference candidates; no payer mix or profile write is performed.", "Use protected Toolkit profile review before writing profile_metric_values.")
    pack = {
        "workflow_id": "payer_discovery_evidence_pack", "public_alpha_metric_key": METRIC_KEY, "status": status, "query": query,
        "metadata": {"mcp_server": MCP_SERVER, "mcp_tool": MCP_TOOL, "read_only": True, "generated_at": retrieved_at, "profile_write_policy": "Toolkit owns payer metric approval and profile writes."},
        "source_hierarchy": SOURCE_HIERARCHY,
        "type_of_coverage_policy": {"allowed_values": list(PAYER_TYPES), "coverage_basis": "TOC candidates must retain payer type, geography, plan/contract identity, period, numerator, and CMS denominator scope."},
        "denominator_policy": {"official_families": [row["source_family"] for row in SOURCE_HIERARCHY], "rejected_families": list(REJECTED_DENOMINATOR_FAMILIES), "rule": "Census population or ACS insurance counts are contextual geography evidence, never a payer enrollment denominator."},
        "missingness_states": list(MISSINGNESS_STATES), "payer_coverage_evidence_rows": rows, "coverage": coverage, "blockers": blockers,
        "unresolved_identifiers": sorted({str(x) for row in rows for x in row["value"].get("unresolved_identifiers", []) if x}), "evidence": evidence,
    }
    return to_structured(pack)  # type: ignore[return-value]


def _valid_types(values: list[str]) -> list[str]:
    return sorted({str(value).strip().lower().replace("-", "_") for value in values if str(value).strip()}.intersection(PAYER_TYPES))


def _candidate(row: dict[str, Any], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    payer_type = str(row.get("payer_type") or row.get("type_of_coverage") or "").strip().lower().replace("-", "_")
    family = str(row.get("source_family") or "")
    source = _SOURCE_BY_FAMILY.get(family)
    missingness = str(row.get("missingness_state") or "")
    reasons: list[str] = []
    if payer_type not in PAYER_TYPES: reasons.append("payer_type")
    if source is None: reasons.append("source_family")
    elif payer_type not in source["payer_types"]: reasons.append("payer_type_not_allowed_for_source_family")
    if family in REJECTED_DENOMINATOR_FAMILIES or "census" in family: reasons.append("census_not_payer_denominator")
    period = str(row.get("source_period") or row.get("period") or row.get("year") or "")
    url = str(row.get("source_url") or row.get("url") or row.get("landing_page") or "")
    if not period: reasons.append("source_period")
    if not url: reasons.append("source_url_or_landing_page")
    if row.get("denominator_value") is None and not missingness: reasons.append("denominator_value")
    status = missingness if missingness in MISSINGNESS_STATES else ("needs_review" if reasons else "supported")
    unresolved = list(row.get("unresolved_identifiers") or []) if isinstance(row.get("unresolved_identifiers"), list) else []
    value = {"system_slug": row.get("system_slug") or query["system_slug"], "system_name": row.get("system_name") or query["system_name"], "payer_type": payer_type, "type_of_coverage": payer_type, "state": str(row.get("state") or query["state"]), "county_fips": row.get("county_fips") or "", "plan_id": row.get("plan_id") or row.get("contract_id") or "", "numerator_value": row.get("numerator_value"), "denominator_value": row.get("denominator_value"), "denominator_scope": row.get("denominator_scope") or (source["denominator"] if source else ""), "source_period": period, "source_row_id": row.get("source_row_id") or row.get("id") or "", "coverage_notes": row.get("coverage_notes") or "", "unresolved_identifiers": unresolved, "missing_reasons": reasons}
    receipt = _receipt(family or "payer_discovery_evidence_workflow", str(row.get("source_name") or "Public payer source"), {**query, "source_row_id": value["source_row_id"]}, str(row.get("match_basis") or "official_payer_enrollment_candidate"), retrieved_at, "Candidate row; Toolkit must verify geography, period, plan identity, and denominator before publication.", "Route to protected profile review; do not infer payer mix from census or population counts.", source_period=period or "missing_source_period", source_url=url or PROJECT_LANDING_PAGE, dataset_id=str(row.get("dataset_id") or family or "payer_discovery_evidence_pack"))
    return {"field": METRIC_KEY, "value": value, "status": status, "source_family": family, "payer_type": payer_type, "source_period": period, "evidence": receipt}


def _coverage(rows: list[dict[str, Any]], required: list[str], *, query: dict[str, Any], retrieved_at: str) -> dict[str, Any]:
    supported = {str(row.get("payer_type")) for row in rows if row.get("status") == "supported"}
    missing = [payer for payer in required if payer not in supported]
    return {"required_payer_types": required, "supported_payer_types": sorted(supported), "missing_payer_types": missing, "coverage_state": "complete" if required and not missing else ("unresolved" if rows else "not_evaluated"), "evidence": _receipt("payer_discovery_evidence_workflow", "payer_discovery_evidence_pack", query, "payer_type_coverage_review", retrieved_at, "Coverage is complete only for explicitly supported official payer types.", "Resolve missing payer types through official CMS sources.")}


def _blockers(rows: list[dict[str, Any]], coverage: dict[str, Any], *, query: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if not rows: blockers.append({"status": "not_yet_researched", "detail": "No payer source rows supplied."})
    if coverage["missing_payer_types"]: blockers.append({"status": "unavailable_public", "detail": {"missing_payer_types": coverage["missing_payer_types"]}})
    if any("census_not_payer_denominator" in row["value"]["missing_reasons"] for row in rows): blockers.append({"status": "blocked_source_conflict", "detail": "Census/ACS population rows cannot establish payer enrollment denominators."})
    return blockers


def _pack_status(rows: list[dict[str, Any]], blockers: list[dict[str, Any]]) -> str:
    if any(item["status"] == "blocked_source_conflict" for item in blockers): return "blocked_source_conflict"
    if any(row.get("status") == "supported" for row in rows) and not any(item["status"] == "unavailable_public" for item in blockers): return "source_candidates_ready"
    return blockers[0]["status"] if blockers else "needs_review"


def _receipt(family: str, dataset: str, query: dict[str, Any], match: str, retrieved_at: str, caveat: str, next_step: str, *, source_period: str = "request_time_public_source_pack", source_url: str = PROJECT_LANDING_PAGE, dataset_id: str | None = None) -> dict[str, Any]:
    return evidence_receipt(source_name=dataset, dataset_id=dataset_id or dataset, source_period=source_period, source_url=source_url, landing_page=source_url, cache_status="workflow_input", cache_freshness="Caller-supplied rows require source retrieval review.", query=query, match_basis=match, confidence="source_scoped_candidate_pack", caveat=caveat, next_step=next_step, retrieved_at=retrieved_at)
