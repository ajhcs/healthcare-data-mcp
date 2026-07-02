"""Live-gateway policy metadata and provenance evaluation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from shared.utils.mcp_response import (
    evidence_receipt_validation_summary,
    to_structured,
)
from shared.utils.server_registry import SERVER_BY_ID
from shared.utils.source_backed_result import validate_source_claim_paths


@dataclass(frozen=True)
class LiveToolSpec:
    """One approved live tool exposed through the live gateway."""

    server: str
    module: str
    tool_name: str
    category: str
    scopes: tuple[str, ...] = ("mcp:read",)
    request_size_limit_bytes: int = 32_768
    result_size_limit_bytes: int = 262_144
    result_limit: int = 100
    rate_limit_class: str = "standard"
    source_caveat_class: str = "public_source"
    require_provenance: bool = True


SOURCE_CAVEAT_CLASSES: dict[str, str] = {
    "provider_enrollment_public_record": (
        "CMS provider enrollment, ownership, and CHOW rows are public records; "
        "name searches are candidate matches unless an exact identifier supports the join."
    ),
    "cms_quality_summary": (
        "CMS quality summary rows are public source context; exact named measure claims should preserve "
        "the row-level CMS measure receipt."
    ),
    "claims_public_aggregate": (
        "Claims analytics exposed through live-gateway must remain public aggregate analysis and must not be treated as PHI."
    ),
    "exclusion_screening": (
        "LEIE/SAM results are screening sources; final exclusion or eligibility decisions require source-system verification."
    ),
    "public_breach_or_state_record": (
        "Public breach/cyber records are partial disclosure sources and do not prove absence of incidents or cybersecurity attestation."
    ),
    "public_financial_record": (
        "Public financial records depend on filing/source freshness and may not represent current operating performance."
    ),
    "public_workforce_operations": (
        "Workforce and throughput outputs use public aggregate sources or configured caches; validate period and denominator before reporting."
    ),
    "public_community_health": (
        "Community health outputs are public population estimates and should not be interpreted as patient-level facts."
    ),
    "public_research_trials": (
        "Research and trials outputs reflect public registry/API records; sponsor and organization aliases require review before aggregation."
    ),
    "public_source": "Public-source result; preserve the owning tool's source caveats and evidence receipts.",
}

_CATEGORY_CAVEAT_CLASS = {
    "provider_enrollment": "provider_enrollment_public_record",
    "hospital_quality": "cms_quality_summary",
    "claims_analytics": "claims_public_aggregate",
    "exclusions": "exclusion_screening",
    "public_records": "public_breach_or_state_record",
    "financial_intelligence": "public_financial_record",
    "operations": "public_workforce_operations",
    "workforce": "public_workforce_operations",
    "community_health": "public_community_health",
    "research_trials": "public_research_trials",
}


def effective_source_caveat_class(spec: LiveToolSpec) -> str:
    return _CATEGORY_CAVEAT_CLASS.get(spec.category, spec.source_caveat_class)


def source_caveat(spec: LiveToolSpec) -> str:
    return SOURCE_CAVEAT_CLASSES[effective_source_caveat_class(spec)]


def attach_gateway_policy(spec: LiveToolSpec, result: Any, *, provenance_status: Mapping[str, Any] | None = None) -> Any:
    registry_spec = SERVER_BY_ID[spec.server]
    policy = {
        "gateway": "live-gateway",
        "tool": spec.tool_name,
        "server": spec.server,
        "dataset_ids": list(registry_spec.dataset_ids),
        "cache_needs": list(registry_spec.cache_needs),
        "server_safety_notes": list(registry_spec.safety_notes),
        "allowed_scopes": list(spec.scopes),
        "request_size_limit_bytes": spec.request_size_limit_bytes,
        "result_size_limit_bytes": spec.result_size_limit_bytes,
        "result_limit": spec.result_limit,
        "rate_limit_class": spec.rate_limit_class,
        "source_caveat_class": effective_source_caveat_class(spec),
        "source_caveat": source_caveat(spec),
        "audit_event": "tool_call",
        "requires_provenance": spec.require_provenance,
        "provenance_status": dict(provenance_status or evaluate_provenance_status(result)),
    }
    if isinstance(result, dict):
        response = dict(result)
        response["live_gateway_policy"] = policy
        return response
    return {"result": result, "live_gateway_policy": policy}


def evaluate_provenance_status(result: Any) -> dict[str, Any]:
    payload = to_structured(result)
    if not isinstance(payload, Mapping):
        return {
            "status": "non_object_result",
            "evidence_present": False,
            "evidence_valid": False,
            "source_metadata_present": False,
            "identity_present": False,
            "source_claim_paths_status": "not_evaluated",
            "source_claim_paths_valid": False,
        }

    evidence_summary = evidence_receipt_validation_summary(payload, require_content=True)
    source_claim_summary = validate_source_claim_paths(payload, require_boundary_traceability=True)
    source_claim_paths_valid = bool(source_claim_summary.get("valid"))
    result_status = {
        "status": evidence_summary["status"],
        "evidence_present": evidence_summary["evidence_present"],
        "evidence_valid": evidence_summary["evidence_valid"],
        "source_metadata_present": bool(_nested_values_for_keys(payload, {"source_metadata"})),
        "identity_present": bool(_nested_values_for_keys(payload, {"identity", "identities", "entity", "entities"})),
        "source_claim_paths_status": source_claim_summary["status"],
        "source_claim_paths_valid": source_claim_paths_valid,
    }
    if evidence_summary.get("invalid_evidence_paths"):
        result_status["invalid_evidence_paths"] = evidence_summary["invalid_evidence_paths"]
    if not source_claim_paths_valid:
        result_status["source_claim_path_issues"] = source_claim_summary.get("issues", [])
        if evidence_summary["status"] == "evidence_receipt_valid":
            result_status["status"] = "source_claim_paths_invalid"
    return result_status


def audit_provenance_fields(provenance_status: Mapping[str, Any]) -> dict[str, Any]:
    fields = {
        "provenance_status": provenance_status.get("status"),
        "evidence_present": bool(provenance_status.get("evidence_present")),
        "source_metadata_present": bool(provenance_status.get("source_metadata_present")),
        "identity_present": bool(provenance_status.get("identity_present")),
    }
    if provenance_status.get("invalid_evidence_paths"):
        fields["invalid_evidence_paths"] = provenance_status["invalid_evidence_paths"]
    if provenance_status.get("source_claim_paths_status"):
        fields["source_claim_paths_status"] = provenance_status["source_claim_paths_status"]
        fields["source_claim_paths_valid"] = bool(provenance_status.get("source_claim_paths_valid"))
    if provenance_status.get("source_claim_path_issues"):
        fields["source_claim_path_issues"] = provenance_status["source_claim_path_issues"]
    return fields


def _nested_values_for_keys(value: Any, keys: set[str], *, path: str = "result") -> list[tuple[str, Any]]:
    matches: list[tuple[str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if str(key) in keys:
                matches.append((child_path, child))
            matches.extend(_nested_values_for_keys(child, keys, path=child_path))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, child in enumerate(value):
            matches.extend(_nested_values_for_keys(child, keys, path=f"{path}[{index}]"))
    return matches


__all__ = [
    "LiveToolSpec",
    "SOURCE_CAVEAT_CLASSES",
    "attach_gateway_policy",
    "audit_provenance_fields",
    "effective_source_caveat_class",
    "evaluate_provenance_status",
    "source_caveat",
]
