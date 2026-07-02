"""Policy-runner contract tests for live-gateway provenance handling."""

from __future__ import annotations

from servers.live_gateway.policy_runner import (
    LiveToolSpec,
    attach_gateway_policy,
    audit_provenance_fields,
    build_audit_evidence_export,
    evaluate_provenance_status,
)
from shared.utils.mcp_response import evidence_receipt
from shared.utils.source_backed_result import source_claim


def _boundary_ready_payload() -> dict:
    return {
        "results": [{"npi": "1234567893", "evidence": _evidence(match_basis="npi_exact_row")}],
        "source_metadata": {
            "source_name": "CMS Provider Enrollment",
            "source_url": "https://data.cms.gov/provider-enrollment",
            "dataset_id": "cms-provider-enrollment",
        },
        "evidence": _evidence(match_basis="npi_exact"),
        "identity": {"entity_type": "provider", "npi": "1234567893"},
        "identity_map": {
            "source_claims": [
                source_claim(
                    collection="enrollments",
                    source_name="CMS Provider Enrollment",
                    source_url="https://data.cms.gov/provider-enrollment",
                    evidence_path="evidence",
                    source_metadata_path="source_metadata",
                    identity_paths=["evidence.query"],
                    row_evidence_paths=["results[].evidence"],
                    match_policy="npi_exact",
                )
            ]
        },
    }


def _evidence(*, match_basis: str) -> dict:
    return evidence_receipt(
        source_name="CMS Provider Enrollment",
        source_url="https://data.cms.gov/provider-enrollment",
        dataset_id="cms-provider-enrollment",
        source_period="current public file",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="hit",
        match_basis=match_basis,
        confidence="high",
        caveat="Public enrollment rows require source-system verification before operational decisions.",
        next_step="Review the returned enrollment detail and ownership rows.",
    )


def test_policy_runner_evaluates_boundary_traceability_and_audit_fields() -> None:
    status = evaluate_provenance_status(_boundary_ready_payload())

    assert status == {
        "status": "evidence_receipt_valid",
        "evidence_present": True,
        "evidence_valid": True,
        "source_metadata_present": True,
        "identity_present": True,
        "source_claim_paths_status": "source_claim_paths_valid",
        "source_claim_paths_valid": True,
    }
    assert audit_provenance_fields(status) == {
        "provenance_status": "evidence_receipt_valid",
        "evidence_present": True,
        "source_metadata_present": True,
        "identity_present": True,
        "source_claim_paths_status": "source_claim_paths_valid",
        "source_claim_paths_valid": True,
    }


def test_policy_runner_builds_compact_audit_evidence_export() -> None:
    spec = LiveToolSpec(
        "provider-enrollment",
        "servers.provider_enrollment.server",
        "search_provider_enrollment",
        "provider_enrollment",
    )
    provenance_status = {
        "status": "source_claim_paths_invalid",
        "evidence_present": True,
        "evidence_valid": True,
        "source_metadata_present": True,
        "identity_present": True,
        "source_claim_paths_status": "source_claim_paths_invalid",
        "source_claim_paths_valid": False,
        "source_claim_path_issues": [{"path": "identity_map", "reason": "missing_identity_map"}],
    }

    export = build_audit_evidence_export(
        spec,
        provenance_status=provenance_status,
        trace_id="trace-unit-test",
        outcome="blocked",
        reason="invalid_source_claim_paths",
    )

    assert export == {
        "trace_id": "trace-unit-test",
        "gateway": "live-gateway",
        "tool": "search_provider_enrollment",
        "server": "provider-enrollment",
        "requested_scopes": ["mcp:read"],
        "rate_limit_class": "standard",
        "source_caveat_class": "provider_enrollment_public_record",
        "provenance": {
            "status": "source_claim_paths_invalid",
            "source_claim_paths_status": "source_claim_paths_invalid",
            "source_claim_paths_valid": False,
            "evidence_present": True,
            "evidence_valid": True,
            "source_metadata_present": True,
            "identity_present": True,
        },
        "blocked_reasons": ["invalid_source_claim_paths", "source_claim_paths_invalid", "missing_identity_map"],
        "degraded_reasons": [],
    }


def test_policy_runner_attaches_policy_without_rewriting_result_shape() -> None:
    payload = _boundary_ready_payload()
    spec = LiveToolSpec(
        "provider-enrollment",
        "servers.provider_enrollment.server",
        "search_provider_enrollment",
        "provider_enrollment",
    )

    audit_evidence = build_audit_evidence_export(
        spec,
        provenance_status=evaluate_provenance_status(payload),
        trace_id="trace-allowed",
        outcome="allowed",
        reason="policy_passed",
    )
    response = attach_gateway_policy(
        spec,
        payload,
        provenance_status=evaluate_provenance_status(payload),
        audit_evidence=audit_evidence,
    )

    assert response["results"] == payload["results"]
    assert response["source_metadata"] == payload["source_metadata"]
    assert response["evidence"] == payload["evidence"]
    assert response["identity_map"] == payload["identity_map"]
    assert response["live_gateway_policy"]["tool"] == "search_provider_enrollment"
    assert response["live_gateway_policy"]["source_caveat_class"] == "provider_enrollment_public_record"
    assert response["live_gateway_policy"]["provenance_status"]["source_claim_paths_valid"] is True
    assert response["live_gateway_policy"]["audit_evidence"] == audit_evidence
