"""Deterministic no-network cache-manager evaluation scenarios."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Callable

from shared.cache_manager import core
from shared.utils.mcp_response import validate_report_ingest_payload
from shared.utils.source_status import normalize_source_status
from shared.utils.workflows import build_workflow_plan
from servers.live_gateway.policy_runner import (
    LiveToolSpec,
    build_audit_evidence_export,
    evaluate_provenance_status,
)


def scenario_cache_planning(cache_root: Path) -> dict[str, Any]:
    plan = core.plan_cache_refresh(workflow_id="hospital_competitive_profile", cache_root=cache_root)
    passed = bool(plan["ordered_plan"]) and bool(plan["blockers"]) and all(row["next_action"] for row in plan["ordered_plan"])
    return _result(
        "cache_planning",
        passed,
        plan,
        remediation_hint="Call the cache refresh plan first and follow each dataset next_action before running report workflows.",
    )


def scenario_stale_refresh(cache_root: Path) -> dict[str, Any]:
    manifest = _promote_general_info(cache_root, "stale-run")
    old = time.time() - 120 * 86400
    for artifact in manifest.artifacts:
        os.utime(Path(artifact["path"]), (old, old))
    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=cache_root)["status"]
    return _result(
        "stale_refresh",
        status["readiness_status"] == "stale",
        status,
        remediation_hint="Refresh stale public cache artifacts before treating source-backed facts as report-ready.",
    )


def scenario_corrupt_cache_recovery(cache_root: Path) -> dict[str, Any]:
    manifest = _promote_general_info(cache_root, "corrupt-run")
    Path(manifest.path).write_text("", encoding="utf-8")
    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=cache_root)["status"]
    quarantine = core.quarantine_cache_artifact("cms_hospital_general_info", cache_root=cache_root, reason="eval_corrupt")
    return _result(
        "corrupt_cache_recovery",
        status["readiness_status"] == "corrupt" and quarantine["status"] == "quarantined",
        {"status": status, "quarantine": quarantine},
        remediation_hint="Quarantine corrupt artifacts and rerun validation before any workflow consumes the dataset.",
    )


def scenario_missing_env_and_imports(cache_root: Path) -> dict[str, Any]:
    previous = os.environ.pop("SEC_USER_AGENT", None)
    try:
        report = core.cache_status_payload(cache_root)
    finally:
        if previous is not None:
            os.environ["SEC_USER_AGENT"] = previous
    by_id = {entry["dataset_id"]: entry for entry in report["datasets"]}
    passed = (
        by_id["ahrq_hfmd"]["readiness_status"] == "env_required"
        and by_id["docgraph_referrals"]["readiness_status"] == "licensed_import_required"
        and by_id["state_health_data"]["readiness_status"] == "manual_import_required"
    )
    return _result(
        "missing_env_and_imports",
        passed,
        {key: by_id[key] for key in ("ahrq_hfmd", "docgraph_referrals", "state_health_data")},
        remediation_hint="Surface missing environment, licensed import, and manual import requirements instead of inventing fallback facts.",
    )


def scenario_exact_measure_behavior(cache_root: Path) -> dict[str, Any]:
    plan = build_workflow_plan(
        "hospital_competitive_profile",
        inputs={"ccn": "390223", "measure": "clabsi_sir"},
        cache_status=core.cache_status_payload(cache_root),
    )
    by_tool = {step["tool"]: step for step in plan["steps"]}
    measure_step = by_tool["get_quality_measure_rows"]
    passed = (
        measure_step["mcp_call"]["arguments_template"]["measure"] == "clabsi_sir"
        and measure_step["identity_contract"]["match_policy"] == "exact_identifier_required_for_report_fact"
    )
    return _result(
        "exact_measure_behavior",
        passed,
        measure_step,
        remediation_hint="Use exact CCN and measure identifiers for report-ready quality facts; keep adjacent summaries as context only.",
    )


def scenario_state_limited_refusal(cache_root: Path) -> dict[str, Any]:
    status = core.inspect_cache_source("pa_hospital_reports", cache_root=cache_root)["status"]
    passed = status["readiness_status"] == "state_limited" and "state-specific" in status["next_action"]
    return _result(
        "state_limited_source_refusal",
        passed,
        status,
        remediation_hint="Refuse to generalize a state-limited public source outside its stated jurisdiction.",
    )


def scenario_report_ingest_validation(cache_root: Path) -> dict[str, Any]:
    row = {
        "label": "CMS quality exact measure",
        "value": "0.72",
        "identity": {"ccn": "390223"},
        "identity_fields": ["ccn", "measure_id"],
        "identity_map": {"join_keys": [{"field": "ccn", "value": "390223", "status": "provided"}]},
        "source_name": "CMS Hospital Quality Programs",
        "source_url": "https://data.cms.gov/provider-data/",
        "dataset_id": "cms_hospital_quality",
        "source_period": "fixture source period",
        "landing_page": "https://data.cms.gov/provider-data/",
        "retrieved_at": "2026-05-30T00:00:00Z",
        "source_modified": "2026-05-01T00:00:00Z",
        "cache_status": "ready",
        "cache_freshness": "ready",
        "entity_scope": "workflow:hospital_competitive_profile",
        "query": {"ccn": "390223", "measure": "clabsi_sir"},
        "cache_key": "cms_hospital_quality/current.csv",
        "match_basis": "ccn+measure_id_exact",
        "confidence": "source_row",
        "caveat": "Fixture row for report-ingest validation.",
        "next_step": "Use final-report validation before citing.",
    }
    validate_report_ingest_payload(
        {"fact_rows": [row]},
        require_content=True,
        allow_placeholders=False,
        require_identity_context=True,
    )
    return _result(
        "report_ingest_validation",
        True,
        row,
        remediation_hint="Copy complete evidence, source metadata, identity fields, and caveat fields into report fact rows before citation.",
    )


def scenario_source_substitution_refusal(cache_root: Path) -> dict[str, Any]:
    plan = build_workflow_plan(
        "quality_measure_lookup",
        inputs={"ccn": "390223", "measure": "clabsi_sir"},
        cache_status=core.cache_status_payload(cache_root),
    )
    exact_fact = {
        row["label"]: row
        for row in plan["report_ingest_contract"]["fact_rows"]
    }["Exact CMS quality measure row"]
    exact_fact_manifest = {
        row["label"]: row
        for row in plan["report_fact_manifest"]["fact_rows"]
    }["Exact CMS quality measure row"]
    owner_step = {
        f"{step['server'].replace('-', '_')}.{step['tool']}": step
        for step in plan["steps"]
    }[exact_fact_manifest["owner_step_key"]]
    agent_attempt = {
        "claim": "CLABSI SIR for CCN 390223",
        "attempted_dataset_id": "cms_hospital_general_info",
        "attempted_value_path": "cms_facility.get_facility.hospital_overall_rating",
        "required_dataset_id": "cms_hospital_quality",
        "required_value_path": exact_fact["value_path"],
    }
    evaluation = {
        "accepted": False,
        "failure_reason": "source_substitution",
        "agent_attempt": agent_attempt,
        "expected_owner_step": exact_fact_manifest["owner_step_key"],
    }
    passed = (
        evaluation["accepted"] is False
        and agent_attempt["attempted_dataset_id"] != agent_attempt["required_dataset_id"]
        and owner_step["identity_contract"]["match_policy"] == "exact_identifier_required_for_report_fact"
    )
    return _result(
        "source_substitution_refusal",
        passed,
        evaluation,
        remediation_hint=(
            "Retry with hospital-quality.get_quality_measure_rows and preserve the CMS quality row evidence; "
            "do not substitute facility master data or ratings for an exact measure fact."
        ),
    )


def scenario_missing_source_status_recovery(cache_root: Path) -> dict[str, Any]:
    status = core.inspect_cache_source("cms_hospital_quality", cache_root=cache_root)["status"]
    source_status = normalize_source_status(
        status,
        retrieval_method="cache",
        caveat="Missing cache status blocks report-ready CMS quality facts until the source is refreshed.",
    )
    passed = (
        status["readiness_status"] == "missing"
        and set(source_status) >= {"source_url", "source_period", "cache_status", "cache_freshness", "retrieval_method", "caveat"}
        and source_status["retrieval_method"] == "cache"
        and bool(status["next_action"])
    )
    return _result(
        "missing_source_status_recovery",
        passed,
        {"status": status, "source_status": source_status},
        remediation_hint="Expose normalized source_status and run the dataset next_action instead of treating missing cache data as zero.",
    )


def scenario_workflow_handoff_traceability(cache_root: Path) -> dict[str, Any]:
    plan = build_workflow_plan(
        "system_reconciliation",
        inputs={"system_name": "Jefferson Health", "system_slug": "jefferson-health", "ccn": "390223"},
        cache_status=core.cache_status_payload(cache_root),
    )
    web_route = next(
        route
        for route in plan["identity_map"]["review_routing"]["step_routes"]
        if route["qualified_tool"] == "web-intelligence.scrape_system_profile"
    )
    public_web_fact = {
        row["label"]: row
        for row in plan["report_fact_manifest"]["fact_rows"]
    }["Public web alias context"]
    handoff = {
        "workflow_id": plan["workflow_id"],
        "handoff_paths": public_web_fact["paths"],
        "source_claim_path_contract": public_web_fact["source_claim_path_contract"],
        "review_route": web_route,
    }
    passed = (
        handoff["handoff_paths"]["identity_map_path"] == "web_intelligence.scrape_system_profile.identity_map"
        and handoff["review_route"]["route"] == "candidate_context_review"
        and handoff["source_claim_path_contract"]["source_claims_path"].endswith(".identity_map.source_claims")
    )
    return _result(
        "workflow_handoff_traceability",
        passed,
        handoff,
        remediation_hint="Hand off workflow facts with owner paths, source_claim paths, and review_routing so the next agent can continue safely.",
    )


def scenario_live_gateway_provenance_refusal(cache_root: Path) -> dict[str, Any]:
    del cache_root
    payload = {
        "results": [{"npi": "1234567893"}],
        "source_metadata": {
            "source_name": "CMS Provider Enrollment",
            "source_url": "https://data.cms.gov/provider-enrollment",
            "dataset_id": "cms-provider-enrollment",
        },
        "evidence": {
            "source_name": "CMS Provider Enrollment",
            "source_url": "https://data.cms.gov/provider-enrollment",
            "dataset_id": "cms-provider-enrollment",
            "source_period": "current public file",
            "landing_page": "",
            "retrieved_at": "2026-05-22T00:00:00Z",
            "source_modified": "",
            "cache_status": "hit",
            "cache_freshness": "hit",
            "entity_scope": "",
            "query": None,
            "cache_key": "",
            "match_basis": "npi_exact",
            "confidence": "high",
            "caveat": "Public enrollment rows require source-system verification before operational decisions.",
            "next_step": "Review source claim paths before live routing.",
        },
    }
    provenance_status = evaluate_provenance_status(payload)
    spec = LiveToolSpec(
        "provider-enrollment",
        "servers.provider_enrollment.server",
        "search_provider_enrollment",
        "provider_enrollment",
    )
    audit_evidence = build_audit_evidence_export(
        spec,
        provenance_status=provenance_status,
        trace_id="eval-live-gateway-refusal",
        outcome="blocked",
        reason="invalid_source_claim_paths",
    )
    passed = (
        provenance_status["status"] == "source_claim_paths_invalid"
        and audit_evidence["provenance"]["source_claim_paths_valid"] is False
        and "missing_identity_map" in audit_evidence["blocked_reasons"]
    )
    return _result(
        "live_gateway_provenance_refusal",
        passed,
        {"provenance_status": provenance_status, "audit_evidence": audit_evidence},
        remediation_hint="Add identity_map.source_claims with evidence/source_metadata/identity paths before crossing the live gateway.",
    )


SCENARIOS: tuple[Callable[[Path], dict[str, Any]], ...] = (
    scenario_cache_planning,
    scenario_stale_refresh,
    scenario_corrupt_cache_recovery,
    scenario_missing_env_and_imports,
    scenario_exact_measure_behavior,
    scenario_state_limited_refusal,
    scenario_report_ingest_validation,
    scenario_source_substitution_refusal,
    scenario_missing_source_status_recovery,
    scenario_workflow_handoff_traceability,
    scenario_live_gateway_provenance_refusal,
)


def run_evals(cache_root: Path) -> dict[str, Any]:
    cache_root = cache_root.expanduser().resolve(strict=False)
    if cache_root.exists():
        shutil.rmtree(cache_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    results = [scenario(cache_root) for scenario in SCENARIOS]
    return {
        "status": "pass" if all(result["passed"] for result in results) else "fail",
        "scenario_count": len(results),
        "passed_count": sum(1 for result in results if result["passed"]),
        "results": results,
    }


def _promote_general_info(cache_root: Path, run_id: str):
    spec = core.get_dataset_spec("cms_hospital_general_info")
    staged_artifacts = []
    for index, relative_path in enumerate(spec.expected_artifacts):
        staged = _staged_csv(cache_root, "cms_hospital_general_info", run_id, index)
        validation = core.validate_cache_source("cms_hospital_general_info", cache_root=cache_root, staged_path=staged)[
            "validation"
        ]
        staged_artifacts.append((relative_path, staged, spec.source_urls[0], core.CacheValidationResult(**validation)))
    return core._promote_many(spec, cache_root, staged_artifacts, run_id)  # noqa: SLF001


def _staged_csv(cache_root: Path, dataset_id: str, run_id: str, index: int = 0) -> Path:
    staged = cache_root / "bronze" / dataset_id / run_id / f"source-{index}.csv"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_text(
        "facility_id,facility_name,address,city,state,zip_code,hospital_type,hospital_ownership,"
        "emergency_services,hospital_overall_rating\n"
        "390223,Example Hospital,1 Main St,Philadelphia,PA,19104,Acute Care,Voluntary,Yes,4\n",
        encoding="utf-8",
    )
    return staged


def _result(name: str, passed: bool, details: Any, *, remediation_hint: str) -> dict[str, Any]:
    return {
        "scenario": name,
        "passed": passed,
        "details": details,
        "remediation_hint": remediation_hint,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic cache-manager evals.")
    parser.add_argument("--cache-root", type=Path, default=Path("build/evals/cache-manager"))
    args = parser.parse_args()
    print(json.dumps(run_evals(args.cache_root), indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
