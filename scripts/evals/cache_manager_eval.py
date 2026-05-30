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
from shared.utils.workflows import build_workflow_plan


def scenario_cache_planning(cache_root: Path) -> dict[str, Any]:
    plan = core.plan_cache_refresh(workflow_id="hospital_competitive_profile", cache_root=cache_root)
    passed = bool(plan["ordered_plan"]) and bool(plan["blockers"]) and all(row["next_action"] for row in plan["ordered_plan"])
    return _result("cache_planning", passed, plan)


def scenario_stale_refresh(cache_root: Path) -> dict[str, Any]:
    manifest = _promote_general_info(cache_root, "stale-run")
    old = time.time() - 120 * 86400
    for artifact in manifest.artifacts:
        os.utime(Path(artifact["path"]), (old, old))
    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=cache_root)["status"]
    return _result("stale_refresh", status["readiness_status"] == "stale", status)


def scenario_corrupt_cache_recovery(cache_root: Path) -> dict[str, Any]:
    manifest = _promote_general_info(cache_root, "corrupt-run")
    Path(manifest.path).write_text("", encoding="utf-8")
    status = core.inspect_cache_source("cms_hospital_general_info", cache_root=cache_root)["status"]
    quarantine = core.quarantine_cache_artifact("cms_hospital_general_info", cache_root=cache_root, reason="eval_corrupt")
    return _result(
        "corrupt_cache_recovery",
        status["readiness_status"] == "corrupt" and quarantine["status"] == "quarantined",
        {"status": status, "quarantine": quarantine},
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
    return _result("missing_env_and_imports", passed, {key: by_id[key] for key in ("ahrq_hfmd", "docgraph_referrals", "state_health_data")})


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
    return _result("exact_measure_behavior", passed, measure_step)


def scenario_state_limited_refusal(cache_root: Path) -> dict[str, Any]:
    status = core.inspect_cache_source("pa_hospital_reports", cache_root=cache_root)["status"]
    passed = status["readiness_status"] == "state_limited" and "state-specific" in status["next_action"]
    return _result("state_limited_source_refusal", passed, status)


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
    return _result("report_ingest_validation", True, row)


SCENARIOS: tuple[Callable[[Path], dict[str, Any]], ...] = (
    scenario_cache_planning,
    scenario_stale_refresh,
    scenario_corrupt_cache_recovery,
    scenario_missing_env_and_imports,
    scenario_exact_measure_behavior,
    scenario_state_limited_refusal,
    scenario_report_ingest_validation,
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


def _result(name: str, passed: bool, details: Any) -> dict[str, Any]:
    return {"scenario": name, "passed": passed, "details": details}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic cache-manager evals.")
    parser.add_argument("--cache-root", type=Path, default=Path("build/evals/cache-manager"))
    args = parser.parse_args()
    print(json.dumps(run_evals(args.cache_root), indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
