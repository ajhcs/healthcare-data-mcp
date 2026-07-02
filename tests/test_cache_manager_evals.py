from __future__ import annotations

from scripts.evals.cache_manager_eval import run_evals


def test_cache_manager_deterministic_evals_pass(tmp_path):
    result = run_evals(tmp_path / "cache")

    assert result["status"] == "pass"
    assert result["passed_count"] == result["scenario_count"]
    assert {
        "cache_planning",
        "stale_refresh",
        "corrupt_cache_recovery",
        "missing_env_and_imports",
        "exact_measure_behavior",
        "state_limited_source_refusal",
        "report_ingest_validation",
        "source_substitution_refusal",
        "missing_source_status_recovery",
        "workflow_handoff_traceability",
        "live_gateway_provenance_refusal",
    } == {row["scenario"] for row in result["results"]}
    for row in result["results"]:
        assert row["remediation_hint"], row
        assert isinstance(row["details"], dict), row

    by_scenario = {row["scenario"]: row for row in result["results"]}
    assert by_scenario["source_substitution_refusal"]["details"]["failure_reason"] == "source_substitution"
    assert by_scenario["workflow_handoff_traceability"]["details"]["review_route"]["route"] == "candidate_context_review"
    assert "missing_identity_map" in by_scenario["live_gateway_provenance_refusal"]["details"]["audit_evidence"][
        "blocked_reasons"
    ]
