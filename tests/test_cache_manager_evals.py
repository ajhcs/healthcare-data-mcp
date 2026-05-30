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
    } == {row["scenario"] for row in result["results"]}
