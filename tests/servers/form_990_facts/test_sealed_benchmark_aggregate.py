from scripts.benchmark_form_990_plugin import _aggregate


def _run(condition: str, *, valid: bool, correct: int) -> dict:
    return {
        "condition": condition,
        "valid": valid,
        "latency_ms": 100.0,
        "telemetry": {
            "tokens": {},
            "calls": {},
            "mcp_result_bytes": 0,
        },
        "score": {
            "fact_correct": correct,
            "fact_total": 10,
            "provenance_correct": correct,
            "provenance_total": 28,
            "coverage_correct": 4,
            "coverage_total": 4,
            "scope_honest": True,
        },
    }


def test_invalid_runs_count_as_zero_without_shrinking_scheduled_denominators() -> None:
    aggregate = _aggregate(
        [
            _run("plugin", valid=True, correct=10),
            _run("native", valid=True, correct=10),
            _run("native", valid=False, correct=2),
        ]
    )

    assert aggregate["native"]["valid_runs"] == 1
    assert aggregate["native"]["fact_correct"] == 10
    assert aggregate["native"]["fact_total"] == 20
    assert aggregate["native"]["valid_run_fact_total"] == 10
    assert aggregate["native"]["provenance_correct"] == 10
    assert aggregate["native"]["provenance_total"] == 56
