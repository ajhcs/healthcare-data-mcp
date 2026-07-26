from __future__ import annotations

from pathlib import Path

BENCHMARK_ROOT = Path(__file__).parents[1] / "benchmarks"
ANSWER_READY_NAMES = {
    "gold.json",
    "gold.snapshot.json",
    "registry_resolver_outputs.json",
    "scores.json",
}


def test_benchmark_root_contains_only_boundary_and_history() -> None:
    assert {path.name for path in BENCHMARK_ROOT.iterdir()} == {
        "BOUNDARY.md",
        "README.md",
        "historical",
    }


def test_answer_ready_benchmark_artifacts_are_quarantined() -> None:
    historical = BENCHMARK_ROOT / "historical"
    leaked = [
        path
        for path in BENCHMARK_ROOT.rglob("*")
        if path.is_file()
        and (
            path.name in ANSWER_READY_NAMES
            or path.name.endswith("_question_gold_draft.json")
            or "runs" in path.parts
            or "research" in path.parts
            or "inputs" in path.parts
        )
        and not path.is_relative_to(historical)
    ]
    assert leaked == []


def test_boundary_names_the_invalid_retrieval_comparison() -> None:
    boundary = (BENCHMARK_ROOT / "BOUNDARY.md").read_text()
    readme = (BENCHMARK_ROOT / "README.md").read_text()
    for text in (boundary, readme):
        assert "pre-resolved scope assistance" in text
        assert "end-to-end financial retrieval" in text
