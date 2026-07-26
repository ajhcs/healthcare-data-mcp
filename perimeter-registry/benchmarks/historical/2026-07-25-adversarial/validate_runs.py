from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    root = Path(__file__).parent
    assignments = json.loads((root / "orders.json").read_text())["assignments"]
    expected = {
        "registry_r1": ("registry_assisted", 1),
        "registry_r2": ("registry_assisted", 2),
        "baseline_r1": ("manual_style_source_research_baseline", 1),
        "baseline_r2": ("manual_style_source_research_baseline", 2),
    }
    for run_name, (arm, repetition) in expected.items():
        data = json.loads((root / "runs" / f"{run_name}.json").read_text())
        assert data["arm"] == arm, run_name
        assert data["repetition"] == repetition, run_name
        assert data["order"] == assignments[run_name], run_name
        assert data["input_tokens"] is None, run_name
        assert "unavailable" in data["input_token_note"].casefold(), run_name
        answers = data["answers"]
        assert [item["question_id"] for item in answers] == data["order"], run_name
        assert len(answers) == 8 and len({item["question_id"] for item in answers}) == 8
        assert all(item["answer"].strip() for item in answers)
        assert all(item["latency_seconds"] >= 0 for item in answers)
        assert all(isinstance(item["primary_source_urls"], list) for item in answers)
        assert sum(item["research_tool_calls"] for item in answers) == data["research_tool_calls"]
    print("validated 4 counterbalanced adversarial runs / 32 answers")


if __name__ == "__main__":
    main()
