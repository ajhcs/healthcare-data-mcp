from __future__ import annotations

import json
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from perimeter_registry import Registry  # noqa: E402


def percentile(values: list[int], fraction: float) -> int:
    return sorted(values)[round((len(values) - 1) * fraction)]


def main() -> None:
    run_root = Path(__file__).parent
    questions = json.loads((run_root / "questions.json").read_text())["questions"]
    registries = {
        name: Registry.from_fixture(name)
        for name in sorted({item["fixture"] for item in questions})
    }
    for _ in range(1_000):
        for item in questions:
            registries[item["fixture"]].resolve(item["question"])

    iterations = 10_000
    values: list[int] = []
    started = time.perf_counter_ns()
    for _ in range(iterations):
        cycle_start = time.perf_counter_ns()
        for item in questions:
            registries[item["fixture"]].resolve(item["question"])
        values.append(time.perf_counter_ns() - cycle_start)
    total = time.perf_counter_ns() - started
    call_count = iterations * len(questions)
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "warmup_cycles": 1_000,
        "timed_cycles": iterations,
        "questions_per_cycle": len(questions),
        "lookup_calls": call_count,
        "total_seconds": total / 1_000_000_000,
        "mean_seconds_per_lookup": total / call_count / 1_000_000_000,
        "median_seconds_per_eight_question_cycle": statistics.median(values)
        / 1_000_000_000,
        "p95_seconds_per_eight_question_cycle": percentile(values, 0.95)
        / 1_000_000_000,
        "note": (
            "Warmed in-process deterministic resolution only; "
            "excludes model and file-loading latency."
        ),
    }
    (run_root / "runs/lookup_timings.json").write_text(
        json.dumps(payload, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
