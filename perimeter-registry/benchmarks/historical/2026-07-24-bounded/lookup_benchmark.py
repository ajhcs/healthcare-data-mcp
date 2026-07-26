from __future__ import annotations

import json
import platform
import statistics
import time
from pathlib import Path

from perimeter_registry import Registry

WARM_ITERATIONS = 10_000
COLD_ITERATIONS = 300


def summarize(values_ns: list[int]) -> dict[str, float]:
    ordered = sorted(values_ns)
    p95_index = min(len(ordered) - 1, int(len(ordered) * 0.95))
    return {
        "mean_ms": statistics.fmean(values_ns) / 1_000_000,
        "median_ms": statistics.median(values_ns) / 1_000_000,
        "p95_ms": ordered[p95_index] / 1_000_000,
        "max_ms": max(values_ns) / 1_000_000,
    }


def main() -> None:
    root = Path(__file__).parent
    questions = json.loads((root / "questions.json").read_text())["questions"]
    by_fixture: dict[str, list[dict[str, str]]] = {}
    for item in questions:
        by_fixture.setdefault(item["fixture"], []).append(item)

    warm_results: dict[str, object] = {}
    for fixture, items in by_fixture.items():
        registry = Registry.from_fixture(fixture)
        for item in items:
            registry.resolve(item["question"])
            samples: list[int] = []
            for _ in range(WARM_ITERATIONS):
                started = time.perf_counter_ns()
                registry.resolve(item["question"])
                samples.append(time.perf_counter_ns() - started)
            warm_results[item["id"]] = summarize(samples)

    cold_results: dict[str, object] = {}
    for fixture, items in by_fixture.items():
        question = items[0]["question"]
        samples = []
        for _ in range(COLD_ITERATIONS):
            started = time.perf_counter_ns()
            Registry.from_fixture(fixture).resolve(question)
            samples.append(time.perf_counter_ns() - started)
        cold_results[fixture] = summarize(samples)

    output = {
        "version": 1,
        "clock": "time.perf_counter_ns",
        "python": platform.python_version(),
        "platform": platform.platform(),
        "warm_iterations_per_question": WARM_ITERATIONS,
        "cold_iterations_per_fixture": COLD_ITERATIONS,
        "warm_in_process_resolution": warm_results,
        "cold_fixture_load_validate_and_resolution": cold_results,
    }
    destination = root / "runs" / "lookup_timings.json"
    destination.write_text(json.dumps(output, indent=2) + "\n")
    print(destination)


if __name__ == "__main__":
    main()
