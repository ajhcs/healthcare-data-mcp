#!/usr/bin/env python3
"""Benchmark warm exact-fact retrieval independently from cold IRS ingestion."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import median
from time import perf_counter_ns
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from servers.form_990_facts.fact_store import Form990FactStore  # noqa: E402


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * percentile)))
    return ordered[index]


def main() -> None:
    parser = argparse.ArgumentParser(description="Warm post-ingestion Form 990 exact-fact benchmark.")
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=250)
    parser.add_argument(
        "--native-research-seconds",
        type=float,
        default=None,
        help="Optional separately observed repeated native-research latency for the same exact fact set.",
    )
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error("--iterations must be positive")

    with args.manifest.open(newline="", encoding="utf-8-sig") as source:
        queries = [(row["ein"], int(row["tax_year"])) for row in csv.DictReader(source)]
    if not queries:
        parser.error("manifest has no queries")

    store = Form990FactStore(args.database)
    readiness = [store.get_facts(ein=ein, tax_year=tax_year) for ein, tax_year in queries]
    missing = [
        {"ein": ein, "tax_year": tax_year}
        for (ein, tax_year), result in zip(queries, readiness, strict=True)
        if result["status"] != "ready"
    ]
    if missing:
        raise SystemExit(f"benchmark scope is not fully ingested: {json.dumps(missing)}")

    samples_ms: list[float] = []
    for iteration in range(args.iterations):
        ein, tax_year = queries[iteration % len(queries)]
        started = perf_counter_ns()
        result = store.get_facts(ein=ein, tax_year=tax_year)
        elapsed_ms = (perf_counter_ns() - started) / 1_000_000
        if result["status"] != "ready":
            raise RuntimeError("warm query unexpectedly became unavailable")
        samples_ms.append(elapsed_ms)

    output = {
        "benchmark": "warm_post_ingestion_exact_ein_year_query",
        "cold_ingestion_included": False,
        "database": str(args.database),
        "queries": len(queries),
        "iterations": args.iterations,
        "warm_latency_ms": {
            "min": round(min(samples_ms), 3),
            "median": round(median(samples_ms), 3),
            "p95": round(_percentile(samples_ms, 0.95), 3),
            "max": round(max(samples_ms), 3),
        },
        "all_queries_ready": True,
        "native_research_reference_seconds": args.native_research_seconds,
        "speedup_vs_native_research_median": (
            round((args.native_research_seconds * 1000) / median(samples_ms), 1)
            if args.native_research_seconds is not None
            else None
        ),
        "comparison_note": (
            "Native-research time must be observed separately for the same exact fact set; "
            "the benchmark never folds cold IRS archive retrieval into user-facing latency."
        ),
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
