# Fixed benchmark protocol

> **Historical clarification (2026-07-26):** This protocol gave the assisted
> arm curated fixtures and precomputed resolver output containing answer-ready
> identity and perimeter facts. It measured pre-resolved scope assistance, not
> end-to-end financial retrieval. This frozen protocol is retained for audit and
> is not an approved template for a new retrieval comparison.

## Objective and terminology

This bounded benchmark compares a registry-assisted AI answer workflow with a **manual-style source-research baseline**: a fresh AI agent that researches primary sources without registry records. It is not a human-analyst benchmark, and no claim about manual human labor is permitted.

The cases are exactly Jefferson Health, Penn Medicine/UPHS, and UPMC. Fixtures are intentionally small and are not exhaustive entity censuses. The benchmark tests perimeter and identifier selection, not broad finance retrieval.

## Frozen inputs and blinding

`questions.json` contains 12 natural questions (four per system). `gold.json` contains primary-source-derived gold, accepted ambiguity, and explicit failure conditions. Answering agents may read their assigned questions but must not inspect `gold.json`, the source dossier, other run files, or the report. The gold is opened for scoring only after all four run files exist.

The registry arm may read only its questions, deterministic resolver output, and the three registry fixtures. It may not browse. The baseline arm receives the same questions and may research only primary/official sources; it may not read any perimeter-registry fixture, resolver output, gold, dossier, report, or other run. Each run uses a fresh agent context.

## Pairing and counterbalance

Two repetitions are run per arm. `orders.json` assigns order A to registry R1 and baseline R2, and reverse order B to registry R2 and baseline R1. This balances order across arms while deliberately retaining a very small sample. Each agent answers all 12 questions sequentially and records per-answer elapsed time and primary-source research tool calls.

## Metrics

Per answer: all-or-nothing perimeter/identifier correctness; explicit scope/caveat label; false aggregation; primary-source provenance coverage. Per run: wall-clock latency and research tool calls. Input tokens are recorded only if directly observable; otherwise `null`, not estimated. Registry lookup latency is measured separately by `lookup_benchmark.py` using warmed in-process calls and is never conflated with model end-to-end latency.

No statistical significance, nationwide generalization, human-time savings, or production readiness is inferred from 48 total answers (24 per arm). The decision rule is directional: expand only if the registry reduces false aggregation and improves correctness without materially degrading provenance; otherwise revise or stop.
