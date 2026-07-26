# Adversarial generalization benchmark protocol

> **Historical clarification (2026-07-26):** This protocol gave the assisted
> arm curated fixtures and precomputed resolver output containing answer-ready
> identity, perimeter, identifier, caveat, and provenance facts. It measured
> pre-resolved scope assistance, not end-to-end financial retrieval. The frozen
> protocol remains only as audit evidence.

Frozen at **2026-07-25T20:35:50Z** under run directory **2026-07-25T200452Z**.

## Objective and limits

This deliberately small benchmark compares a registry-assisted AI workflow with
a **manual-style source-research baseline**: a fresh AI agent researching
primary sources without registry records. It is not a human-analyst benchmark.
The cases are Nebraska Medicine/UNMC and Kaiser Permanente, selected because
their public-university/interim-governance and contractual three-pillar/Risant
structures differ from the prior Jefferson, Penn, and UPMC cases.

The test covers scope correctness, ambiguity, typed identifiers, source
provenance, false aggregation, and missed-data detection. It does not test broad
financial retrieval, nationwide generalization, production readiness, or human
labor savings.

## Frozen inputs and blinding

The eight questions in questions.json and hidden gold in gold.json were frozen
before any answer run. The registry arm may read only its assigned question
order, the Nebraska and Kaiser fixture JSON, and registry_resolver_outputs.json;
it may not browse or read gold, source dossiers, other runs, scores, or reports. The
exact fixture bytes evaluated by the four runs are preserved after completion as
`inputs/nebraska.evaluated.json` and `inputs/kaiser.evaluated.json`. The committed
snapshots provide the durable integrity record; unnecessary high-entropy checksum
literals are omitted from the manifest.
The baseline arm may read only its assigned questions and research primary or
official sources; it may not read registry fixtures, resolver outputs, gold,
source dossiers, other runs, scores, or reports.

Each run uses a fresh agent context. Two repetitions are run per arm. Order A is
assigned to registry R1 and baseline R2; reverse order B is assigned to registry
R2 and baseline R1.

## Run records

Each answer records its question ID, answer, primary-source URLs, start/end UTC
timestamps, elapsed seconds, and research tool calls. Each run records aggregate
wall time, total research calls, and input tokens only if directly observable.
Unavailable input-token telemetry must be null and may not be estimated.

Deterministic resolver lookup is measured separately using warmed in-process
calls and is never included in end-to-end model wall time.

## Metrics and decision rule

All-or-nothing correctness requires every gold requirement and no failure
condition. The scorer also records scope/caveat labeling, false aggregation,
primary-source provenance coverage, and missed-data detection.

With only 32 answers total (16 per arm), results are directional. Advance only
if the registry preserves or improves correctness and missed-data detection
without increasing false aggregation or materially degrading provenance.
A 5–10x latency target is evaluated only from matched run-level wall time; no
token or human-time claim is permitted without direct telemetry.

## Post-freeze adjudication — 2026-07-25T21:16:00Z

Independent review found a newer/broader official University dental-plan source for
N4. Frozen gold and strict scores were not rewritten. `RESULTS.md` reports the
truth-adjudicated correctness sensitivity, and the live Nebraska fixture was corrected
only after the evaluated fixture bytes were preserved. K2's claimed medical-group EIN
remains unsupported by the cited PBGC pagination URLs.
