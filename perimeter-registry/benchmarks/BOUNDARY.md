# HSPR evaluation data boundary

This boundary separates three kinds of data that were previously adjacent and
easy to mistake for a runnable retrieval benchmark.

## 1. Legitimate registry metadata

The records under `../src/perimeter_registry/data/` are product fixtures for
identity and perimeter resolution. Their typed entities, identifiers,
relationships, reporting scopes, caveats, and source citations are legitimate
HSPR metadata.

For the July 2026 questions, however, those records are also answer-bearing.
Giving them to one arm is an evaluation of pre-resolved scope assistance, not a
test of that arm's ability to discover the same facts from primary sources.
They must not be included in a future retrieval-comparison packet unless the
approved protocol explicitly treats them as provided evidence for every arm.

## 2. Hidden gold and scoring data

Frozen gold, draft gold, scoring rubrics, score files, reviewer notes, and
source dossiers live only under `historical/`. They are retained for audit and
must never be available to an answering agent.

Repository placement is an audit boundary, not a security boundary. A future
runner must construct an explicit allowlisted packet outside this tree rather
than relying on instructions that tell an agent not to open nearby files.

## 3. Answer-ready historical material

Deterministic resolver outputs, evaluated fixture snapshots, completed answers,
and research notes also live only under `historical/`. They are historical
evidence, not agent inputs. Resolver snapshots in particular are effectively
fact sheets for the frozen questions and must never be used as evidence of live
retrieval performance.

## Current state

No active `agent-inputs` directory or runnable agent packet is committed. The
historical scripts remain solely to preserve provenance; running or adapting
them does not create an approved benchmark. Benchmark redesign, end-to-end financial retrieval, cohort expansion, and
end-to-end reruns are intentionally deferred.
