# Protocol

## Design

- Cohort: 35 declared standard systems, 50 reproducibly sampled systems, and 15
  declared tricky systems. The 4-system pilot is disjoint from all 100.
- Questions: three per system—latest annual top-line revenue, operating result
  (or an explicit unavailable answer with closest precisely labeled subtotal),
  and fiscal-year-end total assets.
- Arms: `hspr-medium`, `hspr-xhigh`, `native-medium`, `native-xhigh`, all using
  Luna and identical live-research tools/runtime. HSPR access is the sole
  treatment difference; reasoning level is the other factor.
- Run order: paired and counterbalanced by system/question/repetition. Target
  three fresh repetitions per arm, subject to pilot variance and exact power.

## Isolation

Questions, registry packets, runtime configs/traces, and sealed gold occupy
separate roots. Answer worktrees receive an allowlisted packet containing only
one question, response schema, and (for HSPR arms) the audited identity packet.
They do not receive the repository checkout or sealed root. The scorer receives
answers and adjudicated gold but no credentials or answer-agent context. This is
filesystem/process isolation and allowlisting, not cryptographic secrecy.

## Telemetry

Use monotonic timestamps for run start/end, HSPR lookup start/end, local reads,
each observable web search/open/click/download, first/last authoritative
financial evidence, and final answer. Record tool-call counts, latency, and
token/cost fields only when emitted by the runtime. Never infer or fabricate
hidden reasoning time. Raw event traces are immutable inputs to derived timing.

## Outcomes and analysis

Score financial correctness, entity/perimeter correctness, provenance/locator
quality, caveat quality, false aggregation, tool calls, wall latency, observable
step timing, available token/cost telemetry, and HSPR construction/maintenance
burden. Primary comparisons are paired; uncertainty uses system-level clusters.
The AHRQ frame supports selection, not an unqualified national-prevalence claim.

