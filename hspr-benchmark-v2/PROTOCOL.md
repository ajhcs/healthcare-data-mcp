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
separate roots. The implemented scheduler does not launch answer agents.
Temporary working-directory minimization is not filesystem isolation and is
rejected. Before a pilot, an executor must create an OS-sandboxed or equivalently
access-controlled answer context allowlisted to one question, response schema,
and (for HSPR arms) the audited identity packet. The scorer receives answers and
adjudicated gold but no answer-agent context. This is not cryptographic secrecy.

## Telemetry

Use monotonic timestamps for run start/end. HSPR lookup, local reads, web tool
calls, first/last authoritative financial evidence, and final answer must use
timestamps emitted at the event by the runtime/tool layer. File-copy staging is
not an HSPR lookup. Keyword matches do not establish source authority. Missing
event timestamps, token counts, and cost stay null. Never infer hidden timing.

## Outcomes and analysis

Score financial correctness, entity/perimeter correctness, provenance/locator
quality, caveat quality, false aggregation, tool calls, wall latency, observable
step timing, available token/cost telemetry, and HSPR construction/maintenance
burden. Primary comparisons are paired; uncertainty uses system-level clusters.
The AHRQ frame supports selection, not an unqualified national-prevalence claim.
