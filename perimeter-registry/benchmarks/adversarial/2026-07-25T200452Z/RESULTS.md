# Adversarial perimeter-registry generalization benchmark

Benchmark ID: `2026-07-25T200452Z`
Questions/gold frozen: `2026-07-25T20:35:50Z`
Results finalized: `2026-07-25T21:16:00Z`

## Method

This deliberately small study used two structures not present in the prior Jefferson/Penn/UPMC set: Nebraska Medicine/UNMC's public-university and time-bounded member-governance structure, and Kaiser Permanente's contractual three-pillar, regional-plan/independent-medical-group, and Risant reporting-perimeter structure.

Eight natural scope questions and hidden primary-source gold were frozen before answering. Four fresh high-reasoning AI contexts produced 32 answers: two registry-assisted repetitions without browsing and two **manual-style source-research baseline** repetitions researching official primary sources without registry access. Order A and its reverse were counterbalanced. This is an AI source-research comparison, not a human-analyst benchmark. An independent fresh reviewer scored every answer strictly against frozen gold. The exact evaluated fixture bytes are preserved under `inputs/`.

## Frozen strict result

| Arm | Correct | Scope/caveat | False aggregation | Provenance | Missed-data detection | Aggregate run wall time | Research calls |
|---|---:|---:|---:|---:|---:|---:|---:|
| Registry assisted | 16/16 (100%) | 16/16 (100%) | 0/16 | 60/60 (100%) | 16/16 (100%) | 365.070365551 s | 0 |
| Manual-style source research | 8/16 (50%) | 8/16 (50%) | 0/16 | 44/60 (73.3%) | 11/16 (68.8%) | 839.000 s | 61 |

Both baseline repetitions missed the same four questions. N1 omitted part of Bellevue's TNMC tax-control chain and/or a required brand/legal-parent or consolidation boundary. N3 omitted the exact 2023-06-30 source period and, in R2, used sources that did not support all typed identifiers. N4 omitted Nebraska Medicine's time-bounded sole-member relationship over UNMC Physicians. K2 supplied CPMG EIN 84-0832336, but neither cited PBGC pagination URL displays the entity/EIN pair; a direct PBGC `fulltext` request returned the unfiltered corpus. The exact medical-group EIN therefore remains unresolved in the evaluated evidence.

No answer in either arm silently aggregated distinct entity, facility, reporting, or identifier records. Accordingly this sample does not demonstrate a reduction in false aggregation; it demonstrates preservation at zero.

## Post-freeze source adjudication

Independent review found one real fixture/gold coverage miss on N4. The official University of Nebraska 2026 dental plan names University of Nebraska as employer, the Board of Regents as plan sponsor, and 47-0049123 as the employer identification number. That resolves the evaluated fixture's campus-versus-system ambiguity: the EIN belongs on a University/Board legal record, not the UNMC campus record.

Frozen scores above remain immutable. A narrow truth-adjudicated correctness sensitivity changes the registry result from 16/16 to **14/16**, because both registry N4 answers repeated the stale unresolved-scope caveat. Baseline remains **8/16** because both N4 answers still omit the independently required sole-member fact. This sensitivity was not a rerun or a wholesale adjusted rescore. The live fixture was corrected after preserving the evaluated bytes: it now has a separate Board/University node with EIN 47-0049123, leaves the UNMC campus without an inherited EIN, and adds the dated sole-member relationship. The resolver now refuses an EIN for an exact record that has none.

## Timing

New adversarial run-level timings:

| Matched order | Registry run | Baseline run | Baseline / registry |
|---|---:|---:|---:|
| A | 199.000 s | 354.000 s | 1.778894x |
| Reverse B | 166.070365551 s | 485.000 s | 2.920449x |
| Aggregate | 365.070365551 s | 839.000 s | **2.298187x** |

These are summed top-level run wall times across the matched evaluation, not campaign elapsed time; runs overlapped. The wide order-cell spread and two repetitions cannot separate model reasoning, wrapper/order, transient tool, and research-delay effects. Registry fixture research and pre-ingestion labor are outside measured wall time.

For the prior timestamped Jefferson/Penn/UPMC rerun, the actual aggregate matched run-level wall times were **261.129 seconds registry-assisted versus 699.538 seconds fresh AI source research**, yielding **2.68x** (699.538 / 261.129). Neither that rerun nor this adversarial run reached the 5–10x target.

Deterministic lookup was measured separately after 1,000 warmup cycles: 10,000 eight-question cycles / 80,000 calls took 3.930248786 seconds, mean **0.000049128109825 s per lookup**, with median **0.000294935 s** and p95 **0.000671271 s** per eight-question cycle. It excludes fixture loading, model reasoning, wrappers, browsing, and fixture construction, and is not included in the end-to-end ratio.

All four `input_tokens` fields are `null`: the execution environment exposed no input-token telemetry. Research-call and wall-time counts are not token proxies. **No token total, ratio, or savings claim is made.**

## Validation and change audit

The inherited Ruff failure was measured on this branch and parent `codex/perimeter-registry` with the same isolated Ruff 0.16.0 binary: both had exactly **527 diagnostics in 163 files**, with zero branch-only and zero parent-only findings. The remediation added no ignores, blanket `noqa`, exclusions, or CI bypass. Ruff now passes repository-wide. The pre-adjudication full suite passed **1,015 tests with 4 skips in 110.56 seconds**. After the N4 source correction, the corrected perimeter fixture/resolver suite passed **49 tests**, and the final full suite passed **1,015 tests with 4 skips in 111.96 seconds**.

The benchmark used public primary sources. It did not modify or call the IRS 990 Evidence Service, change secrets, deploy, alter production services, or change network settings. Form 990-adjacent lint-only edits and exact timestamps are recorded in `LINT_AND_FORM990_CHANGE_AUDIT.md`.

## Limitations and decision

There are only two systems, eight repeated prompts, two runs per arm, and 16 answers per arm; answers are clustered, not independent. Model/version and sampling telemetry were not captured. Live source research and curated local records are different evidence surfaces. The source miss discovered at N4 shows the registry needs a freshness/adjudication gate before frozen gold is trusted. No nationwide, human-time, lifecycle-cost, token, or 5–10x claim is supported.

**Recommendation:** continue only as a bounded research prototype. The registry clearly preserved scope discipline and reduced research wall time on these cases, but its truth-adjudicated score is 14/16 until the corrected N4 path is rerun, and end-to-end speed is 2.30x, not 5–10x. The smallest next experiment is a targeted fresh rerun of N4 plus the unsupported K2 EIN probe after a primary-source freshness preflight, using the same model, reasoning setting, answer wrapper, and separately instrumented model time, local-read time, search/open wait, and page-review time. That can test restoration to 16/16 and whether research delay—not wrapper/model overhead—can fairly support a 5–10x target before adding another system.
