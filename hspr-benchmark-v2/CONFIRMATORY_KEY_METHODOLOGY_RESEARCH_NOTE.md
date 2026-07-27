# Confirmatory cohort and answer-key methodology research note

Status: prospective, non-secret methodology only. No replacement system is
selected here, no answer key was created or inspected, and no benchmark answer
arm was run.

Repository baseline: checked-in `HEAD` `5171a87852efb15377ab84cfc425ad3ecaac0844`
on 2026-07-27. The public-history classifier and its tests also had uncommitted
worktree changes; this note labels those separately rather than treating them as
versioned protocol.

## Conclusions

The checked-in cohort builder reproduces the present 35 standard / 50 random /
15 tricky composition from a 639-row frame, but it is not yet a complete frozen
sampling specification: it records only a 12-character frame-hash prefix,
depends on Python's `random.sample`, embeds a checkout-dependent absolute path
in its output, and has no reserve or replacement algorithm. The regression test
checks repeatability in one environment, uniqueness, pilot disjointness, and the
three group counts; it does not bind the exact 100 IDs or full frame digest
([builder](../hspr_benchmark/cohort.py#L71-L129),
[test](../tests/test_hspr_benchmark_v2.py#L259-L270)).

The public-history audit was subsequently tightened in the worktree after an
independent gate review rejected a proximity-based classifier. Policy
`strict-identity-hit-v4` permits only digest-pinned, reviewed versions of the
declared frame and cohort-construction files. Every other identity hit blocks as
known answer-bearing material or `identity_hit_review_required`. The audit now
binds its exact question and identity inputs by SHA-256; the runner verifies
those hashes and the exact audit implementation. It scans resolved commit SHAs,
extracts PDF and ZIP-based Office text, and fails on unsupported opaque
artifacts. These changes remain prospective until reviewed, tested, and committed
([audit](../hspr_benchmark/public_history_audit.py#L13-L148),
[boundary rationale](WEB_BOUNDARY_OPTIONS.md#L11-L16)).

The future confirmatory key is approximately 300 records because the protocol
has 100 systems and three questions per system. No checked-in key-construction
state machine exists. The one-lead workflow and the collision-replacement rules
below are therefore prospective protocol, not a description of an existing
implementation ([design](PROTOCOL.md#L5-L14),
[construction-burden fields](config/construction-burden-template.json#L1-L5)).

## Exact public cohort construction

The current construction is:

1. Read exactly 639 rows and retain system ID, system name, and acute beds;
   missing bed counts become zero.
2. Take 35 enumerated standard IDs and 15 enumerated tricky IDs. Require their
   50-ID union to exist in the frame and to be disjoint from the four pilot IDs.
3. Exclude those 54 IDs. Sort the remaining 585 rows by `(acute_beds,
   system_id)`, split that order at integer quartile boundaries, and sample
   `12/12/13/13` without replacement using an integer SHA-256 derivation of the
   fixed string seed.
4. Concatenate standard, random, and tricky rows and require 100 unique IDs.

These operations are directly implemented in the builder
([constants and seed](../hspr_benchmark/cohort.py#L11-L68),
[frame and sampling](../hspr_benchmark/cohort.py#L71-L116)). The published
protocol confirms 35/50/15, pilot disjointness, and the three metric questions
([protocol](PROTOCOL.md#L5-L9)).

Before confirmatory key research, freeze a private construction attestation with
the full frame SHA-256, exact code revision, Python version, exact original 100
IDs, original group labels, the random members' quartile labels, seed, and a
canonical path-independent serialization digest. The tracked public cohort's
hash prefix and method remain useful provenance, but are not sufficient alone
for cross-environment byte reproduction
([emitted provenance](../hspr_benchmark/cohort.py#L117-L128)).

## Prospective collision and replacement rules

This collision procedure was used to construct and document the clean
replacement Batch 1. Under the revised evidence-proportionate execution policy,
its outputs are diagnostic and do not gate key construction or answer runs.

A **genuinely answer-bearing collision** is a blob reachable from a frozen,
explicitly enumerated public ref that contains an adjudicated active identity
term and material that supplies or materially shortcuts a target financial
answer, perimeter, authoritative source, exact locator, or benchmark result.
Only exact, reviewed digests of declared cohort/sampling metadata are
automatically nonblocking. All other identity hits—including apparently neutral
catalog mentions—fail closed into protected review because file-distance and
keyword heuristics cannot prove nonassociation. The review record stores ref
SHA, blob ID, path, matched identity term, classification reason, reviewer, and
any explicitly approved safe-blob digest, but not answer values in Git.

When the optional audit is run, it should first cover ID and canonical name, then be rerun over the final
identity packet's aliases, legal-entity names, and identifiers. It must traverse
every commit/blob reachable from every freshly fetched public branch, tag, and
pull-request head, record the exact tip SHAs, and be rerun immediately before
every official batch. Release assets and force-pushed or otherwise unreachable
material require a separate attestation; this Git audit does not claim to cover
them. The
runner records a supplied audit as a private, digest-bound diagnostic attached
to the exact active manifest; its pass/fail status and public-ref freshness do
not control execution
([audit traversal and identity expansion](../hspr_benchmark/public_history_audit.py#L83-L148),
[run gate](../hspr_benchmark/runner.py#L284-L303),
[documented requirement](WEB_BOUNDARY_OPTIONS.md#L61-L82)).

Replacement is allowed only for a confirmed answer-bearing public-history
collision or a separately preregistered frame-eligibility defect. Research
difficulty, an unavailable metric, an unattractive value, or an answer-arm
result can never trigger replacement.

Apply replacements one-for-one under these frozen rules:

- Never rerun `build_cohort` after changing a declared member. Freeze and
  preserve the original seeded random 50. A standard or tricky replacement must
  be outside that original random 50, all pilots/prior excluded packets, the
  remaining active cohort, and earlier rejected candidates.
- Preserve `standard` or `tricky` membership. Before looking at candidate names,
  freeze operational rubrics and a deterministic reserve-ranking procedure for
  each declared group; the current code enumerates those groups but does not
  define their substantive selection rubrics.
- For a collided random member, preserve its original `acute_bed_q1`–`q4`
  stratum. Rank the unused members of that original frozen quartile with a
  language-independent SHA-256 ordering over a separately frozen replacement
  seed, stratum, and system ID. Audit candidates in that order and take the
  first collision-free candidate.
- Never redraw unaffected members, backfill across strata, or select a candidate
  by evidence availability. Preserve every rejected candidate and reason in a
  protected append-only log. Version the cohort attestation after each
  replacement.

Collision-screening changes the inclusion mechanism. Report each replacement
and its reason after all answer trials are immutable, label the resulting cohort
as the original sample with prospective same-stratum collision replacement, and
include a sensitivity analysis excluding replacements. Do not claim it is an
untouched simple random sample.

The checked-in history shows the relevant fail-closed precedent: candidates
were excluded for improper key delegation, a generic-name collision, or an
insufficiently defensible key; artifacts were preserved and no answer arm ran
([precedent](WEB_BOUNDARY_OPTIONS.md#L61-L72)). For the confirmatory cohort,
however, insufficient evidence is an evidence gap—not a collision replacement
trigger.

## One-Sol-High-lead, approximately 300-record workflow

`PILOT_PREREGISTRATION.md` is retained as historical, pilot-specific provenance;
its two-independent-key and high-reasoning-adjudication requirement is not
silently rewritten ([pilot rule](PILOT_PREREGISTRATION.md#L6-L14)). The future
confirmatory preregistration prospectively supersedes that construction rule for
the confirmatory key only, under the user-approved model of one Sol High lead
with end-to-end responsibility for identity, research, validation, and sealing.

Freeze ten balanced 10-system batches before research: five batches contain
4 standard / 5 random / 1 tricky system, and five contain 3 standard / 5 random /
2 tricky systems. Each batch therefore targets 30 records. Use a frozen
deterministic within-group ordering; batch order must not react to research
difficulty or observed values.

For each system, the lead completes all three records together so reporting
perimeter and period stay coherent:

1. build and validate the identity-only packet;
2. pass the full identity-aware history audit before financial research;
3. retrieve the latest authoritative primary financial source and preserve a
   protected source snapshot/digest;
4. draft revenue/top-line, operating result (including explicit not-reported and
   closest precisely labelled subtotal handling), and fiscal-year-end assets;
5. record exact source label, displayed value and scale, period/date, reporting
   perimeter, URL, locator, aggregation decision, caveats, and uncertainty;
6. perform a separate lead-validation pass by reopening the preserved source and
   checking label, sign, scale, period, perimeter, locator, and cross-record
   consistency before marking any record ready.

Those fields are required by the answer schema and scorer
([answer schema](config/response-schema.json#L5-L49),
[scoring semantics](../hspr_benchmark/scoring.py#L87-L129)). The lead remains the
sole owner of the final evidence decision. Sol Medium helpers may perform
narrowly scoped retrieval or verification and return evidence memos; the lead
must independently reopen the primary source, reconcile the memo, and make
every identity, perimeter, metric, and value decision. Helper work is logged
and cannot become a record directly. A second decision layer is added only for
a concrete unresolved disagreement or evidence gap, not as a routine
adjudication stage.

### Batch states and evidence-gap escalation

| State | Entry requirement | Exit rule |
| --- | --- | --- |
| `planned` | Frozen cohort/batch membership and digests | Identity work begins |
| `identity_review` | Identity-only records in progress | Complete packet plus zero-blocking identity-aware audit |
| `researching` | History-clear systems only | All 30 records drafted, or a structured gap opens |
| `evidence_gap` | Missing source, unresolved perimeter, conflicting official documents, absent locator, or unclear metric label | Return to `researching` with new primary evidence, or remain blocked |
| `lead_validation` | Thirty draft records and source snapshots | Independent reopen/check of every record |
| `ready_to_seal` | All records validated; counts/schema/cross-record checks pass; no open gaps | Immutable batch digest written |
| `sealed` | Batch digest and protected append-only log finalized | No mutation; corrections create a superseding batch version |
| `superseded` | A later version replaces the batch | Retain for audit; never score it |

An evidence gap receives a typed log entry, attempted sources/searches, the
precise unresolved proposition, and next action. The lead escalates in order:
official system financial statements/annual report; official regulator, SEC,
municipal disclosure, or tax filing where applicable; alternate official copy
of the same filing; then a second, temporally separated lead review. A genuinely
unreported operating-result metric follows the preregistered unavailable-plus-
closest-labelled-subtotal rule. Lack of defensible evidence for any other
required gold field leaves the batch in `evidence_gap` and blocks sealing and
all answer execution; it does not justify swapping systems. Construction burden
records actor, timing, source calls, updates, ambiguities, and notes
([burden template](config/construction-burden-template.json#L1-L5)).

## Separation from answer arms

The key lead must not run, monitor, inspect, or adapt to any answer arm or answer
output before the complete key, questions, tolerances, analysis code, and
preregistration are frozen. Key research occurs in a protected non-repository
root. Questions, identity packet, research logs, source snapshots, batch
artifacts, and gold remain unpublished until all answer trials are immutable.

Only the identity-only registry may cross to HSPR answer contexts; it excludes
financial values, report URLs, locators, hints, and scoring data and must pass
both structural and sealed-key-aware leakage audits
([registry boundary](registry/README.md#L1-L12),
[leakage audit](../hspr_benchmark/leakage.py#L13-L38)). Answer contexts have no
repository or sealed-key mount, while the scorer alone receives answers and
gold ([isolation](PROTOCOL.md#L18-L25)). No official arm may run until the exact
protected cohort/questions have a sealed ready key, identity-aware public-history
audit, registry/key leakage audit, manifest binding, and the independent runtime
boundary gates ([subscription gate](SUBSCRIPTION_PROTOCOL.md#L42-L48),
[runner gate](../hspr_benchmark/runner.py#L250-L310)).
