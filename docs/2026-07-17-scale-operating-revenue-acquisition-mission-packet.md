# Scale Operating Revenue Acquisition Mission Packet

Tracking bead: `HDM-pzz`

Classification: evidence acquisition

Downstream coordinator: `healthcare-toolkit-2rr9.6.3`

## Goal

Acquire and freeze public aggregate `operating_revenue_usd` evidence for the
six systems in the existing Scale roster. Emit one deterministic Public
Evidence Bundle with source-local values or explicit missingness, exact
receipts, periods, definitions, organizational boundaries, conflicts, and
semantic hashes. Do not calculate, normalize, rank, recommend, or publish a
Scale score.

## Fixed Scope

The systems are ChristianaCare, Jefferson Health, Temple Health, Penn Medicine,
Cooper University Health Care, and Main Line Health. The acquisition prefers
the latest audited consolidated health-system financial statement available at
the frozen cutoff. A value is admissible only when the statement reports an
operating-revenue total for a boundary that can be identified without inferred
affiliate aggregation. Total revenue, net patient service revenue, unaudited
annualization, and peer estimates are not substitutes.

Every populated observation retains the statement label, units/scale, fiscal
period, consolidation boundary, row/page locator, retrieved source bytes, and
portable receipt. A source-backed zero is preserved as zero. Absence, boundary
ambiguity, definition mismatch, or conflicting audited totals is represented
as structured missingness or an open conflict, never imputed.

The initial acquisition admits four audited source-local candidates
(ChristianaCare, Jefferson, Penn, and Main Line) only as non-approved context.
Temple's available year-end report is explicitly unaudited and therefore
contributes no numeric candidate. Cooper's current audit endpoint returned an
exactly frozen HTTP 403 response and likewise contributes no numeric candidate.
All six `operating_revenue_usd` coverage rows remain
`blocked_source_conflict` pending a common period and reviewed product boundary.

## Deliverables

- Additive internal acquisition models and validation for one field-neutral
  six-system Scale input-family bundle.
- Frozen acquisition specification, source-byte fixtures, extracted facts,
  and canonical Public Evidence Bundle input/output for
  `operating_revenue_usd`.
- A thin CLI that rebuilds the committed artifacts from a clean tree and fails
  on dirty input, source/receipt drift, roster narrowing, definition or period
  substitution, fabricated zeroes, missing receipts, imputation, or any Scale
  computation/output field.
- Tests for schema/runtime validation, semantic hashes, deterministic rebuilds,
  exact all-six coverage, conflict preservation, and the no-calculation
  boundary.
- A durable handoff recording the producer commit and exact artifact hashes for
  Toolkit bead `healthcare-toolkit-2rr9.6.3.1`.

## Verification

Run the focused acquisition and contract suites, Ruff, strict mypy, two clean
rebuilds, repository release checks, and independent Standards and mission
reviews. Hard/high findings block handoff; medium findings are corrected or
explicitly dispositioned.

## Authority, Privacy, and Rollback

Only public aggregate evidence is allowed. No patient-level data, PHI,
credentials, profile writes, production mutation, Scale calculation,
sensitivity run, projection, adjudication, recommendation, or promotion is
authorized. Rollback is a focused revert of the additive acquisition code,
fixtures, tests, and documentation; immutable upstream roster/bed evidence is
not rewritten.
