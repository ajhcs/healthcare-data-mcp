# Scale Roster and Bed-Basis Acquisition Mission Packet

Tracking bead: `HDM-nuq`

Cross-repository parent: `healthcare-toolkit-2rr9.6`

Downstream beads: Healthcare Agents `beads-0z7`, then Healthcare Toolkit
`healthcare-toolkit-2rr9.6.1`

## Mission

Produce the first governed Scale evidence slice for ChristianaCare, Jefferson
Health, Temple Health, Penn Medicine, Cooper University Health Care, and Main
Line Health. Resolve public system/facility identity, enumerate the hospital
roster in scope, and acquire receipted bed observations with an explicit bed
basis. The output is evidence, not a Scale score or strategic conclusion.

## Frozen boundary

- Start from merged Data MCP main `0de3622e98f01115145641bce1da79e276b6f459`.
- Emit `ushso.public-evidence-bundle.v1` and preserve its canonical hash,
  receipts, cache/input artifacts, coverage edges, parser/connector versions,
  source timestamps, and exact producer commit.
- Data MCP owns acquisition, source-local normalization, caching, receipts,
  stable source/entity/observation identifiers, and source-local derivations.
- Toolkit owns product identity adjudication, Scale formulas, projections,
  assurance, promotion, and release. Agents owns specialist evaluation.
- Do not add Toolkit database access, release credentials, a shared model, or a
  sibling-checkout dependency to Data MCP.

## Required evidence model

For every system, represent:

1. Public system identity and known aliases.
2. Every candidate hospital/facility considered for the roster, including the
   source-local facility identifier and inclusion, exclusion, or unresolved
   disposition evidence.
3. Bed observations tied to one facility, measure, period, receipt, and bed
   basis. Preserve source terminology; do not silently equate licensed,
   certified, maintained, available, staffed, or operating beds.
4. Explicit missingness using `not_yet_researched`, `unavailable_public`,
   `not_applicable`, or `blocked_source_conflict` whenever a comparable value
   cannot be supported.
5. Coverage rows whose entity and measure exactly match every referenced
   observation.

Conflicting identities, ownership dates, facility status, and bed bases remain
structured conflicts. A convenient system total must not be synthesized from
incompatible bases.

## Source and receipt gates

- Prefer current primary public sources with stable archived artifacts. Record
  access time, observation period, source publication/update time when known,
  media type, exact checksum, and connector/parser provenance.
- Reject non-finite values, malformed or reversed time periods, duplicate
  artifact identities, checksum conflicts, non-portable cache locators, receipt
  drift, and any coverage edge that changes entity or measure meaning.
- Dirty working-tree bytes must not affect output. Contract fixtures and rebuilds
  execute from temporary checkouts at explicit commits.
- A source conflict is a result, not permission to choose the favorable value.

## Deliverables

- Connector/parser changes needed for roster and bed-basis acquisition.
- A synthetic or safely redistributable all-six contract fixture exercising
  values, conflicts, and every allowed missingness state.
- Public Evidence Bundle schema/runtime parity tests and negative tests for
  identities, hashes, coverage, temporal fields, bed-basis semantics, and cache
  locator portability.
- A frozen handoff containing the exact bundle hash and producer commit for
  `beads-0z7`.

## Verification

Run Ruff, focused contract/connector tests, full pytest, schema export parity,
package build, `twine check`, and wheel import smoke. Rebuild twice from the
same pinned commit and compare canonical bytes and hashes. Review the final diff
against repository standards and this packet; hard/high findings block handoff,
and medium findings require correction or a recorded disposition.

## Authority and sequencing

Cole Lyons is the sole initial implementation owner for this slice. Agents
`beads-0z7` and Toolkit `healthcare-toolkit-2rr9.6.1` remain unassigned until
this bead freezes and verifies its handoff. No parallel implementation owner may
edit the shared contract seam.

## No-go and rollback

No partial Scale score, public projection, promotion attempt, package release,
deployment, or production migration is authorized. If the slice cannot produce
comparable bed evidence for all six systems, hand off the receipted evidence and
structured blockers; do not impute or narrow the denominator silently.

Rollback is a reviewable revert of this bead's additive connector, fixture,
contract-test, and documentation commits. Preserve immutable receipts and
published contract v1; breaking changes require a new version and adapter.
