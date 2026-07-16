# Scale Roster and Bed-Basis Frozen Handoff

Tracking bead: `HDM-nuq`

Downstream consumer: Healthcare Toolkit `healthcare-toolkit-2rr9.6.2`

## Frozen acquisition

- Workflow: `scale-roster-bed-basis.v1`
- Public contract: `ushso.public-evidence-bundle.v1`
- Connector: `scale-roster-bed-connector.v1`
- Parser: `scale-roster-bed-parser.v1`
- Cache run: `scale-roster-beds-20260716-08`
- Acquisition cutoff: `2026-07-16T21:40:49.364509+00:00`
- Candidate universe: 63 facilities across six enterprise-wide systems
- Frozen artifacts: 27
- Normalized observations: 249
- Coverage rows: 291
- Open conflicts: 5

Official-system artifacts retain `unknown_review_required` rights. Raw official
HTML and PDF bytes remain in the external immutable cache run and are not
committed. Government source artifacts use `public_domain`. The checked-in
manifest uses only portable `hc-cache://` locators.

The workflow froze the current Q1 2026 CMS POS distribution, the FY26 Maryland
licensed-bed table, Pennsylvania's 2024 general and specialty hospital
workbooks, New Jersey's current acute-care license workbook, Delaware's current
hospital license page, and the AHRQ 2023 linkage file. Their distinct periods
and bed bases remain explicit on every observation and receipt.

## Evidence disposition

The bundle provides a source-local roster disposition for every candidate and
basis-specific facility observations where exact official, state, HGI, POS,
HCRIS, or AHRQ rows were available. Every other candidate has explicit coverage using
`not_yet_researched`, `unavailable_public`, `not_applicable`, or
`blocked_source_conflict`.

No system bed total, hospital count, additive rollup, modeled Scale input,
Scale component, or Scale score is emitted. In particular:

- Union Hospital retains official, Maryland FY26 licensed, POS, and HCRIS
  values as separate bases and an open conflict.
- Temple Main and Episcopal retain receipted Pennsylvania, POS, and HCRIS
  shared-reporting values while remaining blocked from any campus allocation.
- Chestnut Hill retains ownership and bed-basis conflicts.
- Jefferson's March 2025 33-location enterprise roster remains unallocated to
  current state licenses and CCNs where exact joins were not established.
  Pennsylvania name-only workbook rows are separate source-local entities with
  unresolved candidate relationships, never automatic merges.
- Penn's six principal hospitals remain separate from Cedar Avenue and the
  additional rehabilitation/behavioral candidates.
- Cooper's current About page calls Children's Regional a third hospital; that
  new source assertion is preserved as an unresolved conflict rather than
  silently forced into the prior two-hospital assumption.
- Bryn Mawr Rehabilitation is not calculated as a residual.

## Verification record

Pre-merge candidate bundle hash with the placeholder producer commit:
`sha256:9f6bd13a8b62f0c5aa8112ec5d528c23bbcf9b884540c8b472f825d2f0e7d057`.
The authoritative handoff hash must be rebuilt after merge with the merge SHA
in `producer.commit`; it will differ from this candidate hash.

Final PR URL: <https://github.com/ajhcs/healthcare-data-mcp/pull/38>.

Final implementation merge SHA: `4496c07aafa72852d3f0caf80593e66745902c97`.

Final producer-bound `bundle_sha256`:
`sha256:241a6a909613df116802a2d96965ce9678b76e2887c0f1af9f146186ddd75568`.

Standards review: passed with no hard, high, or medium findings after the
`CONTRIBUTING.md` exception was narrowed, parser/build/acquisition boundaries
were separated, and declarative records were made keyword-explicit.

Mission-packet review: passed with no hard, high, or medium findings after
governed offline byte verification, shared reporting entities, current
state/AHRQ sources, exact HCRIS periods, and PA source-local identity handling
were corrected.

Clean-checkout reproducibility: passed at the implementation merge SHA. Two
governed offline rebuilds reparsed and checksum-verified every raw artifact in
cache run `scale-roster-beds-20260716-08`. Their normalized input files were
byte-identical (`sha256:ca4dfd5e5b55826fa862ac5e8a8e7516cbb4f615843524aa6679e0002581a1b5`),
and their producer-bound bundle files were byte-identical
(`sha256:25b4a6ee35b00cc12d75803a87c1d38c5bc8321efaf27549fe29746f0c2bd8cd`).
Both outputs passed runtime and published JSON Schema validation, receipt and
artifact lineage assertions, exact producer-commit validation, and the no-score
gate. Merged-main Ruff and the focused contract/acquisition suite also passed
(56 tests).

Pre-merge checks: Ruff; focused acquisition/handoff suite (30 passed); full
pytest (838 passed); schema export parity; package sdist/wheel build; `twine
check`; and installed-wheel import smoke. Pinned-commit byte-for-byte rebuild
and merged-main verification remain pending until the producer commit is final.

## Rollback

Revert implementation merge commit
`4496c07aafa72852d3f0caf80593e66745902c97`. Do not mutate or delete the frozen
external cache run or rewrite Public Evidence Bundle v1. If a source or parser
must change, acquire a new cache run and handoff hash; never replace bytes under
the existing `hc-cache://scale-roster-bed-basis.v1/scale-roster-beds-20260716-08/`
identity.
