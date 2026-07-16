# Scale Roster and Bed-Basis Frozen Handoff

Tracking bead: `HDM-nuq`

Downstream consumer: Healthcare Toolkit `healthcare-toolkit-2rr9.6.2`

## Frozen acquisition

- Workflow: `scale-roster-bed-basis.v1`
- Public contract: `ushso.public-evidence-bundle.v1`
- Connector: `scale-roster-bed-connector.v1`
- Parser: `scale-roster-bed-parser.v1`
- Cache run: `scale-roster-beds-20260716-07`
- Acquisition cutoff: `2026-07-16T21:30:49.641346+00:00`
- Candidate universe: 63 facilities across six enterprise-wide systems
- Frozen artifacts: 27
- Normalized observations: 249
- Coverage rows: 285
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
- Penn's six principal hospitals remain separate from Cedar Avenue and the
  additional rehabilitation/behavioral candidates.
- Cooper's current About page calls Children's Regional a third hospital; that
  new source assertion is preserved as an unresolved conflict rather than
  silently forced into the prior two-hospital assumption.
- Bryn Mawr Rehabilitation is not calculated as a residual.

## Verification record

Pre-merge candidate bundle hash with the placeholder producer commit:
`sha256:77eeeb485095b469ab0d390642758788a580bda7d924529f31adb1a310263818`.
The authoritative handoff hash must be rebuilt after merge with the merge SHA
in `producer.commit`; it will differ from this candidate hash.

Final PR URL: pending.

Final merge SHA: pending.

Final bundle SHA-256: pending.

Standards review: pending.

Mission-packet review: pending.

Checks and clean-checkout reproducibility evidence: pending.

Pre-merge checks: Ruff; focused acquisition/handoff suite (29 passed); full
pytest (837 passed); schema export parity; package sdist/wheel build; `twine
check`; and installed-wheel import smoke. Pinned-commit byte-for-byte rebuild
and merged-main verification remain pending until the producer commit is final.

## Rollback

Revert the additive `HDM-nuq` merge commit. Do not mutate or delete the frozen
external cache run or rewrite Public Evidence Bundle v1. If a source or parser
must change, acquire a new cache run and handoff hash; never replace bytes under
the existing `hc-cache://scale-roster-bed-basis.v1/scale-roster-beds-20260716-07/`
identity.
