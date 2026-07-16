# Scale Roster and Bed-Basis Frozen Handoff

Tracking bead: `HDM-nuq`

Downstream consumer: Healthcare Toolkit `healthcare-toolkit-2rr9.6.2`

## Frozen acquisition

- Workflow: `scale-roster-bed-basis.v1`
- Public contract: `ushso.public-evidence-bundle.v1`
- Connector: `scale-roster-bed-connector.v1`
- Parser: `scale-roster-bed-parser.v1`
- Cache run: `scale-roster-beds-20260716-04`
- Acquisition cutoff: `2026-07-16T21:02:50.828901+00:00`
- Candidate universe: 63 facilities across six enterprise-wide systems
- Frozen artifacts: 24
- Normalized observations: 153
- Coverage rows: 198
- Open conflicts: 4

Official-system artifacts retain `unknown_review_required` rights. Raw official
HTML and PDF bytes remain in the external immutable cache run and are not
committed. Government source artifacts use `public_domain`. The checked-in
manifest uses only portable `hc-cache://` locators.

Q1 2026 CMS POS was not discoverable through the current CMS catalog at the
cutoff. The workflow therefore froze the exact Q4 2025 file and labels every
POS receipt with the fallback vintage and stale-source caveat.

## Evidence disposition

The bundle provides a source-local roster disposition for every candidate and
basis-specific facility observations where exact official, HGI, POS, or HCRIS
rows were available. Every other candidate has explicit coverage using
`not_yet_researched`, `unavailable_public`, `not_applicable`, or
`blocked_source_conflict`.

No system bed total, hospital count, additive rollup, modeled Scale input,
Scale component, or Scale score is emitted. In particular:

- Union Hospital retains official, POS, and HCRIS values as separate bases and
  an open conflict.
- Temple Main and Episcopal remain blocked from a shared-CCN allocation.
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
`sha256:18604b2c72c62a7f8f56b7fe2818c5440d8073a9cd3ed45fb100b6fd11d3ea91`.
The authoritative handoff hash must be rebuilt after merge with the merge SHA
in `producer.commit`; it will differ from this candidate hash.

Final PR URL: pending.

Final merge SHA: pending.

Final bundle SHA-256: pending.

Standards review: pending.

Mission-packet review: pending.

Checks and clean-checkout reproducibility evidence: pending.

## Rollback

Revert the additive `HDM-nuq` merge commit. Do not mutate or delete the frozen
external cache run or rewrite Public Evidence Bundle v1. If a source or parser
must change, acquire a new cache run and handoff hash; never replace bytes under
the existing `hc-cache://scale-roster-bed-basis.v1/scale-roster-beds-20260716-04/`
identity.
