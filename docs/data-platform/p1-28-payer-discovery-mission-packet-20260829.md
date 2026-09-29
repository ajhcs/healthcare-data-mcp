# P1-28 Payer discovery and type-of-coverage candidates

Tracking bead: `healthcare-toolkit-rrna.p1-28-payer-discovery-20260829`

## Goal

Produce read-only, source-bound payer type-of-coverage (TOC) and reference
candidates for profile review. Preserve explicit denominator, geography,
period, plan/contract identity, coverage, and unresolved states.

## Scope

- F7 TOC/reference candidate normalization.
- Official CMS Medicare Advantage, Part D, and Marketplace enrollment
  denominator source families.
- Explicit `not_yet_researched`, `unavailable_public`, `not_applicable`, and
  `blocked_source_conflict` states.
- Fail closed when census or ACS population/insurance rows are offered as
  payer denominators.

## Out of scope

Network acquisition, payer-mix calculation, profile writes, census modeling,
credential handling, deployment, and shared MCP server wiring. A later join
may expose the isolated producer through the profiler server.

## Acceptance criteria

- Every candidate identifies one allowed TOC: Medicare Advantage, Part D, or
  Marketplace.
- Every supported row retains official source family, period, geography,
  plan/contract identity, and denominator value/scope.
- Coverage distinguishes complete, unresolved, and not-evaluated states.
- Census/ACS population data cannot become a payer denominator.
- Focused tests cover all three official families, census rejection, and empty
  research state.

## Planned commits

1. `feat(profile): add payer discovery evidence candidates`
2. `docs(profile): document payer source boundaries`
3. `test(profile): cover payer denominator rejection`

## Verification and handoff

Run the focused payer tests, compile check, `git diff --check`, and the exact
base-to-head diff review. Do not run the broad suite, wait/full gate, deploy,
push, or production writes in this lane. Handoff reports the exact SHA chain,
clean worktree, and known limitation that server wiring is deferred.

Owner: Luna Max direct writer
Status: implementation complete pending independent acceptance review
