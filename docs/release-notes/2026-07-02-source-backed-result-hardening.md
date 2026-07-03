# Source-Backed Result Hardening

This release closes the post-migration source-backed result hardening sequence.

Highlights:

- Every MCP server exposes a standard `source-ledger` resource that links agents
  to the source-backed result contract and source capability ledger.
- Workflow plans now include report fact manifests, review-routing metadata, and
  source-claim path contracts for safer cross-server handoffs.
- Doctor/cache readiness surfaces normalize source status fields so missing
  cache/source facts remain explicit unknowns.
- Live-gateway policy decisions export compact non-secret `audit_evidence` with
  trace IDs, requested scopes, provenance status, and blocked/degraded reasons.
- Deterministic agent evals now cover source substitution refusals, missing
  source-status recovery, workflow handoffs, and live-gateway provenance
  refusals with remediation hints.

Verification:

- `pytest`
- Wave gates from `docs/SOURCE_BACKED_RESULT_POST_MIGRATION_PLAN.md`
