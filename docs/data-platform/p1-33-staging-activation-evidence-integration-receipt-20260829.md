# P1-33 staging activation-evidence integration receipt

Date: 2026-08-29  
Repository: `healthcare-data-mcp`  
Reviewed candidate: `6b2c9631ff262ec3c9eceec808c419b4b61429d1`  
Integrated main head: `cd161b8648ccf60a65a4e4a0f056c9823549a437`

## Decision

The local merge admits the additive P1-33 v2 blocker evidence after an
independent non-writer Luna Max review.  The retained v1 pending-approval
schema, fixture, test, and runbook are unchanged.  The v2 receipt binds the
human-authorized target pair exactly:

- Data MCP target: `7b11b83268c72634dbfe36f4fe5f61b447975770`
- Toolkit target: `d21074bf9eae5bcf793186a2e003f64a7d543e27`

Reviewer receipt digest: `0d85480979b51da884899da46c95b06927ca68abb48658bb6b65135d168e8a49`.
Writer correction receipt digest: `2a7e446a97837eb769908b58d95bad66f2ffca7176a8d4aac385e820d0118906`.

Focused tests, Ruff (Python only), formatting, Pyright, compile, JSON/schema,
canonical receipt-hash, and diff checks passed.  The initial reviewer Ruff
invocation included a JSON schema and produced Python F821 noise; the
authoritative Python-only rerun passed.  No source probe, service start,
database or migration action, credential resolution, host-storage mutation,
production ingress, push, or destructive action occurred.

## Activation boundary and rollback

This is evidence integration, not a healthy activation claim.  The runbook
still blocks loopback startup because `/mnt/d/services` is recorded as
`plumbob:plumbob 0777` and `/mnt/d/services/healthcare-toolkit-staging` is
absent.  Only caller-owned temporary fixtures were permitted.  Do not create
the staging root, harden host custody, provision credentials or database
roles, start listeners, or expose ingress under this authorization.

If a later approved rehearsal fails a Class A invariant, restore the prior
verified pointer and preserve this receipt and the source worktree.  No
destructive rollback is authorized here.
