# P1-33 Data MCP blocked host-custody evidence

This is an additive blocker receipt for the P1-33 activation preparation. It
does not replace or rewrite the retained v1 pending-approval receipt at
`contracts/healthcare-data-platform/staging/v1/fixtures/pending-approval.json`.
The v1 receipt remains bound to the P1-31 preparation commit
`ce43147c30793a4dbb35ed97649998a3a5426441`.

The v2 receipt and schema are:

```text
contracts/healthcare-data-platform/staging/v2/fixtures/blocked-host-custody-20260829.json
contracts/healthcare-data-platform/staging/v2/staging-activation-blocked-host-custody.schema.json
```

The receipt binds the user-authorized activation target
`7b11b83268c72634dbfe36f4fe5f61b447975770` and paired Toolkit target
`d21074bf9eae5bcf793186a2e003f64a7d543e27`. The implementation checkout base
`f477e126cc09d5a8ce7edc7ffcd4833cb484f766` is only an observed preparation
checkout and is not substituted for either activation target.

## Truthful blocked state

The top-level state is `blocked_host_custody` with `status: pending` and
`activation_performed: false`. Fixture verification is `execution: not_run`;
the receipt makes no healthy-activation, source-access, migration, or runtime
claim. The blocker records the absent
`/mnt/d/services/healthcare-toolkit-staging` root and unverified parent
custody; the documented `plumbob:plumbob 0777` parent is unsafe. No service was
started and no host path was created or altered.

The permitted read-only preflight command
`ss -H -ltnp '( sport = :8110 or sport = :3020 )'` ran at
`2026-08-29T16:08:11Z` and found both loopback staging ports free. This is port
availability evidence only, not listener or activation evidence.

The isolated fixture seams are caller-owned templates and remain pending:

```text
{tmp_path}/control/scheduler.json
{tmp_path}/control/control.sqlite3
{tmp_path}/raw-custody/
```

Each seam has `ownership: caller_owned_tmp_path`, `evidence_state: pending`,
and `content_present: false`. Source bytes, credentials, and runtime state are
not written.

## Receipt integrity

`receipt_hash` is a real content digest, not a placeholder. Compute it as
`sha256:` followed by the SHA-256 digest of the UTF-8 compact JSON encoding of
the receipt after removing only the `receipt_hash` field, with keys sorted and
separators `(',', ':')`. The fixture test recomputes this value and verifies
the exact target pins and all blocked-state invariants.

The retained v1 receipt and this v2 blocker receipt are both preparation
evidence. Any approved future run must create a new immutable runtime receipt;
it must not rewrite either preparation artifact as a success receipt.
