# P1-33 R3 wave integration receipt

Date: 2026-08-29  
Status: `prepared_not_applied`; offline fixture contract only. No source probe,
listener, database, credential, host, production, push, reset, rebase, or
destructive action occurred.

## Exact identity

| Field | Value |
| --- | --- |
| Baseline main | `63539f8412831ee62b067c76c3b5c395108481cc` |
| Manifest-bound executable candidate | `2f2dca2f3e22238942dee14c808a13f6024c77d5` |
| Integration evidence head | `773dc916ac6364ed6ed25c96f492de796ca76b68` |
| Branch | `codex/p1-staging-unblock-wave-data-mcp-20260829-v2` |
| Fixture receipt | `sha256:280c0dc3d5c9d8ba42b0fa0b31fa9cf7e9dca26f85d8d7dfac3d098628b0dcff` |

The implementation candidate remains `2f2dca2f...c77d5` because the runner
binds its manifest to an executable commit; the later integration-head commits
are additive receipts/documentation and are verified as descendants. Runtime
activation must use the manifest-bound candidate, not the historical
`7b11b832...` preparation SHA.

## Evidence and review

- Three deterministic runs are byte-identical and record no-op, changed input,
  durable acknowledgement before raw custody/checkpoint, duplicate/replay
  idempotency, lease recovery, quarantine, immutable custody, current/as-of
  projection, pointer-only rollback, lineage/missingness, stream bounds,
  zero listeners, and denied external egress.
- Manifest closure contains 28 executable/imported artifacts and each hash
  matches both the working tree and `git show <candidate>:<path>`.
- Focused gate: **9 passed** on the exact integration graph; Ruff, format,
  compilation, and `git diff --check` passed. The writer/reviewer receipt
  records 15 focused tests for the correction scope and Luna Max APPROVE digest
  `sha256:0fab1b2d3b16939dec3309e39babb430cd93a4ece472bbe002a7db742958362a`.
- A fresh wheel installation remains blocked by unavailable approved index/
  wheelhouse and DNS; no clean-install success is claimed.

## Artifact hashes

| Artifact | SHA-256 |
| --- | --- |
| `requirements/staging-fixture.lock` | `015444671a3f480fbe694b53b8ae00e85c48de3538058a5be538ed9d357dfc29` |
| `ops/staging/data-mcp-staging-fixture-manifest.json` | `b4de2ca3f88966a2299ee4880319f9348aec099b5b1168bd8ddcd12a9e9c89f4` |
| `scripts/run_data_mcp_staging_fixture.py` | `4ff3c268c3e985362469b0ecccf2e2a6f8866dc1effdee3af2c9358c0b6be1ab` |
| `contracts/healthcare-data-platform/staging/v1/fixtures/deterministic-source-input.json` | `8c3b3ef42219298cbc81593fc10c50fc2979817fc8817ae274b892746f53fb4d` |
| `contracts/healthcare-data-platform/staging/v1/fixtures/deterministic-source-input.schema.json` | `ec9168a6834031618fc29fa64d355c29096dd146241df9e7e50e5e3b2a235e87` |

Historical pending/correction receipts remain unchanged. The canonical main
worktree remains at `63539f8`, ahead of origin by 146 and behind by 1, with
managed `.beads/issues.jsonl` preserved. This integration branch is not a
source-native Phase 1 proof and cannot authorize Phase 2 promotion.
