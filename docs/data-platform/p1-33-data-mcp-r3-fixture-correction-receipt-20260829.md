# P1-33 R3 fixture correction receipt

This additive receipt supersedes the implementation details in the original
R3 candidate note without rewriting that historical note or any prior receipt.
It remains an offline, prepared-not-applied candidate; it is not source-native
Phase 1 runtime evidence or a promotion decision.

## Exact correction

| Field | Value |
| --- | --- |
| Repository | `healthcare-data-mcp` |
| Worktree | `/tmp/healthcare-data-mcp-r3-fixture-20260829-v2` |
| Branch | `codex/r3-data-mcp-fixture-20260829-v2` |
| Correction base | `ec3c0375d0f752e7bcbe5746c0ab9bd184d0d2c4` |
| Reviewed code/test candidate | `ca0f54d6fa054399f708b480102aee7adb5ab5e0` |
| Baseline | `63539f8412831ee62b067c76c3b5c395108481cc` |
| Manifest | `ops/staging/data-mcp-staging-fixture-manifest.json` |

The focused correction commit is:

```text
77caefe fix(staging): close fixture safety review findings
26108f9 test(staging): assert fixture correction receipts
ca0f54d fix(staging): type fixture control bounds
```

The candidate runner rejects an all-zero or malformed SHA, requires the
requested SHA to equal `manifest.reviewed_candidate_sha`, verifies the commit
exists and is an ancestor of the executing checkout, and compares every
manifest artifact against both the working tree and the candidate Git tree
before creating the caller-owned fixture root.

The manifest closes the reviewed tree over the runner, fixture contracts,
catalog, scheduler/queue/raw-artifact schemas, and the shared cadence,
scheduler, queue, and raw-custody imports.  It records 15 artifacts and the
expected deterministic receipt file hash
`sha256:40ce19ddad2c1a7d20abe1ed88226fc6eabc490f0f5024a6a6ab30c7928f429f`.

## Safety corrections

- The process-wide audit guard is installed before PyYAML, jsonschema, or any
  repository module imports.  It denies connect, bind, listen, accept, send,
  and all covered address-resolution events, including `getnameinfo`,
  `gethostbyaddr`, and `getfqdn`.
- The queue acknowledgement checkpoint is durably written first, raw custody
  is finalized second, and the scheduler checkpoint is written third.  The
  receipt records and asserts this exact order.  SQLite enters and re-observes
  `journal_mode=wal` on initial open and restart.
- Control JSON writes validate their schema, write a canonical temporary file,
  fsync the file, atomically replace the destination, fsync the directory,
  reopen the destination, and revalidate its bytes and schema.
- Stream bytes, chunk count, chunk size, and elapsed deadline are enforced at
  the fixture boundary.  Queue and raw-custody bounds remain active as well.
- Pointer-only rollback updates both `current.json` and
  `projections.json.current`; the changed artifact is absent from current,
  remains present in the as-of projection and immutable raw custody, and the
  prior current artifact remains readable.

## Verification

```text
python3 -m pytest -q tests/test_data_mcp_staging_bundle.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py  -> 13 passed
ruff check scripts/run_data_mcp_staging_fixture.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py  -> passed
ruff format --check scripts/run_data_mcp_staging_fixture.py tests/test_data_mcp_staging_fixture_runner.py tests/test_data_mcp_staging_lock.py  -> passed
python3 -m py_compile scripts/run_data_mcp_staging_fixture.py  -> passed
git diff --check  -> passed
```

The focused tests cover candidate mismatch/all-zero rejection before root
creation, all required network-resolution events, stream bounds, deterministic
receipts across isolated roots, WAL observation, durable ordering, rollback
projection consistency, and the complete fixture boundary set.  The runner's
receipt reports every check as `passed`, with ordering
`acknowledgement_checkpoint`, `raw_custody`, `scheduler_checkpoint`, no
listeners, no source egress, no credentials, and no migrations.

The writer self-review inspected the exact `ec3c0375..ca0f54d` correction diff
and found no architecture change, scope expansion, production mutation,
remote reconciliation, or historical receipt rewrite.  No bounded correction
findings remain for this lane.

The canonical Data MCP main worktree remains at `63539f8`, ahead of origin by
146 and behind by 1, with its modified managed `.beads/issues.jsonl`
preserved.  The embedded Beads lock blocker and unavailable Grok Build
repository egress remain unchanged; no Bead export was hand-edited.
