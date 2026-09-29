# P1-16 raw-object lifecycle guardrails

P1-04 owns immutable raw bytes and manifests. P1-16 adds a separate lifecycle
index under `lifecycle/metadata/` so retention and consumer references can
change without rewriting evidence.

## Eligibility and quota behavior

An artifact can enter recoverable quarantine only when all of the following
are true:

1. Its retention class has an explicit `expires_at`.
2. The expiry and `grace_until` timestamps have elapsed.
3. `reference_count` is zero.
4. `legal_hold` is false.
5. The immutable manifest and content hash still verify.

`append_only`, `indefinite`, and `legal_hold` classes are non-expiring. A
compaction pass counts physical content-addressed objects once, selects the
oldest eligible rows when a quota is exceeded, and records the before/after
byte totals. Shared objects remain in place until every manifest reference is
gone.

## Recoverable cleanup

Compaction moves the manifest and, when unshared, the object into a unique
`lifecycle/quarantine/` directory. It writes a secret-free receipt and updates
only lifecycle metadata. `restore(artifact_id)` moves the files back and
re-verifies the manifest hash and object bytes before returning the artifact to
active custody. Unreferenced objects with no manifest are quarantined as
orphans rather than deleted.

There is intentionally no permanent-delete API in this lane. Destructive GC,
legal-hold release, production activation, and credentials remain separately
authorized human actions. If a live store exceeds quota but no row meets all
eligibility checks, the run returns `blocked_reason` and leaves every object
untouched.

## Verification and rollback

The lifecycle metadata schema is
`contracts/healthcare-data-platform/storage/v1/raw-artifact-lifecycle.schema.json`.
Run the focused custody/lifecycle suite before a wave merge:

```text
pytest -q tests/test_healthcare_data_platform_raw_custody.py tests/test_healthcare_data_platform_lifecycle.py
ruff check shared/storage/lifecycle.py tests/test_healthcare_data_platform_lifecycle.py
ruff format --check shared/storage/lifecycle.py tests/test_healthcare_data_platform_lifecycle.py
git diff --check 3f2fd437e00471065201d0e546b77692eacf9200..HEAD
```

Rollback is recoverable: stop the compaction worker, restore quarantined
artifacts from their receipts, and revert the lifecycle integration commit.
Never remove the `lifecycle/quarantine/` directories or raw objects as part of
an application rollback.
