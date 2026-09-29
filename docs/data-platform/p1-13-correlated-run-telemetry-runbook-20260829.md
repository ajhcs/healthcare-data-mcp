# P1-13 correlated run telemetry runbook

Status: bounded local implementation complete for independent review.  The
telemetry lane is transport-neutral and performs no network acquisition,
queue/scheduler mutation, custody write, projection write, deployment,
credential operation, or production activation.

## Record one run event

Use `build_run_telemetry()` to construct an immutable event.  Every event must
carry the same run correlation plus the source, artifact, and observation
envelope identities that the producing seam already owns.  Measurements are
non-negative and finite; `freshness_state="unknown"` is explicit when the
source timestamp is unavailable.

```python
from datetime import datetime, timezone

from shared.telemetry import TelemetryRecorder, build_run_telemetry

event = build_run_telemetry(
    "telemetry:run:cms-pdc-20260829",
    "run:cms:pdc-20260829",
    "source:cms:pdc",
    "artifact:cms:pdc:20260829",
    "hdp:observation-envelope:cms:pdc-20260829",
    datetime.now(timezone.utc),
    "succeeded",
    freshness_state="fresh",
    freshness_seconds=42.0,
    bytes_in=2_048,
    bytes_out=1_920,
    rows_in=120,
    rows_out=120,
    retry_count=0,
    dimensions={"dataset": "pdc", "operation": "load", "stage": "envelope"},
)

with TelemetryRecorder("telemetry.sqlite") as recorder:
    receipt = recorder.record(event)
```

`record()` is idempotent by `telemetry_id` and the canonical SHA-256.  Repeating
the same event returns a `duplicate` receipt.  Reusing the identity with
different measurements raises `TelemetryCollisionError` and leaves the first
record unchanged.  `record_event()` and `append()` are equivalent aliases.

## Failure and dead-letter events

Use a bounded `failure_code` and caller-owned `failure_retryable` flag for a
failed event.  Failure and DLQ messages are normalized, control characters are
removed, and credential-shaped fragments (authorization, bearer, password,
token, key, and credential-bearing URLs) are replaced with `[REDACTED]` before
hashing or persistence.  Do not pass stack traces, source URLs, request
parameters, payload excerpts, PHI, or credentials.

```python
failed = build_run_telemetry(
    "telemetry:run:cms-pdc-20260829-failure",
    "run:cms:pdc-20260829",
    "source:cms:pdc",
    "artifact:cms:pdc:20260829",
    "hdp:observation-envelope:cms:pdc-20260829",
    datetime.now(timezone.utc),
    "dead_lettered",
    failure_code="source_timeout",
    failure_message="Authorization: Bearer caller-secret",
    failure_retryable=False,
    dlq_state="queued",
    dlq_count=1,
    dlq_reason="source item exceeded retry policy",
)
```

`dead_lettered` requires `dlq_state="queued"` and a positive count.  A normal
event uses `dlq_state="none"`, zero count, and no reason.  This keeps a DLQ
outcome measurable without retaining the work item or its payload.

## Cardinality and retention bounds

Only these dimension keys are approved by default: `dataset`, `environment`,
`operation`, `release`, `source_kind`, `stage`, and `worker_class`.  Each event
has at most eight safe dimensions, each value is at most 128 characters, and a
recorder defaults to at most 128 distinct values per key and 100,000 events.
Callers may lower those limits or provide a strict subset allowlist.  Unknown
or sensitive-looking keys fail closed.  The SQLite store contains timestamps,
IDs, counters, status, redacted error evidence, and dimensions only; it has no
payload, request-body, or raw-object column.

## Inspect a correlated run

```python
events = recorder.list_events("run:cms:pdc-20260829")
summary = recorder.summarize("run:cms:pdc-20260829")
print(summary.as_dict())
```

Events are ordered by observed timestamp and telemetry identity.  The summary
reports status/failure counts, first/last observation times, maximum freshness
and lag, byte/row totals, retry totals, and DLQ totals.  If a run spans more
than one source, artifact, or envelope identity, that summary field is `null`
rather than guessing an owner; the individual events remain the correlation
source of truth.

## Rollback evidence

The recorder is additive and local.  Stop telemetry callers, retain the
SQLite database and exported summaries for audit, and omit or revert the
reviewed P1-13 commit range.  Do not delete telemetry records or alter queue,
scheduler, raw-custody, or envelope state.  Re-enabling telemetry requires the
matching schema/application commit pair.

## Verification and handoff

Run from the isolated worktree:

```text
pytest -q tests/test_healthcare_data_platform_telemetry.py --tb=short
ruff check shared/telemetry tests/test_healthcare_data_platform_telemetry.py
ruff format --check shared/telemetry tests/test_healthcare_data_platform_telemetry.py
pyright shared/telemetry tests/test_healthcare_data_platform_telemetry.py
python3 -m compileall -q shared/telemetry tests/test_healthcare_data_platform_telemetry.py
python3 -m jsonschema -i contracts/healthcare-data-platform/telemetry/v1/fixtures/valid-run-telemetry.json contracts/healthcare-data-platform/telemetry/v1/telemetry.schema.json
git diff --check d5d3d0697cad7cb75cf836cb7872ce4ec4f7a248..HEAD
```

The supplied W12 worktree is:

```text
/tmp/.healthcare-data-mcp-p1-w12-source-20260829-worktrees/p1-13-telemetry
codex/healthcare-toolkit-rrna.p1-13-telemetry-20260829
```

The WTB CLI must be run with task ID
`healthcare-toolkit-rrna.p1-13-telemetry-20260829`.  If the pre-provisioned
path has no recorded manifest, do not synthesize a receipt; report the command
output together with `git rev-parse HEAD`, `git status --short --branch`, and
the exact commit chain.
