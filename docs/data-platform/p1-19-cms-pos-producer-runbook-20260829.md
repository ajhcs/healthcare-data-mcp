# P1-19 CMS POS producer runbook

Tracking task: `hdp-p1-19-cms-pos-20260829`

## Purpose and boundary

`shared.acquisition.cms_pos.CmsPosProducer` is a dry-run/source-admission
seam. It receives release metadata, distribution metadata, an adapter catalog,
a `ConditionalResponse`, and caller-owned byte chunks. It does not perform
HTTP, discover CMS URLs, write files, retain raw custody, advance a cursor,
write a database/queue, or publish a profile fact. `PRVDR_NUM` and every other
CSV column remain source-native; no canonical identity is emitted.

The catalog registration must use `source:cms:pos`, an HTTPS source URL and
release locator, `change_mode="release_metadata"` (or another explicitly
reviewed mode), and `rights_status="approved_public"`. The release's
`source_url` and `release_locator` must match that registration exactly.

## Stable release and distribution identity

Every release carries a stable `release:cms:pos:*` ID, source period, source
and landing URLs, publication timestamp, and a semantic SHA-256 fingerprint.
The CSV distribution carries a stable `distribution:cms:pos:*` ID, HTTPS URL,
`text/csv` media type, optional ETag/Last-Modified validators, and a semantic
distribution fingerprint. Stream bytes receive a separate content SHA-256;
the artifact and receipt identities are derived from the release,
distribution, and content tuple. Changing a same-release label, locator,
distribution validator, or byte fingerprint is not silently accepted.

## State handling

| Probe state | Meaning | Rows | Acknowledged |
| --- | --- | ---: | ---: |
| `changed` | New release/content completed bounded CSV validation | present | yes |
| `no_op` | Conditional `304`; no body is consumed and the unchanged check is recorded | none | yes |
| `replayed` | Same accepted release/distribution/content supplied with prior receipt | same deterministic rows | yes |
| `drift` | Same release's semantic metadata/distribution/content conflicts, or response hash disagrees | none | no |
| `failed_probe` | Non-2xx/304 response, missing body, interrupted stream, or malformed CSV | none | no |

No-op, drift, and failed-probe results do not advance a cursor or imply a
custody write. A verified `304` no-op is acknowledged as a successful
conditional check, while still carrying no rows or artifact admission. A drift result may carry a content fingerprint for safe
diagnostics, but it never carries an artifact admission or source rows.
Replaying a completed result returns the same `receipt_id`, `artifact_id`,
`idempotency_key`, row IDs, and row fingerprints. That is evidence of
idempotency only; this module performs no duplicate suppression side effect.

## Authorized source preview

Rows are available on the result for the explicitly scoped caller, while
`authorized_source_preview(limit=N)` requires the producer to have been
constructed with `preview_authorized=True`. The preview is capped at 100 rows
and includes the source/distribution/receipt references, opaque source row ID,
`PRVDR_NUM`, selector, row fingerprint, and source fields. Without that flag,
the method raises `CmsPosAuthorizationError`. Preview authorization is not a
canonical identity or publication grant.

Example fixture-only invocation:

```python
from shared.adapters import ConditionalResponse
from shared.acquisition.cms_pos import (
    CmsPosDistribution,
    CmsPosProducer,
    CmsPosRelease,
    build_cms_pos_catalog,
)

producer = CmsPosProducer(build_cms_pos_catalog(), budget=producer_budget)
result = producer.produce(
    release,
    distribution,
    source_chunks,
    conditional_response=ConditionalResponse(200),
    preview_authorized=True,
)
preview = result.authorized_source_preview(limit=10)
```

Production callers must supply a reviewed budget and source receipt; this
example intentionally shows no client, secret, or persistence operation.

## Operational response

1. Record only the serialized receipt and metadata identities in an evidence
   ledger. Do not log `rows`, raw chunks, CSV values, credentials, or request
   headers.
2. For `changed`, pass the completed source-scoped result to the separately
   reviewed custody/admission workflow. Do not promote `PRVDR_NUM` to a
   Toolkit identity in this step.
3. For `no_op`, retain the prior accepted custody reference and leave the
   source cursor unchanged.
4. For `drift`, quarantine the candidate, compare release/distribution
   metadata and content fingerprints, and require an upstream review before
   retrying. Never replace the prior accepted artifact in place.
5. For `failed_probe`, retain the bounded failure code and retry only through
   the caller's reviewed scheduler/adapter policy. This module does not retry.

## Verification evidence

From the P1-19 worktree, run:

```bash
pytest -q tests/shared/test_cms_pos.py
ruff check shared/acquisition/cms_pos.py tests/shared/test_cms_pos.py
ruff format --check shared/acquisition/cms_pos.py tests/shared/test_cms_pos.py
pyright shared/acquisition/cms_pos.py tests/shared/test_cms_pos.py
python3 -m compileall -q shared/acquisition/cms_pos.py tests/shared/test_cms_pos.py
for fixture in contracts/healthcare-data-platform/cms-pos/v1/fixtures/*.json; do
  python3 -m jsonschema -i "$fixture" \
    contracts/healthcare-data-platform/cms-pos/v1/cms-pos.schema.json
done
git diff --check 8e042b5f0548a9410616a5fc199ab09cb352f18d..HEAD
```

The repository-wide `scripts/check-fast.sh` gate is run by the wave
coordinator against the merged candidate. There is no Alembic migration or
schema database change in this lane: the contract is an additive JSON Schema
and the producer is transport/storage neutral. A coordinator running an
Alembic check should record “not applicable — no migration files or persistence
paths changed.”

## Rollback and handoff

The additive range is:

- `7561420839edc3281d41f7a95070395b26335e6f` — mission packet;
- `58f583e` — producer, contract, and synthetic fixtures;
- `0417e66` — focused tests and typed-boundary/parser hardening.

To roll back before integration, omit or revert the reviewed P1-19 range and
disable its caller. Keep any externally held custody/evidence for audit; do
not delete source records or mutate existing CMS loaders, queues, databases,
or canonical projections. Re-enable only with the matching contract and
producer code under independent review.

Handoff must include the exact base/head SHAs, changed-path list, focused
command output, JSON Schema result for all four fixtures, `git diff --check`,
the no-network/no-persistence limitation, and this rollback range. No Grok
retry, Beads/Dolt wait, push, deployment, credential, or production action is
part of the P1-19 lane.

Owner: Luna Max direct writer

Status: implementation complete pending independent review
