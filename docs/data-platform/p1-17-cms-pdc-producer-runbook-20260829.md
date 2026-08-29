# P1-17 CMS PDC producer runbook

This runbook describes the local, transport-neutral CMS Provider Data Catalog
(PDC) producer. It consumes caller-owned release metadata and byte chunks only;
it does not fetch CMS, write a cache, publish a queue message, advance a
cursor, or mutate a current projection.

## Inputs and catalog admission

Create a `CmsPdcCatalogEntry` from the approved source catalog. The entry must
carry `source:cms:pdc`, explicit stable CMS dataset and distribution IDs, HTTPS
source/distribution/release locators, approved-public rights, an expected
source-schema fingerprint, and byte/chunk/time limits within the adapter SDK
bound. Dataset and distribution display titles are descriptive metadata, never
identity.

```python
from shared.acquisition.cms_pdc import CmsPdcCatalogEntry, CmsPdcProducer

catalog = CmsPdcCatalogEntry(
    dataset_id="xubh-q36u",
    distribution_id="xubh-q36u-csv",
    dataset_title="Hospital General Information",
    distribution_title="Hospital General Information CSV",
    distribution_format="csv",
    source_url="https://data.cms.gov/provider-data/api/1/datastore/query",
    distribution_url="https://data.cms.gov/provider-data/sites/default/files/resources/xubh-q36u.csv",
    release_locator="https://data.cms.gov/provider-data",
    schema_fingerprint="sha256:<approved-column-schema-digest>",
)
producer = CmsPdcProducer(catalog)
```

For a source-neutral `AdapterCatalog`, use
`CmsPdcProducer.from_adapter_catalog(...)` and supply the stable distribution
metadata and schema fingerprint. A missing registration, disabled source, or
unapproved rights state fails before any stream is consumed.

## Release and change semantics

`CmsPdcRelease` binds the dataset/distribution IDs and URLs to a release ID,
modified timestamp, observed schema fingerprint, and adapter probe state. The
producer computes a semantic release fingerprint separately from the bounded
stream content fingerprint:

- `changed/release`: first observation or a new release with new content;
- `changed/content`: same release metadata with changed content;
- `no_op`: a conditional `not_modified` result or a new metadata release with
  identical content;
- `replayed`: the same release and content as an acknowledged prior receipt;
- `schema_drift`: observed schema fingerprint differs from the catalog baseline;
- `interrupted`: byte/chunk/deadline bound stopped an incomplete stream;
- `failed_probe`: the caller reported a failed conditional probe.

Pass the prior acknowledged receipt when retrying a release. A `not_modified`
probe requires that prior receipt and never consumes a second stream. Exact
replays are deterministic and carry the prior receipt ID. A changed content
claim is never silently treated as a duplicate.

## Bounded streaming

The producer delegates all byte consumption to `shared.adapters.stream_bounded`.
The SDK reports the exact digest, bytes, chunks, completion state, and
acknowledgement flag without retaining source payloads. An interrupted result
is unacknowledged and marks `current_projection_preserved=true`; the caller
may retry the same release from its own source/custody layer. A declared source
content fingerprint, when present, must match the completed stream or the
producer raises a validation error.

No callback is accepted by this producer, so source chunks cannot accidentally
be interpreted as a persistence or publication acknowledgement.

## Schema drift and recovery

Schema drift is checked before streaming. The returned receipt has
`state="schema_drift"`, `schema_state="drift"`, `stream_state="not_started"`,
`acknowledged=false`, and `current_projection_preserved=true`. Do not promote
or publish that receipt. Review the CMS catalog/distribution metadata, update
the approved schema baseline through the catalog owner, and retry as a new
release only after independent review. The checked-in
`valid-schema-drift.json` fixture demonstrates this path.

## Validation and rollback

Validate every emitted receipt with the checked-in
`contracts/healthcare-data-platform/cms-pdc/v1/cms-pdc.schema.json`. Receipts
contain only IDs, URLs, fingerprints, bounded counters, states, and a bounded
constant error category. Never place response bodies, rows, PHI, credentials,
query parameters, or stack traces in a release or receipt.

The focused suite covers changed/no-op/replay, metadata-only and content
changes, schema drift, failed probes, interrupted streams, catalog mismatch,
stable IDs, schema fixtures, and payload exclusion:

```bash
pytest -q tests/test_cms_pdc_producer.py
ruff check shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
ruff format --check shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
pyright shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
python -m compileall -q shared/acquisition/cms_pdc tests/test_cms_pdc_producer.py
python -m jsonschema -i contracts/healthcare-data-platform/cms-pdc/v1/fixtures/valid-changed.json contracts/healthcare-data-platform/cms-pdc/v1/cms-pdc.schema.json
git diff --check <base>..HEAD
```

Rollback is additive: revert or omit the P1-17 mission, contract, producer,
fixture, test, and runbook commits in reverse order. Because this lane has no
network, persistence, queue, cursor, projection, or deployment authority,
rollback requires no data repair or production action.
