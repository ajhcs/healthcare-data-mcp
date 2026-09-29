# P1-04 content-addressed raw custody

P1-04 is the immutable evidence seam between an authorized source probe and a
later parser/producer. It is intentionally not a consumer cache: a raw object
is addressed by its SHA-256 bytes, has a source/release-bound manifest, and is
never overwritten or deleted by this module. Query-serving projections and
retention/compaction policy belong to later lanes.

## Contract

`contracts/healthcare-data-platform/storage/v1/raw-artifact.schema.json` defines
`hdp.raw-artifact.v1`. A manifest binds:

- source ID and HTTPS source URL;
- source release ID, media type, capture time, and optional response fingerprint;
- exact byte length, chunk shape, and `sha256:` content fingerprint;
- a deterministic artifact ID and idempotency key derived from source, release,
  and content;
- approved-public rights and an optional prior artifact generation;
- a portable `objects/sha256/<prefix>/<digest>` locator plus metadata hash.

The writer rejects unclear, restricted, or blocked rights, malformed identities,
wrong hashes, unbounded input, path symlinks, and source-mismatched rollback
references. The configured ceiling is 131,072 bytes and 128 chunks, with a
maximum 65,536-byte chunk.

## Write and replay semantics

`RawArtifactStore.put()` accepts an already-authorized metadata object and an
iterable of bytes. Chunks are written to a metadata-bound partial file and its
atomic state sidecar. An interruption returns an `interrupted` receipt without
creating a final object or manifest; retrying the same metadata and prefix
resumes at the next chunk. A complete stream is hash-verified before the
partial file is atomically renamed into the content-addressed object path and
the manifest is atomically created.

Replaying the same artifact consumes and verifies the supplied bytes and returns
`duplicate` without rewriting either immutable file. Reusing an artifact or
partial identity with different metadata or bytes raises
`ArtifactCollisionError`. Finalized objects and manifests have no delete API;
tampered files fail hash or metadata verification on read.

`prior_artifact_id` must resolve to an existing artifact from the same source.
This preserves a recoverable generation chain: callers can select the prior
manifest and bytes during a later rollback without mutating or removing the
current evidence.

## Scope and next lanes

The implementation is local and synchronous for deterministic tests. It does
not perform network acquisition, queue acknowledgement, parsing, publication,
retention expiry, legal-hold evaluation, or production deployment. P1-07 will
attach AHRQ producer envelopes and durable acknowledgements; P1-16 will add
quota, reference-count, grace-period, and legal-hold lifecycle controls.
