"""Contract and interruption tests for immutable raw custody."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from shared.storage.raw_custody import (
    ArtifactCollisionError,
    RawArtifactMetadata,
    RawArtifactStore,
    RawCustodyError,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "contracts/healthcare-data-platform/storage/v1/fixtures/valid-raw-artifact.json"
SCHEMA = ROOT / "contracts/healthcare-data-platform/storage/v1/raw-artifact.schema.json"
BODY = b'{"release":"2026-08-22","rows":[{"id":"facility-1"}]}\n'


def _metadata(*, release_id: str = "release:ahrq:2026-08-22", body: bytes = BODY) -> RawArtifactMetadata:
    content_sha256 = "sha256:" + hashlib.sha256(body).hexdigest()
    identity = hashlib.sha256(f"source:ahrq:lighthouse|{release_id}|{content_sha256}".encode()).hexdigest()[:32]
    chunk_size = 16
    return RawArtifactMetadata(
        schema_version="hdp.raw-artifact.v1",
        record_type="raw_artifact",
        artifact_id=f"artifact:raw:{identity}",
        source_id="source:ahrq:lighthouse",
        source_url="https://example.org/ahrq/release.json",
        release_id=release_id,
        media_type="application/json",
        content_sha256=content_sha256,
        byte_length=len(body),
        chunk_count=(len(body) + chunk_size - 1) // chunk_size,
        chunk_size=chunk_size,
        idempotency_key=f"idempotency:raw:{identity}",
        captured_at="2026-08-28T23:00:00Z",
        rights_status="approved_public",
    )


def _chunks(body: bytes = BODY, *, size: int = 16) -> list[bytes]:
    return [body[index : index + size] for index in range(0, len(body), size)]


def test_valid_manifest_fixture_matches_strict_schema() -> None:
    from jsonschema import Draft202012Validator, FormatChecker

    value = json.loads(FIXTURE.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    assert list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)) == []
    assert RawArtifactMetadata.from_mapping(value["artifact"]).artifact_id == value["artifact"]["artifact_id"]


def test_store_writes_immutable_object_and_manifest_and_replays_idempotently(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)

    first = store.put(metadata, _chunks())
    duplicate = store.put(metadata, _chunks())

    assert first.state == "stored"
    assert duplicate.state == "duplicate"
    assert first.object_key == duplicate.object_key
    assert first.metadata_key == duplicate.metadata_key
    assert store.read_bytes(metadata.artifact_id) == BODY
    assert store.read_metadata(metadata.artifact_id) == metadata
    assert json.dumps(duplicate.as_dict(), sort_keys=True)


def test_same_identity_with_different_bytes_is_rejected(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    store.put(metadata, _chunks())

    with pytest.raises(ArtifactCollisionError, match="bytes differ"):
        store.put(metadata, _chunks(BODY.replace(b"facility-1", b"facility-2")))


def test_metadata_collision_cannot_overwrite_finalized_sidecar(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    store.put(metadata, _chunks())
    changed_metadata = replace(metadata, response_fingerprint="sha256:" + "1" * 64)

    with pytest.raises(ArtifactCollisionError, match="different metadata"):
        store.put(changed_metadata, _chunks())


def test_interruption_leaves_partial_and_retry_resumes_without_duplicate_bytes(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    chunks = _chunks()

    interrupted = store.put(metadata, chunks, interrupt_after_chunks=1)
    assert interrupted.state == "interrupted"
    assert interrupted.byte_length == len(chunks[0])
    assert interrupted.chunk_count == 1
    assert not [path for path in (tmp_path / "objects").rglob("*") if path.is_file()]

    completed = store.put(metadata, chunks)
    assert completed.state == "stored"
    assert completed.resumed is True
    assert store.read_bytes(metadata.artifact_id) == BODY
    assert not list((tmp_path / "partials").rglob("*.part"))


def test_resume_prefix_mismatch_is_fail_closed(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    store.put(metadata, _chunks(), interrupt_after_chunks=1)
    changed = list(_chunks())
    changed[0] = b"x" * len(changed[0])

    with pytest.raises(ArtifactCollisionError, match="partial prefix"):
        store.put(metadata, changed)


def test_interruption_at_final_chunk_finalizes_atomically(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)

    receipt = store.put(metadata, _chunks(), interrupt_after_chunks=metadata.chunk_count)

    assert receipt.state == "stored"
    assert store.read_bytes(metadata.artifact_id) == BODY


def test_prior_generation_is_required_and_bound_to_same_source(tmp_path: Path) -> None:
    first = _metadata()
    second = _metadata(release_id="release:ahrq:2026-08-29", body=BODY + b" ")
    second = replace(second, prior_artifact_id=first.artifact_id)
    store = RawArtifactStore(tmp_path)
    store.put(first, _chunks())

    receipt = store.put(second, _chunks(BODY + b" "))

    assert receipt.prior_artifact_id == first.artifact_id
    assert store.read_metadata(second.artifact_id).prior_artifact_id == first.artifact_id


def test_prior_generation_bytes_must_still_exist_and_match(tmp_path: Path) -> None:
    first = _metadata()
    second = replace(
        _metadata(release_id="release:ahrq:2026-08-29", body=BODY + b" "),
        prior_artifact_id=first.artifact_id,
    )
    store = RawArtifactStore(tmp_path)
    store.put(first, _chunks())
    prior_object = tmp_path / store._object_key(first.content_sha256)  # noqa: SLF001
    prior_object.unlink()

    with pytest.raises(RawCustodyError, match="verified durable custody"):
        store.put(second, _chunks(BODY + b" "))


def test_missing_or_foreign_prior_generation_is_rejected(tmp_path: Path) -> None:
    metadata = _metadata()
    missing = replace(metadata, prior_artifact_id="artifact:raw:" + "a" * 32)
    store = RawArtifactStore(tmp_path)

    with pytest.raises(RawCustodyError, match="prior_artifact_id"):
        store.put(missing, _chunks())

    first = _metadata()
    store.put(first, _chunks())
    foreign = replace(
        _metadata(release_id="release:ahrq:2026-08-29", body=BODY + b" "),
        source_id="source:cms:dataset",
        prior_artifact_id=first.artifact_id,
    )
    with pytest.raises(RawCustodyError, match="content-addressed|source_id"):
        store.put(foreign, _chunks(BODY + b" "))


def test_restricted_rights_are_not_written_to_custody(tmp_path: Path) -> None:
    metadata = replace(_metadata(), rights_status="restricted")

    with pytest.raises(RawCustodyError, match="approved_public"):
        RawArtifactStore(tmp_path).put(metadata, _chunks())


def test_bounds_and_chunk_shape_are_enforced(tmp_path: Path) -> None:
    metadata = _metadata()

    with pytest.raises(RawCustodyError, match="configured custody bounds"):
        RawArtifactStore(tmp_path, max_bytes=len(BODY) - 1).put(metadata, _chunks())
    with pytest.raises(RawCustodyError, match="non-final chunks"):
        RawArtifactStore(tmp_path).put(metadata, [BODY[:8], BODY[8:16], BODY[16:32], BODY[32:]])
    with pytest.raises(RawCustodyError, match="non-empty bytes"):
        RawArtifactStore(tmp_path / "empty").put(metadata, [b""])


def test_metadata_identity_and_schema_fields_are_strict(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)

    with pytest.raises(RawCustodyError, match="content-addressed"):
        store.put(replace(metadata, artifact_id="artifact:raw:" + "a" * 32), _chunks())
    with pytest.raises(RawCustodyError, match="unknown fields"):
        RawArtifactMetadata.from_mapping({**metadata.as_dict(), "unexpected": True})
    with pytest.raises(RawCustodyError, match="HTTPS"):
        store.put(replace(metadata, source_url="http://example.org/release.json"), _chunks())


def test_tampered_object_and_manifest_fail_verification(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    receipt = store.put(metadata, _chunks())
    object_path = tmp_path / receipt.object_key
    object_path.write_bytes(BODY.replace(b"facility-1", b"facility-9"))

    with pytest.raises(ArtifactCollisionError, match="immutable hash"):
        store.read_bytes(metadata.artifact_id)

    manifest_path = tmp_path / receipt.metadata_key
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["metadata_sha256"] = "sha256:" + "f" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ArtifactCollisionError, match="metadata hash"):
        store.read_metadata(metadata.artifact_id)


def test_finalized_object_symlink_is_rejected(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    receipt = store.put(metadata, _chunks())
    object_path = tmp_path / receipt.object_key
    object_path.unlink()
    outside = tmp_path / "outside-object"
    outside.write_bytes(BODY)
    object_path.symlink_to(outside)

    with pytest.raises(ArtifactCollisionError, match="symlink"):
        store.read_bytes(metadata.artifact_id)


def test_crash_after_object_promotion_is_recovered_without_rewrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    original = store._write_manifest  # noqa: SLF001

    def fail_after_promotion(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated manifest crash")

    monkeypatch.setattr(store, "_write_manifest", fail_after_promotion)
    with pytest.raises(RuntimeError, match="manifest crash"):
        store.put(metadata, _chunks())
    object_path = tmp_path / "objects" / "sha256" / metadata.content_sha256[7:9] / metadata.content_sha256[7:]
    assert object_path.read_bytes() == BODY
    assert not list((tmp_path / "partials").rglob("*.part"))

    monkeypatch.setattr(store, "_write_manifest", original)
    recovered = store.put(metadata, _chunks())
    assert recovered.state == "stored"
    assert recovered.resumed is True
    assert store.read_bytes(metadata.artifact_id) == BODY


def test_unmarked_orphan_object_cannot_recreate_altered_metadata(tmp_path: Path) -> None:
    metadata = _metadata()
    store = RawArtifactStore(tmp_path)
    receipt = store.put(metadata, _chunks())
    (tmp_path / receipt.metadata_key).unlink()
    altered = replace(metadata, source_url="https://evil.example/altered.json")

    with pytest.raises(ArtifactCollisionError, match="recovery state marker"):
        store.put(altered, _chunks())
