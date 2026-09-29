"""Adversarial tests for recoverable raw-object lifecycle controls."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from shared.storage.lifecycle import (
    LifecycleConflictError,
    LifecycleError,
    LifecycleMetadata,
    RawArtifactLifecycle,
)
from shared.storage.raw_custody import ArtifactCollisionError, RawArtifactMetadata, RawArtifactStore


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "contracts/healthcare-data-platform/storage/v1/fixtures/valid-raw-artifact-lifecycle.json"
SCHEMA = ROOT / "contracts/healthcare-data-platform/storage/v1/raw-artifact-lifecycle.schema.json"
BODY = b'{"release":"2026-08-22","rows":[{"id":"facility-1"}]}\n'


def _metadata(*, release_id: str = "release:ahrq:2026-08-22") -> RawArtifactMetadata:
    content_sha256 = "sha256:" + hashlib.sha256(BODY).hexdigest()
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
        byte_length=len(BODY),
        chunk_count=(len(BODY) + chunk_size - 1) // chunk_size,
        chunk_size=chunk_size,
        idempotency_key=f"idempotency:raw:{identity}",
        captured_at="2026-08-28T23:00:00Z",
        rights_status="approved_public",
    )


def _chunks() -> list[bytes]:
    return [BODY[index : index + 16] for index in range(0, len(BODY), 16)]


def _stored(tmp_path: Path) -> tuple[RawArtifactStore, RawArtifactMetadata]:
    store = RawArtifactStore(tmp_path)
    metadata = _metadata()
    store.put(metadata, _chunks())
    return store, metadata


def test_lifecycle_fixture_matches_strict_schema() -> None:
    from jsonschema import Draft202012Validator, FormatChecker

    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(fixture)) == []
    assert LifecycleMetadata.from_mapping(fixture).legal_hold is True


def test_register_reference_and_legal_hold_write_secret_free_receipts(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)

    registered = lifecycle.register(
        metadata.artifact_id,
        retention_class="warm",
        expires_at="2026-09-01T00:00:00Z",
        grace_until="2026-09-02T00:00:00Z",
        now="2026-08-29T00:00:00Z",
    )
    referenced = lifecycle.add_reference(metadata.artifact_id)
    held = lifecycle.set_legal_hold(metadata.artifact_id)

    assert registered.action == "registered"
    assert referenced.reference_count == 1
    assert held.legal_hold is True
    receipt_files = list((tmp_path / "lifecycle" / "receipts").glob("*.json"))
    assert len(receipt_files) == 3
    for path in receipt_files:
        assert "facility-1" not in path.read_text(encoding="utf-8")


def test_referenced_or_held_evidence_survives_expired_compaction(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store, quota_bytes=1)
    lifecycle.register(
        metadata.artifact_id,
        retention_class="warm",
        reference_count=1,
        expires_at="2026-08-29T00:00:00Z",
        grace_until="2026-08-29T00:00:00Z",
        now="2026-08-28T00:00:00Z",
    )

    protected = lifecycle.compact(now="2026-09-01T00:00:00Z")

    assert protected.quarantined_artifacts == ()
    assert protected.blocked_reason is not None
    assert store.read_bytes(metadata.artifact_id) == BODY

    lifecycle.release_reference(metadata.artifact_id)
    lifecycle.set_legal_hold(metadata.artifact_id)
    still_held = lifecycle.compact(now="2026-09-02T00:00:00Z")
    assert still_held.quarantined_artifacts == ()
    assert store.read_bytes(metadata.artifact_id) == BODY


def test_grace_period_blocks_cleanup_then_quarantine_is_restorable(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store, quota_bytes=1)
    lifecycle.register(
        metadata.artifact_id,
        retention_class="cold",
        expires_at="2026-08-30T00:00:00Z",
        grace_until="2026-09-05T00:00:00Z",
        now="2026-08-29T00:00:00Z",
    )

    before_grace = lifecycle.compact(now="2026-09-01T00:00:00Z")
    assert before_grace.quarantined_artifacts == ()
    assert store.read_bytes(metadata.artifact_id) == BODY

    after_grace = lifecycle.compact(now="2026-09-06T00:00:00Z")
    assert after_grace.quarantined_artifacts == (metadata.artifact_id,)
    quarantined = lifecycle.read(metadata.artifact_id)
    assert quarantined.state == "quarantined"
    restored = lifecycle.restore(metadata.artifact_id)
    assert restored.action == "restored"
    assert lifecycle.read(metadata.artifact_id).state == "active"
    assert store.read_bytes(metadata.artifact_id) == BODY


def test_non_expiring_classes_reject_expiry_and_remain_active(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)

    with pytest.raises(LifecycleError, match="non-expiring"):
        lifecycle.register(metadata.artifact_id, retention_class="indefinite", expires_at="2026-01-01T00:00:00Z")
    registered = lifecycle.register(metadata.artifact_id, retention_class="append_only")
    result = lifecycle.compact(now="2099-01-01T00:00:00Z", quota_bytes=1)
    assert registered.state == "active"
    assert result.quarantined_artifacts == ()
    assert store.read_bytes(metadata.artifact_id) == BODY


def test_orphan_object_is_quarantined_recoverably(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)
    orphan_payload = b"orphan bytes\n"
    orphan_digest = hashlib.sha256(orphan_payload).hexdigest()
    orphan_path = tmp_path / "objects" / "sha256" / orphan_digest[:2] / orphan_digest
    orphan_path.parent.mkdir(parents=True)
    orphan_path.write_bytes(orphan_payload)

    result = lifecycle.compact(now="2026-08-29T00:00:00Z")

    assert result.quarantined_artifacts == ()
    assert result.quarantined_orphans == (orphan_digest,)
    assert not orphan_path.exists()
    assert list((tmp_path / "lifecycle" / "quarantine" / "orphans").glob(f"*-{orphan_digest}"))
    assert store.read_bytes(metadata.artifact_id) == BODY


def test_lifecycle_metadata_rejects_unknown_fields_bad_counters_and_symlinks(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)
    lifecycle.register(metadata.artifact_id)
    lifecycle_path = next((tmp_path / "lifecycle" / "metadata").rglob("*.json"))
    original = lifecycle_path.read_text(encoding="utf-8")
    value = json.loads(original)
    value["reference_count"] = True
    lifecycle_path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(LifecycleError, match="integer"):
        lifecycle.read(metadata.artifact_id)
    lifecycle_path.write_text(original, encoding="utf-8")

    outside = tmp_path / "outside.json"
    outside.write_text(original, encoding="utf-8")
    lifecycle_path.unlink()
    lifecycle_path.symlink_to(outside)
    with pytest.raises(LifecycleConflictError, match="symlink"):
        lifecycle.read(metadata.artifact_id)


def test_release_reference_cannot_underflow_and_registration_is_idempotent(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)
    first = lifecycle.register(metadata.artifact_id)
    again = lifecycle.register(metadata.artifact_id, retention_class="hot")
    assert again.action == "already_registered"
    assert first.content_sha256 == again.content_sha256
    with pytest.raises(LifecycleConflictError, match="negative"):
        lifecycle.release_reference(metadata.artifact_id)


def test_compaction_verifies_active_object_bytes_before_quarantine(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store, quota_bytes=1)
    lifecycle.register(
        metadata.artifact_id,
        retention_class="cold",
        expires_at="2026-08-29T00:00:00Z",
        grace_until="2026-08-29T00:00:00Z",
        now="2026-08-28T00:00:00Z",
    )
    object_path = (
        tmp_path / "objects" / "sha256" / hashlib.sha256(BODY).hexdigest()[:2] / hashlib.sha256(BODY).hexdigest()
    )
    object_path.write_bytes(b"tampered bytes\n")

    with pytest.raises(ArtifactCollisionError, match="immutable hash verification"):
        lifecycle.compact(now="2026-09-01T00:00:00Z")

    assert lifecycle.read(metadata.artifact_id).state == "active"


def test_shared_content_remains_recoverable_while_quarantine_records_retain_object(tmp_path: Path) -> None:
    store, first = _stored(tmp_path)
    second = _metadata(release_id="release:ahrq:2026-08-23")
    second_manifest = store._manifest_path(second.artifact_id)
    store._write_manifest(second, second_manifest, store._object_key(second.content_sha256))
    lifecycle = RawArtifactLifecycle(store, quota_bytes=len(BODY) + 1)
    for item in (first, second):
        lifecycle.register(
            item.artifact_id,
            retention_class="cold",
            expires_at="2026-08-29T00:00:00Z",
            grace_until="2026-08-29T00:00:00Z",
            now="2026-08-28T00:00:00Z",
        )

    result = lifecycle.compact(now="2026-09-01T00:00:00Z")

    assert set(result.quarantined_artifacts) == {first.artifact_id, second.artifact_id}
    assert lifecycle.read(first.artifact_id).object_retained is True
    assert lifecycle.read(second.artifact_id).object_retained is True
    assert lifecycle.restore(first.artifact_id).action == "restored"
    assert lifecycle.restore(second.artifact_id).action == "restored"
    assert store.read_bytes(first.artifact_id) == BODY
    assert store.read_bytes(second.artifact_id) == BODY


def test_lifecycle_rejects_parent_symlink_before_writing(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)
    outside = tmp_path.parent / f"{tmp_path.name}-outside"
    outside.mkdir()
    (tmp_path / "lifecycle").symlink_to(outside, target_is_directory=True)

    with pytest.raises(LifecycleConflictError, match="symlink"):
        lifecycle.register(metadata.artifact_id)
    assert not list(outside.rglob("*.json"))


def test_restore_rolls_lifecycle_metadata_back_when_verification_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store, quota_bytes=1)
    lifecycle.register(
        metadata.artifact_id,
        retention_class="cold",
        expires_at="2026-08-29T00:00:00Z",
        grace_until="2026-08-29T00:00:00Z",
        now="2026-08-28T00:00:00Z",
    )
    lifecycle.compact(now="2026-09-01T00:00:00Z")

    def fail_read(_artifact_id: str) -> bytes:
        raise ArtifactCollisionError("forced restore verification failure")

    monkeypatch.setattr(store, "read_bytes", fail_read)
    with pytest.raises(ArtifactCollisionError, match="forced restore"):
        lifecycle.restore(metadata.artifact_id)

    assert lifecycle.read(metadata.artifact_id).state == "quarantined"
    assert not store._manifest_path(metadata.artifact_id).exists()


def test_legal_hold_retention_cannot_be_cleared_without_policy_transition(tmp_path: Path) -> None:
    store, metadata = _stored(tmp_path)
    lifecycle = RawArtifactLifecycle(store)
    lifecycle.register(metadata.artifact_id, retention_class="legal_hold")

    with pytest.raises(LifecycleConflictError, match="legal_hold retention"):
        lifecycle.set_legal_hold(metadata.artifact_id, held=False)
