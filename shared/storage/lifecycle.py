"""Recoverable lifecycle controls for immutable raw evidence.

``RawArtifactStore`` owns byte and manifest immutability.  This module owns
the mutable *lifecycle index* beside that store: retention class, references,
legal hold, grace/expiry timestamps, and a recoverable quarantine state.  A
compaction run may move an eligible object and manifest into quarantine, but it
never permanently deletes source evidence.  Restoration verifies the original
content hash before making the artifact visible again.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import Iterable, Literal, Mapping, TypeAlias, cast

from shared.storage.raw_custody import RawArtifactMetadata, RawArtifactStore, RawCustodyError


RetentionClass = Literal[
    "hot",
    "warm",
    "cold",
    "rebuildable",
    "append_only",
    "indefinite",
    "legal_hold",
]
LifecycleState = Literal["active", "quarantined"]
LifecycleAction = Literal[
    "registered",
    "already_registered",
    "referenced",
    "released",
    "legal_hold_set",
    "legal_hold_cleared",
    "quarantined",
    "restored",
]
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]

LIFECYCLE_SCHEMA_VERSION = "hdp.raw-lifecycle.v1"
LIFECYCLE_RECORD_TYPE = "raw_artifact_lifecycle"
LIFECYCLE_RECEIPT_RECORD_TYPE = "raw_artifact_lifecycle_receipt"
COMPACTION_RECORD_TYPE = "raw_artifact_compaction_receipt"
RETENTION_CLASSES: tuple[RetentionClass, ...] = (
    "hot",
    "warm",
    "cold",
    "rebuildable",
    "append_only",
    "indefinite",
    "legal_hold",
)
NON_EXPIRING_CLASSES = frozenset({"append_only", "indefinite", "legal_hold"})
MAX_REFERENCE_COUNT = 2**31 - 1


class LifecycleError(ValueError):
    """Raised when lifecycle metadata or a cleanup action is unsafe."""


class LifecycleConflictError(LifecycleError):
    """Raised when a lifecycle action would violate an immutable invariant."""


@dataclass(frozen=True, slots=True)
class LifecycleMetadata:
    """Mutable lifecycle metadata bound to one immutable artifact."""

    artifact_id: str
    content_sha256: str
    byte_length: int
    retention_class: RetentionClass
    reference_count: int
    legal_hold: bool
    created_at: str
    grace_until: str | None
    expires_at: str | None
    state: LifecycleState = "active"
    quarantined_at: str | None = None
    quarantine_manifest: str | None = None
    quarantine_object: str | None = None
    object_retained: bool = False

    def as_dict(self) -> dict[str, object]:
        """Return the schema-shaped lifecycle mapping."""

        return {
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "record_type": LIFECYCLE_RECORD_TYPE,
            "artifact_id": self.artifact_id,
            "content_sha256": self.content_sha256,
            "byte_length": self.byte_length,
            "retention_class": self.retention_class,
            "reference_count": self.reference_count,
            "legal_hold": self.legal_hold,
            "created_at": self.created_at,
            "grace_until": self.grace_until,
            "expires_at": self.expires_at,
            "state": self.state,
            "quarantined_at": self.quarantined_at,
            "quarantine_manifest": self.quarantine_manifest,
            "quarantine_object": self.quarantine_object,
            "object_retained": self.object_retained,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "LifecycleMetadata":
        """Parse strict lifecycle metadata and reject malformed values."""

        expected = {
            "schema_version",
            "record_type",
            "artifact_id",
            "content_sha256",
            "byte_length",
            "retention_class",
            "reference_count",
            "legal_hold",
            "created_at",
            "grace_until",
            "expires_at",
            "state",
            "quarantined_at",
            "quarantine_manifest",
            "quarantine_object",
            "object_retained",
        }
        unknown = set(value) - expected
        if unknown:
            raise LifecycleError(f"lifecycle metadata has unknown fields: {sorted(unknown)}")
        if value.get("schema_version") != LIFECYCLE_SCHEMA_VERSION:
            raise LifecycleError("lifecycle metadata schema_version is unsupported")
        if value.get("record_type") != LIFECYCLE_RECORD_TYPE:
            raise LifecycleError("lifecycle metadata record_type is unsupported")
        artifact_id = _required_text(value.get("artifact_id"), "artifact_id")
        content_sha256 = _sha256_text(value.get("content_sha256"), "content_sha256")
        byte_length = _bounded_int(value.get("byte_length"), "byte_length", 1, 2**63 - 1)
        retention_value = _required_text(value.get("retention_class"), "retention_class")
        if retention_value not in RETENTION_CLASSES:
            raise LifecycleError("retention_class is unsupported")
        reference_count = _bounded_int(value.get("reference_count"), "reference_count", 0, MAX_REFERENCE_COUNT)
        legal_hold = value.get("legal_hold")
        if not isinstance(legal_hold, bool):
            raise LifecycleError("legal_hold must be a boolean")
        created_at = _timestamp(value.get("created_at"), "created_at")
        grace_until = _optional_timestamp(value.get("grace_until"), "grace_until")
        expires_at = _optional_timestamp(value.get("expires_at"), "expires_at")
        state_value = _required_text(value.get("state"), "state")
        if state_value not in {"active", "quarantined"}:
            raise LifecycleError("state is unsupported")
        quarantined_at = _optional_timestamp(value.get("quarantined_at"), "quarantined_at")
        quarantine_manifest = _optional_text(value.get("quarantine_manifest"), "quarantine_manifest")
        quarantine_object = _optional_text(value.get("quarantine_object"), "quarantine_object")
        object_retained = value.get("object_retained")
        if not isinstance(object_retained, bool):
            raise LifecycleError("object_retained must be a boolean")
        if legal_hold and retention_value not in {"legal_hold", "append_only", "indefinite"}:
            # The original class is retained when an operator adds a hold;
            # registration may still set a temporary class before the hold.
            pass
        if state_value == "active" and (quarantined_at or quarantine_manifest or quarantine_object):
            raise LifecycleError("active lifecycle metadata cannot carry quarantine paths")
        if state_value == "quarantined" and quarantined_at is None:
            raise LifecycleError("quarantined lifecycle metadata requires quarantined_at")
        return cls(
            artifact_id=artifact_id,
            content_sha256=content_sha256,
            byte_length=byte_length,
            retention_class=cast(RetentionClass, retention_value),
            reference_count=reference_count,
            legal_hold=legal_hold,
            created_at=created_at,
            grace_until=grace_until,
            expires_at=expires_at,
            state=cast(LifecycleState, state_value),
            quarantined_at=quarantined_at,
            quarantine_manifest=quarantine_manifest,
            quarantine_object=quarantine_object,
            object_retained=object_retained,
        )


@dataclass(frozen=True, slots=True)
class LifecycleReceipt:
    """Secret-free receipt for a lifecycle mutation."""

    action: LifecycleAction
    artifact_id: str
    content_sha256: str
    reference_count: int
    legal_hold: bool
    state: LifecycleState
    detail: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "record_type": LIFECYCLE_RECEIPT_RECORD_TYPE,
            "action": self.action,
            "artifact_id": self.artifact_id,
            "content_sha256": self.content_sha256,
            "reference_count": self.reference_count,
            "legal_hold": self.legal_hold,
            "state": self.state,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class CompactionReceipt:
    """Receipt for one bounded, recoverable compaction pass."""

    quota_bytes: int
    before_bytes: int
    after_bytes: int
    quarantined_artifacts: tuple[str, ...]
    quarantined_orphans: tuple[str, ...]
    blocked_reason: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": LIFECYCLE_SCHEMA_VERSION,
            "record_type": COMPACTION_RECORD_TYPE,
            "quota_bytes": self.quota_bytes,
            "before_bytes": self.before_bytes,
            "after_bytes": self.after_bytes,
            "quarantined_artifacts": list(self.quarantined_artifacts),
            "quarantined_orphans": list(self.quarantined_orphans),
            "blocked_reason": self.blocked_reason,
        }


class RawArtifactLifecycle:
    """Reference-counted, legal-hold-aware lifecycle controller.

    Lifecycle metadata is mutable, but raw artifact bytes and manifests remain
    immutable.  Cleanup is always recoverable quarantine; no public method
    performs permanent deletion.
    """

    def __init__(
        self,
        root_or_store: str | Path | RawArtifactStore,
        *,
        quota_bytes: int = 16 * 1024 * 1024,
        default_grace: timedelta = timedelta(days=7),
    ) -> None:
        if isinstance(root_or_store, RawArtifactStore):
            self.store = root_or_store
        else:
            self.store = RawArtifactStore(root_or_store)
        if quota_bytes < 1:
            raise LifecycleError("quota_bytes must be positive")
        if default_grace < timedelta(0):
            raise LifecycleError("default_grace cannot be negative")
        self.root = self.store.root
        self.quota_bytes = quota_bytes
        self.default_grace = default_grace

    def register(
        self,
        artifact_id: str,
        *,
        retention_class: RetentionClass = "append_only",
        reference_count: int = 0,
        legal_hold: bool = False,
        grace_until: str | None = None,
        expires_at: str | None = None,
        now: str | None = None,
    ) -> LifecycleReceipt:
        """Register verified custody metadata idempotently."""

        item = self._verified_item(artifact_id)
        if retention_class not in RETENTION_CLASSES:
            raise LifecycleError("retention_class is unsupported")
        _bounded_int(reference_count, "reference_count", 0, MAX_REFERENCE_COUNT)
        current = _timestamp(now, "now") if now is not None else _utc_now()
        grace = _optional_timestamp(grace_until, "grace_until")
        expiry = _optional_timestamp(expires_at, "expires_at")
        if grace is None and retention_class not in NON_EXPIRING_CLASSES:
            grace = _format_timestamp(_parse_timestamp(current) + self.default_grace)
        if retention_class in NON_EXPIRING_CLASSES and expiry is not None:
            raise LifecycleError("non-expiring retention classes cannot carry expires_at")
        if grace is not None and expiry is not None and _parse_timestamp(grace) > _parse_timestamp(expiry):
            raise LifecycleError("grace_until cannot be after expires_at")
        existing = self._read_optional(artifact_id)
        if existing is not None:
            if existing.content_sha256 != item.content_sha256 or existing.byte_length != item.byte_length:
                raise LifecycleConflictError("lifecycle metadata does not match immutable artifact")
            return self._receipt("already_registered", existing, "registration is idempotent")
        metadata = LifecycleMetadata(
            artifact_id=artifact_id,
            content_sha256=item.content_sha256,
            byte_length=item.byte_length,
            retention_class=retention_class,
            reference_count=reference_count,
            legal_hold=legal_hold or retention_class == "legal_hold",
            created_at=current,
            grace_until=grace,
            expires_at=expiry,
        )
        self._write_metadata(metadata, overwrite=False)
        return self._receipt("registered", metadata)

    def read(self, artifact_id: str) -> LifecycleMetadata:
        """Read lifecycle metadata and verify it remains bound to custody."""

        metadata = self._read_optional(artifact_id)
        if metadata is None:
            raise LifecycleError("artifact has no lifecycle metadata")
        item = self._verified_item(artifact_id, allow_quarantined=True)
        if item.content_sha256 != metadata.content_sha256 or item.byte_length != metadata.byte_length:
            raise LifecycleConflictError("lifecycle metadata drifted from immutable custody")
        return metadata

    def add_reference(self, artifact_id: str, *, count: int = 1) -> LifecycleReceipt:
        """Increase the durable reference count before a consumer cites bytes."""

        delta = _bounded_int(count, "count", 1, MAX_REFERENCE_COUNT)
        metadata = self.read(artifact_id)
        if metadata.state != "active":
            raise LifecycleConflictError("quarantined evidence must be restored before citing it")
        if metadata.reference_count > MAX_REFERENCE_COUNT - delta:
            raise LifecycleError("reference_count would overflow")
        updated = replace(metadata, reference_count=metadata.reference_count + delta)
        self._write_metadata(updated, overwrite=True)
        return self._receipt("referenced", updated)

    def release_reference(self, artifact_id: str, *, count: int = 1) -> LifecycleReceipt:
        """Release references without allowing a negative count."""

        delta = _bounded_int(count, "count", 1, MAX_REFERENCE_COUNT)
        metadata = self.read(artifact_id)
        if delta > metadata.reference_count:
            raise LifecycleConflictError("reference_count cannot become negative")
        updated = replace(metadata, reference_count=metadata.reference_count - delta)
        self._write_metadata(updated, overwrite=True)
        return self._receipt("released", updated)

    def set_legal_hold(self, artifact_id: str, held: bool = True) -> LifecycleReceipt:
        """Set or clear a legal hold; held artifacts are never compacted."""

        if not isinstance(held, bool):
            raise LifecycleError("held must be a boolean")
        metadata = self.read(artifact_id)
        updated = replace(metadata, legal_hold=held)
        self._write_metadata(updated, overwrite=True)
        return self._receipt("legal_hold_set" if held else "legal_hold_cleared", updated)

    def compact(
        self,
        *,
        now: str | None = None,
        quota_bytes: int | None = None,
        max_artifacts: int | None = None,
    ) -> CompactionReceipt:
        """Quarantine eligible expired artifacts until the quota is satisfied.

        An artifact is eligible only when its policy has expired, its grace
        period has elapsed, its reference count is zero, and no legal hold is
        active.  Quarantine moves are recoverable and emit a local receipt.
        """

        quota = self.quota_bytes if quota_bytes is None else _bounded_int(quota_bytes, "quota_bytes", 1, 2**63 - 1)
        current = _parse_timestamp(_timestamp(now, "now")) if now is not None else datetime.now(timezone.utc)
        records = [metadata for path in self._metadata_paths() if (metadata := self._read_path(path)) is not None]
        before = sum(metadata.byte_length for metadata in records if metadata.state == "active")
        candidates = sorted(
            (metadata for metadata in records if self._eligible(metadata, current)),
            key=lambda value: (value.expires_at or value.created_at, value.created_at, value.artifact_id),
        )
        if max_artifacts is not None:
            _bounded_int(max_artifacts, "max_artifacts", 1, len(candidates) or 1)
            candidates = candidates[:max_artifacts]
        target_bytes = max(0, before - quota)
        selected: list[LifecycleMetadata] = []
        reclaimed = 0
        for candidate in candidates:
            # Expired rows are also compacted while within quota so the state
            # is explicit; quota pressure merely determines how many are
            # needed when the store is oversized.
            selected.append(candidate)
            reclaimed += candidate.byte_length
            if target_bytes and reclaimed >= target_bytes:
                break
            if not target_bytes and max_artifacts is None:
                continue
        quarantined: list[str] = []
        for candidate in selected:
            self._quarantine(candidate, current)
            quarantined.append(candidate.artifact_id)
        orphaned = self._quarantine_orphans(current)
        after = before - sum(
            next(metadata.byte_length for metadata in records if metadata.artifact_id == artifact_id)
            for artifact_id in quarantined
        )
        blocked = None
        if before > quota and after > quota:
            blocked = "quota remains above target because no additional eligible unreferenced evidence exists"
        receipt = CompactionReceipt(
            quota_bytes=quota,
            before_bytes=before,
            after_bytes=after,
            quarantined_artifacts=tuple(quarantined),
            quarantined_orphans=tuple(orphaned),
            blocked_reason=blocked,
        )
        self._write_run_receipt(receipt, current)
        return receipt

    def restore(self, artifact_id: str) -> LifecycleReceipt:
        """Restore one quarantined artifact and re-verify immutable bytes."""

        metadata = self.read(artifact_id)
        if metadata.state != "quarantined":
            return self._receipt("already_registered", metadata, "artifact is already active")
        if metadata.quarantine_manifest is None:
            raise LifecycleError("quarantined artifact is missing its manifest path")
        manifest_path = self._confined(self.root / metadata.quarantine_manifest)
        active_manifest = self._manifest_path(artifact_id)
        if active_manifest.exists():
            raise LifecycleConflictError("active manifest already exists during restore")
        object_path = self._object_path(metadata.content_sha256)
        object_moved = False
        if metadata.quarantine_object is not None:
            quarantine_object = self._confined(self.root / metadata.quarantine_object)
            if object_path.exists():
                raise LifecycleConflictError("active object already exists during restore")
            self._safe_move(quarantine_object, object_path)
            object_moved = True
        try:
            self._safe_move(manifest_path, active_manifest)
            restored = replace(
                metadata,
                state="active",
                quarantined_at=None,
                quarantine_manifest=None,
                quarantine_object=None,
                object_retained=False,
            )
            self._write_metadata(restored, overwrite=True)
            self.store.read_bytes(artifact_id)
        except Exception:
            if active_manifest.exists() and not active_manifest.is_symlink():
                self._safe_move(active_manifest, manifest_path)
            if object_moved and object_path.exists() and not object_path.is_symlink():
                self._safe_move(object_path, self._confined(self.root / cast(str, metadata.quarantine_object)))
            raise
        return self._receipt("restored", restored)

    def _eligible(self, metadata: LifecycleMetadata, now: datetime) -> bool:
        if metadata.state != "active" or metadata.reference_count != 0 or metadata.legal_hold:
            return False
        if metadata.retention_class in NON_EXPIRING_CLASSES or metadata.expires_at is None:
            return False
        eligible_at = _parse_timestamp(metadata.expires_at)
        if metadata.grace_until is not None:
            eligible_at = max(eligible_at, _parse_timestamp(metadata.grace_until))
        return now >= eligible_at

    def _quarantine(self, metadata: LifecycleMetadata, now: datetime) -> None:
        item = self._verified_item(metadata.artifact_id)
        if item.content_sha256 != metadata.content_sha256:
            raise LifecycleConflictError("quarantine candidate hash differs from lifecycle metadata")
        manifest = self._manifest_path(metadata.artifact_id)
        object_path = self._object_path(metadata.content_sha256)
        if manifest.is_symlink() or object_path.is_symlink():
            raise LifecycleConflictError("refusing to quarantine a symlinked custody path")
        if not manifest.exists() or not object_path.exists():
            raise LifecycleError("quarantine candidate is missing immutable custody")
        digest = _artifact_digest(metadata.artifact_id)
        destination = (
            self.root / "lifecycle" / "quarantine" / digest / _format_timestamp(now).replace(":", "").replace("Z", "")
        )
        destination.mkdir(parents=True, exist_ok=True)
        quarantine_manifest = destination / "manifest.json"
        quarantine_object = destination / "object"
        object_references = self._manifest_index().get(metadata.content_sha256, ())
        move_object = len(object_references) <= 1
        self._safe_move(manifest, quarantine_manifest)
        try:
            if move_object:
                self._safe_move(object_path, quarantine_object)
            updated = replace(
                metadata,
                state="quarantined",
                quarantined_at=_format_timestamp(now),
                quarantine_manifest=str(quarantine_manifest.relative_to(self.root)),
                quarantine_object=str(quarantine_object.relative_to(self.root)) if move_object else None,
                object_retained=not move_object,
            )
            self._write_metadata(updated, overwrite=True)
        except Exception:
            if quarantine_object.exists() and not quarantine_object.is_symlink():
                self._safe_move(quarantine_object, object_path)
            if quarantine_manifest.exists() and not quarantine_manifest.is_symlink():
                self._safe_move(quarantine_manifest, manifest)
            raise

    def _quarantine_orphans(self, now: datetime) -> list[str]:
        references = self._manifest_index()
        quarantined: list[str] = []
        objects_root = self.root / "objects" / "sha256"
        if not objects_root.exists():
            return quarantined
        for prefix in objects_root.iterdir():
            if prefix.is_symlink() or not prefix.is_dir():
                continue
            for object_path in prefix.iterdir():
                if object_path.is_symlink() or not object_path.is_file():
                    continue
                digest = object_path.name
                if digest in references:
                    continue
                destination = (
                    self.root
                    / "lifecycle"
                    / "quarantine"
                    / "orphans"
                    / f"{_format_timestamp(now).replace(':', '')}-{digest}"
                )
                destination.parent.mkdir(parents=True, exist_ok=True)
                self._safe_move(object_path, destination)
                quarantined.append(digest)
        return quarantined

    def _verified_item(self, artifact_id: str, *, allow_quarantined: bool = False) -> RawArtifactMetadata:
        try:
            return self.store.read_metadata(artifact_id)
        except RawCustodyError:
            if not allow_quarantined:
                raise LifecycleError("artifact is not in active custody")
            metadata = self._read_optional(artifact_id)
            if metadata is None or metadata.state != "quarantined":
                raise LifecycleError("artifact is not in verifiable lifecycle custody")
            if metadata.quarantine_manifest is None:
                raise LifecycleError("quarantined artifact has no manifest")
            manifest_path = self._confined(self.root / metadata.quarantine_manifest)
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
            artifact = value.get("artifact")
            if not isinstance(artifact, Mapping):
                raise LifecycleError("quarantined manifest has no artifact metadata")
            item = RawArtifactMetadata.from_mapping(artifact)
            if item.artifact_id != artifact_id:
                raise LifecycleConflictError("quarantined manifest identity mismatch")
            return item

    def _read_optional(self, artifact_id: str) -> LifecycleMetadata | None:
        path = self._lifecycle_path(artifact_id)
        if not path.exists():
            return None
        return self._read_path(path)

    def _read_path(self, path: Path) -> LifecycleMetadata | None:
        try:
            if path.is_symlink():
                raise LifecycleConflictError("refusing to follow symlinked lifecycle metadata")
            value = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(value, Mapping):
                raise LifecycleError("lifecycle metadata must be an object")
            return LifecycleMetadata.from_mapping(value)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise LifecycleError(f"unable to read lifecycle metadata: {path.name}") from exc

    def _write_metadata(self, metadata: LifecycleMetadata, *, overwrite: bool) -> None:
        _atomic_json(self._lifecycle_path(metadata.artifact_id), metadata.as_dict(), overwrite=overwrite)

    def _write_run_receipt(self, receipt: CompactionReceipt, now: datetime) -> None:
        name = f"{_format_timestamp(now).replace(':', '')}-{sha256(_canonical_json(receipt.as_dict())).hexdigest()[:16]}.json"
        _atomic_json(self.root / "lifecycle" / "receipts" / name, receipt.as_dict(), overwrite=False)

    def _receipt(
        self, action: LifecycleAction, metadata: LifecycleMetadata, detail: str | None = None
    ) -> LifecycleReceipt:
        return LifecycleReceipt(
            action=action,
            artifact_id=metadata.artifact_id,
            content_sha256=metadata.content_sha256,
            reference_count=metadata.reference_count,
            legal_hold=metadata.legal_hold,
            state=metadata.state,
            detail=detail,
        )

    def _metadata_paths(self) -> Iterable[Path]:
        directory = self.root / "lifecycle" / "metadata"
        if not directory.exists():
            return ()
        return tuple(path for path in directory.rglob("*.json") if not path.is_symlink())

    def _manifest_index(self) -> dict[str, tuple[tuple[str, Path], ...]]:
        result: dict[str, list[tuple[str, Path]]] = {}
        directory = self.root / "metadata"
        if not directory.exists():
            return {}
        for path in directory.rglob("*.json"):
            if path.is_symlink():
                continue
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
                artifact = value.get("artifact") if isinstance(value, Mapping) else None
                if not isinstance(artifact, Mapping):
                    continue
                artifact_id = artifact.get("artifact_id")
                digest = artifact.get("content_sha256")
                if isinstance(artifact_id, str) and isinstance(digest, str) and digest.startswith("sha256:"):
                    result.setdefault(digest.removeprefix("sha256:"), []).append((artifact_id, path))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
        return {key: tuple(value) for key, value in result.items()}

    def _lifecycle_path(self, artifact_id: str) -> Path:
        digest = _artifact_digest(artifact_id)
        return self.root / "lifecycle" / "metadata" / digest[:2] / f"{digest}.json"

    def _manifest_path(self, artifact_id: str) -> Path:
        digest = _artifact_digest(artifact_id)
        return self.root / "metadata" / digest[:2] / f"{digest}.json"

    def _object_path(self, content_sha256: str) -> Path:
        digest = content_sha256.removeprefix("sha256:")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise LifecycleError("content_sha256 is malformed")
        return self.root / "objects" / "sha256" / digest[:2] / digest

    def _confined(self, path: Path) -> Path:
        root = self.root.resolve()
        candidate = path.resolve(strict=False)
        if not candidate.is_relative_to(root):
            raise LifecycleConflictError("lifecycle path escapes the custody root")
        return candidate

    def _safe_move(self, source: Path, destination: Path) -> None:
        source = self._confined(source)
        destination = self._confined(destination)
        if source.is_symlink() or destination.is_symlink():
            raise LifecycleConflictError("refusing to move a symlinked lifecycle path")
        if not source.exists():
            raise LifecycleError(f"lifecycle source is missing: {source.name}")
        if destination.exists():
            raise LifecycleConflictError(f"lifecycle destination already exists: {destination.name}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        source.rename(destination)


# Name used by the delivery plan and by the future hosted worker.
RawObjectLifecycle = RawArtifactLifecycle


def _artifact_digest(artifact_id: str) -> str:
    if not isinstance(artifact_id, str) or not artifact_id.startswith("artifact:"):
        raise LifecycleError("artifact_id is malformed")
    return sha256(artifact_id.encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _atomic_json(path: Path, value: object, *, overwrite: bool) -> None:
    if path.is_symlink():
        raise LifecycleConflictError(f"refusing to follow symlinked path: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json(value)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        if overwrite:
            temporary.replace(path)
        else:
            os.link(temporary, path)
            temporary.unlink(missing_ok=True)
        _fsync_directory(path.parent)
    except FileExistsError as exc:
        temporary.unlink(missing_ok=True)
        raise LifecycleConflictError(f"refusing to overwrite immutable path: {path.name}") from exc
    except OSError:
        temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _required_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LifecycleError(f"{label} must be a non-empty string")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, label)


def _bounded_int(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise LifecycleError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _sha256_text(value: object, label: str) -> str:
    text = _required_text(value, label)
    if (
        not text.startswith("sha256:")
        or len(text) != 71
        or any(character not in "0123456789abcdef" for character in text[7:])
    ):
        raise LifecycleError(f"{label} must be a sha256 digest")
    return text


def _timestamp(value: object, label: str) -> str:
    text = _required_text(value, label)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LifecycleError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise LifecycleError(f"{label} must include a timezone")
    return _format_timestamp(parsed)


def _optional_timestamp(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _timestamp(value, label)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_now() -> str:
    return _format_timestamp(datetime.now(timezone.utc))


__all__ = [
    "CompactionReceipt",
    "LifecycleConflictError",
    "LifecycleError",
    "LifecycleMetadata",
    "LifecycleReceipt",
    "RawArtifactLifecycle",
    "RawObjectLifecycle",
    "RETENTION_CLASSES",
]
