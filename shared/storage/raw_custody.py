"""Bounded, immutable, content-addressed raw artifact custody.

The store is deliberately local and synchronous.  It is a reference seam for
the later hosted worker: callers provide already-authorized source metadata and
bounded chunks, while this module provides content hashes, atomic finalization,
resumable interruption, duplicate idempotency, and prior-generation lineage.
No delete or overwrite operation is exposed for finalized artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
import tempfile
from typing import Iterable, Literal, Mapping, TypeAlias, cast
from urllib.parse import urlparse


ArtifactState = Literal["stored", "duplicate", "interrupted"]
RightsStatus = Literal["approved_public", "terms_unclear", "restricted", "blocked"]
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]

MAX_ARTIFACT_BYTES = 131_072
MAX_ARTIFACT_CHUNKS = 128
MAX_CHUNK_BYTES = 65_536

_SCHEMA_VERSION = "hdp.raw-artifact.v1"
_RECORD_TYPE = "raw_artifact"
_MANIFEST_RECORD_TYPE = "raw_artifact_manifest"
_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:[a-z0-9][a-z0-9._:-]*$")
_ARTIFACT_ID = re.compile(r"^artifact:raw:[a-f0-9]{32}$")
_IDEMPOTENCY_KEY = re.compile(r"^idempotency:raw:[a-f0-9]{32}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_MEDIA_TYPE = re.compile(r"^[a-z0-9][a-z0-9!#$&^_.+*-]*/[a-z0-9][a-z0-9!#$&^_.+*-]*$")
_SCHEMA_PATH = (
    Path(__file__).resolve().parents[2] / "contracts/healthcare-data-platform/storage/v1/raw-artifact.schema.json"
)


class RawCustodyError(ValueError):
    """Raised when metadata, bytes, or custody lineage is unsafe."""


class ArtifactCollisionError(RawCustodyError):
    """Raised when an immutable artifact identity is reused with new content."""


@dataclass(frozen=True, slots=True)
class RawArtifactMetadata:
    """Source and byte claims required before a raw artifact can be stored."""

    schema_version: str
    record_type: str
    artifact_id: str
    source_id: str
    source_url: str
    release_id: str
    media_type: str
    content_sha256: str
    byte_length: int
    chunk_count: int
    chunk_size: int
    idempotency_key: str
    captured_at: str
    rights_status: RightsStatus
    prior_artifact_id: str | None = None
    response_fingerprint: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "RawArtifactMetadata":
        """Parse a strict metadata mapping without accepting unknown fields."""

        expected = {
            "schema_version",
            "record_type",
            "artifact_id",
            "source_id",
            "source_url",
            "release_id",
            "media_type",
            "content_sha256",
            "byte_length",
            "chunk_count",
            "chunk_size",
            "idempotency_key",
            "captured_at",
            "rights_status",
            "prior_artifact_id",
            "response_fingerprint",
        }
        unknown = set(value) - expected
        if unknown:
            raise RawCustodyError(f"raw artifact metadata has unknown fields: {sorted(unknown)}")

        schema_version = _required_string(value.get("schema_version"), "schema_version")
        record_type = _required_string(value.get("record_type"), "record_type")
        artifact_id = _required_string(value.get("artifact_id"), "artifact_id", _ARTIFACT_ID)
        source_id = _required_string(value.get("source_id"), "source_id", _SOURCE_ID)
        source_url = _https_url(value.get("source_url"), "source_url")
        release_id = _required_string(value.get("release_id"), "release_id", _RELEASE_ID)
        media_type = _required_string(value.get("media_type"), "media_type", _MEDIA_TYPE)
        content_sha256 = _required_string(value.get("content_sha256"), "content_sha256", _SHA256)
        byte_length = _bounded_int(value.get("byte_length"), "byte_length", 1, MAX_ARTIFACT_BYTES)
        chunk_count = _bounded_int(value.get("chunk_count"), "chunk_count", 1, MAX_ARTIFACT_CHUNKS)
        chunk_size = _bounded_int(value.get("chunk_size"), "chunk_size", 1, MAX_CHUNK_BYTES)
        idempotency_key = _required_string(value.get("idempotency_key"), "idempotency_key", _IDEMPOTENCY_KEY)
        captured_at = _timestamp(value.get("captured_at"), "captured_at")
        rights_value = _required_string(value.get("rights_status"), "rights_status")
        if rights_value not in {"approved_public", "terms_unclear", "restricted", "blocked"}:
            raise RawCustodyError("rights_status is unsupported")
        prior_artifact_id = _optional_string(value.get("prior_artifact_id"), "prior_artifact_id", _ARTIFACT_ID)
        response_fingerprint = _optional_string(value.get("response_fingerprint"), "response_fingerprint", _SHA256)
        return cls(
            schema_version=schema_version,
            record_type=record_type,
            artifact_id=artifact_id,
            source_id=source_id,
            source_url=source_url,
            release_id=release_id,
            media_type=media_type,
            content_sha256=content_sha256,
            byte_length=byte_length,
            chunk_count=chunk_count,
            chunk_size=chunk_size,
            idempotency_key=idempotency_key,
            captured_at=captured_at,
            rights_status=cast(RightsStatus, rights_value),
            prior_artifact_id=prior_artifact_id,
            response_fingerprint=response_fingerprint,
        )

    def as_dict(self) -> dict[str, object]:
        """Return stable JSON-ready metadata."""

        return {
            "schema_version": self.schema_version,
            "record_type": self.record_type,
            "artifact_id": self.artifact_id,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "release_id": self.release_id,
            "media_type": self.media_type,
            "content_sha256": self.content_sha256,
            "byte_length": self.byte_length,
            "chunk_count": self.chunk_count,
            "chunk_size": self.chunk_size,
            "idempotency_key": self.idempotency_key,
            "captured_at": self.captured_at,
            "rights_status": self.rights_status,
            "prior_artifact_id": self.prior_artifact_id,
            "response_fingerprint": self.response_fingerprint,
        }

    def canonical_bytes(self) -> bytes:
        """Return canonical bytes used for the sidecar metadata hash."""

        return _canonical_json(self.as_dict())


@dataclass(frozen=True, slots=True)
class CustodyReceipt:
    """Secret-free evidence for a store attempt."""

    state: ArtifactState
    artifact_id: str
    source_id: str
    release_id: str
    content_sha256: str
    byte_length: int
    chunk_count: int
    chunk_size: int
    idempotency_key: str
    object_key: str
    metadata_key: str
    prior_artifact_id: str | None
    resumed: bool

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe custody receipt."""

        return {
            "schema_version": _SCHEMA_VERSION,
            "record_type": "raw_custody_receipt",
            "state": self.state,
            "artifact_id": self.artifact_id,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "content_sha256": self.content_sha256,
            "byte_length": self.byte_length,
            "chunk_count": self.chunk_count,
            "chunk_size": self.chunk_size,
            "idempotency_key": self.idempotency_key,
            "object_key": self.object_key,
            "metadata_key": self.metadata_key,
            "prior_artifact_id": self.prior_artifact_id,
            "resumed": self.resumed,
        }


class RawArtifactStore:
    """Bounded local object store with immutable final artifacts."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_bytes: int = MAX_ARTIFACT_BYTES,
        max_chunks: int = MAX_ARTIFACT_CHUNKS,
    ) -> None:
        if max_bytes < 1 or max_bytes > MAX_ARTIFACT_BYTES:
            raise RawCustodyError("max_bytes is outside the bounded custody range")
        if max_chunks < 1 or max_chunks > MAX_ARTIFACT_CHUNKS:
            raise RawCustodyError("max_chunks is outside the bounded custody range")
        self.root = Path(root)
        self.max_bytes = max_bytes
        self.max_chunks = max_chunks

    def put(
        self,
        metadata: RawArtifactMetadata | Mapping[str, object],
        chunks: Iterable[bytes],
        *,
        interrupt_after_chunks: int | None = None,
    ) -> CustodyReceipt:
        """Store bounded chunks, resuming a matching partial attempt if present."""

        item = _coerce_metadata(metadata)
        self._validate_metadata(item)
        if interrupt_after_chunks is not None and interrupt_after_chunks < 1:
            raise RawCustodyError("interrupt_after_chunks must be positive")
        if item.prior_artifact_id is not None:
            self._validate_prior(item)

        object_path = self._object_path(item.content_sha256)
        manifest_path = self._manifest_path(item.artifact_id)
        object_key = self._object_key(item.content_sha256)
        metadata_key = self._metadata_key(item.artifact_id)
        if manifest_path.exists():
            return self._duplicate_or_collision(item, manifest_path, object_path, object_key, metadata_key, chunks)
        if object_path.exists() and object_path.is_symlink():
            raise ArtifactCollisionError("content-addressed object path is a symlink")

        partial_path, partial_state_path = self._partial_paths(item.idempotency_key)
        self._ensure_parent_dirs(object_path, manifest_path, partial_path, partial_state_path)
        existing_count, existing_bytes = self._load_partial(item, partial_path, partial_state_path)
        resumed = existing_count > 0
        received_count = existing_count
        received_bytes = existing_bytes
        processed_this_call = 0
        with partial_path.open("ab") as handle:
            previous_chunk: bytes | None = None
            for index, chunk in enumerate(chunks, start=1):
                self._validate_chunk(chunk, item, index)
                if previous_chunk is not None and len(previous_chunk) != item.chunk_size:
                    raise RawCustodyError("non-final chunks must use declared chunk_size")
                previous_chunk = chunk
                if index <= existing_count:
                    expected = _read_chunk(partial_path, item.chunk_size, index)
                    if expected != chunk:
                        raise ArtifactCollisionError("resume chunk differs from the immutable partial prefix")
                    continue
                handle.write(chunk)
                handle.flush()
                _fsync(handle)
                received_count += 1
                received_bytes += len(chunk)
                processed_this_call += 1
                if received_bytes > self.max_bytes or received_count > self.max_chunks:
                    raise RawCustodyError("artifact exceeds configured custody bounds")
                self._write_partial_state(partial_state_path, item, received_count, received_bytes)
                if (
                    interrupt_after_chunks is not None
                    and processed_this_call >= interrupt_after_chunks
                    and received_count < item.chunk_count
                ):
                    return self._receipt(
                        item,
                        "interrupted",
                        object_key,
                        metadata_key,
                        received_count,
                        item.chunk_size,
                        resumed,
                        received_bytes,
                    )

        if previous_chunk is not None and len(previous_chunk) > item.chunk_size:
            raise RawCustodyError("final chunk exceeds declared chunk_size")
        if received_count != item.chunk_count or received_bytes != item.byte_length:
            raise RawCustodyError("received chunks do not match declared artifact bounds")
        if _sha256_bytes(partial_path.read_bytes()) != item.content_sha256:
            raise ArtifactCollisionError("received bytes do not match content_sha256")

        if object_path.exists():
            if _sha256_bytes(object_path.read_bytes()) != item.content_sha256:
                raise ArtifactCollisionError("existing object bytes do not match content_sha256")
            partial_path.unlink(missing_ok=True)
        else:
            if object_path.is_symlink():
                raise ArtifactCollisionError("content-addressed object path is a symlink")
            partial_path.replace(object_path)
        self._write_manifest(item, manifest_path, object_key)
        partial_state_path.unlink(missing_ok=True)
        return self._receipt(
            item,
            "stored",
            object_key,
            metadata_key,
            item.chunk_count,
            item.chunk_size,
            resumed,
            item.byte_length,
        )

    def read_metadata(self, artifact_id: str) -> RawArtifactMetadata:
        """Read and verify one immutable artifact sidecar."""

        if _ARTIFACT_ID.fullmatch(artifact_id) is None:
            raise RawCustodyError("artifact_id is malformed")
        manifest_path = self._manifest_path(artifact_id)
        try:
            value = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RawCustodyError(f"unable to read artifact manifest: {artifact_id}") from exc
        if not isinstance(value, dict):
            raise RawCustodyError("artifact manifest must be an object")
        _validate_schema(value)
        artifact = value.get("artifact")
        if not isinstance(artifact, Mapping):
            raise RawCustodyError("artifact manifest is missing metadata")
        item = RawArtifactMetadata.from_mapping(artifact)
        if item.artifact_id != artifact_id:
            raise ArtifactCollisionError("artifact manifest identity does not match its path")
        expected_hash = _sha256_bytes(item.canonical_bytes())
        if value.get("metadata_sha256") != expected_hash:
            raise ArtifactCollisionError("artifact metadata hash does not match its content")
        if value.get("object_key") != self._object_key(item.content_sha256):
            raise ArtifactCollisionError("artifact object key does not match content hash")
        self._validate_metadata(item)
        return item

    def read_bytes(self, artifact_id: str) -> bytes:
        """Read and hash-verify finalized bytes for one artifact."""

        item = self.read_metadata(artifact_id)
        path = self._object_path(item.content_sha256)
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise RawCustodyError(f"unable to read artifact object: {artifact_id}") from exc
        if len(payload) != item.byte_length or _sha256_bytes(payload) != item.content_sha256:
            raise ArtifactCollisionError("artifact object failed immutable hash verification")
        return payload

    def _validate_metadata(self, item: RawArtifactMetadata) -> None:
        if item.schema_version != _SCHEMA_VERSION or item.record_type != _RECORD_TYPE:
            raise RawCustodyError("raw artifact metadata version or record_type is unsupported")
        if item.rights_status != "approved_public":
            raise RawCustodyError("raw artifact custody requires approved_public rights")
        expected_identity = _identity_digest(item.source_id, item.release_id, item.content_sha256)
        if item.artifact_id != f"artifact:raw:{expected_identity}":
            raise RawCustodyError("artifact_id is not content-addressed to source, release, and bytes")
        if item.idempotency_key != f"idempotency:raw:{expected_identity}":
            raise RawCustodyError("idempotency_key is not bound to artifact identity")
        if item.chunk_count != (item.byte_length + item.chunk_size - 1) // item.chunk_size:
            raise RawCustodyError("chunk_count does not match byte_length and chunk_size")
        if item.chunk_size > self.max_bytes or item.chunk_count > self.max_chunks or item.byte_length > self.max_bytes:
            raise RawCustodyError("artifact exceeds configured custody bounds")
        manifest = self._manifest_value(item, self._object_key(item.content_sha256))
        _validate_schema(manifest)

    def _validate_prior(self, item: RawArtifactMetadata) -> None:
        assert item.prior_artifact_id is not None
        if item.prior_artifact_id == item.artifact_id:
            raise RawCustodyError("prior_artifact_id must reference a different generation")
        try:
            prior = self.read_metadata(item.prior_artifact_id)
        except RawCustodyError as exc:
            raise RawCustodyError("prior_artifact_id does not reference durable custody") from exc
        if prior.source_id != item.source_id:
            raise RawCustodyError("prior artifact source_id does not match current artifact")

    def _duplicate_or_collision(
        self,
        item: RawArtifactMetadata,
        manifest_path: Path,
        object_path: Path,
        object_key: str,
        metadata_key: str,
        chunks: Iterable[bytes],
    ) -> CustodyReceipt:
        existing = self.read_metadata(item.artifact_id)
        if existing.as_dict() != item.as_dict():
            raise ArtifactCollisionError("artifact identity was reused with different metadata")
        payload = _consume_chunks(chunks, item, self.max_bytes, self.max_chunks)
        if _sha256_bytes(payload) != item.content_sha256:
            raise ArtifactCollisionError("idempotent replay bytes differ from content_sha256")
        if not object_path.exists() or _sha256_bytes(object_path.read_bytes()) != item.content_sha256:
            raise ArtifactCollisionError("finalized artifact object is missing or changed")
        return self._receipt(
            item,
            "duplicate",
            object_key,
            metadata_key,
            item.chunk_count,
            item.chunk_size,
            False,
            item.byte_length,
        )

    def _load_partial(self, item: RawArtifactMetadata, partial_path: Path, state_path: Path) -> tuple[int, int]:
        if partial_path.exists() != state_path.exists():
            raise RawCustodyError("partial object and partial state must exist together")
        if not partial_path.exists():
            return 0, 0
        try:
            raw_state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RawCustodyError("unable to read partial custody state") from exc
        if not isinstance(raw_state, dict) or raw_state.get("metadata") != item.as_dict():
            raise ArtifactCollisionError("partial custody state does not match artifact metadata")
        count = raw_state.get("chunk_count")
        byte_length = raw_state.get("byte_length")
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or isinstance(byte_length, bool)
            or not isinstance(byte_length, int)
        ):
            raise RawCustodyError("partial custody state has invalid counters")
        actual_length = partial_path.stat().st_size
        if count < 1 or count >= item.chunk_count or byte_length != actual_length:
            raise RawCustodyError("partial custody state counters are inconsistent")
        if byte_length != count * item.chunk_size:
            raise RawCustodyError("partial custody state must end on a full chunk")
        return count, byte_length

    def _write_partial_state(self, path: Path, item: RawArtifactMetadata, count: int, byte_length: int) -> None:
        _atomic_write_json(
            path,
            {
                "schema_version": _SCHEMA_VERSION,
                "record_type": "raw_artifact_partial",
                "metadata": item.as_dict(),
                "chunk_count": count,
                "byte_length": byte_length,
            },
            overwrite=True,
        )

    def _write_manifest(self, item: RawArtifactMetadata, path: Path, object_key: str) -> None:
        if path.exists():
            existing = self.read_metadata(item.artifact_id)
            if existing.as_dict() != item.as_dict():
                raise ArtifactCollisionError("artifact manifest overwrite would change immutable metadata")
            return
        if path.is_symlink():
            raise ArtifactCollisionError("artifact manifest path is a symlink")
        _atomic_write_json(path, self._manifest_value(item, object_key), overwrite=False)

    def _manifest_value(self, item: RawArtifactMetadata, object_key: str) -> dict[str, object]:
        return {
            "schema_version": _SCHEMA_VERSION,
            "record_type": _MANIFEST_RECORD_TYPE,
            "artifact": item.as_dict(),
            "object_key": object_key,
            "metadata_sha256": _sha256_bytes(item.canonical_bytes()),
        }

    def _receipt(
        self,
        item: RawArtifactMetadata,
        state: ArtifactState,
        object_key: str,
        metadata_key: str,
        chunk_count: int,
        chunk_size: int,
        resumed: bool,
        received_bytes: int,
    ) -> CustodyReceipt:
        return CustodyReceipt(
            state=state,
            artifact_id=item.artifact_id,
            source_id=item.source_id,
            release_id=item.release_id,
            content_sha256=item.content_sha256,
            byte_length=received_bytes,
            chunk_count=chunk_count,
            chunk_size=chunk_size,
            idempotency_key=item.idempotency_key,
            object_key=object_key,
            metadata_key=metadata_key,
            prior_artifact_id=item.prior_artifact_id,
            resumed=resumed,
        )

    def _validate_chunk(self, chunk: object, item: RawArtifactMetadata, index: int) -> None:
        if not isinstance(chunk, bytes) or not chunk:
            raise RawCustodyError("artifact chunks must be non-empty bytes")
        if len(chunk) > item.chunk_size or index > self.max_chunks:
            raise RawCustodyError("artifact chunk exceeds configured custody bounds")

    def _ensure_parent_dirs(self, *paths: Path) -> None:
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)

    def _object_path(self, content_sha256: str) -> Path:
        digest = content_sha256.removeprefix("sha256:")
        return self.root / "objects" / "sha256" / digest[:2] / digest

    def _manifest_path(self, artifact_id: str) -> Path:
        digest = sha256(artifact_id.encode("utf-8")).hexdigest()
        return self.root / "metadata" / digest[:2] / f"{digest}.json"

    def _partial_paths(self, idempotency_key: str) -> tuple[Path, Path]:
        digest = sha256(idempotency_key.encode("utf-8")).hexdigest()
        base = self.root / "partials" / digest[:2] / digest
        return base.with_suffix(".part"), base.with_suffix(".state.json")

    def _object_key(self, content_sha256: str) -> str:
        digest = content_sha256.removeprefix("sha256:")
        return f"objects/sha256/{digest[:2]}/{digest}"

    def _metadata_key(self, artifact_id: str) -> str:
        digest = sha256(artifact_id.encode("utf-8")).hexdigest()
        return f"metadata/{digest[:2]}/{digest}.json"


def _coerce_metadata(value: RawArtifactMetadata | Mapping[str, object]) -> RawArtifactMetadata:
    if isinstance(value, RawArtifactMetadata):
        return value
    return RawArtifactMetadata.from_mapping(value)


def _identity_digest(source_id: str, release_id: str, content_sha256: str) -> str:
    material = f"{source_id}|{release_id}|{content_sha256}".encode("utf-8")
    return sha256(material).hexdigest()[:32]


def _consume_chunks(
    chunks: Iterable[bytes],
    item: RawArtifactMetadata,
    max_bytes: int,
    max_chunks: int,
) -> bytes:
    values: list[bytes] = []
    total = 0
    for index, chunk in enumerate(chunks, start=1):
        if not isinstance(chunk, bytes) or not chunk or len(chunk) > item.chunk_size:
            raise RawCustodyError("artifact chunks must be bounded non-empty bytes")
        if index > max_chunks:
            raise RawCustodyError("artifact exceeds configured chunk bound")
        if values and len(values[-1]) != item.chunk_size:
            raise RawCustodyError("non-final chunks must use declared chunk_size")
        values.append(chunk)
        total += len(chunk)
        if total > max_bytes:
            raise RawCustodyError("artifact exceeds configured byte bound")
    if not values or len(values) != item.chunk_count or total != item.byte_length:
        raise RawCustodyError("replayed chunks do not match declared artifact bounds")
    if len(values) > 1 and len(values[-1]) > item.chunk_size:
        raise RawCustodyError("final chunk exceeds declared chunk_size")
    return b"".join(values)


def _read_chunk(path: Path, chunk_size: int, index: int) -> bytes:
    with path.open("rb") as handle:
        handle.seek((index - 1) * chunk_size)
        return handle.read(chunk_size)


def _canonical_json(value: object) -> bytes:
    try:
        return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    except UnicodeError as exc:
        raise RawCustodyError("metadata contains malformed Unicode") from exc


def _atomic_write_json(path: Path, value: object, *, overwrite: bool) -> None:
    payload = _canonical_json(value)
    if path.exists() and not overwrite:
        raise ArtifactCollisionError(f"refusing to overwrite immutable path: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as handle:
        temp_path = Path(handle.name)
        handle.write(payload)
        handle.flush()
        _fsync(handle)
    try:
        temp_path.replace(path)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise


def _fsync(handle: object) -> None:
    fileno = getattr(handle, "fileno", None)
    if callable(fileno):
        import os

        os.fsync(cast(int, fileno()))


def _validate_schema(value: Mapping[str, object]) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:  # pragma: no cover - dependency installation failure
        raise RawCustodyError("jsonschema is required for raw custody validation") from exc
    try:
        schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RawCustodyError("unable to read raw artifact schema") from exc
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, value)),
        key=lambda error: str(error.absolute_path),
    )
    if errors:
        raise RawCustodyError(f"raw artifact manifest failed schema validation: {errors[0].message}")


def _required_string(value: object, label: str, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value:
        raise RawCustodyError(f"{label} must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise RawCustodyError(f"{label} contains malformed Unicode") from exc
    if pattern is not None and pattern.fullmatch(value) is None:
        raise RawCustodyError(f"{label} has an invalid format")
    return value


def _optional_string(value: object, label: str, pattern: re.Pattern[str] | None = None) -> str | None:
    if value is None:
        return None
    return _required_string(value, label, pattern)


def _bounded_int(value: object, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise RawCustodyError(f"{label} must be an integer between {minimum} and {maximum}")
    return value


def _https_url(value: object, label: str) -> str:
    result = _required_string(value, label)
    parsed = urlparse(result)
    if parsed.scheme != "https" or not parsed.netloc:
        raise RawCustodyError(f"{label} must use HTTPS")
    return result


def _timestamp(value: object, label: str) -> str:
    raw = _required_string(value, label)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RawCustodyError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise RawCustodyError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


__all__ = [
    "ArtifactCollisionError",
    "CustodyReceipt",
    "RawArtifactMetadata",
    "RawArtifactStore",
    "RawCustodyError",
]
