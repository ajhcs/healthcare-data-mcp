"""Strict CMS PDC catalog, release, and receipt contracts.

The contract layer contains no transport or persistence.  It gives the
producer a stable dataset/distribution identity, a semantic release boundary,
and a schema-validated, payload-free receipt vocabulary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
from pathlib import Path
import re
from typing import Literal, Mapping, TypeAlias, cast
from urllib.parse import urlsplit

from shared.adapters import AdapterCatalogEntry, AdapterChangeMode
from shared.adapters.bounds import MAX_STREAM_BYTES, MAX_STREAM_CHUNKS, MAX_STREAM_SECONDS
from shared.adapters.contracts import RightsStatus


CMS_PDC_SOURCE_ID = "source:cms:pdc"
CMS_PDC_SCHEMA_VERSION = "hdp.cms-pdc.v1"
CMS_PDC_RECORD_TYPE = "cms_pdc_receipt"
MAX_ID_LENGTH = 200
MAX_TITLE_LENGTH = 200
MAX_BYTES = 1_073_741_824
MAX_CHUNKS = 1_000_000
SAFE_ERROR_CATEGORIES = frozenset({"schema_drift", "probe_failed", "stream_interrupted"})

CmsPdcState: TypeAlias = Literal["changed", "no_op", "schema_drift", "replayed", "interrupted", "failed_probe"]
CmsPdcChangeKind: TypeAlias = Literal["release", "content", "none", "schema_drift", "replay", "interrupted", "failed"]
CmsPdcSchemaState: TypeAlias = Literal["valid", "drift", "not_checked"]
CmsPdcStreamState: TypeAlias = Literal["completed", "interrupted", "not_started"]
ProbeState: TypeAlias = Literal["changed", "not_modified", "failed_probe"]
DistributionFormat: TypeAlias = Literal["csv", "json", "parquet"]
CmsPdcErrorCategory: TypeAlias = Literal["schema_drift", "probe_failed", "stream_interrupted"]
JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]

_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:cms:pdc:[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_RECEIPT_ID = re.compile(r"^receipt:cms:pdc:[0-9a-f]{64}$")
_IDEMPOTENCY_KEY = re.compile(r"^replay:cms:pdc:[0-9a-f]{64}$")
_URL = re.compile(r"^https://[A-Za-z0-9._:/-]+$")


class CmsPdcError(ValueError):
    """Raised when a CMS PDC contract or producer input is unsafe."""


def _text(value: object, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CmsPdcError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise CmsPdcError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise CmsPdcError(f"{label} contains a control character")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise CmsPdcError(f"{label} contains malformed Unicode") from exc
    return value


def _id(value: object, label: str) -> str:
    result = _text(value, label, MAX_ID_LENGTH)
    if _ID.fullmatch(result) is None:
        raise CmsPdcError(f"{label} has an invalid stable identifier")
    return result


def _release_id(value: object, label: str = "release_id") -> str:
    result = _text(value, label, MAX_ID_LENGTH)
    if _RELEASE_ID.fullmatch(result) is None:
        raise CmsPdcError(f"{label} has an invalid format")
    return result


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise CmsPdcError(f"{label} must be a sha256 fingerprint")
    return value


def _optional_sha256(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _sha256(value, label)


def _https(value: object, label: str) -> str:
    result = _text(value, label, 2048)
    if _URL.fullmatch(result) is None:
        raise CmsPdcError(f"{label} must be a bounded HTTPS URL without query parameters")
    try:
        parsed = urlsplit(result)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise CmsPdcError(f"{label} has a malformed authority") from exc
    if parsed.scheme != "https" or not parsed.netloc or hostname is None or parsed.username or parsed.password:
        raise CmsPdcError(f"{label} has a malformed authority")
    return result


def _timestamp(value: object, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CmsPdcError(f"{label} must be an ISO-8601 timestamp") from exc
    else:
        raise CmsPdcError(f"{label} must be an ISO-8601 timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CmsPdcError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _format_timestamp(value: datetime) -> str:
    return _timestamp(value, "timestamp").isoformat().replace("+00:00", "Z")


def _bounded_int(value: object, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise CmsPdcError(f"{label} must be an integer between 0 and {maximum}")
    return value


def _canonical(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CmsPdcError("CMS PDC contract material is not canonical JSON") from exc


def _schema_path() -> Path:
    return Path(__file__).resolve().parents[3] / "contracts/healthcare-data-platform/cms-pdc/v1/cms-pdc.schema.json"


def validate_cms_pdc_receipt(value: Mapping[str, object]) -> dict[str, object]:
    """Validate one receipt against the checked-in CMS PDC v1 schema."""

    payload = dict(value)
    try:
        from jsonschema import Draft202012Validator, FormatChecker

        schema = json.loads(_schema_path().read_text(encoding="utf-8"))
        errors = sorted(
            Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, payload)),
            key=lambda error: str(error.absolute_path),
        )
    except ImportError as exc:  # pragma: no cover - dependency setup failure
        raise CmsPdcError("jsonschema is required for CMS PDC validation") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CmsPdcError("unable to read CMS PDC schema") from exc
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.absolute_path)
        suffix = f" at {location}" if location else ""
        raise CmsPdcError(f"CMS PDC receipt failed schema validation{suffix}: {error.message}")
    return payload


@dataclass(frozen=True, slots=True)
class CmsPdcCatalogEntry:
    """Validated stable CMS PDC registration and stream limits."""

    dataset_id: str
    distribution_id: str
    dataset_title: str
    distribution_title: str
    distribution_format: DistributionFormat
    source_url: str
    distribution_url: str
    release_locator: str
    change_mode: AdapterChangeMode = "etag"
    rights_status: RightsStatus = "approved_public"
    enabled: bool = True
    schema_fingerprint: str = "sha256:" + "0" * 64
    max_bytes: int = MAX_STREAM_BYTES
    max_chunks: int = MAX_STREAM_CHUNKS
    max_seconds: float = MAX_STREAM_SECONDS
    source_id: str = CMS_PDC_SOURCE_ID

    def __post_init__(self) -> None:
        if self.source_id != CMS_PDC_SOURCE_ID:
            raise CmsPdcError("source_id must be source:cms:pdc")
        object.__setattr__(self, "dataset_id", _id(self.dataset_id, "dataset_id"))
        object.__setattr__(self, "distribution_id", _id(self.distribution_id, "distribution_id"))
        object.__setattr__(self, "dataset_title", _text(self.dataset_title, "dataset_title", MAX_TITLE_LENGTH))
        object.__setattr__(
            self, "distribution_title", _text(self.distribution_title, "distribution_title", MAX_TITLE_LENGTH)
        )
        if self.distribution_format not in {"csv", "json", "parquet"}:
            raise CmsPdcError("distribution_format is unsupported")
        object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        object.__setattr__(self, "distribution_url", _https(self.distribution_url, "distribution_url"))
        object.__setattr__(self, "release_locator", _https(self.release_locator, "release_locator"))
        object.__setattr__(self, "schema_fingerprint", _sha256(self.schema_fingerprint, "schema_fingerprint"))
        _bounded_int(self.max_bytes, "max_bytes", MAX_STREAM_BYTES)
        if self.max_bytes < 1 or self.max_bytes > MAX_STREAM_BYTES:
            raise CmsPdcError(f"max_bytes must be between 1 and {MAX_STREAM_BYTES}")
        _bounded_int(self.max_chunks, "max_chunks", MAX_STREAM_CHUNKS)
        if self.max_chunks < 1:
            raise CmsPdcError("max_chunks must be positive")
        try:
            finite_seconds = math.isfinite(self.max_seconds)
        except (OverflowError, TypeError):
            finite_seconds = False
        if (
            isinstance(self.max_seconds, bool)
            or not isinstance(self.max_seconds, (int, float))
            or self.max_seconds <= 0
            or self.max_seconds > MAX_STREAM_SECONDS
            or not finite_seconds
        ):
            raise CmsPdcError(f"max_seconds must be finite and between 0 and {MAX_STREAM_SECONDS}")
        try:
            adapter = AdapterCatalogEntry(
                source_id=self.source_id,
                source_url=self.source_url,
                change_mode=self.change_mode,
                release_locator=self.release_locator,
                rights_status=self.rights_status,
                enabled=self.enabled,
                max_bytes=self.max_bytes,
                max_chunks=self.max_chunks,
                max_seconds=self.max_seconds,
            )
        except ValueError as exc:
            raise CmsPdcError(str(exc)) from exc
        if adapter.source_id != self.source_id:  # pragma: no cover - defensive invariant
            raise CmsPdcError("catalog adapter source identity drifted")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CmsPdcCatalogEntry":
        expected = {
            "dataset_id",
            "distribution_id",
            "dataset_title",
            "distribution_title",
            "distribution_format",
            "source_url",
            "distribution_url",
            "release_locator",
            "change_mode",
            "rights_status",
            "enabled",
            "schema_fingerprint",
            "max_bytes",
            "max_chunks",
            "max_seconds",
            "source_id",
        }
        unknown = set(value) - expected
        if unknown:
            raise CmsPdcError(f"catalog entry has unknown fields: {sorted(unknown)}")
        return cls(
            dataset_id=cast(str, value.get("dataset_id")),
            distribution_id=cast(str, value.get("distribution_id")),
            dataset_title=cast(str, value.get("dataset_title")),
            distribution_title=cast(str, value.get("distribution_title")),
            distribution_format=cast(DistributionFormat, value.get("distribution_format")),
            source_url=cast(str, value.get("source_url")),
            distribution_url=cast(str, value.get("distribution_url")),
            release_locator=cast(str, value.get("release_locator")),
            change_mode=cast(AdapterChangeMode, value.get("change_mode", "etag")),
            rights_status=cast(RightsStatus, value.get("rights_status", "approved_public")),
            enabled=cast(bool, value.get("enabled", True)),
            schema_fingerprint=cast(str, value.get("schema_fingerprint")),
            max_bytes=cast(int, value.get("max_bytes", MAX_STREAM_BYTES)),
            max_chunks=cast(int, value.get("max_chunks", MAX_STREAM_CHUNKS)),
            max_seconds=cast(float, value.get("max_seconds", MAX_STREAM_SECONDS)),
            source_id=cast(str, value.get("source_id", CMS_PDC_SOURCE_ID)),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "dataset_id": self.dataset_id,
            "distribution_id": self.distribution_id,
            "dataset_title": self.dataset_title,
            "distribution_title": self.distribution_title,
            "distribution_format": self.distribution_format,
            "source_url": self.source_url,
            "distribution_url": self.distribution_url,
            "release_locator": self.release_locator,
            "change_mode": self.change_mode,
            "rights_status": self.rights_status,
            "enabled": self.enabled,
            "schema_fingerprint": self.schema_fingerprint,
            "max_bytes": self.max_bytes,
            "max_chunks": self.max_chunks,
            "max_seconds": self.max_seconds,
        }


@dataclass(frozen=True, slots=True)
class CmsPdcRelease:
    """Semantic CMS PDC release metadata supplied by an acquisition adapter."""

    dataset_id: str
    distribution_id: str
    release_id: str
    modified_at: datetime
    schema_fingerprint: str
    source_url: str
    distribution_url: str
    probe_state: ProbeState = "changed"
    declared_content_sha256: str | None = None
    source_id: str = CMS_PDC_SOURCE_ID

    def __post_init__(self) -> None:
        if self.source_id != CMS_PDC_SOURCE_ID:
            raise CmsPdcError("release source_id must be source:cms:pdc")
        object.__setattr__(self, "dataset_id", _id(self.dataset_id, "dataset_id"))
        object.__setattr__(self, "distribution_id", _id(self.distribution_id, "distribution_id"))
        object.__setattr__(self, "release_id", _release_id(self.release_id))
        object.__setattr__(self, "modified_at", _timestamp(self.modified_at, "modified_at"))
        object.__setattr__(self, "schema_fingerprint", _sha256(self.schema_fingerprint, "schema_fingerprint"))
        object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        object.__setattr__(self, "distribution_url", _https(self.distribution_url, "distribution_url"))
        if self.probe_state not in {"changed", "not_modified", "failed_probe"}:
            raise CmsPdcError("probe_state is unsupported")
        object.__setattr__(
            self, "declared_content_sha256", _optional_sha256(self.declared_content_sha256, "declared_content_sha256")
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CmsPdcRelease":
        expected = {
            "dataset_id",
            "distribution_id",
            "release_id",
            "modified_at",
            "schema_fingerprint",
            "source_url",
            "distribution_url",
            "probe_state",
            "declared_content_sha256",
            "source_id",
        }
        unknown = set(value) - expected
        if unknown:
            raise CmsPdcError(f"release has unknown fields: {sorted(unknown)}")
        return cls(
            dataset_id=cast(str, value.get("dataset_id")),
            distribution_id=cast(str, value.get("distribution_id")),
            release_id=cast(str, value.get("release_id")),
            modified_at=_timestamp(value.get("modified_at"), "modified_at"),
            schema_fingerprint=cast(str, value.get("schema_fingerprint")),
            source_url=cast(str, value.get("source_url")),
            distribution_url=cast(str, value.get("distribution_url")),
            probe_state=cast(ProbeState, value.get("probe_state", "changed")),
            declared_content_sha256=cast(str | None, value.get("declared_content_sha256")),
            source_id=cast(str, value.get("source_id", CMS_PDC_SOURCE_ID)),
        )

    def semantic_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "dataset_id": self.dataset_id,
            "distribution_id": self.distribution_id,
            "release_id": self.release_id,
            "modified_at": _format_timestamp(self.modified_at),
            "schema_fingerprint": self.schema_fingerprint,
            "source_url": self.source_url,
            "distribution_url": self.distribution_url,
        }

    def as_dict(self) -> dict[str, object]:
        return {
            **self.semantic_dict(),
            "probe_state": self.probe_state,
            "declared_content_sha256": self.declared_content_sha256,
        }


def canonical_release_fingerprint(release: CmsPdcRelease) -> str:
    """Return the deterministic semantic fingerprint for a CMS PDC release."""

    if not isinstance(release, CmsPdcRelease):
        raise CmsPdcError("release must be a CmsPdcRelease")
    return "sha256:" + sha256(_canonical(release.semantic_dict())).hexdigest()


@dataclass(frozen=True, slots=True)
class CmsPdcReceipt:
    """Payload-free release, schema, and bounded-stream outcome."""

    receipt_id: str
    idempotency_key: str
    state: CmsPdcState
    change_kind: CmsPdcChangeKind
    source_id: str
    dataset_id: str
    distribution_id: str
    release_id: str
    release_fingerprint: str
    content_sha256: str | None
    prior_release_fingerprint: str | None
    prior_content_sha256: str | None
    prior_receipt_id: str | None
    probe_state: ProbeState
    schema_state: CmsPdcSchemaState
    stream_state: CmsPdcStreamState
    acknowledged: bool
    received_bytes: int
    chunk_count: int
    current_projection_preserved: bool
    observed_at: datetime
    source_url: str
    distribution_url: str
    error: str | None = None

    def __post_init__(self) -> None:
        receipt_id = _text(self.receipt_id, "receipt_id", 80)
        idempotency_key = _text(self.idempotency_key, "idempotency_key", 80)
        if _RECEIPT_ID.fullmatch(receipt_id) is None:
            raise CmsPdcError("receipt_id has an invalid format")
        if _IDEMPOTENCY_KEY.fullmatch(idempotency_key) is None:
            raise CmsPdcError("idempotency_key has an invalid format")
        object.__setattr__(self, "receipt_id", receipt_id)
        object.__setattr__(self, "idempotency_key", idempotency_key)
        if self.state not in {"changed", "no_op", "schema_drift", "replayed", "interrupted", "failed_probe"}:
            raise CmsPdcError("state is unsupported")
        if self.change_kind not in {"release", "content", "none", "schema_drift", "replay", "interrupted", "failed"}:
            raise CmsPdcError("change_kind is unsupported")
        if self.source_id != CMS_PDC_SOURCE_ID:
            raise CmsPdcError("source_id must be source:cms:pdc")
        object.__setattr__(self, "dataset_id", _id(self.dataset_id, "dataset_id"))
        object.__setattr__(self, "distribution_id", _id(self.distribution_id, "distribution_id"))
        object.__setattr__(self, "release_id", _release_id(self.release_id))
        object.__setattr__(self, "release_fingerprint", _sha256(self.release_fingerprint, "release_fingerprint"))
        object.__setattr__(self, "content_sha256", _optional_sha256(self.content_sha256, "content_sha256"))
        object.__setattr__(
            self,
            "prior_release_fingerprint",
            _optional_sha256(self.prior_release_fingerprint, "prior_release_fingerprint"),
        )
        object.__setattr__(
            self, "prior_content_sha256", _optional_sha256(self.prior_content_sha256, "prior_content_sha256")
        )
        if self.prior_receipt_id is not None:
            prior_receipt_id = _text(self.prior_receipt_id, "prior_receipt_id", 80)
            if _RECEIPT_ID.fullmatch(prior_receipt_id) is None:
                raise CmsPdcError("prior_receipt_id has an invalid format")
            object.__setattr__(self, "prior_receipt_id", prior_receipt_id)
        if self.probe_state not in {"changed", "not_modified", "failed_probe"}:
            raise CmsPdcError("probe_state is unsupported")
        if self.schema_state not in {"valid", "drift", "not_checked"}:
            raise CmsPdcError("schema_state is unsupported")
        if self.stream_state not in {"completed", "interrupted", "not_started"}:
            raise CmsPdcError("stream_state is unsupported")
        if not isinstance(self.acknowledged, bool) or not isinstance(self.current_projection_preserved, bool):
            raise CmsPdcError("receipt booleans are invalid")
        object.__setattr__(self, "received_bytes", _bounded_int(self.received_bytes, "received_bytes", MAX_BYTES))
        object.__setattr__(self, "chunk_count", _bounded_int(self.chunk_count, "chunk_count", MAX_CHUNKS))
        object.__setattr__(self, "observed_at", _timestamp(self.observed_at, "observed_at"))
        object.__setattr__(self, "source_url", _https(self.source_url, "source_url"))
        object.__setattr__(self, "distribution_url", _https(self.distribution_url, "distribution_url"))
        if self.error is not None:
            error = _text(self.error, "error", 32)
            if error not in SAFE_ERROR_CATEGORIES:
                raise CmsPdcError("error must be a safe CMS PDC category")
            object.__setattr__(self, "error", cast(CmsPdcErrorCategory, error))
        if self.state == "schema_drift" and (
            self.change_kind != "schema_drift"
            or self.schema_state != "drift"
            or self.stream_state != "not_started"
            or self.acknowledged
            or not self.current_projection_preserved
        ):
            raise CmsPdcError("schema drift receipt must preserve projection and remain unacknowledged")
        if self.state == "interrupted" and (
            self.change_kind != "interrupted"
            or self.stream_state != "interrupted"
            or self.acknowledged
            or not self.current_projection_preserved
        ):
            raise CmsPdcError("interrupted receipt must remain unacknowledged")
        if self.state == "failed_probe" and (
            self.change_kind != "failed"
            or self.stream_state != "not_started"
            or self.acknowledged
            or not self.current_projection_preserved
        ):
            raise CmsPdcError("failed probe receipt must preserve projection and remain unacknowledged")
        if self.state == "no_op" and (
            self.change_kind != "none"
            or self.content_sha256 is None
            or self.schema_state != "valid"
            or self.stream_state != "not_started"
            or not self.acknowledged
            or self.received_bytes != 0
            or self.chunk_count != 0
            or not self.current_projection_preserved
            or self.error is not None
        ):
            raise CmsPdcError("no-op receipt must preserve projection with zero stream counters")
        if self.state == "replayed" and (
            self.change_kind != "replay"
            or self.content_sha256 is None
            or self.schema_state != "valid"
            or self.stream_state != "not_started"
            or not self.acknowledged
            or self.received_bytes != 0
            or self.chunk_count != 0
            or not self.current_projection_preserved
            or self.error is not None
        ):
            raise CmsPdcError("replay receipt must preserve projection with zero stream counters")

    def as_dict(self) -> dict[str, object]:
        payload = {
            "schema_version": CMS_PDC_SCHEMA_VERSION,
            "record_type": CMS_PDC_RECORD_TYPE,
            "receipt_id": self.receipt_id,
            "idempotency_key": self.idempotency_key,
            "state": self.state,
            "change_kind": self.change_kind,
            "source_id": self.source_id,
            "dataset_id": self.dataset_id,
            "distribution_id": self.distribution_id,
            "release_id": self.release_id,
            "release_fingerprint": self.release_fingerprint,
            "content_sha256": self.content_sha256,
            "prior_release_fingerprint": self.prior_release_fingerprint,
            "prior_content_sha256": self.prior_content_sha256,
            "prior_receipt_id": self.prior_receipt_id,
            "probe_state": self.probe_state,
            "schema_state": self.schema_state,
            "stream_state": self.stream_state,
            "acknowledged": self.acknowledged,
            "received_bytes": self.received_bytes,
            "chunk_count": self.chunk_count,
            "current_projection_preserved": self.current_projection_preserved,
            "observed_at": _format_timestamp(self.observed_at),
            "source_url": self.source_url,
            "distribution_url": self.distribution_url,
            "error": self.error,
        }
        validate_cms_pdc_receipt(payload)
        return payload

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CmsPdcReceipt":
        payload = validate_cms_pdc_receipt(value)
        expected = {
            "schema_version",
            "record_type",
            "receipt_id",
            "idempotency_key",
            "state",
            "change_kind",
            "source_id",
            "dataset_id",
            "distribution_id",
            "release_id",
            "release_fingerprint",
            "content_sha256",
            "prior_release_fingerprint",
            "prior_content_sha256",
            "prior_receipt_id",
            "probe_state",
            "schema_state",
            "stream_state",
            "acknowledged",
            "received_bytes",
            "chunk_count",
            "current_projection_preserved",
            "observed_at",
            "source_url",
            "distribution_url",
            "error",
        }
        unknown = set(payload) - expected
        if unknown:
            raise CmsPdcError(f"receipt has unknown fields: {sorted(unknown)}")
        return cls(
            receipt_id=cast(str, payload["receipt_id"]),
            idempotency_key=cast(str, payload["idempotency_key"]),
            state=cast(CmsPdcState, payload["state"]),
            change_kind=cast(CmsPdcChangeKind, payload["change_kind"]),
            source_id=cast(str, payload["source_id"]),
            dataset_id=cast(str, payload["dataset_id"]),
            distribution_id=cast(str, payload["distribution_id"]),
            release_id=cast(str, payload["release_id"]),
            release_fingerprint=cast(str, payload["release_fingerprint"]),
            content_sha256=cast(str | None, payload["content_sha256"]),
            prior_release_fingerprint=cast(str | None, payload["prior_release_fingerprint"]),
            prior_content_sha256=cast(str | None, payload["prior_content_sha256"]),
            prior_receipt_id=cast(str | None, payload["prior_receipt_id"]),
            probe_state=cast(ProbeState, payload["probe_state"]),
            schema_state=cast(CmsPdcSchemaState, payload["schema_state"]),
            stream_state=cast(CmsPdcStreamState, payload["stream_state"]),
            acknowledged=cast(bool, payload["acknowledged"]),
            received_bytes=cast(int, payload["received_bytes"]),
            chunk_count=cast(int, payload["chunk_count"]),
            current_projection_preserved=cast(bool, payload["current_projection_preserved"]),
            observed_at=_timestamp(payload["observed_at"], "observed_at"),
            source_url=cast(str, payload["source_url"]),
            distribution_url=cast(str, payload["distribution_url"]),
            error=cast(str | None, payload["error"]),
        )


__all__ = [
    "CMS_PDC_RECORD_TYPE",
    "CMS_PDC_SCHEMA_VERSION",
    "CMS_PDC_SOURCE_ID",
    "CmsPdcCatalogEntry",
    "CmsPdcChangeKind",
    "CmsPdcErrorCategory",
    "CmsPdcError",
    "CmsPdcReceipt",
    "CmsPdcRelease",
    "CmsPdcState",
    "SAFE_ERROR_CATEGORIES",
    "canonical_release_fingerprint",
    "validate_cms_pdc_receipt",
]
