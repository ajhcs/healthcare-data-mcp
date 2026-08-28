"""Fixture-driven AHRQ official-release change detection.

The detector resolves and validates source-native release metadata, computes
semantic release and raw-response fingerprints, and returns a secret-free
receipt. It deliberately performs no network I/O or artifact publication;
those responsibilities are admitted by later custody and producer lanes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Literal, Mapping, TypeAlias, cast


DetectionState = Literal["changed", "unchanged", "failed_probe"]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA = _ROOT / "contracts/healthcare-data-platform/ahrq/v1/ahrq-release.schema.json"
_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9._:-]*$")
_RELEASE_ID = re.compile(r"^release:[a-z0-9][a-z0-9._:-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_FIXTURE_ID = re.compile(r"^fixture:ahrq:[a-z0-9][a-z0-9._:-]*$")
_RECEIPT_ID = re.compile(r"^receipt:ahrq:[a-f0-9]{24}$")
_EXPECTED_SOURCE_ID = "source:ahrq:lighthouse"
_MAX_RESPONSE_BYTES = 131_072


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


class AhrqDetectorError(ValueError):
    """Raised when official-release metadata or a detection transition is unsafe."""


@dataclass(frozen=True, slots=True)
class ReleaseMetadata:
    """Canonical metadata resolved from one official AHRQ response."""

    source_id: str
    source_url: str
    release_id: str
    release_label: str
    published_at: datetime

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "ReleaseMetadata":
        source_id = _required_string(value.get("source_id"), "source_id", _SOURCE_ID)
        source_url = _required_https(value.get("source_url"), "source_url")
        release_id = _required_string(value.get("release_id"), "release_id", _RELEASE_ID)
        release_label = _required_string(value.get("release_label"), "release_label")
        published_at = _timestamp(value.get("published_at"), "published_at")
        return cls(source_id, source_url, release_id, release_label, published_at)

    def canonical_bytes(self) -> bytes:
        """Return stable semantic metadata bytes used for the release hash."""

        value = {
            "source_id": self.source_id,
            "source_url": self.source_url,
            "release_id": self.release_id,
            "release_label": self.release_label,
            "published_at": _iso(self.published_at),
        }
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")

    @property
    def release_fingerprint(self) -> str:
        """Return the semantic release fingerprint."""

        return _sha256(self.canonical_bytes())


@dataclass(frozen=True, slots=True)
class DetectionReceipt:
    """Secret-free evidence for one release probe."""

    state: DetectionState
    source_id: str
    release_id: str | None
    release_label: str | None
    published_at: str | None
    release_fingerprint: str | None
    response_fingerprint: str | None
    prior_release_fingerprint: str | None
    prior_response_fingerprint: str | None
    receipt_id: str
    failure_reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-safe receipt for an evidence ledger."""

        return {
            "state": self.state,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "release_label": self.release_label,
            "published_at": self.published_at,
            "release_fingerprint": self.release_fingerprint,
            "response_fingerprint": self.response_fingerprint,
            "prior_release_fingerprint": self.prior_release_fingerprint,
            "prior_response_fingerprint": self.prior_response_fingerprint,
            "receipt_id": self.receipt_id,
            "failure_reason": self.failure_reason,
        }


class AhrqChangeDetector:
    """Detect semantic AHRQ release changes without acquiring or publishing bytes."""

    def load_fixture(self, path: str | Path) -> dict[str, object]:
        """Load a strict, schema-validated official-release probe fixture."""

        try:
            value = json.loads(Path(path).read_text(encoding="utf-8"), object_pairs_hook=_strict_pairs)
        except AhrqDetectorError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise AhrqDetectorError(f"unable to read AHRQ release fixture: {path}") from exc
        if not isinstance(value, dict):
            raise AhrqDetectorError("AHRQ release fixture must be an object")
        _validate_schema(value, _SCHEMA, "AHRQ release fixture")
        return value

    def detect(self, fixture: Mapping[str, object], *, prior: DetectionReceipt | None = None) -> DetectionReceipt:
        """Resolve metadata and classify changed, unchanged, or failed probes."""

        _validate_schema(dict(fixture), _SCHEMA, "AHRQ release fixture")
        source_id = _required_string(fixture.get("source_id"), "source_id", _SOURCE_ID)
        if source_id != _EXPECTED_SOURCE_ID:
            raise AhrqDetectorError(f"unsupported AHRQ source_id: {source_id}")
        source_url = _required_https(fixture.get("source_url"), "source_url")
        if prior is not None:
            _validate_prior(prior, source_id)
        if prior is not None and prior.source_id != source_id:
            raise AhrqDetectorError("prior receipt source_id does not match fixture")
        probe_state = fixture.get("probe_state")
        if probe_state == "failed_probe":
            return self._failed_receipt(fixture, source_id, prior)
        if probe_state != "release_metadata":
            raise AhrqDetectorError("unsupported AHRQ probe state")

        metadata_value = fixture.get("release_metadata")
        metadata = ReleaseMetadata.from_mapping(_mapping(metadata_value, "release_metadata"))
        if metadata.source_id != source_id:
            raise AhrqDetectorError("release metadata source_id does not match fixture")
        if metadata.source_url != source_url:
            raise AhrqDetectorError("release metadata source_url does not match fixture")
        body = fixture.get("response_body")
        if not isinstance(body, str) or not body:
            raise AhrqDetectorError("release metadata response_body must be non-empty text")
        try:
            response_bytes = body.encode("utf-8")
        except UnicodeError as exc:
            raise AhrqDetectorError("response_body contains malformed Unicode") from exc
        if len(response_bytes) > _MAX_RESPONSE_BYTES:
            raise AhrqDetectorError("response_body exceeds the 131072-byte probe bound")
        _validate_response_metadata(body, metadata)
        response_fingerprint = _sha256(response_bytes)
        release_fingerprint = metadata.release_fingerprint
        prior_release = prior.release_fingerprint if prior is not None else None
        prior_response = prior.response_fingerprint if prior is not None else None
        state: Literal["changed", "unchanged"] = (
            "unchanged" if prior_release is not None and prior_release == release_fingerprint else "changed"
        )
        receipt_id = _receipt_id(
            source_id=source_id,
            state=state,
            release_id=metadata.release_id,
            release_label=metadata.release_label,
            published_at=_iso(metadata.published_at),
            release_fingerprint=release_fingerprint,
            response_fingerprint=response_fingerprint,
            prior_release_fingerprint=prior_release,
            prior_response_fingerprint=prior_response,
            failure_reason=None,
        )
        return DetectionReceipt(
            state=state,
            source_id=source_id,
            release_id=metadata.release_id,
            release_label=metadata.release_label,
            published_at=_iso(metadata.published_at),
            release_fingerprint=release_fingerprint,
            response_fingerprint=response_fingerprint,
            prior_release_fingerprint=prior_release,
            prior_response_fingerprint=prior_response,
            receipt_id=receipt_id,
        )

    def _failed_receipt(
        self,
        fixture: Mapping[str, object],
        source_id: str,
        prior: DetectionReceipt | None,
    ) -> DetectionReceipt:
        reason = _required_string(fixture.get("failure_reason"), "failure_reason")
        prior_release = prior.release_fingerprint if prior is not None else None
        prior_response = prior.response_fingerprint if prior is not None else None
        release_id = prior.release_id if prior is not None else None
        release_label = prior.release_label if prior is not None else None
        published_at = prior.published_at if prior is not None else None
        receipt_id = _receipt_id(
            source_id=source_id,
            state="failed_probe",
            release_id=release_id,
            release_label=release_label,
            published_at=published_at,
            release_fingerprint=prior_release,
            response_fingerprint=prior_response,
            prior_release_fingerprint=prior_release,
            prior_response_fingerprint=prior_response,
            failure_reason=reason,
        )
        return DetectionReceipt(
            state="failed_probe",
            source_id=source_id,
            release_id=release_id,
            release_label=release_label,
            published_at=published_at,
            release_fingerprint=prior_release,
            response_fingerprint=prior_response,
            prior_release_fingerprint=prior_release,
            prior_response_fingerprint=prior_response,
            receipt_id=receipt_id,
            failure_reason=reason,
        )


def _validate_schema(value: object, path: Path, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:  # pragma: no cover - dependency installation failure
        raise AhrqDetectorError("jsonschema is required for AHRQ validation") from exc
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AhrqDetectorError(f"unable to read {label} schema") from exc
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(cast(JsonValue, value)),
        key=lambda error: str(error.absolute_path),
    )
    if errors:
        error = errors[0]
        raise AhrqDetectorError(f"{label} failed schema validation: {error.message}")


def _validate_response_metadata(body: str, metadata: ReleaseMetadata) -> None:
    try:
        parsed = json.loads(body, object_pairs_hook=_strict_pairs)
    except AhrqDetectorError:
        raise
    except json.JSONDecodeError as exc:
        raise AhrqDetectorError("response_body is not valid JSON metadata") from exc
    if not isinstance(parsed, dict):
        raise AhrqDetectorError("response_body metadata must be an object")
    for key, expected in (("source_id", metadata.source_id), ("source_url", metadata.source_url)):
        if key in parsed and parsed[key] != expected:
            raise AhrqDetectorError(f"response_body {key} does not match resolved release metadata")
    for key, expected in (
        ("release_id", metadata.release_id),
        ("release_label", metadata.release_label),
        ("published_at", _iso(metadata.published_at)),
    ):
        if parsed.get(key) != expected:
            raise AhrqDetectorError(f"response_body {key} does not match resolved release metadata")


def _validate_prior(prior: DetectionReceipt, source_id: str) -> None:
    """Validate prior receipt lineage before it can influence classification."""

    if prior.source_id != source_id:
        raise AhrqDetectorError("prior receipt source_id does not match fixture")
    if prior.state not in {"changed", "unchanged", "failed_probe"}:
        raise AhrqDetectorError("prior receipt has an unsupported state")
    if _RECEIPT_ID.fullmatch(prior.receipt_id) is None:
        raise AhrqDetectorError("prior receipt_id is malformed")
    for label, value in (
        ("release_fingerprint", prior.release_fingerprint),
        ("response_fingerprint", prior.response_fingerprint),
        ("prior_release_fingerprint", prior.prior_release_fingerprint),
        ("prior_response_fingerprint", prior.prior_response_fingerprint),
    ):
        if value is not None and _SHA256.fullmatch(value) is None:
            raise AhrqDetectorError(f"prior {label} is malformed")
    has_release = prior.release_fingerprint is not None
    has_response = prior.response_fingerprint is not None
    if has_release != has_response:
        raise AhrqDetectorError("prior receipt must contain both release and response fingerprints")
    has_prior_release = prior.prior_release_fingerprint is not None
    has_prior_response = prior.prior_response_fingerprint is not None
    if has_prior_release != has_prior_response:
        raise AhrqDetectorError("prior receipt history must contain both release and response fingerprints")
    if prior.state in {"changed", "unchanged"} and not has_release:
        raise AhrqDetectorError("successful prior receipt is missing release fingerprints")
    if prior.state in {"changed", "unchanged"} and prior.failure_reason is not None:
        raise AhrqDetectorError("successful prior receipt cannot contain a failure reason")
    if prior.state == "failed_probe" and (prior.failure_reason is None or not prior.failure_reason.strip()):
        raise AhrqDetectorError("failed prior receipt is missing a failure reason")
    if has_release:
        if prior.release_id is None or _RELEASE_ID.fullmatch(prior.release_id) is None:
            raise AhrqDetectorError("prior release_id is malformed")
        if prior.release_label is None or not prior.release_label.strip():
            raise AhrqDetectorError("prior release_label is missing")
        if prior.published_at is None:
            raise AhrqDetectorError("prior published_at is missing")
        _timestamp(prior.published_at, "prior published_at")
    elif any(value is not None for value in (prior.release_id, prior.release_label, prior.published_at)):
        raise AhrqDetectorError("prior release metadata is incomplete")
    expected_id = _receipt_id(
        source_id=source_id,
        state=prior.state,
        release_id=prior.release_id,
        release_label=prior.release_label,
        published_at=prior.published_at,
        release_fingerprint=prior.release_fingerprint,
        response_fingerprint=prior.response_fingerprint,
        prior_release_fingerprint=prior.prior_release_fingerprint,
        prior_response_fingerprint=prior.prior_response_fingerprint,
        failure_reason=prior.failure_reason,
    )
    if prior.receipt_id != expected_id:
        raise AhrqDetectorError("prior receipt_id does not match receipt content")


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise AhrqDetectorError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AhrqDetectorError(f"{label} must be an object")
    return value


def _required_string(value: object, label: str, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value:
        raise AhrqDetectorError(f"{label} must be a non-empty string")
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise AhrqDetectorError(f"{label} contains malformed Unicode") from exc
    if pattern is not None and pattern.fullmatch(value) is None:
        raise AhrqDetectorError(f"{label} has an invalid format")
    return value


def _required_https(value: object, label: str) -> str:
    result = _required_string(value, label)
    if not result.startswith("https://"):
        raise AhrqDetectorError(f"{label} must use HTTPS")
    return result


def _timestamp(value: object, label: str) -> datetime:
    raw = _required_string(value, label)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AhrqDetectorError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise AhrqDetectorError(f"{label} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _sha256(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


def _receipt_id(
    *,
    source_id: str,
    state: str,
    release_id: str | None,
    release_label: str | None,
    published_at: str | None,
    release_fingerprint: str | None,
    response_fingerprint: str | None,
    prior_release_fingerprint: str | None,
    prior_response_fingerprint: str | None,
    failure_reason: str | None,
) -> str:
    material = json.dumps(
        {
            "source_id": source_id,
            "state": state,
            "release_id": release_id,
            "release_label": release_label,
            "published_at": published_at,
            "release_fingerprint": release_fingerprint,
            "response_fingerprint": response_fingerprint,
            "prior_release_fingerprint": prior_release_fingerprint,
            "prior_response_fingerprint": prior_response_fingerprint,
            "failure_reason": failure_reason,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "receipt:ahrq:" + sha256(material).hexdigest()[:24]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = ["AhrqChangeDetector", "AhrqDetectorError", "DetectionReceipt", "ReleaseMetadata"]
