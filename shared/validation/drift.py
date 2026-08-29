"""Bounded drift validation and recoverable quarantine for source batches.

This module is deliberately transport and projection neutral.  It validates a
source adapter's JSON-shaped result (including the v1 observation envelope),
compares explicit row/key/distribution expectations, and returns a
secret-free report.  A caller may pass accepted data to a projection callback,
but rejected data is never passed to that callback.  Quarantine stores only
structural summaries and custody metadata; it never stores source payloads.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path
import re
from types import MappingProxyType
from typing import Callable, Iterable, Literal, Mapping, Sequence, TypeAlias, cast


JsonScalar: TypeAlias = None | bool | int | float | str
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
DriftKind: TypeAlias = Literal["schema", "row", "key", "distribution"]
ValidationState: TypeAlias = Literal["accepted", "rejected"]
QuarantineState: TypeAlias = Literal["stored", "duplicate"]
SafeSummary: TypeAlias = int | float | bool | None | str | dict[str, "SafeSummary"] | list["SafeSummary"]

VALIDATION_SCHEMA_VERSION = "hdp.validation-report.v1"
QUARANTINE_SCHEMA_VERSION = "hdp.validation-quarantine.v1"
QUARANTINE_RECORD_TYPE = "source_validation_quarantine"
MAX_REASON_CODES = 32
MAX_QUARANTINE_SAMPLES = 32
MAX_SAMPLE_PATH = 300
MAX_QUARANTINE_BYTES = 256 * 1024
MAX_DISTRIBUTION_CATEGORIES = 256
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,299}$")
_PATH = re.compile(r"^[A-Za-z0-9_$.-]+(?:\[\])?(?:\.[A-Za-z0-9_$.-]+(?:\[\])?)*$")


class DriftValidationError(ValueError):
    """Raised when a validation policy or candidate cannot be safely parsed."""


class QuarantineError(DriftValidationError):
    """Raised when a structured quarantine record cannot be safely retained."""


def _canonical_bytes(value: object) -> bytes:
    """Serialize a JSON-shaped value deterministically without allowing NaN."""

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DriftValidationError("candidate is not canonical JSON") from exc


def _fingerprint(value: object) -> str:
    return "sha256:" + sha256(_canonical_bytes(value)).hexdigest()


def _required_text(value: object, label: str, *, maximum: int = MAX_SAMPLE_PATH) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DriftValidationError(f"{label} must be a non-empty string")
    if len(value) > maximum:
        raise DriftValidationError(f"{label} exceeds the {maximum}-character bound")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise DriftValidationError(f"{label} contains a control character")
    return value


def _optional_id(value: object, label: str) -> str | None:
    if value is None:
        return None
    text = _required_text(value, label)
    if _ID.fullmatch(text) is None:
        raise DriftValidationError(f"{label} has an invalid identifier")
    return text


def _optional_sha256(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DriftValidationError(f"{label} must be a sha256 fingerprint")
    return value


def _bounded_int(value: object, label: str, *, minimum: int = 0, maximum: int = 2**31 - 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DriftValidationError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise DriftValidationError(f"{label} must be between {minimum} and {maximum}")
    return value


def _finite_ratio(value: object, label: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DriftValidationError(f"{label} must be a number")
    ratio = float(value)
    if not math.isfinite(ratio) or ratio < minimum:
        raise DriftValidationError(f"{label} must be finite and at least {minimum}")
    return ratio


def _safe_summary(value: object) -> SafeSummary:
    """Return a bounded summary that cannot retain source values.

    Counts, booleans, and null are useful for operations and are safe to keep.
    Strings and arbitrary containers are represented by type/length/hash only;
    this prevents PHI, source rows, credentials, and arbitrary adapter values
    from leaking into receipts or quarantine files.
    """

    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"type": "number", "finite": False}
    if isinstance(value, str):
        return {
            "type": "string",
            "length": len(value),
            "sha256": _fingerprint(value),
        }
    if isinstance(value, Mapping):
        keys = [key for key in value if isinstance(key, str)]
        return {
            "type": "object",
            "key_count": len(keys),
            "keys_sha256": _fingerprint(sorted(keys)),
        }
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        return {
            "type": "array",
            "length": len(value),
            "sha256": _fingerprint([_safe_summary(item) for item in value[:64]]),
        }
    return {"type": type(value).__name__}


def _safe_key(value: object) -> str:
    """Create a deterministic category key without putting it in evidence."""

    return _fingerprint(_safe_summary(value))


def _redacted_identifier(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return _fingerprint(value)


@dataclass(frozen=True, slots=True)
class DistributionRule:
    """Expected category counts for one bounded row/envelope path.

    ``path`` is relative to a row when it contains no ``[]`` and relative to
    the candidate when it begins with ``observations[]`` or ``rows[]``.  A
    denominator rule compares the total count as well as category ratios.
    """

    path: str
    expected_counts: Mapping[str, int]
    min_ratio: float = 0.5
    max_ratio: float = 2.0
    denominator: bool = False
    expected_total: int | None = None

    def __post_init__(self) -> None:
        path = _required_text(self.path, "distribution path")
        if _PATH.fullmatch(path) is None:
            raise DriftValidationError("distribution path is malformed")
        if not isinstance(self.expected_counts, Mapping) or not self.expected_counts:
            raise DriftValidationError("distribution expected_counts must be non-empty")
        if len(self.expected_counts) > MAX_DISTRIBUTION_CATEGORIES:
            raise DriftValidationError("distribution expected_counts exceeds its category bound")
        counts: dict[str, int] = {}
        for category, count in self.expected_counts.items():
            category_text = _required_text(category, "distribution category", maximum=200)
            counts[category_text] = _bounded_int(count, "distribution expected count")
        if sum(counts.values()) < 1:
            raise DriftValidationError("distribution expected_counts must contain a positive denominator")
        min_ratio = _finite_ratio(self.min_ratio, "distribution min_ratio")
        max_ratio = _finite_ratio(self.max_ratio, "distribution max_ratio")
        if max_ratio < min_ratio:
            raise DriftValidationError("distribution max_ratio must be at least min_ratio")
        if not isinstance(self.denominator, bool):
            raise DriftValidationError("distribution denominator must be boolean")
        expected_total = self.expected_total
        if expected_total is not None:
            expected_total = _bounded_int(expected_total, "distribution expected_total")
            if expected_total < 1:
                raise DriftValidationError("distribution expected_total must be positive")
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "expected_counts", MappingProxyType(counts))
        object.__setattr__(self, "min_ratio", min_ratio)
        object.__setattr__(self, "max_ratio", max_ratio)
        object.__setattr__(self, "expected_total", expected_total)


@dataclass(frozen=True, slots=True)
class DriftBaseline:
    """Explicit expectations used to evaluate one adapter or source batch."""

    source_id: str | None = None
    schema_version: str | None = "hdp.observation-envelope.v1"
    expected_release_id: str | None = None
    expected_artifact_id: str | None = None
    expected_row_count: int | None = None
    expected_observation_ids: tuple[str, ...] = ()
    row_id_field: str = "observation_id"
    max_row_count_delta_ratio: float = 1.0
    expected_key_digest: str | None = None
    distribution_rules: tuple[DistributionRule, ...] = ()

    def __post_init__(self) -> None:
        source_id = _optional_id(self.source_id, "baseline source_id")
        schema_version = _optional_id(self.schema_version, "baseline schema_version")
        release_id = _optional_id(self.expected_release_id, "baseline expected_release_id")
        artifact_id = _optional_id(self.expected_artifact_id, "baseline expected_artifact_id")
        row_count = self.expected_row_count
        if row_count is not None:
            row_count = _bounded_int(row_count, "baseline expected_row_count")
        row_id_field = _required_text(self.row_id_field, "baseline row_id_field", maximum=100)
        if _PATH.fullmatch(row_id_field) is None:
            raise DriftValidationError("baseline row_id_field is malformed")
        ratio = _finite_ratio(self.max_row_count_delta_ratio, "baseline max_row_count_delta_ratio")
        key_digest = _optional_sha256(self.expected_key_digest, "baseline expected_key_digest")
        ids = tuple(
            _required_text(item, "baseline observation id", maximum=300) for item in self.expected_observation_ids
        )
        if len(set(ids)) != len(ids):
            raise DriftValidationError("baseline expected_observation_ids must be unique")
        rules = tuple(self.distribution_rules)
        if any(not isinstance(rule, DistributionRule) for rule in rules):
            raise DriftValidationError("baseline distribution_rules must contain DistributionRule values")
        if len(rules) > MAX_DISTRIBUTION_CATEGORIES:
            raise DriftValidationError("baseline distribution_rules exceeds its bound")
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "schema_version", schema_version)
        object.__setattr__(self, "expected_release_id", release_id)
        object.__setattr__(self, "expected_artifact_id", artifact_id)
        object.__setattr__(self, "expected_row_count", row_count)
        object.__setattr__(self, "expected_observation_ids", ids)
        object.__setattr__(self, "max_row_count_delta_ratio", ratio)
        object.__setattr__(self, "expected_key_digest", key_digest)
        object.__setattr__(self, "distribution_rules", rules)

    @classmethod
    def from_envelope(
        cls,
        envelope: Mapping[str, object],
        *,
        distribution_rules: Iterable[DistributionRule] = (),
    ) -> "DriftBaseline":
        """Capture a structural baseline from a known-good envelope."""

        source_release = _mapping(envelope.get("source_release"))
        artifact = _mapping(envelope.get("artifact"))
        observations = envelope.get("observations")
        if not isinstance(observations, list):
            raise DriftValidationError("baseline envelope observations must be an array")
        ids = tuple(_row_identifier(item, "observation_id") for item in observations)
        return cls(
            source_id=_text(source_release.get("source_id")),
            schema_version=_text(envelope.get("schema_version")),
            expected_release_id=_text(source_release.get("release_id")),
            expected_artifact_id=_text(artifact.get("artifact_id")),
            expected_row_count=len(observations),
            expected_observation_ids=ids,
            expected_key_digest=_candidate_key_digest(envelope),
            distribution_rules=tuple(distribution_rules),
        )


@dataclass(frozen=True, slots=True)
class DriftIssue:
    """One secret-free, machine-readable validation failure."""

    kind: DriftKind
    code: str
    path: str
    expected: object = None
    actual: object = None

    def __post_init__(self) -> None:
        if self.kind not in {"schema", "row", "key", "distribution"}:
            raise DriftValidationError("drift issue kind is unsupported")
        code = _required_text(self.code, "drift issue code", maximum=120)
        path = _required_text(self.path, "drift issue path", maximum=MAX_SAMPLE_PATH)
        object.__setattr__(self, "code", code)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "expected", _safe_summary(self.expected))
        object.__setattr__(self, "actual", _safe_summary(self.actual))

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "code": self.code,
            "path": self.path,
            "expected": self.expected,
            "actual": self.actual,
        }


@dataclass(frozen=True, slots=True)
class DriftReport:
    """Validation decision that never mutates a current projection."""

    state: ValidationState
    source_id: str | None
    release_id: str | None
    artifact_id: str | None
    envelope_sha256: str | None
    row_count: int
    issues: tuple[DriftIssue, ...] = ()
    current_projection_preserved: bool = True
    projection_applied: bool = False

    @property
    def accepted(self) -> bool:
        return self.state == "accepted"

    @property
    def rejected(self) -> bool:
        return self.state == "rejected"

    @property
    def issue_codes(self) -> tuple[str, ...]:
        return tuple(issue.code for issue in self.issues)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "record_type": "source_validation_report",
            "state": self.state,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "artifact_id": self.artifact_id,
            "envelope_sha256": self.envelope_sha256,
            "row_count": self.row_count,
            "issue_count": len(self.issues),
            "issue_codes": list(self.issue_codes),
            "issues": [issue.as_dict() for issue in self.issues],
            "current_projection_preserved": self.current_projection_preserved,
            "projection_applied": self.projection_applied,
        }


@dataclass(frozen=True, slots=True)
class QuarantineSample:
    """A redacted structural sample reference, never a source value."""

    path: str
    summary: Mapping[str, SafeSummary]

    def __post_init__(self) -> None:
        path = _required_text(self.path, "quarantine sample path")
        if len(path) > MAX_SAMPLE_PATH:
            raise QuarantineError("quarantine sample path exceeds its bound")
        if not isinstance(self.summary, Mapping):
            raise QuarantineError("quarantine sample summary must be an object")
        safe = {str(key): _safe_summary(value) for key, value in self.summary.items()}
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "summary", MappingProxyType(safe))

    def as_dict(self) -> dict[str, object]:
        return {"path": self.path, "summary": dict(self.summary)}


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    """Secret-free, custody-bound record for one rejected candidate."""

    quarantine_id: str
    source_id: str | None
    release_id: str | None
    artifact_id: str | None
    artifact_sha256: str | None
    custody_locator: str | None
    envelope_sha256: str | None
    reason_codes: tuple[str, ...]
    samples: tuple[QuarantineSample, ...]
    current_projection_preserved: bool = True
    recorded_at: str = ""

    def __post_init__(self) -> None:
        quarantine_id = _required_text(self.quarantine_id, "quarantine_id")
        if _ID.fullmatch(quarantine_id) is None or not quarantine_id.startswith("quarantine:"):
            raise QuarantineError("quarantine_id is malformed")
        source_id = _optional_id(self.source_id, "quarantine source_id")
        release_id = _optional_id(self.release_id, "quarantine release_id")
        artifact_id = _optional_id(self.artifact_id, "quarantine artifact_id")
        artifact_sha256 = _optional_sha256(self.artifact_sha256, "quarantine artifact_sha256")
        custody_locator = _optional_id(self.custody_locator, "quarantine custody_locator")
        envelope_sha256 = _optional_sha256(self.envelope_sha256, "quarantine envelope_sha256")
        if not self.reason_codes or len(self.reason_codes) > MAX_REASON_CODES:
            raise QuarantineError("quarantine reason_codes must be bounded and non-empty")
        reasons = tuple(_required_text(code, "quarantine reason code", maximum=120) for code in self.reason_codes)
        if len(set(reasons)) != len(reasons):
            raise QuarantineError("quarantine reason_codes must be unique")
        samples = tuple(self.samples)
        if len(samples) > MAX_QUARANTINE_SAMPLES or any(not isinstance(sample, QuarantineSample) for sample in samples):
            raise QuarantineError("quarantine samples exceed their bound")
        if not isinstance(self.current_projection_preserved, bool) or not self.current_projection_preserved:
            raise QuarantineError("quarantine must preserve the current projection")
        recorded_at = self.recorded_at or _utc_now()
        _timestamp(recorded_at, "quarantine recorded_at")
        object.__setattr__(self, "quarantine_id", quarantine_id)
        object.__setattr__(self, "source_id", source_id)
        object.__setattr__(self, "release_id", release_id)
        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "custody_locator", custody_locator)
        object.__setattr__(self, "envelope_sha256", envelope_sha256)
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(self, "samples", samples)
        object.__setattr__(self, "recorded_at", recorded_at)

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUARANTINE_SCHEMA_VERSION,
            "record_type": QUARANTINE_RECORD_TYPE,
            "quarantine_id": self.quarantine_id,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "custody_locator": self.custody_locator,
            "envelope_sha256": self.envelope_sha256,
            "reason_codes": list(self.reason_codes),
            "samples": [sample.as_dict() for sample in self.samples],
            "current_projection_preserved": self.current_projection_preserved,
            "recorded_at": self.recorded_at,
        }

    @property
    def record_sha256(self) -> str:
        return _fingerprint(self.as_dict())


@dataclass(frozen=True, slots=True)
class QuarantineReceipt:
    """Evidence for one idempotent quarantine write."""

    state: QuarantineState
    quarantine_id: str
    path: str
    record_sha256: str

    def __post_init__(self) -> None:
        if self.state not in {"stored", "duplicate"}:
            raise QuarantineError("quarantine receipt state is unsupported")
        _required_text(self.quarantine_id, "receipt quarantine_id")
        _required_text(self.path, "receipt path", maximum=1000)
        _optional_sha256(self.record_sha256, "receipt record_sha256")

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": QUARANTINE_SCHEMA_VERSION,
            "record_type": "source_validation_quarantine_receipt",
            "state": self.state,
            "quarantine_id": self.quarantine_id,
            "path": self.path,
            "record_sha256": self.record_sha256,
        }


class QuarantineStore:
    """Atomic, idempotent local store for redacted quarantine records."""

    def __init__(self, root: str | Path, *, max_bytes: int = MAX_QUARANTINE_BYTES) -> None:
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or not 1 <= max_bytes <= MAX_QUARANTINE_BYTES:
            raise QuarantineError(f"max_bytes must be between 1 and {MAX_QUARANTINE_BYTES}")
        self.root = Path(root)
        self.max_bytes = max_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        if self.root.is_symlink() or not self.root.is_dir():
            raise QuarantineError("quarantine root must be a real directory")

    def put(self, record: QuarantineRecord) -> QuarantineReceipt:
        """Store one record without overwriting a different record."""

        if not isinstance(record, QuarantineRecord):
            raise QuarantineError("quarantine store accepts QuarantineRecord values")
        data = _canonical_bytes(record.as_dict()) + b"\n"
        if len(data) > self.max_bytes:
            raise QuarantineError("quarantine record exceeds configured byte bound")
        path = self.root / f"{_safe_filename(record.quarantine_id)}.json"
        if path.is_symlink():
            raise QuarantineError("quarantine record path must not be a symlink")
        if path.exists():
            try:
                existing = _record_from_mapping(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError, QuarantineError) as exc:
                raise QuarantineError("existing quarantine record is unreadable") from exc
            if _without_recorded_at(existing.as_dict()) != _without_recorded_at(record.as_dict()):
                raise QuarantineError("quarantine identity collides with different evidence")
            return QuarantineReceipt("duplicate", record.quarantine_id, str(path), existing.record_sha256)
        try:
            with path.open("x", encoding="utf-8") as handle:
                handle.write(data.decode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
        except FileExistsError:
            return self.put(record)
        except OSError as exc:
            raise QuarantineError("unable to persist quarantine record") from exc
        return QuarantineReceipt("stored", record.quarantine_id, str(path), record.record_sha256)

    def read(self, quarantine_id: str) -> QuarantineRecord:
        """Read and validate one previously retained record."""

        _required_text(quarantine_id, "quarantine_id")
        path = self.root / f"{_safe_filename(quarantine_id)}.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise QuarantineError("quarantine record is unavailable or malformed") from exc
        return _record_from_mapping(value)

    def records(self) -> tuple[QuarantineRecord, ...]:
        """Return records in deterministic filename order."""

        result: list[QuarantineRecord] = []
        for path in sorted(self.root.glob("quarantine-*.json")):
            result.append(self.read(path.stem.replace("quarantine-", "quarantine:", 1)))
        return tuple(result)


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    """Decision plus optional quarantine evidence from the admission seam."""

    report: DriftReport
    quarantine: QuarantineRecord | None = None
    quarantine_receipt: QuarantineReceipt | None = None

    @property
    def state(self) -> ValidationState:
        return self.report.state

    @property
    def accepted(self) -> bool:
        return self.report.accepted

    @property
    def rejected(self) -> bool:
        return self.report.rejected

    @property
    def current_projection_preserved(self) -> bool:
        return self.report.current_projection_preserved

    @property
    def issue_codes(self) -> tuple[str, ...]:
        return self.report.issue_codes

    def as_dict(self) -> dict[str, object]:
        value = self.report.as_dict()
        value["quarantine"] = self.quarantine.as_dict() if self.quarantine is not None else None
        value["quarantine_receipt"] = self.quarantine_receipt.as_dict() if self.quarantine_receipt is not None else None
        return value


def validate_observation_candidate(
    candidate: Mapping[str, object],
    baseline: DriftBaseline | None = None,
) -> DriftReport:
    """Validate one adapter result, including the pinned observation schema.

    A mapping with ``observations`` is treated as a v1 observation envelope;
    a mapping with ``rows`` is treated as a generic adapter batch.  Both paths
    receive row, key, and distribution checks.  The returned report contains
    only safe summaries and never returns the candidate payload.
    """

    if not isinstance(candidate, Mapping):
        return DriftReport(
            "rejected",
            None,
            None,
            None,
            None,
            0,
            (_issue("schema.invalid", "candidate", "schema", None, type(candidate).__name__),),
        )
    expected = baseline or DriftBaseline(schema_version=None)
    issues: list[DriftIssue] = []
    source_id, release_id, artifact_id = _envelope_metadata(candidate)
    try:
        envelope_sha256 = _fingerprint(candidate)
    except DriftValidationError:
        envelope_sha256 = None
        issues.append(_issue("schema.non_json", "candidate", "schema", "JSON object", candidate))

    schema_version = candidate.get("schema_version")
    if expected.schema_version is not None and schema_version != expected.schema_version:
        issues.append(
            _issue("schema.version_drift", "schema_version", "schema_version", expected.schema_version, schema_version)
        )

    looks_like_envelope = "observations" in candidate or schema_version == "hdp.observation-envelope.v1"
    if looks_like_envelope:
        issues.extend(_validate_pinned_envelope(candidate))
    elif "rows" not in candidate:
        issues.append(_issue("schema.missing_rows", "rows", "schema", "array", candidate.get("rows")))

    rows, collection_path = _candidate_rows(candidate)
    row_count = len(rows)
    issues.extend(_row_issues(rows, collection_path, expected))
    issues.extend(_key_issues(candidate, expected, source_id, release_id, artifact_id))
    issues.extend(_distribution_issues(candidate, rows, collection_path, expected.distribution_rules))
    return DriftReport(
        state="rejected" if issues else "accepted",
        source_id=source_id,
        release_id=release_id,
        artifact_id=artifact_id,
        envelope_sha256=envelope_sha256,
        row_count=row_count,
        issues=tuple(_dedupe_issues(issues)),
    )


def validate_drift(candidate: Mapping[str, object], baseline: DriftBaseline | None = None) -> DriftReport:
    """Short alias for :func:`validate_observation_candidate`."""

    return validate_observation_candidate(candidate, baseline)


def build_quarantine_record(
    candidate: Mapping[str, object],
    report: DriftReport,
    *,
    custody_locator: str | None = None,
    artifact_sha256: str | None = None,
    recorded_at: str | None = None,
) -> QuarantineRecord:
    """Build bounded, redacted evidence for a rejected candidate."""

    if not report.rejected:
        raise QuarantineError("only rejected validation reports can be quarantined")
    source_id, release_id, artifact_id = _envelope_metadata(candidate)
    if source_id is None:
        source_id = report.source_id
    if release_id is None:
        release_id = report.release_id
    if artifact_id is None:
        artifact_id = report.artifact_id
    artifact_sha256 = _optional_sha256(artifact_sha256, "artifact_sha256")
    if artifact_sha256 is None:
        artifact_sha256 = _artifact_hash(candidate)
    custody_locator = _optional_id(custody_locator, "custody_locator") or _envelope_custody_locator(candidate)
    identity = {
        "source_id": source_id,
        "release_id": release_id,
        "artifact_id": artifact_id,
        "envelope_sha256": report.envelope_sha256,
        "reason_codes": sorted(report.issue_codes),
    }
    quarantine_id = "quarantine:" + sha256(_canonical_bytes(identity)).hexdigest()[:32]
    samples = _quarantine_samples(candidate, report.issues)
    return QuarantineRecord(
        quarantine_id=quarantine_id,
        source_id=source_id,
        release_id=release_id,
        artifact_id=artifact_id,
        artifact_sha256=artifact_sha256,
        custody_locator=custody_locator,
        envelope_sha256=report.envelope_sha256,
        reason_codes=tuple(sorted(report.issue_codes)),
        samples=samples,
        recorded_at=recorded_at or _utc_now(),
    )


def validate_and_project(
    candidate: Mapping[str, object],
    baseline: DriftBaseline | None = None,
    *,
    quarantine_store: QuarantineStore | None = None,
    current_projection: Callable[[Mapping[str, object]], None] | None = None,
    custody_locator: str | None = None,
    artifact_sha256: str | None = None,
    recorded_at: str | None = None,
) -> ValidationOutcome:
    """Validate, quarantine failures, and invoke projection only on success.

    ``current_projection`` is intentionally caller-owned.  It is never called
    for a rejected candidate, which preserves the last known-good projection.
    A quarantine store is optional for dry-run inspection but required by a
    production caller before treating a rejection as handled.
    """

    report = validate_observation_candidate(candidate, baseline)
    if report.rejected:
        record = build_quarantine_record(
            candidate,
            report,
            custody_locator=custody_locator,
            artifact_sha256=artifact_sha256,
            recorded_at=recorded_at,
        )
        receipt = quarantine_store.put(record) if quarantine_store is not None else None
        return ValidationOutcome(report=report, quarantine=record, quarantine_receipt=receipt)
    if current_projection is not None:
        current_projection(candidate)
        report = replace(report, projection_applied=True, current_projection_preserved=False)
    return ValidationOutcome(report=report)


def _validate_pinned_envelope(candidate: Mapping[str, object]) -> list[DriftIssue]:
    try:
        from shared.contracts.healthcare_data_platform import (
            HealthcareDataPlatformContractError,
            validate_source_scoped_observation_envelope,
        )

        validate_source_scoped_observation_envelope(candidate)
    except HealthcareDataPlatformContractError as exc:
        message = str(exc)
        if "additionalProperties" in message or "unexpected" in message or "Additional properties" in message:
            code = "schema.unknown_field"
        elif "schema_version" in message:
            code = "schema.version_drift"
        elif "lineage" in message or "does not match" in message:
            code = "key.lineage_drift"
        else:
            code = "schema.invalid"
        return [_issue(code, "observation-envelope", "schema", "pinned-v1", message)]
    except (ImportError, OSError) as exc:
        return [_issue("schema.validator_unavailable", "observation-envelope", "schema", "available", str(exc))]
    return []


def _row_issues(rows: list[object], collection_path: str, baseline: DriftBaseline) -> list[DriftIssue]:
    issues: list[DriftIssue] = []
    if not rows:
        issues.append(_issue("row.empty", collection_path, "row", "at least one row", 0))
    identifiers: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            issues.append(_issue("row.malformed", f"{collection_path}[{index}]", "row", "object", row))
            continue
        identifier = _row_identifier_optional(row, baseline.row_id_field)
        if identifier is None:
            issues.append(
                _issue(
                    "row.missing_key",
                    f"{collection_path}[{index}].{baseline.row_id_field}",
                    "row",
                    "non-empty string",
                    row.get(baseline.row_id_field),
                )
            )
            continue
        identifiers.append(identifier)
    duplicates = sorted({identifier for identifier in identifiers if identifiers.count(identifier) > 1})
    if duplicates:
        issues.append(
            _issue(
                "row.duplicate_key",
                f"{collection_path}[] .{baseline.row_id_field}".replace(" ", ""),
                "row",
                "unique",
                len(duplicates),
            )
        )
    if baseline.expected_row_count is not None:
        expected_count = baseline.expected_row_count
        if expected_count == 0:
            if len(rows) != 0:
                issues.append(
                    _issue("row.denominator_drift", collection_path, "distribution", expected_count, len(rows))
                )
        else:
            delta_ratio = abs(len(rows) - expected_count) / expected_count
            if delta_ratio > baseline.max_row_count_delta_ratio:
                issues.append(
                    _issue("row.denominator_drift", collection_path, "distribution", expected_count, len(rows))
                )
    if baseline.expected_observation_ids:
        expected_ids = list(baseline.expected_observation_ids)
        actual_set = set(identifiers)
        expected_set = set(expected_ids)
        missing = expected_set - actual_set
        unexpected = actual_set - expected_set
        if missing:
            issues.append(_issue("row.missing_key", collection_path, "row", len(expected_set), len(actual_set)))
        if unexpected:
            issues.append(_issue("row.unexpected_key", collection_path, "row", len(expected_set), len(actual_set)))
        if not missing and not unexpected and identifiers != expected_ids:
            issues.append(_issue("row.order_drift", collection_path, "row", expected_ids, identifiers))
    return issues


def _key_issues(
    candidate: Mapping[str, object],
    baseline: DriftBaseline,
    source_id: str | None,
    release_id: str | None,
    artifact_id: str | None,
) -> list[DriftIssue]:
    issues: list[DriftIssue] = []
    if baseline.source_id is not None and source_id != baseline.source_id:
        issues.append(_issue("key.source_drift", "source_release.source_id", "key", baseline.source_id, source_id))
    if baseline.expected_release_id is not None and release_id != baseline.expected_release_id:
        issues.append(
            _issue("key.release_drift", "source_release.release_id", "key", baseline.expected_release_id, release_id)
        )
    if baseline.expected_artifact_id is not None and artifact_id != baseline.expected_artifact_id:
        issues.append(
            _issue("key.artifact_drift", "artifact.artifact_id", "key", baseline.expected_artifact_id, artifact_id)
        )
    key_digest = _candidate_key_digest(candidate)
    if baseline.expected_key_digest is not None and key_digest != baseline.expected_key_digest:
        issues.append(_issue("key.identity_drift", "lineage", "key", baseline.expected_key_digest, key_digest))
    if "observations" in candidate:
        issues.extend(_envelope_key_issues(candidate))
    return issues


def _envelope_key_issues(candidate: Mapping[str, object]) -> list[DriftIssue]:
    issues: list[DriftIssue] = []
    source_release = _mapping(candidate.get("source_release"))
    artifact = _mapping(candidate.get("artifact"))
    receipt = _mapping(candidate.get("receipt"))
    activity = _mapping(candidate.get("activity"))
    lineage = _mapping(candidate.get("lineage"))
    source_id = source_release.get("source_id")
    release_id = source_release.get("release_id")
    artifact_id = artifact.get("artifact_id")
    receipt_id = receipt.get("receipt_id")
    activity_id = activity.get("activity_id")
    comparisons = (
        ("artifact.release_ref", artifact.get("release_ref"), release_id),
        ("receipt.source_release_ref", receipt.get("source_release_ref"), release_id),
        ("receipt.artifact_ref", receipt.get("artifact_ref"), artifact_id),
        ("lineage.source_release_ref", lineage.get("source_release_ref"), release_id),
        ("lineage.artifact_ref", lineage.get("artifact_ref"), artifact_id),
        ("lineage.receipt_ref", lineage.get("receipt_ref"), receipt_id),
        ("lineage.activity_ref", lineage.get("activity_ref"), activity_id),
    )
    for path, actual, expected in comparisons:
        if actual != expected:
            issues.append(_issue("key.lineage_drift", path, "key", expected, actual))
    output_refs = activity.get("output_artifact_refs")
    if output_refs != [artifact_id]:
        issues.append(
            _issue("key.activity_output_drift", "activity.output_artifact_refs", "key", [artifact_id], output_refs)
        )
    observations = candidate.get("observations")
    if isinstance(observations, list):
        for index, item in enumerate(observations):
            if not isinstance(item, Mapping):
                continue
            scope = _mapping(item.get("source_scope"))
            scope_comparisons = (
                ("source_id", scope.get("source_id"), source_id),
                ("release_ref", scope.get("release_ref"), release_id),
                ("artifact_ref", scope.get("artifact_ref"), artifact_id),
                ("activity_ref", item.get("activity_ref"), activity_id),
                ("receipt_ref", item.get("receipt_ref"), receipt_id),
            )
            for suffix, actual, expected in scope_comparisons:
                if actual != expected:
                    issues.append(_issue("key.scope_drift", f"observations[{index}].{suffix}", "key", expected, actual))
    authority_limits = candidate.get("authority_limits")
    if isinstance(authority_limits, Mapping):
        enabled = [key for key, value in authority_limits.items() if value is not False]
        if enabled:
            issues.append(_issue("key.authority_escalation", "authority_limits", "key", False, len(enabled)))
    return issues


def _distribution_issues(
    candidate: Mapping[str, object],
    rows: list[object],
    collection_path: str,
    rules: tuple[DistributionRule, ...],
) -> list[DriftIssue]:
    issues: list[DriftIssue] = []
    for rule in rules:
        values = _rule_values(candidate, rows, collection_path, rule.path)
        actual_counts: dict[str, int] = {}
        for value in values:
            category = _category(value)
            actual_counts[category] = actual_counts.get(category, 0) + 1
        actual_total = len(values)
        if rule.expected_total is not None and actual_total != rule.expected_total:
            issues.append(
                _issue(
                    "distribution.denominator_drift" if rule.denominator else "distribution.total_drift",
                    rule.path,
                    "distribution",
                    rule.expected_total,
                    actual_total,
                )
            )
        for category, expected_count in rule.expected_counts.items():
            actual_count = actual_counts.get(category, 0)
            if expected_count == 0:
                if actual_count:
                    issues.append(_issue("distribution.category_drift", rule.path, "distribution", 0, actual_count))
                continue
            ratio = actual_count / expected_count
            if ratio < rule.min_ratio or ratio > rule.max_ratio:
                issues.append(
                    _issue(
                        "distribution.denominator_drift" if rule.denominator else "distribution.category_drift",
                        rule.path,
                        "distribution",
                        expected_count,
                        actual_count,
                    )
                )
        unexpected = set(actual_counts) - set(rule.expected_counts)
        if unexpected:
            issues.append(
                _issue(
                    "distribution.unexpected_category",
                    rule.path,
                    "distribution",
                    len(rule.expected_counts),
                    len(actual_counts),
                )
            )
    return issues


def _rule_values(candidate: Mapping[str, object], rows: list[object], collection_path: str, path: str) -> list[object]:
    if path.startswith("observations[].") or path.startswith("rows[]."):
        relative = path.split("[].", 1)[1]
        return [
            _path_value(row, relative.split("."))
            for row in rows
            if isinstance(row, Mapping) and _path_value(row, relative.split(".")) is not _MISSING
        ]
    if "[]." in path:
        prefix, relative = path.split("[].", 1)
        values = _path_value(candidate, prefix.split("."))
        if isinstance(values, list):
            return [
                _path_value(item, relative.split("."))
                for item in values
                if _path_value(item, relative.split(".")) is not _MISSING
            ]
        return []
    values = [_path_value(row, path.split(".")) for row in rows if isinstance(row, Mapping)]
    return [value for value in values if value is not _MISSING]


_MISSING = object()


def _candidate_rows(candidate: Mapping[str, object]) -> tuple[list[object], str]:
    observations = candidate.get("observations")
    if isinstance(observations, list):
        return observations, "observations"
    rows = candidate.get("rows")
    if isinstance(rows, list):
        return rows, "rows"
    return [], "observations" if "observations" in candidate else "rows"


def _envelope_metadata(candidate: Mapping[str, object]) -> tuple[str | None, str | None, str | None]:
    source_release = _mapping(candidate.get("source_release"))
    artifact = _mapping(candidate.get("artifact"))
    return (
        _text(source_release.get("source_id")),
        _text(source_release.get("release_id")),
        _text(artifact.get("artifact_id")),
    )


def _envelope_custody_locator(candidate: Mapping[str, object]) -> str | None:
    artifact = _mapping(candidate.get("artifact"))
    custody = _mapping(artifact.get("custody"))
    return _text(custody.get("locator"))


def _artifact_hash(candidate: Mapping[str, object]) -> str | None:
    artifact = _mapping(candidate.get("artifact"))
    value = artifact.get("content_sha256")
    return value if isinstance(value, str) and _SHA256.fullmatch(value) else None


def _candidate_key_digest(candidate: Mapping[str, object]) -> str | None:
    source_release = _mapping(candidate.get("source_release"))
    artifact = _mapping(candidate.get("artifact"))
    lineage = _mapping(candidate.get("lineage"))
    replay = _mapping(lineage.get("replay"))
    values = {
        "record_id": candidate.get("record_id"),
        "source_id": source_release.get("source_id"),
        "release_id": source_release.get("release_id"),
        "artifact_id": artifact.get("artifact_id"),
        "lineage_id": lineage.get("lineage_id"),
        "idempotency_key": replay.get("idempotency_key"),
    }
    if all(value is None for value in values.values()):
        return None
    try:
        return _fingerprint(values)
    except DriftValidationError:
        return None


def _envelope_key_values(candidate: Mapping[str, object]) -> dict[str, object]:
    source_release = _mapping(candidate.get("source_release"))
    artifact = _mapping(candidate.get("artifact"))
    lineage = _mapping(candidate.get("lineage"))
    replay = _mapping(lineage.get("replay"))
    return {
        "record_id": candidate.get("record_id"),
        "source_id": source_release.get("source_id"),
        "release_id": source_release.get("release_id"),
        "artifact_id": artifact.get("artifact_id"),
        "lineage_id": lineage.get("lineage_id"),
        "idempotency_key": replay.get("idempotency_key"),
    }


def _quarantine_samples(
    candidate: Mapping[str, object], issues: tuple[DriftIssue, ...]
) -> tuple[QuarantineSample, ...]:
    samples: list[QuarantineSample] = []
    seen: set[str] = set()
    for issue in issues[:MAX_QUARANTINE_SAMPLES]:
        if issue.path in seen:
            continue
        seen.add(issue.path)
        value = _path_value(candidate, _path_parts(issue.path))
        if value is _MISSING:
            summary = {"present": False}
        else:
            summary = {"present": True, "value": _safe_summary(value)}
        samples.append(QuarantineSample(issue.path, summary))
    if not samples:
        samples.append(QuarantineSample("candidate", {"present": True, "value": _safe_summary(candidate)}))
    return tuple(samples)


def _path_parts(path: str) -> list[str]:
    path = path.replace("[]", "")
    return [part for part in path.split(".") if part and part not in {"observation-envelope"}]


def _path_value(value: object, parts: Sequence[str]) -> object:
    current = value
    for part in parts:
        if isinstance(current, Mapping):
            if part not in current:
                return _MISSING
            current = current[part]
        elif isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                return _MISSING
            if index < 0 or index >= len(current):
                return _MISSING
            current = current[index]
        else:
            return _MISSING
    return current


def _row_identifier(row: object, field: str) -> str:
    identifier = _row_identifier_optional(row, field)
    if identifier is None:
        raise DriftValidationError(f"baseline row is missing {field}")
    return identifier


def _row_identifier_optional(row: object, field: str) -> str | None:
    if not isinstance(row, Mapping):
        return None
    value = _path_value(row, field.split("."))
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _category(value: object) -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return "<null>"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return type(value).__name__


def _issue(code: str, path: str, kind: DriftKind, expected: object, actual: object) -> DriftIssue:
    return DriftIssue(kind=kind, code=code, path=path, expected=expected, actual=actual)


def _dedupe_issues(issues: Iterable[DriftIssue]) -> list[DriftIssue]:
    result: list[DriftIssue] = []
    seen: set[tuple[str, str, str]] = set()
    for issue in issues:
        key = (issue.kind, issue.code, issue.path)
        if key not in seen:
            seen.add(key)
            result.append(issue)
    return result[:MAX_REASON_CODES]


def _safe_filename(value: str) -> str:
    return "quarantine-" + re.sub(r"[^A-Za-z0-9._-]+", "-", value.removeprefix("quarantine:"))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise QuarantineError(f"{label} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QuarantineError(f"{label} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QuarantineError(f"{label} must include a timezone")
    return value


def _record_from_mapping(value: object) -> QuarantineRecord:
    if not isinstance(value, Mapping):
        raise QuarantineError("quarantine record must be an object")
    if value.get("schema_version") != QUARANTINE_SCHEMA_VERSION or value.get("record_type") != QUARANTINE_RECORD_TYPE:
        raise QuarantineError("quarantine record schema is unsupported")
    raw_samples = value.get("samples")
    if not isinstance(raw_samples, list):
        raise QuarantineError("quarantine samples must be an array")
    samples: list[QuarantineSample] = []
    for raw in raw_samples:
        if not isinstance(raw, Mapping):
            raise QuarantineError("quarantine sample must be an object")
        summary = raw.get("summary")
        if not isinstance(summary, Mapping):
            raise QuarantineError("quarantine sample summary must be an object")
        samples.append(QuarantineSample(cast(str, raw.get("path")), cast(Mapping[str, SafeSummary], summary)))
    reasons = value.get("reason_codes")
    if not isinstance(reasons, list):
        raise QuarantineError("quarantine reason_codes must be an array")
    return QuarantineRecord(
        quarantine_id=cast(str, value.get("quarantine_id")),
        source_id=cast(str | None, value.get("source_id")),
        release_id=cast(str | None, value.get("release_id")),
        artifact_id=cast(str | None, value.get("artifact_id")),
        artifact_sha256=cast(str | None, value.get("artifact_sha256")),
        custody_locator=cast(str | None, value.get("custody_locator")),
        envelope_sha256=cast(str | None, value.get("envelope_sha256")),
        reason_codes=tuple(cast(str, reason) for reason in reasons),
        samples=tuple(samples),
        current_projection_preserved=value.get("current_projection_preserved", True),
        recorded_at=cast(str, value.get("recorded_at", "")),
    )


def _without_recorded_at(value: Mapping[str, object]) -> dict[str, object]:
    """Compare retries by evidence identity while retaining first-seen time."""

    return {key: item for key, item in value.items() if key != "recorded_at"}


__all__ = [
    "DistributionRule",
    "DriftBaseline",
    "DriftIssue",
    "DriftReport",
    "DriftValidationError",
    "QuarantineError",
    "QuarantineReceipt",
    "QuarantineRecord",
    "QuarantineSample",
    "QuarantineStore",
    "QUARANTINE_RECORD_TYPE",
    "QUARANTINE_SCHEMA_VERSION",
    "VALIDATION_SCHEMA_VERSION",
    "ValidationOutcome",
    "build_quarantine_record",
    "validate_and_project",
    "validate_drift",
    "validate_observation_candidate",
]
