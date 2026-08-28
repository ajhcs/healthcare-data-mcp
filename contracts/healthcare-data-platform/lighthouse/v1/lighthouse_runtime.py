"""Bounded, fixture-only lighthouse acquisition and delivery reference.

This module intentionally lives beside the contract rather than in a runtime
service.  It exercises the source-native custody and envelope invariants with
an in-memory disposable receiver; callers must provide an approved fixture and
cannot turn this spike into a live or production writer accidentally.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, Mapping


FixtureState = Literal["no_op", "changed", "failed_probe"]
DeliveryState = Literal["no_op", "changed", "interrupted", "duplicate", "failed_probe"]

_ROOT = Path(__file__).resolve().parent
_FIXTURE_SCHEMA = _ROOT / "lighthouse.schema.json"
_ENVELOPE_SCHEMA = _ROOT / "lighthouse-envelope.schema.json"
_SHA256_PREFIX = "sha256:"


class LighthouseContractError(ValueError):
    """Raised when a fixture, budget, envelope, or delivery is unsafe."""


@dataclass(frozen=True)
class LighthouseBudget:
    """Hard limits for one deterministic probe and delivery."""

    max_bytes: int
    max_chunks: int
    chunk_size: int
    max_seconds: int

    @classmethod
    def from_fixture(cls, fixture: Mapping[str, Any]) -> "LighthouseBudget":
        raw = fixture.get("budget")
        if not isinstance(raw, Mapping):
            raise LighthouseContractError("fixture budget must be an object")
        try:
            values = {key: int(raw[key]) for key in ("max_bytes", "max_chunks", "chunk_size", "max_seconds")}
        except (KeyError, TypeError, ValueError) as exc:
            raise LighthouseContractError("fixture budget is incomplete") from exc
        budget = cls(**values)
        if budget.max_bytes < 1 or budget.max_bytes > 131072:
            raise LighthouseContractError("max_bytes is outside the bounded lighthouse range")
        if budget.max_chunks < 1 or budget.max_chunks > 128:
            raise LighthouseContractError("max_chunks is outside the bounded lighthouse range")
        if budget.chunk_size < 1 or budget.chunk_size > 65536:
            raise LighthouseContractError("chunk_size is outside the bounded lighthouse range")
        if budget.max_seconds < 1 or budget.max_seconds > 60:
            raise LighthouseContractError("max_seconds is outside the bounded lighthouse range")
        return budget


@dataclass(frozen=True)
class LighthouseReceipt:
    """Secret-free evidence returned by a probe or delivery attempt."""

    state: DeliveryState
    source_id: str
    release_id: str
    release_fingerprint: str
    response_fingerprint: str
    prior_release_fingerprint: str | None
    fixture_id: str
    idempotency_key: str | None
    artifact_id: str | None
    artifact_sha256: str | None
    envelope_id: str | None
    acknowledged: bool
    received_bytes: int
    chunk_count: int
    elapsed_ms: int
    max_bytes: int
    max_chunks: int
    max_seconds: int
    failure_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-safe receipt suitable for an evidence ledger."""

        return {
            "state": self.state,
            "source_id": self.source_id,
            "release_id": self.release_id,
            "release_fingerprint": self.release_fingerprint,
            "response_fingerprint": self.response_fingerprint,
            "prior_release_fingerprint": self.prior_release_fingerprint,
            "fixture_id": self.fixture_id,
            "idempotency_key": self.idempotency_key,
            "artifact_id": self.artifact_id,
            "artifact_sha256": self.artifact_sha256,
            "envelope_id": self.envelope_id,
            "acknowledged": self.acknowledged,
            "received_bytes": self.received_bytes,
            "chunk_count": self.chunk_count,
            "elapsed_ms": self.elapsed_ms,
            "budget": {
                "max_bytes": self.max_bytes,
                "max_chunks": self.max_chunks,
                "max_seconds": self.max_seconds,
            },
            "failure_reason": self.failure_reason,
        }


@dataclass
class _StoredDelivery:
    envelope: dict[str, Any]
    body: bytes


def _schema_validate(value: object, schema_path: Path, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator
    except ImportError as exc:  # pragma: no cover - dependency installation failure
        raise LighthouseContractError("jsonschema is required for lighthouse validation") from exc
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LighthouseContractError(f"unable to read {label} schema") from exc
    errors = sorted(Draft202012Validator(schema).iter_errors(value), key=lambda error: list(error.absolute_path))
    if errors:
        error = errors[0]
        location = ".".join(str(item) for item in error.absolute_path)
        suffix = f" at {location}" if location else ""
        raise LighthouseContractError(f"{label} failed schema validation{suffix}: {error.message}")


def _strict_object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LighthouseContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_fixture(path: Path) -> dict[str, Any]:
    """Load one fixture with duplicate-key rejection and schema validation."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_strict_object_pairs)
    except LighthouseContractError:
        raise
    except (OSError, json.JSONDecodeError) as exc:
        raise LighthouseContractError(f"unable to read fixture: {path}") from exc
    if not isinstance(value, dict):
        raise LighthouseContractError("lighthouse fixture must be an object")
    validate_fixture(value)
    return value


def validate_fixture(fixture: Mapping[str, Any]) -> None:
    """Validate a fixture and its byte-level artifact claims."""

    value = dict(fixture)
    _schema_validate(value, _FIXTURE_SCHEMA, "lighthouse fixture")
    budget = LighthouseBudget.from_fixture(value)
    state = value["probe_state"]
    body = value["body"]
    artifact = value["artifact"]
    if not isinstance(artifact, Mapping):
        raise LighthouseContractError("fixture artifact must be an object")
    if state == "changed":
        if not isinstance(body, str):
            raise LighthouseContractError("changed fixture must contain a body")
        raw = body.encode("utf-8")
        actual_hash = _SHA256_PREFIX + sha256(raw).hexdigest()
        if artifact["byte_length"] != len(raw) or artifact["content_sha256"] != actual_hash:
            raise LighthouseContractError("changed fixture artifact hash or length does not match its body")
        if len(raw) > budget.max_bytes:
            raise LighthouseContractError("fixture body exceeds max_bytes")
        expected_chunks = (len(raw) + budget.chunk_size - 1) // budget.chunk_size
        if expected_chunks > budget.max_chunks:
            raise LighthouseContractError("fixture body exceeds max_chunks")
    elif artifact["byte_length"] != 0:
        raise LighthouseContractError("no-op and failed probes must not claim received bytes")


class DisposableReceiver:
    """In-memory receiver that acknowledges only fully verified envelopes."""

    def __init__(self) -> None:
        self._deliveries: dict[str, _StoredDelivery] = {}
        self._artifacts: dict[str, bytes] = {}
        self._partial: dict[str, bytearray] = {}

    @property
    def deliveries(self) -> Mapping[str, _StoredDelivery]:
        """Expose read-only delivery evidence for assertions and receipts."""

        return self._deliveries

    @property
    def artifacts(self) -> Mapping[str, bytes]:
        """Expose immutable artifact bytes held by the disposable receiver."""

        return self._artifacts

    def stream(
        self,
        envelope: Mapping[str, Any],
        chunks: list[bytes],
        *,
        interrupt_after_chunks: int | None = None,
        budget: LighthouseBudget,
        deadline: float | None = None,
    ) -> tuple[DeliveryState, bool, int, int]:
        """Receive bounded chunks, returning state, ack, bytes, and chunk count."""

        _schema_validate(dict(envelope), _ENVELOPE_SCHEMA, "lighthouse envelope")
        delivery = envelope["delivery"]
        if not isinstance(delivery, Mapping):
            raise LighthouseContractError("envelope delivery must be an object")
        idempotency_key = delivery["idempotency_key"]
        artifact = envelope["artifact"]
        if not isinstance(idempotency_key, str) or not isinstance(artifact, Mapping):
            raise LighthouseContractError("envelope delivery lineage is malformed")
        artifact_id = artifact["artifact_id"]
        if idempotency_key in self._deliveries:
            stored = self._deliveries[idempotency_key]
            if dict(stored.envelope) != dict(envelope):
                raise LighthouseContractError("idempotency key was reused with different envelope content")
            return "duplicate", True, len(stored.body), int(delivery["chunk_count"])

        if len(chunks) > budget.max_chunks:
            raise LighthouseContractError("delivery exceeds max_chunks")
        declared_chunk_count = delivery["chunk_count"]
        declared_chunk_size = delivery["chunk_size"]
        if declared_chunk_count != len(chunks):
            raise LighthouseContractError("delivery chunk_count does not match received chunks")
        if declared_chunk_size != budget.chunk_size:
            raise LighthouseContractError("delivery chunk_size does not match the fixture budget")
        received = bytearray()
        for index, chunk in enumerate(chunks, start=1):
            if not isinstance(chunk, bytes) or not chunk:
                raise LighthouseContractError("delivery chunks must be non-empty bytes")
            if len(chunk) > declared_chunk_size:
                raise LighthouseContractError("delivery chunk exceeds declared chunk_size")
            if index < declared_chunk_count and len(chunk) != declared_chunk_size:
                raise LighthouseContractError("non-final delivery chunks must use declared chunk_size")
            if deadline is not None and perf_counter() >= deadline:
                self._partial[idempotency_key] = received
                return "interrupted", False, len(received), index - 1
            received.extend(chunk)
            if len(received) > budget.max_bytes:
                raise LighthouseContractError("delivery exceeds max_bytes")
            if interrupt_after_chunks is not None and index >= interrupt_after_chunks:
                self._partial[idempotency_key] = received
                return "interrupted", False, len(received), index

        if deadline is not None and perf_counter() >= deadline:
            self._partial[idempotency_key] = received
            return "interrupted", False, len(received), len(chunks)

        expected_length = artifact["byte_length"]
        expected_hash = artifact["content_sha256"]
        actual_hash = _SHA256_PREFIX + sha256(received).hexdigest()
        if len(received) != expected_length or actual_hash != expected_hash:
            raise LighthouseContractError("received bytes do not match the envelope artifact claim")
        if artifact_id in self._artifacts and self._artifacts[artifact_id] != bytes(received):
            raise LighthouseContractError("artifact id collision with different bytes")

        self._artifacts[str(artifact_id)] = bytes(received)
        self._deliveries[idempotency_key] = _StoredDelivery(dict(envelope), bytes(received))
        self._partial.pop(idempotency_key, None)
        return "changed", True, len(received), len(chunks)


class LighthouseSpike:
    """Run no-op, changed, failed-probe, interruption, and replay semantics."""

    def __init__(self, receiver: DisposableReceiver | None = None) -> None:
        self.receiver = receiver or DisposableReceiver()

    def run(
        self,
        fixture: Mapping[str, Any],
        *,
        prior_release_fingerprint: str | None = None,
        interrupt_after_chunks: int | None = None,
    ) -> LighthouseReceipt:
        """Execute one fixture probe without live I/O or production side effects."""

        started = perf_counter()
        validate_fixture(fixture)
        budget = LighthouseBudget.from_fixture(fixture)
        state = fixture["probe_state"]
        source_id = str(fixture["source_id"])
        release_id = str(fixture["release_id"])
        release_fingerprint = str(fixture["release_fingerprint"])
        response_fingerprint = str(fixture["response_fingerprint"])
        prior = prior_release_fingerprint or fixture.get("prior_release_fingerprint")
        common = {
            "source_id": source_id,
            "release_id": release_id,
            "release_fingerprint": release_fingerprint,
            "response_fingerprint": response_fingerprint,
            "prior_release_fingerprint": str(prior) if prior is not None else None,
            "fixture_id": str(fixture["fixture_id"]),
            "max_bytes": budget.max_bytes,
            "max_chunks": budget.max_chunks,
            "max_seconds": budget.max_seconds,
        }
        if state == "failed_probe":
            return LighthouseReceipt(
                **common,
                state="failed_probe",
                idempotency_key=None,
                artifact_id=None,
                artifact_sha256=None,
                envelope_id=None,
                acknowledged=False,
                received_bytes=0,
                chunk_count=0,
                elapsed_ms=_elapsed_ms(started),
                failure_reason=str(fixture["failure_reason"]),
            )

        if state == "no_op" or prior == release_fingerprint:
            return LighthouseReceipt(
                **common,
                state="no_op",
                idempotency_key=None,
                artifact_id=None,
                artifact_sha256=None,
                envelope_id=None,
                acknowledged=False,
                received_bytes=0,
                chunk_count=0,
                elapsed_ms=_elapsed_ms(started),
            )

        body = fixture["body"]
        if not isinstance(body, str):  # defensive after validate_fixture
            raise LighthouseContractError("changed fixture body is missing")
        raw = body.encode("utf-8")
        artifact = fixture["artifact"]
        if not isinstance(artifact, Mapping):
            raise LighthouseContractError("fixture artifact is missing")
        artifact_id = str(artifact["artifact_id"])
        idempotency_key = f"idempotency:lighthouse:{artifact_id.removeprefix('artifact:lighthouse:')}"
        envelope_id = f"envelope:lighthouse:{artifact_id.removeprefix('artifact:lighthouse:')}"
        records = _records_from_body(body)
        envelope = {
            "schema_version": "hdp.lighthouse-envelope.v1",
            "record_type": "lighthouse_observation_envelope",
            "envelope_id": envelope_id,
            "source_id": source_id,
            "source_url": str(fixture["source_url"]),
            "release_id": release_id,
            "release_fingerprint": release_fingerprint,
            "response_fingerprint": response_fingerprint,
            "artifact": {
                "artifact_id": artifact_id,
                "release_ref": release_id,
                "content_sha256": str(artifact["content_sha256"]),
                "byte_length": len(raw),
                "custody_locator": f"receiver://lighthouse/{artifact_id.removeprefix('artifact:lighthouse:')}",
                "immutable": True,
            },
            "records": records,
            "delivery": {
                "idempotency_key": idempotency_key,
                "chunk_count": (len(raw) + budget.chunk_size - 1) // budget.chunk_size,
                "chunk_size": budget.chunk_size,
                "acknowledged": True,
            },
            "authority_limits": {
                "publication_allowed": False,
                "current_projection_allowed": False,
                "production_allowed": False,
            },
        }
        chunks = [raw[index : index + budget.chunk_size] for index in range(0, len(raw), budget.chunk_size)]
        delivery_state, acknowledged, received_bytes, chunk_count = self.receiver.stream(
            envelope,
            chunks,
            interrupt_after_chunks=interrupt_after_chunks,
            budget=budget,
            deadline=started + budget.max_seconds,
        )
        return LighthouseReceipt(
            **common,
            state=delivery_state,
            idempotency_key=idempotency_key,
            artifact_id=artifact_id,
            artifact_sha256=str(artifact["content_sha256"]),
            envelope_id=envelope_id,
            acknowledged=acknowledged,
            received_bytes=received_bytes,
            chunk_count=chunk_count,
            elapsed_ms=_elapsed_ms(started),
        )


def _elapsed_ms(started: float) -> int:
    return max(0, int((perf_counter() - started) * 1000))


def _records_from_body(body: str) -> list[dict[str, Any]]:
    try:
        value = json.loads(body)
    except json.JSONDecodeError as exc:
        raise LighthouseContractError("changed fixture body must be JSON") from exc
    if not isinstance(value, Mapping) or not isinstance(value.get("facilities"), list):
        raise LighthouseContractError("changed fixture body must contain a facilities array")
    records: list[dict[str, Any]] = []
    for index, item in enumerate(value["facilities"], start=1):
        if not isinstance(item, Mapping):
            raise LighthouseContractError("facility row must be an object")
        records.append({"source_row_id": f"row:facility-{index}", "fields": dict(item)})
    if not records:
        raise LighthouseContractError("changed fixture must contain at least one source row")
    return records


__all__ = [
    "DisposableReceiver",
    "LighthouseBudget",
    "LighthouseContractError",
    "LighthouseReceipt",
    "LighthouseSpike",
    "load_fixture",
    "validate_fixture",
]
