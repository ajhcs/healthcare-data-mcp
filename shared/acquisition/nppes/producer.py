"""Bounded, transport-neutral producer for an NPPES V2 full baseline.

The producer consumes caller-owned byte iterables.  It computes content and
row fingerprints while parsing only the source-native identifier metadata
needed for later review.  It never downloads NPPES, writes custody or a
database, advances a cursor, promotes an identity, or mutates a current
projection.
"""

from __future__ import annotations

import codecs
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
import re
import time
from typing import Callable, Iterable, Literal, Mapping, Protocol, cast

from shared.acquisition.nppes.contracts import (
    MAX_DEACTIVATION_SAMPLES,
    MAX_IDENTIFIER_SAMPLES,
    MAX_NPPES_BYTES,
    MAX_NPPES_CHUNK_BYTES,
    MAX_NPPES_CHUNKS,
    MAX_NPPES_SECONDS,
    NPPES_SOURCE_ID,
    NppesBaselineReceipt,
    NppesCatalog,
    NppesCatalogEntry,
    NppesContractError,
    NppesDeactivationOpportunity,
    NppesFileDescriptor,
    NppesFileKind,
    NppesFileReceipt,
    NppesFileState,
    NppesOutcomeState,
    NppesReleaseDescriptor,
    NppesReplayConflictError,
    NppesSchemaDriftError,
    NppesSourceRow,
)


FileChunks = Iterable[bytes]
RowSink = Callable[[NppesSourceRow], None]

_NPI = re.compile(r"^[0-9]{10}$")
_HEADER = re.compile(r"[^a-z0-9]+")
_MAX_ROW_BYTES = 1_048_576
_FILE_KINDS = ("provider", "location", "endpoint", "reference", "deactivation")


class NppesProducerError(NppesContractError):
    """Raised for invalid producer inputs that cannot be represented safely."""


class NppesStreamInterrupted(NppesProducerError):
    """Raised internally when a source stream exceeds its explicit bounds."""


class NppesRowSink(Protocol):
    """Callable protocol for staging exact source-row identity metadata."""

    def __call__(self, row: NppesSourceRow) -> None:
        """Receive one source row without its payload fields."""


class _Digest(Protocol):
    """Small structural type for hashlib-compatible streaming digests."""

    def update(self, value: bytes, /) -> None:
        """Add bytes to the digest."""

    def hexdigest(self) -> str:
        """Return the hexadecimal digest."""

        ...


class _Decoder(Protocol):
    """Incremental text decoder shape used by the bounded CSV parser."""

    def decode(self, input: bytes, final: bool = False, /) -> str:
        """Decode one bounded byte chunk."""

        ...


@dataclass(frozen=True, slots=True)
class NppesStreamBudget:
    """Byte, chunk, line, and monotonic-time limits for one source file."""

    max_bytes: int = MAX_NPPES_BYTES
    max_chunks: int = MAX_NPPES_CHUNKS
    max_seconds: int = MAX_NPPES_SECONDS
    max_chunk_bytes: int = MAX_NPPES_CHUNK_BYTES

    def __post_init__(self) -> None:
        for name, value, lower, upper in (
            ("max_bytes", self.max_bytes, 1, MAX_NPPES_BYTES),
            ("max_chunks", self.max_chunks, 1, MAX_NPPES_CHUNKS),
            ("max_seconds", self.max_seconds, 1, MAX_NPPES_SECONDS),
            ("max_chunk_bytes", self.max_chunk_bytes, 1, MAX_NPPES_CHUNK_BYTES),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
                raise NppesProducerError(f"{name} must be an integer between {lower} and {upper}")
        if self.max_chunk_bytes > self.max_bytes:
            raise NppesProducerError("max_chunk_bytes cannot exceed max_bytes")


@dataclass(slots=True)
class _ParsedFile:
    """Ephemeral parser state; it is converted to a bounded receipt."""

    file_kind: NppesFileKind
    evidence_locator: str
    on_row: RowSink | None
    decoder: _Decoder
    text_buffer: str = ""
    header: tuple[str, ...] | None = None
    header_indexes: dict[str, int] | None = None
    line_number: int = 0
    row_count: int = 0
    identifier_count: int = 0
    deactivation_count: int = 0
    identifier_samples: list[str] | None = None
    deactivation_opportunities: list[NppesDeactivationOpportunity] | None = None
    source_row_ids: set[str] | None = None
    row_digest: _Digest | None = None

    def __post_init__(self) -> None:
        self.identifier_samples = []
        self.deactivation_opportunities = []
        self.source_row_ids = set()
        self.row_digest = sha256()

    def feed(self, chunk: bytes) -> None:
        try:
            decoded = self.decoder.decode(chunk, False)
        except UnicodeDecodeError as exc:
            raise NppesSchemaDriftError("source file is not valid UTF-8") from exc
        self.text_buffer += decoded
        if len(self.text_buffer.encode("utf-8")) > _MAX_ROW_BYTES:
            raise NppesSchemaDriftError("source row exceeds bounded parser line size")
        while "\n" in self.text_buffer:
            line, self.text_buffer = self.text_buffer.split("\n", 1)
            self._parse_line(line.rstrip("\r"))
            if len(self.text_buffer.encode("utf-8")) > _MAX_ROW_BYTES:
                raise NppesSchemaDriftError("source row exceeds bounded parser line size")

    def finish(self) -> None:
        try:
            self.text_buffer += self.decoder.decode(b"", True)
        except UnicodeDecodeError as exc:
            raise NppesSchemaDriftError("source file ends with an incomplete UTF-8 sequence") from exc
        if self.text_buffer:
            self._parse_line(self.text_buffer.rstrip("\r"))
        if self.header is None:
            raise NppesSchemaDriftError("source file is missing a CSV header")
        if self.file_kind == "provider" and self.row_count == 0:
            raise NppesSchemaDriftError("provider file contains no rows")

    def _parse_line(self, line: str) -> None:
        self.line_number += 1
        if not line.strip():
            return
        try:
            values = next(csv.reader([line]))
        except (csv.Error, StopIteration) as exc:
            raise NppesSchemaDriftError(f"malformed CSV row at line {self.line_number}") from exc
        if self.header is None:
            self._set_header(values)
            return
        if len(values) != len(self.header):
            raise NppesSchemaDriftError(f"CSV column count changed at line {self.line_number}")
        assert self.header_indexes is not None
        npi = values[self.header_indexes["npi"]]
        if _NPI.fullmatch(npi) is None:
            raise NppesSchemaDriftError(f"NPI is not exactly ten ASCII digits at line {self.line_number}")
        source_row_id = self._value(values, "source_row_id") or f"{self.file_kind}:{self.line_number}"
        assert self.source_row_ids is not None
        if source_row_id in self.source_row_ids:
            raise NppesSchemaDriftError(f"duplicate source_row_id at line {self.line_number}")
        self.source_row_ids.add(source_row_id)
        reference_id = self._value(values, "reference_id")
        deactivation_date = self._value(values, "deactivation_date")
        row_payload = {
            "file_kind": self.file_kind,
            "row_number": self.line_number,
            "source_row_id": source_row_id,
            "npi": npi,
            "reference_id": reference_id,
            "deactivation_date": deactivation_date,
        }
        try:
            encoded = json.dumps(row_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            assert self.row_digest is not None
            self.row_digest.update(encoded + b"\n")
            row = NppesSourceRow(
                file_kind=self.file_kind,
                row_number=self.line_number,
                source_row_id=source_row_id,
                npi=npi,
                row_sha256="sha256:" + sha256(encoded).hexdigest(),
                reference_id=reference_id,
                deactivation_date=deactivation_date,
            )
        except NppesContractError:
            raise
        except (TypeError, UnicodeError, ValueError) as exc:
            raise NppesSchemaDriftError(
                f"source row metadata cannot be fingerprinted at line {self.line_number}"
            ) from exc
        self.row_count += 1
        self.identifier_count += 1
        assert self.identifier_samples is not None
        if len(self.identifier_samples) < MAX_IDENTIFIER_SAMPLES:
            self.identifier_samples.append(npi)
        if self.file_kind == "deactivation":
            self.deactivation_count += 1
            assert self.deactivation_opportunities is not None
            if len(self.deactivation_opportunities) < MAX_DEACTIVATION_SAMPLES:
                if deactivation_date is None:
                    raise NppesSchemaDriftError("deactivation row requires deactivation_date")
                self.deactivation_opportunities.append(
                    NppesDeactivationOpportunity(
                        npi=npi,
                        source_row_id=source_row_id,
                        deactivation_date=deactivation_date,
                        evidence_locator=self.evidence_locator,
                    )
                )
        if self.on_row is not None:
            try:
                self.on_row(row)
            except Exception as exc:  # pragma: no cover - caller-owned sink behavior
                raise NppesProducerError("row sink rejected source identity metadata") from exc

    def _set_header(self, values: list[str]) -> None:
        normalized = tuple(_normalize_header(value) for value in values)
        if any(not value for value in normalized):
            raise NppesSchemaDriftError("source file contains a blank CSV header")
        if len(set(normalized)) != len(normalized):
            raise NppesSchemaDriftError("source file contains duplicate CSV headers")
        if "npi" not in normalized and "npi_number" not in normalized:
            raise NppesSchemaDriftError(f"{self.file_kind} file is missing the NPI column")
        npi_key = "npi" if "npi" in normalized else "npi_number"
        indexes = {"npi": normalized.index(npi_key)}
        for logical, aliases in {
            "source_row_id": ("source_row_id", "source_record_id", "record_id"),
            "reference_id": ("reference_id", "other_name", "endpoint_id", "location_id"),
            "deactivation_date": ("deactivation_date", "deactivation_dt", "deactivated_date"),
        }.items():
            for alias in aliases:
                if alias in normalized:
                    indexes[logical] = normalized.index(alias)
                    break
        if self.file_kind == "deactivation" and "deactivation_date" not in indexes:
            raise NppesSchemaDriftError("deactivation file is missing the deactivation date column")
        self.header = normalized
        self.header_indexes = indexes

    def _value(self, values: list[str], logical: str) -> str | None:
        assert self.header_indexes is not None
        index = self.header_indexes.get(logical)
        if index is None:
            return None
        value = values[index]
        return value if value else None

    def row_digest_value(self) -> str:
        assert self.row_digest is not None
        return "sha256:" + self.row_digest.hexdigest()


def _normalize_header(value: str) -> str:
    return _HEADER.sub("_", value.strip().casefold()).strip("_")


def _timestamp(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise NppesProducerError("recorded_at datetime must include a timezone")
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, str) and value:
        return value
    raise NppesProducerError("recorded_at must be a timezone-aware datetime or ISO timestamp")


def _coerce_catalog(value: NppesCatalog | NppesCatalogEntry | Mapping[str, object]) -> NppesCatalogEntry:
    if isinstance(value, NppesCatalogEntry):
        return value
    if isinstance(value, NppesCatalog):
        return value.source
    if not isinstance(value, Mapping):
        raise NppesProducerError("catalog must be an NppesCatalog, NppesCatalogEntry, or mapping")
    if "sources" in value:
        return NppesCatalog.from_mapping(value).source
    return NppesCatalogEntry(
        source_id=cast(str, value.get("source_id", NPPES_SOURCE_ID)),
        title=cast(str, value.get("title", "")),
        source_url=cast(str, value.get("source_url", "")),
        release_locator=cast(str, value.get("release_locator", "")),
        change_mode=cast(Literal["release_metadata", "content_hash", "last_modified"], value.get("change_mode")),
        rights_status=cast(Literal["approved_public", "pending_review", "blocked"], value.get("rights_status")),
        enabled=cast(bool, value.get("enabled", True)),
        max_bytes=cast(int, value.get("max_bytes", MAX_NPPES_BYTES)),
        max_chunks=cast(int, value.get("max_chunks", MAX_NPPES_CHUNKS)),
        max_seconds=cast(int, value.get("max_seconds", MAX_NPPES_SECONDS)),
        max_chunk_bytes=cast(int, value.get("max_chunk_bytes", MAX_NPPES_CHUNK_BYTES)),
    )


def _coerce_release(value: NppesReleaseDescriptor | Mapping[str, object]) -> NppesReleaseDescriptor:
    if isinstance(value, NppesReleaseDescriptor):
        return value
    if not isinstance(value, Mapping):
        raise NppesProducerError("release must be an NppesReleaseDescriptor or mapping")
    return NppesReleaseDescriptor.from_mapping(value)


def _coerce_previous(value: NppesBaselineReceipt | Mapping[str, object] | None) -> NppesBaselineReceipt | None:
    if value is None:
        return None
    if isinstance(value, NppesBaselineReceipt):
        return value
    if not isinstance(value, Mapping):
        raise NppesProducerError("previous receipt must be an NppesBaselineReceipt or mapping")
    return NppesBaselineReceipt.from_mapping(value)


def _receipt_id(release: NppesReleaseDescriptor, state: str, files: tuple[NppesFileReceipt, ...]) -> str:
    payload = {
        "release_sha256": release.release_sha256,
        "state": state,
        "files": [item.as_dict() for item in files],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return f"receipt:nppes:{sha256(encoded).hexdigest()[:32]}"


def _empty_receipt(
    release: NppesReleaseDescriptor,
    *,
    state: str,
    recorded_at: str,
    failure_code: str | None = None,
) -> NppesBaselineReceipt:
    receipt_state = cast(NppesOutcomeState, state)
    receipt_id = _receipt_id(release, receipt_state, ())
    return NppesBaselineReceipt(
        receipt_id=receipt_id,
        source_id=release.source_id,
        source_url=release.source_url,
        release_id=release.release_id,
        release_label=release.release_label,
        release_sha256=cast(str, release.release_sha256),
        probe_state=release.probe_state,
        state=receipt_state,
        final_url=release.final_url,
        http_status=release.http_status,
        recorded_at=recorded_at,
        evidence_locator=release.evidence_locator,
        files=(),
        failure_code=failure_code,
    )


def _unavailable_receipt(descriptor: NppesFileDescriptor) -> NppesFileReceipt:
    state = cast("NppesFileState", descriptor.availability)
    return NppesFileReceipt(
        file_kind=descriptor.file_kind,
        source_file_name=descriptor.source_file_name,
        source_url=descriptor.source_url,
        evidence_locator=descriptor.evidence_locator,
        state=state,
        content_sha256=None,
        byte_length=0,
        chunk_count=0,
        row_count=0,
        identifier_count=0,
        row_sha256=None,
    )


def _consume_file(
    descriptor: NppesFileDescriptor,
    chunks: FileChunks,
    budget: NppesStreamBudget,
    *,
    on_row: RowSink | None,
    clock: Callable[[], float],
) -> tuple[NppesFileReceipt, tuple[NppesDeactivationOpportunity, ...]]:
    parsed = _ParsedFile(
        file_kind=descriptor.file_kind,
        evidence_locator=descriptor.evidence_locator,
        on_row=on_row,
        decoder=codecs.getincrementaldecoder("utf-8-sig")(),
    )
    started = clock()
    digest = sha256()
    byte_length = 0
    chunk_count = 0
    try:
        for chunk in chunks:
            if not isinstance(chunk, bytes):
                raise NppesSchemaDriftError("source file stream chunks must be bytes")
            if not chunk:
                raise NppesSchemaDriftError("source file stream chunks must not be empty")
            if clock() - started > budget.max_seconds:
                raise NppesStreamInterrupted("source file stream exceeded its time bound")
            if chunk_count >= budget.max_chunks or len(chunk) > budget.max_chunk_bytes:
                raise NppesStreamInterrupted("source file stream exceeded its chunk bound")
            if byte_length + len(chunk) > budget.max_bytes:
                raise NppesStreamInterrupted("source file stream exceeded its byte bound")
            parsed.feed(chunk)
            digest.update(chunk)
            byte_length += len(chunk)
            chunk_count += 1
            if clock() - started > budget.max_seconds:
                raise NppesStreamInterrupted("source file stream exceeded its time bound")
        parsed.finish()
    except NppesStreamInterrupted:
        return (
            NppesFileReceipt(
                file_kind=descriptor.file_kind,
                source_file_name=descriptor.source_file_name,
                source_url=descriptor.source_url,
                evidence_locator=descriptor.evidence_locator,
                state="interrupted",
                content_sha256="sha256:" + digest.hexdigest(),
                byte_length=byte_length,
                chunk_count=chunk_count,
                row_count=parsed.row_count,
                identifier_count=parsed.identifier_count,
                row_sha256=parsed.row_digest_value(),
                identifier_samples=tuple(parsed.identifier_samples or ()),
                deactivation_count=parsed.deactivation_count,
                error_code="stream_interrupted",
            ),
            tuple(parsed.deactivation_opportunities or ()),
        )
    except NppesContractError as exc:
        return (
            NppesFileReceipt(
                file_kind=descriptor.file_kind,
                source_file_name=descriptor.source_file_name,
                source_url=descriptor.source_url,
                evidence_locator=descriptor.evidence_locator,
                state="schema_drift",
                content_sha256="sha256:" + digest.hexdigest(),
                byte_length=byte_length,
                chunk_count=chunk_count,
                row_count=parsed.row_count,
                identifier_count=parsed.identifier_count,
                row_sha256=parsed.row_digest_value(),
                identifier_samples=tuple(parsed.identifier_samples or ()),
                deactivation_count=parsed.deactivation_count,
                error_code=_error_code(exc),
            ),
            tuple(parsed.deactivation_opportunities or ()),
        )
    except Exception:  # pragma: no cover - source iterable failures are environment-specific
        return (
            NppesFileReceipt(
                file_kind=descriptor.file_kind,
                source_file_name=descriptor.source_file_name,
                source_url=descriptor.source_url,
                evidence_locator=descriptor.evidence_locator,
                state="schema_drift",
                content_sha256="sha256:" + digest.hexdigest(),
                byte_length=byte_length,
                chunk_count=chunk_count,
                row_count=parsed.row_count,
                identifier_count=parsed.identifier_count,
                row_sha256=parsed.row_digest_value(),
                identifier_samples=tuple(parsed.identifier_samples or ()),
                deactivation_count=parsed.deactivation_count,
                error_code="stream_error",
            ),
            tuple(parsed.deactivation_opportunities or ()),
        )

    content_sha256 = "sha256:" + digest.hexdigest()
    if descriptor.expected_sha256 is not None and descriptor.expected_sha256 != content_sha256:
        return (
            NppesFileReceipt(
                file_kind=descriptor.file_kind,
                source_file_name=descriptor.source_file_name,
                source_url=descriptor.source_url,
                evidence_locator=descriptor.evidence_locator,
                state="schema_drift",
                content_sha256=content_sha256,
                byte_length=byte_length,
                chunk_count=chunk_count,
                row_count=parsed.row_count,
                identifier_count=parsed.identifier_count,
                row_sha256=parsed.row_digest_value(),
                identifier_samples=tuple(parsed.identifier_samples or ()),
                deactivation_count=parsed.deactivation_count,
                error_code="content_hash_mismatch",
            ),
            tuple(parsed.deactivation_opportunities or ()),
        )
    return (
        NppesFileReceipt(
            file_kind=descriptor.file_kind,
            source_file_name=descriptor.source_file_name,
            source_url=descriptor.source_url,
            evidence_locator=descriptor.evidence_locator,
            state="accepted",
            content_sha256=content_sha256,
            byte_length=byte_length,
            chunk_count=chunk_count,
            row_count=parsed.row_count,
            identifier_count=parsed.identifier_count,
            row_sha256=parsed.row_digest_value(),
            identifier_samples=tuple(parsed.identifier_samples or ()),
            deactivation_count=parsed.deactivation_count,
        ),
        tuple(parsed.deactivation_opportunities or ()),
    )


def _error_code(exc: BaseException) -> str:
    message = str(exc).casefold()
    if "npi" in message:
        return "invalid_npi"
    if "header" in message or "column" in message:
        return "schema_drift"
    if "deactivation" in message:
        return "deactivation_schema_drift"
    if "source row" in message or "row" in message:
        return "row_schema_drift"
    return "schema_drift"


def _budget(catalog: NppesCatalogEntry) -> NppesStreamBudget:
    return NppesStreamBudget(
        max_bytes=catalog.max_bytes,
        max_chunks=catalog.max_chunks,
        max_seconds=catalog.max_seconds,
        max_chunk_bytes=catalog.max_chunk_bytes,
    )


class NppesBaselineProducer:
    """Produce a source-scoped NPPES baseline receipt from bounded streams."""

    def __init__(self, catalog: NppesCatalog | NppesCatalogEntry | Mapping[str, object]) -> None:
        self.catalog = _coerce_catalog(catalog)
        if not self.catalog.enabled or self.catalog.rights_status != "approved_public":
            raise NppesProducerError("NPPES catalog registration is not enabled with approved public rights")

    def produce(
        self,
        release: NppesReleaseDescriptor | Mapping[str, object],
        files: Mapping[NppesFileKind, FileChunks] | None = None,
        *,
        previous: NppesBaselineReceipt | Mapping[str, object] | None = None,
        recorded_at: datetime | str | None = None,
        row_sink: RowSink | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> NppesBaselineReceipt:
        """Consume one release without granting projection or identity authority."""

        descriptor = _coerce_release(release)
        if descriptor.source_id != self.catalog.source_id:
            raise NppesProducerError("release source_id does not match catalog source_id")
        if not self.catalog.source_url.startswith("https://"):
            raise NppesProducerError("catalog source URL is not HTTPS")
        prior = _coerce_previous(previous)
        timestamp = _timestamp(recorded_at)
        if descriptor.probe_state == "failed_probe":
            return _empty_receipt(descriptor, state="failed_probe", recorded_at=timestamp, failure_code="failed_probe")
        if descriptor.probe_state == "not_modified":
            return _empty_receipt(descriptor, state="no_op", recorded_at=timestamp)
        if files is None:
            raise NppesProducerError("changed NPPES release requires file streams")
        unknown = set(files) - set(_FILE_KINDS)
        if unknown:
            raise NppesProducerError(f"unknown NPPES file stream kinds: {sorted(unknown)}")

        descriptors = {item.file_kind: item for item in descriptor.files}
        prior_same_release = prior is not None and prior.release_sha256 == descriptor.release_sha256
        sinks_disabled = prior_same_release
        receipts: list[NppesFileReceipt] = []
        opportunities: list[NppesDeactivationOpportunity] = []
        first_failure: str | None = None
        budget = _budget(self.catalog)
        for kind in _FILE_KINDS:
            file_descriptor = descriptors.get(kind)
            stream = files.get(kind)
            if file_descriptor is None:
                if kind == "provider":
                    first_failure = first_failure or "provider_file_missing_from_release"
                    receipts.append(
                        NppesFileReceipt(
                            file_kind="provider",
                            source_file_name="missing.csv",
                            source_url=self.catalog.source_url,
                            evidence_locator=self.catalog.source_url,
                            state="schema_drift",
                            content_sha256=None,
                            byte_length=0,
                            chunk_count=0,
                            row_count=0,
                            identifier_count=0,
                            row_sha256=None,
                            error_code="provider_file_missing_from_release",
                        )
                    )
                continue
            if file_descriptor.availability != "present":
                if stream is not None:
                    raise NppesProducerError(f"{kind} file is unavailable but a stream was supplied")
                receipts.append(_unavailable_receipt(file_descriptor))
                continue
            if stream is None:
                first_failure = first_failure or f"{kind}_stream_missing"
                receipts.append(
                    NppesFileReceipt(
                        file_kind=kind,
                        source_file_name=file_descriptor.source_file_name,
                        source_url=file_descriptor.source_url,
                        evidence_locator=file_descriptor.evidence_locator,
                        state="not_consumed",
                        content_sha256=None,
                        byte_length=0,
                        chunk_count=0,
                        row_count=0,
                        identifier_count=0,
                        row_sha256=None,
                        error_code=f"{kind}_stream_missing",
                    )
                )
                continue
            receipt, file_opportunities = _consume_file(
                file_descriptor,
                stream,
                budget,
                on_row=None if sinks_disabled else row_sink,
                clock=clock,
            )
            receipts.append(receipt)
            if receipt.state == "accepted":
                opportunities.extend(file_opportunities)
            if receipt.state != "accepted":
                first_failure = first_failure or receipt.error_code or "file_rejected"

        file_receipts = tuple(receipts)
        if first_failure is not None:
            outcome = (
                "blocked"
                if any(item.state in {"interrupted", "not_consumed"} for item in file_receipts)
                else "schema_drift"
            )
            return self._receipt(
                descriptor,
                outcome=outcome,
                recorded_at=timestamp,
                files=file_receipts,
                opportunities=tuple(opportunities),
                failure_code=first_failure,
            )

        if prior_same_release and prior is not None:
            if not _same_file_identity(prior.files, file_receipts):
                raise NppesReplayConflictError("replayed NPPES release has different file identity")
            return self._receipt(
                descriptor,
                outcome="replayed",
                recorded_at=timestamp,
                files=file_receipts,
                opportunities=tuple(opportunities),
            )
        return self._receipt(
            descriptor,
            outcome="changed",
            recorded_at=timestamp,
            files=file_receipts,
            opportunities=tuple(opportunities),
        )

    def _receipt(
        self,
        release: NppesReleaseDescriptor,
        *,
        outcome: str,
        recorded_at: str,
        files: tuple[NppesFileReceipt, ...],
        opportunities: tuple[NppesDeactivationOpportunity, ...],
        failure_code: str | None = None,
    ) -> NppesBaselineReceipt:
        receipt_id = _receipt_id(release, outcome, files)
        deactivation_count = sum(item.deactivation_count for item in files)
        return NppesBaselineReceipt(
            receipt_id=receipt_id,
            source_id=release.source_id,
            source_url=release.source_url,
            release_id=release.release_id,
            release_label=release.release_label,
            release_sha256=cast(str, release.release_sha256),
            probe_state=release.probe_state,
            state=cast(NppesOutcomeState, outcome),
            final_url=release.final_url,
            http_status=release.http_status,
            recorded_at=recorded_at,
            evidence_locator=release.evidence_locator,
            files=files,
            deactivation_opportunities=opportunities[:MAX_DEACTIVATION_SAMPLES],
            deactivation_count=deactivation_count,
            failure_code=failure_code,
        )


def _same_file_identity(previous: tuple[NppesFileReceipt, ...], current: tuple[NppesFileReceipt, ...]) -> bool:
    previous_by_kind = {item.file_kind: item for item in previous}
    current_by_kind = {item.file_kind: item for item in current}
    if previous_by_kind.keys() != current_by_kind.keys():
        return False
    for kind, old in previous_by_kind.items():
        new = current_by_kind[kind]
        if (
            old.state != new.state
            or old.content_sha256 != new.content_sha256
            or old.byte_length != new.byte_length
            or old.row_count != new.row_count
            or old.row_sha256 != new.row_sha256
        ):
            return False
    return True


def produce_nppes_baseline(
    catalog: NppesCatalog | NppesCatalogEntry | Mapping[str, object],
    release: NppesReleaseDescriptor | Mapping[str, object],
    files: Mapping[NppesFileKind, FileChunks] | None = None,
    *,
    previous: NppesBaselineReceipt | Mapping[str, object] | None = None,
    recorded_at: datetime | str | None = None,
    row_sink: RowSink | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> NppesBaselineReceipt:
    """Convenience wrapper for :class:`NppesBaselineProducer`."""

    return NppesBaselineProducer(catalog).produce(
        release,
        files,
        previous=previous,
        recorded_at=recorded_at,
        row_sink=row_sink,
        clock=clock,
    )


__all__ = [
    "FileChunks",
    "NppesBaselineProducer",
    "NppesProducerError",
    "NppesRowSink",
    "NppesStreamBudget",
    "NppesStreamInterrupted",
    "RowSink",
    "produce_nppes_baseline",
]
