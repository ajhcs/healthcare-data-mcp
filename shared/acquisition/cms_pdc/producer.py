"""Bounded, catalog-aware CMS Provider Data Catalog producer.

The producer accepts caller-owned release metadata and byte chunks.  It uses
the source-neutral adapter SDK for streaming and returns a deterministic,
payload-free receipt.  Network acquisition, persistence, publication, and
projection mutation remain outside this module.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import sha256
import json
from typing import TypeAlias, cast

from shared.adapters import AdapterCatalog, BoundsError, StreamBudget, StreamReceipt, stream_bounded

from shared.acquisition.cms_pdc.contract import (
    CMS_PDC_SOURCE_ID,
    CmsPdcCatalogEntry,
    CmsPdcChangeKind,
    CmsPdcError,
    CmsPdcReceipt,
    CmsPdcRelease,
    CmsPdcSchemaState,
    CmsPdcState,
    CmsPdcStreamState,
    DistributionFormat,
    canonical_release_fingerprint,
)


JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


def _canonical(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CmsPdcError("CMS PDC identity material is not canonical JSON") from exc


def _digest(value: Mapping[str, object]) -> str:
    return sha256(_canonical(value)).hexdigest()


def _idempotency_key(release: CmsPdcRelease) -> str:
    material = {
        "source_id": release.source_id,
        "dataset_id": release.dataset_id,
        "distribution_id": release.distribution_id,
        "release_fingerprint": canonical_release_fingerprint(release),
    }
    return "replay:cms:pdc:" + _digest(material)


def _receipt_id(material: Mapping[str, object]) -> str:
    return "receipt:cms:pdc:" + _digest(material)


def _parse_release(value: CmsPdcRelease | Mapping[str, object]) -> CmsPdcRelease:
    if isinstance(value, CmsPdcRelease):
        return value
    if not isinstance(value, Mapping):
        raise CmsPdcError("release must be a CmsPdcRelease or mapping")
    return CmsPdcRelease.from_mapping(value)


def _parse_prior(value: CmsPdcReceipt | Mapping[str, object] | None) -> CmsPdcReceipt | None:
    if value is None or isinstance(value, CmsPdcReceipt):
        return value
    if not isinstance(value, Mapping):
        raise CmsPdcError("prior receipt must be a CmsPdcReceipt or mapping")
    return CmsPdcReceipt.from_mapping(value)


def _same_catalog(release: CmsPdcRelease, catalog: CmsPdcCatalogEntry) -> None:
    for label, actual, expected in (
        ("source_id", release.source_id, catalog.source_id),
        ("dataset_id", release.dataset_id, catalog.dataset_id),
        ("distribution_id", release.distribution_id, catalog.distribution_id),
        ("source_url", release.source_url, catalog.source_url),
        ("distribution_url", release.distribution_url, catalog.distribution_url),
    ):
        if actual != expected:
            raise CmsPdcError(f"release {label} does not match the catalog registration")


def _same_prior_catalog(prior: CmsPdcReceipt, catalog: CmsPdcCatalogEntry) -> None:
    for label, actual, expected in (
        ("source_id", prior.source_id, catalog.source_id),
        ("dataset_id", prior.dataset_id, catalog.dataset_id),
        ("distribution_id", prior.distribution_id, catalog.distribution_id),
        ("source_url", prior.source_url, catalog.source_url),
        ("distribution_url", prior.distribution_url, catalog.distribution_url),
    ):
        if actual != expected:
            raise CmsPdcError(f"prior receipt {label} does not match the catalog registration")


@dataclass(frozen=True, slots=True)
class CmsPdcProducer:
    """Produce bounded CMS PDC receipts from a validated catalog binding."""

    catalog: CmsPdcCatalogEntry
    budget: StreamBudget

    def __init__(self, catalog: CmsPdcCatalogEntry, *, budget: StreamBudget | None = None) -> None:
        if not isinstance(catalog, CmsPdcCatalogEntry):
            raise CmsPdcError("catalog must be a CmsPdcCatalogEntry")
        if catalog.rights_status != "approved_public":
            raise CmsPdcError("CMS PDC catalog entry lacks approved public rights")
        if not catalog.enabled:
            raise CmsPdcError("CMS PDC catalog entry is disabled")
        try:
            selected = budget or StreamBudget(
                max_bytes=catalog.max_bytes,
                max_chunks=catalog.max_chunks,
                max_seconds=catalog.max_seconds,
            )
        except BoundsError as exc:
            raise CmsPdcError(f"catalog stream bounds exceed the adapter SDK ceilings: {exc}") from exc
        if not isinstance(selected, StreamBudget):
            raise CmsPdcError("budget must be a StreamBudget")
        if selected.max_bytes > catalog.max_bytes:
            raise CmsPdcError("stream budget exceeds the catalog byte bound")
        if selected.max_chunks > catalog.max_chunks:
            raise CmsPdcError("stream budget exceeds the catalog chunk bound")
        if selected.max_seconds > catalog.max_seconds:
            raise CmsPdcError("stream budget exceeds the catalog time bound")
        object.__setattr__(self, "catalog", catalog)
        object.__setattr__(self, "budget", selected)

    @classmethod
    def from_adapter_catalog(
        cls,
        catalog: AdapterCatalog,
        *,
        dataset_id: str,
        distribution_id: str,
        dataset_title: str,
        distribution_title: str,
        distribution_format: str,
        distribution_url: str,
        schema_fingerprint: str,
        budget: StreamBudget | None = None,
    ) -> "CmsPdcProducer":
        """Bind stable CMS IDs to the source-neutral catalog registration."""

        if not isinstance(catalog, AdapterCatalog):
            raise CmsPdcError("catalog must implement AdapterCatalog")
        registration = catalog.registration(CMS_PDC_SOURCE_ID)
        if registration is None:
            raise CmsPdcError("catalog has no source:cms:pdc registration")
        try:
            entry = CmsPdcCatalogEntry(
                dataset_id=dataset_id,
                distribution_id=distribution_id,
                dataset_title=dataset_title,
                distribution_title=distribution_title,
                distribution_format=cast(DistributionFormat, distribution_format),
                source_url=registration.source_url,
                distribution_url=distribution_url,
                release_locator=registration.release_locator,
                change_mode=registration.change_mode,
                rights_status=registration.rights_status,
                enabled=registration.enabled,
                schema_fingerprint=schema_fingerprint,
                max_bytes=registration.max_bytes,
                max_chunks=registration.max_chunks,
                max_seconds=registration.max_seconds,
            )
        except ValueError as exc:
            raise CmsPdcError(str(exc)) from exc
        return cls(entry, budget=budget)

    def produce(
        self,
        release: CmsPdcRelease | Mapping[str, object],
        chunks: Iterable[bytes],
        *,
        prior: CmsPdcReceipt | Mapping[str, object] | None = None,
    ) -> CmsPdcReceipt:
        """Classify one caller-owned CMS PDC stream without mutating state."""

        release_value = _parse_release(release)
        prior_value = _parse_prior(prior)
        _same_catalog(release_value, self.catalog)
        if prior_value is not None:
            _same_prior_catalog(prior_value, self.catalog)
        release_fingerprint = canonical_release_fingerprint(release_value)
        idempotency_key = _idempotency_key(release_value)

        if release_value.schema_fingerprint != self.catalog.schema_fingerprint:
            return self._receipt(
                release=release_value,
                release_fingerprint=release_fingerprint,
                idempotency_key=idempotency_key,
                prior=prior_value,
                state="schema_drift",
                change_kind="schema_drift",
                content_sha256=None,
                stream_state="not_started",
                acknowledged=False,
                received_bytes=0,
                chunk_count=0,
                current_projection_preserved=True,
                schema_state="drift",
                error="schema_drift",
            )

        if release_value.probe_state == "failed_probe":
            return self._receipt(
                release=release_value,
                release_fingerprint=release_fingerprint,
                idempotency_key=idempotency_key,
                prior=prior_value,
                state="failed_probe",
                change_kind="failed",
                content_sha256=None,
                stream_state="not_started",
                acknowledged=False,
                received_bytes=0,
                chunk_count=0,
                current_projection_preserved=True,
                schema_state="not_checked",
                error="probe_failed",
            )

        if release_value.probe_state == "not_modified":
            if (
                prior_value is None
                or prior_value.content_sha256 is None
                or not prior_value.acknowledged
                or prior_value.stream_state != "completed"
            ):
                raise CmsPdcError("not_modified probe requires an acknowledged completed prior receipt")
            if (
                release_value.declared_content_sha256 is not None
                and release_value.declared_content_sha256 != prior_value.content_sha256
            ):
                raise CmsPdcError("declared content fingerprint conflicts with the not_modified receipt")
            return self._receipt(
                release=release_value,
                release_fingerprint=release_fingerprint,
                idempotency_key=idempotency_key,
                prior=prior_value,
                state="no_op",
                change_kind="none",
                content_sha256=prior_value.content_sha256,
                stream_state="not_started",
                acknowledged=True,
                received_bytes=0,
                chunk_count=0,
                current_projection_preserved=True,
                schema_state="valid",
                error=None,
            )

        try:
            stream = stream_bounded(chunks, self.budget)
        except BoundsError as exc:
            raise CmsPdcError(str(exc)) from exc
        if stream.state == "interrupted":
            return self._receipt(
                release=release_value,
                release_fingerprint=release_fingerprint,
                idempotency_key=idempotency_key,
                prior=prior_value,
                state="interrupted",
                change_kind="interrupted",
                content_sha256=stream.content_sha256,
                stream_state="interrupted",
                acknowledged=False,
                received_bytes=stream.received_bytes,
                chunk_count=stream.chunk_count,
                current_projection_preserved=True,
                schema_state="valid",
                error="stream_interrupted",
            )
        if release_value.declared_content_sha256 is not None and (
            release_value.declared_content_sha256 != stream.content_sha256
        ):
            raise CmsPdcError("declared content fingerprint does not match the bounded stream")
        state, change_kind, preserved = self._classify_complete(release_fingerprint, stream, prior_value)
        if state in {"no_op", "replayed"}:
            stream_state: CmsPdcStreamState = "not_started"
            received_bytes = 0
            chunk_count = 0
            preserved = True
        else:
            stream_state = "completed"
            received_bytes = stream.received_bytes
            chunk_count = stream.chunk_count
        return self._receipt(
            release=release_value,
            release_fingerprint=release_fingerprint,
            idempotency_key=idempotency_key,
            prior=prior_value,
            state=state,
            change_kind=change_kind,
            content_sha256=stream.content_sha256,
            stream_state=stream_state,
            acknowledged=True,
            received_bytes=received_bytes,
            chunk_count=chunk_count,
            current_projection_preserved=preserved,
            schema_state="valid",
            error=None,
        )

    @staticmethod
    def _classify_complete(
        release_fingerprint: str,
        stream: StreamReceipt,
        prior: CmsPdcReceipt | None,
    ) -> tuple[CmsPdcState, CmsPdcChangeKind, bool]:
        if prior is None:
            return "changed", "release", False
        prior_accepted = prior.acknowledged and prior.stream_state == "completed"
        same_release = prior.release_fingerprint == release_fingerprint
        same_content = prior.content_sha256 == stream.content_sha256
        if prior_accepted and same_content and prior.content_sha256 is not None:
            if same_release and prior.state in {"changed", "replayed"}:
                return "replayed", "replay", True
            if same_release and prior.state == "no_op":
                return "no_op", "none", True
            if not same_release:
                return "no_op", "none", True
        if same_release:
            return "changed", "content", False
        return "changed", "release", False

    def _receipt(
        self,
        *,
        release: CmsPdcRelease,
        release_fingerprint: str,
        idempotency_key: str,
        prior: CmsPdcReceipt | None,
        state: CmsPdcState,
        change_kind: CmsPdcChangeKind,
        content_sha256: str | None,
        stream_state: CmsPdcStreamState,
        acknowledged: bool,
        received_bytes: int,
        chunk_count: int,
        current_projection_preserved: bool,
        schema_state: CmsPdcSchemaState,
        error: str | None,
    ) -> CmsPdcReceipt:
        material = {
            "idempotency_key": idempotency_key,
            "state": state,
            "change_kind": change_kind,
            "release_fingerprint": release_fingerprint,
            "content_sha256": content_sha256,
            "stream_state": stream_state,
            "schema_state": schema_state,
            "received_bytes": received_bytes,
            "chunk_count": chunk_count,
            "error": error,
        }
        return CmsPdcReceipt(
            receipt_id=_receipt_id(material),
            idempotency_key=idempotency_key,
            state=state,
            change_kind=change_kind,
            source_id=release.source_id,
            dataset_id=release.dataset_id,
            distribution_id=release.distribution_id,
            release_id=release.release_id,
            release_fingerprint=release_fingerprint,
            content_sha256=content_sha256,
            prior_release_fingerprint=prior.release_fingerprint if prior is not None else None,
            prior_content_sha256=prior.content_sha256 if prior is not None else None,
            prior_receipt_id=prior.receipt_id if prior is not None else None,
            probe_state=release.probe_state,
            schema_state=schema_state,
            stream_state=stream_state,
            acknowledged=acknowledged,
            received_bytes=received_bytes,
            chunk_count=chunk_count,
            current_projection_preserved=current_projection_preserved,
            observed_at=release.modified_at,
            source_url=release.source_url,
            distribution_url=release.distribution_url,
            error=error,
        )


__all__ = ["CmsPdcProducer"]
