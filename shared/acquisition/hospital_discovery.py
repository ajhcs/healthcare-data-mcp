"""Sealed hospital-discovery v3 manifest and evidence registration contract.

This lane records discovery receipts only.  A URL or candidate is never treated
as hospital ownership authority: promotion is an explicit, outstanding review
step owned by the hospital-data owner.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import ipaddress
from pathlib import Path
import re
from typing import Literal, Mapping, cast
from urllib.parse import urlparse

from shared.utils.cache import write_atomic_json

SchemaVersion = Literal["hospital-discovery.manifest.v3"]
UrlState = Literal["unverified", "verified", "redirected", "invalid"]
ProbeState = Literal["pending", "succeeded", "failed"]
CandidateState = Literal["not_evaluated", "candidate", "rejected"]

_URL_STATES = {"unverified", "verified", "redirected", "invalid"}
_PROBE_STATES = {"pending", "succeeded", "failed"}
_CANDIDATE_STATES = {"not_evaluated", "candidate", "rejected"}
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_SOURCE_ID = re.compile(r"^source:[a-z0-9][a-z0-9-]*:[a-z0-9][a-z0-9-]*$")


class HospitalDiscoveryError(ValueError):
    """Raised when a hospital discovery manifest violates its contract."""


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HospitalDiscoveryError(f"{name} must be a non-empty string")
    return value.strip()


def _url(value: object, name: str) -> str:
    result = _text(value, name)
    parsed = urlparse(result)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
        raise HospitalDiscoveryError(f"{name} must be an absolute HTTP(S) URL")
    host = parsed.hostname.casefold().rstrip(".")
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise HospitalDiscoveryError(f"{name} has an unsafe local authority")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address is not None and (address.is_private or address.is_loopback or address.is_link_local or address.is_reserved):
        raise HospitalDiscoveryError(f"{name} has an unsafe private authority")
    return result


def _digest(value: str, name: str) -> str:
    if _SHA256.fullmatch(value) is None:
        raise HospitalDiscoveryError(f"{name} must be lowercase sha256:<64hex>")
    return value


def canonical_digest(value: Mapping[str, object], *, excluding: str | None = None) -> str:
    """Return the canonical lowercase digest for a JSON object."""
    payload = {key: item for key, item in value.items() if key != excluding}
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _state(value: object, name: str, allowed: set[str]) -> str:
    result = _text(value, name)
    if result not in allowed:
        raise HospitalDiscoveryError(f"{name} has unsupported state: {result}")
    return result


@dataclass(frozen=True, slots=True)
class HospitalUrlProbe:
    """Receipt for one source URL and its bounded probe outcome."""

    source_id: str
    url: str
    url_state: UrlState = "unverified"
    probe_state: ProbeState = "pending"
    final_url: str = ""
    http_status: int | None = None
    content_sha256: str = ""
    note: str = ""

    def __post_init__(self) -> None:
        if _SOURCE_ID.fullmatch(self.source_id) is None:
            raise HospitalDiscoveryError("source_id must be source-scoped")
        _url(self.url, "url")
        _state(self.url_state, "url_state", _URL_STATES)
        _state(self.probe_state, "probe_state", _PROBE_STATES)
        if self.final_url:
            _url(self.final_url, "final_url")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise HospitalDiscoveryError("http_status must be between 100 and 599")
        if self.probe_state == "succeeded":
            if self.url_state not in {"verified", "redirected"} or not self.final_url or self.http_status is None:
                raise HospitalDiscoveryError("succeeded probe requires verified URL, final_url, and http_status")
            _digest(self.content_sha256, "content_sha256")
        elif self.probe_state == "pending":
            if self.url_state != "unverified" or self.final_url or self.http_status is not None or self.content_sha256:
                raise HospitalDiscoveryError("pending probe cannot carry result fields")
        elif self.url_state == "invalid" and (self.final_url or self.http_status is not None or self.content_sha256):
            raise HospitalDiscoveryError("invalid URL cannot carry result fields")

    def as_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "url": self.url,
            "url_state": self.url_state,
            "probe_state": self.probe_state,
            "final_url": self.final_url,
            "http_status": self.http_status,
            "content_sha256": self.content_sha256,
            "note": self.note,
        }


@dataclass(frozen=True, slots=True)
class HospitalEvidenceRegistration:
    """A candidate evidence row with deliberately non-authoritative ownership."""

    evidence_id: str
    source_id: str
    receipt_id: str
    receipt_sha256: str
    entity_ref: str
    field: str
    observed_value: str
    candidate_state: CandidateState = "not_evaluated"
    authority_state: Literal["non_authoritative"] = "non_authoritative"
    owner_promotion_state: Literal["outstanding"] = "outstanding"
    caveat: str = ""

    def __post_init__(self) -> None:
        for name in ("evidence_id", "source_id", "receipt_id", "entity_ref", "field", "observed_value"):
            _text(getattr(self, name), name)
        if _SOURCE_ID.fullmatch(self.source_id) is None:
            raise HospitalDiscoveryError("source_id must be source-scoped")
        _digest(self.receipt_sha256, "receipt_sha256")
        receipt_payload = {
            "evidence_id": self.evidence_id, "source_id": self.source_id, "receipt_id": self.receipt_id,
            "entity_ref": self.entity_ref, "field": self.field, "observed_value": self.observed_value,
            "candidate_state": self.candidate_state, "authority_state": self.authority_state,
            "owner_promotion_state": self.owner_promotion_state, "caveat": self.caveat,
        }
        if self.receipt_sha256 != canonical_digest(receipt_payload):
            raise HospitalDiscoveryError("receipt_sha256 does not match canonical evidence receipt")
        _state(self.candidate_state, "candidate_state", _CANDIDATE_STATES)
        if self.authority_state != "non_authoritative":
            raise HospitalDiscoveryError("hospital discovery evidence is always non_authoritative")
        if self.owner_promotion_state != "outstanding":
            raise HospitalDiscoveryError("owner promotion must remain outstanding in this lane")

    def as_dict(self) -> dict[str, object]:
        return {
            "evidence_id": self.evidence_id,
            "source_id": self.source_id,
            "receipt_id": self.receipt_id,
            "receipt_sha256": self.receipt_sha256,
            "entity_ref": self.entity_ref,
            "field": self.field,
            "observed_value": self.observed_value,
            "candidate_state": self.candidate_state,
            "authority_state": self.authority_state,
            "owner_promotion_state": self.owner_promotion_state,
            "caveat": self.caveat,
        }


@dataclass(frozen=True, slots=True)
class HospitalDiscoveryManifest:
    """Immutable, sealed registration of hospital discovery inputs."""

    manifest_id: str
    generated_at: str
    sources: tuple[HospitalUrlProbe, ...] = ()
    evidence: tuple[HospitalEvidenceRegistration, ...] = ()
    seal: Literal["sealed"] = "sealed"
    lane: Literal["hospital"] = "hospital"
    schema_version: SchemaVersion = "hospital-discovery.manifest.v3"
    owner_promotion_state: Literal["outstanding"] = "outstanding"
    manifest_sha256: str = ""

    def __post_init__(self) -> None:
        _text(self.manifest_id, "manifest_id")
        _text(self.generated_at, "generated_at")
        if self.seal != "sealed" or self.lane != "hospital":
            raise HospitalDiscoveryError("manifest must be sealed and scoped to hospital")
        if self.schema_version != "hospital-discovery.manifest.v3":
            raise HospitalDiscoveryError("unsupported hospital discovery schema version")
        source_ids = {item.source_id for item in self.sources}
        if len(source_ids) != len(self.sources):
            raise HospitalDiscoveryError("source_id values must be unique")
        evidence_ids = {item.evidence_id for item in self.evidence}
        if len(evidence_ids) != len(self.evidence):
            raise HospitalDiscoveryError("evidence_id values must be unique")
        if any(item.source_id not in source_ids for item in self.evidence):
            raise HospitalDiscoveryError("evidence references an unregistered source")
        if self.manifest_sha256:
            _digest(self.manifest_sha256, "manifest_sha256")

    def as_dict(self) -> dict[str, object]:
        payload = {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "lane": self.lane,
            "seal": self.seal,
            "generated_at": self.generated_at,
            "owner_promotion_state": self.owner_promotion_state,
            "sources": [item.as_dict() for item in self.sources],
            "evidence": [item.as_dict() for item in self.evidence],
        }
        payload["manifest_sha256"] = self.manifest_sha256 or canonical_digest(payload)
        return payload


def register_evidence(
    manifest: HospitalDiscoveryManifest, evidence: HospitalEvidenceRegistration
) -> HospitalDiscoveryManifest:
    """Return a new sealed manifest with one non-authoritative evidence row."""
    return HospitalDiscoveryManifest(
        manifest_id=manifest.manifest_id,
        generated_at=manifest.generated_at,
        sources=manifest.sources,
        evidence=(*manifest.evidence, evidence),
    )


def validate_manifest(value: Mapping[str, object]) -> HospitalDiscoveryManifest:
    """Parse and validate a strict v3 manifest mapping."""
    allowed = {"schema_version", "manifest_id", "lane", "seal", "generated_at", "owner_promotion_state", "sources", "evidence", "manifest_sha256"}
    unknown = set(value) - allowed
    if unknown:
        raise HospitalDiscoveryError(f"unknown manifest fields: {sorted(unknown)}")
    raw_sources = value.get("sources", ())
    raw_evidence = value.get("evidence", ())
    if not isinstance(raw_sources, list) or not isinstance(raw_evidence, list):
        raise HospitalDiscoveryError("sources and evidence must be arrays")
    source_allowed = {"source_id", "url", "url_state", "probe_state", "final_url", "http_status", "content_sha256", "note"}
    evidence_allowed = {"evidence_id", "source_id", "receipt_id", "receipt_sha256", "entity_ref", "field", "observed_value", "candidate_state", "authority_state", "owner_promotion_state", "caveat"}
    sources = []
    for item in raw_sources:
        if not isinstance(item, Mapping) or set(item) - source_allowed:
            raise HospitalDiscoveryError("source entry has unknown fields")
        sources.append(HospitalUrlProbe(
            source_id=_text(item.get("source_id"), "source_id"), url=_text(item.get("url"), "url"),
            url_state=cast(UrlState, item.get("url_state", "unverified")),
            probe_state=cast(ProbeState, item.get("probe_state", "pending")),
            final_url=cast(str, item.get("final_url", "")), http_status=cast(int | None, item.get("http_status")),
            content_sha256=cast(str, item.get("content_sha256", "")), note=cast(str, item.get("note", "")),
        ))
    evidence = []
    for item in raw_evidence:
        if not isinstance(item, Mapping) or set(item) - evidence_allowed:
            raise HospitalDiscoveryError("evidence entry has unknown fields")
        evidence.append(HospitalEvidenceRegistration(
            evidence_id=_text(item.get("evidence_id"), "evidence_id"), source_id=_text(item.get("source_id"), "source_id"),
            receipt_id=_text(item.get("receipt_id"), "receipt_id"), receipt_sha256=_text(item.get("receipt_sha256"), "receipt_sha256"),
            entity_ref=_text(item.get("entity_ref"), "entity_ref"), field=_text(item.get("field"), "field"),
            observed_value=_text(item.get("observed_value"), "observed_value"),
            candidate_state=cast(CandidateState, item.get("candidate_state", "not_evaluated")),
            authority_state=cast(Literal["non_authoritative"], item.get("authority_state", "non_authoritative")),
            owner_promotion_state=cast(Literal["outstanding"], item.get("owner_promotion_state", "outstanding")),
            caveat=cast(str, item.get("caveat", "")),
        ))
    supplied_digest = value.get("manifest_sha256")
    if not isinstance(supplied_digest, str):
        raise HospitalDiscoveryError("manifest_sha256 is required")
    expected_payload = {key: item for key, item in value.items() if key != "manifest_sha256"}
    if supplied_digest != canonical_digest(expected_payload):
        raise HospitalDiscoveryError("manifest_sha256 does not match canonical manifest")
    return HospitalDiscoveryManifest(
        manifest_id=_text(value.get("manifest_id"), "manifest_id"),
        generated_at=_text(value.get("generated_at"), "generated_at"),
        sources=tuple(sources), evidence=tuple(evidence),
        schema_version=cast(SchemaVersion, value.get("schema_version")),
        lane=cast(Literal["hospital"], value.get("lane")),
        seal=cast(Literal["sealed"], value.get("seal")),
        owner_promotion_state=cast(Literal["outstanding"], value.get("owner_promotion_state")),
        manifest_sha256=supplied_digest,
    )


def load_manifest(path: str | Path) -> HospitalDiscoveryManifest:
    with Path(path).open(encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, Mapping):
        raise HospitalDiscoveryError("manifest must be a JSON object")
    return validate_manifest(value)


def write_manifest(manifest: HospitalDiscoveryManifest, path: str | Path) -> Path:
    destination = Path(path)
    write_atomic_json(destination, manifest.as_dict())
    return destination
