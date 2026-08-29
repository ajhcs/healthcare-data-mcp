"""Sealed hospital-discovery v3 manifest and evidence registration contract.

This lane records discovery receipts only.  A URL or candidate is never treated
as hospital ownership authority: promotion is an explicit, outstanding review
step owned by the hospital-data owner.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
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


class HospitalDiscoveryError(ValueError):
    """Raised when a hospital discovery manifest violates its contract."""


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HospitalDiscoveryError(f"{name} must be a non-empty string")
    return value.strip()


def _url(value: object, name: str) -> str:
    result = _text(value, name)
    parsed = urlparse(result)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HospitalDiscoveryError(f"{name} must be an absolute HTTP(S) URL")
    return result


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
        _text(self.source_id, "source_id")
        _url(self.url, "url")
        _state(self.url_state, "url_state", _URL_STATES)
        _state(self.probe_state, "probe_state", _PROBE_STATES)
        if self.final_url:
            _url(self.final_url, "final_url")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise HospitalDiscoveryError("http_status must be between 100 and 599")
        if self.probe_state == "succeeded" and self.url_state == "invalid":
            raise HospitalDiscoveryError("an invalid URL cannot have a successful probe")

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
    entity_ref: str
    field: str
    observed_value: str
    candidate_state: CandidateState = "not_evaluated"
    authority_state: Literal["non_authoritative"] = "non_authoritative"
    owner_promotion_state: Literal["outstanding"] = "outstanding"
    caveat: str = ""

    def __post_init__(self) -> None:
        for name in ("evidence_id", "source_id", "entity_ref", "field", "observed_value"):
            _text(getattr(self, name), name)
        _state(self.candidate_state, "candidate_state", _CANDIDATE_STATES)
        if self.authority_state != "non_authoritative":
            raise HospitalDiscoveryError("hospital discovery evidence is always non_authoritative")
        if self.owner_promotion_state != "outstanding":
            raise HospitalDiscoveryError("owner promotion must remain outstanding in this lane")

    def as_dict(self) -> dict[str, object]:
        return {
            "evidence_id": self.evidence_id,
            "source_id": self.source_id,
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

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "lane": self.lane,
            "seal": self.seal,
            "generated_at": self.generated_at,
            "owner_promotion_state": self.owner_promotion_state,
            "sources": [item.as_dict() for item in self.sources],
            "evidence": [item.as_dict() for item in self.evidence],
        }


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
    allowed = {"schema_version", "manifest_id", "lane", "seal", "generated_at", "owner_promotion_state", "sources", "evidence"}
    unknown = set(value) - allowed
    if unknown:
        raise HospitalDiscoveryError(f"unknown manifest fields: {sorted(unknown)}")
    raw_sources = value.get("sources", ())
    raw_evidence = value.get("evidence", ())
    if not isinstance(raw_sources, list) or not isinstance(raw_evidence, list):
        raise HospitalDiscoveryError("sources and evidence must be arrays")
    source_allowed = {"source_id", "url", "url_state", "probe_state", "final_url", "http_status", "content_sha256", "note"}
    evidence_allowed = {"evidence_id", "source_id", "entity_ref", "field", "observed_value", "candidate_state", "authority_state", "owner_promotion_state", "caveat"}
    sources = []
    for item in raw_sources:
        if not isinstance(item, Mapping) or set(item) - source_allowed:
            raise HospitalDiscoveryError("source entry has unknown fields")
        sources.append(HospitalUrlProbe(**cast(dict[str, object], item)))
    evidence = []
    for item in raw_evidence:
        if not isinstance(item, Mapping) or set(item) - evidence_allowed:
            raise HospitalDiscoveryError("evidence entry has unknown fields")
        evidence.append(HospitalEvidenceRegistration(**cast(dict[str, object], item)))
    return HospitalDiscoveryManifest(
        manifest_id=_text(value.get("manifest_id"), "manifest_id"),
        generated_at=_text(value.get("generated_at"), "generated_at"),
        sources=tuple(sources), evidence=tuple(evidence),
        schema_version=cast(SchemaVersion, value.get("schema_version")),
        lane=cast(Literal["hospital"], value.get("lane")),
        seal=cast(Literal["sealed"], value.get("seal")),
        owner_promotion_state=cast(Literal["outstanding"], value.get("owner_promotion_state")),
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
