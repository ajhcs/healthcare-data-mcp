from __future__ import annotations

from dataclasses import dataclass
from typing import Any

RawRecord = dict[str, Any]


@dataclass(frozen=True)
class SystemConcept:
    system_id: str
    name: str
    aliases: tuple[str, ...]
    concept_type: str
    legal_entity: bool
    evidence_ids: tuple[str, ...]
    confidence: str

    @classmethod
    def from_raw(cls, raw: RawRecord) -> SystemConcept:
        return cls(
            system_id=raw["id"],
            name=raw["name"],
            aliases=tuple(raw["aliases"]),
            concept_type=raw["concept_type"],
            legal_entity=raw["legal_entity"],
            evidence_ids=tuple(raw["evidence_ids"]),
            confidence=raw["confidence"],
        )


@dataclass(frozen=True)
class EntityRecord:
    entity_id: str
    name: str
    entity_type: str
    aliases: tuple[str, ...]
    identifiers: tuple[tuple[str, str], ...]
    lifecycle_status: str
    effective_start: str | None
    effective_end: str | None
    evidence_ids: tuple[str, ...]
    notes: tuple[str, ...]
    queryable: bool = True

    @classmethod
    def from_raw(cls, raw: RawRecord) -> EntityRecord:
        return cls(
            entity_id=raw["id"],
            name=raw["name"],
            entity_type=raw["entity_type"],
            aliases=tuple(raw["aliases"]),
            identifiers=tuple((item["scheme"], item["value"]) for item in raw["identifiers"]),
            lifecycle_status=raw["lifecycle_status"],
            effective_start=raw.get("effective_start"),
            effective_end=raw.get("effective_end"),
            evidence_ids=tuple(raw["evidence_ids"]),
            notes=tuple(raw.get("notes", ())),
        )

    def identifier_pairs(self) -> tuple[tuple[str, str], ...]:
        return self.identifiers


@dataclass(frozen=True)
class EvidenceObservation:
    evidence_id: str
    title: str
    url: str | None
    locator: str
    source_period: str
    confidence: str
    limitation: str | None

    @classmethod
    def from_raw(cls, raw: RawRecord) -> EvidenceObservation:
        return cls(
            evidence_id=raw["id"],
            title=raw["title"],
            url=raw["url"],
            locator=raw["locator"],
            source_period=raw["source_period"],
            confidence=raw["confidence"],
            limitation=raw.get("limitation"),
        )


@dataclass(frozen=True)
class RelationshipRecord:
    relationship_id: str
    source_id: str
    relationship_type: str
    target_id: str
    target_kind: str
    effective_start: str | None
    effective_end: str | None
    evidence_ids: tuple[str, ...]
    confidence: str
    status: str
    notes: tuple[str, ...]

    @classmethod
    def from_raw(cls, raw: RawRecord) -> RelationshipRecord:
        return cls(
            relationship_id=raw["id"],
            source_id=raw["source_id"],
            relationship_type=raw["relationship_type"],
            target_id=raw["target_id"],
            target_kind=raw["target_kind"],
            effective_start=raw["effective_start"],
            effective_end=raw.get("effective_end"),
            evidence_ids=tuple(raw["evidence_ids"]),
            confidence=raw["confidence"],
            status=raw["status"],
            notes=tuple(raw.get("notes", ())),
        )


@dataclass(frozen=True)
class FacilityRecord:
    facility_id: str
    name: str
    ccn: str
    operator_entity_id: str | None
    operator_name: str
    operator_ein: str | None
    evidence_ids: tuple[str, ...]
    confidence: str

    @classmethod
    def from_raw(cls, raw: RawRecord) -> FacilityRecord:
        return cls(
            facility_id=raw["id"],
            name=raw["name"],
            ccn=raw["ccn"],
            operator_entity_id=raw.get("operator_entity_id"),
            operator_name=raw["operator_name"],
            operator_ein=raw.get("operator_ein"),
            evidence_ids=tuple(raw["evidence_ids"]),
            confidence=raw["confidence"],
        )


@dataclass(frozen=True)
class Measurement:
    measurement_id: str
    label: str
    amount_usd: int | None
    period: str
    basis: str
    audited: bool | None

    @classmethod
    def from_raw(cls, raw: RawRecord) -> Measurement:
        return cls(
            measurement_id=raw["id"],
            label=raw["label"],
            amount_usd=raw.get("amount_usd"),
            period=raw["period"],
            basis=raw["basis"],
            audited=raw.get("audited"),
        )


@dataclass(frozen=True)
class ReportingPerimeter:
    perimeter_id: str
    label: str
    perimeter_type: str
    includes: tuple[str, ...]
    excludes: tuple[str, ...]
    flags: tuple[str, ...]
    measurements: tuple[Measurement, ...]
    evidence_ids: tuple[str, ...]
    confidence: str

    @classmethod
    def from_raw(cls, raw: RawRecord) -> ReportingPerimeter:
        return cls(
            perimeter_id=raw["id"],
            label=raw["label"],
            perimeter_type=raw["perimeter_type"],
            includes=tuple(raw["includes"]),
            excludes=tuple(raw["excludes"]),
            flags=tuple(raw["flags"]),
            measurements=tuple(Measurement.from_raw(item) for item in raw["measurements"]),
            evidence_ids=tuple(raw["evidence_ids"]),
            confidence=raw["confidence"],
        )


@dataclass(frozen=True)
class GapRecord:
    gap_id: str
    status: str
    description: str

    @classmethod
    def from_raw(cls, raw: RawRecord) -> GapRecord:
        return cls(
            gap_id=raw["id"],
            status=raw["status"],
            description=raw["description"],
        )


@dataclass(frozen=True)
class ScopeOption:
    scope_id: str
    label: str
    amount_usd: int | None = None
    period: str | None = None
    basis: str | None = None
    includes: tuple[str, ...] = ()
    excludes: tuple[str, ...] = ()
    flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class Resolution:
    status: str
    question: str | None
    options: tuple[ScopeOption, ...]
    entity_ids: tuple[str, ...] = ()
    flags: tuple[str, ...] = ()
    identifiers: tuple[tuple[str, str], ...] = ()
    as_of: str | None = None
    tax_period_year: int | None = None
