from __future__ import annotations

from dataclasses import replace
from datetime import date

from .models import (
    EntityRecord,
    EvidenceObservation,
    FacilityRecord,
    GapRecord,
    RelationshipRecord,
    ReportingPerimeter,
    Resolution,
    SystemConcept,
)
from .resolver import DeterministicResolver
from .store import RegistryStore


class Registry:
    """Read-only facade over one validated, local perimeter-registry fixture."""

    def __init__(self, store: RegistryStore) -> None:
        self._store = store
        self._resolver = DeterministicResolver(store)

    @classmethod
    def jefferson(cls) -> Registry:
        return cls(RegistryStore.jefferson())

    @property
    def system(self) -> SystemConcept:
        return self._store.systems[0]

    @property
    def entities(self) -> tuple[EntityRecord, ...]:
        return self._store.entities

    @property
    def facilities(self) -> tuple[FacilityRecord, ...]:
        return self._store.facilities

    @property
    def reporting_perimeters(self) -> tuple[ReportingPerimeter, ...]:
        return self._store.perimeters

    @property
    def gaps(self) -> tuple[GapRecord, ...]:
        return self._store.gaps

    @property
    def relationships(self) -> tuple[RelationshipRecord, ...]:
        return self._store.relationships

    def resolve(self, request: str, as_of: str | None = None) -> Resolution:
        return self._resolver.resolve(request, as_of)

    def lookup_entity(self, alias: str, as_of: str | None = None) -> EntityRecord:
        entity = self._store.entity_for_alias(alias)
        if as_of is None or entity.effective_end is None:
            return entity
        try:
            query_date = date.fromisoformat(as_of)
            end_date = date.fromisoformat(entity.effective_end)
        except ValueError as error:
            raise ValueError("as_of and effective_end must be ISO dates") from error
        if query_date < end_date and entity.lifecycle_status == "merged":
            return replace(entity, lifecycle_status="active")
        return entity

    def lookup_facility(self, ccn: str) -> FacilityRecord:
        for facility in self._store.facilities:
            if facility.ccn == ccn:
                return facility
        raise LookupError(ccn)

    def evidence(self, evidence_id: str) -> EvidenceObservation:
        return self._store.evidence_observation(evidence_id)

    def relationships_for(self, record_id: str) -> tuple[RelationshipRecord, ...]:
        return self._store.relationships_for(record_id)
