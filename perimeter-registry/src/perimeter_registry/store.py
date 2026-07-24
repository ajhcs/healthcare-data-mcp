from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from importlib.resources import files
from types import MappingProxyType
from typing import Any

from .models import (
    EntityRecord,
    EvidenceObservation,
    FacilityRecord,
    GapRecord,
    RawRecord,
    RelationshipRecord,
    ReportingPerimeter,
    SystemConcept,
)


class RegistryDataError(ValueError):
    """Raised when inspectable registry data violates the local contract."""


def normalize_text(value: str) -> str:
    return " ".join(value.casefold().split())


def _unique_by_id(records: list[RawRecord], record_type: str) -> dict[str, RawRecord]:
    indexed: dict[str, RawRecord] = {}
    for record in records:
        record_id = record.get("id")
        if not isinstance(record_id, str) or not record_id:
            raise RegistryDataError(f"{record_type} record requires a non-empty id")
        if record_id in indexed:
            raise RegistryDataError(f"duplicate {record_type} id: {record_id}")
        indexed[record_id] = record
    return indexed


def _require_evidence(record: RawRecord, evidence_ids: set[str], record_type: str) -> None:
    references = record.get("evidence_ids")
    if not isinstance(references, list) or not references:
        raise RegistryDataError(f"{record_type} {record['id']} requires evidence_ids")
    unknown = set(references) - evidence_ids
    if unknown:
        raise RegistryDataError(
            f"{record_type} {record['id']} references unknown evidence: {sorted(unknown)}"
        )


class RegistryStore:
    def __init__(self, raw: Mapping[str, Any]) -> None:
        self._raw = MappingProxyType(dict(raw))
        self._validate()
        self.systems = tuple(SystemConcept.from_raw(item) for item in raw["systems"])
        self.entities = tuple(EntityRecord.from_raw(item) for item in raw["entities"])
        self.relationships = tuple(
            RelationshipRecord.from_raw(item) for item in raw["relationships"]
        )
        self.evidence = tuple(EvidenceObservation.from_raw(item) for item in raw["evidence"])
        self.facilities = tuple(FacilityRecord.from_raw(item) for item in raw["facilities"])
        self.perimeters = tuple(
            ReportingPerimeter.from_raw(item) for item in raw["reporting_perimeters"]
        )
        self.gaps = tuple(GapRecord.from_raw(item) for item in raw["gaps"])
        self._entities_by_id = {item.entity_id: item for item in self.entities}
        self._evidence_by_id = {item.evidence_id: item for item in self.evidence}
        self._perimeters_by_id = {item.perimeter_id: item for item in self.perimeters}
        self._entity_aliases = self._index_entity_aliases()

    @classmethod
    def jefferson(cls) -> RegistryStore:
        fixture = files("perimeter_registry.data").joinpath("jefferson.json")
        with fixture.open(encoding="utf-8") as handle:
            return cls(json.load(handle))

    def entity(self, entity_id: str) -> EntityRecord:
        try:
            return self._entities_by_id[entity_id]
        except KeyError as error:
            raise LookupError(entity_id) from error

    def entity_for_alias(self, alias: str) -> EntityRecord:
        normalized = normalize_text(alias)
        try:
            return self._entity_aliases[normalized]
        except KeyError as error:
            raise LookupError(alias) from error

    def perimeter(self, perimeter_id: str) -> ReportingPerimeter:
        try:
            return self._perimeters_by_id[perimeter_id]
        except KeyError as error:
            raise LookupError(perimeter_id) from error

    def evidence_observation(self, evidence_id: str) -> EvidenceObservation:
        try:
            return self._evidence_by_id[evidence_id]
        except KeyError as error:
            raise LookupError(evidence_id) from error

    def relationships_for(self, record_id: str) -> tuple[RelationshipRecord, ...]:
        return tuple(
            item
            for item in self.relationships
            if item.source_id == record_id or item.target_id == record_id
        )

    def _index_entity_aliases(self) -> dict[str, EntityRecord]:
        aliases: dict[str, EntityRecord] = {}
        for entity in self.entities:
            for alias in (entity.entity_id, entity.name, *entity.aliases):
                normalized = normalize_text(alias)
                prior = aliases.get(normalized)
                if prior is not None and prior.entity_id != entity.entity_id:
                    raise RegistryDataError(f"ambiguous entity alias: {alias}")
                aliases[normalized] = entity
        return aliases

    def _validate(self) -> None:
        if self._raw.get("schema_version") != 1:
            raise RegistryDataError("schema_version must be 1")
        required_collections = (
            "systems",
            "entities",
            "relationships",
            "evidence",
            "facilities",
            "reporting_perimeters",
            "gaps",
        )
        for name in required_collections:
            if not isinstance(self._raw.get(name), list):
                raise RegistryDataError(f"{name} must be a list")

        indexes = {
            name: _unique_by_id(self._raw[name], name)
            for name in required_collections
            if name != "gaps"
        }
        evidence_ids = set(indexes["evidence"])
        entity_ids = set(indexes["entities"])
        perimeter_ids = set(indexes["reporting_perimeters"])

        for evidence in self._raw["evidence"]:
            for field in ("locator", "source_period", "confidence"):
                if not evidence.get(field):
                    raise RegistryDataError(f"evidence {evidence['id']} requires {field}")
        for collection in (
            "systems",
            "entities",
            "relationships",
            "facilities",
            "reporting_perimeters",
        ):
            for record in self._raw[collection]:
                _require_evidence(record, evidence_ids, collection)
        for facility in self._raw["facilities"]:
            operator_id = facility.get("operator_entity_id")
            if operator_id is not None and operator_id not in entity_ids:
                raise RegistryDataError(f"unknown facility operator: {operator_id}")
        for relationship in self._raw["relationships"]:
            if relationship["relationship_type"] == "affiliate":
                raise RegistryDataError("affiliate is not an allowed relationship type")
            if "effective_start" not in relationship:
                raise RegistryDataError(
                    f"relationship {relationship['id']} requires effective_start"
                )
            if relationship["source_id"] not in entity_ids:
                raise RegistryDataError(f"unknown relationship source: {relationship['source_id']}")
            valid_targets: Iterable[str]
            if relationship["target_kind"] == "entity":
                valid_targets = entity_ids
            elif relationship["target_kind"] == "reporting_perimeter":
                valid_targets = perimeter_ids
            elif relationship["target_kind"] == "facility":
                valid_targets = indexes["facilities"]
            elif relationship["target_kind"] == "documented_alias":
                valid_targets = {
                    alias for entity in self._raw["entities"] for alias in entity["aliases"]
                }
            else:
                raise RegistryDataError(
                    f"unknown relationship target kind: {relationship['target_kind']}"
                )
            if relationship["target_id"] not in valid_targets:
                raise RegistryDataError(f"unknown relationship target: {relationship['target_id']}")
