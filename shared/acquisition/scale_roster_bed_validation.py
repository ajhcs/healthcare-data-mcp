"""Shared integrity checks for frozen Scale roster/bed acquisitions."""

from __future__ import annotations

from pydantic import JsonValue

from shared.acquisition.scale_roster_bed_models import (
    PARSER_VERSION,
    AcquisitionSpec,
    FactSpec,
    FrozenAcquisition,
)
from shared.contracts.public_evidence import canonical_sha256


def normalized_fact_payload(fact: FactSpec, value: JsonValue) -> dict[str, JsonValue]:
    return {
        "fact_id": fact.fact_id,
        "entity_id": fact.entity_id,
        "measure_id": fact.measure_id,
        "value_type": fact.value_type,
        "value": value,
        "unit": fact.unit,
        "period_label": fact.period_label,
        "period_start": fact.period_start,
        "period_end": fact.period_end,
        "denominator_scope": fact.denominator_scope,
        "row_locator": fact.row_locator,
    }


def validate_frozen_against_spec(spec: AcquisitionSpec, frozen: FrozenAcquisition) -> None:
    source_ids = {item.source_id for item in spec.sources}
    frozen_source_ids = {item.source_id for item in frozen.artifacts}
    if source_ids != frozen_source_ids:
        raise ValueError("frozen artifact sources must exactly match the acquisition specification")
    source_by_id = {item.source_id: item for item in spec.sources}
    for artifact in frozen.artifacts:
        source = source_by_id[artifact.source_id]
        if artifact.source_url != source.url:
            raise ValueError(f"frozen source URL drift for {artifact.source_id}")
        expected_fingerprint = canonical_sha256(
            {
                "encoding": source.encoding,
                "header_row": source.header_row,
                "parser_kind": source.parser_kind,
                "parser_version": PARSER_VERSION,
            }
        )
        if artifact.schema_fingerprint != expected_fingerprint:
            raise ValueError(f"frozen parser schema drift for {artifact.source_id}")
        checksum_fragment = artifact.checksum_sha256.removeprefix("sha256:")[:16]
        if artifact.artifact_id != f"artifact:{artifact.source_id}:{checksum_fragment}":
            raise ValueError(f"artifact identity/checksum conflict for {artifact.source_id}")
    expected_fact_ids = {item.fact_id for item in spec.facts if item.missingness is None}
    frozen_fact_ids = {item.fact_id for item in frozen.extracted_facts}
    if expected_fact_ids != frozen_fact_ids:
        raise ValueError("frozen extracted facts must exactly match all populated fact specifications")


__all__ = ["normalized_fact_payload", "validate_frozen_against_spec"]
