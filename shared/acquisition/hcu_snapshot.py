"""Receipt-bound, source-scoped producer for an external HCU snapshot.

The caller supplies already-authorized JSON-compatible mappings.  This module
only verifies their public receipt and deterministic fingerprints, then emits
the pinned observation envelope.  It never acquires, stores, publishes, or
promotes HCU data.
"""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import re
from typing import Mapping, cast

from shared.contracts.healthcare_data_platform import validate_observation_envelope


HCU_SOURCE_ID = "source:hcu"
HCU_PACKET_ID = "p0-09-observation-provenance-delta-v1"
# These are the constants pinned by the shared v1 envelope contract.  HCU's
# own bead/base remain producer metadata in the receipt, never authority.
HCU_TRACKING_BEAD = "healthcare-toolkit-rrna.9"
HCU_FROZEN_DISPATCH_BASE = "11d16f8303226619161f9bef03cb312f693b2d49"
HCU_PRODUCER_NAME = "healthcare-data-mcp:hcu-snapshot-producer"
HCU_RECEIPT_SCHEMA = "hdp.hcu-snapshot-receipt.v1"
HCU_ENVELOPE_SCHEMA_VERSION = "hdp.observation-envelope.v1"
HCU_LAYER_COUNT = 10
_ID = re.compile(r"^[a-z0-9][a-z0-9._:-]*$")
_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_LOCATOR = re.compile(r"^(?:https://|object://|parquet://|docs/|contracts/)[A-Za-z0-9._:/-]+$")
_SELECTOR = re.compile(r"^(?:row|record|field|json-pointer):[A-Za-z0-9._:/-]+$")


class HcuSnapshotError(ValueError):
    """Raised when an HCU manifest, receipt, or snapshot fails closed."""


def _obj(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise HcuSnapshotError(f"{label} must be an object")
    return dict(value)


def _text(value: object, label: str, *, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise HcuSnapshotError(f"{label} must be a non-empty string")
    result = value.strip()
    if pattern is not None and pattern.fullmatch(result) is None:
        raise HcuSnapshotError(f"{label} has an invalid format")
    return result


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
    except (TypeError, ValueError) as exc:
        raise HcuSnapshotError("manifest, checkpoint, and snapshot must be JSON-compatible") from exc


def _hash(value: object) -> str:
    return "sha256:" + sha256(_canonical(value)).hexdigest()


def _fingerprint(value: object, label: str) -> str:
    digest = _text(value, label, pattern=_SHA)
    return digest


def _timestamp(value: object | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    text = _text(value, "recorded_at")
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HcuSnapshotError("recorded_at must be an RFC 3339 timestamp") from exc
    return text


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "record"


def _authority_limits() -> dict[str, bool]:
    return {
        "acquisition_allowed": False,
        "mutation_allowed": False,
        "deletion_allowed": False,
        "publication_allowed": False,
        "release_allowed": False,
        "production_allowed": False,
        "runtime_allowed": False,
        "current_projection_allowed": False,
        "identity_promotion_allowed": False,
    }


def build_hcu_observation_envelope(
    manifest: Mapping[str, object],
    snapshot: Mapping[str, object],
    receipt: Mapping[str, object],
    *,
    prior_lineage_id: str | None = None,
) -> dict[str, object]:
    """Verify an external receipt and emit a deterministic HCU envelope."""

    manifest_value = dict(manifest)
    snapshot_value = dict(snapshot)
    manifest_hash = _hash(manifest_value)
    snapshot_bytes = _canonical(snapshot_value)
    snapshot_hash = "sha256:" + sha256(snapshot_bytes).hexdigest()
    checkpoint = _obj(manifest_value.get("checkpoint"), "manifest.checkpoint")
    checkpoint_hash = _hash(checkpoint)

    source_id = _text(manifest_value.get("source_id"), "manifest.source_id")
    if source_id != HCU_SOURCE_ID:
        raise HcuSnapshotError("manifest source_id is not HCU")
    release_id = _text(manifest_value.get("release_id"), "manifest.release_id")
    if not release_id.startswith("release:") or not _ID.fullmatch(source_id) or not _ID.fullmatch(release_id):
        raise HcuSnapshotError("manifest source/release identifiers are invalid")
    release_label = _text(manifest_value.get("release_label"), "manifest.release_label")
    evidence_locator = _text(manifest_value.get("evidence_locator"), "manifest.evidence_locator", pattern=_LOCATOR)
    rights_state = _text(manifest_value.get("rights_state"), "manifest.rights_state")
    if rights_state != "authorized":
        raise HcuSnapshotError("HCU manifest rights_state must be authorized")

    raw_layers = manifest_value.get("ontology_layers")
    if not isinstance(raw_layers, list) or len(raw_layers) != HCU_LAYER_COUNT:
        raise HcuSnapshotError("HCU manifest must contain exactly ten ontology layers")
    layers: list[dict[str, str]] = []
    layer_ids: set[str] = set()
    for index, raw_layer in enumerate(raw_layers):
        layer = _obj(raw_layer, f"ontology_layers[{index}]")
        layer_id = _text(layer.get("layer_id"), f"ontology_layers[{index}].layer_id")
        label = _text(layer.get("label"), f"ontology_layers[{index}].label")
        if layer_id in layer_ids:
            raise HcuSnapshotError(f"duplicate ontology layer: {layer_id}")
        layer_ids.add(layer_id)
        layers.append({"layer_id": layer_id, "label": label})

    receipt_value = dict(receipt)
    if _text(receipt_value.get("state"), "receipt.state") != "succeeded":
        raise HcuSnapshotError("external HCU receipt must be succeeded")
    if _text(receipt_value.get("rights_state"), "receipt.rights_state") != "authorized":
        raise HcuSnapshotError("external HCU receipt is not authorized")
    if _text(receipt_value.get("source_id"), "receipt.source_id") != source_id:
        raise HcuSnapshotError("receipt source_id does not match manifest")
    if _text(receipt_value.get("release_id"), "receipt.release_id") != release_id:
        raise HcuSnapshotError("receipt release_id does not match manifest")
    if _fingerprint(receipt_value.get("manifest_sha256"), "receipt.manifest_sha256") != manifest_hash:
        raise HcuSnapshotError("HCU manifest fingerprint does not match receipt")
    if _fingerprint(receipt_value.get("snapshot_sha256"), "receipt.snapshot_sha256") != snapshot_hash:
        raise HcuSnapshotError("HCU snapshot fingerprint does not match receipt")
    if _fingerprint(receipt_value.get("checkpoint_sha256"), "receipt.checkpoint_sha256") != checkpoint_hash:
        raise HcuSnapshotError("HCU checkpoint fingerprint does not match receipt")
    receipt_without_hash = {key: value for key, value in receipt_value.items() if key != "receipt_sha256"}
    if _fingerprint(receipt_value.get("receipt_sha256"), "receipt.receipt_sha256") != _hash(receipt_without_hash):
        raise HcuSnapshotError("external HCU receipt fingerprint is invalid")
    receipt_id = _text(receipt_value.get("receipt_id"), "receipt.receipt_id")
    receipt_locator = _text(
        receipt_value.get("evidence_locator", evidence_locator), "receipt.evidence_locator", pattern=_LOCATOR
    )
    recorded_at = _timestamp(receipt_value.get("recorded_at"))

    raw_records = snapshot_value.get("records")
    if not isinstance(raw_records, list) or not raw_records:
        raise HcuSnapshotError("HCU snapshot records must be a non-empty array")
    if len(raw_records) > 10000:
        raise HcuSnapshotError("HCU snapshot exceeds bounded record limit")
    records: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, raw_record in enumerate(raw_records):
        record = _obj(raw_record, f"snapshot.records[{index}]")
        source_record_id = _text(record.get("source_record_id"), f"records[{index}].source_record_id")
        layer_id = _text(record.get("layer_id"), f"records[{index}].layer_id")
        if layer_id not in layer_ids:
            raise HcuSnapshotError(f"records[{index}] references unknown ontology layer")
        review_state = _text(record.get("review_state"), f"records[{index}].review_state")
        source_selector = _text(record.get("source_selector"), f"records[{index}].source_selector", pattern=_SELECTOR)
        valid_date = _text(record.get("valid_date"), f"records[{index}].valid_date", pattern=_DATE)
        if source_record_id in seen:
            raise HcuSnapshotError(f"duplicate HCU source record: {source_record_id}")
        seen.add(source_record_id)
        records.append(
            {
                "source_record_id": source_record_id,
                "layer_id": layer_id,
                "review_state": review_state,
                "source_value": record.get("source_value"),
                "valid_date": valid_date,
                "source_selector": source_selector,
            }
        )

    artifact_digest = snapshot_hash.removeprefix("sha256:")
    artifact_id = f"artifact:hcu:snapshot:{artifact_digest[:32]}"
    material = f"{source_id}|{release_id}|{snapshot_hash}|{checkpoint_hash}".encode()
    digest = sha256(material).hexdigest()
    record_id = f"hdp:observation-envelope:hcu:{digest[:32]}"
    activity_id = f"activity:hcu:snapshot:{digest[:32]}"
    run_id = f"run:hcu:{digest[:32]}"
    lineage_id = f"lineage:hcu:{digest[:32]}"
    idempotency_key = f"idempotency:hcu:{digest[:32]}"
    # Opaque upstream IDs can collide after human-readable slugging (e.g.
    # ``A-B`` and ``A B``), so retain a digest suffix for deterministic keys.
    observation_ids = [
        f"observation:hcu:{_slug(str(item['source_record_id']))}:{sha256(str(item['source_record_id']).encode()).hexdigest()[:12]}"
        for item in records
    ]
    replay_state = "replayed" if prior_lineage_id is not None else "first_seen"
    if prior_lineage_id is not None and (
        not isinstance(prior_lineage_id, str) or not prior_lineage_id.startswith("lineage:")
    ):
        raise HcuSnapshotError("prior_lineage_id is invalid")
    custody_locator = f"object://hcu/snapshots/{artifact_digest}"
    observations: list[dict[str, object]] = []
    for observation_id, record in zip(observation_ids, records):
        source_record_id = cast(str, record["source_record_id"])
        missing = record["source_value"] is None
        observations.append(
            {
                "observation_id": observation_id,
                "identity_key": f"identity:hcu:{sha256(source_record_id.encode()).hexdigest()[:24]}",
                "subject_ref": f"source-record:{_slug(source_record_id)}",
                "attribute_term_ref": "term:hcu-source-record",
                "value": None if missing else {**record, "ontology_layers": layers, "checkpoint": checkpoint},
                "value_state": "not_yet_researched" if missing else "observed",
                "source_scope": {
                    "scope_id": f"scope:hcu:{digest[:16]}",
                    "source_id": source_id,
                    "release_ref": release_id,
                    "artifact_ref": artifact_id,
                    "custody_locator": custody_locator,
                    "selector": record["source_selector"],
                    "authority_state": "source_scoped",
                },
                "activity_ref": activity_id,
                "receipt_ref": receipt_id,
                "valid_time": {
                    "precision": "day",
                    "as_of": record["valid_date"],
                    "valid_from": record["valid_date"],
                    "valid_to": None,
                },
                "transaction_time": {"recorded_from": recorded_at, "recorded_to": None},
                "conflict": {
                    "state": "missingness" if missing else "none",
                    "reason": "HCU source value is absent" if missing else "HCU source record retained",
                    "resolution": "abstained" if missing else "not_required",
                    "competing_observation_refs": [],
                },
                "promotion_state": "unpromoted_observation",
            }
        )
    envelope: dict[str, object] = {
        "schema_version": HCU_ENVELOPE_SCHEMA_VERSION,
        "record_type": "observation_envelope",
        "record_id": record_id,
        "packet_id": HCU_PACKET_ID,
        "tracking_bead": HCU_TRACKING_BEAD,
        "frozen_dispatch_base": HCU_FROZEN_DISPATCH_BASE,
        "source_release": {
            "source_id": source_id,
            "release_id": release_id,
            "release_label": release_label,
            "source_kind": "official_dataset",
            "release_sha256": manifest_hash,
            "evidence_locator": evidence_locator,
            "coverage_state": "present",
        },
        "artifact": {
            "artifact_id": artifact_id,
            "release_ref": release_id,
            "artifact_kind": "observation_batch",
            "media_type": "application/json",
            "content_sha256": snapshot_hash,
            "byte_length": len(snapshot_bytes),
            "custody": {
                "locator": custody_locator,
                "storage_plane": "object_storage",
                "immutable": True,
                "retention": "append_only",
            },
        },
        "receipt": {
            "receipt_id": receipt_id,
            "producer": HCU_PRODUCER_NAME,
            "receipt_schema": HCU_RECEIPT_SCHEMA,
            "source_release_ref": release_id,
            "artifact_ref": artifact_id,
            "source_release_sha256": manifest_hash,
            "artifact_sha256": snapshot_hash,
            "receipt_sha256": _text(receipt_value.get("receipt_sha256"), "receipt.receipt_sha256", pattern=_SHA),
            "state": "succeeded",
            "recorded_at": recorded_at,
            "evidence_locator": receipt_locator,
        },
        "activity": {
            "activity_id": activity_id,
            "run_id": run_id,
            "activity_type": "observation",
            "actor": {"actor_type": "deterministic_transform", "actor_id": HCU_PRODUCER_NAME},
            "started_at": recorded_at,
            "ended_at": recorded_at,
            "status": "succeeded",
            "input_artifact_refs": [artifact_id],
            "output_artifact_refs": [artifact_id],
        },
        "observations": observations,
        "lineage": {
            "lineage_id": lineage_id,
            "source_release_ref": release_id,
            "artifact_ref": artifact_id,
            "receipt_ref": receipt_id,
            "activity_ref": activity_id,
            "observation_ids": observation_ids,
            "deterministic_order": observation_ids,
            "replay": {
                "idempotency_key": idempotency_key,
                "state": replay_state,
                "replay_of": prior_lineage_id,
                "deterministic": True,
            },
        },
        "authority_limits": _authority_limits(),
    }
    return validate_observation_envelope(envelope)


__all__ = ["HCU_LAYER_COUNT", "HCU_SOURCE_ID", "HcuSnapshotError", "build_hcu_observation_envelope"]
