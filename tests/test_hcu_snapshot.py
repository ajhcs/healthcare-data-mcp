"""Focused HCU receipt and source-preservation tests."""

from __future__ import annotations

from hashlib import sha256
import json
from typing import cast

import pytest

from shared.acquisition.hcu_snapshot import HCU_SOURCE_ID, HcuSnapshotError, build_hcu_observation_envelope


def _hash(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return "sha256:" + sha256(payload).hexdigest()


def _inputs() -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    checkpoint = {"checkpoint_id": "checkpoint:hcu:2026-08-29", "cursor": "snapshot:42"}
    layers = [{"layer_id": f"layer-{i:02d}", "label": f"Layer {i}"} for i in range(1, 11)]
    manifest: dict[str, object] = {
        "source_id": HCU_SOURCE_ID,
        "release_id": "release:hcu:2026-08-29",
        "release_label": "HCU synthetic fixture",
        "evidence_locator": "https://hcu.example/snapshot/42",
        "rights_state": "authorized",
        "ontology_layers": layers,
        "checkpoint": checkpoint,
    }
    snapshot: dict[str, object] = {
        "records": [
            {"source_record_id": "hcu-001", "layer_id": "layer-01", "review_state": "reviewed", "source_value": {"name": "North"}, "valid_date": "2026-08-01", "source_selector": "record:hcu-001"},
            {"source_record_id": "hcu-002", "layer_id": "layer-02", "review_state": "held", "source_value": "pending", "valid_date": "2026-08-02", "source_selector": "record:hcu-002"},
        ]
    }
    receipt: dict[str, object] = {
        "receipt_id": "receipt:hcu:42",
        "source_id": HCU_SOURCE_ID,
        "release_id": manifest["release_id"],
        "manifest_sha256": _hash(manifest),
        "snapshot_sha256": _hash(snapshot),
        "checkpoint_sha256": _hash(checkpoint),
        "rights_state": "authorized",
        "state": "succeeded",
        "recorded_at": "2026-08-29T12:00:00Z",
        "evidence_locator": "https://hcu.example/receipt/42",
    }
    receipt["receipt_sha256"] = _hash(receipt)
    return manifest, snapshot, receipt


def test_hcu_envelope_preserves_ten_layers_ids_review_and_checkpoint() -> None:
    manifest, snapshot, receipt = _inputs()
    envelope = build_hcu_observation_envelope(manifest, snapshot, receipt)
    limits = cast(dict[str, bool], envelope["authority_limits"])
    assert all(value is False for value in limits.values())
    observations = envelope["observations"]
    assert isinstance(observations, list) and len(observations) == 2
    value = observations[0]["value"]
    assert isinstance(value, dict)
    assert value["source_record_id"] == "hcu-001"
    assert value["review_state"] == "reviewed"
    assert len(value["ontology_layers"]) == 10
    assert value["checkpoint"]["cursor"] == "snapshot:42"
    held = cast(dict[str, object], observations[1])
    held_value = cast(dict[str, object], held["value"])
    assert held_value["review_state"] == "held"
    assert held["promotion_state"] == "unpromoted_observation"


@pytest.mark.parametrize("field", ["manifest_sha256", "snapshot_sha256", "checkpoint_sha256", "receipt_sha256"])
def test_hcu_receipt_fingerprint_mismatch_fails_closed(field: str) -> None:
    manifest, snapshot, receipt = _inputs()
    receipt[field] = "sha256:" + "0" * 64
    with pytest.raises(HcuSnapshotError, match="fingerprint"):
        build_hcu_observation_envelope(manifest, snapshot, receipt)


def test_hcu_rejects_wrong_layer_count_and_untrusted_rights() -> None:
    manifest, snapshot, receipt = _inputs()
    manifest["ontology_layers"] = manifest["ontology_layers"][:-1]
    receipt["manifest_sha256"] = _hash(manifest)
    receipt["receipt_sha256"] = _hash({key: value for key, value in receipt.items() if key != "receipt_sha256"})
    with pytest.raises(HcuSnapshotError, match="exactly ten"):
        build_hcu_observation_envelope(manifest, snapshot, receipt)
    manifest, snapshot, receipt = _inputs()
    manifest["rights_state"] = "pending"
    with pytest.raises(HcuSnapshotError, match="rights_state"):
        build_hcu_observation_envelope(manifest, snapshot, receipt)
