"""Tests for the additive P1-33 blocked-host-custody evidence receipt."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
PENDING_SCHEMA = ROOT / "contracts/healthcare-data-platform/staging/v1/staging-activation-receipt.schema.json"
PENDING_FIXTURE = ROOT / "contracts/healthcare-data-platform/staging/v1/fixtures/pending-approval.json"
BLOCKED_SCHEMA = (
    ROOT / "contracts/healthcare-data-platform/staging/v2/staging-activation-blocked-host-custody.schema.json"
)
BLOCKED_FIXTURE = ROOT / "contracts/healthcare-data-platform/staging/v2/fixtures/blocked-host-custody-20260829.json"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _canonical_receipt_hash(receipt: dict[str, Any]) -> str:
    without_hash = {key: value for key, value in receipt.items() if key != "receipt_hash"}
    canonical = json.dumps(without_hash, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def test_original_pending_receipt_remains_the_retained_v1_preparation() -> None:
    schema = _load(PENDING_SCHEMA)
    receipt = _load(PENDING_FIXTURE)
    assert list(Draft202012Validator(schema).iter_errors(receipt)) == []
    assert receipt["source_commit"] == "ce43147c30793a4dbb35ed97649998a3a5426441"
    assert receipt["state"] == "pending_approval"
    assert receipt["activation_performed"] is False
    assert "verification" not in receipt


def test_blocked_receipt_validates_targets_and_canonical_content_hash() -> None:
    schema = _load(BLOCKED_SCHEMA)
    receipt = _load(BLOCKED_FIXTURE)
    assert list(Draft202012Validator(schema).iter_errors(receipt)) == []
    assert receipt["targets"] == {
        "data_mcp_commit": "7b11b83268c72634dbfe36f4fe5f61b447975770",
        "paired_toolkit_commit": "d21074bf9eae5bcf793186a2e003f64a7d543e27",
    }
    assert receipt["hash_algorithm"] == "sha256"
    assert receipt["hash_scope"] == "canonical_json_without_receipt_hash"
    assert receipt["receipt_hash"] == _canonical_receipt_hash(receipt)


def test_blocked_receipt_records_free_ports_and_caller_owned_fixture_seams() -> None:
    receipt = _load(BLOCKED_FIXTURE)
    preflight = receipt["port_preflight"]
    assert preflight["command"] == "ss -H -ltnp '( sport = :8110 or sport = :3020 )'"
    assert preflight["observed_at"] == "2026-08-29T16:08:11Z"
    assert {item["port"] for item in preflight["ports"]} == {8110, 3020}
    assert all(item["state"] == "free" and item["listeners"] == [] for item in preflight["ports"])

    verification = receipt["fixture_verification"]
    assert verification["state"] == "blocked_host_custody"
    assert verification["execution"] == "not_run"
    assert verification["temporary_root"] == {
        "ownership": "caller_owned_tmp_path",
        "path_template": "{tmp_path}",
        "provided_by_caller": True,
        "source_bytes_written": False,
    }
    artifacts = verification["artifacts"]
    assert artifacts["scheduler"]["path_template"] == "{tmp_path}/control/scheduler.json"
    assert artifacts["queue"]["path_template"] == "{tmp_path}/control/control.sqlite3"
    assert artifacts["raw_custody"]["path_template"] == "{tmp_path}/raw-custody"
    assert all(item["evidence_state"] == "pending" and not item["content_present"] for item in artifacts.values())


def test_blocked_receipt_rejects_activation_claims_wrong_target_and_non_tmp_paths() -> None:
    schema = _load(BLOCKED_SCHEMA)
    receipt = _load(BLOCKED_FIXTURE)

    activated = json.loads(json.dumps(receipt))
    activated["activation_performed"] = True
    assert list(Draft202012Validator(schema).iter_errors(activated))

    wrong_target = json.loads(json.dumps(receipt))
    wrong_target["targets"]["data_mcp_commit"] = "f477e126cc09d5a8ce7edc7ffcd4833cb484f766"
    assert list(Draft202012Validator(schema).iter_errors(wrong_target))

    non_tmp = json.loads(json.dumps(receipt))
    non_tmp["fixture_verification"]["artifacts"]["queue"]["path_template"] = (
        "/mnt/d/services/healthcare-toolkit-staging/control.sqlite3"
    )
    assert list(Draft202012Validator(schema).iter_errors(non_tmp))
