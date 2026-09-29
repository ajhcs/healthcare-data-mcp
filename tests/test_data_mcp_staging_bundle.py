"""Focused conformance tests for the local Data MCP staging bundle."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import cast

import pytest
import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode

from shared.queue.durable import DurableQueue
from shared.storage.raw_custody import RawArtifactMetadata, RawArtifactStore
from shared.utils.source_cadence import load_catalog
from shared.utils.source_scheduler import DurableScheduler
from shared.utils.source_cadence import PollState


ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = ROOT / "ops/staging/data-mcp-staging-bundle.yaml"
CATALOG_PATH = ROOT / "contracts/healthcare-data-platform/catalog/v1/fixtures/valid-source-catalog.json"


class _StrictLoader(yaml.SafeLoader):
    """PyYAML loader that rejects duplicate mapping keys."""


def _strict_mapping(loader: yaml.SafeLoader, node: MappingNode, deep: bool = False) -> dict[object, object]:
    if not isinstance(node, MappingNode):
        raise ConstructorError(None, None, "expected a mapping", node.start_mark)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ConstructorError(
                "while constructing a mapping", node.start_mark, f"duplicate key: {key!r}", key_node.start_mark
            )
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _strict_mapping)


def _bundle() -> dict[str, object]:
    with BUNDLE_PATH.open("r", encoding="utf-8") as handle:
        value = yaml.load(handle, Loader=_StrictLoader)
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _mapping(value: object, label: str) -> dict[str, object]:
    assert isinstance(value, dict), label
    return cast(dict[str, object], value)


def _mapping_list(value: object, label: str) -> list[dict[str, object]]:
    assert isinstance(value, list), label
    return [_mapping(item, f"{label}[{index}]") for index, item in enumerate(value)]


def test_bundle_is_strict_staging_only_and_rejects_duplicate_keys() -> None:
    bundle = _bundle()
    assert bundle["schema_version"] == "hdp.data-mcp-staging-bundle.v1"
    assert bundle["record_type"] == "staging_release_bundle"
    assert bundle["apiVersion"] == "hdp.staging/v1"
    assert bundle["kind"] == "DataMcpStagingBundle"
    assert bundle["environment"] == "staging"

    activation = _mapping(bundle["activation"], "activation")
    assert activation == {"enabled": False, "production": False, "deployment_mutations": False}
    network = _mapping(_mapping(bundle["spec"], "spec")["network"], "network")
    assert network["egress"] == "deny"
    assert network["bind_host"] == "127.0.0.1"
    assert network["allowed_hosts"] == []
    assert network["source_probes"] == "disabled"

    with pytest.raises(ConstructorError, match="duplicate key"):
        yaml.load("one: 1\none: 2\n", Loader=_StrictLoader)

    def walk(value: object) -> None:
        if isinstance(value, dict):
            for key, nested in value.items():
                key_text = str(key).casefold()
                assert key_text not in {"password", "private_key", "api_key", "secret_value", "token_value"}
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)
        elif isinstance(value, str):
            lowered = value.casefold()
            assert "-----begin" not in lowered
            assert "password=" not in lowered
            assert "token=" not in lowered
            assert "api_key=" not in lowered

    walk(bundle)


def test_topology_is_finite_and_keeps_raw_custody_separate() -> None:
    spec = _mapping(_bundle()["spec"], "spec")
    topology = _mapping(spec["topology"], "topology")
    flow = _mapping_list(topology["data_flow"], "data_flow")
    assert {item["id"] for item in flow} == {
        "catalog-to-scheduler",
        "scheduler-to-queue",
        "queue-to-worker",
        "worker-to-raw-custody",
        "worker-to-control-store",
    }

    scheduler = _mapping(topology["scheduler"], "scheduler")
    assert scheduler["replicas"] == 1
    assert scheduler["leader_mode"] == "single_leader"
    scheduler_limits = _mapping(scheduler["limits"], "scheduler limits")
    assert 0 < cast(int, scheduler_limits["poll_interval_seconds"]) <= 86_400
    assert 0 < cast(int, scheduler_limits["max_intents_per_tick"]) <= 1_024
    scheduler_permissions = _mapping(scheduler["permissions"], "scheduler permissions")
    assert scheduler_permissions["network_egress"] is False
    assert scheduler_permissions["source_payload_access"] is False
    assert scheduler_permissions["raw_custody_write"] is False

    workers = _mapping(topology["worker_pool"], "worker_pool")
    assert workers["replicas"] == 2
    lease = _mapping(workers["lease"], "worker lease")
    assert 0 < cast(int, lease["ttl_seconds"]) <= 86_400
    assert 0 < cast(int, lease["max_attempts"]) <= 100
    stream = _mapping(_mapping(workers["limits"], "worker limits")["stream"], "stream limits")
    assert 0 < cast(int, stream["max_bytes"]) <= 131_072
    assert 0 < cast(int, stream["max_chunks"]) <= 128
    assert 0 < cast(int, stream["max_chunk_bytes"]) <= 65_536
    worker_permissions = _mapping(workers["permissions"], "worker permissions")
    assert worker_permissions["network_egress"] is False
    assert worker_permissions["source_payload_access"] == "fixture_only"

    control_store = _mapping(topology["control_store"], "control_store")
    raw_custody = _mapping(topology["raw_custody"], "raw_custody")
    assert control_store["backend"] == "sqlite"
    assert control_store["journal_mode"] == "WAL"
    assert _mapping(control_store["queue"], "queue")["payload_storage"] == "forbidden"
    assert raw_custody["backend"] == "filesystem"
    assert raw_custody["immutable"] is True
    assert raw_custody["append_only"] is True
    assert raw_custody["delete_api"] is False
    raw_permissions = _mapping(raw_custody["permissions"], "raw permissions")
    assert raw_permissions["scheduler_write"] is False
    assert raw_permissions["control_store_write"] is False


def test_contract_references_and_migration_controls_are_local_and_reviewable() -> None:
    spec = _mapping(_bundle()["spec"], "spec")
    refs = _mapping(spec["source_refs"], "source_refs")
    for label, value in refs.items():
        assert isinstance(value, str), label
        assert (ROOT / value).is_file(), f"missing {label}: {value}"

    scheduler = _mapping(_mapping(_mapping(spec["topology"], "topology")["scheduler"], "scheduler"), "scheduler")
    assert scheduler["source_catalog_ref"] == refs["catalog"]
    assert scheduler["schema_ref"] == refs["scheduler_schema"]

    migrations = _mapping(spec["migrations"], "migrations")
    assert migrations["strategy"] == "forward_only"
    assert migrations["required_in_fixture"] is False
    preflight = _mapping(migrations["preflight"], "migration preflight")
    assert preflight["read_only"] is True
    snapshot = _mapping(migrations["snapshot"], "migration snapshot")
    assert snapshot["required_before_apply"] is True
    apply = _mapping(migrations["apply"], "migration apply")
    assert apply["requires_approval"] is True
    assert apply["production_allowed"] is False
    rollback = _mapping(migrations["rollback"], "migration rollback")
    assert rollback["destructive"] is False

    secrets = _mapping(spec["secrets"], "secrets")
    policy = _mapping(secrets["policy"], "secret policy")
    assert policy["inline_values"] is False
    for entry in _mapping_list(secrets["references"], "secret references"):
        assert "provider_ref" in entry
        assert str(entry["provider_ref"]).startswith("secret://staging/")
        assert "value" not in entry and "secret" not in entry


def test_isolated_fixture_proves_scheduler_queue_and_raw_custody_boundaries(tmp_path: Path) -> None:
    bundle = _bundle()
    spec = _mapping(bundle["spec"], "spec")
    fixture = _mapping(_mapping(spec["verification"], "verification")["fixture"], "fixture")
    control_root = tmp_path / "control"
    raw_root = tmp_path / "raw-custody"
    control_root.mkdir()
    assert control_root != raw_root
    assert control_root not in raw_root.parents
    assert raw_root not in control_root.parents
    assert fixture["control_store_relative_path"] == "control/control.sqlite3"
    assert fixture["raw_custody_relative_path"] == "raw-custody"

    registrations = load_catalog(CATALOG_PATH)
    now = datetime(2026, 8, 29, tzinfo=timezone.utc)
    states = tuple(
        PollState.initial(registration.source_id, next_due_at=now)
        for registration in registrations
        if registration.enabled
    )
    scheduler = DurableScheduler(registrations, states)
    intents = scheduler.schedule(now=now)
    assert len(intents) == 2
    checkpoint = scheduler.checkpoint(control_root / "scheduler.json")
    restored = DurableScheduler.restore(checkpoint, registrations)
    assert dict(restored.intents) == dict(scheduler.intents)
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["states"]

    payload = b"fixture-source-bytes"
    payload_hash = "sha256:" + sha256(payload).hexdigest()
    work_db = control_root / "control.sqlite3"
    queue = DurableQueue(
        work_db,
        max_active_items=cast(
            int,
            _mapping(_mapping(_mapping(spec["topology"], "topology")["worker_pool"], "worker")["limits"], "limits")[
                "max_active_items"
            ],
        ),
        max_active_bytes=cast(
            int,
            _mapping(_mapping(_mapping(spec["topology"], "topology")["worker_pool"], "worker")["limits"], "limits")[
                "max_active_bytes"
            ],
        ),
        lease_ttl_seconds=300,
        max_attempts=3,
    )
    enqueue = queue.enqueue(
        "source:ahrq:lighthouse",
        work_identity="release:ahrq:fixture:20260829",
        work_id="work:ahrq:fixture:20260829",
        content_sha256=payload_hash,
        byte_length=len(payload),
        payload_ref="fixture://source/ahrq/20260829",
        now=now,
    )
    assert enqueue.action == "enqueued"
    lease = queue.claim("worker:fixture", now=now)
    assert lease is not None
    assert lease.byte_size == len(payload)
    completed = queue.complete(lease.work_id, lease.owner, lease.lease_token, now=now)
    assert completed.state == "completed"
    queue.close()
    assert payload not in work_db.read_bytes()

    identity_material = "|".join(("source:ahrq:lighthouse", "release:ahrq:fixture:20260829", payload_hash))
    artifact_id = "artifact:raw:" + sha256(identity_material.encode("utf-8")).hexdigest()[:32]
    raw_store = RawArtifactStore(raw_root)
    metadata = RawArtifactMetadata(
        schema_version="hdp.raw-artifact.v1",
        record_type="raw_artifact",
        artifact_id=artifact_id,
        source_id="source:ahrq:lighthouse",
        source_url="https://example.gov/ahrq/release.json",
        release_id="release:ahrq:fixture:20260829",
        media_type="application/octet-stream",
        content_sha256=payload_hash,
        byte_length=len(payload),
        chunk_count=1,
        chunk_size=len(payload),
        idempotency_key=artifact_id.replace("artifact:raw:", "idempotency:raw:"),
        captured_at="2026-08-29T00:00:00Z",
        rights_status="approved_public",
        prior_artifact_id=None,
        response_fingerprint=None,
    )
    receipt = raw_store.put(metadata, [payload])
    assert receipt.state == "stored"
    assert raw_store.read_bytes(artifact_id) == payload
    assert receipt.object_key.startswith("objects/sha256/")
    assert receipt.metadata_key.startswith("metadata/")
    assert (raw_root / receipt.object_key).is_file()
    assert (raw_root / receipt.metadata_key).is_file()
    assert not (control_root / receipt.object_key).exists()


def test_rollback_steps_are_ordered_and_non_destructive() -> None:
    spec = _mapping(_bundle()["spec"], "spec")
    rollback = _mapping(spec["rollback"], "rollback")
    assert rollback["automated"] is True
    assert rollback["approval_required"] is True
    assert rollback["destructive"] is False
    assert rollback["preserve_queue_and_receipts"] is True
    assert rollback["preserve_raw_objects"] is True
    steps = _mapping_list(rollback["steps"], "rollback steps")
    ids = [cast(str, step["id"]) for step in steps]
    assert len(ids) == len(set(ids))
    known = set(ids)
    for index, step in enumerate(steps):
        dependencies = step["after"]
        assert isinstance(dependencies, list)
        assert set(dependencies).issubset(known)
        assert all(ids.index(cast(str, dependency)) < index for dependency in dependencies)
    assert ids[0] == "pause-scheduler"
    assert ids[-1] == "resume-scheduler"


def test_bundle_does_not_advertise_runtime_mutation() -> None:
    bundle = _bundle()
    spec = _mapping(bundle["spec"], "spec")
    verification = _mapping(spec["verification"], "verification")
    expected = _mapping(verification["fixture"], "fixture expected")
    expected_values = _mapping(expected["expected"], "expected")
    assert expected_values["production_activation"] is False
    assert expected_values["control_store_contains_payloads"] is False
    assert expected_values["raw_objects_content_addressed"] is True
    # The fixture catalog intentionally contains a disabled, unapproved
    # optional source; scheduling it produces no work and does not activate it.
    registrations = load_catalog(CATALOG_PATH)
    disabled = next(item for item in registrations if not item.enabled)
    scheduler = DurableScheduler(
        registrations,
        (PollState.initial(disabled.source_id, next_due_at=datetime.now(timezone.utc)),),
    )
    assert scheduler.schedule(now=datetime.now(timezone.utc)) == ()
