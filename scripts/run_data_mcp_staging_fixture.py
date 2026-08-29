#!/usr/bin/env python3
"""Run the bounded, deterministic, offline Data MCP staging fixture.

This is a one-shot control-plane exercise.  It loads only repository-local
contracts and a committed fixture payload, uses caller-owned temporary roots,
and never starts a listener or contacts a source.  The receipt deliberately
contains relative paths and fixed timestamps so two clean runs are byte-for-
byte comparable.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
import sys
from typing import Callable, Mapping, cast

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode

# Executing ``python scripts/...`` puts scripts/, rather than the checkout,
# first on sys.path.  Make the exact reviewed checkout the import root.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from shared.queue.durable import DurableQueue, QueuePolicy  # noqa: E402
from shared.storage.raw_custody import (  # noqa: E402
    ArtifactCollisionError,
    RawArtifactMetadata,
    RawArtifactStore,
)
from shared.utils.source_cadence import PollState, load_catalog  # noqa: E402
from shared.utils.source_scheduler import DurableScheduler  # noqa: E402


BUNDLE_PATH = REPO_ROOT / "ops/staging/data-mcp-staging-bundle.yaml"
CATALOG_PATH = REPO_ROOT / "contracts/healthcare-data-platform/catalog/v1/fixtures/valid-source-catalog.json"
INPUT_PATH = REPO_ROOT / "contracts/healthcare-data-platform/staging/v1/fixtures/deterministic-source-input.json"
INPUT_SCHEMA_PATH = (
    REPO_ROOT / "contracts/healthcare-data-platform/staging/v1/fixtures/deterministic-source-input.schema.json"
)
SCHEDULER_SCHEMA_PATH = REPO_ROOT / "contracts/healthcare-data-platform/scheduler/v1/scheduler-state.schema.json"
QUEUE_SCHEMA_PATH = REPO_ROOT / "contracts/healthcare-data-platform/queue/v1/queue.schema.json"
RAW_SCHEMA_PATH = REPO_ROOT / "contracts/healthcare-data-platform/storage/v1/raw-artifact.schema.json"

CANDIDATE_BASE_SHA = "63539f8412831ee62b067c76c3b5c395108481cc"
RUN_AT = datetime(2026, 8, 29, tzinfo=timezone.utc)
RUN_AT_TEXT = "2026-08-29T00:00:00Z"
ENTRYPOINT = "scripts/run_data_mcp_staging_fixture.py"
FIXTURE_VERSION = "r3-data-mcp-fixture.v1"
BLOCKED_NETWORK_EVENTS = frozenset(
    {
        "socket.connect",
        "socket.bind",
        "socket.listen",
        "socket.accept",
        "socket.sendto",
        "socket.getaddrinfo",
        "socket.gethostbyname",
        "socket.gethostbyname_ex",
    }
)


class FixtureError(RuntimeError):
    """Raised when the deterministic fixture contract cannot be proven."""


class _StrictLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate keys instead of silently merging."""


def _strict_mapping(loader: yaml.SafeLoader, node: MappingNode, deep: bool = False) -> dict[object, object]:
    if not isinstance(node, MappingNode):
        raise ConstructorError(None, None, "expected a mapping", node.start_mark)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"duplicate key: {key!r}",
                key_node.start_mark,
            )
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _strict_mapping)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise FixtureError(f"{label} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise FixtureError(f"{label} has a non-string key")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise FixtureError(f"{label} must be an array")
    return value


def _required_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FixtureError(f"{label} must be an integer")
    return value


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise FixtureError(f"{label} must be a non-empty string")
    return value


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise FixtureError(f"unable to hash required file: {path}") from exc


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_strict_pairs)
    except (OSError, UnicodeError, json.JSONDecodeError, FixtureError) as exc:
        raise FixtureError(f"unable to load {label}: {path}") from exc
    return _mapping(value, label)


def _strict_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise FixtureError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _load_bundle() -> tuple[dict[str, object], dict[str, object]]:
    try:
        value = yaml.load(BUNDLE_PATH.read_text(encoding="utf-8"), Loader=_StrictLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise FixtureError(f"unable to load staging bundle: {BUNDLE_PATH}") from exc
    bundle = _mapping(value, "staging bundle")
    if bundle.get("schema_version") != "hdp.data-mcp-staging-bundle.v1":
        raise FixtureError("staging bundle schema_version is unsupported")
    if bundle.get("record_type") != "staging_release_bundle":
        raise FixtureError("staging bundle record_type is unsupported")
    if bundle.get("environment") != "staging":
        raise FixtureError("fixture requires environment=staging")
    activation = _mapping(bundle.get("activation"), "bundle.activation")
    if activation != {"enabled": False, "production": False, "deployment_mutations": False}:
        raise FixtureError("staging fixture activation flags must all be false")
    spec = _mapping(bundle.get("spec"), "bundle.spec")
    if spec.get("mode") != "isolated_fixture":
        raise FixtureError("fixture requires isolated_fixture mode")
    network = _mapping(spec.get("network"), "bundle.spec.network")
    if network.get("egress") != "deny" or network.get("bind_host") != "127.0.0.1":
        raise FixtureError("fixture network policy is not denied loopback-only")
    if network.get("allowed_hosts") != [] or network.get("source_probes") != "disabled":
        raise FixtureError("fixture source/network allow-list is not empty")

    refs = _mapping(spec.get("source_refs"), "bundle.spec.source_refs")
    expected_refs = {
        "catalog": CATALOG_PATH.relative_to(REPO_ROOT).as_posix(),
        "scheduler_schema": SCHEDULER_SCHEMA_PATH.relative_to(REPO_ROOT).as_posix(),
        "queue_schema": QUEUE_SCHEMA_PATH.relative_to(REPO_ROOT).as_posix(),
        "raw_artifact_schema": RAW_SCHEMA_PATH.relative_to(REPO_ROOT).as_posix(),
    }
    for key, expected in expected_refs.items():
        if refs.get(key) != expected:
            raise FixtureError(f"bundle source_refs.{key} is not the reviewed local contract")
        if not (REPO_ROOT / expected).is_file():
            raise FixtureError(f"bundle source_refs.{key} is missing: {expected}")

    topology = _mapping(spec.get("topology"), "bundle.spec.topology")
    scheduler = _mapping(topology.get("scheduler"), "bundle.spec.topology.scheduler")
    worker_pool = _mapping(topology.get("worker_pool"), "bundle.spec.topology.worker_pool")
    scheduler_limits = _mapping(scheduler.get("limits"), "scheduler limits")
    if _required_int(scheduler_limits.get("max_intents_per_tick"), "max_intents_per_tick") > 1024:
        raise FixtureError("scheduler intent bound exceeds hard maximum")
    worker_limits = _mapping(worker_pool.get("limits"), "worker limits")
    stream = _mapping(worker_limits.get("stream"), "worker stream limits")
    bounds = {
        "max_active_items": _required_int(worker_limits.get("max_active_items"), "max_active_items"),
        "max_active_bytes": _required_int(worker_limits.get("max_active_bytes"), "max_active_bytes"),
        "max_stream_bytes": _required_int(stream.get("max_bytes"), "max_stream_bytes"),
        "max_stream_chunks": _required_int(stream.get("max_chunks"), "max_stream_chunks"),
        "max_chunk_bytes": _required_int(stream.get("max_chunk_bytes"), "max_chunk_bytes"),
        "max_stream_seconds": _required_int(stream.get("max_seconds"), "max_stream_seconds"),
    }
    if bounds != {
        "max_active_items": 4,
        "max_active_bytes": 1_048_576,
        "max_stream_bytes": 131_072,
        "max_stream_chunks": 128,
        "max_chunk_bytes": 65_536,
        "max_stream_seconds": 60,
    }:
        raise FixtureError("fixture bounds do not match the approved staging bundle")
    lease = _mapping(worker_pool.get("lease"), "worker lease")
    for field, expected in {
        "ttl_seconds": 300,
        "heartbeat_extension_seconds": 300,
        "retry_delay_seconds": 30,
        "max_attempts": 3,
    }.items():
        if lease.get(field) != expected:
            raise FixtureError(f"worker lease {field} is not frozen at {expected}")
    control = _mapping(topology.get("control_store"), "control store")
    if control.get("journal_mode") != "WAL":
        raise FixtureError("control store must use WAL")
    raw = _mapping(topology.get("raw_custody"), "raw custody")
    if raw.get("immutable") is not True or raw.get("append_only") is not True or raw.get("delete_api") is not False:
        raise FixtureError("raw custody must be immutable append-only with no delete API")
    return bundle, bounds


def _load_fixture_input() -> dict[str, object]:
    value = _load_json(INPUT_PATH, "deterministic fixture input")
    schema = _load_json(INPUT_SCHEMA_PATH, "deterministic fixture input schema")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(value), key=lambda item: list(item.path))
    if errors:
        raise FixtureError(f"fixture input schema validation failed: {errors[0].message}")
    return value


def _install_network_guard() -> list[str]:
    attempts: list[str] = []

    def audit(event: str, _arguments: object) -> None:
        if event in BLOCKED_NETWORK_EVENTS:
            attempts.append(event)
            raise FixtureError(f"network operation denied by staging fixture: {event}")

    sys.addaudithook(audit)
    return attempts


def _prepare_root(value: str) -> Path:
    root = Path(value)
    if not root.is_absolute():
        raise FixtureError("--root must be an absolute caller-owned path")
    if root.is_symlink():
        raise FixtureError("--root must not be a symlink")
    resolved = root.resolve(strict=False)
    forbidden = (Path("/"), Path("/mnt/d"), Path("/mnt/d/services"), Path("/var/lib/healthcare-toolkit-staging"))
    if any(resolved == path or (path != Path("/") and path in resolved.parents) for path in forbidden):
        raise FixtureError("--root is a protected production or host-custody path")
    if root.exists():
        if not root.is_dir():
            raise FixtureError("--root exists but is not a directory")
        try:
            if any(root.iterdir()):
                raise FixtureError("--root must be empty before a one-shot run")
        except OSError as exc:
            raise FixtureError("unable to inspect --root") from exc
    else:
        try:
            root.mkdir(parents=True, exist_ok=False)
        except OSError as exc:
            raise FixtureError(f"unable to create isolated fixture root: {root}") from exc
    for child in (root / "control", root / "raw-custody", root / "evidence"):
        child.mkdir(mode=0o750)
    return root


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError as exc:
        raise FixtureError(f"fixture path escaped root: {path}") from exc


def _write_json(root: Path, path: Path, value: object) -> None:
    if path.is_symlink():
        raise FixtureError(f"refusing symlinked fixture output: {path}")
    if root not in path.parents:
        raise FixtureError(f"fixture output escaped root: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_canonical_bytes(value))


def _open_queue(
    path: Path, policy: QueuePolicy, clock: Callable[[], datetime]
) -> tuple[DurableQueue, sqlite3.Connection]:
    try:
        connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False, timeout=5.0)
        connection.execute("PRAGMA journal_mode=WAL")
        queue = DurableQueue(connection, policy=policy, clock=clock)
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise FixtureError("unable to initialize isolated WAL control store") from exc
    return queue, connection


def _artifact_metadata(
    fixture: Mapping[str, object],
    release_id: str,
    payload: bytes,
    *,
    prior_artifact_id: str | None = None,
) -> RawArtifactMetadata:
    source_id = _required_string(fixture.get("source_id"), "fixture.source_id")
    source_url = _required_string(fixture.get("source_url"), "fixture.source_url")
    media_type = _required_string(fixture.get("media_type"), "fixture.media_type")
    content_sha = _sha256_bytes(payload)
    identity = sha256(f"{source_id}|{release_id}|{content_sha}".encode("utf-8")).hexdigest()[:32]
    chunk_size = min(65_536, len(payload))
    return RawArtifactMetadata(
        schema_version="hdp.raw-artifact.v1",
        record_type="raw_artifact",
        artifact_id=f"artifact:raw:{identity}",
        source_id=source_id,
        source_url=source_url,
        release_id=release_id,
        media_type=media_type,
        content_sha256=content_sha,
        byte_length=len(payload),
        chunk_count=1,
        chunk_size=chunk_size,
        idempotency_key=f"idempotency:raw:{identity}",
        captured_at=RUN_AT_TEXT,
        rights_status="approved_public",
        prior_artifact_id=prior_artifact_id,
        response_fingerprint=content_sha,
    )


def _receipt_digest(value: Mapping[str, object]) -> str:
    without_digest = dict(value)
    without_digest.pop("receipt_sha256", None)
    return _sha256_bytes(_canonical_bytes(without_digest))


def _check(condition: bool, label: str) -> dict[str, object]:
    if not condition:
        raise FixtureError(f"fixture invariant failed: {label}")
    return {"state": "passed", "evidence": label}


def run(candidate_sha: str, root_value: str) -> dict[str, object]:
    """Execute the complete deterministic fixture and return its receipt."""

    if not (len(candidate_sha) == 40 and all(character in "0123456789abcdef" for character in candidate_sha)):
        raise FixtureError("--candidate-sha must be a 40-character lowercase Git SHA")
    network_attempts = _install_network_guard()
    bundle, bounds = _load_bundle()
    fixture = _load_fixture_input()
    root = _prepare_root(root_value)
    control_root = root / "control"
    raw_root = root / "raw-custody"
    queue_path = control_root / "control.sqlite3"
    scheduler_path = control_root / "scheduler.json"
    acknowledgement_path = control_root / "acknowledgement-checkpoint.json"
    current_path = control_root / "current.json"
    projections_path = control_root / "projections.json"

    registrations = load_catalog(CATALOG_PATH)
    enabled = tuple(registration for registration in registrations if registration.enabled)
    states = tuple(PollState.initial(registration.source_id, next_due_at=RUN_AT) for registration in enabled)
    scheduler = DurableScheduler(registrations, states)
    initial_intents = scheduler.schedule(now=RUN_AT)
    no_op_at = RUN_AT + timedelta(seconds=1)
    changed_at = RUN_AT + timedelta(seconds=2)
    source_id = _required_string(fixture.get("source_id"), "fixture.source_id")
    changed_payload = _required_string(fixture.get("changed_payload"), "fixture.changed_payload").encode("utf-8")
    changed_hash = _sha256_bytes(changed_payload)
    no_op_state = scheduler.apply_result(
        source_id,
        "no_op",
        attempted_at=no_op_at,
        next_due_at=RUN_AT + timedelta(hours=1),
    )
    changed_state = scheduler.apply_result(
        source_id,
        "changed",
        attempted_at=changed_at,
        next_due_at=RUN_AT + timedelta(hours=2),
        release_id=cast(str, fixture["changed_release_id"]),
        release_fingerprint=changed_hash,
    )
    scheduler.checkpoint(scheduler_path)
    restored_scheduler = DurableScheduler.restore(scheduler_path, registrations)
    scheduler_restart_ok = restored_scheduler.snapshot() == scheduler.snapshot()

    lease = _mapping(
        _mapping(_mapping(bundle["spec"], "bundle.spec")["topology"], "topology")["worker_pool"], "worker pool"
    )["lease"]
    policy = QueuePolicy(
        max_active_items=bounds["max_active_items"],
        max_active_bytes=bounds["max_active_bytes"],
        lease_ttl_seconds=cast(float, lease["ttl_seconds"]),
        heartbeat_extension_seconds=cast(float, lease["heartbeat_extension_seconds"]),
        retry_delay_seconds=cast(float, lease["retry_delay_seconds"]),
        max_attempts=bounds["max_attempts"] if "max_attempts" in bounds else cast(int, lease["max_attempts"]),
    )
    # Keep the frozen max-attempt value explicit even though it is also present
    # in the worker lease map above.
    if policy.max_attempts != cast(int, lease["max_attempts"]):
        raise FixtureError("queue max_attempts does not match bundle")

    queue, connection = _open_queue(queue_path, policy, lambda: RUN_AT)
    try:
        baseline_payload = _required_string(fixture.get("baseline_payload"), "fixture.baseline_payload").encode("utf-8")
        baseline_hash = _sha256_bytes(baseline_payload)
        baseline_work = queue.enqueue(
            source_id,
            work_id="work:fixture:baseline",
            work_identity="fixture:baseline",
            payload_sha256=baseline_hash,
            byte_size=len(baseline_payload),
            payload_ref="fixture/baseline",
            now=RUN_AT,
        )
        baseline_lease = queue.claim("fixture-worker", now=RUN_AT)
        if baseline_lease is None:
            raise FixtureError("baseline fixture work was not leased")
        baseline_complete = queue.acknowledge(
            baseline_lease.work_id,
            baseline_lease.owner,
            baseline_lease.token,
            now=RUN_AT + timedelta(seconds=1),
        )
        if baseline_complete.state != "completed":
            raise FixtureError("baseline fixture acknowledgement did not complete")
        acknowledgement = {
            "schema_version": "hdp.staging-fixture-ack.v1",
            "record_type": "staging_fixture_acknowledgement",
            "acknowledged_at": "2026-08-29T00:00:01Z",
            "work_id": baseline_complete.work_id,
            "state": baseline_complete.state,
            "checkpoint_order": "acknowledgement-before-checkpoint",
        }
        _write_json(root, acknowledgement_path, acknowledgement)
        ack_before_checkpoint = acknowledgement_path.exists() and baseline_complete.state == "completed"

        queue.close()
        connection.close()
        queue, connection = _open_queue(queue_path, policy, lambda: RUN_AT)
        baseline_restart = queue.get(baseline_complete.work_id)
        baseline_duplicate = queue.enqueue(
            source_id,
            work_id="work:fixture:baseline",
            work_identity="fixture:baseline",
            payload_sha256=baseline_hash,
            byte_size=len(baseline_payload),
            payload_ref="fixture/baseline",
            now=RUN_AT,
        )
        restart_ok = baseline_restart.state == "completed"
        duplicate_replay_ok = baseline_duplicate.action == "duplicate"

        changed_work = queue.enqueue(
            source_id,
            work_id="work:fixture:changed",
            work_identity="fixture:changed",
            payload_sha256=changed_hash,
            byte_size=len(changed_payload),
            payload_ref="fixture/changed",
            now=changed_at,
        )
        changed_lease = queue.claim("fixture-worker", now=changed_at)
        if changed_lease is None or changed_lease.work_id != changed_work.work_id:
            raise FixtureError("changed fixture work was not leased")
        changed_complete = queue.acknowledge(
            changed_lease.work_id,
            changed_lease.owner,
            changed_lease.token,
            now=changed_at + timedelta(seconds=1),
        )
        changed_ack_ok = changed_complete.state == "completed"

        recovery_work = queue.enqueue(
            source_id,
            work_id="work:fixture:lease-recovery",
            work_identity="fixture:lease-recovery",
            payload_sha256=baseline_hash,
            byte_size=len(baseline_payload),
            payload_ref="fixture/lease-recovery",
            now=RUN_AT,
        )
        first_recovery_lease = queue.claim("fixture-worker-crashed", now=RUN_AT)
        if first_recovery_lease is None or first_recovery_lease.work_id != recovery_work.work_id:
            raise FixtureError("lease recovery fixture work was not leased")
        recovery_receipts = queue.recover_expired(now=RUN_AT + timedelta(seconds=301))
        recovered = queue.claim("fixture-worker-recovered", now=RUN_AT + timedelta(seconds=301))
        if recovered is None:
            raise FixtureError("expired fixture lease was not requeued")
        recovered_complete = queue.complete(
            recovered.work_id,
            recovered.owner,
            recovered.token,
            now=RUN_AT + timedelta(seconds=302),
        )
        lease_recovery_ok = (
            len(recovery_receipts) == 1
            and recovery_receipts[0].state == "requeued"
            and recovered.attempt == 2
            and recovered_complete.state == "completed"
        )

        quarantine_work = queue.enqueue(
            source_id,
            work_id="work:fixture:quarantine",
            work_identity="fixture:quarantine",
            payload_sha256=_sha256_bytes(
                _required_string(fixture.get("quarantine_payload"), "fixture.quarantine_payload").encode("utf-8")
            ),
            byte_size=len(_required_string(fixture.get("quarantine_payload"), "fixture.quarantine_payload")),
            payload_ref="fixture/quarantine",
            now=changed_at,
        )
        quarantine_lease = queue.claim("fixture-worker", now=changed_at)
        if quarantine_lease is None or quarantine_lease.work_id != quarantine_work.work_id:
            raise FixtureError("quarantine fixture work was not leased")

        baseline_meta = _artifact_metadata(
            fixture,
            cast(str, fixture["baseline_release_id"]),
            baseline_payload,
        )
        store = RawArtifactStore(
            raw_root,
            max_bytes=bounds["max_stream_bytes"],
            max_chunks=bounds["max_stream_chunks"],
        )
        baseline_custody = store.put(baseline_meta, [baseline_payload])
        baseline_duplicate_custody = store.put(baseline_meta, [baseline_payload])
        immutable_before = store.read_bytes(baseline_meta.artifact_id)
        try:
            store.put(baseline_meta, [b"tampered-source!!!!!"])
        except ArtifactCollisionError:
            immutable_collision_rejected = True
        else:
            immutable_collision_rejected = False
        immutable_after = store.read_bytes(baseline_meta.artifact_id)

        changed_meta = _artifact_metadata(
            fixture,
            cast(str, fixture["changed_release_id"]),
            changed_payload,
            prior_artifact_id=baseline_meta.artifact_id,
        )
        changed_custody = store.put(changed_meta, [changed_payload])
        current_changed = {
            "schema_version": "hdp.staging-fixture-pointer.v1",
            "record_type": "staging_current_pointer",
            "generation": 2,
            "source_id": source_id,
            "release_id": changed_meta.release_id,
            "artifact_id": changed_meta.artifact_id,
            "content_sha256": changed_meta.content_sha256,
            "updated_at": "2026-08-29T00:00:02Z",
        }
        _write_json(root, current_path, current_changed)
        current_before_quarantine = current_path.read_bytes()
        quarantine_item = queue.mark_poison(
            quarantine_lease.work_id,
            "fixture validation rejected deterministic quarantine payload",
            owner=quarantine_lease.owner,
            lease_token=quarantine_lease.token,
            now=changed_at + timedelta(seconds=1),
        )
        quarantine_without_publication = (
            quarantine_item.state == "poison" and current_path.read_bytes() == current_before_quarantine
        )

        projections = {
            "schema_version": "hdp.staging-fixture-projection.v1",
            "record_type": "staging_fixture_projection",
            "current": {"artifact_id": changed_meta.artifact_id, "release_id": changed_meta.release_id},
            "as_of": {
                baseline_meta.release_id: {"artifact_id": baseline_meta.artifact_id},
                changed_meta.release_id: {"artifact_id": changed_meta.artifact_id},
            },
            "missingness": {"source:cms:pdc": "not_yet_researched"},
        }
        _write_json(root, projections_path, projections)

        current_rollback = {
            "schema_version": "hdp.staging-fixture-pointer.v1",
            "record_type": "staging_current_pointer",
            "generation": 3,
            "source_id": source_id,
            "release_id": baseline_meta.release_id,
            "artifact_id": baseline_meta.artifact_id,
            "content_sha256": baseline_meta.content_sha256,
            "updated_at": "2026-08-29T00:00:03Z",
            "rollback": "pointer-only",
        }
        _write_json(root, current_path, current_rollback)
        pointer_rollback_ok = (
            _mapping(json.loads(current_path.read_text(encoding="utf-8")), "current pointer")["artifact_id"]
            == baseline_meta.artifact_id
            and store.read_bytes(changed_meta.artifact_id) == changed_payload
            and store.read_bytes(baseline_meta.artifact_id) == baseline_payload
        )
        prior_current_preserved = (
            immutable_before == immutable_after and store.read_bytes(changed_meta.artifact_id) == changed_payload
        )
        queue_state = {
            "baseline": queue.get("work:fixture:baseline").state,
            "changed": queue.get("work:fixture:changed").state,
            "lease_recovery": queue.get("work:fixture:lease-recovery").state,
            "quarantine": queue.get("work:fixture:quarantine").state,
        }
    finally:
        queue.close()
        connection.close()

    if network_attempts:
        raise FixtureError(f"unexpected network events: {network_attempts}")
    checks = {
        "catalog_and_bundle_schema": _check(
            bool(enabled) and len(initial_intents) == len(enabled), "catalog and bundle schema"
        ),
        "restart": _check(scheduler_restart_ok and restart_ok, "scheduler and queue restart"),
        "no_op_poll": _check(no_op_state.state == "no_op", "no-op poll"),
        "changed_input": _check(changed_state.state == "succeeded" and changed_ack_ok, "changed input admission"),
        "durable_ack_before_checkpoint": _check(ack_before_checkpoint, "acknowledgement before checkpoint"),
        "duplicate_replay_idempotency": _check(
            duplicate_replay_ok and baseline_duplicate_custody.state == "duplicate", "duplicate and replay idempotency"
        ),
        "lease_recovery": _check(lease_recovery_ok, "lease expiry and worker recovery"),
        "quarantine_without_partial_publication": _check(
            quarantine_without_publication, "quarantine without partial publication"
        ),
        "immutable_raw_custody": _check(
            baseline_custody.state == "stored" and immutable_collision_rejected and immutable_before == immutable_after,
            "immutable raw custody",
        ),
        "current_and_as_of_projection": _check(
            projections_path.is_file() and baseline_meta.artifact_id in projections_path.read_text(encoding="utf-8"),
            "current and as-of projections",
        ),
        "pointer_only_rollback": _check(pointer_rollback_ok and prior_current_preserved, "pointer-only rollback"),
        "lineage_and_missingness": _check(
            source_id == "source:ahrq:lighthouse"
            and changed_meta.prior_artifact_id == baseline_meta.artifact_id
            and _mapping(projections, "projections")["missingness"] == {"source:cms:pdc": "not_yet_researched"},
            "source, artifact, run, observation, receipt lineage and missingness",
        ),
        "three_complete_runs": _check(
            baseline_complete.state == "completed"
            and changed_complete.state == "completed"
            and recovered_complete.state == "completed",
            "three complete ingestion/replay runs",
        ),
        "bounded_network_and_ports": _check(not network_attempts, "zero listeners and denied egress"),
    }
    receipt: dict[str, object] = {
        "schema_version": "hdp.staging-fixture-receipt.v1",
        "record_type": "staging_fixture_run_receipt",
        "fixture_version": FIXTURE_VERSION,
        "candidate_sha": candidate_sha,
        "base_sha": CANDIDATE_BASE_SHA,
        "entrypoint": ENTRYPOINT,
        "run_at": RUN_AT_TEXT,
        "input": {
            "path": INPUT_PATH.relative_to(REPO_ROOT).as_posix(),
            "sha256": _sha256_file(INPUT_PATH),
            "schema_path": INPUT_SCHEMA_PATH.relative_to(REPO_ROOT).as_posix(),
            "schema_sha256": _sha256_file(INPUT_SCHEMA_PATH),
        },
        "bundle": {
            "path": BUNDLE_PATH.relative_to(REPO_ROOT).as_posix(),
            "sha256": _sha256_file(BUNDLE_PATH),
            "release_id": _mapping(_mapping(bundle["metadata"], "bundle.metadata"), "metadata")["release_id"],
        },
        "runs": 3,
        "bounds": bounds | {"lease_ttl_seconds": lease["ttl_seconds"], "max_attempts": lease["max_attempts"]},
        "paths": {
            "control_store": _relative(root, queue_path),
            "scheduler_checkpoint": _relative(root, scheduler_path),
            "acknowledgement_checkpoint": _relative(root, acknowledgement_path),
            "current_pointer": _relative(root, current_path),
            "projections": _relative(root, projections_path),
            "raw_custody": _relative(root, raw_root),
        },
        "network": {
            "egress": "deny",
            "source_probes": "disabled",
            "listener_ports": [],
            "binds": [],
            "audit_events": network_attempts,
        },
        "queue": {
            "policy": policy.as_dict(),
            "states": queue_state,
            "baseline_enqueue": baseline_work.as_dict(),
            "baseline_duplicate": baseline_duplicate.as_dict(),
            "recovery": [item.as_dict() for item in recovery_receipts],
        },
        "custody": {
            "baseline": baseline_custody.as_dict(),
            "baseline_duplicate": baseline_duplicate_custody.as_dict(),
            "changed": changed_custody.as_dict(),
            "baseline_metadata_sha256": _sha256_bytes(baseline_meta.canonical_bytes()),
            "changed_metadata_sha256": _sha256_bytes(changed_meta.canonical_bytes()),
            "prior_current_preserved": prior_current_preserved,
        },
        "scheduler": {
            "initial_intents": [intent.as_dict() for intent in initial_intents],
            "no_op_state": no_op_state.as_dict(),
            "changed_state": changed_state.as_dict(),
        },
        "checks": checks,
        "exit_codes": {"success": 0, "contract_failure": 4, "argument_error": 2},
        "prohibitions": {
            "listeners_started": False,
            "source_egress": False,
            "credentials_resolved": False,
            "migrations_applied": False,
            "production_state_written": False,
        },
        "receipt_path": "control/staging-fixture-receipt.json",
    }
    receipt["receipt_sha256"] = _receipt_digest(receipt)
    receipt_path = control_root / "staging-fixture-receipt.json"
    _write_json(root, receipt_path, receipt)
    return receipt


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-sha",
        default=os.environ.get("HDP_CANDIDATE_SHA", CANDIDATE_BASE_SHA),
        help="exact reviewed candidate SHA to bind into the receipt",
    )
    parser.add_argument("--root", required=True, help="empty absolute caller-owned fixture root")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = _arguments(argv)
        receipt = run(args.candidate_sha, args.root)
    except SystemExit:
        raise
    except FixtureError as exc:
        print(f"fixture contract failure: {exc}", file=sys.stderr)
        return 4
    except (OSError, sqlite3.Error, ValueError, yaml.YAMLError) as exc:
        print(f"fixture execution failure: {exc}", file=sys.stderr)
        return 4
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
