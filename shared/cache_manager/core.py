"""Registry-backed cache contracts, manifests, validation, and local-safe refreshes."""

from __future__ import annotations

import csv
import hashlib
import ipaddress
import json
import os
import re
import shutil
import socket
import tempfile
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlsplit, urlunsplit

import httpx

from shared.utils.cache import write_atomic_json
from shared.utils.server_registry import SERVER_REGISTRY
from shared.utils.workflows import WORKFLOW_DEFINITIONS, WORKFLOW_SOURCE_ALIASES

DEFAULT_CACHE_ROOT = Path.home() / ".healthcare-data-mcp" / "cache"
ALLOWED_READINESS_STATES = {
    "ready",
    "missing",
    "stale",
    "corrupt",
    "partial",
    "state_limited",
    "manual_import_required",
    "licensed_import_required",
    "unsupported",
    "env_required",
    "pattern",
}
READY_VALIDATION_STATUSES = {"pass", "warn"}
PUBLIC_SCHEMES = {"http", "https"}
PRIVATE_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}
DEFAULT_MAX_BYTES = 250_000_000


@dataclass(frozen=True, slots=True)
class CacheDatasetSpec:
    dataset_id: str
    title: str
    source_system: str
    source_authority: str
    landing_page: str
    source_urls: tuple[str, ...]
    acquisition_mode: str
    owning_servers: tuple[str, ...]
    owning_tools: tuple[str, ...]
    workflow_roles: tuple[str, ...]
    source_period_semantics: str
    ttl_days: int | None
    expected_artifacts: tuple[str, ...]
    required_env: tuple[str, ...] = ()
    optional_env: tuple[str, ...] = ()
    expected_grain: str = ""
    primary_keys: tuple[str, ...] = ()
    join_keys: tuple[str, ...] = ()
    required_columns: tuple[str, ...] = ()
    recommended_indexes: tuple[str, ...] = ()
    column_aliases: dict[str, tuple[str, ...]] = field(default_factory=dict)
    exact_measure_artifacts: tuple[str, ...] = ()
    min_row_count: int = 1
    expected_state_coverage: str = "national_or_source_declared"
    validation_profile: str = "tabular_public_source"
    source_caveat: str = ""
    missing_data_policy: str = "Missing or unavailable cache data is an unknown, not a negative factual claim."
    report_eligibility_rules: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CacheValidationResult:
    status: str
    defects: tuple[dict[str, Any], ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)
    report_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CacheArtifactManifest:
    dataset_id: str
    artifact_id: str
    run_id: str
    artifact_role: str
    path: str
    source_url: str = ""
    landing_page: str = ""
    retrieved_at: str = ""
    source_modified: str = ""
    etag: str = ""
    last_modified: str = ""
    checksum_sha256: str = ""
    content_length: int | None = None
    row_count: int | None = None
    schema_fingerprint: str = ""
    source_period: str = ""
    cache_status: str = "missing"
    validation_status: str = "fail"
    validator_version: str = "cache-manager-v1"
    loader_version: str = "cache-manager-v1"
    promoted_at: str = ""
    previous_artifact_id: str = ""
    caveat: str = ""
    next_step: str = ""
    artifacts: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CacheRun:
    run_id: str
    dataset_id: str
    requested_by: str
    request_source: str
    started_at: str
    completed_at: str = ""
    status: str = "running"
    phase: str = "planned"
    dry_run: bool = True
    force: bool = False
    input_manifest: str = ""
    output_manifests: tuple[str, ...] = ()
    audit_event_ids: tuple[str, ...] = ()
    error: str = ""
    recovery_hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def dataset_specs() -> dict[str, CacheDatasetSpec]:
    from servers.discovery.server import DATASET_CATALOG

    env_by_server = {
        spec.server_id: (
            tuple(key.name for key in spec.required_env),
            tuple(key.name for key in spec.optional_env),
        )
        for spec in SERVER_REGISTRY
    }
    specs: dict[str, CacheDatasetSpec] = {}
    workflow_roles_by_dataset = _workflow_roles_by_dataset()
    for dataset_id, dataset in sorted(DATASET_CATALOG.items()):
        servers = tuple(str(server) for server in dataset.get("server", ()))
        required_env: list[str] = []
        optional_env: list[str] = []
        for server_id in servers:
            server_required, server_optional = env_by_server.get(server_id, ((), ()))
            required_env.extend(server_required)
            optional_env.extend(server_optional)
        schema = dataset.get("schema") if isinstance(dataset.get("schema"), dict) else {}
        source_urls = tuple(str(url) for url in dataset.get("source_urls", ()) if str(url).strip())
        landing_page = next((url for url in source_urls if "/resource/" not in url and "download" not in url), "")
        specs[dataset_id] = CacheDatasetSpec(
            dataset_id=dataset_id,
            title=str(dataset.get("title") or dataset_id),
            source_system=str(dataset.get("source_system") or "public source"),
            source_authority=_source_authority(str(dataset.get("source_system") or "")),
            landing_page=landing_page or (source_urls[0] if source_urls else ""),
            source_urls=source_urls,
            acquisition_mode=_acquisition_mode(dataset_id, dataset),
            owning_servers=servers,
            owning_tools=tuple(str(tool) for tool in dataset.get("source_status_tool", "").split(",") if tool),
            workflow_roles=tuple(sorted(workflow_roles_by_dataset.get(dataset_id, set()))),
            source_period_semantics=_source_period_semantics(dataset_id, dataset),
            ttl_days=None if any(_is_pattern(path) for path in dataset.get("cache_files", ())) else int(dataset.get("cache_ttl_days", 90)),
            expected_artifacts=tuple(str(path) for path in dataset.get("cache_files", ())),
            required_env=tuple(dict.fromkeys(required_env)),
            optional_env=tuple(dict.fromkeys(optional_env)),
            expected_grain=str(dataset.get("grain") or ""),
            primary_keys=tuple(str(field) for field in schema.get("identity_fields", ())),
            join_keys=tuple(str(field) for field in schema.get("join_keys", ())),
            required_columns=tuple(str(field) for field in schema.get("required_fields", ())),
            recommended_indexes=tuple(str(field) for field in schema.get("join_keys", ())),
            column_aliases={
                str(canonical): tuple(str(alias) for alias in aliases)
                for canonical, aliases in schema.get("column_aliases", {}).items()
                if isinstance(aliases, (list, tuple))
            },
            exact_measure_artifacts=tuple(str(path) for path in schema.get("exact_measure_artifacts", ())),
            min_row_count=int(dataset.get("min_row_count", 1) or 1),
            expected_state_coverage=_expected_coverage(dataset_id, dataset),
            validation_profile=_validation_profile(dataset_id, dataset),
            source_caveat=_source_caveat(dataset_id, dataset),
            report_eligibility_rules=_report_rules(dataset_id, dataset),
        )
    return specs


def get_dataset_spec(dataset_id: str) -> CacheDatasetSpec:
    specs = dataset_specs()
    try:
        return specs[dataset_id]
    except KeyError as exc:
        raise ValueError(f"Unknown dataset_id: {dataset_id}") from exc


def list_cache_sources(
    *,
    cache_root: str | Path | None = None,
    server: str | None = None,
    workflow: str | None = None,
    status: str | None = None,
    source_system: str | None = None,
    acquisition_mode: str | None = None,
) -> dict[str, Any]:
    report = cache_status_payload(cache_root)
    rows = report.get("datasets", report["entries"])
    if server:
        rows = [row for row in rows if server in row.get("owning_servers", [])]
    if workflow:
        wanted = set(_workflow_dataset_ids(workflow))
        rows = [row for row in rows if row.get("dataset_id") in wanted]
    if status:
        rows = [row for row in rows if row.get("status") == status or row.get("readiness_status") == status]
    if source_system:
        needle = source_system.casefold()
        rows = [row for row in rows if needle in str(row.get("source_system", "")).casefold()]
    if acquisition_mode:
        rows = [row for row in rows if row.get("acquisition_mode") == acquisition_mode]
    return {
        "cache_root": report["cache_root"],
        "checked_at": report["checked_at"],
        "count": len(rows),
        "sources": rows,
        "summary": _count_statuses(rows),
    }


def cache_status_payload(cache_root: str | Path | None = None) -> dict[str, Any]:
    root = _cache_root(cache_root)
    checked_at = _now()
    datasets = [_dataset_status(spec, root, checked_at) for spec in dataset_specs().values()]
    entries = _artifact_entries(datasets)
    return {
        "cache_root": str(root),
        "checked_at": checked_at,
        "summary": _count_statuses(entries),
        "entries": entries,
        "datasets": datasets,
        "readiness_model": "manifest_backed_source_readiness",
        "allowed_states": sorted(ALLOWED_READINESS_STATES),
    }


def inspect_cache_source(dataset_id: str, *, cache_root: str | Path | None = None) -> dict[str, Any]:
    spec = get_dataset_spec(dataset_id)
    status = _dataset_status(spec, _cache_root(cache_root), _now())
    return {
        "spec": spec.to_dict(),
        "status": status,
        "current_manifest": get_cache_manifest(dataset_id, cache_root=cache_root),
        "lineage": get_cache_lineage(dataset_id, cache_root=cache_root),
    }


def plan_cache_refresh(
    dataset_ids: list[str] | tuple[str, ...] | None = None,
    *,
    workflow_id: str | None = None,
    cache_root: str | Path | None = None,
    force: bool = False,
    max_bytes: int = DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    root = _cache_root(cache_root)
    ids = list(dataset_ids or ())
    if workflow_id:
        ids.extend(_workflow_dataset_ids(workflow_id))
    if not ids:
        ids = list(dataset_specs())
    ordered = []
    blockers = []
    for dataset_id in dict.fromkeys(ids):
        spec = get_dataset_spec(dataset_id)
        status = _dataset_status(spec, root, _now())
        action = _next_action(spec, status, force=force)
        if status["readiness_status"] not in {"ready"}:
            blockers.append({"dataset_id": dataset_id, "status": status["readiness_status"], "next_action": action})
        ordered.append(
            {
                "dataset_id": dataset_id,
                "title": spec.title,
                "status": status["readiness_status"],
                "validation_status": status.get("validation_status"),
                "acquisition_mode": spec.acquisition_mode,
                "required_env": list(spec.required_env),
                "missing_env": [name for name in spec.required_env if not os.environ.get(name)],
                "source_urls": list(spec.source_urls[:3]),
                "expected_artifacts": list(spec.expected_artifacts),
                "max_bytes": max_bytes,
                "next_action": action,
                "validation_gates": list(spec.report_eligibility_rules),
                "source_caveat": spec.source_caveat,
            }
        )
    return {
        "dry_run": True,
        "cache_root": str(root),
        "workflow_id": workflow_id or "",
        "ordered_plan": ordered,
        "blockers": blockers,
        "next_actions": [item["next_action"] for item in ordered if item["next_action"]],
    }


def start_cache_refresh(
    dataset_ids: list[str] | tuple[str, ...],
    *,
    cache_root: str | Path | None = None,
    dry_run: bool = True,
    force: bool = False,
    max_bytes: int = DEFAULT_MAX_BYTES,
    allow_stale_fallback: bool = False,
    requested_by: str = "local-agent",
    request_source: str = "MCP stdio",
) -> dict[str, Any]:
    if not dataset_ids:
        raise ValueError("dataset_ids must contain at least one registered dataset_id")
    if len(dataset_ids) > 5:
        raise ValueError("cache refresh is bounded to at most 5 explicit datasets per request")
    root = _cache_root(cache_root)
    run_ids = []
    results = []
    for dataset_id in dict.fromkeys(dataset_ids):
        spec = get_dataset_spec(dataset_id)
        _validate_max_bytes(max_bytes)
        run_id = _new_run_id(dataset_id)
        run = CacheRun(
            run_id=run_id,
            dataset_id=dataset_id,
            requested_by=requested_by,
            request_source=request_source,
            started_at=_now(),
            dry_run=dry_run,
            force=force,
            phase="dry_run" if dry_run else "acquire",
        )
        if dry_run:
            completed = _replace_run(run, status="planned", completed_at=_now(), recovery_hint="Call start_cache_refresh with dry_run=false from a trusted local deployment to acquire.")
            _write_run(root, completed)
            run_ids.append(run_id)
            results.append({"dataset_id": dataset_id, "run": completed.to_dict(), "plan": plan_cache_refresh([dataset_id], cache_root=root, force=force, max_bytes=max_bytes)})
            continue
        result = _refresh_one(spec, root, run, max_bytes=max_bytes, allow_stale_fallback=allow_stale_fallback)
        run_ids.append(run_id)
        results.append(result)
    return {"ok": True, "dry_run": dry_run, "run_ids": run_ids, "results": results}


def validate_cache_source(
    dataset_id: str,
    *,
    cache_root: str | Path | None = None,
    staged_path: str | Path | None = None,
    relative_path: str = "",
) -> dict[str, Any]:
    spec = get_dataset_spec(dataset_id)
    root = _cache_root(cache_root)
    if staged_path is not None:
        path = _confined_path(root, staged_path)
        validation_relative_path = _resolve_relative_path(spec, relative_path, require_for_multi=False)
        validation = _validate_artifact(spec, path, relative_path=validation_relative_path)
        return {"dataset_id": dataset_id, "path": str(path), "validation": validation.to_dict()}
    status = _dataset_status(spec, root, _now())
    return {"dataset_id": dataset_id, "status": status, "validation": status.get("validation", {})}


def promote_cache_artifact(
    dataset_id: str,
    staged_path: str | Path,
    *,
    cache_root: str | Path | None = None,
    run_id: str | None = None,
    source_url: str = "",
    relative_path: str = "",
) -> dict[str, Any]:
    spec = get_dataset_spec(dataset_id)
    root = _cache_root(cache_root)
    staged = _confined_path(root, staged_path)
    if source_url:
        _validate_public_url(source_url, allowed_urls=spec.source_urls)
    resolved_relative_path = _resolve_relative_path(spec, relative_path, require_for_multi=True)
    _validate_artifact_source_url(spec, resolved_relative_path, source_url)
    validation = _validate_artifact(spec, staged, relative_path=resolved_relative_path)
    if validation.status == "fail":
        raise ValueError("Cannot promote artifact that failed validation")
    run = run_id or _new_run_id(dataset_id)
    return _promote(
        spec,
        root,
        staged,
        run,
        source_url=source_url,
        validation=validation,
        relative_path=resolved_relative_path,
    ).to_dict()


def quarantine_cache_artifact(dataset_id: str, *, cache_root: str | Path | None = None, reason: str = "operator_requested") -> dict[str, Any]:
    spec = get_dataset_spec(dataset_id)
    root = _cache_root(cache_root)
    manifest = _current_manifest(spec.dataset_id, root)
    if manifest is None:
        return {"dataset_id": dataset_id, "status": "missing", "message": "No current artifact manifest to quarantine."}
    quarantine_dir = root / "quarantine" / dataset_id
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    moved = []
    moved_artifacts = []
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for artifact in _manifest_artifacts(spec, root, manifest):
        moved_artifact = dict(artifact)
        path = _confined_path(root, str(artifact.get("path") or ""))
        target = _quarantine_target(root, quarantine_dir, path, timestamp=timestamp, label="artifact")
        if path.exists():
            shutil.move(str(path), target)
            moved.append(str(target))
            moved_artifact["path"] = str(target)
        compatibility = _compatibility_copy_path(root, str(artifact.get("relative_path") or ""))
        if compatibility is not None and compatibility.resolve(strict=False) != path.resolve(strict=False):
            compatibility_target = _quarantine_target(
                root,
                quarantine_dir,
                compatibility,
                timestamp=timestamp,
                label="published",
            )
            if compatibility.exists():
                shutil.move(str(compatibility), compatibility_target)
                moved.append(str(compatibility_target))
        moved_artifacts.append(moved_artifact)
    quarantined_path = str(moved_artifacts[0]["path"]) if moved_artifacts else manifest.path
    quarantined = CacheArtifactManifest(
        **{
            **manifest.to_dict(),
            "path": quarantined_path,
            "artifacts": tuple(moved_artifacts),
            "cache_status": "corrupt",
            "validation_status": "fail",
            "next_step": "Run start_cache_refresh for this dataset or rollback_cache_artifact if a previous artifact exists.",
        }
    )
    _write_artifact_manifest(root, quarantined)
    write_atomic_json(_dataset_manifest_path(root, dataset_id), quarantined.to_dict())
    _audit(root, "quarantine", dataset_id, {"reason": reason, "artifact_id": manifest.artifact_id})
    return {"dataset_id": dataset_id, "status": "quarantined", "artifact_id": manifest.artifact_id, "paths": moved}


def rollback_cache_artifact(dataset_id: str, *, cache_root: str | Path | None = None) -> dict[str, Any]:
    spec = get_dataset_spec(dataset_id)
    root = _cache_root(cache_root)
    current = _current_manifest(dataset_id, root)
    if current is None or not current.previous_artifact_id:
        return {"dataset_id": dataset_id, "status": "rollback_unavailable", "recovery_hint": "No previous promoted artifact is recorded."}
    previous = _artifact_manifest(root, current.previous_artifact_id)
    if previous is None:
        return {"dataset_id": dataset_id, "status": "rollback_unavailable", "recovery_hint": "Previous artifact manifest is missing."}
    restored_artifacts = []
    for artifact in _manifest_artifacts(spec, root, previous):
        previous_path = _confined_path(root, str(artifact.get("path") or ""))
        promoted = _promoted_path(root, spec, str(artifact.get("relative_path") or Path(previous_path).name), previous_path)
        promoted.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(previous_path, promoted)
        restored_artifacts.append({**artifact, "path": str(promoted), "promoted_at": _now()})
    restored_path = str(restored_artifacts[0]["path"]) if restored_artifacts else previous.path
    restored = CacheArtifactManifest(**{**previous.to_dict(), "path": restored_path, "artifacts": tuple(restored_artifacts), "promoted_at": _now(), "previous_artifact_id": current.artifact_id, "cache_status": "ready"})
    _write_artifact_manifest(root, restored)
    write_atomic_json(_dataset_manifest_path(root, dataset_id), restored.to_dict())
    _write_compatibility_copies(root, restored_artifacts)
    _audit(root, "rollback", dataset_id, {"from": current.artifact_id, "to": previous.artifact_id})
    return {"dataset_id": dataset_id, "status": "rolled_back", "artifact_id": previous.artifact_id, "paths": [str(item["path"]) for item in restored_artifacts]}


def get_cache_manifest(dataset_id: str, *, cache_root: str | Path | None = None) -> dict[str, Any]:
    get_dataset_spec(dataset_id)
    manifest = _current_manifest(dataset_id, _cache_root(cache_root))
    return manifest.to_dict() if manifest else {"dataset_id": dataset_id, "status": "missing_manifest"}


def get_cache_lineage(dataset_id: str, *, cache_root: str | Path | None = None) -> dict[str, Any]:
    get_dataset_spec(dataset_id)
    root = _cache_root(cache_root)
    current = _current_manifest(dataset_id, root)
    runs = []
    for path in sorted((root / "manifests" / "runs").glob(f"{dataset_id}-*.json"))[-10:]:
        runs.append(_read_json(path))
    return {
        "dataset_id": dataset_id,
        "current_artifact_id": current.artifact_id if current else "",
        "current_manifest": current.to_dict() if current else None,
        "recent_runs": runs,
        "downstream_workflows": sorted(_workflow_roles_by_dataset().get(dataset_id, set())),
    }


def get_cache_job(run_id: str, *, cache_root: str | Path | None = None) -> dict[str, Any]:
    root = _cache_root(cache_root)
    matches = list((root / "manifests" / "runs").glob(f"*{run_id}*.json"))
    if not matches:
        return {"run_id": run_id, "status": "not_found"}
    return _read_json(matches[0])


def _refresh_one(spec: CacheDatasetSpec, root: Path, run: CacheRun, *, max_bytes: int, allow_stale_fallback: bool) -> dict[str, Any]:
    missing_env = [name for name in spec.required_env if not os.environ.get(name)]
    if missing_env:
        completed = _replace_run(run, status="blocked", phase="env_required", completed_at=_now(), error="Missing required env: " + ", ".join(missing_env), recovery_hint="Configure required environment variables and retry.")
        _write_run(root, completed)
        return {"dataset_id": spec.dataset_id, "run": completed.to_dict()}
    if spec.acquisition_mode in {"manual_import", "licensed_import", "unsupported"}:
        completed = _replace_run(run, status="blocked", phase=spec.acquisition_mode, completed_at=_now(), error=f"{spec.acquisition_mode} cannot be downloaded by cache-manager.", recovery_hint=_manual_recovery(spec))
        _write_run(root, completed)
        return {"dataset_id": spec.dataset_id, "run": completed.to_dict()}
    source_url = _select_source_url(spec)
    _validate_public_url(source_url, allowed_urls=spec.source_urls)
    staging = root / "bronze" / spec.dataset_id / run.run_id
    staging.mkdir(parents=True, exist_ok=True)
    required_artifacts = _required_artifacts(spec)
    if not required_artifacts:
        required_artifacts = (f"source{Path(urlparse(source_url).path).suffix or '.data'}",)
    try:
        staged_artifacts: list[tuple[str, Path, str, CacheValidationResult]] = []
        defects: list[dict[str, Any]] = []
        for index, relative_path in enumerate(required_artifacts):
            artifact_source_url = _source_url_for_artifact(spec, index)
            suffix = Path(relative_path).suffix or Path(urlparse(artifact_source_url).path).suffix or ".data"
            staged = staging / f"source-{index}{suffix}"
            _download(artifact_source_url, staged, max_bytes=max_bytes, allowed_urls=spec.source_urls)
            validation = _validate_artifact(spec, staged, relative_path=relative_path)
            if validation.status == "fail":
                defects.append({"relative_path": relative_path, "validation": validation.to_dict()})
            staged_artifacts.append((relative_path, staged, artifact_source_url, validation))
        if defects:
            completed = _replace_run(run, status="failed", phase="validate", completed_at=_now(), error=json.dumps(defects), recovery_hint="Inspect validation defects; current promoted cache was not changed.")
            _write_run(root, completed)
            return {"dataset_id": spec.dataset_id, "run": completed.to_dict(), "validation": {"status": "fail", "defects": defects}}
        manifest = _promote_many(spec, root, staged_artifacts, run.run_id)
        completed = _replace_run(run, status="completed", phase="promoted", completed_at=_now(), output_manifests=(manifest.artifact_id,))
        _write_run(root, completed)
        return {"dataset_id": spec.dataset_id, "run": completed.to_dict(), "manifest": manifest.to_dict(), "validation": {"status": manifest.validation_status}}
    except Exception as exc:
        hint = "Current promoted artifact was left unchanged."
        if allow_stale_fallback:
            hint += " allow_stale_fallback was requested; downstream tools may use previous ready artifacts with stale caveats."
        completed = _replace_run(run, status="failed", phase="acquire", completed_at=_now(), error=f"{type(exc).__name__}: {exc}", recovery_hint=hint)
        _write_run(root, completed)
        return {"dataset_id": spec.dataset_id, "run": completed.to_dict()}


def _dataset_status(spec: CacheDatasetSpec, root: Path, now_iso: str) -> dict[str, Any]:
    manifest = _current_manifest(spec.dataset_id, root)
    missing_env = [name for name in spec.required_env if not os.environ.get(name)]
    if spec.acquisition_mode == "manual_import" and spec.dataset_id == "state_health_data":
        base_status = "manual_import_required"
    elif spec.expected_state_coverage == "state_limited":
        base_status = "state_limited"
    elif spec.acquisition_mode == "manual_import":
        base_status = "manual_import_required"
    elif spec.acquisition_mode == "licensed_import":
        base_status = "licensed_import_required"
    elif spec.acquisition_mode == "unsupported":
        base_status = "unsupported"
    elif missing_env:
        base_status = "env_required"
    elif any(_is_pattern(path) for path in spec.expected_artifacts):
        base_status = "pattern"
    else:
        base_status = "missing"
    artifacts = [_artifact_status(root, path, spec.ttl_days) for path in spec.expected_artifacts]
    integrity_defects: list[dict[str, Any]] = []
    if manifest:
        manifest_artifacts = _manifest_artifacts(spec, root, manifest)
        status, validation_status, report_eligible, integrity_defects = _manifest_readiness(
            spec,
            root,
            manifest,
            manifest_artifacts,
        )
        artifacts = _merge_artifact_statuses(artifacts, manifest_artifacts)
        source_period = manifest.source_period
    elif any(item["status"] == "ready" for item in artifacts):
        status = "partial"
        validation_status = "missing_manifest"
        report_eligible = False
        source_period = ""
    else:
        status = base_status
        validation_status = "not_validated"
        report_eligible = False
        source_period = ""
    return {
        "dataset_id": spec.dataset_id,
        "title": spec.title,
        "status": status,
        "readiness_status": status,
        "validation_status": validation_status,
        "source_period": source_period,
        "report_eligible": report_eligible,
        "source_system": spec.source_system,
        "source_authority": spec.source_authority,
        "acquisition_mode": spec.acquisition_mode,
        "owning_servers": list(spec.owning_servers),
        "workflow_roles": list(spec.workflow_roles),
        "ttl_days": spec.ttl_days,
        "checked_at": now_iso,
        "expected_artifacts": list(spec.expected_artifacts),
        "artifacts": artifacts,
        "manifest": manifest.to_dict() if manifest else None,
        "source_caveat": spec.source_caveat,
        "next_action": _next_action(spec, {"readiness_status": status, "missing_env": missing_env}),
        "missing_env": missing_env,
        "integrity_defects": integrity_defects if manifest else [],
    }


def _artifact_status(root: Path, relative_path: str, ttl_days: int | None) -> dict[str, Any]:
    if _is_pattern(relative_path):
        return {"relative_path": relative_path, "status": "pattern"}
    path = _confined_path(root, root / relative_path)
    if not path.exists():
        return {"relative_path": relative_path, "path": str(path), "status": "missing"}
    stat = path.stat()
    return {
        "relative_path": relative_path,
        "path": str(path),
        "status": "stale" if _is_stale(path, ttl_days) else "ready",
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


def _manifest_artifacts(spec: CacheDatasetSpec, root: Path, manifest: CacheArtifactManifest) -> list[dict[str, Any]]:
    if manifest.artifacts:
        return [dict(item) for item in manifest.artifacts if isinstance(item, dict)]
    if not manifest.path:
        return []
    relative_path = _required_artifacts(spec)[0] if _required_artifacts(spec) else Path(manifest.path).name
    return [
        {
            "relative_path": relative_path,
            "path": manifest.path,
            "source_url": manifest.source_url,
            "checksum_sha256": manifest.checksum_sha256,
            "content_length": manifest.content_length,
            "row_count": manifest.row_count,
            "schema_fingerprint": manifest.schema_fingerprint,
            "validation_status": manifest.validation_status,
            "report_eligible": manifest.validation_status in READY_VALIDATION_STATUSES,
            "promoted_at": manifest.promoted_at,
        }
    ]


def _manifest_readiness(
    spec: CacheDatasetSpec,
    root: Path,
    manifest: CacheArtifactManifest,
    manifest_artifacts: list[dict[str, Any]],
) -> tuple[str, str, bool, list[dict[str, Any]]]:
    required = _required_artifacts(spec)
    by_relative = {str(item.get("relative_path")): item for item in manifest_artifacts}
    if not required:
        required = tuple(by_relative)
    present = 0
    stale = False
    corrupt = False
    all_report_eligible = True
    validation_statuses: set[str] = set()
    integrity_defects: list[dict[str, Any]] = []
    for relative_path in required:
        item = by_relative.get(relative_path)
        if not item:
            continue
        try:
            path = _confined_path(root, str(item.get("path") or ""))
        except ValueError:
            corrupt = True
            validation_statuses.add("fail")
            integrity_defects.append(
                {
                    "relative_path": relative_path,
                    "field": "path",
                    "expected": "cache-root confined manifest path",
                    "observed": str(item.get("path") or ""),
                    "recovery_hint": "Quarantine this manifest, then refresh or re-import from the registered source.",
                }
            )
            continue
        if not path.exists():
            corrupt = True
            validation_statuses.add("fail")
            integrity_defects.append(
                {
                    "relative_path": relative_path,
                    "field": "path",
                    "expected": "existing promoted artifact",
                    "observed": "missing",
                    "recovery_hint": "Rollback this artifact if available, then refresh or re-import from the registered source.",
                }
            )
            continue
        present += 1
        content_length = path.stat().st_size
        expected_length = item.get("content_length")
        if expected_length is not None and int(expected_length) != content_length:
            corrupt = True
            validation_statuses.add("fail")
            integrity_defects.append(
                {
                    "relative_path": relative_path,
                    "field": "content_length",
                    "expected": int(expected_length),
                    "observed": content_length,
                    "recovery_hint": "Quarantine or rollback this artifact, then refresh or re-import from the registered source.",
                }
            )
        expected_checksum = str(item.get("checksum_sha256") or "")
        observed_checksum = _sha256(path)
        if expected_checksum and expected_checksum != observed_checksum:
            corrupt = True
            validation_statuses.add("fail")
            integrity_defects.append(
                {
                    "relative_path": relative_path,
                    "field": "checksum_sha256",
                    "expected": expected_checksum,
                    "observed": observed_checksum,
                    "recovery_hint": "Quarantine or rollback this artifact, then refresh or re-import from the registered source.",
                }
            )
        compatibility_defects = _compatibility_copy_integrity_defects(
            root,
            relative_path,
            promoted=path,
            expected_length=int(expected_length) if expected_length is not None else None,
            expected_checksum=expected_checksum,
        )
        if compatibility_defects:
            corrupt = True
            validation_statuses.add("fail")
            integrity_defects.extend(compatibility_defects)
        if _is_stale(path, spec.ttl_days):
            stale = True
        validation = _validate_artifact(spec, path, relative_path=relative_path)
        item_status = "fail" if validation.status == "fail" else str(item.get("validation_status") or manifest.validation_status)
        validation_statuses.add(item_status)
        if validation.status == "fail" or item_status not in READY_VALIDATION_STATUSES:
            corrupt = True
        all_report_eligible = all_report_eligible and validation.report_eligible and item_status in READY_VALIDATION_STATUSES
    if corrupt or manifest.cache_status == "corrupt":
        return "corrupt", "fail", False, integrity_defects
    if not required:
        return "missing", "not_validated", False, integrity_defects
    if present == 0:
        return "missing", "not_validated", False, integrity_defects
    if present < len(required):
        return "partial", _combined_validation_status(validation_statuses), False, integrity_defects
    if stale:
        return "stale", _combined_validation_status(validation_statuses), False, integrity_defects
    ready = manifest.cache_status == "ready" and all_report_eligible
    return ("ready" if ready else "corrupt"), _combined_validation_status(validation_statuses), ready, integrity_defects


def _merge_artifact_statuses(filesystem_artifacts: list[dict[str, Any]], manifest_artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_relative = {str(item.get("relative_path")): dict(item) for item in filesystem_artifacts}
    for item in manifest_artifacts:
        relative_path = str(item.get("relative_path") or "")
        if not relative_path:
            continue
        merged = {**by_relative.get(relative_path, {}), **item}
        path = Path(str(merged.get("path") or ""))
        if path.exists():
            merged["status"] = str(item.get("validation_status") or "ready")
            if merged["status"] in READY_VALIDATION_STATUSES:
                merged["status"] = "ready"
            merged["size_bytes"] = path.stat().st_size
            merged["modified_at"] = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
        by_relative[relative_path] = merged
    return list(by_relative.values())


def _combined_validation_status(statuses: set[str]) -> str:
    if not statuses:
        return "not_validated"
    if "fail" in statuses:
        return "fail"
    if "warn" in statuses:
        return "warn"
    if statuses == {"pass"}:
        return "pass"
    return sorted(statuses)[0]


def _artifact_entries(datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for dataset in datasets:
        artifacts = dataset.get("artifacts") or []
        if not artifacts:
            entries.append({**dataset, "relative_path": "", "ttl_days": dataset.get("ttl_days")})
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            if _aggregate_state_artifact_duplicate(str(dataset.get("dataset_id")), str(artifact.get("relative_path", ""))):
                continue
            artifact_status = str(artifact.get("status") or dataset.get("readiness_status") or dataset.get("status"))
            entries.append(
                {
                    **artifact,
                    "dataset_id": dataset["dataset_id"],
                    "title": dataset["title"],
                    "status": artifact_status,
                    "readiness_status": artifact_status
                    if artifact_status in {"ready", "stale", "missing", "pattern"}
                    else dataset.get("readiness_status"),
                    "dataset_readiness_status": dataset.get("readiness_status"),
                    "ttl_days": dataset.get("ttl_days"),
                    "validation_status": dataset.get("validation_status"),
                    "source_period": dataset.get("source_period"),
                    "report_eligible": dataset.get("report_eligible"),
                    "source_system": dataset.get("source_system"),
                    "source_authority": dataset.get("source_authority"),
                    "acquisition_mode": dataset.get("acquisition_mode"),
                    "owning_servers": dataset.get("owning_servers", []),
                    "workflow_roles": dataset.get("workflow_roles", []),
                    "source_caveat": dataset.get("source_caveat"),
                    "next_action": dataset.get("next_action"),
                    "missing_env": dataset.get("missing_env", []),
                }
            )
    return entries


def _aggregate_state_artifact_duplicate(dataset_id: str, relative_path: str) -> bool:
    if dataset_id != "state_health_data":
        return False
    return any(
        marker in relative_path
        for marker in (
            "state-health-data/phc4/",
            "state-health-data/pa-hospital-reports/",
            "state-health-data/pa-doh-hospital-extract/",
            "state-health-data/nj-hospital-public-data/",
            "state-health-data/de-hospital-discharge/",
        )
    )


def _validate_artifact(spec: CacheDatasetSpec, path: Path, *, relative_path: str = "") -> CacheValidationResult:
    defects: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {"path": str(path)}
    if not path.exists():
        defects.append({"severity": "error", "field": "path", "expected": "existing file", "observed": "missing", "recovery_hint": "Refresh or import the registered dataset."})
        return CacheValidationResult("fail", tuple(defects), metrics, False)
    size = path.stat().st_size
    metrics["content_length"] = size
    if size <= 0:
        defects.append({"severity": "error", "field": "content_length", "expected": ">0", "observed": size, "recovery_hint": "Source download is empty or truncated; refresh again."})
    row_count = _count_rows(path)
    if row_count is not None:
        metrics["row_count"] = row_count
        if row_count < spec.min_row_count:
            defects.append({"severity": "error", "field": "row_count", "expected": f">={spec.min_row_count}", "observed": row_count, "recovery_hint": "Verify the source URL and dataset period."})
    schema_hash = _schema_fingerprint(path)
    if schema_hash:
        metrics["schema_fingerprint"] = schema_hash
    columns = _read_columns(path)
    if columns:
        normalized_columns = _normalized_columns(spec, columns)
        metrics["raw_columns"] = columns
        metrics["columns"] = sorted(normalized_columns)
        missing_columns = _missing_required_columns(spec, normalized_columns)
        for field in missing_columns:
            defects.append(
                {
                    "severity": "error",
                    "field": "schema",
                    "expected": field,
                    "observed": "missing",
                    "recovery_hint": "Use the registered source artifact with declared schema and exact identifier fields.",
                }
            )
        missing_identifiers = _missing_required_identifier_groups(spec, normalized_columns, relative_path=relative_path)
        for label in missing_identifiers:
            defects.append(
                {
                    "severity": "error",
                    "field": "identity",
                    "expected": label,
                    "observed": "missing",
                    "recovery_hint": "Do not promote report-ready caches without exact identifier columns.",
                }
            )
    elif spec.required_columns or spec.join_keys or spec.primary_keys:
        defects.append(
            {
                "severity": "error",
                "field": "schema",
                "expected": "readable header",
                "observed": "unavailable",
                "recovery_hint": "Use CSV/TSV/text or Parquet artifacts with readable schema metadata.",
            }
        )
    status = "fail" if any(defect["severity"] == "error" for defect in defects) else "pass"
    report_eligible = status == "pass" and bool(columns or not (spec.required_columns or spec.join_keys or spec.primary_keys))
    return CacheValidationResult(status, tuple(defects), metrics, report_eligible)


def _promote(
    spec: CacheDatasetSpec,
    root: Path,
    staged: Path,
    run_id: str,
    *,
    source_url: str,
    validation: CacheValidationResult,
    relative_path: str,
) -> CacheArtifactManifest:
    previous = _current_manifest(spec.dataset_id, root)
    previous_artifacts = _manifest_artifacts(spec, root, previous) if previous else []
    artifact = _copy_promoted_artifact(spec, root, staged, relative_path, source_url, validation)
    artifacts_by_relative = {str(item.get("relative_path")): item for item in previous_artifacts}
    artifacts_by_relative[relative_path] = artifact
    artifact_id = f"{spec.dataset_id}-{uuid.uuid4().hex[:12]}"
    artifacts = tuple(artifacts_by_relative[path] for path in _ordered_artifact_keys(spec, artifacts_by_relative))
    primary_path = str(artifacts[0]["path"]) if artifacts else artifact["path"]
    _write_compatibility_copies(root, artifacts)
    cache_status, validation_status, _report_eligible, _integrity_defects = _manifest_readiness(
        spec,
        root,
        CacheArtifactManifest(
            dataset_id=spec.dataset_id,
            artifact_id=artifact_id,
            run_id=run_id,
            artifact_role="silver",
            path=primary_path,
            cache_status="ready",
            validation_status=validation.status,
            artifacts=artifacts,
        ),
        list(artifacts),
    )
    manifest = CacheArtifactManifest(
        dataset_id=spec.dataset_id,
        artifact_id=artifact_id,
        run_id=run_id,
        artifact_role="silver",
        path=primary_path,
        source_url=source_url,
        landing_page=spec.landing_page,
        retrieved_at=_now(),
        checksum_sha256=str(artifact.get("checksum_sha256") or ""),
        content_length=int(artifact.get("content_length") or 0),
        row_count=validation.metrics.get("row_count"),
        schema_fingerprint=str(validation.metrics.get("schema_fingerprint") or ""),
        source_period=_source_period_from_spec(spec),
        cache_status=cache_status,
        validation_status=validation_status,
        promoted_at=_now(),
        previous_artifact_id=previous.artifact_id if previous else "",
        caveat=spec.source_caveat,
        next_step="Use get_workflow_cache_readiness before citing report facts.",
        artifacts=artifacts,
    )
    _write_artifact_manifest(root, manifest)
    write_atomic_json(_dataset_manifest_path(root, spec.dataset_id), manifest.to_dict())
    _audit(root, "promote", spec.dataset_id, {"artifact_id": artifact_id, "run_id": run_id})
    return manifest


def _promote_many(
    spec: CacheDatasetSpec,
    root: Path,
    staged_artifacts: list[tuple[str, Path, str, CacheValidationResult]],
    run_id: str,
) -> CacheArtifactManifest:
    previous = _current_manifest(spec.dataset_id, root)
    prepared: list[tuple[Path, Path, str, str, CacheValidationResult]] = []
    for relative_path, staged, source_url, validation in staged_artifacts:
        promoted = _promoted_path(root, spec, relative_path, staged)
        promoted.parent.mkdir(parents=True, exist_ok=True)
        tmp = promoted.with_suffix(promoted.suffix + f".{uuid.uuid4().hex}.tmp")
        shutil.copy2(staged, tmp)
        prepared.append((tmp, promoted, relative_path, source_url, validation))
    artifacts: list[dict[str, Any]] = []
    try:
        for tmp, promoted, relative_path, source_url, validation in prepared:
            tmp.replace(promoted)
            artifacts.append(_artifact_manifest_entry(relative_path, promoted, source_url, validation))
    except Exception:
        for tmp, _, _, _, _ in prepared:
            tmp.unlink(missing_ok=True)
        raise
    artifact_id = f"{spec.dataset_id}-{uuid.uuid4().hex[:12]}"
    primary = artifacts[0] if artifacts else {}
    manifest = CacheArtifactManifest(
        dataset_id=spec.dataset_id,
        artifact_id=artifact_id,
        run_id=run_id,
        artifact_role="silver",
        path=str(primary.get("path") or ""),
        source_url=str(primary.get("source_url") or ""),
        landing_page=spec.landing_page,
        retrieved_at=_now(),
        checksum_sha256=str(primary.get("checksum_sha256") or ""),
        content_length=int(primary.get("content_length") or 0) if primary.get("content_length") is not None else None,
        row_count=primary.get("row_count"),
        schema_fingerprint=str(primary.get("schema_fingerprint") or ""),
        source_period=_source_period_from_spec(spec),
        cache_status="ready",
        validation_status="pass",
        promoted_at=_now(),
        previous_artifact_id=previous.artifact_id if previous else "",
        caveat=spec.source_caveat,
        next_step="Use get_workflow_cache_readiness before citing report facts.",
        artifacts=tuple(artifacts),
    )
    _write_compatibility_copies(root, artifacts)
    status, validation_status, _report_eligible, _integrity_defects = _manifest_readiness(spec, root, manifest, artifacts)
    manifest = CacheArtifactManifest(**{**manifest.to_dict(), "cache_status": status, "validation_status": validation_status})
    _write_artifact_manifest(root, manifest)
    write_atomic_json(_dataset_manifest_path(root, spec.dataset_id), manifest.to_dict())
    _audit(root, "promote", spec.dataset_id, {"artifact_id": artifact_id, "run_id": run_id})
    return manifest


def _copy_promoted_artifact(
    spec: CacheDatasetSpec,
    root: Path,
    staged: Path,
    relative_path: str,
    source_url: str,
    validation: CacheValidationResult,
) -> dict[str, Any]:
    promoted = _promoted_path(root, spec, relative_path, staged)
    promoted.parent.mkdir(parents=True, exist_ok=True)
    tmp = promoted.with_suffix(promoted.suffix + f".{uuid.uuid4().hex}.tmp")
    shutil.copy2(staged, tmp)
    tmp.replace(promoted)
    return _artifact_manifest_entry(relative_path, promoted, source_url, validation)


def _artifact_manifest_entry(
    relative_path: str,
    promoted: Path,
    source_url: str,
    validation: CacheValidationResult,
) -> dict[str, Any]:
    return {
        "relative_path": relative_path,
        "path": str(promoted),
        "source_url": source_url,
        "checksum_sha256": _sha256(promoted),
        "content_length": promoted.stat().st_size,
        "row_count": validation.metrics.get("row_count"),
        "schema_fingerprint": str(validation.metrics.get("schema_fingerprint") or ""),
        "validation_status": validation.status,
        "report_eligible": validation.report_eligible,
        "promoted_at": _now(),
    }


def _write_compatibility_copies(root: Path, artifacts: tuple[dict[str, Any], ...] | list[dict[str, Any]]) -> None:
    for artifact in artifacts:
        relative = str(artifact.get("relative_path") or "")
        if not relative or _is_pattern(relative):
            continue
        promoted = _confined_path(root, str(artifact.get("path") or ""))
        target = _compatibility_copy_path(root, relative)
        if target is None:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.resolve() != promoted.resolve():
            shutil.copy2(promoted, target)


def _compatibility_copy_path(root: Path, relative_path: str) -> Path | None:
    if not relative_path or _is_pattern(relative_path):
        return None
    return _confined_path(root, root / relative_path)


def _quarantine_target(root: Path, quarantine_dir: Path, path: Path, *, timestamp: str, label: str) -> Path:
    return _confined_path(root, quarantine_dir / f"{label}-{Path(path).name}.{timestamp}.{uuid.uuid4().hex[:8]}")


def _compatibility_copy_integrity_defects(
    root: Path,
    relative_path: str,
    *,
    promoted: Path,
    expected_length: int | None,
    expected_checksum: str,
) -> list[dict[str, Any]]:
    target = _compatibility_copy_path(root, relative_path)
    if target is None:
        return []
    if target.resolve(strict=False) == promoted.resolve(strict=False):
        return []
    defects: list[dict[str, Any]] = []
    if not target.exists():
        defects.append(
            {
                "relative_path": relative_path,
                "field": "compatibility_copy",
                "expected": str(target),
                "observed": "missing",
                "recovery_hint": "Re-promote, rollback, or refresh this artifact before downstream tools read the cache.",
            }
        )
        return defects
    if expected_length is not None and target.stat().st_size != expected_length:
        defects.append(
            {
                "relative_path": relative_path,
                "field": "compatibility_copy_content_length",
                "expected": expected_length,
                "observed": target.stat().st_size,
                "recovery_hint": "Re-promote, rollback, or refresh this artifact before downstream tools read the cache.",
            }
        )
    if expected_checksum:
        observed_checksum = _sha256(target)
        if observed_checksum != expected_checksum:
            defects.append(
                {
                    "relative_path": relative_path,
                    "field": "compatibility_copy_checksum_sha256",
                    "expected": expected_checksum,
                    "observed": observed_checksum,
                    "recovery_hint": "Re-promote, rollback, or refresh this artifact before downstream tools read the cache.",
                }
            )
    return defects


def _download(source_url: str, dest: Path, *, max_bytes: int, allowed_urls: tuple[str, ...]) -> None:
    current = source_url
    for _ in range(5):
        _validate_public_url(current, allowed_urls=allowed_urls)
        with httpx.stream("GET", current, follow_redirects=False, timeout=60.0) as response:
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if not location:
                    raise ValueError("Redirect response did not include Location")
                current = str(httpx.URL(current).join(location))
                continue
            response.raise_for_status()
            total = 0
            with tempfile.NamedTemporaryFile(dir=dest.parent, delete=False) as handle:
                tmp = Path(handle.name)
                try:
                    for chunk in response.iter_bytes():
                        total += len(chunk)
                        if total > max_bytes:
                            raise ValueError("download exceeds max_bytes")
                        handle.write(chunk)
                except Exception:
                    tmp.unlink(missing_ok=True)
                    raise
            tmp.replace(dest)
            return
    raise ValueError("Too many redirects")


def _validate_public_url(url: str, *, allowed_urls: tuple[str, ...]) -> None:
    if _normalize_url(url) not in {_normalize_url(item) for item in allowed_urls}:
        raise ValueError("URL is not an exact allowlisted source for this dataset")
    parsed = urlparse(url)
    if parsed.scheme not in PUBLIC_SCHEMES:
        raise ValueError("Only http(s) source URLs are allowed")
    host = parsed.hostname or ""
    if host.casefold() in PRIVATE_HOSTS:
        raise ValueError("Private, loopback, and link-local targets are not allowed")
    for info in socket.getaddrinfo(host, None):
        address = info[4][0]
        ip = ipaddress.ip_address(address)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
            raise ValueError("Private, loopback, link-local, multicast, and reserved network targets are not allowed")


def _normalize_url(url: str) -> str:
    split = urlsplit(url)
    scheme = split.scheme.casefold()
    hostname = (split.hostname or "").casefold()
    port = split.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None
    netloc = hostname if port is None else f"{hostname}:{port}"
    query = urlencode(sorted(parse_qsl(split.query, keep_blank_values=True)), doseq=True)
    return urlunsplit((scheme, netloc, split.path or "/", query, ""))


def _confined_path(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved_root = root.expanduser().resolve()
    resolved = candidate.expanduser().resolve(strict=False)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("Path escapes the configured cache root") from exc
    return resolved


def _cache_root(cache_root: str | Path | None) -> Path:
    root = Path(cache_root) if cache_root is not None else DEFAULT_CACHE_ROOT
    return root.expanduser().resolve(strict=False)


def _workflow_roles_by_dataset() -> dict[str, set[str]]:
    roles: dict[str, set[str]] = {}
    for workflow_id, workflow in WORKFLOW_DEFINITIONS.items():
        for dataset_id in _workflow_dataset_ids(workflow_id, workflow.required_sources):
            roles.setdefault(dataset_id, set()).add(workflow_id)
    return roles


def _workflow_dataset_ids(workflow_id: str, sources: tuple[str, ...] | None = None) -> list[str]:
    workflow = WORKFLOW_DEFINITIONS.get(workflow_id)
    raw_sources = sources if sources is not None else workflow.required_sources if workflow else ()
    dataset_ids: list[str] = []
    for source in raw_sources:
        alias = WORKFLOW_SOURCE_ALIASES.get(source)
        if alias:
            dataset_ids.extend(str(item) for item in alias.get("canonical_dataset_ids", ()))
        else:
            dataset_ids.append(source)
    return list(dict.fromkeys(dataset_ids))


def _source_authority(source_system: str) -> str:
    value = source_system.casefold()
    if "cms" in value:
        return "Centers for Medicare & Medicaid Services"
    if "ahrq" in value:
        return "Agency for Healthcare Research and Quality"
    if "cdc" in value or "places" in value:
        return "Centers for Disease Control and Prevention"
    if "socrata" in value:
        return "public Socrata open-data portal"
    return source_system or "public source authority"


def _acquisition_mode(dataset_id: str, dataset: dict[str, Any]) -> str:
    requires = " ".join(str(item).casefold() for item in dataset.get("requires_import", ()))
    if "licensed" in requires or dataset_id == "docgraph_referrals":
        return "licensed_import"
    if "manual" in requires or dataset_id in {"state_health_data", "phc4_public_reports"}:
        return "manual_import"
    if dataset_id in {"web_intelligence", "mcp_metadata_surfaces"}:
        return "unsupported"
    urls = dataset.get("source_urls", ())
    if urls:
        return "stable_download"
    return "live_api_cache"


def _expected_coverage(dataset_id: str, dataset: dict[str, Any]) -> str:
    if dataset_id.startswith(("pa_", "nj_", "de_", "phc4_")) or dataset_id in {"state_health_data"}:
        return "state_limited"
    if dataset_id in {"docgraph_referrals"}:
        return "licensed_import_scope"
    return "national_or_public_api"


def _validation_profile(dataset_id: str, dataset: dict[str, Any]) -> str:
    if _expected_coverage(dataset_id, dataset) == "state_limited":
        return "state_limited_supplement"
    if dataset.get("supports_exact_inventory"):
        return "exact_measure_inventory"
    return "tabular_public_source"


def _source_period_semantics(dataset_id: str, dataset: dict[str, Any]) -> str:
    if dataset_id.startswith("cms_"):
        return "CMS release period or file modified date; do not infer currentness beyond the source period."
    if dataset_id.startswith(("pa_", "nj_", "de_", "phc4_")):
        return "State-specific reporting year or publication period."
    return "Source-declared period, retrieved_at, or modified timestamp."


def _source_period_from_spec(spec: CacheDatasetSpec) -> str:
    return spec.source_period_semantics


def _source_caveat(dataset_id: str, dataset: dict[str, Any]) -> str:
    unsupported = dataset.get("unsupported_assertions") or []
    if unsupported:
        return "Unsupported assertions: " + "; ".join(str(item) for item in unsupported)
    if _expected_coverage(dataset_id, dataset) == "state_limited":
        return "State-limited supplement; never present as all-U.S. coverage."
    if dataset_id == "nppes_registry":
        return "NPPES is provider identity context, not proof of ownership, affiliation, or referral relationships."
    return "Preserve source period, validation status, identity keys, and missing-data caveats before citing."


def _report_rules(dataset_id: str, dataset: dict[str, Any]) -> tuple[str, ...]:
    rules = ["validation_status must be pass or warn", "source_period or retrieved_at must be present"]
    join_keys = dataset.get("schema", {}).get("join_keys", ()) if isinstance(dataset.get("schema"), dict) else ()
    if join_keys:
        rules.append("joins must use exact identifiers: " + ", ".join(str(key) for key in join_keys))
    if dataset.get("supports_exact_inventory"):
        rules.append("exact measure rows must use measure_id; adjacent summaries are not substitutes")
    return tuple(rules)


def _next_action(spec: CacheDatasetSpec, status: dict[str, Any], *, force: bool = False) -> str:
    state = str(status.get("readiness_status") or status.get("status") or "missing")
    if status.get("missing_env"):
        return "Configure required environment variables: " + ", ".join(status["missing_env"])
    if state == "ready" and not force:
        return "No refresh needed; inspect manifest and preserve evidence fields before use."
    if state == "manual_import_required":
        return _manual_recovery(spec)
    if state == "licensed_import_required":
        return "Acquire the licensed source outside cache-manager, then import/validate the scoped local artifact."
    if state == "state_limited":
        return "State-limited supplement; use only as state-specific context and do not present as all-U.S. coverage."
    if state == "unsupported":
        return "Use the owning live/read-only tool; cache-manager does not acquire this dataset."
    if state == "corrupt":
        return f"Quarantine or rollback {spec.dataset_id}, then refresh or re-import from the registered source."
    if state == "partial":
        return f"Promote or refresh all required artifacts for registered dataset_id {spec.dataset_id}; partial cache data is not report-ready."
    if state == "missing":
        return (
            f"Run plan_cache_refresh then start_cache_refresh for registered dataset_id {spec.dataset_id}. "
            "Missing cache data is an unknown, not a negative factual claim."
        )
    return f"Run plan_cache_refresh then start_cache_refresh for registered dataset_id {spec.dataset_id}."


def _manual_recovery(spec: CacheDatasetSpec) -> str:
    return "Manual import required for " + spec.dataset_id + "; place the source-approved file under the configured cache root and run validate_cache_source."


def _is_pattern(path: str) -> bool:
    return "{" in path or "*" in path


def _is_stale(path: Path, ttl_days: int | None) -> bool:
    if ttl_days is None or not path.exists():
        return False
    age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age_seconds > ttl_days * 86400


def _current_manifest(dataset_id: str, root: Path) -> CacheArtifactManifest | None:
    path = _dataset_manifest_path(root, dataset_id)
    data = _read_json(path)
    if not isinstance(data, dict):
        return None
    try:
        return CacheArtifactManifest(**data)
    except TypeError:
        return None


def _artifact_manifest(root: Path, artifact_id: str) -> CacheArtifactManifest | None:
    data = _read_json(root / "manifests" / "artifacts" / f"{artifact_id}.json")
    if not isinstance(data, dict):
        return None
    try:
        return CacheArtifactManifest(**data)
    except TypeError:
        return None


def _write_artifact_manifest(root: Path, manifest: CacheArtifactManifest) -> None:
    write_atomic_json(root / "manifests" / "artifacts" / f"{manifest.artifact_id}.json", manifest.to_dict())


def _write_run(root: Path, run: CacheRun) -> None:
    write_atomic_json(root / "manifests" / "runs" / f"{run.dataset_id}-{run.run_id}.json", run.to_dict())


def _dataset_manifest_path(root: Path, dataset_id: str) -> Path:
    return root / "manifests" / "datasets" / f"{dataset_id}.json"


def _promoted_path(root: Path, spec: CacheDatasetSpec, relative_path: str, staged: Path) -> Path:
    suffix = Path(relative_path).suffix or staged.suffix or ".data"
    name = Path(relative_path).name or f"current{suffix}"
    return _confined_path(root, root / "silver" / spec.dataset_id / "artifacts" / uuid.uuid4().hex / name)


def _required_artifacts(spec: CacheDatasetSpec) -> tuple[str, ...]:
    return tuple(path for path in spec.expected_artifacts if not _is_pattern(path))


def _ordered_artifact_keys(spec: CacheDatasetSpec, artifacts_by_relative: dict[str, dict[str, Any]]) -> list[str]:
    ordered = [path for path in _required_artifacts(spec) if path in artifacts_by_relative]
    ordered.extend(path for path in artifacts_by_relative if path not in ordered)
    return ordered


def _source_url_for_artifact(spec: CacheDatasetSpec, index: int) -> str:
    if not spec.source_urls:
        raise ValueError("Dataset has no public allowlisted source URL")
    if len(spec.source_urls) == 1:
        return spec.source_urls[0]
    try:
        return spec.source_urls[index]
    except IndexError as exc:
        raise ValueError("Dataset does not declare enough source URLs for required artifacts") from exc


def _resolve_relative_path(spec: CacheDatasetSpec, relative_path: str, *, require_for_multi: bool) -> str:
    required = _required_artifacts(spec)
    if not required:
        if relative_path and (_is_pattern(relative_path) or Path(relative_path).is_absolute() or ".." in Path(relative_path).parts):
            raise ValueError("relative_path must be a safe cache-root relative artifact path")
        return relative_path
    if not relative_path:
        if len(required) == 1 or not require_for_multi:
            return required[0]
        raise ValueError(
            "relative_path is required for multi-artifact datasets; allowed values: "
            + ", ".join(required)
        )
    if relative_path not in required:
        raise ValueError("relative_path must exactly match an expected artifact: " + ", ".join(required))
    return relative_path


def _validate_artifact_source_url(spec: CacheDatasetSpec, relative_path: str, source_url: str) -> None:
    if not source_url or not relative_path:
        return
    required = list(_required_artifacts(spec))
    if relative_path not in required:
        return
    expected = _source_url_for_artifact(spec, required.index(relative_path))
    if _normalize_url(source_url) != _normalize_url(expected):
        raise ValueError("source_url does not match the registered source URL for relative_path")


def _select_source_url(spec: CacheDatasetSpec) -> str:
    for url in spec.source_urls:
        parsed = urlparse(url)
        if parsed.scheme in PUBLIC_SCHEMES and parsed.netloc:
            return url
    raise ValueError("Dataset has no public allowlisted source URL")


def _validate_max_bytes(max_bytes: int) -> None:
    if max_bytes <= 0 or max_bytes > DEFAULT_MAX_BYTES:
        raise ValueError(f"max_bytes must be between 1 and {DEFAULT_MAX_BYTES}")


def _new_run_id(dataset_id: str) -> str:
    return f"{dataset_id}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _replace_run(run: CacheRun, **changes: Any) -> CacheRun:
    data = run.to_dict()
    data.update(changes)
    return CacheRun(**data)


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _count_rows(path: Path) -> int | None:
    if path.suffix.casefold() not in {".csv", ".txt", ".tsv", ".data"}:
        return None
    try:
        with path.open("rb") as handle:
            lines = sum(1 for _ in handle)
        return max(0, lines - 1)
    except OSError:
        return None


def _schema_fingerprint(path: Path) -> str:
    columns = _read_columns(path)
    if not columns:
        return ""
    header = ",".join(columns)
    return hashlib.sha256(header.encode("utf-8")).hexdigest() if header else ""


def _read_columns(path: Path) -> list[str]:
    suffix = path.suffix.casefold()
    if suffix in {".csv", ".txt", ".tsv", ".data"}:
        delimiter = "\t" if suffix == ".tsv" else ","
        try:
            with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
                row = next(csv.reader(handle, delimiter=delimiter), [])
        except (OSError, StopIteration, csv.Error):
            return []
        return [str(column).strip() for column in row if str(column).strip()]
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq

            return [str(name).strip() for name in pq.read_schema(path).names if str(name).strip()]
        except Exception:
            return []
    return []


def _normalize_column(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    return _COLUMN_ALIASES.get(normalized, normalized)


def _normalized_columns(spec: CacheDatasetSpec, columns: list[str]) -> set[str]:
    normalized = {_normalize_column(column) for column in columns}
    alias_map: dict[str, str] = {}
    for canonical, aliases in spec.column_aliases.items():
        canonical_normalized = _normalize_column(canonical)
        alias_map[canonical_normalized] = canonical_normalized
        for alias in aliases:
            alias_map[_normalize_column(alias)] = canonical_normalized
    for column in list(normalized):
        if column in alias_map:
            normalized.add(alias_map[column])
    return normalized


_COLUMN_ALIASES = {
    "facility_id": "facility_id",
    "provider_id": "facility_id",
    "cms_certification_number": "facility_id",
    "ccn": "facility_id",
    "prvdr_num": "facility_id",
    "federal_provider_number": "facility_id",
    "measure_id": "measure_id",
    "measure_code": "measure_id",
}

_IDENTIFIER_GROUPS = {
    "facility_id": {"facility_id", "provider_id", "ccn", "prvdr_num"},
    "measure_id": {"measure_id"},
    "health_sys_id": {"health_sys_id"},
    "npi": {"npi"},
    "pecos_enrollment_id": {"pecos_enrollment_id"},
    "uei": {"uei"},
    "cage": {"cage"},
}


def _missing_required_columns(spec: CacheDatasetSpec, normalized_columns: set[str]) -> list[str]:
    missing = []
    for column_name in spec.required_columns:
        normalized = _normalize_column(column_name)
        if normalized not in normalized_columns:
            missing.append(column_name)
    return missing


def _missing_required_identifier_groups(
    spec: CacheDatasetSpec,
    normalized_columns: set[str],
    *,
    relative_path: str = "",
) -> list[str]:
    required_groups: dict[str, set[str]] = {}
    for column_name in (*spec.primary_keys, *spec.join_keys):
        normalized = _normalize_column(column_name)
        group = _IDENTIFIER_GROUPS.get(normalized)
        if group:
            required_groups[normalized] = {_normalize_column(item) for item in group}
        elif normalized:
            required_groups[normalized] = {normalized}
    if _artifact_requires_exact_measure(spec, relative_path, normalized_columns):
        required_groups["measure_id"] = {"measure_id"}
    missing = []
    for label, accepted in sorted(required_groups.items()):
        if not normalized_columns.intersection(accepted):
            missing.append(label)
    return missing


def _artifact_requires_exact_measure(
    spec: CacheDatasetSpec,
    relative_path: str,
    normalized_columns: set[str],
) -> bool:
    if spec.validation_profile != "exact_measure_inventory":
        return False
    if relative_path:
        return relative_path in spec.exact_measure_artifacts
    return bool(normalized_columns.intersection({"measure_name", "measure_id", "hcahps_question", "score"}))


def _count_statuses(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        status = str(entry.get("readiness_status") or entry.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _audit(root: Path, action: str, dataset_id: str, payload: dict[str, Any]) -> str:
    event_id = uuid.uuid4().hex
    record = {
        "event_id": event_id,
        "at": _now(),
        "action": action,
        "dataset_id": dataset_id,
        "payload": payload,
    }
    path = root / "manifests" / "audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
    return event_id


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
