"""Pinned Healthcare Data Platform v1 consumer bindings.

The Toolkit contract bundle is copied into this repository as a reviewed,
hash-pinned input.  This module is deliberately small: it resolves the one
published bundle, verifies every declared artifact, and validates the
source-scoped observation envelope before a producer can hand it to a
consumer.  It does not acquire data, write a database, or grant promotion or
runtime authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Mapping


class HealthcareDataPlatformContractError(ValueError):
    """Raised when the pinned v1 contract or an envelope is not conformant."""


@dataclass(frozen=True)
class ContractBinding:
    """A manifest-indexed source contract binding."""

    contract_id: str
    family: str
    artifact_id: str
    path: str
    semver: str
    source_scope: str
    temporal_semantics: str
    unit_semantics: str


@dataclass(frozen=True)
class HealthcareDataPlatformBundle:
    """The verified, immutable bundle pin used by this consumer."""

    bundle_id: str
    bundle_semver: str
    compatibility_floor: str
    bundle_sha256: str
    bindings: tuple[ContractBinding, ...]


_ROOT = Path(__file__).resolve().parents[2]
_BUNDLE_ROOT = _ROOT / "contracts" / "healthcare-data-platform" / "bundle" / "v1"
_MANIFEST_PATH = _BUNDLE_ROOT / "contract-bundle.json"
_MANIFEST_SCHEMA_PATH = _BUNDLE_ROOT / "contract-bundle.schema.json"
_OBSERVATION_SCHEMA_PATH = (
    _ROOT / "contracts" / "healthcare-data-platform" / "observation" / "v1" / "observation-envelope.schema.json"
)
_EXPECTED_BUNDLE_SHA256 = "sha256:dda28dc777b264892e9f0817bbf40fb73f12ff34d35115d4d3d4a146e73b371c"
_SEMVER_RE = re.compile(r"^(?P<major>0|[1-9][0-9]*)\.(?P<minor>0|[1-9][0-9]*)\.(?P<patch>0|[1-9][0-9]*)$")
_MISSINGNESS_STATES = frozenset(
    {"abstained", "not_yet_researched", "unavailable_public", "not_applicable", "blocked_source_conflict"}
)


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HealthcareDataPlatformContractError(f"unable to read contract JSON: {path}") from exc
    if not isinstance(value, dict):
        raise HealthcareDataPlatformContractError(f"contract JSON must be an object: {path}")
    return value


def _sha256(path: Path) -> str:
    try:
        digest = sha256(path.read_bytes()).hexdigest()
    except OSError as exc:
        raise HealthcareDataPlatformContractError(f"unable to read contract artifact: {path}") from exc
    return f"sha256:{digest}"


def _as_dict(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise HealthcareDataPlatformContractError(f"{label} must be an object")
    return value


def _as_list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise HealthcareDataPlatformContractError(f"{label} must be an array")
    return value


def _required_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise HealthcareDataPlatformContractError(f"{label} must be a non-empty string")
    return value


def _validate_json_schema(instance: object, schema_path: Path, label: str) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:  # pragma: no cover - installation failure, not a contract result
        raise HealthcareDataPlatformContractError("jsonschema is required for contract validation") from exc
    schema = _read_json(schema_path)
    errors = sorted(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(instance), key=str)
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.absolute_path)
        suffix = f" at {location}" if location else ""
        raise HealthcareDataPlatformContractError(f"{label} failed schema validation{suffix}: {first.message}")


def _manifest_bindings(manifest: Mapping[str, object]) -> tuple[ContractBinding, ...]:
    source_contracts = _as_list(manifest.get("source_contracts"), "source_contracts")
    bindings: list[ContractBinding] = []
    seen_ids: set[str] = set()
    for raw in source_contracts:
        item = _as_dict(raw, "source contract")
        contract_id = _required_string(item.get("contract_id"), "source contract contract_id")
        if contract_id in seen_ids:
            raise HealthcareDataPlatformContractError(f"duplicate source contract id: {contract_id}")
        seen_ids.add(contract_id)
        bindings.append(
            ContractBinding(
                contract_id=contract_id,
                family=_required_string(item.get("family"), f"{contract_id}.family"),
                artifact_id=_required_string(item.get("artifact_id"), f"{contract_id}.artifact_id"),
                path=_required_string(item.get("path"), f"{contract_id}.path"),
                semver=_required_string(item.get("semver"), f"{contract_id}.semver"),
                source_scope=_required_string(item.get("source_scope"), f"{contract_id}.source_scope"),
                temporal_semantics=_required_string(
                    item.get("temporal_semantics"), f"{contract_id}.temporal_semantics"
                ),
                unit_semantics=_required_string(item.get("unit_semantics"), f"{contract_id}.unit_semantics"),
            )
        )
    if not bindings:
        raise HealthcareDataPlatformContractError("contract bundle must declare at least one source contract")
    return tuple(bindings)


def _validate_semver_policy(manifest: Mapping[str, object]) -> None:
    semver = _required_string(manifest.get("bundle_semver"), "bundle_semver")
    floor = _required_string(manifest.get("compatibility_floor"), "compatibility_floor")
    match = _SEMVER_RE.fullmatch(semver)
    if match is None:
        raise HealthcareDataPlatformContractError(f"unsupported bundle SemVer: {semver}")
    floor_match = re.fullmatch(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$", floor)
    if floor_match is None or floor != f"{match.group('major')}.{match.group('minor')}":
        raise HealthcareDataPlatformContractError("bundle compatibility floor must match its major and minor version")


def validate_pinned_bundle() -> HealthcareDataPlatformBundle:
    """Verify the manifest, artifact paths/hashes, and source bindings.

    The returned value is safe to cache for the lifetime of a process because
    all bytes are checked before it is constructed.
    """

    manifest = _read_json(_MANIFEST_PATH)
    actual_manifest_hash = _sha256(_MANIFEST_PATH)
    if actual_manifest_hash != _EXPECTED_BUNDLE_SHA256:
        raise HealthcareDataPlatformContractError(
            f"stale or modified contract bundle hash: expected {_EXPECTED_BUNDLE_SHA256}, got {actual_manifest_hash}"
        )
    _validate_json_schema(manifest, _MANIFEST_SCHEMA_PATH, "contract bundle manifest")
    _validate_semver_policy(manifest)
    bindings = _manifest_bindings(manifest)

    artifacts = _as_list(manifest.get("artifacts"), "artifacts")
    artifact_by_id: dict[str, dict[str, object]] = {}
    for raw in artifacts:
        artifact = _as_dict(raw, "artifact")
        artifact_id = _required_string(artifact.get("artifact_id"), "artifact_id")
        if artifact_id in artifact_by_id:
            raise HealthcareDataPlatformContractError(f"duplicate artifact id: {artifact_id}")
        artifact_by_id[artifact_id] = artifact
        relative_path = _required_string(artifact.get("path"), f"{artifact_id}.path")
        path = (_ROOT / relative_path).resolve()
        if _ROOT not in path.parents:
            raise HealthcareDataPlatformContractError(f"artifact path escapes repository root: {relative_path}")
        expected_hash = _required_string(artifact.get("sha256"), f"{artifact_id}.sha256")
        actual_hash = _sha256(path)
        if actual_hash != expected_hash:
            raise HealthcareDataPlatformContractError(
                f"stale artifact hash for {artifact_id}: expected {expected_hash}, got {actual_hash}"
            )

    for binding in bindings:
        candidate = artifact_by_id.get(binding.artifact_id)
        if candidate is None:
            raise HealthcareDataPlatformContractError(f"binding references unknown artifact: {binding.artifact_id}")
        artifact = candidate
        if artifact.get("path") != binding.path or artifact.get("source_contract_id") != binding.contract_id:
            raise HealthcareDataPlatformContractError(f"binding/artifact mismatch for {binding.contract_id}")
        if artifact.get("family") != binding.family or artifact.get("semver") != binding.semver:
            raise HealthcareDataPlatformContractError(f"binding family/version mismatch for {binding.contract_id}")

    return HealthcareDataPlatformBundle(
        bundle_id=_required_string(manifest.get("bundle_id"), "bundle_id"),
        bundle_semver=_required_string(manifest.get("bundle_semver"), "bundle_semver"),
        compatibility_floor=_required_string(manifest.get("compatibility_floor"), "compatibility_floor"),
        bundle_sha256=actual_manifest_hash,
        bindings=bindings,
    )


def validate_contract_bundle() -> HealthcareDataPlatformBundle:
    """Compatibility alias for callers that validate the published pin."""

    return validate_pinned_bundle()


def contract_bindings() -> tuple[ContractBinding, ...]:
    """Return the verified five-family binding registry."""

    return validate_pinned_bundle().bindings


def _require_equal(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise HealthcareDataPlatformContractError(f"{label} does not match its envelope lineage")


def validate_source_scoped_observation_envelope(payload: Mapping[str, object]) -> dict[str, object]:
    """Validate one v1 source-scoped envelope and its cross-field lineage.

    JSON Schema enforces the closed shape and explicit missingness states.  The
    additional checks below enforce identity and replay joins that JSON Schema
    cannot express across sibling objects.
    """

    bundle = validate_pinned_bundle()
    envelope = dict(payload)
    _validate_json_schema(envelope, _OBSERVATION_SCHEMA_PATH, "observation envelope")
    source_release = _as_dict(envelope["source_release"], "source_release")
    artifact = _as_dict(envelope["artifact"], "artifact")
    receipt = _as_dict(envelope["receipt"], "receipt")
    activity = _as_dict(envelope["activity"], "activity")
    lineage = _as_dict(envelope["lineage"], "lineage")
    observations = _as_list(envelope["observations"], "observations")

    source_id = source_release["source_id"]
    release_id = source_release["release_id"]
    artifact_id = artifact["artifact_id"]
    receipt_id = receipt["receipt_id"]
    activity_id = activity["activity_id"]
    _require_equal(artifact["release_ref"], release_id, "artifact release_ref")
    _require_equal(receipt["source_release_ref"], release_id, "receipt source_release_ref")
    _require_equal(receipt["artifact_ref"], artifact_id, "receipt artifact_ref")
    _require_equal(activity["output_artifact_refs"], [artifact_id], "activity output_artifact_refs")
    _require_equal(lineage["source_release_ref"], release_id, "lineage source_release_ref")
    _require_equal(lineage["artifact_ref"], artifact_id, "lineage artifact_ref")
    _require_equal(lineage["receipt_ref"], receipt_id, "lineage receipt_ref")
    _require_equal(lineage["activity_ref"], activity_id, "lineage activity_ref")
    first_observation = _as_dict(observations[0], "observation")
    first_scope = _as_dict(first_observation["source_scope"], "observation.source_scope")
    _require_equal(source_release["source_id"], first_scope["source_id"], "source_id")

    custody = _as_dict(artifact["custody"], "artifact.custody")
    observation_ids: list[str] = []
    for raw in observations:
        observation = _as_dict(raw, "observation")
        observation_id = _required_string(observation["observation_id"], "observation_id")
        if observation_id in observation_ids:
            raise HealthcareDataPlatformContractError(f"duplicate observation id: {observation_id}")
        observation_ids.append(observation_id)
        scope = _as_dict(observation["source_scope"], "observation.source_scope")
        _require_equal(scope["source_id"], source_id, f"{observation_id}.source_id")
        _require_equal(scope["release_ref"], release_id, f"{observation_id}.release_ref")
        _require_equal(scope["artifact_ref"], artifact_id, f"{observation_id}.artifact_ref")
        _require_equal(scope["custody_locator"], custody["locator"], f"{observation_id}.custody_locator")
        _require_equal(observation["activity_ref"], activity_id, f"{observation_id}.activity_ref")
        _require_equal(observation["receipt_ref"], receipt_id, f"{observation_id}.receipt_ref")
        value_state = observation["value_state"]
        conflict = _as_dict(observation["conflict"], f"{observation_id}.conflict")
        if value_state in _MISSINGNESS_STATES and observation["value"] is not None:
            raise HealthcareDataPlatformContractError(f"{observation_id} missingness must use null value")
        if value_state == "observed" and observation["value"] is None:
            raise HealthcareDataPlatformContractError(f"{observation_id} observed value cannot be null")
        if value_state == "blocked_source_conflict" and conflict["state"] != "source_conflict":
            raise HealthcareDataPlatformContractError(f"{observation_id} blocked value must carry source conflict")

    lineage_ids = _as_list(lineage["observation_ids"], "lineage.observation_ids")
    deterministic_order = _as_list(lineage["deterministic_order"], "lineage.deterministic_order")
    if lineage_ids != observation_ids or deterministic_order != observation_ids:
        raise HealthcareDataPlatformContractError("lineage observation ids must match envelope order exactly")

    replay = _as_dict(lineage["replay"], "lineage.replay")
    if replay["state"] == "first_seen" and replay["replay_of"] is not None:
        raise HealthcareDataPlatformContractError("first_seen replay cannot reference prior lineage")
    if replay["state"] in {"replayed", "unchanged"} and replay["replay_of"] is None:
        raise HealthcareDataPlatformContractError("replayed or unchanged envelope requires prior lineage")
    if bundle.bundle_sha256 != _EXPECTED_BUNDLE_SHA256:
        raise HealthcareDataPlatformContractError("consumer bundle pin is not the reviewed v1 bundle")
    return envelope


def validate_observation_envelope(payload: Mapping[str, object]) -> dict[str, object]:
    """Short alias for source-scoped envelope validation."""

    return validate_source_scoped_observation_envelope(payload)


__all__ = [
    "ContractBinding",
    "HealthcareDataPlatformBundle",
    "HealthcareDataPlatformContractError",
    "contract_bindings",
    "validate_contract_bundle",
    "validate_observation_envelope",
    "validate_pinned_bundle",
    "validate_source_scoped_observation_envelope",
]
