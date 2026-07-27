"""Audit a protected, offline capture of public GitHub metadata for identity leakage."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .public_history_audit import _normalized, _relationship_terms, _searchable_blob, _term_matches

AUDIT_POLICY = "strict-github-network-metadata-capture-v2"
CAPTURE_POLICY = "complete-public-github-network-surface-v2"
SCHEMA_VERSION = 2
CAPTURE_SCHEMA_VERSION = 2
API_VERSION = "2026-03-10"
API_HOST = "api.github.com"
REPOSITORY = {
    "id": 1206377365,
    "owner": "ajhcs",
    "name": "healthcare-data-mcp",
    "visibility": "public",
}
MAX_CAPTURE_AGE_SECONDS = 600

REQUIRED_SURFACES = frozenset(
    {
        "repository",
        "issues",
        "issue_comments",
        "pull_requests",
        "pull_request_reviews",
        "pull_request_review_comments",
        "commit_comments",
        "releases",
        "release_assets",
        "discussions",
        "wiki",
        "actions_runs",
        "actions_jobs",
        "check_runs",
        "check_annotations",
        "actions_logs",
        "actions_artifacts",
        "deployments",
        "deployment_statuses",
        "labels",
        "milestones",
        "security_advisories",
        "forks",
        "fork_git_refs",
        "fork_metadata",
        "github_uploads",
        "pages",
    }
)

PARENT_SURFACES = {
    "pull_request_reviews": "pull_requests",
    "release_assets": "releases",
    "actions_jobs": "actions_runs",
    "actions_logs": "actions_runs",
    "check_runs": "actions_runs",
    "check_annotations": "check_runs",
    "deployment_statuses": "deployments",
    "fork_git_refs": "forks",
    "fork_metadata": "forks",
}

NONCOMPLETE_STATES = {
    "discussions": {"disabled"},
    "wiki": {"absent"},
    "pages": {"absent"},
}

_CAPTURE_KEYS = {
    "schema_version",
    "capture_policy",
    "collector_implementation_sha256",
    "endpoint_spec_sha256",
    "api_version",
    "api_host",
    "repository",
    "questions_sha256",
    "identity_sha256",
    "public_ref_shas",
    "capture_started_at_utc",
    "capture_completed_at_utc",
    "surfaces",
    "evidence",
    "revalidation",
    "revalidation_sha256",
    "snapshot_root_sha256",
}
_SURFACE_KEYS = {
    "state",
    "expected_count",
    "records",
    "covered_parent_ids",
    "evidence_ids",
    "first_pass_sha256",
    "second_pass_sha256",
}
_RECORD_KEYS = {"id", "updated_at", "content"}
_EVIDENCE_KEYS = {"path", "sha256", "size"}
_REPOSITORY_KEYS = {"id", "node_id", "owner", "name", "visibility"}
_AUDIT_RESULT_KEYS = {
    "schema_version",
    "policy",
    "capture_schema_version",
    "capture_policy",
    "audit_implementation_sha256",
    "collector_implementation_sha256",
    "endpoint_spec_sha256",
    "capture_manifest_sha256",
    "questions_sha256",
    "identity_sha256",
    "audited_identity_packet",
    "active_system_count",
    "repository",
    "api_version",
    "api_host",
    "public_ref_shas",
    "capture_started_at_utc",
    "capture_completed_at_utc",
    "audited_at_utc",
    "stabilized",
    "surface_complete",
    "required_surfaces",
    "surface_counts",
    "surface_digests",
    "snapshot_root_sha256",
    "revalidation_sha256",
    "hits",
    "blocking_hits",
    "uninspectable_artifacts",
    "incomplete_surfaces",
    "passed",
}
_REVALIDATION_KEYS = {
    "id",
    "repository_id",
    "surface",
    "method",
    "url",
    "request_json",
    "kind",
    "status_code",
    "etag",
    "last_modified",
    "response_sha256",
    "response_size",
    "link",
    "next_url",
}
_LINK_NEXT = re.compile(r'<([^>]+)>;\s*rel="next"')
_GITHUB_UPLOAD = re.compile(
    r"https://(?:github\.com/user-attachments/assets/|user-images\.githubusercontent\.com/|"
    r"github\.com/[^/]+/[^/]+/releases/download/)[^\s\]\[()<>'\"]+",
    re.IGNORECASE,
)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _valid_sha256(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def allowed_github_download_host(host: str | None) -> bool:
    if host in {
        API_HOST,
        "github.com",
        "objects.githubusercontent.com",
        "objects-origin.githubusercontent.com",
        "private-user-images.githubusercontent.com",
        "release-assets.githubusercontent.com",
        "user-images.githubusercontent.com",
        "pipelines.actions.githubusercontent.com",
        "results-receiver.actions.githubusercontent.com",
    }:
        return True
    if host is None:
        return False
    return bool(
        re.fullmatch(r"github-production-(?:repository-file|user-asset)-[0-9a-f]+\.s3\.amazonaws\.com", host)
        or re.fullmatch(r"productionresultssa[0-9]+\.blob\.core\.windows\.net", host)
    )


def endpoint_spec_sha256() -> str:
    return _sha256(
        _canonical_bytes(
            {
                "required_surfaces": sorted(REQUIRED_SURFACES),
                "parent_surfaces": PARENT_SURFACES,
                "noncomplete_states": {key: sorted(value) for key, value in NONCOMPLETE_STATES.items()},
                "network_scope": {
                    "canonical_repository_id": REPOSITORY["id"],
                    "fork_identity": "canonical_forks_plus_detailed_parent_or_source_binding",
                    "record_namespace": "fork:{repository_id}:",
                    "revalidation_scope": "repository_id_x_required_surface",
                },
            }
        )
    )


def implementation_sha256() -> str:
    return _sha256(Path(__file__).read_bytes())


def collector_implementation_sha256() -> str:
    return _sha256(Path(__file__).with_name("github_metadata_collector.py").read_bytes())


def _strict_json(path: Path) -> tuple[bytes, dict[str, Any]]:
    raw = path.read_bytes()

    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON number is not permitted: {value}")

    document = json.loads(raw, parse_constant=reject_constant)
    if not isinstance(document, dict):
        raise TypeError("GitHub capture manifest must be a JSON object")
    return raw, document


def _parse_utc(value: object, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} is not a valid ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def _protected_capture_file(path: Path, root: Path) -> Path:
    if path.is_symlink():
        raise ValueError(f"GitHub capture evidence may not be a symlink: {path}")
    resolved = path.resolve(strict=True)
    protected_root = root.resolve(strict=True)
    if path.absolute() != resolved or protected_root not in resolved.parents:
        raise ValueError(f"GitHub capture evidence escapes its protected root: {path}")
    if not resolved.is_file() or stat.S_IMODE(resolved.stat().st_mode) & 0o077:
        raise ValueError(f"GitHub capture evidence must be a private regular file: {path}")
    return resolved


def _identity_terms(questions: dict[str, Any], identity: dict[str, Any]) -> tuple[dict[str, str], dict[str, set[str]]]:
    question_rows = questions.get("questions")
    if not isinstance(question_rows, list) or not question_rows:
        raise ValueError("active questions must contain at least one question")
    systems: dict[str, str] = {}
    question_terms: dict[str, set[str]] = {}
    for question in question_rows:
        if not isinstance(question, dict):
            raise TypeError("active question rows must be objects")
        system_id = str(question.get("system_id", ""))
        system_name = str(question.get("system", ""))
        if not system_id or not system_name:
            raise ValueError("active question is missing system identity")
        if system_id in systems and systems[system_id] != system_name:
            raise ValueError("active questions disagree on a system name")
        systems[system_id] = system_name
        question_terms.setdefault(system_id, set()).update(
            str(value)
            for value in (question.get("question_id"), question.get("prompt"), question.get("question"))
            if isinstance(value, str) and value
        )
    terms = {system_id: {system_id, name, *question_terms.get(system_id, set())} for system_id, name in systems.items()}
    identity_rows = identity.get("systems")
    if not isinstance(identity_rows, list):
        raise TypeError("identity packet systems must be a list")
    seen: set[str] = set()
    for system in identity_rows:
        if not isinstance(system, dict):
            raise TypeError("identity packet system rows must be objects")
        system_id = str(system.get("system_id", ""))
        if system_id not in terms:
            raise ValueError("identity packet contains a system outside the active questions")
        if system_id in seen:
            raise ValueError("identity packet repeats a system")
        seen.add(system_id)
        candidates: list[object] = [system.get("canonical_name"), *system.get("aliases", [])]
        candidates.extend(entity.get("name") for entity in system.get("legal_entities", []) if isinstance(entity, dict))
        candidates.extend(item.get("identifier") for item in system.get("identifiers", []) if isinstance(item, dict))
        for relationship in system.get("relationships", []):
            candidates.extend(_relationship_terms(relationship))
        terms[system_id].update(str(value) for value in candidates if value)
    if seen != set(systems):
        raise ValueError("identity packet does not cover every active system")
    return systems, terms


def _matched_terms(content: str, candidates: set[str]) -> list[str]:
    folded = content.casefold()
    normalized = _normalized(content)
    digits_only = re.sub(r"\D", "", content)
    return sorted(term for term in candidates if _term_matches(term, folded, normalized, digits_only))


def _surface_records(surface_name: str, surface: object) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(surface, dict) or set(surface) != _SURFACE_KEYS:
        raise ValueError(f"GitHub capture surface has an invalid shape: {surface_name}")
    state = surface["state"]
    allowed_states = {"complete", *NONCOMPLETE_STATES.get(surface_name, set())}
    if state not in allowed_states:
        raise ValueError(f"GitHub capture surface has an invalid state: {surface_name}")
    records = surface["records"]
    if not isinstance(records, list) or type(surface["expected_count"]) is not int:
        raise ValueError(f"GitHub capture surface count is invalid: {surface_name}")
    if surface["expected_count"] < 0 or surface["expected_count"] != len(records):
        raise ValueError(f"GitHub capture surface count is incomplete: {surface_name}")
    if state != "complete" and records:
        raise ValueError(f"disabled or absent GitHub surface contains records: {surface_name}")
    normalized_records: list[dict[str, Any]] = []
    ids: set[str] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != _RECORD_KEYS:
            raise ValueError(f"GitHub capture record has an invalid shape: {surface_name}")
        record_id = str(record["id"])
        if not record_id or record_id in ids:
            raise ValueError(f"GitHub capture record ID is missing or duplicated: {surface_name}")
        if record["updated_at"] is not None:
            _parse_utc(record["updated_at"], f"{surface_name} record updated_at")
        _canonical_bytes(record["content"])
        ids.add(record_id)
        normalized_records.append({"id": record_id, "updated_at": record["updated_at"], "content": record["content"]})
    normalized_records.sort(key=lambda item: item["id"])
    digest = _sha256(_canonical_bytes(normalized_records))
    if surface["first_pass_sha256"] != digest or surface["second_pass_sha256"] != digest:
        raise ValueError(f"GitHub capture surface did not stabilize: {surface_name}")
    evidence_ids = surface["evidence_ids"]
    covered_parent_ids = surface["covered_parent_ids"]
    if (
        not isinstance(evidence_ids, list)
        or not evidence_ids
        or len(evidence_ids) != len(set(evidence_ids))
        or not all(isinstance(item, str) and item for item in evidence_ids)
        or not isinstance(covered_parent_ids, list)
        or len(covered_parent_ids) != len(set(covered_parent_ids))
        or not all(isinstance(item, str) and item for item in covered_parent_ids)
    ):
        raise ValueError(f"GitHub capture coverage declaration is invalid: {surface_name}")
    return normalized_records, digest


def _snapshot_material(
    surfaces: dict[str, Any],
    surface_digests: dict[str, str],
    evidence: dict[str, dict[str, Any]],
    revalidation_sha256: str | None = None,
) -> dict[str, Any]:
    material = {
        "surfaces": {
            name: {
                "state": surfaces[name]["state"],
                "expected_count": surfaces[name]["expected_count"],
                "covered_parent_ids": sorted(surfaces[name]["covered_parent_ids"]),
                "evidence_ids": sorted(surfaces[name]["evidence_ids"]),
                "records_sha256": surface_digests[name],
            }
            for name in sorted(surfaces)
        },
        "evidence": {
            evidence_id: {
                "path": entry["path"],
                "sha256": entry["sha256"],
                "size": entry["size"],
            }
            for evidence_id, entry in sorted(evidence.items())
        },
    }
    if revalidation_sha256 is not None:
        material["revalidation_sha256"] = revalidation_sha256
    return material


def _validate_revalidation(revalidation: object) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(revalidation, list) or not revalidation:
        raise ValueError("GitHub capture has no conditional-revalidation ledger")
    normalized: list[dict[str, Any]] = []
    ids: set[str] = set()
    covered_surfaces: set[str] = set()
    for entry in revalidation:
        if not isinstance(entry, dict) or set(entry) != _REVALIDATION_KEYS:
            raise ValueError("GitHub capture revalidation entry has an invalid shape")
        request_id = entry["id"]
        repository_id = entry["repository_id"]
        surface = entry["surface"]
        if not isinstance(request_id, str) or not request_id or request_id in ids:
            raise ValueError("GitHub capture revalidation request ID is invalid or duplicated")
        if type(repository_id) is not int or repository_id <= 0:
            raise ValueError("GitHub capture revalidation repository ID is invalid")
        if surface not in REQUIRED_SURFACES:
            raise ValueError("GitHub capture revalidation entry names an unknown surface")
        if entry["method"] not in {"GET", "POST"} or entry["kind"] not in {"json", "download", "probe"}:
            raise ValueError("GitHub capture revalidation request method or kind is invalid")
        if entry["method"] == "GET" and entry["request_json"] is not None:
            raise ValueError("GitHub GET revalidation request unexpectedly has a JSON body")
        if entry["method"] == "POST" and not isinstance(entry["request_json"], dict):
            raise ValueError("GitHub POST revalidation request is missing its JSON body")
        try:
            parsed = urlsplit(str(entry["url"]))
        except ValueError as error:
            raise ValueError("GitHub capture revalidation URL is invalid") from error
        if (
            parsed.scheme != "https"
            or not allowed_github_download_host(parsed.hostname)
            or parsed.username
            or parsed.password
        ):
            raise ValueError("GitHub capture revalidation URL escapes the allowed GitHub hosts")
        if type(entry["status_code"]) is not int or entry["status_code"] not in {200, 404}:
            raise ValueError("GitHub capture revalidation response status is unsupported")
        if entry["etag"] is not None and not isinstance(entry["etag"], str):
            raise ValueError("GitHub capture revalidation ETag is invalid")
        if entry["last_modified"] is not None and not isinstance(entry["last_modified"], str):
            raise ValueError("GitHub capture revalidation Last-Modified value is invalid")
        if entry["link"] is not None and not isinstance(entry["link"], str):
            raise ValueError("GitHub capture revalidation Link value is invalid")
        if entry["next_url"] is not None and not isinstance(entry["next_url"], str):
            raise ValueError("GitHub capture revalidation next URL is invalid")
        match = _LINK_NEXT.search(entry["link"] or "")
        linked_next = match.group(1) if match else None
        if entry["next_url"] != linked_next:
            raise ValueError("GitHub capture revalidation Link chain is inconsistent")
        if (
            not _valid_sha256(entry["response_sha256"])
            or type(entry["response_size"]) is not int
            or entry["response_size"] < 0
        ):
            raise ValueError("GitHub capture revalidation response digest is invalid")
        _canonical_bytes(entry["request_json"])
        ids.add(request_id)
        covered_surfaces.add(surface)
        normalized.append(entry)
    if covered_surfaces != REQUIRED_SURFACES:
        raise ValueError("GitHub capture revalidation ledger does not cover every required surface")
    normalized.sort(key=lambda item: item["id"])
    request_targets = {(entry["repository_id"], entry["surface"], entry["url"]) for entry in normalized}
    if any(
        entry["next_url"] is not None
        and (entry["repository_id"], entry["surface"], entry["next_url"]) not in request_targets
        for entry in normalized
    ):
        raise ValueError("GitHub capture revalidation Link chain is incomplete")
    return normalized, _sha256(_canonical_bytes(normalized))


def validate_audit_document(
    document: object,
    *,
    questions_sha256: str,
    identity_sha256: str,
    public_ref_shas: dict[str, str],
    active_system_count: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate a captured-metadata audit at the official runner boundary."""
    if not isinstance(document, dict) or set(document) != _AUDIT_RESULT_KEYS:
        raise ValueError("GitHub metadata leakage audit has an invalid field set")
    if (
        document["schema_version"] != SCHEMA_VERSION
        or document["policy"] != AUDIT_POLICY
        or document["capture_schema_version"] != CAPTURE_SCHEMA_VERSION
        or document["capture_policy"] != CAPTURE_POLICY
        or document["audit_implementation_sha256"] != implementation_sha256()
        or document["collector_implementation_sha256"] != collector_implementation_sha256()
        or document["endpoint_spec_sha256"] != endpoint_spec_sha256()
    ):
        raise ValueError("GitHub metadata leakage audit policy or implementation is unsupported")
    repository = document["repository"]
    if (
        not isinstance(repository, dict)
        or set(repository) != _REPOSITORY_KEYS
        or {key: repository[key] for key in REPOSITORY} != REPOSITORY
        or not isinstance(repository["node_id"], str)
        or not repository["node_id"]
    ):
        raise ValueError("GitHub metadata leakage audit targets the wrong repository")
    if document["api_version"] != API_VERSION or document["api_host"] != API_HOST:
        raise ValueError("GitHub metadata leakage audit has an invalid API binding")
    if (
        document["questions_sha256"] != questions_sha256
        or document["identity_sha256"] != identity_sha256
        or document["public_ref_shas"] != public_ref_shas
        or document["active_system_count"] != active_system_count
        or document["audited_identity_packet"] is not True
    ):
        raise ValueError("GitHub metadata leakage audit is not bound to the active inputs")
    if (
        document["passed"] is not True
        or document["stabilized"] is not True
        or document["surface_complete"] is not True
        or document["blocking_hits"] != []
        or document["uninspectable_artifacts"] != []
        or document["incomplete_surfaces"] != []
    ):
        raise ValueError("GitHub metadata leakage audit is absent, incomplete, or blocking")
    if document["hits"] != []:
        raise ValueError("passing GitHub metadata leakage audit contains identity hits")
    if document["required_surfaces"] != sorted(REQUIRED_SURFACES):
        raise ValueError("GitHub metadata leakage audit does not cover the required surfaces")
    counts = document["surface_counts"]
    digests = document["surface_digests"]
    if (
        not isinstance(counts, dict)
        or set(counts) != REQUIRED_SURFACES
        or any(type(value) is not int or value < 0 for value in counts.values())
        or counts["repository"] != 1
        or not isinstance(digests, dict)
        or set(digests) != REQUIRED_SURFACES
        or any(not _valid_sha256(value) for value in digests.values())
        or not _valid_sha256(document["snapshot_root_sha256"])
        or not _valid_sha256(document["capture_manifest_sha256"])
        or not _valid_sha256(document["revalidation_sha256"])
    ):
        raise ValueError("GitHub metadata leakage audit surface or digest attestation is invalid")
    capture_started = _parse_utc(document["capture_started_at_utc"], "capture_started_at_utc")
    capture_completed = _parse_utc(document["capture_completed_at_utc"], "capture_completed_at_utc")
    audited_at = _parse_utc(document["audited_at_utc"], "audited_at_utc")
    validation_time = (now or datetime.now(UTC)).astimezone(UTC)
    if capture_started > capture_completed or capture_completed > audited_at:
        raise ValueError("GitHub metadata leakage audit timestamps are inconsistent")
    audit_age = (validation_time - audited_at).total_seconds()
    capture_age = (validation_time - capture_completed).total_seconds()
    if (
        audit_age < -300
        or capture_age < -300
        or audit_age > MAX_CAPTURE_AGE_SECONDS
        or capture_age > MAX_CAPTURE_AGE_SECONDS
    ):
        raise ValueError("GitHub metadata leakage audit is not recent enough for an official batch")
    return document


def audit_github_metadata_capture(
    capture_manifest: Path,
    questions_path: Path,
    identity_path: Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate and scan one protected, already-captured GitHub public-surface snapshot."""
    audit_time = (now or datetime.now(UTC)).astimezone(UTC)
    capture_root = capture_manifest.parent.resolve(strict=True)
    if stat.S_IMODE(capture_root.stat().st_mode) & 0o077:
        raise ValueError("protected GitHub capture root permits group or other access")
    manifest_path = _protected_capture_file(capture_manifest, capture_root)
    capture_bytes, capture = _strict_json(manifest_path)
    if set(capture) != _CAPTURE_KEYS:
        raise ValueError("GitHub capture manifest has an invalid field set")
    if capture["schema_version"] != CAPTURE_SCHEMA_VERSION or capture["capture_policy"] != CAPTURE_POLICY:
        raise ValueError("GitHub capture manifest policy is unsupported")
    if capture["collector_implementation_sha256"] != collector_implementation_sha256():
        raise ValueError("GitHub capture collector implementation does not match the auditor")
    if capture["endpoint_spec_sha256"] != endpoint_spec_sha256():
        raise ValueError("GitHub capture endpoint specification does not match the auditor")
    if capture["api_version"] != API_VERSION or capture["api_host"] != API_HOST:
        raise ValueError("GitHub capture API binding is invalid")
    if not isinstance(capture["repository"], dict) or set(capture["repository"]) != _REPOSITORY_KEYS:
        raise ValueError("GitHub capture repository binding has an invalid shape")
    if (
        {key: capture["repository"][key] for key in REPOSITORY} != REPOSITORY
        or not isinstance(capture["repository"]["node_id"], str)
        or not capture["repository"]["node_id"]
    ):
        raise ValueError("GitHub capture targets the wrong repository")

    question_bytes, questions = _strict_json(questions_path)
    identity_bytes, identity = _strict_json(identity_path)
    questions_sha256 = _sha256(question_bytes)
    identity_sha256 = _sha256(identity_bytes)
    if capture["questions_sha256"] != questions_sha256 or capture["identity_sha256"] != identity_sha256:
        raise ValueError("GitHub capture is not bound to the active questions and identity packet")
    systems, terms = _identity_terms(questions, identity)

    public_ref_shas = capture["public_ref_shas"]
    if not isinstance(public_ref_shas, dict) or not public_ref_shas:
        raise ValueError("GitHub capture has no public-ref binding")
    if any(
        not isinstance(ref, str) or not ref or not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{40}", sha) is None
        for ref, sha in public_ref_shas.items()
    ):
        raise ValueError("GitHub capture public-ref binding is malformed")
    capture_started = _parse_utc(capture["capture_started_at_utc"], "capture_started_at_utc")
    capture_completed = _parse_utc(capture["capture_completed_at_utc"], "capture_completed_at_utc")
    if capture_started > capture_completed:
        raise ValueError("GitHub capture completed before it started")
    capture_age = (audit_time - capture_completed).total_seconds()
    if capture_age < -300 or capture_age > MAX_CAPTURE_AGE_SECONDS:
        raise ValueError("GitHub capture is not recent enough for audit")

    surfaces = capture["surfaces"]
    if not isinstance(surfaces, dict) or set(surfaces) != REQUIRED_SURFACES:
        raise ValueError("GitHub capture does not contain the exact required surface set")
    surface_records: dict[str, list[dict[str, Any]]] = {}
    surface_digests: dict[str, str] = {}
    for surface_name in sorted(REQUIRED_SURFACES):
        records, digest = _surface_records(surface_name, surfaces[surface_name])
        surface_records[surface_name] = records
        surface_digests[surface_name] = digest
    if len(surface_records["repository"]) != 1 or surfaces["repository"]["state"] != "complete":
        raise ValueError("GitHub repository surface must contain exactly one complete record")
    for surface_name, parent_name in PARENT_SURFACES.items():
        expected_parent_ids = {record["id"] for record in surface_records[parent_name]}
        covered_parent_ids = set(surfaces[surface_name]["covered_parent_ids"])
        if covered_parent_ids != expected_parent_ids:
            raise ValueError(f"GitHub nested surface does not cover every parent: {surface_name}")
    for surface_name in REQUIRED_SURFACES - set(PARENT_SURFACES):
        if surfaces[surface_name]["covered_parent_ids"]:
            raise ValueError(f"GitHub top-level surface declares unexpected parent coverage: {surface_name}")

    evidence = capture["evidence"]
    if not isinstance(evidence, dict) or not evidence:
        raise ValueError("GitHub capture contains no evidence files")
    referenced_evidence = {evidence_id for surface in surfaces.values() for evidence_id in surface["evidence_ids"]}
    if referenced_evidence != set(evidence):
        raise ValueError("GitHub capture evidence is missing, unknown, or unreferenced")
    evidence_owners: dict[str, str] = {}
    for surface_name, surface in surfaces.items():
        for evidence_id in surface["evidence_ids"]:
            if evidence_id in evidence_owners:
                raise ValueError(
                    f"GitHub capture evidence is reused across surfaces: {evidence_owners[evidence_id]}, {surface_name}"
                )
            evidence_owners[evidence_id] = surface_name
    _revalidation, revalidation_sha256 = _validate_revalidation(capture["revalidation"])
    if capture["revalidation_sha256"] != revalidation_sha256:
        raise ValueError("GitHub capture conditional-revalidation ledger digest mismatch")
    network_repository_ids = {int(REPOSITORY["id"])}
    fork_record_ids = {record["id"] for record in surface_records["forks"]}
    for record in surface_records["fork_metadata"]:
        content = record["content"]
        if not isinstance(content, dict):
            raise TypeError("GitHub fork metadata record is malformed")
        fork_id = content.get("id")
        owner = content.get("owner")
        parent = content.get("parent")
        source = content.get("source")
        if (
            type(fork_id) is not int
            or fork_id == int(REPOSITORY["id"])
            or fork_id in network_repository_ids
            or record["id"] != f"fork:{fork_id}"
            or content.get("visibility") != "public"
            or not isinstance(content.get("node_id"), str)
            or not content["node_id"]
            or not isinstance(owner, dict)
            or not isinstance(owner.get("login"), str)
            or int(REPOSITORY["id"])
            not in {
                parent.get("id") if isinstance(parent, dict) else None,
                source.get("id") if isinstance(source, dict) else None,
            }
        ):
            raise ValueError("GitHub fork metadata is outside the canonical repository network")
        network_repository_ids.add(fork_id)
    if {record["id"] for record in surface_records["fork_metadata"]} != fork_record_ids:
        raise ValueError("GitHub fork metadata does not exactly cover canonical fork enumeration")
    ledger_repository_ids = {entry["repository_id"] for entry in _revalidation}
    if ledger_repository_ids != network_repository_ids:
        raise ValueError("GitHub revalidation ledger does not cover every network repository")
    ledger_scope = {(entry["repository_id"], entry["surface"]) for entry in _revalidation}
    expected_scope = {
        (repository_id, surface) for repository_id in network_repository_ids for surface in REQUIRED_SURFACES
    }
    if ledger_scope != expected_scope:
        raise ValueError("GitHub revalidation ledger does not cover every surface for every network repository")
    hits: list[dict[str, Any]] = []
    uninspectable: list[dict[str, str]] = []
    observed_upload_urls: set[str] = set()
    evidence_paths: set[Path] = set()
    for evidence_id, entry in sorted(evidence.items()):
        if not isinstance(entry, dict) or set(entry) != _EVIDENCE_KEYS:
            raise ValueError(f"GitHub capture evidence entry has an invalid shape: {evidence_id}")
        if not isinstance(evidence_id, str) or not evidence_id:
            raise ValueError("GitHub capture evidence ID is invalid")
        relative = Path(str(entry["path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"GitHub capture evidence path is unsafe: {evidence_id}")
        evidence_path = _protected_capture_file(capture_root / relative, capture_root)
        if evidence_path in evidence_paths:
            raise ValueError(f"GitHub capture evidence file is reused: {evidence_id}")
        evidence_paths.add(evidence_path)
        content_bytes = evidence_path.read_bytes()
        if (
            type(entry["size"]) is not int
            or entry["size"] < 0
            or entry["size"] != len(content_bytes)
            or not _valid_sha256(entry["sha256"])
            or entry["sha256"] != _sha256(content_bytes)
        ):
            raise ValueError(f"GitHub capture evidence digest or size mismatch: {evidence_id}")
        searchable, issues = _searchable_blob(content_bytes, {relative.as_posix()})
        uninspectable.extend({"evidence_id": evidence_id, **issue} for issue in issues)
        observed_upload_urls.update(_GITHUB_UPLOAD.findall(searchable))
        for system_id, system_name in systems.items():
            matched = _matched_terms(searchable, terms[system_id])
            if matched:
                hits.append(
                    {
                        "system_id": system_id,
                        "system": system_name,
                        "surface": "evidence",
                        "record_id": evidence_id,
                        "matched_terms": matched,
                        "classification": "identity_hit_review_required",
                    }
                )

    for surface_name, records in sorted(surface_records.items()):
        for record in records:
            searchable = _canonical_bytes(record["content"]).decode("utf-8")
            observed_upload_urls.update(_GITHUB_UPLOAD.findall(searchable))
            for system_id, system_name in systems.items():
                matched = _matched_terms(searchable, terms[system_id])
                if matched:
                    hits.append(
                        {
                            "system_id": system_id,
                            "system": system_name,
                            "surface": surface_name,
                            "record_id": record["id"],
                            "matched_terms": matched,
                            "classification": "identity_hit_review_required",
                        }
                    )

    captured_upload_urls = {
        str(record["content"].get("url"))
        for record in surface_records["github_uploads"]
        if isinstance(record["content"], dict) and isinstance(record["content"].get("url"), str)
    }
    incomplete_surfaces: list[dict[str, Any]] = []
    missing_uploads = sorted(observed_upload_urls - captured_upload_urls)
    if missing_uploads:
        incomplete_surfaces.append(
            {
                "surface": "github_uploads",
                "reason": "referenced_github_upload_not_captured",
                "count": len(missing_uploads),
            }
        )

    snapshot_material = _snapshot_material(surfaces, surface_digests, evidence, revalidation_sha256)
    snapshot_root = _sha256(_canonical_bytes(snapshot_material))
    if capture["snapshot_root_sha256"] != snapshot_root:
        raise ValueError("GitHub capture snapshot root digest mismatch")
    blocking_hits = hits
    passed = not blocking_hits and not uninspectable and not incomplete_surfaces
    return {
        "schema_version": SCHEMA_VERSION,
        "policy": AUDIT_POLICY,
        "capture_schema_version": CAPTURE_SCHEMA_VERSION,
        "capture_policy": CAPTURE_POLICY,
        "audit_implementation_sha256": implementation_sha256(),
        "collector_implementation_sha256": collector_implementation_sha256(),
        "endpoint_spec_sha256": endpoint_spec_sha256(),
        "capture_manifest_sha256": _sha256(capture_bytes),
        "questions_sha256": questions_sha256,
        "identity_sha256": identity_sha256,
        "audited_identity_packet": True,
        "active_system_count": len(systems),
        "repository": capture["repository"],
        "api_version": API_VERSION,
        "api_host": API_HOST,
        "public_ref_shas": public_ref_shas,
        "capture_started_at_utc": capture_started.isoformat(),
        "capture_completed_at_utc": capture_completed.isoformat(),
        "audited_at_utc": audit_time.isoformat(),
        "stabilized": True,
        "surface_complete": not incomplete_surfaces,
        "required_surfaces": sorted(REQUIRED_SURFACES),
        "surface_counts": {name: len(records) for name, records in sorted(surface_records.items())},
        "surface_digests": surface_digests,
        "snapshot_root_sha256": snapshot_root,
        "revalidation_sha256": revalidation_sha256,
        "hits": hits,
        "blocking_hits": blocking_hits,
        "uninspectable_artifacts": uninspectable,
        "incomplete_surfaces": incomplete_surfaces,
        "passed": passed,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture-manifest", type=Path, required=True)
    parser.add_argument("--questions", type=Path, required=True)
    parser.add_argument("--identity", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit_github_metadata_capture(args.capture_manifest, args.questions, args.identity)
    with args.output.open("x", encoding="utf-8") as descriptor:
        json.dump(result, descriptor, indent=2)
        descriptor.write("\n")
    args.output.chmod(0o600)
    print(
        json.dumps(
            {
                "passed": result["passed"],
                "blocking_hit_count": len(result["blocking_hits"]),
                "uninspectable_count": len(result["uninspectable_artifacts"]),
                "incomplete_surface_count": len(result["incomplete_surfaces"]),
            }
        )
    )


if __name__ == "__main__":
    main()
