import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hspr_benchmark.github_metadata_audit import (
    API_HOST,
    API_VERSION,
    CAPTURE_POLICY,
    CAPTURE_SCHEMA_VERSION,
    PARENT_SURFACES,
    REQUIRED_SURFACES,
    REPOSITORY,
    _canonical_bytes,
    _snapshot_material,
    audit_github_metadata_capture,
    endpoint_spec_sha256,
    validate_audit_document,
)
from hspr_benchmark import trial_executor


NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
PUBLIC_REFS = {"refs/remotes/origin/main": "a" * 40}


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _record_digest(records: list[dict[str, object]]) -> str:
    return _digest(_canonical_bytes(sorted(records, key=lambda item: str(item["id"]))))


def _private_write(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    path.chmod(0o600)


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    questions = tmp_path / "questions.json"
    identity = tmp_path / "identity.json"
    _private_write(
        questions,
        json.dumps(
            {
                "publication_status": "protected_unpublished_active_packet",
                "questions": [{"system_id": "HSI1", "system": "Hidden Health"}],
            }
        ).encode(),
    )
    _private_write(
        identity,
        json.dumps(
            {
                "systems": [
                    {
                        "system_id": "HSI1",
                        "canonical_name": "Hidden Health",
                        "aliases": ["Concealed Care"],
                        "legal_entities": [{"name": "Secret Parent Incorporated"}],
                        "identifiers": [{"identifier": "12-3456789"}],
                        "relationships": [{"target_name": "Secret Hospital"}],
                    }
                ]
            }
        ).encode(),
    )
    return questions, identity


def _capture(
    tmp_path: Path,
    questions: Path,
    identity: Path,
    *,
    evidence_overrides: dict[str, bytes] | None = None,
    completed_at: datetime = NOW - timedelta(minutes=1),
) -> tuple[Path, dict[str, object]]:
    root = tmp_path / "capture"
    root.mkdir(mode=0o700)
    surfaces: dict[str, dict[str, object]] = {}
    evidence: dict[str, dict[str, object]] = {}
    records_by_surface: dict[str, list[dict[str, object]]] = {}
    evidence_overrides = evidence_overrides or {}
    for surface_name in sorted(REQUIRED_SURFACES):
        records: list[dict[str, object]] = []
        state = "complete"
        if surface_name == "repository":
            records = [
                {
                    "id": str(REPOSITORY["id"]),
                    "updated_at": completed_at.isoformat(),
                    "content": {"description": "public healthcare integration repository"},
                }
            ]
        elif surface_name == "discussions":
            state = "disabled"
        elif surface_name in {"wiki", "pages"}:
            state = "absent"
        records_by_surface[surface_name] = records
        evidence_id = f"evidence-{surface_name}"
        evidence_path = root / f"{surface_name}.json"
        evidence_bytes = evidence_overrides.get(
            surface_name,
            json.dumps({"surface": surface_name, "items": []}, sort_keys=True).encode(),
        )
        _private_write(evidence_path, evidence_bytes)
        evidence[evidence_id] = {
            "path": evidence_path.name,
            "sha256": _digest(evidence_bytes),
            "size": len(evidence_bytes),
        }
        digest = _record_digest(records)
        surfaces[surface_name] = {
            "state": state,
            "expected_count": len(records),
            "records": records,
            "covered_parent_ids": [],
            "evidence_ids": [evidence_id],
            "first_pass_sha256": digest,
            "second_pass_sha256": digest,
        }
    for child, parent in PARENT_SURFACES.items():
        surfaces[child]["covered_parent_ids"] = [str(record["id"]) for record in records_by_surface[parent]]
    snapshot_root = _digest(
        _canonical_bytes(
            _snapshot_material(
                surfaces, {name: _record_digest(records) for name, records in records_by_surface.items()}, evidence
            )
        )
    )
    manifest: dict[str, object] = {
        "schema_version": CAPTURE_SCHEMA_VERSION,
        "capture_policy": CAPTURE_POLICY,
        "endpoint_spec_sha256": endpoint_spec_sha256(),
        "api_version": API_VERSION,
        "api_host": API_HOST,
        "repository": {**REPOSITORY, "node_id": "repository-node-id"},
        "questions_sha256": _digest(questions.read_bytes()),
        "identity_sha256": _digest(identity.read_bytes()),
        "public_ref_shas": PUBLIC_REFS,
        "capture_started_at_utc": (completed_at - timedelta(minutes=2)).isoformat(),
        "capture_completed_at_utc": completed_at.isoformat(),
        "surfaces": surfaces,
        "evidence": evidence,
        "snapshot_root_sha256": snapshot_root,
    }
    manifest_path = root / "manifest.json"
    _private_write(manifest_path, json.dumps(manifest, sort_keys=True).encode())
    return manifest_path, manifest


def _rewrite_manifest(path: Path, manifest: dict[str, object]) -> None:
    _private_write(path, json.dumps(manifest, sort_keys=True).encode())


def test_github_metadata_capture_passes_exact_stable_complete_fixture(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    manifest, _ = _capture(tmp_path, questions, identity)
    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    assert result["passed"]
    assert result["blocking_hits"] == []
    assert result["uninspectable_artifacts"] == []
    assert result["incomplete_surfaces"] == []
    assert set(result["surface_counts"]) == REQUIRED_SURFACES
    validate_audit_document(
        result,
        questions_sha256=_digest(questions.read_bytes()),
        identity_sha256=_digest(identity.read_bytes()),
        public_ref_shas=PUBLIC_REFS,
        active_system_count=1,
        now=NOW,
    )


def test_github_metadata_capture_blocks_identity_in_raw_evidence(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    manifest, _ = _capture(
        tmp_path,
        questions,
        identity,
        evidence_overrides={"issue_comments": b'{"body":"Secret Parent Incorporated"}'},
    )
    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    assert not result["passed"]
    assert result["blocking_hits"][0]["classification"] == "identity_hit_review_required"
    assert "Secret Parent Incorporated" in result["blocking_hits"][0]["matched_terms"]


def test_github_metadata_capture_blocks_exact_question_prompt(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    question_document = json.loads(questions.read_text())
    prompt = "What was the audited operating revenue for the specified fiscal period?"
    question_document["questions"][0]["prompt"] = prompt
    _private_write(questions, json.dumps(question_document).encode())
    manifest, _ = _capture(
        tmp_path,
        questions,
        identity,
        evidence_overrides={"pull_request_reviews": json.dumps({"body": prompt}).encode()},
    )
    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    assert not result["passed"]
    assert prompt in result["blocking_hits"][0]["matched_terms"]


def test_github_metadata_capture_blocks_opaque_payload(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    manifest, _ = _capture(
        tmp_path,
        questions,
        identity,
        evidence_overrides={"release_assets": b"\x89PNG\r\n\x1a\n"},
    )
    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    assert not result["passed"]
    assert result["uninspectable_artifacts"] == [
        {
            "evidence_id": "evidence-release_assets",
            "path": "release_assets.json",
            "reason": "unsupported_opaque_artifact",
        }
    ]


@pytest.mark.parametrize("failure", ["missing_surface", "unstable", "missing_parent", "reused_evidence"])
def test_github_metadata_capture_rejects_incomplete_or_unstable_manifests(tmp_path: Path, failure: str) -> None:
    questions, identity = _inputs(tmp_path)
    manifest_path, manifest = _capture(tmp_path, questions, identity)
    surfaces = manifest["surfaces"]
    assert isinstance(surfaces, dict)
    if failure == "missing_surface":
        del surfaces["milestones"]
    elif failure == "unstable":
        surfaces["issues"]["second_pass_sha256"] = "b" * 64
    elif failure == "missing_parent":
        surfaces["actions_jobs"]["covered_parent_ids"] = ["unseen-run"]
    else:
        surfaces["issues"]["evidence_ids"] = surfaces["labels"]["evidence_ids"]
    _rewrite_manifest(manifest_path, manifest)
    with pytest.raises(ValueError):
        audit_github_metadata_capture(manifest_path, questions, identity, now=NOW)


def test_github_metadata_capture_rejects_stale_snapshot(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    manifest, _ = _capture(tmp_path, questions, identity, completed_at=NOW - timedelta(minutes=11))
    with pytest.raises(ValueError, match="not recent enough"):
        audit_github_metadata_capture(manifest, questions, identity, now=NOW)


def test_github_metadata_capture_requires_linked_github_upload_record(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    upload = b'{"body":"https://github.com/user-attachments/assets/example-object"}'
    manifest, _ = _capture(tmp_path, questions, identity, evidence_overrides={"issues": upload})
    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    assert not result["passed"]
    assert result["incomplete_surfaces"] == [
        {"surface": "github_uploads", "reason": "referenced_github_upload_not_captured", "count": 1}
    ]


def test_runner_validator_rejects_tampered_surface_and_future_or_stale_audit(tmp_path: Path) -> None:
    questions, identity = _inputs(tmp_path)
    manifest, _ = _capture(tmp_path, questions, identity)
    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    result["surface_counts"] = {**result["surface_counts"], "issues": -1}
    with pytest.raises(ValueError, match="surface or digest"):
        validate_audit_document(
            result,
            questions_sha256=_digest(questions.read_bytes()),
            identity_sha256=_digest(identity.read_bytes()),
            public_ref_shas=PUBLIC_REFS,
            active_system_count=1,
            now=NOW,
        )

    result = audit_github_metadata_capture(manifest, questions, identity, now=NOW)
    with pytest.raises(ValueError, match="not recent enough"):
        validate_audit_document(
            result,
            questions_sha256=_digest(questions.read_bytes()),
            identity_sha256=_digest(identity.read_bytes()),
            public_ref_shas=PUBLIC_REFS,
            active_system_count=1,
            now=NOW + timedelta(minutes=11),
        )


def test_official_boundary_requires_github_metadata_attestation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trial_executor, "OFFICIAL_WEB_BOUNDARY_VALIDATED", True)
    attestation = {
        "active_manifest_sha256": "a" * 64,
        "public_history_audit_sha256": "b" * 64,
        "public_ref_shas": PUBLIC_REFS,
    }
    with pytest.raises(RuntimeError, match="GitHub-metadata boundary"):
        trial_executor.require_official_web_boundary(attestation)
    trial_executor.require_official_web_boundary({**attestation, "github_metadata_audit_sha256": "c" * 64})
