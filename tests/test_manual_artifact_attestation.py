import hashlib
import json
import subprocess
from pathlib import Path
from typing import Literal, overload

import pytest

from hspr_benchmark import runner
from hspr_benchmark.manual_artifact_attestation import (
    SCHEMA_VERSION,
    canonical_sha256,
    protected_attestation_bytes,
    sha256_bytes,
    validate_attestation,
)
from hspr_benchmark.public_history_audit import audit_public_history, implementation_sha256


@overload
def _git(repo: Path, *arguments: str, binary: Literal[False] = False) -> str: ...


@overload
def _git(repo: Path, *arguments: str, binary: Literal[True]) -> bytes: ...


def _git(repo: Path, *arguments: str, binary: bool = False) -> str | bytes:
    completed = subprocess.run(["git", *arguments], cwd=repo, check=True, capture_output=True)
    return completed.stdout if binary else completed.stdout.decode().strip()


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _attestation(
    *,
    raw: list[dict[str, str]],
    questions_sha256: str,
    identity_sha256: str | None,
    public_ref_shas: dict[str, str],
    blob_digests: dict[str, str],
    dispositions: list[str] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "publication_status": "protected_scorer_only_exact_record_review",
        "bindings": {
            "audit_implementation_sha256": implementation_sha256(),
            "questions_sha256": questions_sha256,
            "identity_sha256": identity_sha256,
            "public_ref_shas": public_ref_shas,
            "raw_uninspectables_sha256": canonical_sha256(raw),
        },
        "records": [
            {
                "blob_sha1": item["blob"],
                "blob_sha256": blob_digests[item["blob"]],
                "path": item["path"],
                "raw_reason": item["reason"],
                "review_method": "static_source_equivalence",
                "review_method_version": "1",
                "disposition": (dispositions or ["cleared"] * len(raw))[index],
                "findings": ["exact record reviewed"],
            }
            for index, item in enumerate(raw)
        ],
    }


def test_exact_records_only_suppress_cleared_artifacts() -> None:
    raw = [
        {"blob": "a" * 40, "path": "one.pyc", "reason": "opaque"},
        {"blob": "b" * 40, "path": "two.pdf", "reason": "manual"},
    ]
    digests = {"a" * 40: "1" * 64, "b" * 40: "2" * 64}
    document = _attestation(
        raw=raw,
        questions_sha256="3" * 64,
        identity_sha256="4" * 64,
        public_ref_shas={"refs/remotes/origin/main": "5" * 40},
        blob_digests=digests,
        dispositions=["cleared", "unresolved"],
    )
    residual = validate_attestation(
        document,
        raw_uninspectables=raw,
        audit_implementation_sha256=implementation_sha256(),
        questions_sha256="3" * 64,
        identity_sha256="4" * 64,
        public_ref_shas={"refs/remotes/origin/main": "5" * 40},
        blob_sha256=digests.__getitem__,
    )
    assert residual == [raw[1]]

    document["records"][0]["disposition"] = "cleared_source_derived"  # type: ignore[index]
    with pytest.raises(ValueError, match="disposition"):
        validate_attestation(
            document,
            raw_uninspectables=raw,
            audit_implementation_sha256=implementation_sha256(),
            questions_sha256="3" * 64,
            identity_sha256="4" * 64,
            public_ref_shas={"refs/remotes/origin/main": "5" * 40},
            blob_sha256=digests.__getitem__,
        )


@pytest.mark.parametrize("mutation", ["missing", "extra", "stale", "reused"])
def test_attestation_rejects_non_exact_record_sets(mutation: str) -> None:
    raw = [{"blob": "a" * 40, "path": "one.pyc", "reason": "opaque"}]
    digests = {"a" * 40: "1" * 64}
    document = _attestation(
        raw=raw,
        questions_sha256="3" * 64,
        identity_sha256=None,
        public_ref_shas={"HEAD": "5" * 40},
        blob_digests=digests,
    )
    if mutation == "missing":
        document["records"] = []
    elif mutation == "extra":
        document["records"][0]["path"] = "other.pyc"  # type: ignore[index]
    elif mutation == "stale":
        document["bindings"]["questions_sha256"] = "9" * 64  # type: ignore[index]
    else:
        document["records"].append(dict(document["records"][0]))  # type: ignore[union-attr,index]
    with pytest.raises(ValueError):
        validate_attestation(
            document,
            raw_uninspectables=raw,
            audit_implementation_sha256=implementation_sha256(),
            questions_sha256="3" * 64,
            identity_sha256=None,
            public_ref_shas={"HEAD": "5" * 40},
            blob_sha256=digests.__getitem__,
        )


def test_protected_attestation_rejects_readable_mode_and_symlink(tmp_path: Path) -> None:
    attestation = tmp_path / "attestation.json"
    attestation.write_text("{}")
    attestation.chmod(0o644)
    with pytest.raises(ValueError, match="group or other"):
        protected_attestation_bytes(attestation)
    attestation.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(attestation)
    with pytest.raises(ValueError, match="symlink"):
        protected_attestation_bytes(link)


def test_auditor_and_runner_require_exact_protected_attestation(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    opaque = repo / "cache.pyc"
    opaque.write_bytes(b"\x00opaque")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "opaque fixture")

    active = tmp_path / "active"
    active.mkdir(mode=0o700)
    questions = active / "questions.json"
    questions.write_text(
        json.dumps(
            {
                "publication_status": "protected_unpublished_active_packet",
                "questions": [{"question_id": "Q1", "system_id": "HSI1", "system": "Hidden Health"}],
            }
        )
    )
    registry = active / "identity.json"
    registry.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "as_of": "2026-07-27",
                "systems": [
                    {
                        "system_id": "HSI1",
                        "canonical_name": "Hidden Health",
                        "aliases": [],
                        "legal_entities": [],
                        "relationships": [],
                        "identifiers": [],
                        "effective_from": None,
                        "effective_to": None,
                        "ambiguity_warnings": [],
                        "identity_provenance": [],
                    }
                ],
            }
        )
    )
    for protected in (questions, registry):
        protected.chmod(0o600)

    raw_audit = audit_public_history(repo, questions, ["HEAD"], registry)
    assert len(raw_audit["raw_uninspectable_artifacts"]) == 1
    assert raw_audit["residual_uninspectable_artifacts"] == raw_audit["raw_uninspectable_artifacts"]
    raw = raw_audit["raw_uninspectable_artifacts"]
    object_id = raw[0]["blob"]
    blob_bytes = bytes(_git(repo, "cat-file", "blob", object_id, binary=True))
    attestation_document = _attestation(
        raw=raw,
        questions_sha256=_digest(questions),
        identity_sha256=_digest(registry),
        public_ref_shas=raw_audit["public_ref_shas"],
        blob_digests={object_id: sha256_bytes(blob_bytes)},
    )
    attestation = active / "manual-attestation.json"
    attestation.write_text(json.dumps(attestation_document))
    attestation.chmod(0o600)
    audited = audit_public_history(repo, questions, ["HEAD"], registry, attestation)
    assert audited["raw_uninspectable_artifacts"] == raw
    assert audited["residual_uninspectable_artifacts"] == []
    assert audited["manual_artifact_attestation_sha256"] == _digest(attestation)

    history = active / "history.json"
    history.write_text(json.dumps(audited))
    preregistration = active / "preregistration.json"
    preregistration.write_text(json.dumps({"status": "frozen_before_answer_trials", "design": {"questions": 1}}))
    github_audit = active / "github.json"
    github_audit.write_text("{}")
    for protected in (history, preregistration, github_audit):
        protected.chmod(0o600)
    manifest = active / "manifest.json"
    files = {
        "questions": questions,
        "registry": registry,
        "public_history_audit": history,
        "manual_artifact_attestation": attestation,
        "github_metadata_audit": github_audit,
        "preregistration": preregistration,
    }
    manifest.write_text(
        json.dumps(
            {
                "publication_status": "protected_unpublished_active_packet",
                "files": {name: {"path": path.name, "sha256": _digest(path)} for name, path in files.items()},
            }
        )
    )
    manifest.chmod(0o600)
    monkeypatch.setattr(runner, "REPOSITORY_ROOT", repo)
    monkeypatch.setattr(runner, "_current_public_ref_shas", lambda: audited["public_ref_shas"])
    monkeypatch.setattr(runner, "validate_github_metadata_audit_document", lambda *args, **kwargs: None)
    result = runner._validate_active_inputs(manifest, questions, registry)
    assert result["manual_artifact_attestation_sha256"] == _digest(attestation)

    attestation.chmod(0o640)
    with pytest.raises(ValueError, match="private regular file"):
        runner._validate_active_inputs(manifest, questions, registry)
