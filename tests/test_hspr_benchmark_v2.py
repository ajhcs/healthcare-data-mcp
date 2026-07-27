import base64
import hashlib
import json
import subprocess
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hspr_benchmark import runner
from hspr_benchmark.cohort import PILOT_IDS, build_cohort
from hspr_benchmark.container_launcher import (
    assemble_answer_packet,
    codex_docker_command,
    docker_command,
    stage_runtime,
    summarize_codex_trace,
    write_new_text,
)
from hspr_benchmark.leakage import audit_registry_packet
from hspr_benchmark.github_metadata_audit import (
    API_HOST as GITHUB_API_HOST,
    API_VERSION as GITHUB_API_VERSION,
    AUDIT_POLICY as GITHUB_AUDIT_POLICY,
    CAPTURE_POLICY as GITHUB_CAPTURE_POLICY,
    REQUIRED_SURFACES as GITHUB_REQUIRED_SURFACES,
    REPOSITORY as GITHUB_REPOSITORY,
    endpoint_spec_sha256 as github_endpoint_spec_sha256,
    implementation_sha256 as github_implementation_sha256,
)
from hspr_benchmark.public_history_audit import AUDIT_POLICY, audit_public_history, implementation_sha256
from hspr_benchmark.runner import (
    _validate_active_inputs,
    _validate_sealed_gold,
    counterbalanced_schedule,
    observable_events,
)
from hspr_benchmark.scoring import paired_cluster_bootstrap, score_answer
from hspr_benchmark.trial_executor import (
    OFFICIAL_WEB_BOUNDARY_VALIDATED,
    _validate_answer_shape,
    _validate_subscription_credential_boundary,
    run_trial,
    trial_prompt,
)

ROOT = Path(__file__).resolve().parents[1]


def _git(repo: Path, *arguments: str) -> None:
    subprocess.run(["git", *arguments], cwd=repo, check=True, capture_output=True)


def _passing_github_audit(questions: Path, identity: Path, public_ref_shas: dict[str, str]) -> dict[str, object]:
    now = datetime.now(UTC)
    empty_digest = hashlib.sha256(b"[]").hexdigest()
    counts = {surface: 0 for surface in GITHUB_REQUIRED_SURFACES}
    counts["repository"] = 1
    return {
        "schema_version": 1,
        "policy": GITHUB_AUDIT_POLICY,
        "capture_schema_version": 1,
        "capture_policy": GITHUB_CAPTURE_POLICY,
        "audit_implementation_sha256": github_implementation_sha256(),
        "endpoint_spec_sha256": github_endpoint_spec_sha256(),
        "capture_manifest_sha256": "b" * 64,
        "questions_sha256": hashlib.sha256(questions.read_bytes()).hexdigest(),
        "identity_sha256": hashlib.sha256(identity.read_bytes()).hexdigest(),
        "audited_identity_packet": True,
        "active_system_count": 1,
        "repository": {**GITHUB_REPOSITORY, "node_id": "repository-node-id"},
        "api_version": GITHUB_API_VERSION,
        "api_host": GITHUB_API_HOST,
        "public_ref_shas": public_ref_shas,
        "capture_started_at_utc": (now - timedelta(minutes=2)).isoformat(),
        "capture_completed_at_utc": (now - timedelta(minutes=1)).isoformat(),
        "audited_at_utc": now.isoformat(),
        "stabilized": True,
        "surface_complete": True,
        "required_surfaces": sorted(GITHUB_REQUIRED_SURFACES),
        "surface_counts": counts,
        "surface_digests": {surface: empty_digest for surface in GITHUB_REQUIRED_SURFACES},
        "snapshot_root_sha256": "c" * 64,
        "hits": [],
        "blocking_hits": [],
        "uninspectable_artifacts": [],
        "incomplete_surfaces": [],
        "passed": True,
    }


def test_public_history_audit_detects_removed_answer_bearing_material(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    questions = tmp_path / "questions.json"
    questions.write_text(json.dumps({"questions": [{"system_id": "HSI1", "system": "Hidden Health"}]}))
    historical = repo / "perimeter-registry/benchmarks/historical/gold.json"
    historical.parent.mkdir(parents=True)
    historical.write_text('{"system":"Secret Parent Incorporated"}')
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "answer bearing")
    historical.unlink()
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "remove answer bearing file")
    identity = tmp_path / "identity.json"
    identity.write_text(
        json.dumps(
            {
                "systems": [
                    {
                        "system_id": "HSI1",
                        "canonical_name": "Hidden Health",
                        "aliases": [],
                        "legal_entities": [{"name": "Secret Parent Incorporated"}],
                        "identifiers": [],
                    }
                ]
            }
        )
    )
    without_identity = audit_public_history(repo, questions, ["HEAD"])
    assert without_identity["passed"]
    result = audit_public_history(repo, questions, ["HEAD"], identity)
    assert not result["passed"]
    assert result["schema_version"] == 4
    assert result["policy"] == AUDIT_POLICY
    assert result["audit_implementation_sha256"] == implementation_sha256()
    assert result["questions_sha256"] == hashlib.sha256(questions.read_bytes()).hexdigest()
    assert result["identity_sha256"] == hashlib.sha256(identity.read_bytes()).hexdigest()
    assert result["audited_identity_packet"]
    assert result["public_ref_shas"]["HEAD"]
    assert any(hit["classification"] == "answer_bearing" for hit in result["blocking_hits"])


def test_public_history_audit_only_allows_digest_pinned_selection_metadata(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    selection = repo / "hspr-benchmark-v2/public/cohort.json"
    selection.parent.mkdir(parents=True)
    cohort = json.loads((ROOT / "hspr-benchmark-v2/public/cohort.json").read_text())
    selected = cohort["systems"][0]
    selection.write_bytes((ROOT / "hspr-benchmark-v2/public/cohort.json").read_bytes())
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "selection only")
    questions = tmp_path / "questions.json"
    questions.write_text(
        json.dumps({"questions": [{"system_id": selected["system_id"], "system": selected["system_name"]}]})
    )
    allowed = audit_public_history(repo, questions, ["HEAD"])
    assert allowed["passed"]
    assert {hit["classification"] for hit in allowed["hits"]} == {"declared_selection_metadata"}

    selection.write_text(selection.read_text() + "\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "unreviewed selection mutation")
    blocked = audit_public_history(repo, questions, ["HEAD"])
    assert not blocked["passed"]
    assert any(hit["classification"] == "identity_hit_review_required" for hit in blocked["blocking_hits"])


def test_public_history_audit_blocks_identity_hit_without_answer_value_signal(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    generic = repo / "src/catalog.txt"
    generic.parent.mkdir()
    generic.write_text("Hidden Health is in the catalog; no financial values are stored here.")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "identity catalog")
    questions = tmp_path / "questions.json"
    questions.write_text(json.dumps({"questions": [{"system_id": "HSI1", "system": "Hidden Health"}]}))
    blocked = audit_public_history(repo, questions, ["HEAD"])
    assert not blocked["passed"]
    assert any(hit["classification"] == "identity_hit_review_required" for hit in blocked["blocking_hits"])


def test_public_history_audit_scans_commit_messages(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    _git(repo, "commit", "--allow-empty", "-m", "Hidden Health total assets research")
    questions = tmp_path / "questions.json"
    questions.write_text(json.dumps({"questions": [{"system_id": "HSI1", "system": "Hidden Health"}]}))
    blocked = audit_public_history(repo, questions, ["HEAD"])
    assert not blocked["passed"]
    assert any(hit["path"].startswith("git-commit-message/") for hit in blocked["blocking_hits"])


def test_public_history_audit_expands_relationships_identifiers_and_binary_paths(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    binary = repo / "docs/Secret-Parent-Incorporated/report.pdf"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(b"%PDF-1.7\x00\xff")
    identifier = repo / "docs/identifier.txt"
    identifier.write_text("Public filer identifier 123456789")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "binary identity artifacts")
    questions = tmp_path / "questions.json"
    questions.write_text(json.dumps({"questions": [{"system_id": "HSI1", "system": "Hidden Health"}]}))
    identity = tmp_path / "identity.json"
    identity.write_text(
        json.dumps(
            {
                "systems": [
                    {
                        "system_id": "HSI1",
                        "canonical_name": "Hidden Health",
                        "aliases": [],
                        "legal_entities": [],
                        "identifiers": [{"identifier": "12-3456789"}],
                        "relationships": [{"target_name": "Secret Parent Incorporated"}],
                    }
                ]
            }
        )
    )
    blocked = audit_public_history(repo, questions, ["HEAD"], identity)
    assert not blocked["passed"]
    matched = {term for hit in blocked["blocking_hits"] for term in hit["matched_terms"]}
    assert {"12-3456789", "Secret Parent Incorporated"} <= matched


def test_public_history_audit_extracts_archives_and_rejects_opaque_artifacts(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "benchmark@example.invalid")
    _git(repo, "config", "user.name", "Benchmark Test")
    archive = repo / "docs/report.docx"
    archive.parent.mkdir()
    with zipfile.ZipFile(archive, "w") as document:
        document.writestr("word/document.xml", "<w:t>Hidden Health total assets 123</w:t>")
        document.writestr("word/embeddings/statement.txt", b"\x00\x01 hidden binary statement")
    opaque = repo / "docs/chart.png"
    opaque.write_bytes(b"\x89PNG\r\n\x1a\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "generic binary artifacts")
    questions = tmp_path / "questions.json"
    questions.write_text(json.dumps({"questions": [{"system_id": "HSI1", "system": "Hidden Health"}]}))
    result = audit_public_history(repo, questions, ["HEAD"])
    assert not result["passed"]
    assert any(hit["path"] == "docs/report.docx" for hit in result["blocking_hits"])
    assert {(item["path"], item["reason"]) for item in result["uninspectable_artifacts"]} == {
        ("docs/chart.png", "unsupported_opaque_artifact"),
        ("docs/report.docx", "archive_contains_unsupported_members"),
    }


def test_protected_active_and_sealed_manifests_bind_private_inputs(tmp_path: Path, monkeypatch) -> None:
    active = tmp_path / "active"
    sealed = tmp_path / "sealed"
    active.mkdir(mode=0o700)
    sealed.mkdir(mode=0o700)
    questions = active / "questions.json"
    registry = active / "identity.json"
    history = active / "history.json"
    github_metadata = active / "github-metadata.json"
    preregistration = active / "preregistration.json"
    questions.write_text(
        json.dumps(
            {
                "publication_status": "protected_unpublished_active_packet",
                "questions": [{"system_id": "HSI1", "system": "Hidden Health"}],
            }
        )
    )
    registry.write_text('{"systems":[]}')
    history.write_text(
        json.dumps(
            {
                "schema_version": 4,
                "policy": AUDIT_POLICY,
                "audit_implementation_sha256": implementation_sha256(),
                "passed": True,
                "audited_at_utc": datetime.now(UTC).isoformat(),
                "blocking_hits": [],
                "uninspectable_artifacts": [],
                "active_system_count": 1,
                "audited_identity_packet": True,
                "questions_sha256": hashlib.sha256(questions.read_bytes()).hexdigest(),
                "identity_sha256": hashlib.sha256(registry.read_bytes()).hexdigest(),
                "public_ref_shas": {"refs/remotes/origin/main": "a" * 40},
            }
        )
    )
    github_metadata.write_text(
        json.dumps(
            _passing_github_audit(
                questions,
                registry,
                {"refs/remotes/origin/main": "a" * 40},
            )
        )
    )
    preregistration.write_text(json.dumps({"status": "frozen_before_answer_trials", "design": {"questions": 1}}))
    for path in (questions, registry, history, github_metadata, preregistration):
        path.chmod(0o600)

    def digest(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    active_manifest = active / "manifest.json"
    active_manifest.write_text(
        json.dumps(
            {
                "publication_status": "protected_unpublished_active_packet",
                "files": {
                    "questions": {"path": questions.name, "sha256": digest(questions)},
                    "registry": {"path": registry.name, "sha256": digest(registry)},
                    "public_history_audit": {"path": history.name, "sha256": digest(history)},
                    "github_metadata_audit": {
                        "path": github_metadata.name,
                        "sha256": digest(github_metadata),
                    },
                    "preregistration": {
                        "path": preregistration.name,
                        "sha256": digest(preregistration),
                    },
                },
            }
        )
    )
    active_manifest.chmod(0o600)
    monkeypatch.setattr(
        runner,
        "_current_public_ref_shas",
        lambda: {"refs/remotes/origin/main": "a" * 40},
    )
    attestation = _validate_active_inputs(active_manifest, questions, registry)
    assert attestation["public_ref_shas"] == {"refs/remotes/origin/main": "a" * 40}
    packet = assemble_answer_packet(
        tmp_path / "answer-packet",
        question={"question_id": "q"},
        response_schema=ROOT / "hspr-benchmark-v2/config/response-schema.json",
        registry_packet=registry,
        registry_approved_root=active,
    )
    assert (packet / "hspr-identity.json").read_text() == registry.read_text()

    original_history = history.read_text()
    history_document = json.loads(original_history)
    history_document["identity_sha256"] = "b" * 64
    history.write_text(json.dumps(history_document))
    manifest_document = json.loads(active_manifest.read_text())
    manifest_document["files"]["public_history_audit"]["sha256"] = digest(history)
    active_manifest.write_text(json.dumps(manifest_document))
    with pytest.raises(ValueError, match="not bound to the active registry"):
        _validate_active_inputs(active_manifest, questions, registry)
    history.write_text(original_history)
    manifest_document["files"]["public_history_audit"]["sha256"] = digest(history)
    active_manifest.write_text(json.dumps(manifest_document))

    adjudication = sealed / "adjudication"
    adjudication.mkdir(mode=0o700)
    gold = adjudication / "gold.json"
    gold.write_text('{"records":[{},{}]}')
    gold.chmod(0o600)
    sealed_manifest = sealed / "manifest.json"
    sealed_manifest.write_text(
        json.dumps(
            {
                "adjudicated_gold_path": "adjudication/gold.json",
                "adjudicated_gold_sha256": digest(gold),
                "adjudicated_records": 2,
            }
        )
    )
    sealed_manifest.chmod(0o600)
    sealed_attestation = _validate_sealed_gold(gold, sealed_manifest)
    assert sealed_attestation["adjudicated_records"] == 2

    monkeypatch.setattr(
        runner,
        "_current_public_ref_shas",
        lambda: {"refs/remotes/origin/main": "b" * 40},
    )
    with pytest.raises(ValueError, match="currently fetched public refs"):
        _validate_active_inputs(active_manifest, questions, registry)
    monkeypatch.setattr(
        runner,
        "_current_public_ref_shas",
        lambda: {"refs/remotes/origin/main": "a" * 40},
    )
    registry.write_text('{"systems":[{"tampered":true}]}')
    try:
        _validate_active_inputs(active_manifest, questions, registry)
    except ValueError as error:
        assert "digest mismatch" in str(error)
    else:
        raise AssertionError("protected active input drift must fail closed")


def test_cohort_is_reproducible_unique_and_disjoint() -> None:
    path = ROOT / "qa/reports/health_system_metrics_reconciliation.csv"
    first = build_cohort(path)
    second = build_cohort(path)
    assert first == second
    systems = first["systems"]
    assert len(systems) == 100
    assert len({row["system_id"] for row in systems}) == 100
    assert not ({row["system_id"] for row in systems} & PILOT_IDS)
    assert [row["selection_group"] for row in systems].count("standard") == 35
    assert [row["selection_group"] for row in systems].count("random") == 50
    assert [row["selection_group"] for row in systems].count("tricky") == 15


def test_identity_packet_passes_and_financial_packet_fails(tmp_path: Path) -> None:
    example = ROOT / "hspr-benchmark-v2/registry/identity-packet.example.json"
    assert audit_registry_packet(example)["passed"]
    leaked = json.loads(example.read_text())
    leaked["systems"][0]["measurements"] = [{"revenue": 123}]
    path = tmp_path / "leaked.json"
    path.write_text(json.dumps(leaked))
    audit = audit_registry_packet(path)
    assert not audit["passed"]
    assert any("forbidden" in item or "unknown" in item for item in audit["findings"])


def test_key_audit_detects_scaled_amount_source_url_and_locator(tmp_path: Path) -> None:
    packet = json.loads((ROOT / "hspr-benchmark-v2/registry/identity-packet.example.json").read_text())
    packet["systems"][0]["ambiguity_warnings"] = [
        "The relevant figure is 22.807769 billion.",
        "Use Consolidated Statements of Operations 2025 column total operating revenues.",
    ]
    packet["systems"][0]["identity_provenance"] = [
        {
            "claim": "identity",
            "source_url": "https://example.org/report.pdf?download=1",
            "accessed": "2026-07-26",
        }
    ]
    key = {
        "records": [
            {
                "question_id": "q",
                "validated_value": 22807769000,
                "primary_source": "https://example.org/report.pdf",
                "exact_locator": "Consolidated Statements of Operations, 2025 column, total operating revenues",
            }
        ]
    }
    packet_path = tmp_path / "packet.json"
    key_path = tmp_path / "gold.json"
    packet_path.write_text(json.dumps(packet))
    key_path.write_text(json.dumps(key))
    findings = audit_registry_packet(packet_path, key_path)["findings"]
    assert any("amount overlap" in item for item in findings)
    assert any("source URL overlap" in item for item in findings)
    assert any("locator n-gram overlap" in item for item in findings)


def test_scoring_and_clustered_pairing() -> None:
    gold = {
        "validated_value": 100,
        "tolerance": {"absolute_usd": 1},
        "requested_metric_available": True,
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31"},
        "units": {"currency": "USD", "normalized_scale": "ones"},
        "entity_perimeter": {
            "description": "Example Health and affiliates, consolidated.",
            "includes": ["Example Health"],
            "excludes": ["one hospital alone"],
            "source_defined_consolidation": True,
        },
        "primary_source": {"url": "https://example.org/u"},
        "exact_locator": "line l",
        "required_caveats": [{"code": "system", "text": "Consolidated system result, not one hospital."}],
        "aggregation_permitted": True,
    }
    answer = {
        "answer_status": "reported",
        "value": 0.1005,
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31"},
        "units": {"currency": "USD", "scale": "thousands"},
        "reporting_perimeter": "Example Health and affiliates, consolidated",
        "primary_source_url": "https://example.org/u",
        "exact_locator": "line l",
        "caveats": ["This is the consolidated system result, not one hospital."],
        "aggregated_entities": ["affiliate"],
    }
    assert score_answer(answer, gold)["fully_correct"]
    wrong_scale = {**answer, "units": {"currency": "USD", "scale": "millions"}}
    assert not score_answer(wrong_scale, gold)["financial_correct"]
    rows = [
        {"system_id": "a", "arm_id": "x", "m": 1},
        {"system_id": "a", "arm_id": "y", "m": 0},
        {"system_id": "b", "arm_id": "x", "m": 0},
        {"system_id": "b", "arm_id": "y", "m": 0},
    ]
    result = paired_cluster_bootstrap(rows, "m", "x", "y", draws=100)
    assert result["paired_difference"] == 0.5
    assert result["system_clusters"] == 2


def test_scoring_ignores_schema_required_null_period_placeholders() -> None:
    gold = {
        "validated_value": 100,
        "tolerance": {"absolute_usd": 0},
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31"},
        "units": {"currency": "USD", "scale": "ones"},
        "entity_perimeter": "Example Health",
        "primary_source": {"url": "https://example.org/report"},
    }
    answer = {
        "answer_status": "reported",
        "value": 100,
        "period": {
            "type": "fiscal_year",
            "start": "2024-01-01",
            "end": "2024-12-31",
            "date": None,
        },
        "units": {"currency": "USD", "scale": "ones"},
        "reporting_perimeter": "Example Health",
        "primary_source_url": "https://example.org/report",
        "exact_locator": "",
        "caveats": [],
        "aggregated_entities": [],
    }
    assert score_answer(answer, gold)["financial_correct"]


def test_schedule_is_deterministic_and_balanced() -> None:
    first = counterbalanced_schedule(["q1", "q2"], ["a", "b", "c", "d"], 3, "seed")
    assert first == counterbalanced_schedule(["q1", "q2"], ["a", "b", "c", "d"], 3, "seed")
    assert len(first) == 24
    assert {row["arm_id"] for row in first} == {"a", "b", "c", "d"}


def test_subscription_credential_strips_refresh_authority_and_rejects_api_keys() -> None:
    def token(expiry: int) -> str:
        claims = base64.urlsafe_b64encode(json.dumps({"exp": expiry}).encode()).decode().rstrip("=")
        return f"header.{claims}.signature"

    source = {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "tokens": {
            "access_token": token(3000),
            "id_token": token(3000),
            "account_id": "account",
            "refresh_token": "long-lived-refresh-secret",  # pragma: allowlist secret
        },
    }
    minimized = json.loads(
        runner.ephemeral_chatgpt_credential(
            json.dumps(source).encode(), now_epoch_seconds=1000, minimum_validity_seconds=900
        )
    )
    assert minimized["auth_mode"] == "chatgpt"
    assert minimized["OPENAI_API_KEY"] is None
    assert set(minimized["tokens"]) == {"access_token", "account_id"}
    assert "long-lived-refresh-secret" not in json.dumps(minimized)

    source["OPENAI_API_KEY"] = "forbidden"  # pragma: allowlist secret
    try:
        runner.ephemeral_chatgpt_credential(json.dumps(source).encode(), now_epoch_seconds=1000)
    except ValueError as error:
        assert "API keys are forbidden" in str(error)
    else:
        raise AssertionError("API-key authentication must be rejected")


def test_subscription_credential_rejects_expiring_tokens() -> None:
    claims = base64.urlsafe_b64encode(json.dumps({"exp": 1500}).encode()).decode().rstrip("=")
    token = f"header.{claims}.signature"
    source = {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "tokens": {"access_token": token, "id_token": token, "account_id": "account", "refresh_token": "secret"},
    }
    try:
        runner.ephemeral_chatgpt_credential(json.dumps(source).encode(), now_epoch_seconds=1000)
    except ValueError as error:
        assert "expires too soon" in str(error)
    else:
        raise AssertionError("a token expiring inside the trial window must be rejected")


def test_subscription_credential_rejects_expired_identity_claims_when_access_is_fresh() -> None:
    def token(expiry: int) -> str:
        claims = base64.urlsafe_b64encode(json.dumps({"exp": expiry}).encode()).decode().rstrip("=")
        return f"header.{claims}.signature"

    source = {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "tokens": {
            "access_token": token(3000),
            "id_token": token(500),
            "account_id": "account",
            "refresh_token": "secret",
        },
    }
    with pytest.raises(ValueError, match="id_token expires too soon"):
        runner.ephemeral_chatgpt_credential(
            json.dumps(source).encode(), now_epoch_seconds=1000, minimum_validity_seconds=900
        )


def test_trial_boundary_rejects_refresh_credentials_and_extra_fields() -> None:
    minimized = {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "tokens": {
            "access_token": "short-lived-access",
            "account_id": "account",
        },
    }
    _validate_subscription_credential_boundary(json.dumps(minimized).encode())
    for mutated in (
        {**minimized, "extra": "forbidden"},
        {**minimized, "OPENAI_API_KEY": "forbidden"},  # pragma: allowlist secret
        {  # pragma: allowlist secret
            **minimized,
            "tokens": {**minimized["tokens"], "refresh_token": "reusable"},
        },
        {**minimized, "tokens": {**minimized["tokens"], "id_token": "unneeded-identity"}},
    ):
        try:
            _validate_subscription_credential_boundary(json.dumps(mutated).encode())
        except ValueError:
            pass
        else:
            raise AssertionError("non-minimized answer credentials must be rejected")


def test_official_execution_fails_closed_while_public_web_boundary_is_unresolved() -> None:
    assert OFFICIAL_WEB_BOUNDARY_VALIDATED is False
    try:
        runner.execute_batch(
            questions_path=Path("unused"),
            arms_path=Path("unused"),
            registry_path=Path("unused"),
            sealed_gold=Path("unused"),
            response_schema=Path("unused"),
            runtime_source=Path("unused"),
            codex_package_dir=Path("unused"),
            credential_path=Path("unused"),
            output_root=Path("unused"),
            start=1,
            trial_count=1,
        )
    except RuntimeError as error:
        assert "public-history boundary" in str(error)
    else:
        raise AssertionError("unresolved native-web repository access must fail closed")


def test_batch_executor_writes_immutable_separate_trial_outputs(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(runner, "require_official_web_boundary", lambda *_: None)
    sealed = tmp_path / ".benchmark-sealed"
    sealed.mkdir()
    gold = sealed / "gold.json"
    gold.write_text("{}")
    credential = tmp_path / "auth.json"
    credential.write_text('{"fake":"credential-material"}')
    calls = []

    def fake_run_trial(**kwargs):
        calls.append(kwargs)
        kwargs["output_dir"].mkdir(mode=0o700)
        for name in ("native-trace.json", "answer.json", "observable-summary.json"):
            (kwargs["output_dir"] / name).write_text("{}")
        return {"trace": {"duration_ns": 5}}

    monkeypatch.setattr(runner, "audit_registry_packet", lambda *_: {"passed": True, "findings": []})
    monkeypatch.setattr(runner, "ephemeral_chatgpt_credential", lambda _: b'{"minimal":true}')
    monkeypatch.setattr(
        runner,
        "_validate_sealed_gold",
        lambda *_: {"adjudicated_gold_sha256": "a" * 64, "adjudicated_records": 12},
    )
    monkeypatch.setattr(runner, "_build_input_manifest", lambda **_: {"git_revision": "frozen"})
    monkeypatch.setattr(runner, "run_trial", fake_run_trial)
    arguments = {
        "questions_path": ROOT / "hspr-benchmark-v2/public/pilot_questions.json",
        "arms_path": ROOT / "hspr-benchmark-v2/config/arms.json",
        "registry_path": ROOT / "hspr-benchmark-v2/registry/pilot.identity.json",
        "sealed_gold": gold,
        "response_schema": ROOT / "hspr-benchmark-v2/config/response-schema.json",
        "runtime_source": ROOT / "hspr-benchmark-v2/runtime",
        "codex_package_dir": tmp_path / "codex",
        "credential_path": credential,
        "output_root": tmp_path / "runs",
    }
    result = runner.execute_batch(
        **arguments,
        start=1,
        trial_count=2,
    )
    assert len(result["completed"]) == len(calls) == 2
    assert all(call["allow_live_credential"] for call in calls)
    assert all(call["sealed_gold"] == gold for call in calls)
    assert all(call["credential_json"] == b'{"minimal":true}' for call in calls)
    assert len(list((tmp_path / "runs").glob("trial-*/trial-metadata.json"))) == 2
    resumed = runner.execute_batch(**arguments, start=1, trial_count=2)
    assert len(resumed["completed"]) == 2
    assert len(calls) == 2
    try:
        runner.execute_batch(**arguments, start=1, trial_count=1, attempt=2)
    except ValueError as error:
        assert "already has a completed attempt" in str(error)
    else:
        raise AssertionError("a completed schedule position must not allow another attempt")

    trial_dirs = sorted((tmp_path / "runs").glob("trial-*"))
    (trial_dirs[0] / "answer.json").write_text('{"tampered":true}')
    try:
        runner.execute_batch(**arguments, start=1, trial_count=1)
    except ValueError as error:
        assert "artifact digest mismatch" in str(error)
    else:
        raise AssertionError("tampered completed artifacts must not be accepted")
    (trial_dirs[0] / "answer.json").write_text("{}")

    metadata_path = trial_dirs[1] / "trial-metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["question_id"] = "wrong"
    metadata_path.write_text(json.dumps(metadata))
    try:
        runner.execute_batch(**arguments, start=2, trial_count=1)
    except ValueError as error:
        assert "metadata identity mismatch" in str(error)
    else:
        raise AssertionError("mismatched completed metadata must not be accepted")


def test_batch_executor_preserves_failure_and_requires_new_attempt(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(runner, "require_official_web_boundary", lambda *_: None)
    gold = tmp_path / "gold.json"
    gold.write_text("{}")
    credential = tmp_path / "auth.json"
    credential.write_text('{"fake":"credential-material"}')
    monkeypatch.setattr(runner, "audit_registry_packet", lambda *_: {"passed": True, "findings": []})
    monkeypatch.setattr(runner, "ephemeral_chatgpt_credential", lambda _: b'{"minimal":true}')
    monkeypatch.setattr(runner, "_validate_sealed_gold", lambda *_: {"adjudicated_records": 12})
    manifest = {"git_revision": "frozen"}
    monkeypatch.setattr(runner, "_build_input_manifest", lambda **_: manifest)

    def fail_trial(**kwargs):
        kwargs["output_dir"].mkdir(mode=0o700)
        (kwargs["output_dir"] / "native-trace.json").write_text("{}")
        raise RuntimeError("controlled failure")

    monkeypatch.setattr(runner, "run_trial", fail_trial)
    arguments = {
        "questions_path": ROOT / "hspr-benchmark-v2/public/pilot_questions.json",
        "arms_path": ROOT / "hspr-benchmark-v2/config/arms.json",
        "registry_path": ROOT / "hspr-benchmark-v2/registry/pilot.identity.json",
        "sealed_gold": gold,
        "response_schema": ROOT / "hspr-benchmark-v2/config/response-schema.json",
        "runtime_source": ROOT / "hspr-benchmark-v2/runtime",
        "codex_package_dir": tmp_path / "codex",
        "credential_path": credential,
        "output_root": tmp_path / "runs",
        "start": 1,
        "trial_count": 1,
    }
    try:
        runner.execute_batch(**arguments)
    except RuntimeError as error:
        assert "controlled failure" in str(error)
    else:
        raise AssertionError("controlled failure must propagate")
    assert len(list((tmp_path / "runs").glob("in-progress-*"))) == 1
    try:
        runner.execute_batch(**arguments)
    except FileExistsError as error:
        assert "higher --attempt" in str(error)
    else:
        raise AssertionError("partial attempt must not be overwritten")

    def succeed_trial(**kwargs):
        kwargs["output_dir"].mkdir(mode=0o700)
        for name in ("native-trace.json", "answer.json", "observable-summary.json"):
            (kwargs["output_dir"] / name).write_text("{}")
        return {"trace": {"duration_ns": 5}}

    monkeypatch.setattr(runner, "run_trial", succeed_trial)
    result = runner.execute_batch(**arguments, attempt=2)
    assert len(result["completed"]) == 1
    assert len(list((tmp_path / "runs").glob("trial-*-a2"))) == 1

    manifest["git_revision"] = "drifted"
    try:
        runner.execute_batch(**{**arguments, "start": 2}, attempt=1)
    except ValueError as error:
        assert "input-manifest.json" in str(error)
    else:
        raise AssertionError("input drift must stop resumed execution")


def test_runtime_events_never_invent_timestamps_or_authority() -> None:
    events = observable_events(
        '{"type":"web_search","query":"audited 10-k"}\n{"type":"web_open","timestamp":"2026-07-26T17:00:00Z"}\n'
    )
    assert events[0]["runtime_timestamp"] is None
    assert events[0]["timing_available"] is False
    assert events[1]["runtime_timestamp"] == "2026-07-26T17:00:00Z"
    assert not any("authoritative_financial_evidence" in event["event"] for event in events)


def test_packet_allowlist_and_docker_boundary(tmp_path: Path) -> None:
    packet = assemble_answer_packet(
        tmp_path / "packet",
        question={"question_id": "q"},
        response_schema=ROOT / "hspr-benchmark-v2/config/response-schema.json",
        registry_packet=ROOT / "hspr-benchmark-v2/registry/pilot.identity.json",
    )
    assert {path.name for path in packet.iterdir()} == {
        "question.json",
        "response-schema.json",
        "hspr-identity.json",
        "hspr-materialization-timing.json",
    }
    output = tmp_path / "output"
    output.mkdir()
    runtime = stage_runtime(ROOT / "hspr-benchmark-v2/runtime", tmp_path / "runtime")
    assert (runtime / "live-boundary-probe.mjs").is_file()
    command = docker_command(
        image="node:22-alpine",
        packet_dir=packet,
        output_dir=output,
        runtime_dir=runtime,
        runtime_command=("node", "/runtime/isolation-probe.mjs"),
        network="none",
    )
    rendered = " ".join(command)
    assert "--read-only" in command and "--cap-drop ALL" in rendered
    assert "/var/run/docker.sock" not in rendered
    assert ".benchmark-sealed" not in rendered
    assert str(ROOT) not in rendered


def test_answer_packet_rejects_any_sealed_named_source(tmp_path: Path) -> None:
    sealed = tmp_path / ".benchmark-sealed"
    sealed.mkdir()
    forbidden = sealed / "gold.json"
    forbidden.write_text("{}")
    try:
        assemble_answer_packet(
            tmp_path / "packet",
            question={"question_id": "q"},
            response_schema=ROOT / "hspr-benchmark-v2/config/response-schema.json",
            registry_packet=forbidden,
        )
    except ValueError as error:
        assert "sealed files are forbidden" in str(error)
    else:
        raise AssertionError("sealed-named source must never enter an answer packet")


def test_codex_command_has_stdin_supervisor_not_credential_mount(tmp_path: Path) -> None:
    for name in ("packet", "output", "runtime"):
        (tmp_path / name).mkdir()
    codex = tmp_path / "@openai/codex"
    (codex / "bin").mkdir(parents=True)
    (codex / "bin/codex.js").write_text("// test")
    (codex / "package.json").write_text('{"name":"@openai/codex","version":"0.145.0"}')
    command = codex_docker_command(
        packet_dir=tmp_path / "packet",
        output_dir=tmp_path / "output",
        runtime_dir=tmp_path / "runtime",
        codex_package_dir=codex,
        model="gpt-5.6-luna",
        reasoning="medium",
        prompt="answer the allowlisted question",
    )
    rendered = " ".join(command)
    assert "--interactive" in command
    assert "/runtime/app-server-supervisor.mjs" in command
    assert "dst=/auth" not in rendered
    assert "/auth:rw,noexec,nosuid,nodev" in rendered
    assert "OPENAI_API_KEY" not in rendered
    assert "dst=/output" not in rendered
    assert "--dangerously-bypass-approvals-and-sandbox" not in command
    assert "credential-supervisor.mjs" not in rendered
    assert "codex.js" not in rendered
    assert "node@sha256:" in rendered


def test_mount_and_host_output_guards_reject_broad_or_linked_paths(tmp_path: Path) -> None:
    packet = tmp_path / "packet"
    runtime = tmp_path / "runtime"
    output = tmp_path / "output"
    for path in (packet, runtime, output):
        path.mkdir()
    try:
        docker_command(
            image="node:22-alpine",
            packet_dir=Path("/"),
            runtime_dir=runtime,
            output_dir=output,
            runtime_command=("true",),
            network="none",
        )
    except ValueError as error:
        assert "unsafe mount" in str(error)
    else:
        raise AssertionError("filesystem root mount must be rejected")
    target = tmp_path / "target"
    target.write_text("untouched")
    link = tmp_path / "result.json"
    link.symlink_to(target)
    try:
        write_new_text(link, "overwrite")
    except FileExistsError:
        pass
    else:
        raise AssertionError("host result writer must not follow a symlink")
    assert target.read_text() == "untouched"


def test_native_trace_summary_uses_capture_times_only() -> None:
    trace = {
        "events": [
            {"event": "run_start", "monotonic_ns": 1},
            {
                "event": "codex_native_event",
                "monotonic_ns": 2,
                "payload": {
                    "type": "item.started",
                    "item": {
                        "id": "w",
                        "type": "web_search",
                        "action": "open https://example.org/a",
                    },
                },
            },
            {
                "event": "codex_native_event",
                "monotonic_ns": 3,
                "payload": {
                    "type": "item.completed",
                    "item": {
                        "id": "f",
                        "type": "agent_message",
                        "text": "https://example.org/a",
                    },
                },
            },
            {
                "event": "codex_native_event",
                "monotonic_ns": 4,
                "payload": {"type": "turn.completed", "usage": {"input_tokens": 9}},
            },
            {"event": "run_end", "monotonic_ns": 5, "exit_code": 0},
        ]
    }
    summary = summarize_codex_trace(trace, authoritative_urls=["https://example.org/a"])
    assert summary["first_authoritative_evidence_monotonic_ns"] == 2
    assert summary["last_authoritative_evidence_monotonic_ns"] == 3
    assert summary["final_answer_monotonic_ns"] == 3
    assert summary["usage"] == {"input_tokens": 9}
    assert summary["cost"] is None


def test_trial_prompt_differs_only_for_registry_instruction() -> None:
    hspr = trial_prompt(True)
    native = trial_prompt(False)
    assert "/input/hspr-identity.json" in hspr
    assert "/input/hspr-identity.json" not in native
    shared = "Retrieve the financial result now from a live authoritative primary source"
    assert shared in hspr and shared in native


def test_trial_executor_requires_explicit_live_credential_opt_in(tmp_path: Path) -> None:
    try:
        run_trial(
            question={"question_id": "q"},
            arm={"hspr_available": False, "reasoning": "medium"},
            registry_packet=tmp_path / "registry.json",
            sealed_gold=tmp_path / "gold.json",
            response_schema=tmp_path / "schema.json",
            runtime_source=tmp_path / "runtime",
            codex_package_dir=tmp_path / "codex",
            output_dir=tmp_path / "output",
            credential_json=b"{}",
        )
    except PermissionError as error:
        assert "risk-aware" in str(error)
    else:
        raise AssertionError("live credential execution must fail closed")


def test_trial_executor_blocks_direct_official_run_while_web_boundary_is_unresolved(tmp_path: Path) -> None:
    try:
        run_trial(
            question={"question_id": "q"},
            arm={"hspr_available": False, "reasoning": "medium"},
            registry_packet=tmp_path / "registry.json",
            sealed_gold=tmp_path / "gold.json",
            response_schema=tmp_path / "schema.json",
            runtime_source=tmp_path / "runtime",
            codex_package_dir=tmp_path / "codex",
            output_dir=tmp_path / "output",
            credential_json=b"{}",
            allow_live_credential=True,
        )
    except RuntimeError as error:
        assert "public-history boundary" in str(error)
    else:
        raise AssertionError("direct live trial execution must honor the unresolved web boundary")


def test_host_answer_validation_enforces_nested_schema() -> None:
    schema = ROOT / "hspr-benchmark-v2/config/response-schema.json"
    schema_document = json.loads(schema.read_text())
    assert schema_document["properties"]["units"]["properties"]["currency"]["type"] == "string"
    subtotal_units = schema_document["properties"]["closest_reported_subtotal"]["properties"]["units"]
    assert subtotal_units["properties"]["currency"]["type"] == "string"
    answer = {
        "question_id": "q",
        "answer_status": "reported",
        "reporting_perimeter": "Example Health",
        "metric_label": "Revenue",
        "period": {"type": "fiscal_year", "start": "2024-01-01", "end": "2024-12-31", "date": None},
        "units": {"currency": "USD", "scale": "ones"},
        "value": 100,
        "primary_source_url": "https://example.org/report",
        "exact_locator": "page 1",
        "caveats": [],
        "aggregated_entities": [],
        "clarification_needed": False,
        "closest_reported_subtotal": None,
    }
    _validate_answer_shape(answer, "q", schema)
    for field, invalid in (
        ("answer_status", "maybe"),
        ("period", {"type": "fiscal_year"}),
        ("units", "USD"),
    ):
        malformed = {**answer, field: invalid}
        try:
            _validate_answer_shape(malformed, "q", schema)
        except ValueError as error:
            assert "locked response schema" in str(error)
        else:
            raise AssertionError(f"invalid {field} must be rejected by the host")
