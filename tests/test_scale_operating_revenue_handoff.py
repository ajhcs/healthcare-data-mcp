"""Acceptance and adversarial tests for the first Scale input-family cycle."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from shared.acquisition.scale_input_family import (
    SYSTEM_SLUGS,
    ScaleInputFamilyAcquisition,
    build_acquisition,
    build_public_evidence_input,
    require_clean_repository,
    require_outputs_outside_repository,
    require_repository_commit,
    repository_top_level,
    verify_source_bytes,
)
from shared.acquisition.scale_operating_revenue_packet import acquisition
from shared.contracts.public_evidence import build_public_evidence_bundle

ROOT = Path(__file__).resolve().parents[1]
ACQUISITION = ROOT / "contracts" / "v1" / "fixtures" / "scale-operating-revenue-acquisition.json"
EVIDENCE = ROOT / "contracts" / "v1" / "fixtures" / "scale-operating-revenue-input.json"
SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"


def _checked_in() -> ScaleInputFamilyAcquisition:
    return ScaleInputFamilyAcquisition.model_validate_json(ACQUISITION.read_text(encoding="utf-8"))


def test_checked_in_acquisition_and_evidence_are_deterministic() -> None:
    expected = acquisition()
    assert expected == acquisition() == _checked_in()
    evidence = build_public_evidence_input(expected)
    assert evidence.producer.commit == "0" * 40
    assert json.loads(EVIDENCE.read_text(encoding="utf-8")) == evidence.model_dump(mode="json")

    bound = build_public_evidence_input(expected, producer_commit="a" * 40)
    assert bound.producer.commit == "a" * 40
    with pytest.raises(ValueError, match="producer commit"):
        build_public_evidence_input(expected, producer_commit="not-a-commit")


def test_all_six_rows_remain_blocked_non_imputed_and_non_approved() -> None:
    frozen = _checked_in()
    assert tuple(frozen.systems) == SYSTEM_SLUGS
    assert [item.system_slug for item in frozen.candidates] == list(SYSTEM_SLUGS)
    assert {item.missingness for item in frozen.candidates} == {"blocked_source_conflict"}
    assert all(not item.imputed and not item.approved_for_scale for item in frozen.candidates)
    assert sum(item.candidate_value is not None for item in frozen.candidates) == 4
    assert frozen.candidates[2].system_slug == "temple-health"
    assert frozen.candidates[2].candidate_value is None
    assert frozen.source_artifacts[2].audit_status == "unaudited"
    assert "unaudited_source" in frozen.candidates[2].blocker_codes
    assert frozen.candidates[4].system_slug == "cooper-university-health-care"
    assert frozen.candidates[4].candidate_value is None


def test_public_bundle_preserves_candidates_as_distinct_from_input_coverage() -> None:
    evidence_input = build_public_evidence_input(_checked_in())
    bundle = build_public_evidence_bundle(evidence_input)
    assert len(bundle.entities) == 6
    assert len(bundle.observations) == 4
    assert len(bundle.sources) == 6
    assert len(bundle.coverage) == 6
    assert len(bundle.conflicts) == 6
    assert all(item.measure_id.startswith("source_local_candidate.") for item in bundle.observations)
    assert {item.measure_id for item in bundle.coverage} == {"operating_revenue_usd"}
    assert all(item.status == "blocked_source_conflict" and not item.observation_refs for item in bundle.coverage)
    assert {item.status for item in bundle.conflicts} == {"open"}
    assert bundle.request.parameters["no_scale_score"] is True
    assert bundle.request.parameters["candidate_values_are_not_approved_inputs"] is True

    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_contract_rejects_roster_narrowing_hash_drift_and_imputation() -> None:
    payload = _checked_in().model_dump(mode="json")

    narrowed = {**payload, "systems": payload["systems"][:-1]}
    with pytest.raises(ValidationError, match="six-system order"):
        ScaleInputFamilyAcquisition.model_validate(narrowed)

    drifted = {**payload, "producer_version": "fabricated"}
    with pytest.raises(ValidationError, match="acquisition_sha256"):
        ScaleInputFamilyAcquisition.model_validate(drifted)

    rows = [dict(item) for item in payload["candidates"]]
    rows[0]["imputed"] = True
    with pytest.raises(ValidationError):
        build_acquisition({**payload, "candidates": rows})


def test_contract_rejects_weakened_no_execution_and_cross_system_receipts() -> None:
    payload = _checked_in().model_dump(mode="json")
    with pytest.raises(ValidationError, match="prohibitions"):
        build_acquisition({**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]})

    rows = [dict(item) for item in payload["candidates"]]
    rows[0]["source_artifact_refs"] = rows[1]["source_artifact_refs"]
    with pytest.raises(ValidationError, match="borrow"):
        build_acquisition({**payload, "candidates": rows})


def test_frozen_cache_verification_rejects_success_and_blocked_byte_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _checked_in().model_dump(mode="json")
    run_root = tmp_path / payload["workflow_id"] / payload["acquisition_id"]
    run_root.mkdir(parents=True)
    artifacts = [dict(item) for item in payload["source_artifacts"]]
    for artifact in artifacts:
        raw = str(artifact["artifact_id"]).encode()
        (run_root / str(artifact["artifact_id"])).write_bytes(raw)
        artifact["content_length"] = len(raw)
        artifact["payload_sha256"] = f"sha256:{hashlib.sha256(raw).hexdigest()}"
    frozen = build_acquisition({**payload, "source_artifacts": artifacts})

    row_text = {
        item.system_slug: " ".join(
            (
                item.extraction.period_marker,
                item.extraction.units_marker,
                item.extraction.basis_marker,
                item.extraction.definition_marker,
                item.extraction.row_pattern.replace(r"\s+", " ")
                .replace("(?P<value>", "")
                .replace(")", ""),
            )
        )
        for item in frozen.candidates
        if item.extraction is not None
    }

    class _Page:
        def __init__(self, text: str) -> None:
            self._text = text

        def extract_text(self) -> str:
            return self._text

    class _Reader:
        def __init__(self, stream: io.BytesIO) -> None:
            raw = stream.read().decode()
            slug = next(item.system_slug for item in frozen.candidates if item.system_slug in raw)
            candidate = next(item for item in frozen.candidates if item.system_slug == slug)
            count = candidate.extraction.page_number if candidate.extraction is not None else 1
            text = row_text.get(slug, "")
            self.pages = [_Page(text) for _ in range(count)]

    monkeypatch.setattr("shared.acquisition.scale_input_family.PdfReader", _Reader)
    verify_source_bytes(frozen, tmp_path)

    first = next(item for item in artifacts if item["custody_state"] == "frozen_verified")
    (run_root / str(first["artifact_id"])).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="byte drift"):
        verify_source_bytes(frozen, tmp_path)

    original = str(first["artifact_id"]).encode()
    (run_root / str(first["artifact_id"])).write_bytes(original)
    blocked = next(item for item in artifacts if item["custody_state"] == "blocked_http_response")
    (run_root / str(blocked["artifact_id"])).write_bytes(b"tampered")
    with pytest.raises(ValueError, match="byte drift"):
        verify_source_bytes(frozen, tmp_path)


def test_rebuild_rejects_dirty_source_and_in_repository_outputs(tmp_path: Path) -> None:
    repository = tmp_path / "source"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.org"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repository, check=True)
    marker = repository / "tracked.txt"
    marker.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "test"], cwd=repository, check=True)

    assert repository_top_level(repository / "tracked.txt") == repository.resolve()
    require_clean_repository(repository)
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    require_repository_commit(repository, head)
    with pytest.raises(ValueError, match="commit drift"):
        require_repository_commit(repository, "0" * 40)
    require_outputs_outside_repository(repository, [tmp_path / "output.json"])
    with pytest.raises(ValueError, match="outside"):
        require_outputs_outside_repository(repository, [repository / "output.json"])

    marker.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean Git"):
        require_clean_repository(repository)
    subprocess.run(["git", "restore", "tracked.txt"], cwd=repository, check=True)

    marker.write_text("staged\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repository, check=True)
    with pytest.raises(ValueError, match="clean Git"):
        require_clean_repository(repository)
    subprocess.run(["git", "restore", "--staged", "tracked.txt"], cwd=repository, check=True)
    subprocess.run(["git", "restore", "tracked.txt"], cwd=repository, check=True)

    untracked = repository / "untracked.txt"
    untracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean Git"):
        require_clean_repository(repository)
    untracked.unlink()


def test_extraction_contract_rejects_definition_period_basis_and_value_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    for field, value in (
        ("definition", "fabricated definition"),
        ("basis", "fabricated boundary"),
        ("source_period", "FY2099"),
    ):
        rows = [dict(item) for item in payload["candidates"]]
        rows[0][field] = value
        with pytest.raises(ValidationError):
            build_acquisition({**payload, "candidates": rows})

    rows = [dict(item) for item in payload["candidates"]]
    rows[0]["candidate_value"] = 0
    with pytest.raises(ValidationError, match="raw value and scale"):
        build_acquisition({**payload, "candidates": rows})
