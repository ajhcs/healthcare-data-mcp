"""Acceptance and adversarial tests for essential-service designation v7."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from jsonschema import Draft202012Validator
from pydantic import ValidationError
import pytest

import scripts.acquire_scale_input_family as acquisition_cli

from shared.acquisition.scale_essential_service_designation_count_contract import (
    EssentialServiceDesignationCountAcquisition,
    build_essential_service_designation_count_acquisition,
)
from shared.acquisition.scale_essential_service_designation_count_evidence import (
    build_essential_service_designation_count_public_evidence_input,
)
from shared.acquisition.scale_essential_service_designation_count_packet import (
    acquisition,
    verify_essential_service_designation_count_source_bytes,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import build_public_evidence_bundle, canonical_sha256

ROOT = Path(__file__).resolve().parents[1]
V7 = ROOT / "contracts/v7"
ACQUISITION = V7 / "fixtures/scale-essential-service-designation-count-acquisition.json"
EVIDENCE = V7 / "fixtures/scale-essential-service-designation-count-input.json"
SCHEMA = V7 / "scale-essential-service-designation-count-acquisition.schema.json"
E2E_PATH_ENVS = (
    "HDM_JHH_AHRQ_CACHE_ROOT", "HDM_SCALE_AHRQ_LINKAGE",
    "HDM_SCALE_CMS_PSF_ZIP", "HDM_SCALE_CMS_PROVIDER_TYPE_MANUAL",
    "HDM_SCALE_CMS_PSF_RELEASE_PAGE",
)


def checked_in() -> EssentialServiceDesignationCountAcquisition:
    return EssentialServiceDesignationCountAcquisition.model_validate_json(ACQUISITION.read_text())


def mutate(section: str, field: str, value: object) -> dict[str, object]:
    payload = checked_in().model_dump(mode="json")
    if section == "root":
        return {**payload, field: value}
    plural = {"cell": "cells", "evaluation": "source_evaluations", "artifact": "source_artifacts"}[section]
    values = [dict(item) for item in payload[plural]]
    values[0][field] = value
    return {**payload, plural: values}


def test_v7_runtime_schema_and_fixtures_are_exact() -> None:
    frozen = acquisition()
    assert frozen == checked_in()
    assert json.loads(ACQUISITION.read_text()) == frozen.model_dump(mode="json")
    evidence = build_essential_service_designation_count_public_evidence_input(frozen)
    assert json.loads(EVIDENCE.read_text()) == evidence.model_dump(mode="json")
    schema = json.loads(SCHEMA.read_text())
    assert schema == EssentialServiceDesignationCountAcquisition.model_json_schema()
    assert not list(Draft202012Validator(schema).iter_errors(frozen.model_dump(mode="json")))


def test_exact_six_missing_cells_and_no_counting() -> None:
    frozen = checked_in()
    assert frozen.systems == SYSTEM_SLUGS
    assert len(frozen.cells) == 6
    assert all(
        cell.candidate_value is None
        and cell.missingness == "unavailable_public"
        and not cell.provider_type_aggregated
        and not cell.combination_codes_expanded
        and not cell.combination_codes_deduplicated
        and not cell.stale_ahrq_rollup_used
        and not cell.expired_or_terminated_included
        and not cell.state_federal_mixed
        and not cell.narrative_substitution_used
        and not cell.missing_as_zero
        and not cell.imputed
        and not cell.fabricated_zero
        and not cell.approved_for_scale
        for cell in frozen.cells
    )
    assert frozen.approved_designation_taxonomy_receipt is None
    assert frozen.approved_facility_system_crosswalk_receipt is None


def test_public_bundle_has_ordered_ten_receipts_and_six_open_conflicts() -> None:
    bundle = build_public_evidence_bundle(
        build_essential_service_designation_count_public_evidence_input(checked_in(), producer_commit="a" * 40)
    )
    assert len(bundle.entities) == 6
    assert bundle.observations == []
    assert len(bundle.sources) == 10
    assert len(bundle.coverage) == len(bundle.conflicts) == 6
    assert all(item.status == "unavailable_public" and not item.observation_refs for item in bundle.coverage)
    assert all(item.status == "open" and len(item.receipt_refs) == 10 for item in bundle.conflicts)
    receipts = [item.receipt.receipt_id for item in bundle.sources]
    assert receipts[-4:] == [
        "receipt:all-six:essential-service-designation-count:ahrq-hospital-linkage-2023",
        "receipt:all-six:essential-service-designation-count:cms-psf-april-2026",
        "receipt:all-six:essential-service-designation-count:cms-psf-manual-rev-13757",
        "receipt:all-six:essential-service-designation-count:cms-psf-release-page-april-2026",
    ]
    assert [item.conflict_id for item in bundle.conflicts] == [
        f"conflict:{slug}:essential-service-designation-count:taxonomy-period-boundary"
        for slug in SYSTEM_SLUGS
    ]


def test_every_source_evaluation_is_non_countable() -> None:
    assert all(
        not item.reports_system_count and not item.approved_taxonomy
        and not item.approved_current_crosswalk
        and not item.provider_type_aggregation_performed
        and not item.usable_for_scale_input
        for item in checked_in().source_evaluations
    )


def test_http_receipt_timestamps_are_exact_and_acquisition_follows_retrieval() -> None:
    frozen = checked_in()
    by_id = {item.artifact_id: item for item in frozen.source_artifacts}
    assert by_id["artifact:cms:psf-parquet-april-2026"].retrieved_at == "2026-07-19T09:04:42Z"
    assert by_id["artifact:cms:claims-processing-manual-ch3:rev-13757"].retrieved_at == "2026-07-19T09:04:42Z"
    page = by_id["artifact:cms:psf-release-page:april-2026"]
    assert page.retrieved_at == "2026-07-19T09:04:43Z"
    assert page.source_modified == "2026-07-19T04:20:34Z"
    assert frozen.acquired_at.isoformat() == "2026-07-19T09:04:43+00:00"


@pytest.mark.parametrize(("section", "field", "value"), [
    ("root", "systems", ["christianacare"]),
    ("root", "approved_designation_taxonomy_receipt", "fabricated"),
    ("root", "approved_facility_system_crosswalk_receipt", "fabricated"),
    ("root", "prohibited_outputs", ["scale_score"]),
    ("cell", "candidate_value", 0), ("cell", "source_period", "2026-04"),
    ("cell", "missingness", "not_yet_researched"),
    ("cell", "provider_type_aggregated", True),
    ("cell", "combination_codes_expanded", True),
    ("cell", "combination_codes_deduplicated", True),
    ("cell", "stale_ahrq_rollup_used", True),
    ("cell", "expired_or_terminated_included", True),
    ("cell", "state_federal_mixed", True),
    ("cell", "narrative_substitution_used", True),
    ("cell", "missing_as_zero", True), ("cell", "imputed", True),
    ("cell", "fabricated_zero", True), ("cell", "approved_for_scale", True),
    ("evaluation", "approved_taxonomy", True),
    ("evaluation", "approved_current_crosswalk", True),
    ("evaluation", "provider_type_aggregation_performed", True),
    ("evaluation", "usable_for_scale_input", True),
    ("artifact", "payload_sha256", "sha256:" + "a" * 64),
])
def test_rejects_fabricated_authority_counting_and_drift(section: str, field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        build_essential_service_designation_count_acquisition(mutate(section, field, value))


def test_prior_emergency_department_lineage_is_exact_and_blocked() -> None:
    prior = checked_in().prior_cycle
    assert prior.data_merge.replace("-", "") == "ec350c6a0b4ed62aefc9c6e5e1be0a0c0e6b5f62"  # pragma: allowlist secret
    assert prior.admission_merge.replace("-", "") == "c4adbb0444ffac141247a170dd03a538a80855d3"  # pragma: allowlist secret
    assert prior.cumulative_packet_sha256 == "sha256:7679f9a26936cf5508e1909ceec974f1025eaf7212c5ac844bc2a21ff5d8551e"
    assert prior.terminal_status == "blocked" and prior.failure_code == "human_review_required"


def test_self_hash_and_deep_freeze() -> None:
    frozen = checked_in()
    assert frozen.acquisition_sha256 == canonical_sha256(frozen.model_dump(mode="json", exclude={"acquisition_sha256"}))
    payload = frozen.model_dump(mode="json")
    payload["acquisition_sha256"] = "sha256:" + "a" * 64
    with pytest.raises(ValidationError, match="self-hash drift"):
        EssentialServiceDesignationCountAcquisition.model_validate(payload)
    with pytest.raises(ValidationError, match="frozen"):
        frozen.cells[0].finding = "mutated"
    assert isinstance(frozen.cells, tuple) and isinstance(frozen.cells[0].blocker_codes, tuple)


def test_v1_through_v6_prior_fixture_bytes_remain_unchanged() -> None:
    expected = {
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-acquisition.json": "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0",  # pragma: allowlist secret
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-acquisition.json": "aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a",  # pragma: allowlist secret
        ROOT / "contracts/v3/fixtures/scale-physician-count-acquisition.json": "e7964104e56b389a19540b541cc490656578aede63d2dcbcbb8ab73571b3192b",  # pragma: allowlist secret
        ROOT / "contracts/v4/fixtures/scale-service-line-count-acquisition.json": "59a1debb97e6dd3cb2cbc6ce680c996cac8dbd17050c3b55563d3c90fa1f3946",  # pragma: allowlist secret
        ROOT / "contracts/v5/fixtures/scale-safety-net-patient-mix-acquisition.json": "ea349d7b65bc0c44912b2dccecf87fed9cb173164a40dbfccd7b6351f1804288",  # pragma: allowlist secret
        ROOT / "contracts/v6/fixtures/scale-emergency-department-count-acquisition.json": "56f638d8fab0e0c769646a424f25bafb7107898f4ef7f7e8ec11e3440f3f5dd1",  # pragma: allowlist secret
        ROOT / "contracts/v6/fixtures/scale-emergency-department-count-input.json": "d1057779b813516f5e8df880c17f862ea0b32c75ef2cbea0052f9ad6f8b0a2bd",  # pragma: allowlist secret
    }
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in expected} == expected


def test_two_isolated_builds_are_byte_identical() -> None:
    first, second = acquisition(), acquisition()
    assert json.dumps(first.model_dump(mode="json"), sort_keys=True) == json.dumps(second.model_dump(mode="json"), sort_keys=True)
    assert canonical_sha256(build_essential_service_designation_count_public_evidence_input(first).model_dump(mode="json")) == canonical_sha256(build_essential_service_designation_count_public_evidence_input(second).model_dump(mode="json"))


def test_source_byte_verifier_accepts_exact_external_custody_when_configured() -> None:
    values = [os.environ.get(name) for name in E2E_PATH_ENVS]
    if any(value is None for value in values):
        pytest.skip("set all essential-service designation custody variables")
    verify_essential_service_designation_count_source_bytes(
        checked_in(), *(Path(value) for value in values if value is not None)
    )


def test_cli_requires_and_dispatches_all_four_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(acquisition_cli, "repository_top_level", lambda _: tmp_path)
    monkeypatch.setattr(acquisition_cli, "require_clean_repository", lambda _: None)
    monkeypatch.setattr(acquisition_cli, "require_repository_commit", lambda *_: None)
    monkeypatch.setattr(acquisition_cli, "require_outputs_outside_repository", lambda *_: None)
    base = [
        "acquire_scale_input_family.py", "--family", "essential_service_designation_count",
        "--source-commit", "a" * 40, "--cache-root", str(tmp_path / "cache"),
        "--acquisition-output", str(tmp_path / "a.json"),
        "--evidence-output", str(tmp_path / "e.json"),
    ]
    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit, match="2"):
        acquisition_cli.main()
    assert "--cms-psf-zip" in capsys.readouterr().err

    frozen = SimpleNamespace(model_dump=Mock(return_value={"acquisition": True}))
    evidence = SimpleNamespace(model_dump=Mock(return_value={"evidence": True}))
    verify, build, write = Mock(), Mock(return_value=evidence), Mock()
    monkeypatch.setattr(acquisition_cli, "essential_service_designation_count_acquisition", Mock(return_value=frozen))
    monkeypatch.setattr(acquisition_cli, "verify_essential_service_designation_count_source_bytes", verify)
    monkeypatch.setattr(acquisition_cli, "build_essential_service_designation_count_public_evidence_input", build)
    monkeypatch.setattr(acquisition_cli, "write_atomic_json", write)
    sources = [tmp_path / name for name in ("link.csv", "psf.zip", "manual.pdf", "release.html")]
    monkeypatch.setattr(sys, "argv", [
        *base, "--ahrq-linkage", str(sources[0]), "--cms-psf-zip", str(sources[1]),
        "--cms-provider-type-manual", str(sources[2]), "--cms-psf-release-page", str(sources[3]),
    ])
    acquisition_cli.main()
    verify.assert_called_once_with(frozen, tmp_path / "cache", *sources)
    build.assert_called_once_with(frozen, producer_commit="a" * 40)
    assert write.call_count == 2
