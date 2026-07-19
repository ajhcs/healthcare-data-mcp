"""Acceptance and adversarial tests for safety-net patient-mix v5."""

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
from shared.acquisition.scale_safety_net_patient_mix_contract import (
    SafetyNetPatientMixAcquisition,
    build_safety_net_patient_mix_acquisition,
)
from shared.acquisition.scale_safety_net_patient_mix_declaration import (
    COMMON_BLOCKERS,
    SAFETY_NET_INDICATOR_COLUMNS,
)
from shared.acquisition.scale_safety_net_patient_mix_evidence import (
    build_safety_net_patient_mix_public_evidence_input,
)
from shared.acquisition.scale_safety_net_patient_mix_packet import (
    acquisition,
    verify_safety_net_patient_mix_source_bytes,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import build_public_evidence_bundle, canonical_sha256

ROOT = Path(__file__).resolve().parents[1]
V5 = ROOT / "contracts" / "v5"
ACQUISITION = V5 / "fixtures" / "scale-safety-net-patient-mix-acquisition.json"
EVIDENCE = V5 / "fixtures" / "scale-safety-net-patient-mix-input.json"
SCHEMA = V5 / "scale-safety-net-patient-mix-acquisition.schema.json"
PUBLIC_SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
E2E_CACHE_ENV = "HDM_JHH_AHRQ_CACHE_ROOT"
E2E_CMS_ENV = "HDM_JHH_CMS_DSH_REPORT"


def _checked_in() -> SafetyNetPatientMixAcquisition:
    return SafetyNetPatientMixAcquisition.model_validate_json(ACQUISITION.read_text())


def test_checked_in_v5_schema_and_public_evidence_are_deterministic() -> None:
    frozen = acquisition()
    assert frozen == acquisition() == _checked_in()
    assert json.loads(ACQUISITION.read_text()) == frozen.model_dump(mode="json")
    evidence = build_safety_net_patient_mix_public_evidence_input(frozen)
    assert json.loads(EVIDENCE.read_text()) == evidence.model_dump(mode="json")
    schema = json.loads(SCHEMA.read_text())
    assert schema == SafetyNetPatientMixAcquisition.model_json_schema()
    assert list(Draft202012Validator(schema).iter_errors(frozen.model_dump(mode="json"))) == []
    bundle = build_public_evidence_bundle(evidence)
    public_schema = json.loads(PUBLIC_SCHEMA.read_text())
    assert list(Draft202012Validator(public_schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_exact_roster_schema_and_six_unavailable_cells() -> None:
    frozen = _checked_in()
    assert tuple(frozen.systems) == SYSTEM_SLUGS
    assert tuple(frozen.safety_net_indicator_columns) == SAFETY_NET_INDICATOR_COLUMNS
    assert all(name in frozen.ahrq_header_columns for name in SAFETY_NET_INDICATOR_COLUMNS)
    assert [row.row_number for row in frozen.identity_rows] == [110, 18, 466, 361, 475, 268]
    assert len(frozen.cells) == 6
    assert all(
        cell.candidate_value is None
        and cell.missingness == "unavailable_public"
        and COMMON_BLOCKERS.issubset(cell.blocker_codes)
        and not cell.imputed
        and not cell.aggregated
        and not cell.fabricated_zero
        and not cell.approved_for_scale
        for cell in frozen.cells
    )
    assert frozen.approved_numerator_receipt is frozen.approved_denominator_receipt is None


def test_source_evaluations_distinguish_flags_dpp_and_patient_mix() -> None:
    frozen = _checked_in()
    assert [item.evaluation_id for item in frozen.source_evaluations] == [
        "evaluation:ahrq-system-safety-net-schema",
        "evaluation:cms-medicare-dsh-definition",
    ]
    assert all(
        not item.common_numerator_denominator_available
        and not item.system_patient_mix_percentage_available
        and not item.usable_for_scale_input
        and item.query_sha256 == canonical_sha256(item.query)
        for item in frozen.source_evaluations
    )
    cms = frozen.cms_dsh_artifact
    assert cms.payload_sha256 == "sha256:a658fb1ec185cea715dbc175b8e225c39c806da2b353f8f86b617bcd8ebf390a"
    assert cms.http_receipt.receipt_sha256 == "sha256:cb06e3e7af69f4f5c5c466ce7fd7d3e316b3bb1a17e8f48334d8fb23da0e817d"
    assert cms.rights_classification == "unknown_review_required"


def test_public_bundle_has_seven_receipts_zero_observations_and_six_open_conflicts() -> None:
    bundle = build_public_evidence_bundle(
        build_safety_net_patient_mix_public_evidence_input(_checked_in(), producer_commit="a" * 40)
    )
    assert len(bundle.entities) == 6
    assert bundle.observations == []
    assert len(bundle.sources) == 7
    assert len(bundle.input_artifacts) == 2
    assert len(bundle.coverage) == len(bundle.conflicts) == 6
    assert {item.measure_id for item in bundle.coverage} == {"safety_net_patient_mix_pct"}
    assert all(item.status == "unavailable_public" and not item.observation_refs for item in bundle.coverage)
    assert {item.status for item in bundle.conflicts} == {"open"}
    assert bundle.request.parameters["no_facility_aggregation"] is True
    assert bundle.request.parameters["no_denominator_substitution"] is True
    assert bundle.request.parameters["no_scale_score"] is True
    assert {source.access_rights for source in bundle.sources} == {"unknown_review_required"}
    with pytest.raises(ValueError, match="producer commit"):
        build_safety_net_patient_mix_public_evidence_input(_checked_in(), producer_commit="bad")


def test_prior_service_line_lineage_is_exact_and_no_go() -> None:
    prior = _checked_in().prior_cycle
    assert prior.admission_merge.replace("-", "") == "46ed66e69bcd595aa8984d2c5b48d6b0ab4f13de"
    assert prior.tracker_merge.replace("-", "") == "df429e9ab47d60025258942e88df036c389c8731"
    assert prior.cumulative_packet_sha256.endswith("6ff9754bf81441464150b0ea976b30f6")
    assert prior.terminal_status == "blocked"
    assert prior.failure_code == "human_review_required"


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("root", "systems", ["christianacare"]),
        ("root", "approved_numerator_receipt", "fabricated"),
        ("root", "approved_denominator_receipt", "fabricated"),
        ("cell", "candidate_value", 0),
        ("cell", "missingness", "not_yet_researched"),
        ("cell", "imputed", True),
        ("cell", "aggregated", True),
        ("cell", "fabricated_zero", True),
        ("cell", "approved_for_scale", True),
        ("cell", "source_period", "FY2024"),
        ("evaluation", "usable_for_scale_input", True),
        ("evaluation", "common_numerator_denominator_available", True),
        ("evaluation", "system_patient_mix_percentage_available", True),
    ],
)
def test_rejects_roster_value_aggregation_denominator_and_authority_drift(
    section: str, field: str, value: object
) -> None:
    payload = _checked_in().model_dump(mode="json")
    if section == "root":
        mutated = {**payload, field: value}
    elif section == "cell":
        cells = [dict(item) for item in payload["cells"]]
        cells[0][field] = value
        mutated = {**payload, "cells": cells}
    else:
        evaluations = [dict(item) for item in payload["source_evaluations"]]
        evaluations[0][field] = value
        mutated = {**payload, "source_evaluations": evaluations}
    with pytest.raises(ValidationError):
        build_safety_net_patient_mix_acquisition(mutated)


def test_rejects_identity_indicator_receipt_finding_and_no_go_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    rows = [dict(item) for item in payload["identity_rows"]]
    rows[0]["health_sys_id"] = "HSI99999999"
    with pytest.raises(ValidationError, match="identity substitution|exact reviewed declaration"):
        build_safety_net_patient_mix_acquisition({**payload, "identity_rows": rows})
    with pytest.raises(ValidationError, match="indicator set drift"):
        build_safety_net_patient_mix_acquisition(
            {**payload, "safety_net_indicator_columns": ["sys_incl_highdpphosp"] * 3}
        )
    artifact = {**payload["cms_dsh_artifact"], "content_length": 1}
    with pytest.raises(ValidationError):
        build_safety_net_patient_mix_acquisition({**payload, "cms_dsh_artifact": artifact})
    cells = [dict(item) for item in payload["cells"]]
    cells[0]["finding"] = "available"
    with pytest.raises(ValidationError, match="finding drift"):
        build_safety_net_patient_mix_acquisition({**payload, "cells": cells})
    with pytest.raises(ValidationError, match="prohibitions"):
        build_safety_net_patient_mix_acquisition(
            {**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]}
        )
    with pytest.raises(ValidationError):
        build_safety_net_patient_mix_acquisition({**payload, "scale_score": 1})


def test_source_byte_verifier_accepts_exact_external_custody() -> None:
    cache_setting = os.environ.get(E2E_CACHE_ENV)
    cms_setting = os.environ.get(E2E_CMS_ENV)
    if cache_setting is None or cms_setting is None:
        pytest.skip(f"set {E2E_CACHE_ENV} and {E2E_CMS_ENV} for exact custody verification")
    verify_safety_net_patient_mix_source_bytes(
        _checked_in(), Path(cache_setting), Path(cms_setting)
    )


def _mock_dsh_reader(
    *, page_count: int = 8, page_3: str = "disproportionate patient percentage total Medicare patient days total patient days",
    page_5: str = "Acute Care Hospital IPPS Worksheet S-10 uncompensated care costs",
    page_7: str = "Medicaid/non-Medicare days Total Medicare Days Total Patient Days",
) -> SimpleNamespace:
    pages = [SimpleNamespace(extract_text=lambda: "") for _ in range(page_count)]
    if page_count >= 3:
        pages[2] = SimpleNamespace(extract_text=lambda: page_3)
    if page_count >= 5:
        pages[4] = SimpleNamespace(extract_text=lambda: page_5)
    if page_count >= 7:
        pages[6] = SimpleNamespace(extract_text=lambda: page_7)
    return SimpleNamespace(pages=pages)


def test_source_byte_verifier_rejects_missing_length_hash_and_semantic_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "shared.acquisition.scale_physician_count_packet.verify_physician_count_source_bytes",
        lambda *_: None,
    )
    with pytest.raises(ValueError, match="source file missing"):
        verify_safety_net_patient_mix_source_bytes(_checked_in(), tmp_path, tmp_path / "missing.pdf")
    report = tmp_path / "report.pdf"
    report.write_bytes(b"x")
    with pytest.raises(ValueError, match="source byte drift"):
        verify_safety_net_patient_mix_source_bytes(_checked_in(), tmp_path, report)
    report.write_bytes(b"x" * _checked_in().cms_dsh_artifact.content_length)
    monkeypatch.setattr(
        "shared.acquisition.scale_safety_net_patient_mix_packet._sha256",
        lambda _: _checked_in().cms_dsh_artifact.payload_sha256,
    )
    for reader, message in [
        (_mock_dsh_reader(page_count=7), "page count drift"),
        (_mock_dsh_reader(page_3="wrong"), "numerator/denominator marker drift"),
        (_mock_dsh_reader(page_5="wrong"), "scope marker drift"),
        (_mock_dsh_reader(page_7="wrong"), "worked-example marker drift"),
    ]:
        monkeypatch.setattr(
            "shared.acquisition.scale_safety_net_patient_mix_packet.PdfReader", lambda _, reader=reader: reader
        )
        with pytest.raises(ValueError, match=message):
            verify_safety_net_patient_mix_source_bytes(_checked_in(), tmp_path, report)


def test_source_byte_verifier_accepts_mocked_exact_structure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "shared.acquisition.scale_physician_count_packet.verify_physician_count_source_bytes",
        lambda *_: None,
    )
    report = tmp_path / "report.pdf"
    report.write_bytes(b"x" * _checked_in().cms_dsh_artifact.content_length)
    monkeypatch.setattr(
        "shared.acquisition.scale_safety_net_patient_mix_packet._sha256",
        lambda _: _checked_in().cms_dsh_artifact.payload_sha256,
    )
    monkeypatch.setattr(
        "shared.acquisition.scale_safety_net_patient_mix_packet.PdfReader",
        lambda _: _mock_dsh_reader(),
    )
    verify_safety_net_patient_mix_source_bytes(_checked_in(), tmp_path, report)


def _mock_cli_preflight(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(acquisition_cli, "repository_top_level", lambda _: tmp_path)
    monkeypatch.setattr(acquisition_cli, "require_clean_repository", lambda _: None)
    monkeypatch.setattr(acquisition_cli, "require_repository_commit", lambda *_: None)
    monkeypatch.setattr(acquisition_cli, "require_outputs_outside_repository", lambda *_: None)


def test_safety_net_cli_requires_and_dispatches_exact_dsh_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_cli_preflight(monkeypatch, tmp_path)
    base = [
        "acquire_scale_input_family.py", "--family", "safety_net_patient_mix_pct",
        "--source-commit", "a" * 40, "--cache-root", str(tmp_path / "cache"),
        "--acquisition-output", str(tmp_path / "out-a.json"),
        "--evidence-output", str(tmp_path / "out-e.json"),
    ]
    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit, match="2"):
        acquisition_cli.main()
    assert "--cms-dsh-report is required" in capsys.readouterr().err

    frozen = SimpleNamespace(model_dump=Mock(return_value={"acquisition": True}))
    evidence = SimpleNamespace(model_dump=Mock(return_value={"evidence": True}))
    verify = Mock()
    build_evidence = Mock(return_value=evidence)
    write = Mock()
    monkeypatch.setattr(acquisition_cli, "safety_net_patient_mix_acquisition", Mock(return_value=frozen))
    monkeypatch.setattr(acquisition_cli, "verify_safety_net_patient_mix_source_bytes", verify)
    monkeypatch.setattr(acquisition_cli, "build_safety_net_patient_mix_public_evidence_input", build_evidence)
    monkeypatch.setattr(acquisition_cli, "write_atomic_json", write)
    report = tmp_path / "dsh.pdf"
    monkeypatch.setattr(sys, "argv", [*base, "--cms-dsh-report", str(report)])
    acquisition_cli.main()
    verify.assert_called_once_with(frozen, tmp_path / "cache", report)
    build_evidence.assert_called_once_with(frozen, producer_commit="a" * 40)


def test_json_schema_rejects_runtime_immutable_mutations() -> None:
    payload = _checked_in().model_dump(mode="json")
    schema = json.loads(SCHEMA.read_text())
    mutations = [
        {**payload, "acquired_at": "2026-07-19T01:15:08+00:00"},
        {**payload, "systems": list(reversed(payload["systems"]))},
        {**payload, "safety_net_indicator_columns": ["fabricated"] * 3},
        {**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]},
    ]
    for mutated in mutations:
        with pytest.raises(ValidationError):
            build_safety_net_patient_mix_acquisition(mutated)
        assert list(Draft202012Validator(schema).iter_errors(mutated))


def test_v1_through_v4_fixture_bytes_remain_unchanged() -> None:
    expected = {
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-acquisition.json": "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0",
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-acquisition.json": "aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a",
        ROOT / "contracts/v3/fixtures/scale-physician-count-acquisition.json": "e7964104e56b389a19540b541cc490656578aede63d2dcbcbb8ab73571b3192b",
        ROOT / "contracts/v4/fixtures/scale-service-line-count-acquisition.json": "59a1debb97e6dd3cb2cbc6ce680c996cac8dbd17050c3b55563d3c90fa1f3946",
        ROOT / "contracts/v4/fixtures/scale-service-line-count-input.json": "22321f105525f32475d395739021ba6730e4b86ab044e85b24fac639e0b265f4",
    }
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in expected} == expected


def test_two_in_memory_builds_are_byte_identical() -> None:
    first = acquisition()
    second = acquisition()
    assert json.dumps(first.model_dump(mode="json"), sort_keys=True) == json.dumps(
        second.model_dump(mode="json"), sort_keys=True
    )
    assert canonical_sha256(build_safety_net_patient_mix_public_evidence_input(first).model_dump(mode="json")) == canonical_sha256(
        build_safety_net_patient_mix_public_evidence_input(second).model_dump(mode="json")
    )
