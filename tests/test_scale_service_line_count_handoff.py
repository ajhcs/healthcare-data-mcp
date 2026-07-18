"""Acceptance and adversarial tests for service-line-count acquisition v4."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from shared.acquisition.scale_service_line_count_contract import (
    ServiceLineCountAcquisition,
    build_service_line_count_acquisition,
)
from shared.acquisition.scale_service_line_count_declaration import (
    AHRQ_HEADER_COLUMNS,
    COMMON_BLOCKERS,
)
from shared.acquisition.scale_service_line_count_evidence import (
    build_service_line_count_public_evidence_input,
)
from shared.acquisition.scale_service_line_count_packet import (
    acquisition,
    verify_service_line_count_source_bytes,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import build_public_evidence_bundle, canonical_sha256
import scripts.acquire_scale_input_family as acquisition_cli

ROOT = Path(__file__).resolve().parents[1]
V4 = ROOT / "contracts" / "v4"
ACQUISITION = V4 / "fixtures" / "scale-service-line-count-acquisition.json"
EVIDENCE = V4 / "fixtures" / "scale-service-line-count-input.json"
SCHEMA = V4 / "scale-service-line-count-acquisition.schema.json"
PUBLIC_SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
E2E_CACHE_ENV = "HDM_KH4_AHRQ_CACHE_ROOT"
E2E_CMS_ENV = "HDM_KH4_CMS_RBCS_REPORT"


def _checked_in() -> ServiceLineCountAcquisition:
    return ServiceLineCountAcquisition.model_validate_json(ACQUISITION.read_text(encoding="utf-8"))


def test_checked_in_v4_schema_and_public_evidence_are_deterministic() -> None:
    frozen = acquisition()
    assert frozen == acquisition() == _checked_in()
    assert json.loads(ACQUISITION.read_text(encoding="utf-8")) == frozen.model_dump(mode="json")
    evidence = build_service_line_count_public_evidence_input(frozen)
    assert json.loads(EVIDENCE.read_text(encoding="utf-8")) == evidence.model_dump(mode="json")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert schema == ServiceLineCountAcquisition.model_json_schema()
    assert list(Draft202012Validator(schema).iter_errors(frozen.model_dump(mode="json"))) == []
    bundle = build_public_evidence_bundle(evidence)
    public_schema = json.loads(PUBLIC_SCHEMA.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(public_schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_exact_sources_roster_and_six_unavailable_cells() -> None:
    frozen = _checked_in()
    assert tuple(frozen.systems) == SYSTEM_SLUGS
    assert tuple(frozen.ahrq_header_columns) == AHRQ_HEADER_COLUMNS
    assert not any("service_line" in column for column in frozen.ahrq_header_columns)
    assert [row.row_number for row in frozen.identity_rows] == [110, 18, 466, 361, 475, 268]
    assert [row.health_sys_id for row in frozen.identity_rows] == [
        "HSI00000218", "HSI00000048", "HSI00001065",
        "HSI00000820", "HSI00001079", "HSI00000608",
    ]
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
    assert frozen.common_taxonomy_receipt is None
    assert frozen.hand_counted_marketing_pages is False


def test_source_evaluations_are_exact_and_do_not_misuse_rbcs() -> None:
    frozen = _checked_in()
    assert [item.evaluation_id for item in frozen.source_evaluations] == [
        "evaluation:ahrq-system-header", "evaluation:cms-rbcs-taxonomy"
    ]
    assert all(
        not item.common_service_taxonomy_available
        and not item.system_service_line_count_available
        and not item.usable_for_scale_input
        and item.query_sha256 == canonical_sha256(item.query)
        for item in frozen.source_evaluations
    )
    assert frozen.cms_taxonomy_artifact.payload_sha256 == "sha256:68ac55dcc2812c6d692134dec827ffc5056f60b5ddcf605575fb6f2025b193e4"
    assert frozen.cms_taxonomy_artifact.http_receipt.receipt_sha256 == "sha256:20ce1b137bd38903bc6a3df8944008ea04c9425186f273feb0401a557f4ea033"


def test_public_bundle_has_zero_observations_and_six_open_conflicts() -> None:
    bundle = build_public_evidence_bundle(
        build_service_line_count_public_evidence_input(_checked_in(), producer_commit="a" * 40)
    )
    assert len(bundle.entities) == 6
    assert bundle.observations == []
    assert len(bundle.sources) == 7
    assert len(bundle.input_artifacts) == 2
    assert len(bundle.coverage) == len(bundle.conflicts) == 6
    assert {item.measure_id for item in bundle.coverage} == {"service_line_count"}
    assert all(item.status == "unavailable_public" and not item.observation_refs for item in bundle.coverage)
    assert {item.status for item in bundle.conflicts} == {"open"}
    assert bundle.request.parameters["no_scale_score"] is True
    assert bundle.request.parameters["no_service_line_hand_count"] is True
    assert bundle.request.parameters["no_claims_aggregation"] is True
    with pytest.raises(ValueError, match="producer commit"):
        build_service_line_count_public_evidence_input(_checked_in(), producer_commit="bad")


def test_prior_physician_lineage_is_exact_and_no_go() -> None:
    prior = _checked_in().prior_cycle
    assert prior.binding_merge.replace("-", "") == "581265a2f2c80f71832b87de787b8b93e3ac8b1c"  # pragma: allowlist secret
    assert prior.admission_merge.replace("-", "") == "cc3ccb3d26e44d410546003b7dec073a2b74ab17"  # pragma: allowlist secret
    assert prior.cumulative_packet_sha256.endswith("bb20fec4810464d1b7efa3d67a07ea119537cbbed9aa5")  # pragma: allowlist secret
    assert prior.terminal_status == "blocked"
    assert prior.failure_code == "human_review_required"


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("root", "systems", ["christianacare"]),
        ("root", "common_taxonomy_receipt", "fabricated"),
        ("root", "hand_counted_marketing_pages", True),
        ("cell", "candidate_value", 12),
        ("cell", "missingness", "populated"),
        ("cell", "imputed", True),
        ("cell", "aggregated", True),
        ("cell", "fabricated_zero", True),
        ("cell", "approved_for_scale", True),
        ("cell", "source_period", "2025"),
        ("evaluation", "usable_for_scale_input", True),
        ("evaluation", "common_service_taxonomy_available", True),
    ],
)
def test_rejects_roster_taxonomy_value_imputation_and_authority_drift(
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
        build_service_line_count_acquisition(mutated)


def test_rejects_identity_receipt_query_finding_and_no_go_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    rows = [dict(item) for item in payload["identity_rows"]]
    rows[0]["health_sys_id"] = "HSI99999999"
    with pytest.raises(ValidationError, match="identity substitution|exact reviewed declaration"):
        build_service_line_count_acquisition({**payload, "identity_rows": rows})

    artifact = {**payload["cms_taxonomy_artifact"], "content_length": 1}
    with pytest.raises(ValidationError):
        build_service_line_count_acquisition({**payload, "cms_taxonomy_artifact": artifact})

    evaluations = [dict(item) for item in payload["source_evaluations"]]
    evaluations[0]["query"] = "hand count marketing pages"
    evaluations[0]["query_sha256"] = canonical_sha256(evaluations[0]["query"])
    with pytest.raises(ValidationError, match="source evaluation meaning drift"):
        build_service_line_count_acquisition({**payload, "source_evaluations": evaluations})

    cells = [dict(item) for item in payload["cells"]]
    cells[0]["finding"] = "available"
    with pytest.raises(ValidationError, match="finding drift"):
        build_service_line_count_acquisition({**payload, "cells": cells})

    with pytest.raises(ValidationError, match="prohibitions"):
        build_service_line_count_acquisition(
            {**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]}
        )
    with pytest.raises(ValidationError):
        build_service_line_count_acquisition({**payload, "scale_score": 1})


def test_source_byte_verifier_accepts_only_exact_receipts(tmp_path: Path) -> None:
    cache_setting = os.environ.get(E2E_CACHE_ENV)
    cms_setting = os.environ.get(E2E_CMS_ENV)
    if cache_setting is None or cms_setting is None:
        pytest.skip(
            f"set {E2E_CACHE_ENV} and {E2E_CMS_ENV} to run exact external-custody verification"
        )
    cache_root = Path(cache_setting)
    cms_report = Path(cms_setting)
    verify_service_line_count_source_bytes(_checked_in(), cache_root, cms_report)
    mutated = tmp_path / "rbcs.pdf"
    raw = bytearray(cms_report.read_bytes())
    raw[-1] ^= 1
    mutated.write_bytes(raw)
    with pytest.raises(ValueError, match="CMS RBCS source byte drift"):
        verify_service_line_count_source_bytes(_checked_in(), cache_root, mutated)


def _unit_cache_with_exact_ahrq_header(tmp_path: Path) -> Path:
    cache_root = tmp_path / "cache"
    source = cache_root / "source.csv"
    source.parent.mkdir(parents=True)
    source.write_text(",".join(AHRQ_HEADER_COLUMNS) + "\n", encoding="cp1252")
    manifest = cache_root / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        json.dumps(
            {
                "artifacts": [
                    {"relative_path": "ahrq_system_2023.csv", "path": str(source)}
                ]
            }
        ),
        encoding="utf-8",
    )
    return cache_root


def _mock_rbcs_reader(*, page_count: int = 48, page_8: str = "HCPCS Code Dictionary Medicare Part B", page_22: str = "Data Limitations Medicare Part B fee-for-service claims") -> SimpleNamespace:
    pages = [SimpleNamespace(extract_text=lambda: "") for _ in range(page_count)]
    if page_count >= 8:
        pages[7] = SimpleNamespace(extract_text=lambda: page_8)
    if page_count >= 22:
        pages[21] = SimpleNamespace(extract_text=lambda: page_22)
    return SimpleNamespace(pages=pages)


def test_source_byte_verifier_rejects_missing_length_and_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_root = _unit_cache_with_exact_ahrq_header(tmp_path)
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet.verify_physician_count_source_bytes",
        lambda *_: None,
    )
    with pytest.raises(ValueError, match="source file missing"):
        verify_service_line_count_source_bytes(
            _checked_in(), cache_root, tmp_path / "missing.pdf"
        )

    wrong_length = tmp_path / "wrong-length.pdf"
    wrong_length.write_bytes(b"not the report")
    with pytest.raises(ValueError, match="source byte drift"):
        verify_service_line_count_source_bytes(_checked_in(), cache_root, wrong_length)

    wrong_hash = tmp_path / "wrong-hash.pdf"
    wrong_hash.write_bytes(b"x" * _checked_in().cms_taxonomy_artifact.content_length)
    with pytest.raises(ValueError, match="source byte drift"):
        verify_service_line_count_source_bytes(_checked_in(), cache_root, wrong_hash)


@pytest.mark.parametrize(
    ("reader", "message"),
    [
        (_mock_rbcs_reader(page_count=47), "page count drift"),
        (_mock_rbcs_reader(page_8="not the taxonomy markers"), "taxonomy-scope marker drift"),
        (_mock_rbcs_reader(page_22="not the limitation markers"), "limitation marker drift"),
    ],
)
def test_source_byte_verifier_rejects_pdf_structure_and_markers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: SimpleNamespace,
    message: str,
) -> None:
    cache_root = _unit_cache_with_exact_ahrq_header(tmp_path)
    report = tmp_path / "report.pdf"
    report.write_bytes(b"x" * _checked_in().cms_taxonomy_artifact.content_length)
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet.verify_physician_count_source_bytes",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet._sha256",
        lambda _: _checked_in().cms_taxonomy_artifact.payload_sha256,
    )
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet.PdfReader", lambda _: reader
    )
    with pytest.raises(ValueError, match=message):
        verify_service_line_count_source_bytes(_checked_in(), cache_root, report)


def test_source_byte_verifier_accepts_mocked_exact_structure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_root = _unit_cache_with_exact_ahrq_header(tmp_path)
    report = tmp_path / "report.pdf"
    report.write_bytes(b"x" * _checked_in().cms_taxonomy_artifact.content_length)
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet.verify_physician_count_source_bytes",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet._sha256",
        lambda _: _checked_in().cms_taxonomy_artifact.payload_sha256,
    )
    monkeypatch.setattr(
        "shared.acquisition.scale_service_line_count_packet.PdfReader",
        lambda _: _mock_rbcs_reader(),
    )
    verify_service_line_count_source_bytes(_checked_in(), cache_root, report)


def test_contract_binds_cms_rights_and_http_receipt() -> None:
    payload = _checked_in().model_dump(mode="json")
    for field, value in (
        ("rights_classification", "public_domain"),
        ("rights_basis", "public URL means unrestricted reuse"),
    ):
        artifact = {**payload["cms_taxonomy_artifact"], field: value}
        with pytest.raises(ValidationError):
            build_service_line_count_acquisition(
                {**payload, "cms_taxonomy_artifact": artifact}
            )
    for field, value in (
        ("final_url", "https://data.cms.gov/fabricated.pdf"),
        ("payload_sha256", "sha256:" + "a" * 64),
        ("receipt_sha256", "sha256:" + "b" * 64),
    ):
        receipt = {**payload["cms_taxonomy_artifact"]["http_receipt"], field: value}
        artifact = {**payload["cms_taxonomy_artifact"], "http_receipt": receipt}
        with pytest.raises(ValidationError):
            build_service_line_count_acquisition(
                {**payload, "cms_taxonomy_artifact": artifact}
            )


def _mock_cli_preflight(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(acquisition_cli, "repository_top_level", lambda _: tmp_path)
    monkeypatch.setattr(acquisition_cli, "require_clean_repository", lambda _: None)
    monkeypatch.setattr(acquisition_cli, "require_repository_commit", lambda *_: None)
    monkeypatch.setattr(acquisition_cli, "require_outputs_outside_repository", lambda *_: None)


def test_service_line_cli_requires_cms_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_cli_preflight(monkeypatch, tmp_path)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "acquire_scale_input_family.py", "--family", "service_line_count",
            "--source-commit", "a" * 40, "--cache-root", str(tmp_path / "cache"),
            "--acquisition-output", str(tmp_path / "out-a.json"),
            "--evidence-output", str(tmp_path / "out-e.json"),
        ],
    )
    with pytest.raises(SystemExit, match="2"):
        acquisition_cli.main()
    assert "--cms-rbcs-report is required" in capsys.readouterr().err


def test_service_line_cli_dispatches_exact_custody_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _mock_cli_preflight(monkeypatch, tmp_path)
    frozen = SimpleNamespace(model_dump=Mock(return_value={"acquisition": True}))
    evidence = SimpleNamespace(model_dump=Mock(return_value={"evidence": True}))
    verify = Mock()
    build_evidence = Mock(return_value=evidence)
    write = Mock()
    monkeypatch.setattr(acquisition_cli, "service_line_count_acquisition", Mock(return_value=frozen))
    monkeypatch.setattr(acquisition_cli, "verify_service_line_count_source_bytes", verify)
    monkeypatch.setattr(
        acquisition_cli, "build_service_line_count_public_evidence_input", build_evidence
    )
    monkeypatch.setattr(acquisition_cli, "write_atomic_json", write)
    cache_root = tmp_path / "cache"
    cms_report = tmp_path / "rbcs.pdf"
    acquisition_output = tmp_path / "out-a.json"
    evidence_output = tmp_path / "out-e.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "acquire_scale_input_family.py", "--family", "service_line_count",
            "--source-commit", "a" * 40, "--cache-root", str(cache_root),
            "--cms-rbcs-report", str(cms_report),
            "--acquisition-output", str(acquisition_output),
            "--evidence-output", str(evidence_output),
        ],
    )
    acquisition_cli.main()
    verify.assert_called_once_with(frozen, cache_root, cms_report)
    build_evidence.assert_called_once_with(frozen, producer_commit="a" * 40)
    assert write.call_args_list[0].args == (acquisition_output, {"acquisition": True})
    assert write.call_args_list[1].args == (evidence_output, {"evidence": True})


def test_json_schema_rejects_runtime_immutable_mutations() -> None:
    payload = _checked_in().model_dump(mode="json")
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    mutations = [
        {**payload, "acquired_at": "2026-07-18T18:15:29+00:00"},
        {**payload, "systems": list(reversed(payload["systems"]))},
        {**payload, "ahrq_header_columns": [*payload["ahrq_header_columns"][:-1], "service_line_count"]},
        {**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]},
    ]
    for mutated in mutations:
        with pytest.raises(ValidationError):
            build_service_line_count_acquisition(mutated)
        assert list(Draft202012Validator(schema).iter_errors(mutated))


def test_v1_v2_v3_fixture_bytes_remain_unchanged() -> None:
    expected = {
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-acquisition.json": "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0",  # pragma: allowlist secret
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-input.json": "04fadae952898bc6dac87d0aaf4a3b04711cc9acc387ec751612f4b937b5b89f",  # pragma: allowlist secret
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-acquisition.json": "aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a",  # pragma: allowlist secret
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-input.json": "29229692c230073770d5ecbd766d385bd2b9f44eb5c6be2d8640d5480b0fc1d3",  # pragma: allowlist secret
        ROOT / "contracts/v3/fixtures/scale-physician-count-acquisition.json": "e7964104e56b389a19540b541cc490656578aede63d2dcbcbb8ab73571b3192b",  # pragma: allowlist secret
        ROOT / "contracts/v3/fixtures/scale-physician-count-input.json": "2c2734cd58f5b97cb6b73c326493c9794e3eb6fd3ded05d7f2ed503033dababa",  # pragma: allowlist secret
    }
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in expected} == expected


def test_two_in_memory_builds_are_byte_identical() -> None:
    first = acquisition()
    second = acquisition()
    assert json.dumps(first.model_dump(mode="json"), sort_keys=True) == json.dumps(
        second.model_dump(mode="json"), sort_keys=True
    )
    assert canonical_sha256(
        build_service_line_count_public_evidence_input(first).model_dump(mode="json")
    ) == canonical_sha256(
        build_service_line_count_public_evidence_input(second).model_dump(mode="json")
    )
