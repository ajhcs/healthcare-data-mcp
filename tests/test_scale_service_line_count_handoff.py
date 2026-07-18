"""Acceptance and adversarial tests for service-line-count acquisition v4."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

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

ROOT = Path(__file__).resolve().parents[1]
V4 = ROOT / "contracts" / "v4"
ACQUISITION = V4 / "fixtures" / "scale-service-line-count-acquisition.json"
EVIDENCE = V4 / "fixtures" / "scale-service-line-count-input.json"
SCHEMA = V4 / "scale-service-line-count-acquisition.schema.json"
PUBLIC_SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
VALIDATED_CACHE = Path.home() / ".healthcare-data-mcp" / "cache"
CMS_REPORT = Path("/tmp/service-line-source-wRLqSf/rbcs.pdf")


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
    assert prior.binding_merge.replace("-", "") == "581265a2f2c80f71832b87de787b8b93e3ac8b1c"
    assert prior.admission_merge.replace("-", "") == "cc3ccb3d26e44d410546003b7dec073a2b74ab17"
    assert prior.cumulative_packet_sha256.endswith("bb20fec4810464d1b7efa3d67a07ea119537cbbed9aa5")
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
    if not CMS_REPORT.exists() or not VALIDATED_CACHE.exists():
        pytest.skip("frozen external source custody is unavailable")
    verify_service_line_count_source_bytes(_checked_in(), VALIDATED_CACHE, CMS_REPORT)
    mutated = tmp_path / "rbcs.pdf"
    raw = bytearray(CMS_REPORT.read_bytes())
    raw[-1] ^= 1
    mutated.write_bytes(raw)
    with pytest.raises(ValueError, match="CMS RBCS source byte drift"):
        verify_service_line_count_source_bytes(_checked_in(), VALIDATED_CACHE, mutated)


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
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-acquisition.json": "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0",
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-input.json": "04fadae952898bc6dac87d0aaf4a3b04711cc9acc387ec751612f4b937b5b89f",
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-acquisition.json": "aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a",
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-input.json": "29229692c230073770d5ecbd766d385bd2b9f44eb5c6be2d8640d5480b0fc1d3",
        ROOT / "contracts/v3/fixtures/scale-physician-count-acquisition.json": "e7964104e56b389a19540b541cc490656578aede63d2dcbcbb8ab73571b3192b",
        ROOT / "contracts/v3/fixtures/scale-physician-count-input.json": "2c2734cd58f5b97cb6b73c326493c9794e3eb6fd3ded05d7f2ed503033dababa",
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
