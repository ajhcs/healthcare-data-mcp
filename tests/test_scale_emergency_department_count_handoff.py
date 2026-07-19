"""Acceptance and adversarial tests for emergency-department count v6."""

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
from shared.acquisition.scale_emergency_department_count_contract import (
    ED_DEFINITION,
    EVALUATION_IDS,
    EXPECTED_ARTIFACTS,
    EmergencyDepartmentCountAcquisition,
    build_emergency_department_count_acquisition,
)
from shared.acquisition.scale_emergency_department_count_declaration import (
    COMMON_BLOCKERS,
    HGI_COLUMNS,
)
from shared.acquisition.scale_emergency_department_count_evidence import (
    build_emergency_department_count_public_evidence_input,
)
from shared.acquisition.scale_emergency_department_count_packet import (
    acquisition,
    verify_emergency_department_count_source_bytes,
)
from shared.acquisition.scale_system_roster import SYSTEM_SLUGS
from shared.contracts.public_evidence import build_public_evidence_bundle, canonical_sha256

ROOT = Path(__file__).resolve().parents[1]
V6 = ROOT / "contracts" / "v6"
ACQUISITION = V6 / "fixtures" / "scale-emergency-department-count-acquisition.json"
EVIDENCE = V6 / "fixtures" / "scale-emergency-department-count-input.json"
SCHEMA = V6 / "scale-emergency-department-count-acquisition.schema.json"
PUBLIC_SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
E2E_PATH_ENVS = (
    "HDM_JHH_AHRQ_CACHE_ROOT",
    "HDM_SCALE_AHRQ_LINKAGE",
    "HDM_SCALE_CMS_HGI",
    "HDM_SCALE_CMS_HGI_METADATA",
    "HDM_SCALE_CMS_HOSPITAL_DICTIONARY",
    "HDM_SCALE_ECFR_ED_DEFINITION",
)


def _checked_in() -> EmergencyDepartmentCountAcquisition:
    return EmergencyDepartmentCountAcquisition.model_validate_json(ACQUISITION.read_text())


def _mutate(section: str, field: str, value: object) -> dict[str, object]:
    payload = _checked_in().model_dump(mode="json")
    if section == "root":
        return {**payload, field: value}
    plural = {"cell": "cells", "evaluation": "source_evaluations", "artifact": "source_artifacts"}[section]
    values = [dict(item) for item in payload[plural]]
    values[0][field] = value
    return {**payload, plural: values}


def test_checked_in_v6_schema_and_public_evidence_are_deterministic() -> None:
    frozen = acquisition()
    assert frozen == acquisition() == _checked_in()
    assert json.loads(ACQUISITION.read_text()) == frozen.model_dump(mode="json")
    evidence = build_emergency_department_count_public_evidence_input(frozen)
    assert json.loads(EVIDENCE.read_text()) == evidence.model_dump(mode="json")
    schema = json.loads(SCHEMA.read_text())
    assert schema == EmergencyDepartmentCountAcquisition.model_json_schema()
    assert list(Draft202012Validator(schema).iter_errors(frozen.model_dump(mode="json"))) == []
    bundle = build_public_evidence_bundle(evidence)
    public_schema = json.loads(PUBLIC_SCHEMA.read_text())
    assert list(Draft202012Validator(public_schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_exact_roster_headers_and_six_unavailable_cells() -> None:
    frozen = _checked_in()
    assert tuple(frozen.systems) == SYSTEM_SLUGS
    assert len(frozen.ahrq_header_columns) == 40
    assert tuple(frozen.hgi_header_columns) == HGI_COLUMNS
    assert [row.row_number for row in frozen.identity_rows] == [110, 18, 466, 361, 475, 268]
    assert len(frozen.cells) == 6
    assert all(
        cell.candidate_value is None and cell.desired_definition == ED_DEFINITION
        and cell.missingness == "unavailable_public"
        and COMMON_BLOCKERS.issubset(cell.blocker_codes)
        and not cell.aggregated and not cell.flag_sum_used
        and not cell.campus_inference_used and not cell.missing_as_no
        and not cell.imputed and not cell.fabricated_zero
        and not cell.approved_for_scale
        for cell in frozen.cells
    )
    assert frozen.approved_department_inventory_receipt is None
    assert frozen.approved_facility_system_crosswalk_receipt is None


def test_no_candidate_aggregation_or_flag_sum_is_persisted() -> None:
    frozen = _checked_in()
    forbidden = {
        "ahrq_linked_hospital_rows", "ahrq_acute_hospital_rows",
        "hgi_yes_flags", "hgi_no_flags", "hgi_missing_rows",
    }
    assert all(
        cell.candidate_value is None
        and forbidden.isdisjoint(cell.model_fields_set)
        for cell in frozen.cells
    )


def test_sources_distinguish_system_ccn_boolean_and_regulatory_units() -> None:
    frozen = _checked_in()
    assert tuple(item.evaluation_id for item in frozen.source_evaluations) == EVALUATION_IDS
    assert [item.evaluated_unit for item in frozen.source_evaluations] == [
        "system_row", "ccn_hospital", "facility_boolean", "dataset_metadata",
        "data_dictionary", "dedicated_emergency_department",
    ]
    assert all(
        not item.reports_system_count
        and not item.enumerates_dedicated_departments
        and not item.usable_for_scale_input
        for item in frozen.source_evaluations
    )
    assert [item.artifact_id for item in frozen.source_artifacts] == list(EXPECTED_ARTIFACTS)
    assert {item.rights_classification for item in frozen.source_artifacts} == {"public_domain"}


def test_public_bundle_has_six_artifacts_eleven_receipts_and_six_open_conflicts() -> None:
    bundle = build_public_evidence_bundle(
        build_emergency_department_count_public_evidence_input(_checked_in(), producer_commit="a" * 40)
    )
    assert len(bundle.entities) == 6
    assert bundle.observations == []
    assert len(bundle.sources) == 11
    assert len(bundle.input_artifacts) == 6
    assert len(bundle.coverage) == len(bundle.conflicts) == 6
    assert {item.measure_id for item in bundle.coverage} == {"emergency_department_count"}
    assert all(item.status == "unavailable_public" and not item.observation_refs for item in bundle.coverage)
    assert {item.status for item in bundle.conflicts} == {"open"}
    assert bundle.request.parameters["no_flag_sum"] is True
    assert bundle.request.parameters["no_facility_aggregation"] is True
    assert bundle.request.parameters["no_campus_inference"] is True
    assert bundle.request.parameters["no_missing_as_no_or_zero"] is True
    assert bundle.request.parameters["no_scale_score"] is True
    prior = _checked_in().prior_cycle.model_dump(mode="json")
    parameters = bundle.request.parameters
    assert parameters["prior_safety_net_data_feature"] == prior["data_feature"]
    assert parameters["prior_safety_net_data_merge"] == prior["data_merge"]
    assert parameters["prior_safety_net_data_tracker_merge"] == prior["data_tracker_merge"]
    assert parameters["prior_safety_net_binding_merge"] == prior["binding_merge"]
    assert parameters["prior_safety_net_binding_tracker_merge"] == prior["binding_tracker_merge"]
    assert parameters["prior_safety_net_agents_review_merge"] == prior["agents_review_merge"]
    assert parameters["prior_safety_net_agents_tracker_merge"] == prior["agents_tracker_merge"]
    assert parameters["prior_safety_net_admission_merge"] == prior["admission_merge"]
    assert parameters["prior_safety_net_tracker_merge"] == prior["tracker_merge"]
    assert parameters["prior_safety_net_packet_sha256"] == prior["cumulative_packet_sha256"]
    assert parameters["prior_safety_net_review_sha256"] == prior["cumulative_review_sha256"]
    assert parameters["prior_safety_net_review_transport_sha256"] == prior["cumulative_review_transport_sha256"]
    assert parameters["prior_safety_net_assurance_sha256"] == prior["cumulative_assurance_sha256"]
    assert parameters["prior_safety_net_assurance_transport_sha256"] == prior["cumulative_assurance_transport_sha256"]
    assert parameters["prior_safety_net_manifest_sha256"] == prior["reusable_manifest_sha256"]
    assert parameters["prior_safety_net_manifest_transport_sha256"] == prior["reusable_manifest_transport_sha256"]
    assert parameters["prior_safety_net_terminal_status"] == "blocked"
    assert parameters["prior_safety_net_failure_code"] == "human_review_required"
    assert {source.access_rights for source in bundle.sources} == {
        "public_domain",
        "unknown_review_required",
    }
    assert all(len(item.receipt_refs) == 11 for item in bundle.conflicts)


def test_producer_commit_requires_exact_lowercase_sha() -> None:
    for value in ("bad", "A" * 40, "a" * 39, "sha256:" + "a" * 64):
        with pytest.raises(ValueError, match="producer commit"):
            build_emergency_department_count_public_evidence_input(_checked_in(), producer_commit=value)


def test_prior_safety_net_lineage_is_exact_and_no_go() -> None:
    prior = _checked_in().prior_cycle
    assert prior.data_merge.replace("-", "") == "50eba1efda522e875ebfb0b3feadfd80f4073a78"  # pragma: allowlist secret
    assert prior.admission_merge.replace("-", "") == "61a67481a9f8bb40e81a2f8f59061664ca5694ba"  # pragma: allowlist secret
    assert prior.tracker_merge.replace("-", "") == "01aba0aa56448f17504e91f7f9754d96eb77ee7c"  # pragma: allowlist secret
    assert prior.cumulative_packet_sha256 == "sha256:af7ac7ce87a991b227673cfa8b6d92374bd01625217e7e21835f39abb289f365"
    assert prior.reusable_manifest_sha256 == "sha256:86f148e3627f4e2b655bb3bab1c0e225ae9a5ab25399e80e2411e3b1a04991c1"
    assert prior.reusable_manifest_transport_sha256 == "sha256:b00d79b155abe12bb24535f4b3b380c17483c415d974af257432f816ed2e268e"
    assert prior.terminal_status == "blocked"
    assert prior.failure_code == "human_review_required"


@pytest.mark.parametrize("field", [
    "data_feature", "data_merge", "data_tracker_merge", "binding_merge",
    "binding_tracker_merge", "agents_review_merge", "agents_tracker_merge",
    "admission_merge", "tracker_merge", "cumulative_packet_sha256",
    "cumulative_review_sha256", "cumulative_review_transport_sha256",
    "cumulative_assurance_sha256", "cumulative_assurance_transport_sha256",
    "reusable_manifest_sha256", "reusable_manifest_transport_sha256",
])
def test_rejects_each_prior_commit_and_artifact_tuple_drift(field: str) -> None:
    payload = _checked_in().model_dump(mode="json")
    prior = dict(payload["prior_cycle"])
    prior[field] = "sha256:" + "a" * 64 if field.endswith("sha256") else "a" * 40
    with pytest.raises(ValidationError):
        build_emergency_department_count_acquisition({**payload, "prior_cycle": prior})


def test_rejects_hyphenated_commit_format_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    prior = dict(payload["prior_cycle"])
    prior["data_merge"] = f"{prior['data_merge'][:8]}-{prior['data_merge'][8:]}"
    with pytest.raises(ValidationError):
        build_emergency_department_count_acquisition({**payload, "prior_cycle": prior})


@pytest.mark.parametrize(("section", "field", "value"), [
    ("root", "systems", ["christianacare"]),
    ("root", "approved_department_inventory_receipt", "fabricated"),
    ("root", "approved_facility_system_crosswalk_receipt", "fabricated"),
    ("root", "prohibited_outputs", ["scale_score"]),
    ("cell", "candidate_value", 0), ("cell", "source_period", "FY2024"),
    ("cell", "missingness", "not_yet_researched"), ("cell", "aggregated", True),
    ("cell", "flag_sum_used", True), ("cell", "campus_inference_used", True),
    ("cell", "missing_as_no", True), ("cell", "imputed", True),
    ("cell", "fabricated_zero", True), ("cell", "approved_for_scale", True),
    ("evaluation", "reports_system_count", True),
    ("evaluation", "enumerates_dedicated_departments", True),
    ("evaluation", "usable_for_scale_input", True),
    ("evaluation", "evaluated_unit", "facility_boolean"),
    ("evaluation", "artifact_ref", "artifact:substituted"),
    ("artifact", "source_url", "https://example.invalid/substitution"),
    ("artifact", "payload_sha256", "sha256:" + "a" * 64),
])
def test_rejects_roster_aggregation_inference_imputation_and_authority_drift(
    section: str, field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        build_emergency_department_count_acquisition(_mutate(section, field, value))


def test_rejects_identity_source_graph_blocker_finding_and_extra_output_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    rows = [dict(item) for item in payload["identity_rows"]]
    rows[0]["health_sys_id"] = "HSI99999999"
    with pytest.raises(ValidationError, match="identity substitution|identity row drift"):
        build_emergency_department_count_acquisition({**payload, "identity_rows": rows})
    cells = [dict(item) for item in payload["cells"]]
    cells[0]["source_artifact_refs"] = cells[0]["source_artifact_refs"][:-1]
    with pytest.raises(ValidationError):
        build_emergency_department_count_acquisition({**payload, "cells": cells})
    cells = [dict(item) for item in payload["cells"]]
    cells[0]["blocker_codes"] = cells[0]["blocker_codes"][:-1]
    with pytest.raises(ValidationError, match="cell blocker drift"):
        build_emergency_department_count_acquisition({**payload, "cells": cells})
    cells = [dict(item) for item in payload["cells"]]
    cells[0]["finding"] = "available"
    with pytest.raises(ValidationError, match="cell finding drift"):
        build_emergency_department_count_acquisition({**payload, "cells": cells})
    with pytest.raises(ValidationError):
        build_emergency_department_count_acquisition({**payload, "scale_score": 1})


def test_self_hash_rejects_direct_transport_mutation() -> None:
    payload = _checked_in().model_dump(mode="json")
    payload["acquisition_sha256"] = "sha256:" + "a" * 64
    with pytest.raises(ValidationError, match="self-hash drift"):
        EmergencyDepartmentCountAcquisition.model_validate(payload)
    assert _checked_in().acquisition_sha256 == canonical_sha256(
        _checked_in().model_dump(mode="json", exclude={"acquisition_sha256"})
    )


def test_validated_contract_graph_rejects_post_hash_mutation() -> None:
    frozen = _checked_in()
    with pytest.raises(ValidationError, match="frozen"):
        frozen.cells[0].finding = "mutated"
    with pytest.raises(ValidationError, match="frozen"):
        frozen.cache_receipt.run_id = "mutated"
    with pytest.raises(ValidationError, match="frozen"):
        frozen.ahrq_system_artifact.source_name = "mutated"
    assert isinstance(frozen.cells, tuple)
    assert isinstance(frozen.cells[0].blocker_codes, tuple)
    assert isinstance(frozen.cells[0].source_artifact_refs, tuple)


def test_source_byte_verifier_accepts_exact_external_custody_when_configured() -> None:
    settings = [os.environ.get(name) for name in E2E_PATH_ENVS]
    if any(value is None for value in settings):
        pytest.skip("set all emergency-department source custody environment variables")
    paths = [Path(value) for value in settings if value is not None]
    verify_emergency_department_count_source_bytes(_checked_in(), *paths)


def test_source_byte_verifier_rejects_missing_external_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "shared.acquisition.scale_physician_count_packet.verify_physician_count_source_bytes",
        lambda *_: None,
    )
    missing = [tmp_path / f"missing-{index}" for index in range(5)]
    with pytest.raises(ValueError, match="source file missing"):
        verify_emergency_department_count_source_bytes(_checked_in(), tmp_path, *missing)


def _mock_cli_preflight(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(acquisition_cli, "repository_top_level", lambda _: tmp_path)
    monkeypatch.setattr(acquisition_cli, "require_clean_repository", lambda _: None)
    monkeypatch.setattr(acquisition_cli, "require_repository_commit", lambda *_: None)
    monkeypatch.setattr(acquisition_cli, "require_outputs_outside_repository", lambda *_: None)


def test_emergency_department_cli_requires_all_five_exact_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _mock_cli_preflight(monkeypatch, tmp_path)
    base = [
        "acquire_scale_input_family.py", "--family", "emergency_department_count",
        "--source-commit", "a" * 40, "--cache-root", str(tmp_path / "cache"),
        "--acquisition-output", str(tmp_path / "out-a.json"),
        "--evidence-output", str(tmp_path / "out-e.json"),
    ]
    monkeypatch.setattr(sys, "argv", base)
    with pytest.raises(SystemExit, match="2"):
        acquisition_cli.main()
    assert "--ahrq-linkage" in capsys.readouterr().err


def test_emergency_department_cli_dispatches_exact_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _mock_cli_preflight(monkeypatch, tmp_path)
    frozen = SimpleNamespace(model_dump=Mock(return_value={"acquisition": True}))
    evidence = SimpleNamespace(model_dump=Mock(return_value={"evidence": True}))
    verify = Mock()
    build_evidence = Mock(return_value=evidence)
    write = Mock()
    monkeypatch.setattr(acquisition_cli, "emergency_department_count_acquisition", Mock(return_value=frozen))
    monkeypatch.setattr(acquisition_cli, "verify_emergency_department_count_source_bytes", verify)
    monkeypatch.setattr(acquisition_cli, "build_emergency_department_count_public_evidence_input", build_evidence)
    monkeypatch.setattr(acquisition_cli, "write_atomic_json", write)
    sources = [tmp_path / name for name in ("link.csv", "hgi.csv", "metadata.json", "dictionary.pdf", "definition.xml")]
    args = [
        "acquire_scale_input_family.py", "--family", "emergency_department_count",
        "--source-commit", "a" * 40, "--cache-root", str(tmp_path / "cache"),
        "--acquisition-output", str(tmp_path / "out-a.json"),
        "--evidence-output", str(tmp_path / "out-e.json"),
        "--ahrq-linkage", str(sources[0]), "--cms-hgi", str(sources[1]),
        "--cms-hgi-metadata", str(sources[2]), "--cms-hospital-dictionary", str(sources[3]),
        "--ecfr-ed-definition", str(sources[4]),
    ]
    monkeypatch.setattr(sys, "argv", args)
    acquisition_cli.main()
    verify.assert_called_once_with(frozen, tmp_path / "cache", *sources)
    build_evidence.assert_called_once_with(frozen, producer_commit="a" * 40)
    assert write.call_count == 2


def test_json_schema_rejects_runtime_immutable_mutations() -> None:
    schema = json.loads(SCHEMA.read_text())
    mutations = [
        _mutate("root", "acquired_at", "2026-07-19T05:07:00Z"),
        _mutate("root", "systems", list(reversed(_checked_in().systems))),
        _mutate("root", "prohibited_outputs", ["scale_score"]),
        _mutate("cell", "flag_sum_used", True),
        _mutate("evaluation", "usable_for_scale_input", True),
        _mutate("artifact", "source_url", "https://example.invalid"),
    ]
    for mutated in mutations:
        with pytest.raises(ValidationError):
            build_emergency_department_count_acquisition(mutated)
        assert list(
            Draft202012Validator(schema).iter_errors(
                json.loads(json.dumps(mutated))
            )
        )


def test_v1_through_v5_fixture_bytes_remain_unchanged() -> None:
    expected = {
        ROOT / "contracts/v1/fixtures/scale-operating-revenue-acquisition.json": "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0",  # pragma: allowlist secret
        ROOT / "contracts/v2/fixtures/scale-annual-discharges-acquisition.json": "aa0027e2af3dc5e29fc2e5245b6e3d36370b83560ed8bbf64f9de12c6908495a",  # pragma: allowlist secret
        ROOT / "contracts/v3/fixtures/scale-physician-count-acquisition.json": "e7964104e56b389a19540b541cc490656578aede63d2dcbcbb8ab73571b3192b",  # pragma: allowlist secret
        ROOT / "contracts/v4/fixtures/scale-service-line-count-acquisition.json": "59a1debb97e6dd3cb2cbc6ce680c996cac8dbd17050c3b55563d3c90fa1f3946",  # pragma: allowlist secret
        ROOT / "contracts/v5/fixtures/scale-safety-net-patient-mix-acquisition.json": "ea349d7b65bc0c44912b2dccecf87fed9cb173164a40dbfccd7b6351f1804288",  # pragma: allowlist secret
        ROOT / "contracts/v5/fixtures/scale-safety-net-patient-mix-input.json": "fd10799454ff317ff8496888749a234a7934326016f3ddad814e60e4055fe537",  # pragma: allowlist secret
    }
    assert {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in expected} == expected


def test_two_in_memory_builds_are_byte_identical() -> None:
    first = acquisition()
    second = acquisition()
    assert json.dumps(first.model_dump(mode="json"), sort_keys=True) == json.dumps(
        second.model_dump(mode="json"), sort_keys=True
    )
    first_evidence = build_emergency_department_count_public_evidence_input(first).model_dump(mode="json")
    second_evidence = build_emergency_department_count_public_evidence_input(second).model_dump(mode="json")
    assert canonical_sha256(first_evidence) == canonical_sha256(second_evidence)
