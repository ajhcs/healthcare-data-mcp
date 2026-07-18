"""Acceptance and adversarial tests for annual-discharges tabular acquisition."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from shared.acquisition.scale_annual_discharges_packet import (
    EXPECTED_LINKAGE_ROWS,
    AnnualDischargesAcquisition,
    acquisition,
    build_annual_discharges_acquisition,
    verify_annual_discharges_source_bytes,
)
from shared.acquisition.scale_input_family import SYSTEM_SLUGS
from shared.acquisition.scale_operating_revenue_packet import acquisition as revenue_acquisition
from shared.acquisition.scale_tabular_input_family import (
    COMMON_BLOCKERS,
    TabularScaleInputFamilyAcquisition,
    build_tabular_acquisition,
    build_tabular_public_evidence_input,
    verify_tabular_source_bytes,
)
from shared.contracts.public_evidence import build_public_evidence_bundle
from shared.utils.cache import write_atomic_json

ROOT = Path(__file__).resolve().parents[1]
ACQUISITION = ROOT / "contracts" / "v2" / "fixtures" / "scale-annual-discharges-acquisition.json"
EVIDENCE = ROOT / "contracts" / "v2" / "fixtures" / "scale-annual-discharges-input.json"
V2_SCHEMA = ROOT / "contracts" / "v2" / "scale-tabular-input-family-acquisition.schema.json"
V1_SCHEMA = ROOT / "contracts" / "v1" / "public-evidence-bundle.schema.json"
REVENUE_ACQUISITION = ROOT / "contracts" / "v1" / "fixtures" / "scale-operating-revenue-acquisition.json"
REVENUE_EVIDENCE = ROOT / "contracts" / "v1" / "fixtures" / "scale-operating-revenue-input.json"
VALIDATED_CACHE = Path.home() / ".healthcare-data-mcp" / "cache"


def _checked_in() -> TabularScaleInputFamilyAcquisition:
    return AnnualDischargesAcquisition.model_validate_json(ACQUISITION.read_text(encoding="utf-8"))


def test_checked_in_acquisition_schema_and_evidence_are_deterministic() -> None:
    expected = acquisition()
    assert expected == acquisition() == _checked_in()
    assert json.loads(ACQUISITION.read_text(encoding="utf-8")) == expected.model_dump(mode="json")
    evidence = build_tabular_public_evidence_input(expected)
    assert json.loads(EVIDENCE.read_text(encoding="utf-8")) == evidence.model_dump(mode="json")
    assert evidence.producer.commit == "0" * 40

    acquisition_schema = json.loads(V2_SCHEMA.read_text(encoding="utf-8"))
    assert acquisition_schema == AnnualDischargesAcquisition.model_json_schema()
    assert list(Draft202012Validator(acquisition_schema).iter_errors(expected.model_dump(mode="json"))) == []
    public_schema = json.loads(V1_SCHEMA.read_text(encoding="utf-8"))
    bundle = build_public_evidence_bundle(evidence)
    assert list(Draft202012Validator(public_schema).iter_errors(bundle.model_dump(mode="json"))) == []


def test_exact_six_source_rows_and_linkage_context_remain_blocked() -> None:
    frozen = _checked_in()
    assert tuple(frozen.systems) == SYSTEM_SLUGS
    assert [item.system_slug for item in frozen.system_rows] == list(SYSTEM_SLUGS)
    assert [item.system_slug for item in frozen.candidates] == list(SYSTEM_SLUGS)
    assert [item.row_number for item in frozen.system_rows] == [110, 18, 466, 361, 475, 268]
    assert [item.health_sys_id for item in frozen.system_rows] == [
        "HSI00000218",
        "HSI00000048",
        "HSI00001065",
        "HSI00000820",
        "HSI00001079",
        "HSI00000608",
    ]
    assert [item.raw_lexical_value for item in frozen.system_rows] == [
        "71250",
        "147361",
        "37387",
        "144099",
        "31354",
        "59916",
    ]
    assert [item.candidate_value for item in frozen.candidates] == [
        71250,
        147361,
        37387,
        144099,
        31354,
        59916,
    ]
    assert len(frozen.linkage_rows) == 30
    assert frozen.technical_definition_receipt is None
    assert frozen.technical_definition_custody == "not_locally_receipted"
    assert frozen.raw_http_receipt_custody == "not_locally_receipted"
    assert frozen.redistribution_license_receipt is None
    assert frozen.redistribution_rights_custody == "unreviewed"
    assert all(COMMON_BLOCKERS.issubset(item.blocker_codes) for item in frozen.candidates)
    assert all(
        item.missingness == "blocked_source_conflict"
        and not item.imputed
        and not item.aggregated
        and not item.fabricated_zero
        and not item.approved_for_scale
        for item in frozen.candidates
    )

    main_line = next(item for item in frozen.linkage_rows if item.compendium_hospital_id == "CHSP00000757")
    assert main_line.hospital_name == "Bryn Mawr Rehabilitation Hospital"
    assert main_line.acutehosp_flag_raw == "0" and main_line.hos_dsch_raw == "2067"
    jeanes = next(item for item in frozen.linkage_rows if item.compendium_hospital_id == "CHSP00002972")
    assert jeanes.acutehosp_flag_raw == "1" and jeanes.hos_dsch_raw == ""


def test_public_bundle_preserves_candidates_as_nonapproved_context() -> None:
    evidence = build_tabular_public_evidence_input(_checked_in(), producer_commit="a" * 40)
    bundle = build_public_evidence_bundle(evidence)
    assert len(bundle.entities) == 6
    assert len(bundle.observations) == 6
    assert len(bundle.sources) == 12
    assert len(bundle.input_artifacts) == 2
    assert len(bundle.coverage) == 6
    assert len(bundle.conflicts) == 6
    assert {item.measure_id for item in bundle.observations} == {"source_local_candidate.annual_discharges"}
    assert {item.measure_id for item in bundle.coverage} == {"annual_discharges"}
    assert all(item.status == "blocked_source_conflict" and not item.observation_refs for item in bundle.coverage)
    assert {item.status for item in bundle.conflicts} == {"open"}
    assert bundle.request.parameters["no_scale_score"] is True
    assert bundle.request.parameters["no_facility_aggregation"] is True
    assert bundle.request.parameters["raw_http_receipt_available"] is False
    assert bundle.request.parameters["redistribution_rights_reviewed"] is False
    assert bundle.request.parameters["technical_definition_receipted"] is False
    with pytest.raises(ValueError, match="producer commit"):
        build_tabular_public_evidence_input(_checked_in(), producer_commit="not-a-commit")


def test_contract_rejects_drift_approval_imputation_aggregation_and_outputs() -> None:
    payload = _checked_in().model_dump(mode="json")
    with pytest.raises(ValidationError, match="six-system order"):
        build_annual_discharges_acquisition({**payload, "systems": payload["systems"][:-1]})

    for field, value in (
        ("approved_for_scale", True),
        ("imputed", True),
        ("aggregated", True),
        ("fabricated_zero", True),
    ):
        candidates = [dict(item) for item in payload["candidates"]]
        candidates[0][field] = value
        with pytest.raises(ValidationError):
            build_annual_discharges_acquisition({**payload, "candidates": candidates})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["candidate_value"] = 0
    with pytest.raises(ValidationError, match="lexical value"):
        build_annual_discharges_acquisition({**payload, "candidates": candidates})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["blocker_codes"] = [
        item for item in candidates[0]["blocker_codes"] if item != "technical_definition_not_receipted"
    ]
    with pytest.raises(ValidationError, match="mandatory comparability blockers"):
        build_annual_discharges_acquisition({**payload, "candidates": candidates})

    with pytest.raises(ValidationError, match="prohibitions"):
        build_annual_discharges_acquisition({**payload, "prohibited_outputs": payload["prohibited_outputs"][:-1]})
    with pytest.raises(ValidationError):
        build_annual_discharges_acquisition({**payload, "scale_score": 1})


def test_contract_rejects_row_receipt_period_boundary_and_hash_drift() -> None:
    payload = _checked_in().model_dump(mode="json")
    system_rows = [dict(item) for item in payload["system_rows"]]
    system_rows[0]["raw_lexical_value"] = "999"
    with pytest.raises(ValidationError):
        build_annual_discharges_acquisition({**payload, "system_rows": system_rows})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["system_row_ref"] = candidates[1]["system_row_ref"]
    with pytest.raises(ValidationError, match="system row reference"):
        build_annual_discharges_acquisition({**payload, "candidates": candidates})

    candidates = [dict(item) for item in payload["candidates"]]
    candidates[0]["linkage_row_refs"] = candidates[0]["linkage_row_refs"][:-1]
    with pytest.raises(ValidationError, match="every exact source-local linkage row"):
        build_annual_discharges_acquisition({**payload, "candidates": candidates})

    for field, value in (
        ("source_period", "2024"),
        ("definition", "fabricated definition"),
        ("basis", "fabricated boundary"),
    ):
        candidates = [dict(item) for item in payload["candidates"]]
        candidates[0][field] = value
        with pytest.raises(ValidationError):
            build_annual_discharges_acquisition({**payload, "candidates": candidates})

    drifted = {**payload, "producer_version": "fabricated"}
    with pytest.raises(ValidationError, match="acquisition_sha256"):
        AnnualDischargesAcquisition.model_validate(drifted)


def test_contract_rejects_coherent_product_to_ahrq_identity_swap() -> None:
    payload = _checked_in().model_dump(mode="json")
    system_rows = [dict(item) for item in payload["system_rows"]]
    first_id = system_rows[0]["health_sys_id"]
    first_name = system_rows[0]["health_sys_name"]
    system_rows[0]["health_sys_id"] = system_rows[1]["health_sys_id"]
    system_rows[0]["health_sys_name"] = system_rows[1]["health_sys_name"]
    system_rows[1]["health_sys_id"] = first_id
    system_rows[1]["health_sys_name"] = first_name
    with pytest.raises(ValidationError, match="product-to-AHRQ identity substitution"):
        build_annual_discharges_acquisition({**payload, "system_rows": system_rows})

    linkage_rows = [dict(item) for item in payload["linkage_rows"]]
    christianacare = next(item for item in linkage_rows if item["system_slug"] == "christianacare")
    jefferson = next(item for item in linkage_rows if item["system_slug"] == "jefferson-health")
    christianacare["health_sys_id"], jefferson["health_sys_id"] = (
        jefferson["health_sys_id"],
        christianacare["health_sys_id"],
    )
    christianacare["health_sys_name"], jefferson["health_sys_name"] = (
        jefferson["health_sys_name"],
        christianacare["health_sys_name"],
    )
    with pytest.raises(ValidationError, match="linkage row product-to-AHRQ identity substitution"):
        build_annual_discharges_acquisition({**payload, "linkage_rows": linkage_rows})


def test_contract_rejects_coherent_linkage_and_reference_deletion() -> None:
    payload = _checked_in().model_dump(mode="json")
    removed = payload["linkage_rows"][0]
    linkage_rows = payload["linkage_rows"][1:]
    removed_ref = (
        f"row:linkage:{removed['compendium_hospital_id']}:{removed['row_number']}"
    )
    candidates = [dict(item) for item in payload["candidates"]]
    owner = next(item for item in candidates if item["system_slug"] == removed["system_slug"])
    owner["linkage_row_refs"] = [ref for ref in owner["linkage_row_refs"] if ref != removed_ref]
    with pytest.raises(ValidationError, match="exact complete frozen linkage row set"):
        build_annual_discharges_acquisition(
            {**payload, "linkage_rows": linkage_rows, "candidates": candidates}
        )


def test_validated_cache_verifier_accepts_exact_bytes_and_rejects_drift(tmp_path: Path) -> None:
    manifest_source = VALIDATED_CACHE / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    if not manifest_source.exists():
        pytest.skip("validated local AHRQ cache is not installed")
    source_manifest = json.loads(manifest_source.read_text(encoding="utf-8"))
    cache_root = tmp_path / "cache"
    artifact_dir = cache_root / "silver"
    artifact_dir.mkdir(parents=True)
    artifacts = []
    for item in source_manifest["artifacts"]:
        source = Path(item["path"])
        target = artifact_dir / item["relative_path"]
        shutil.copyfile(source, target)
        artifacts.append({**item, "path": str(target)})
    manifest = {**source_manifest, "artifacts": artifacts}
    manifest_path = cache_root / "manifests" / "datasets" / "ahrq_health_system_compendium.json"
    write_atomic_json(manifest_path, manifest)
    manifest_raw = manifest_path.read_bytes()

    payload = _checked_in().model_dump(mode="json")
    receipt = {
        **payload["cache_receipt"],
        "manifest_sha256": f"sha256:{hashlib.sha256(manifest_raw).hexdigest()}",
        "manifest_content_length": len(manifest_raw),
    }
    local = build_annual_discharges_acquisition({**payload, "cache_receipt": receipt})
    verify_annual_discharges_source_bytes(local, cache_root)

    generic_payload = local.model_dump(mode="json")
    removed = generic_payload["linkage_rows"][0]
    removed_ref = f"row:linkage:{removed['compendium_hospital_id']}:{removed['row_number']}"
    generic_candidates = [dict(item) for item in generic_payload["candidates"]]
    owner = next(item for item in generic_candidates if item["system_slug"] == removed["system_slug"])
    owner["linkage_row_refs"] = [ref for ref in owner["linkage_row_refs"] if ref != removed_ref]
    incomplete = build_tabular_acquisition(
        {
            **generic_payload,
            "linkage_rows": generic_payload["linkage_rows"][1:],
            "candidates": generic_candidates,
        }
    )
    with pytest.raises(ValueError, match="exact complete frozen linkage row set"):
        verify_tabular_source_bytes(
            incomplete,
            cache_root,
            expected_linkage_rows=EXPECTED_LINKAGE_ROWS,
        )

    system_path = artifact_dir / "ahrq_system_2023.csv"
    system_path.write_bytes(system_path.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="source byte drift"):
        verify_annual_discharges_source_bytes(local, cache_root)


def test_operating_revenue_v1_models_and_bytes_remain_unchanged() -> None:
    assert revenue_acquisition().model_dump(mode="json") == json.loads(
        REVENUE_ACQUISITION.read_text(encoding="utf-8")
    )
    assert hashlib.sha256(REVENUE_ACQUISITION.read_bytes()).hexdigest() == (
        "ebf2be8cc8cd09705193b3e24aa2591af86dca6d3856892491a869bfcebe0cf0"  # pragma: allowlist secret
    )
    assert hashlib.sha256(REVENUE_EVIDENCE.read_bytes()).hexdigest() == (
        "04fadae952898bc6dac87d0aaf4a3b04711cc9acc387ec751612f4b937b5b89f"  # pragma: allowlist secret
    )
