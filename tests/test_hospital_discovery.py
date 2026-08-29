"""Hospital-discovery v3 contract tests."""

from __future__ import annotations

import json

import pytest

from shared.acquisition.hospital_discovery import (
    HospitalDiscoveryError,
    HospitalDiscoveryManifest,
    HospitalEvidenceRegistration,
    HospitalUrlProbe,
    canonical_digest,
    load_manifest,
    register_evidence,
    validate_manifest,
    write_manifest,
)


def _manifest() -> HospitalDiscoveryManifest:
    return HospitalDiscoveryManifest(
        manifest_id="hospital-discovery-20260829",
        generated_at="2026-08-29T12:00:00Z",
        sources=(HospitalUrlProbe("source:cms:hospital", "https://data.cms.gov/hospitals"),),
    )


def test_registration_preserves_seal_and_non_authority() -> None:
    receipt = {
        "evidence_id": "evidence:1",
        "source_id": "source:cms:hospital",
        "receipt_id": "receipt:1",
        "entity_ref": "hospital:390001",
        "field": "name",
        "observed_value": "Example Hospital",
        "candidate_state": "candidate",
        "authority_state": "non_authoritative",
        "owner_promotion_state": "outstanding",
        "caveat": "",
    }
    evidence = HospitalEvidenceRegistration(
        "evidence:1",
        "source:cms:hospital",
        "receipt:1",
        canonical_digest(receipt),
        "hospital:390001",
        "name",
        "Example Hospital",
        "candidate",
    )
    updated = register_evidence(_manifest(), evidence)
    assert updated.as_dict()["seal"] == "sealed"
    assert updated.evidence[0].authority_state == "non_authoritative"
    assert updated.evidence[0].owner_promotion_state == "outstanding"


def test_manifest_round_trip_is_strict(tmp_path) -> None:
    path = write_manifest(_manifest(), tmp_path / "manifest.json")
    assert load_manifest(path).as_dict() == _manifest().as_dict()
    raw = json.loads(path.read_text())
    raw["unexpected"] = True
    with pytest.raises(HospitalDiscoveryError, match="unknown manifest"):
        validate_manifest(raw)


def test_failed_probe_and_invalid_url_are_explicit() -> None:
    probe = HospitalUrlProbe("source:state:hospital", "https://state.example/hospitals", "invalid", "failed")
    assert probe.probe_state == "failed"
    with pytest.raises(HospitalDiscoveryError, match="absolute HTTP"):
        HospitalUrlProbe("source:state:hospital", "/tmp/hospitals")


def test_owner_promotion_cannot_be_claimed_by_discovery_lane() -> None:
    raw = _manifest().as_dict()
    promoted = {
        "evidence_id": "evidence:1",
        "source_id": "source:cms:hospital",
        "receipt_id": "receipt:1",
        "entity_ref": "hospital:1",
        "field": "name",
        "observed_value": "x",
        "candidate_state": "candidate",
        "authority_state": "non_authoritative",
        "owner_promotion_state": "promoted",
        "caveat": "",
    }
    raw["evidence"] = [{**promoted, "receipt_sha256": canonical_digest(promoted)}]
    raw["manifest_sha256"] = canonical_digest({key: value for key, value in raw.items() if key != "manifest_sha256"})
    with pytest.raises(HospitalDiscoveryError, match="outstanding"):
        validate_manifest(raw)


def test_probe_state_matrix_requires_receipted_success() -> None:
    with pytest.raises(HospitalDiscoveryError, match="succeeded probe"):
        HospitalUrlProbe("source:cms:hospital", "https://data.cms.gov/hospitals", "verified", "succeeded")
    with pytest.raises(HospitalDiscoveryError, match="pending probe"):
        HospitalUrlProbe(
            "source:cms:hospital",
            "https://data.cms.gov/hospitals",
            "verified",
            "pending",
            "https://data.cms.gov/hospitals",
        )
    with pytest.raises(HospitalDiscoveryError, match="failed probe"):
        HospitalUrlProbe(
            "source:cms:hospital", "https://data.cms.gov/hospitals", "unverified", "failed", http_status=503
        )


def test_safe_authority_and_digest_bindings() -> None:
    with pytest.raises(HospitalDiscoveryError, match="unsafe"):
        HospitalUrlProbe("source:cms:hospital", "https://127.0.0.1/hospitals")
    with pytest.raises(HospitalDiscoveryError, match="invalid port"):
        HospitalUrlProbe("source:cms:hospital", "https://data.cms.gov:bad/hospitals")
    with pytest.raises(HospitalDiscoveryError, match="lowercase"):
        HospitalEvidenceRegistration(
            "evidence:1", "source:cms:hospital", "receipt:1", "sha256:" + "A" * 64, "hospital:1", "name", "x"
        )


def test_manifest_constructor_rejects_supplied_digest_drift() -> None:
    with pytest.raises(HospitalDiscoveryError, match="manifest_sha256"):
        HospitalDiscoveryManifest(
            "hospital-discovery-20260829", "2026-08-29T12:00:00Z", manifest_sha256="sha256:" + "a" * 64
        )
