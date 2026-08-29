"""Hospital-discovery v3 contract tests."""

from __future__ import annotations

import json

import pytest

from shared.acquisition.hospital_discovery import (
    HospitalDiscoveryError,
    HospitalDiscoveryManifest,
    HospitalEvidenceRegistration,
    HospitalUrlProbe,
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
    evidence = HospitalEvidenceRegistration(
        "evidence:1", "source:cms:hospital", "hospital:390001", "name", "Example Hospital", "candidate"
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
        HospitalUrlProbe("source:local", "/tmp/hospitals")


def test_owner_promotion_cannot_be_claimed_by_discovery_lane() -> None:
    with pytest.raises(HospitalDiscoveryError, match="outstanding"):
        HospitalEvidenceRegistration(
            "evidence:1", "source:cms:hospital", "hospital:1", "name", "x", owner_promotion_state="promoted"
        )
