from datetime import datetime, timezone
import pytest

from shared.acquisition.payer_discovery import PayerCandidate, build_payer_observation_envelope


def test_typed_candidate_rejects_census_and_unregistered_mapping() -> None:
    with pytest.raises(ValueError, match="census/ACS"):
        PayerCandidate.from_mapping({"payer_type": "marketplace", "source_family": "census_population", "source_period": "2024", "geography": "PA", "denominator": 4})
    with pytest.raises(ValueError, match="registered"):
        PayerCandidate.from_mapping({"payer_type": "marketplace", "source_family": "bad", "source_period": "2024", "geography": "PA", "denominator": 4})


def test_observation_envelope_contains_custody_hash_and_validates_schema() -> None:
    envelope = build_payer_observation_envelope(tracking_bead="healthcare-toolkit-rrna.p1-28-payer-discovery-20260829", source_family="cms_marketplace_effectuated_enrollment", source_period="2024", artifact_bytes=b"fixture", candidates=[{"payer_type": "marketplace", "geography": "PA", "denominator": 10}], retrieved_at=datetime.now(timezone.utc).isoformat(), custody_metadata={"artifact_id": "artifact:payer:2024", "custody_locator": "object://payer/2024", "verified": True, "immutable": True})
    assert envelope["schema_version"] == "hdp.observation-envelope.v1"
    assert envelope["artifact"]["content_sha256"].startswith("sha256:")
    assert envelope["authority_limits"]["current_projection_allowed"] is False
