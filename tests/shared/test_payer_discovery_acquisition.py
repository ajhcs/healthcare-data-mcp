from datetime import datetime, timezone
import hashlib
import pytest

from shared.acquisition.payer_discovery import PayerCandidate, SOURCE_CATALOG, build_payer_observation_envelope
from shared.storage.raw_custody import RawArtifactStore


def test_typed_candidate_rejects_census_and_unregistered_mapping() -> None:
    with pytest.raises(ValueError, match="census/ACS"):
        PayerCandidate.from_mapping({"payer_type": "marketplace", "source_family": "census_population", "source_period": "2024", "geography": "PA", "denominator": 4})
    with pytest.raises(ValueError, match="registered"):
        PayerCandidate.from_mapping({"payer_type": "marketplace", "source_family": "bad", "source_period": "2024", "geography": "PA", "denominator": 4})
    with pytest.raises(ValueError, match="unknown missingness"):
        PayerCandidate.from_mapping({"payer_type": "marketplace", "source_family": "cms_marketplace_effectuated_enrollment", "source_period": "2024", "geography": "PA", "missingness": "unknown"})
    with pytest.raises(ValueError, match="semantic population"):
        PayerCandidate.from_mapping({"payer_type": "marketplace", "source_family": "cms_marketplace_effectuated_enrollment", "source_period": "2024", "geography": "PA", "plan_id": "p", "denominator": 4, "denominator_scope": "ACS population"})


def test_observation_envelope_contains_custody_hash_and_validates_schema(tmp_path) -> None:
    raw = b"fixture"
    store = RawArtifactStore(tmp_path / "store")
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    identity = hashlib.sha256((SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"]["source_id"] + "|release:cms_marketplace_effectuated_enrollment:2024|" + content_hash).encode()).hexdigest()[:32]
    metadata = {"schema_version": "hdp.raw-artifact.v1", "record_type": "raw_artifact", "artifact_id": "artifact:raw:" + identity, "source_id": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"]["source_id"], "source_url": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"]["source_url"], "release_id": "release:cms_marketplace_effectuated_enrollment:2024", "media_type": "application/octet-stream", "content_sha256": content_hash, "byte_length": len(raw), "chunk_count": 1, "chunk_size": len(raw), "idempotency_key": "idempotency:raw:" + identity, "captured_at": datetime.now(timezone.utc).isoformat(), "rights_status": "approved_public"}
    store.put(metadata, [raw])
    envelope = build_payer_observation_envelope(tracking_bead="healthcare-toolkit-rrna.9", source_family="cms_marketplace_effectuated_enrollment", source_period="2024", candidates=[{"payer_type": "marketplace", "geography": "PA", "plan_id": "plan-1", "denominator": 10}], retrieved_at=datetime.now(timezone.utc).isoformat(), artifact_store=store, artifact_id=metadata["artifact_id"])
    assert envelope["schema_version"] == "hdp.observation-envelope.v1"
    assert envelope["artifact"]["content_sha256"].startswith("sha256:")
    assert envelope["authority_limits"]["current_projection_allowed"] is False


def test_empty_candidates_emit_explicit_valid_missingness_observation(tmp_path) -> None:
    raw = b"fixture"
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    identity = hashlib.sha256((SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"]["source_id"] + "|release:cms_marketplace_effectuated_enrollment:2024|" + content_hash).encode()).hexdigest()[:32]
    metadata = {"schema_version": "hdp.raw-artifact.v1", "record_type": "raw_artifact", "artifact_id": "artifact:raw:" + identity, "source_id": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"]["source_id"], "source_url": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"]["source_url"], "release_id": "release:cms_marketplace_effectuated_enrollment:2024", "media_type": "application/octet-stream", "content_sha256": content_hash, "byte_length": len(raw), "chunk_count": 1, "chunk_size": len(raw), "idempotency_key": "idempotency:raw:" + identity, "captured_at": datetime.now(timezone.utc).isoformat(), "rights_status": "approved_public"}
    store = RawArtifactStore(tmp_path / "store")
    store.put(metadata, [raw])
    envelope = build_payer_observation_envelope(tracking_bead="healthcare-toolkit-rrna.9", source_family="cms_marketplace_effectuated_enrollment", source_period="2024", candidates=[], retrieved_at=datetime.now(timezone.utc).isoformat(), artifact_store=store, artifact_id=metadata["artifact_id"])
    assert envelope["observations"][0]["value_state"] == "not_yet_researched"
