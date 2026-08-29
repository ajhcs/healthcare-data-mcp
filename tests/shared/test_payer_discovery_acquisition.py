from datetime import datetime, timezone
import hashlib
import json
from typing import cast
import pytest

from shared.acquisition.payer_discovery import (
    PayerCandidate,
    SOURCE_CATALOG,
    build_payer_observation_envelope,
    validate_payer_catalog,
)
from shared.storage.raw_custody import RawArtifactStore


def test_typed_candidate_rejects_census_and_unregistered_mapping() -> None:
    with pytest.raises(ValueError, match="census/ACS"):
        PayerCandidate.from_mapping(
            {
                "payer_type": "marketplace",
                "source_family": "census_population",
                "source_period": "2024",
                "geography": "PA",
                "denominator": 4,
            }
        )


def test_catalog_is_json_safe_and_f7_reference_is_first_class() -> None:
    catalog = validate_payer_catalog()
    assert catalog["schema_version"] == "hdp.source-catalog.v1"
    assert catalog["record_type"] == "source_catalog"
    assert len(cast(list[object], catalog["sources"])) == len(SOURCE_CATALOG)
    candidate = PayerCandidate.from_mapping(
        {
            "payer_type": "f7_reference",
            "source_family": "f7_payer_toc_reference",
            "source_period": "2024",
            "geography": "PA",
            "reference_id": "f7:marketplace:pa",
            "missingness": "not_applicable",
        }
    )
    assert candidate.reference_id == "f7:marketplace:pa"
    with pytest.raises(ValueError, match="registered"):
        PayerCandidate.from_mapping(
            {
                "payer_type": "marketplace",
                "source_family": "bad",
                "source_period": "2024",
                "geography": "PA",
                "denominator": 4,
            }
        )
    with pytest.raises(ValueError, match="unknown missingness"):
        PayerCandidate.from_mapping(
            {
                "payer_type": "marketplace",
                "source_family": "cms_marketplace_effectuated_enrollment",
                "source_period": "2024",
                "geography": "PA",
                "missingness": "unknown",
            }
        )
    with pytest.raises(ValueError, match="semantic population"):
        PayerCandidate.from_mapping(
            {
                "payer_type": "marketplace",
                "source_family": "cms_marketplace_effectuated_enrollment",
                "source_period": "2024",
                "geography": "PA",
                "plan_id": "p",
                "denominator": 4,
                "denominator_scope": "ACS population",
            }
        )


def test_payer_value_schema_rejects_missing_identity_and_preserves_f7_reference() -> None:
    with pytest.raises(ValueError, match="plan or contract"):
        PayerCandidate.from_mapping(
            {
                "payer_type": "marketplace",
                "source_family": "cms_marketplace_effectuated_enrollment",
                "source_period": "2024",
                "geography": "PA",
                "denominator": 4,
            }
        )
    reference = PayerCandidate.from_mapping(
        {
            "payer_type": "f7_reference",
            "source_family": "f7_payer_toc_reference",
            "source_period": "2024",
            "geography": "PA",
            "reference_id": "f7:toc:pa",
        }
    )
    assert reference.reference_id == "f7:toc:pa"


def test_blocked_conflict_requires_caller_attested_source_release_authority() -> None:
    with pytest.raises(ValueError, match="authority"):
        PayerCandidate.from_mapping(
            {
                "payer_type": "marketplace",
                "source_family": "cms_marketplace_effectuated_enrollment",
                "source_period": "2024",
                "geography": "PA",
                "plan_id": "plan-1",
                "missingness": "blocked_source_conflict",
                "competing_observation_ref": "observation:payer:fake",
                "competing_source_id": "source:wrong",
                "competing_release_id": "release:wrong:2024",
            }
        )


def test_observation_envelope_contains_custody_hash_and_validates_schema(tmp_path) -> None:
    raw = b"fixture"
    store = RawArtifactStore(tmp_path / "store")
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    identity = hashlib.sha256(
        (
            SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"].source_id
            + "|release:cms_marketplace_effectuated_enrollment:2024|"
            + content_hash
        ).encode()
    ).hexdigest()[:32]
    metadata = {
        "schema_version": "hdp.raw-artifact.v1",
        "record_type": "raw_artifact",
        "artifact_id": "artifact:raw:" + identity,
        "source_id": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"].source_id,
        "source_url": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"].source_url,
        "release_id": "release:cms_marketplace_effectuated_enrollment:2024",
        "media_type": "application/octet-stream",
        "content_sha256": content_hash,
        "byte_length": len(raw),
        "chunk_count": 1,
        "chunk_size": len(raw),
        "idempotency_key": "idempotency:raw:" + identity,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "rights_status": "approved_public",
    }
    store.put(metadata, [raw])
    envelope = build_payer_observation_envelope(
        tracking_bead="healthcare-toolkit-rrna.9",
        source_family="cms_marketplace_effectuated_enrollment",
        source_period="2024",
        candidates=[{"payer_type": "marketplace", "geography": "PA", "plan_id": "plan-1", "denominator": 10}],
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        artifact_store=store,
        artifact_id=cast(str, metadata["artifact_id"]),
    )
    assert envelope["schema_version"] == "hdp.observation-envelope.v1"
    assert cast(dict[str, object], envelope["artifact"])["content_sha256"].__class__ is str
    assert cast(dict[str, object], envelope["authority_limits"])["current_projection_allowed"] is False
    receipt = cast(dict[str, object], envelope["receipt"])
    assert receipt["source_release_sha256"] != receipt["artifact_sha256"]
    receipt_payload = dict(receipt)
    receipt_payload.pop("receipt_sha256")
    expected = (
        "sha256:"
        + hashlib.sha256(json.dumps(receipt_payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
    assert receipt["receipt_sha256"] == expected


def test_empty_candidates_emit_explicit_valid_missingness_observation(tmp_path) -> None:
    raw = b"fixture"
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    identity = hashlib.sha256(
        (
            SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"].source_id
            + "|release:cms_marketplace_effectuated_enrollment:2024|"
            + content_hash
        ).encode()
    ).hexdigest()[:32]
    metadata = {
        "schema_version": "hdp.raw-artifact.v1",
        "record_type": "raw_artifact",
        "artifact_id": "artifact:raw:" + identity,
        "source_id": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"].source_id,
        "source_url": SOURCE_CATALOG["cms_marketplace_effectuated_enrollment"].source_url,
        "release_id": "release:cms_marketplace_effectuated_enrollment:2024",
        "media_type": "application/octet-stream",
        "content_sha256": content_hash,
        "byte_length": len(raw),
        "chunk_count": 1,
        "chunk_size": len(raw),
        "idempotency_key": "idempotency:raw:" + identity,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "rights_status": "approved_public",
    }
    store = RawArtifactStore(tmp_path / "store")
    store.put(metadata, [raw])
    envelope = build_payer_observation_envelope(
        tracking_bead="healthcare-toolkit-rrna.9",
        source_family="cms_marketplace_effectuated_enrollment",
        source_period="2024",
        candidates=[],
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        artifact_store=store,
        artifact_id=cast(str, metadata["artifact_id"]),
    )
    observations = cast(list[dict[str, object]], envelope["observations"])
    assert observations[0]["value_state"] == "not_yet_researched"
