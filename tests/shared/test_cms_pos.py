"""Focused contract and state-transition tests for the CMS POS producer."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pytest

from shared.adapters import (
    AdapterCatalogEntry,
    ConditionalResponse,
    InMemoryAdapterCatalog,
    StreamBudget,
    fingerprint_bytes,
)
from shared.acquisition.cms_pos import (
    CMS_POS_SOURCE_ID,
    CmsPosAuthorizationError,
    CmsPosDistribution,
    CmsPosError,
    CmsPosProducer,
    CmsPosRelease,
    CmsPosResult,
    build_cms_pos_catalog,
    load_cms_pos_fixture,
    validate_cms_pos_result,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/cms-pos/v1/fixtures"
SOURCE_URL = "https://data.cms.gov/provider-data/dataset/cms-pos"
RELEASE_LOCATOR = "https://data.cms.gov/provider-data/dataset/cms-pos/releases"
BODY = b"PRVDR_NUM,FAC_NAME,STATE\n100001,Example North Hospital,PA\n100002,Example South Hospital,OH\n"


def _release(**updates: object) -> CmsPosRelease:
    values: dict[str, object] = {
        "source_id": CMS_POS_SOURCE_ID,
        "release_id": "release:cms:pos:2026-q1",
        "release_label": "CMS POS synthetic Q1 2026",
        "source_period": "2026-Q1",
        "source_url": SOURCE_URL,
        "release_locator": RELEASE_LOCATOR,
        "landing_page": SOURCE_URL,
        "published_at": "2026-01-15T00:00:00Z",
    }
    values.update(updates)
    return CmsPosRelease.from_mapping(values)


def _distribution(**updates: object) -> CmsPosDistribution:
    values: dict[str, object] = {
        "distribution_id": "distribution:cms:pos:2026-q1-csv",
        "url": "https://data.cms.gov/provider-data/dataset/cms-pos/files/2026-q1.csv",
        "etag": '"cms-pos-q1"',
    }
    values.update(updates)
    return CmsPosDistribution.from_mapping(values)


def _producer(*, budget: StreamBudget | None = None) -> CmsPosProducer:
    return CmsPosProducer(build_cms_pos_catalog(), budget=budget)


def _changed(*, preview_authorized: bool = False) -> tuple[CmsPosProducer, CmsPosResult]:
    producer = _producer()
    result = producer.produce(
        _release(),
        _distribution(),
        [BODY],
        conditional_response=ConditionalResponse(200, response_fingerprint=fingerprint_bytes(BODY)),
        preview_authorized=preview_authorized,
    )
    return producer, result


def test_changed_result_preserves_source_rows_and_requires_preview_authority() -> None:
    _, result = _changed()

    assert result.receipt.probe_state == "changed"
    assert result.receipt.state == "changed"
    assert result.receipt.acknowledged is True
    assert result.receipt.row_count == 2
    assert result.rows[0].source_identifier == "100001"
    assert result.rows[0].fields["FAC_NAME"] == "Example North Hospital"
    assert "canonical_entity_id" not in result.rows[0].fields
    with pytest.raises(CmsPosAuthorizationError, match="explicit authorization"):
        result.authorized_source_preview()

    _, authorized = _changed(preview_authorized=True)
    preview = authorized.authorized_source_preview(limit=1)
    assert preview["authorized"] is True
    preview_rows = preview["rows"]
    assert isinstance(preview_rows, list)
    assert len(preview_rows) == 1
    validate_cms_pos_result(authorized.as_dict())


def test_replay_is_deterministic_and_does_not_change_artifact_identity() -> None:
    producer, first = _changed()
    replay = producer.produce(
        _release(),
        _distribution(),
        [BODY],
        conditional_response=ConditionalResponse(200),
        prior=first.receipt,
    )

    assert replay.receipt.probe_state == "replayed"
    assert replay.receipt.receipt_id == first.receipt.receipt_id
    assert replay.receipt.artifact_id == first.receipt.artifact_id
    assert replay.receipt.idempotency_key == first.receipt.idempotency_key
    assert [row.source_record_id for row in replay.rows] == [row.source_record_id for row in first.rows]
    validate_cms_pos_result(replay.as_dict())


def test_conditional_noop_does_not_consume_body_or_advance_acknowledgement() -> None:
    producer = _producer()
    consumed = False

    def body() -> Iterable[bytes]:
        nonlocal consumed
        consumed = True
        yield BODY

    result = producer.produce(
        _release(),
        _distribution(),
        body(),
        conditional_response=ConditionalResponse(304),
    )

    assert consumed is False
    assert result.receipt.probe_state == "no_op"
    assert result.receipt.acknowledged is False
    assert result.receipt.stream_state == "not_started"
    assert result.rows == ()
    validate_cms_pos_result(result.as_dict())


def test_same_release_content_drift_is_quarantined_without_rows() -> None:
    producer, first = _changed()
    changed_body = BODY.replace(b"Example South", b"Changed South")
    result = producer.produce(
        _release(),
        _distribution(),
        [changed_body],
        conditional_response=ConditionalResponse(200),
        prior=first.receipt,
    )

    assert result.receipt.probe_state == "drift"
    assert result.receipt.failure_reason == "distribution_content_drift"
    assert result.receipt.acknowledged is False
    assert result.rows == ()
    validate_cms_pos_result(result.as_dict())


def test_same_release_metadata_drift_is_quarantined() -> None:
    producer, first = _changed()
    result = producer.produce(
        _release(release_label="CMS POS synthetic Q1 2026 corrected"),
        _distribution(),
        [BODY],
        conditional_response=ConditionalResponse(200),
        prior=first.receipt,
    )

    assert result.receipt.probe_state == "drift"
    assert result.receipt.failure_reason == "release_metadata_drift"
    assert result.rows == ()


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"FAC_NAME,STATE\nExample,PA\n", "malformed_csv_header"),
        (b"PRVDR_NUM,FAC_NAME\n100001,North\n100001,South\n", "duplicate_source_identifier"),
        (b"PRVDR_NUM,canonical_entity_id\n100001,entity-1\n", "reserved_canonical_field"),
        (b"PRVDR_NUM,FAC_NAME\n100001\n", "malformed_csv_row"),
    ],
)
def test_malformed_source_rows_fail_closed_without_payload_in_failure_reason(body: bytes, reason: str) -> None:
    result = _producer().produce(_release(), _distribution(), [body])

    assert result.receipt.probe_state == "failed_probe"
    assert result.receipt.failure_reason == reason
    assert result.rows == ()
    assert body.decode("utf-8") not in str(result.receipt.as_dict())


def test_stream_bounds_are_unacknowledged_and_non_byte_chunks_are_rejected() -> None:
    bounded = _producer(budget=StreamBudget(max_bytes=16, max_chunks=4, max_seconds=60))
    result = bounded.produce(_release(), _distribution(), [BODY])
    assert result.receipt.probe_state == "failed_probe"
    assert result.receipt.failure_reason == "stream_interrupted"
    assert result.receipt.acknowledged is False
    assert result.receipt.stream_state == "interrupted"

    with pytest.raises(CmsPosError, match="byte stream"):
        _producer().produce(_release(), _distribution(), ["not bytes"])  # type: ignore[list-item]


def test_catalog_rights_and_url_boundaries_are_enforced() -> None:
    catalog = InMemoryAdapterCatalog(
        [
            AdapterCatalogEntry(
                source_id=CMS_POS_SOURCE_ID,
                source_url=SOURCE_URL,
                change_mode="release_metadata",
                release_locator=RELEASE_LOCATOR,
                rights_status="approved_public",
                enabled=False,
            )
        ]
    )
    with pytest.raises(CmsPosError, match="enabled"):
        CmsPosProducer(catalog).produce(_release(), _distribution(), [BODY])

    with pytest.raises(CmsPosError, match="source_url"):
        CmsPosProducer(build_cms_pos_catalog()).produce(
            _release(source_url="https://other.example/cms-pos"), _distribution(), [BODY]
        )


def test_fixtures_are_schema_valid_and_duplicate_json_keys_are_rejected() -> None:
    for fixture in FIXTURE_ROOT.glob("*.json"):
        assert load_cms_pos_fixture(fixture)["schema_version"] == "hdp.cms-pos-producer.v1"

    duplicate = FIXTURE_ROOT / "official-source-noop.json"
    text = duplicate.read_text(encoding="utf-8")
    with pytest.raises(CmsPosError, match="duplicate JSON key"):
        from shared.acquisition.cms_pos import _strict_pairs

        import json

        json.loads(text.replace('"rows": [],', '"rows": [],\n  "rows": [],'), object_pairs_hook=_strict_pairs)


def test_release_fingerprint_is_order_and_timezone_stable() -> None:
    first = _release(published_at="2026-01-15T00:00:00Z")
    second = _release(published_at="2026-01-14T19:00:00-05:00")
    assert first.release_fingerprint == second.release_fingerprint

    with pytest.raises(CmsPosError, match="semantic fingerprint"):
        CmsPosRelease.from_mapping({**first.as_dict(), "release_fingerprint": "sha256:" + "0" * 64})


def test_receipt_fields_are_immutable_and_rows_do_not_expose_canonical_identity() -> None:
    _, result = _changed(preview_authorized=True)
    with pytest.raises(TypeError):
        result.rows[0].fields["FAC_NAME"] = "mutated"  # type: ignore[index]
    assert set(result.receipt.as_dict()) >= {
        "receipt_id",
        "probe_state",
        "distribution_fingerprint",
        "acknowledged",
    }
    assert all("entity_id" not in key.lower() for key in result.rows[0].fields)
