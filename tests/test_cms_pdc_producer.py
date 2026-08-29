"""Focused tests for the catalog-aware CMS PDC producer."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from shared.adapters import AdapterCatalogEntry, InMemoryAdapterCatalog, StreamBudget
from shared.acquisition.cms_pdc import (
    CMS_PDC_SOURCE_ID,
    CmsPdcCatalogEntry,
    CmsPdcError,
    CmsPdcProducer,
    CmsPdcReceipt,
    CmsPdcRelease,
    canonical_release_fingerprint,
    validate_cms_pdc_receipt,
)
from shared.acquisition.cms_pdc.contract import CmsPdcSchemaState, JsonValue, ProbeState, RightsStatus


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "contracts/healthcare-data-platform/cms-pdc/v1/cms-pdc.schema.json"
FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/cms-pdc/v1/fixtures"
T0 = datetime(2026, 8, 29, tzinfo=timezone.utc)
SCHEMA_SHA = "sha256:" + "a" * 64
DRIFT_SHA = "sha256:" + "b" * 64


def _catalog(
    *,
    enabled: bool = True,
    rights_status: str = "approved_public",
    schema_fingerprint: str = SCHEMA_SHA,
) -> CmsPdcCatalogEntry:
    return CmsPdcCatalogEntry(
        dataset_id="xubh-q36u",
        distribution_id="xubh-q36u-csv",
        dataset_title="Hospital General Information",
        distribution_title="Hospital General Information CSV",
        distribution_format="csv",
        source_url="https://data.cms.gov/provider-data/api/1/datastore/query",
        distribution_url="https://data.cms.gov/provider-data/sites/default/files/resources/xubh-q36u.csv",
        release_locator="https://data.cms.gov/provider-data",
        rights_status=cast(RightsStatus, rights_status),
        enabled=enabled,
        schema_fingerprint=schema_fingerprint,
    )


def _release(
    *,
    release_id: str = "release:cms:pdc:2026-08-29",
    modified_at: datetime = T0,
    schema_fingerprint: str = SCHEMA_SHA,
    probe_state: str = "changed",
    declared_content_sha256: str | None = None,
    dataset_id: str = "xubh-q36u",
    distribution_id: str = "xubh-q36u-csv",
) -> CmsPdcRelease:
    return CmsPdcRelease(
        dataset_id=dataset_id,
        distribution_id=distribution_id,
        release_id=release_id,
        modified_at=modified_at,
        schema_fingerprint=schema_fingerprint,
        source_url="https://data.cms.gov/provider-data/api/1/datastore/query",
        distribution_url="https://data.cms.gov/provider-data/sites/default/files/resources/xubh-q36u.csv",
        probe_state=cast(ProbeState, probe_state),
        declared_content_sha256=declared_content_sha256,
    )


def test_catalog_binding_requires_stable_ids_and_public_rights() -> None:
    with pytest.raises(CmsPdcError, match="stable identifier"):
        _catalog_entry_with(dataset_id="Hospital General Information")
    with pytest.raises(CmsPdcError, match="approved public rights"):
        CmsPdcProducer(_catalog(enabled=False, rights_status="pending_review"))
    with pytest.raises(CmsPdcError, match="disabled"):
        CmsPdcProducer(_catalog(enabled=False))


def _catalog_entry_with(**changes: object) -> CmsPdcCatalogEntry:
    values = _catalog().as_dict()
    values.update(changes)
    return CmsPdcCatalogEntry.from_mapping(values)


def test_adapter_catalog_registration_is_required_and_preserves_stable_metadata() -> None:
    adapter_entry = AdapterCatalogEntry(
        source_id=CMS_PDC_SOURCE_ID,
        source_url="https://data.cms.gov/provider-data/api/1/datastore/query",
        change_mode="etag",
        release_locator="https://data.cms.gov/provider-data",
        rights_status="approved_public",
    )
    producer = CmsPdcProducer.from_adapter_catalog(
        InMemoryAdapterCatalog([adapter_entry]),
        dataset_id="xubh-q36u",
        distribution_id="xubh-q36u-csv",
        dataset_title="Hospital General Information",
        distribution_title="Hospital General Information CSV",
        distribution_format="csv",
        distribution_url="https://data.cms.gov/provider-data/sites/default/files/resources/xubh-q36u.csv",
        schema_fingerprint=SCHEMA_SHA,
    )

    assert producer.catalog.source_id == CMS_PDC_SOURCE_ID
    assert producer.catalog.dataset_id == "xubh-q36u"
    assert producer.catalog.distribution_id == "xubh-q36u-csv"

    with pytest.raises(CmsPdcError, match="no source:cms:pdc"):
        CmsPdcProducer.from_adapter_catalog(
            InMemoryAdapterCatalog(
                [
                    AdapterCatalogEntry(
                        source_id="source:ahrq:lighthouse",
                        source_url="https://example.gov/ahrq",
                        change_mode="release_metadata",
                        release_locator="https://example.gov/ahrq/releases",
                        rights_status="approved_public",
                    )
                ]
            ),
            dataset_id="xubh-q36u",
            distribution_id="xubh-q36u-csv",
            dataset_title="Hospital",
            distribution_title="CSV",
            distribution_format="csv",
            distribution_url="https://data.cms.gov/provider-data/sites/default/files/resources/xubh-q36u.csv",
            schema_fingerprint=SCHEMA_SHA,
        )


def test_changed_release_has_stable_semantic_and_content_fingerprints() -> None:
    producer = CmsPdcProducer(_catalog())
    release = _release()
    receipt = producer.produce(release, [b"ccn,name\n", b"001,Example\n"])

    assert receipt.state == "changed"
    assert receipt.change_kind == "release"
    assert receipt.release_fingerprint == canonical_release_fingerprint(release)
    assert receipt.content_sha256 is not None
    assert receipt.acknowledged is True
    assert receipt.stream_state == "completed"
    assert receipt.received_bytes == len(b"ccn,name\n001,Example\n")
    assert receipt.chunk_count == 2
    assert receipt.as_dict()["record_type"] == "cms_pdc_receipt"
    assert "ccn,name" not in json.dumps(receipt.as_dict())


def test_same_release_and_content_is_replayed_without_changing_identity() -> None:
    producer = CmsPdcProducer(_catalog())
    release = _release()
    first = producer.produce(release, [b"one"])
    replay = producer.produce(release, [b"one"], prior=first)

    assert replay.state == "replayed"
    assert replay.change_kind == "replay"
    assert replay.content_sha256 == first.content_sha256
    assert replay.prior_receipt_id == first.receipt_id
    assert replay.current_projection_preserved is True
    assert replay.acknowledged is True


def test_release_metadata_noop_and_content_change_are_distinct() -> None:
    producer = CmsPdcProducer(_catalog())
    first_release = _release()
    first = producer.produce(first_release, [b"same-content"])

    metadata_only = producer.produce(
        _release(release_id="release:cms:pdc:2026-08-30", modified_at=T0 + timedelta(days=1)),
        [b"same-content"],
        prior=first,
    )
    content_changed = producer.produce(
        _release(),
        [b"different-content"],
        prior=first,
    )

    assert metadata_only.state == "no_op"
    assert metadata_only.change_kind == "none"
    assert metadata_only.current_projection_preserved is True
    assert content_changed.state == "changed"
    assert content_changed.change_kind == "content"
    assert content_changed.current_projection_preserved is False


def test_not_modified_probe_is_noop_without_consuming_chunks() -> None:
    producer = CmsPdcProducer(_catalog())
    prior = producer.produce(_release(), [b"known"])

    def forbidden_chunks() -> Iterable[bytes]:
        yield from ()
        raise AssertionError("not_modified must not consume the source stream")

    receipt = producer.produce(
        _release(
            release_id="release:cms:pdc:2026-08-30",
            modified_at=T0 + timedelta(days=1),
            probe_state="not_modified",
        ),
        forbidden_chunks(),
        prior=prior,
    )

    assert receipt.state == "no_op"
    assert receipt.stream_state == "not_started"
    assert receipt.received_bytes == 0
    assert receipt.content_sha256 == prior.content_sha256


def test_schema_drift_preserves_projection_and_does_not_consume_chunks() -> None:
    producer = CmsPdcProducer(_catalog())

    def forbidden_chunks() -> Iterable[bytes]:
        yield from ()
        raise AssertionError("schema drift must be rejected before streaming")

    receipt = producer.produce(_release(schema_fingerprint=DRIFT_SHA), forbidden_chunks())

    assert receipt.state == "schema_drift"
    assert receipt.change_kind == "schema_drift"
    assert receipt.schema_state == "drift"
    assert receipt.stream_state == "not_started"
    assert receipt.acknowledged is False
    assert receipt.current_projection_preserved is True
    assert receipt.content_sha256 is None


def test_bounded_stream_interruption_is_unacknowledged_and_recoverable() -> None:
    producer = CmsPdcProducer(_catalog(), budget=StreamBudget(max_bytes=3, max_chunks=2, max_seconds=10))
    receipt = producer.produce(_release(), [b"ab", b"cd"])

    assert receipt.state == "interrupted"
    assert receipt.change_kind == "interrupted"
    assert receipt.stream_state == "interrupted"
    assert receipt.acknowledged is False
    assert receipt.current_projection_preserved is True
    assert receipt.received_bytes == 2
    assert receipt.chunk_count == 1


def test_probe_failure_and_content_claim_mismatch_fail_closed() -> None:
    producer = CmsPdcProducer(_catalog())
    failed = producer.produce(_release(probe_state="failed_probe"), [])
    assert failed.state == "failed_probe"
    assert failed.change_kind == "failed"
    assert failed.stream_state == "not_started"
    assert failed.current_projection_preserved is True

    with pytest.raises(CmsPdcError, match="content fingerprint"):
        producer.produce(_release(declared_content_sha256="sha256:" + "c" * 64), [b"content"])


def test_release_and_prior_catalog_mismatches_are_rejected() -> None:
    producer = CmsPdcProducer(_catalog())
    with pytest.raises(CmsPdcError, match="dataset_id"):
        producer.produce(_release(dataset_id="other-dataset"), [])
    first = producer.produce(_release(), [b"one"])
    with pytest.raises(CmsPdcError, match="distribution_id"):
        producer.produce(_release(), [], prior={**first.as_dict(), "distribution_id": "other"})


def test_schema_and_checked_in_outcome_fixtures_validate() -> None:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    for path in sorted(FIXTURE_ROOT.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        assert list(validator.iter_errors(cast(JsonValue, raw))) == []
        receipt = CmsPdcReceipt.from_mapping(cast(Mapping[str, object], raw))
        assert receipt.as_dict() == raw
        assert validate_cms_pdc_receipt(cast(Mapping[str, object], raw)) == raw


def test_receipt_mapping_rejects_unknown_fields_and_invalid_schema_state() -> None:
    receipt = CmsPdcProducer(_catalog()).produce(_release(), [b"ok"])
    payload = receipt.as_dict()
    payload["unexpected"] = True
    with pytest.raises(CmsPdcError, match="schema validation"):
        CmsPdcReceipt.from_mapping(payload)

    with pytest.raises(CmsPdcError, match="schema_state"):
        CmsPdcReceipt(
            receipt_id=receipt.receipt_id,
            idempotency_key=receipt.idempotency_key,
            state=receipt.state,
            change_kind=receipt.change_kind,
            source_id=receipt.source_id,
            dataset_id=receipt.dataset_id,
            distribution_id=receipt.distribution_id,
            release_id=receipt.release_id,
            release_fingerprint=receipt.release_fingerprint,
            content_sha256=receipt.content_sha256,
            prior_release_fingerprint=receipt.prior_release_fingerprint,
            prior_content_sha256=receipt.prior_content_sha256,
            prior_receipt_id=receipt.prior_receipt_id,
            probe_state=receipt.probe_state,
            schema_state=cast(CmsPdcSchemaState, "invalid"),
            stream_state=receipt.stream_state,
            acknowledged=receipt.acknowledged,
            received_bytes=receipt.received_bytes,
            chunk_count=receipt.chunk_count,
            current_projection_preserved=receipt.current_projection_preserved,
            observed_at=receipt.observed_at,
            source_url=receipt.source_url,
            distribution_url=receipt.distribution_url,
            error=receipt.error,
        )
