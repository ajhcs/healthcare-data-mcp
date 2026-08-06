from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from shared.acquisition.irs_form_990_filing_index_receipt import (
    INDEX_COLUMNS,
    IrsForm990FilerQuery,
    IrsForm990FilerQueryScope,
    IrsForm990FilingIndexReceipt,
    IrsForm990ResponseMetadata,
    annual_index_url,
    build_irs_form_990_filing_index_receipt,
    load_irs_form_990_query_scope,
    validate_irs_form_990_filing_index_receipt,
    write_irs_form_990_filing_index_receipt,
)

ROOT = Path(__file__).resolve().parents[1]
SCOPE_PATH = ROOT / "contracts" / "source-receipts" / "irs-form-990-pilot-scope-v1.json"
SCHEMA_PATH = ROOT / "contracts" / "source-receipts" / "irs-form-990-filing-index-receipt-v1.schema.json"
RECEIPT_PATH = ROOT / "contracts" / "source-receipts" / "irs-form-990-pilot-receipt.json"
RETRIEVED_AT = datetime(2026, 8, 6, 12, 30, tzinfo=timezone.utc)


def _scope(*queries: IrsForm990FilerQuery) -> IrsForm990FilerQueryScope:
    return IrsForm990FilerQueryScope(
        schema_version="ushso.irs-form-990-filer-query-scope.v1",
        scope_id="test-exact-legal-filer-scope",
        scope_status="reviewed_source_query_scope",
        authority="source_custody_only_not_system_identity_or_public_release",
        reviewed_at=RETRIEVED_AT,
        source_basis="Test-only exact legal-filer query scope with no system relationship authority.",
        queries=queries,
        interpretation_limits=(
            "Legal filer only, not a health system.",
            "No ownership or affiliation inference.",
            "No public or financial authority.",
        ),
    )


def _query(*, expected_object_id: str | None = "202523169349306307") -> IrsForm990FilerQuery:
    return IrsForm990FilerQuery(
        query_id="ein-811244422-2024-2025",
        ein="811244422",
        tax_period_year=2024,
        filing_year=2025,
        expected_object_id=expected_object_id,
    )


def _write_index(path: Path, rows: list[tuple[str, ...]]) -> None:
    path.write_text(
        ",".join(INDEX_COLUMNS) + "\n" + "\n".join(",".join(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def _base_rows() -> list[tuple[str, ...]]:
    return [
        (
            "",
            "EFILE",
            "811244422",
            "202412",
            "2025",
            "PROVIDENCE ST JOSEPH HEALTH",
            "990",
            "93493316063075",
            "202523169349306307",
            "2025_TEOS_XML_11C",
        ),
        (
            "23999001",
            "EFILE",
            "811244422",
            "202412",
            "2025",
            "PROVIDENCE ST JOSEPH HEALTH",
            "990A",
            "93493316063076",
            "202523169349306308",
            "2025_TEOS_XML_11C",
        ),
        (
            "23999002",
            "EFILE",
            "811244422",
            "202412",
            "2025",
            "PROVIDENCE ST JOSEPH HEALTH",
            "990T",
            "93493316063077",
            "202523169349306309",
            "2025_TEOS_XML_11C",
        ),
        (
            "23999003",
            "EFILE",
            "470617373",
            "202412",
            "2025",
            "COMMONSPIRIT HEALTH",
            "990",
            "93493316063078",
            "202523169349306310",
            "2025_TEOS_XML_11C",
        ),
    ]


def _responses() -> dict[int, IrsForm990ResponseMetadata]:
    return {
        2025: IrsForm990ResponseMetadata(
            final_url=annual_index_url(2025),
            status_code=200,
            content_type="text/csv; charset=utf-8",
            retrieved_at=RETRIEVED_AT,
            etag="example-etag",
            last_modified="Wed, 06 Aug 2026 12:00:00 GMT",
        )
    }


def test_build_receipt_preserves_all_supported_matches_and_public_boundaries(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, _base_rows())

    receipt = build_irs_form_990_filing_index_receipt(
        {2025: index_path},
        responses=_responses(),
        scope=_scope(_query()),
    )

    assert receipt.assertions.query_count == 1
    assert receipt.assertions.selected_filing_count == 2
    assert receipt.assertions.original_return_count == 1
    assert receipt.assertions.amended_return_count == 1
    assert receipt.assertions.multi_filing_query_count == 1
    assert [filing.return_type for filing in receipt.filings] == ["990", "990A"]
    assert receipt.filings[0].return_id is None
    assert receipt.filings[1].amendment_status == "amended"
    assert receipt.query_results[0].matched_object_ids == (
        "202523169349306307",
        "202523169349306308",
    )
    assert receipt.query_results[0].expected_object_id_present is True
    assert {filing.ein for filing in receipt.filings} == {"811244422"}
    assert all("system" not in key for key in receipt.model_dump() if key != "caveats")


def test_unanchored_query_can_preserve_bounded_not_found_status(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, _base_rows())
    query = IrsForm990FilerQuery(
        query_id="ein-999999999-2024-2025",
        ein="999999999",
        tax_period_year=2024,
        filing_year=2025,
    )

    receipt = build_irs_form_990_filing_index_receipt(
        {2025: index_path},
        responses=_responses(),
        scope=_scope(query),
    )

    assert receipt.filings == ()
    assert receipt.query_results[0].status == "not_found_in_selected_annual_indexes"
    assert receipt.assertions.unmatched_query_count == 1
    assert "no supported row matched" in receipt.caveats[3]


def test_expected_object_id_is_a_fail_closed_anchor(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, _base_rows())

    with pytest.raises(ValueError, match="expected Object ID"):
        build_irs_form_990_filing_index_receipt(
            {2025: index_path},
            responses=_responses(),
            scope=_scope(_query(expected_object_id="202523169349399999")),
        )


def test_scope_rejects_duplicate_natural_query_key() -> None:
    duplicate = _query(expected_object_id="202523169349306308").model_copy(
        update={"query_id": "ein-811244422-2024-2025-amended"}
    )

    with pytest.raises(ValidationError, match="query keys must be unique"):
        _scope(_query(), duplicate)


def test_selected_malformed_source_row_fails_closed(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    malformed = list(_base_rows()[0])
    malformed[7] = "not-a-dln"
    _write_index(index_path, [tuple(malformed)])

    with pytest.raises(ValidationError, match="dln"):
        build_irs_form_990_filing_index_receipt(
            {2025: index_path},
            responses=_responses(),
            scope=_scope(_query()),
        )


@pytest.mark.parametrize(
    "row",
    [
        _base_rows()[0] + ("unexpected-overflow",),
        _base_rows()[0][:-1],
    ],
)
def test_source_row_width_drift_fails_closed(tmp_path: Path, row: tuple[str, ...]) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, [row])

    with pytest.raises(ValueError, match="does not match the header width"):
        build_irs_form_990_filing_index_receipt({2025: index_path}, responses=_responses(), scope=_scope(_query()))


def test_conflicting_duplicate_object_id_fails_closed(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    conflicting = list(_base_rows()[0])
    conflicting[5] = "CONFLICTING TAXPAYER NAME"
    _write_index(index_path, [_base_rows()[0], tuple(conflicting)])

    with pytest.raises(ValueError, match="conflicting canonical rows"):
        build_irs_form_990_filing_index_receipt(
            {2025: index_path},
            responses=_responses(),
            scope=_scope(_query()),
        )


def test_validation_is_idempotent_and_rejects_source_drift(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, _base_rows())
    scope = _scope(_query())
    receipt = build_irs_form_990_filing_index_receipt(
        {2025: index_path},
        responses=_responses(),
        scope=scope,
    )

    validate_irs_form_990_filing_index_receipt(receipt, {2025: index_path}, scope=scope)
    before = receipt.model_dump_json()
    validate_irs_form_990_filing_index_receipt(receipt, {2025: index_path}, scope=scope)
    assert receipt.model_dump_json() == before

    index_path.write_bytes(index_path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="receipt drifted"):
        validate_irs_form_990_filing_index_receipt(receipt, {2025: index_path}, scope=scope)


def test_response_and_schema_drift_fail_closed(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, _base_rows())
    wrong_response = _responses()
    wrong_response[2025] = IrsForm990ResponseMetadata(
        final_url="https://apps.irs.gov/unexpected.csv",
        status_code=200,
        content_type="text/csv",
        retrieved_at=RETRIEVED_AT,
    )

    with pytest.raises(ValueError, match="final URL drifted"):
        build_irs_form_990_filing_index_receipt({2025: index_path}, responses=wrong_response, scope=_scope(_query()))

    index_path.write_text("EIN,TAX_PERIOD\n811244422,202412\n", encoding="utf-8")
    with pytest.raises(ValueError, match="schema drift"):
        build_irs_form_990_filing_index_receipt({2025: index_path}, responses=_responses(), scope=_scope(_query()))


def test_write_receipt_is_public_safe(tmp_path: Path) -> None:
    index_path = tmp_path / "index_2025.csv"
    _write_index(index_path, _base_rows())
    receipt = build_irs_form_990_filing_index_receipt(
        {2025: index_path}, responses=_responses(), scope=_scope(_query())
    )
    destination = tmp_path / "receipt.json"

    write_irs_form_990_filing_index_receipt(destination, receipt)
    payload = json.loads(destination.read_text(encoding="utf-8"))
    encoded = json.dumps(payload)

    assert str(tmp_path) not in encoded
    assert "raw_payload" not in encoded
    assert payload["rights_classification"] == "public_free"
    assert payload["release_checksum_sha256"].startswith("sha256:")
    assert payload["artifacts"][0]["checksum_sha256"] == (
        "sha256:" + hashlib.sha256(index_path.read_bytes()).hexdigest()
    )


def test_committed_scope_is_exact_and_contains_no_system_relationships() -> None:
    scope = load_irs_form_990_query_scope(SCOPE_PATH)
    payload = scope.model_dump(mode="json")

    assert len(scope.queries) == 36
    assert len({query.ein for query in scope.queries}) == 16
    assert {query.filing_year for query in scope.queries} == {2024, 2025, 2026}
    assert all(query.expected_object_id for query in scope.queries)
    assert set(payload["queries"][0]) == {
        "query_id",
        "ein",
        "tax_period_year",
        "filing_year",
        "expected_object_id",
    }


def test_committed_receipt_is_typed_source_custody_only() -> None:
    receipt_payload = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    receipt = IrsForm990FilingIndexReceipt.model_validate(receipt_payload)

    assert receipt.assertions.model_dump() == {
        "artifact_count": 3,
        "query_count": 36,
        "matched_query_count": 36,
        "unmatched_query_count": 0,
        "selected_filing_count": 36,
        "unique_ein_count": 16,
        "original_return_count": 36,
        "amended_return_count": 0,
        "multi_filing_query_count": 0,
    }
    assert [artifact.checksum_sha256 for artifact in receipt.artifacts] == [
        "sha256:c00051a33f65d408ea7f6d5fa008c7f82f9bc6223751f4a6e29d59f5e17f826d",
        "sha256:e54ba6938d98b1746f2fd464a0f77e520efd956dc3da737d73843dc0013c6770",
        "sha256:00c1d156ef89fc676c2a3f59c81100dc0d9f7601d251fdfcd02ba58c0877110f",
    ]
    assert receipt.release_checksum_sha256 == (
        "sha256:0a8e5c3ec965b98cbb6d6a51b6ffea9dffa212a11d2394418e09d2dc693d2797"
    )
    scope = load_irs_form_990_query_scope(SCOPE_PATH)
    canonical_scope = json.dumps(scope.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert receipt.scope_checksum_sha256 == ("sha256:" + hashlib.sha256(canonical_scope).hexdigest())
    forbidden_keys = {
        "absolute_path",
        "ahrq_system_id",
        "canonical_id",
        "enterprise_identity",
        "fast_id",
        "hsid",
        "local_path",
        "manager",
        "operator",
        "owner",
        "ownership",
        "public_release_authority",
        "raw_payload",
        "registry_id",
    }

    def keys(value: object) -> set[str]:
        if isinstance(value, dict):
            return set(value) | {key for child in value.values() for key in keys(child)}
        if isinstance(value, list):
            return {key for child in value for key in keys(child)}
        return set()

    assert keys(receipt_payload).isdisjoint(forbidden_keys)
    assert (
        list(Draft202012Validator(IrsForm990FilingIndexReceipt.model_json_schema()).iter_errors(receipt_payload)) == []
    )


def test_committed_receipt_rejects_canonical_row_hash_tampering() -> None:
    payload = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    payload["filings"][0]["canonical_row_sha256"] = "sha256:" + "0" * 64

    with pytest.raises(ValidationError, match="canonical row hash"):
        IrsForm990FilingIndexReceipt.model_validate(payload)


def test_committed_receipt_rejects_release_checksum_tampering() -> None:
    payload = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    payload["release_checksum_sha256"] = "sha256:" + "0" * 64

    with pytest.raises(ValidationError, match="release checksum"):
        IrsForm990FilingIndexReceipt.model_validate(payload)


def test_committed_receipt_rejects_provenance_metadata_tampering() -> None:
    payload = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    payload["artifacts"][0]["etag"] = '"tampered-etag"'

    with pytest.raises(ValidationError, match="release checksum"):
        IrsForm990FilingIndexReceipt.model_validate(payload)


def test_committed_receipt_rejects_interpretation_boundary_tampering() -> None:
    payload = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    payload["caveats"][0] = "Exact legal-filer rows establish system identity."

    with pytest.raises(ValidationError, match="fixed interpretation boundary"):
        IrsForm990FilingIndexReceipt.model_validate(payload)


def test_committed_receipt_rejects_query_edge_tampering() -> None:
    payload = json.loads(RECEIPT_PATH.read_text(encoding="utf-8"))
    payload["filings"][0]["query_ids"] = [payload["query_results"][1]["query_id"]]

    with pytest.raises(ValidationError, match="edges do not reconcile"):
        IrsForm990FilingIndexReceipt.model_validate(payload)


def test_checked_in_schema_matches_runtime_model() -> None:
    expected = json.dumps(IrsForm990FilingIndexReceipt.model_json_schema(), indent=2, sort_keys=True) + "\n"
    assert SCHEMA_PATH.read_text(encoding="utf-8") == expected
    scope = json.loads(SCOPE_PATH.read_text(encoding="utf-8"))
    assert list(Draft202012Validator(IrsForm990FilerQueryScope.model_json_schema()).iter_errors(scope)) == []
