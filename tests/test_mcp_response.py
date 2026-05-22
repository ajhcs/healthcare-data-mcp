"""Tests for shared FastMCP response helpers."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.tools import Tool
from pydantic import BaseModel

from shared.utils.mcp_response import (
    EVIDENCE_RECEIPT_FIELDS,
    EVIDENCE_RECEIPT_REQUIRED_CONTENT_FIELDS,
    REPORT_SOURCE_METADATA_FIELDS,
    ReportIngestContractError,
    collection_response,
    empty_response,
    evidence_receipt,
    evidence_receipt_validation_summary,
    evidence_receipts_in_payload,
    error_response,
    pagination_meta,
    raise_invalid_params,
    record_response,
    response_envelope,
    to_structured,
    tool_error,
    validate_evidence_receipt,
    validate_report_ingest_payload,
)


class Facility(BaseModel):
    ccn: str
    name: str
    opened_on: date


def test_to_structured_converts_pydantic_models_and_dates() -> None:
    facility = Facility(ccn="390001", name="Jefferson Main", opened_on=date(1995, 4, 12))

    assert to_structured(facility) == {
        "ccn": "390001",
        "name": "Jefferson Main",
        "opened_on": "1995-04-12",
    }


def test_response_envelope_keeps_success_shape_json_compatible() -> None:
    response = response_envelope(
        data=Facility(ccn="390001", name="Jefferson Main", opened_on=date(1995, 4, 12)),
        source="cms",
        ignored_none=None,
    )

    assert response == {
        "ok": True,
        "data": {
            "ccn": "390001",
            "name": "Jefferson Main",
            "opened_on": "1995-04-12",
        },
        "source": "cms",
    }


def test_collection_response_adds_count_and_pagination_metadata() -> None:
    response = collection_response(
        (Facility(ccn=str(index), name=f"Facility {index}", opened_on=date(2020, 1, index)) for index in (1, 2)),
        limit=2,
        offset=4,
        total=10,
        query="acute care",
    )

    assert response["ok"] is True
    assert response["count"] == 2
    assert response["query"] == "acute care"
    assert response["results"] == [
        {"ccn": "1", "name": "Facility 1", "opened_on": "2020-01-01"},
        {"ccn": "2", "name": "Facility 2", "opened_on": "2020-01-02"},
    ]
    assert response["meta"]["pagination"] == {
        "count": 2,
        "limit": 2,
        "offset": 4,
        "total": 10,
        "has_more": True,
        "next_offset": 6,
    }


def test_pagination_meta_uses_limit_when_total_is_unknown() -> None:
    full_page = pagination_meta(count=25, limit=25, offset=50)
    partial_page = pagination_meta(count=10, limit=25, offset=75)

    assert full_page["has_more"] is True
    assert full_page["next_offset"] == 75
    assert partial_page["has_more"] is False
    assert partial_page["next_offset"] is None


def test_record_response_uses_named_key() -> None:
    response = record_response(Facility(ccn="390001", name="Jefferson Main", opened_on=date(1995, 4, 12)), key="facility")

    assert response == {
        "ok": True,
        "facility": {
            "ccn": "390001",
            "name": "Jefferson Main",
            "opened_on": "1995-04-12",
        },
    }


def test_empty_response_returns_zero_result_success() -> None:
    assert empty_response("No facilities found") == {
        "ok": True,
        "results": [],
        "count": 0,
        "message": "No facilities found",
    }


def test_error_response_is_structured_without_raising() -> None:
    response = error_response("Upstream unavailable", code="cms_unavailable", detail={"status": 503}, retryable=True)

    assert response == {
        "ok": False,
        "error": {
            "code": "cms_unavailable",
            "message": "Upstream unavailable",
            "retryable": True,
            "detail": {"status": 503},
        },
    }


def test_tool_error_formats_fastmcp_tool_error() -> None:
    error = tool_error("limit must be positive", code="invalid_params", detail={"limit": 0})

    assert isinstance(error, ToolError)
    assert str(error) == 'invalid_params: limit must be positive | detail={"limit":0}'


def test_raise_invalid_params_raises_tool_error() -> None:
    with pytest.raises(ToolError, match="invalid_params: state is required"):
        raise_invalid_params("state is required")


@pytest.mark.asyncio
async def test_fastmcp_accepts_helper_response_as_structured_output() -> None:
    def migrated_tool() -> dict[str, Any]:
        return collection_response([Facility(ccn="390001", name="Jefferson Main", opened_on=date(1995, 4, 12))])

    tool = Tool.from_function(migrated_tool, structured_output=True)
    converted = await tool.run({}, convert_result=True)

    assert tool.output_schema is not None
    _content, structured = converted
    assert structured["ok"] is True
    assert structured["count"] == 1
    assert structured["results"][0]["opened_on"] == "1995-04-12"


def test_report_ingest_payload_requires_source_metadata_on_nested_fact_rows() -> None:
    payload = {
        "sections": [
            {
                "name": "ownership",
                "fact_rows": [
                    {
                        "label": "Owner",
                        "value": "Jefferson Parent LLC",
                        **{field: "source" for field in REPORT_SOURCE_METADATA_FIELDS if field != "query"},
                        "query": {"ccn": "390001"},
                    }
                ],
            }
        ]
    }

    validate_report_ingest_payload(payload)

    del payload["sections"][0]["fact_rows"][0]["source_url"]
    with pytest.raises(ReportIngestContractError, match="source_url"):
        validate_report_ingest_payload(payload)


def test_report_ingest_payload_can_require_final_report_content() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        source_period="2025 public file",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="ready",
        match_basis="ccn_exact_measure_id_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve source period and measure caveats.",
        next_step="Verify the cited value against the CMS row before publication.",
    )
    payload = {
        "fact_rows": [
            {
                "label": "CLABSI SIR",
                "value": "0.82",
                **receipt,
            }
        ]
    }

    validate_report_ingest_payload(payload, require_content=True, allow_placeholders=False)


def test_report_ingest_payload_can_require_workflow_identity_context() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        source_period="2025 public file",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="ready",
        match_basis="ccn_exact_measure_id_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve source period and measure caveats.",
        next_step="Verify the cited value against the CMS row before publication.",
    )
    payload = {
        "fact_rows": [
            {
                "label": "CLABSI SIR",
                "value": "0.82",
                "identity_fields": ["ccn", "measure_id"],
                "identity_path": "hospital_quality.get_quality_measure_rows.identity",
                "identity_map_path": "hospital_quality.get_quality_measure_rows.identity_map",
                **receipt,
            }
        ]
    }

    validate_report_ingest_payload(
        payload,
        require_content=True,
        allow_placeholders=False,
        require_identity_context=True,
    )


def test_report_ingest_payload_identity_context_accepts_copied_identity_objects() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="ready",
        match_basis="ccn_exact_measure_id_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve source period and measure caveats.",
        next_step="Verify the cited value against the CMS row before publication.",
    )
    payload = {
        "fact_rows": [
            {
                "label": "CLABSI SIR",
                "value": "0.82",
                "identity_fields": ["ccn", "measure_id"],
                "identity": {"ccn": "390223"},
                "identity_map": {"join_keys": [{"field": "ccn", "values": ["390223"]}]},
                **receipt,
            }
        ]
    }

    validate_report_ingest_payload(payload, require_content=True, require_identity_context=True)


def test_report_ingest_payload_identity_context_rejects_dropped_workflow_identity() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="ready",
        match_basis="ccn_exact_measure_id_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve source period and measure caveats.",
        next_step="Verify the cited value against the CMS row before publication.",
    )
    payload = {"fact_rows": [{"label": "CLABSI SIR", "value": "0.82", **receipt}]}

    with pytest.raises(ReportIngestContractError, match="missing identity context") as excinfo:
        validate_report_ingest_payload(payload, require_content=True, require_identity_context=True)

    message = str(excinfo.value)
    assert "identity_fields" in message
    assert "identity_path_or_identity" in message
    assert "identity_map_path_or_identity_map" in message


def test_report_ingest_payload_final_mode_rejects_workflow_placeholders() -> None:
    payload = {
        "fact_rows": [
            {
                "label": "Template row",
                "value": "<pending>",
                **{
                    field: f"copy_from_tool_evidence.{field}"
                    for field in REPORT_SOURCE_METADATA_FIELDS
                    if field != "query"
                },
                "query": {"ccn": "390223"},
            }
        ]
    }

    validate_report_ingest_payload(payload)
    with pytest.raises(ReportIngestContractError, match="workflow evidence placeholders"):
        validate_report_ingest_payload(payload, require_content=True, allow_placeholders=False)


def test_report_ingest_payload_final_mode_rejects_incomplete_content() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        match_basis="ccn_exact_measure_id_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve source period and measure caveats.",
        next_step="Verify the cited value against the CMS row before publication.",
    )
    payload = {"fact_rows": [{"label": "CLABSI SIR", "value": "0.82", **receipt}]}

    with pytest.raises(ReportIngestContractError, match="invalid evidence content") as excinfo:
        validate_report_ingest_payload(payload, require_content=True, allow_placeholders=False)

    assert "source_period_or_retrieved_at_or_source_modified" in str(excinfo.value)
    assert "cache_status_or_cache_freshness" in str(excinfo.value)


def test_evidence_receipt_exposes_canonical_fields_from_source_metadata() -> None:
    receipt = evidence_receipt(
        source_metadata={
            "source_name": "CMS Hospital Quality",
            "source_url": "https://data.cms.gov/example.csv",
            "dataset_id": "abc123",
            "downloaded_at": "2026-05-01T00:00:00+00:00",
            "cache_status": "ready",
            "cache_age_days": 2.5,
        },
        source_period="2025 Q4",
        match_basis="ccn_exact",
        confidence="high",
        caveat="Exact public source row only.",
        next_step="Verify the CCN if no row is returned.",
    )

    assert set(EVIDENCE_RECEIPT_FIELDS) <= set(receipt)
    assert receipt["source_name"] == "CMS Hospital Quality"
    assert receipt["retrieved_at"] == "2026-05-01T00:00:00+00:00"
    assert receipt["cache_freshness"] == "ready; age_days=2.5"
    assert receipt["match_basis"] == "ccn_exact"
    validate_evidence_receipt(receipt)


def test_validate_evidence_receipt_rejects_missing_fields() -> None:
    with pytest.raises(ReportIngestContractError, match="dataset_id"):
        validate_evidence_receipt({"source_name": "CMS"})


def test_validate_evidence_receipt_can_require_report_ready_content() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="ready",
        match_basis="ccn_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve measure period and source caveats.",
        next_step="Copy the row evidence receipt into the report fact row.",
    )

    assert set(EVIDENCE_RECEIPT_REQUIRED_CONTENT_FIELDS) <= set(receipt)
    validate_evidence_receipt(receipt, require_content=True)


def test_validate_evidence_receipt_content_requires_source_period_or_retrieval_and_cache_status() -> None:
    receipt = evidence_receipt(
        source_name="CMS Hospital Quality",
        source_url="https://data.cms.gov/provider-data",
        dataset_id="cms_hospital_quality",
        match_basis="ccn_exact",
        confidence="high",
        caveat="Public CMS quality row; preserve measure period and source caveats.",
        next_step="Copy the row evidence receipt into the report fact row.",
    )

    with pytest.raises(ReportIngestContractError) as excinfo:
        validate_evidence_receipt(receipt, require_content=True)

    message = str(excinfo.value)
    assert "source_period_or_retrieved_at_or_source_modified" in message
    assert "cache_status_or_cache_freshness" in message
    assert "source_url_or_landing_page" not in message


def test_evidence_receipt_validation_summary_walks_nested_payloads_without_leaking_receipts() -> None:
    receipt = evidence_receipt(
        source_name="CMS Provider Enrollment",
        source_url="https://data.cms.gov/provider-enrollment",
        dataset_id="cms-provider-enrollment",
        retrieved_at="2026-05-22T00:00:00Z",
        cache_status="hit",
        match_basis="npi_exact_row",
        confidence="high",
        caveat="Public enrollment rows require source-system verification before operational decisions.",
        next_step="Review enrollment detail before citing the finding.",
    )
    payload = {
        "results": [
            {
                "npi": "1234567893",
                "evidence": receipt,
            }
        ],
        "evidence_receipt": receipt,
    }

    paths = [path for path, _receipt in evidence_receipts_in_payload(payload)]
    summary = evidence_receipt_validation_summary(payload, require_content=True)

    assert paths == ["result.results[0].evidence", "result.evidence_receipt"]
    assert summary == {
        "status": "evidence_receipt_valid",
        "receipt_count": 2,
        "evidence_present": True,
        "evidence_valid": True,
    }


def test_evidence_receipt_validation_summary_reports_invalid_nested_receipt_paths() -> None:
    payload = {
        "results": [
            {
                "npi": "1234567893",
                "evidence": {"source_name": "CMS Provider Enrollment"},
            }
        ]
    }

    summary = evidence_receipt_validation_summary(payload, require_content=True)

    assert summary["status"] == "evidence_receipt_invalid"
    assert summary["receipt_count"] == 1
    assert summary["evidence_present"] is True
    assert summary["evidence_valid"] is False
    assert summary["invalid_evidence_paths"][0]["path"] == "result.results[0].evidence"
    assert "dataset_id" in summary["invalid_evidence_paths"][0]["error"]


def test_evidence_receipt_validation_summary_reports_missing_receipts() -> None:
    summary = evidence_receipt_validation_summary({"results": [{"npi": "1234567893"}]}, require_content=True)

    assert summary == {
        "status": "evidence_receipt_missing",
        "receipt_count": 0,
        "evidence_present": False,
        "evidence_valid": False,
    }
