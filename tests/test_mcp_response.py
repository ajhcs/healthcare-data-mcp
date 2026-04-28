"""Tests for shared FastMCP response helpers."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.tools import Tool
from pydantic import BaseModel

from shared.utils.mcp_response import (
    REPORT_SOURCE_METADATA_FIELDS,
    ReportIngestContractError,
    collection_response,
    empty_response,
    error_response,
    pagination_meta,
    raise_invalid_params,
    record_response,
    response_envelope,
    to_structured,
    tool_error,
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
