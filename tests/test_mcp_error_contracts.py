"""Structured MCP error and input-normalization tests."""

from __future__ import annotations

import pytest

from servers.gateway import server as gateway_server
from shared.utils.input_normalization import normalize_ccn, normalize_npi, normalize_state, normalize_zcta
from shared.utils.mcp_response import ToolExecutionError, error_response, not_found_response


def test_error_response_has_machine_parseable_recovery_payload() -> None:
    result = error_response(
        "No row found",
        code="not_found",
        error_type="NOT_FOUND",
        recoverable=True,
        fix_hint="Search first.",
        available_options=["cms-facility"],
        suggested_tool_calls=[{"tool": "search", "arguments": {"query": "CMS"}}],
    )

    assert result["ok"] is False
    assert result["error"]["type"] == "NOT_FOUND"
    assert result["error"]["recoverable"] is True
    assert result["error"]["data"]["fix_hint"] == "Search first."
    assert result["error"]["data"]["available_options"] == ["cms-facility"]
    assert result["error"]["data"]["suggested_tool_calls"][0]["tool"] == "search"


def test_not_found_response_preserves_backward_code() -> None:
    result = not_found_response("Missing", available_options=["a"])

    assert result["error"]["code"] == "not_found"
    assert result["error"]["type"] == "NOT_FOUND"
    assert result["error"]["data"]["available_options"] == ["a"]


def test_tool_execution_error_payload_has_consistent_envelope() -> None:
    payload = ToolExecutionError(
        "INVALID_ARGUMENT",
        "Bad identifier",
        data={"fix_hint": "Search first."},
    ).to_payload()

    assert payload["ok"] is False
    assert payload["error"]["type"] == "INVALID_ARGUMENT"
    assert payload["error"]["data"]["fix_hint"] == "Search first."


def test_identifier_normalizers_detect_placeholders_and_names() -> None:
    ccn, mistake = normalize_ccn("<ccn>")
    assert ccn == ""
    assert mistake is not None
    assert mistake.error_type == "PLACEHOLDER_INPUT"

    ccn, mistake = normalize_ccn("Example Hospital")
    assert ccn == ""
    assert mistake is not None
    assert mistake.error_type == "NAME_USED_FOR_EXACT_ID"

    assert normalize_ccn("123")[0] == "000123"
    assert normalize_npi("123-456-7890")[0] == "1234567890"
    assert normalize_zcta("9021")[0] == "09021"
    assert normalize_state("pa")[0] == "PA"


@pytest.mark.asyncio
async def test_gateway_fetch_unknown_dataset_returns_recovery_hints() -> None:
    result = await gateway_server.fetch("cms-facilty")

    assert result["ok"] is False
    assert result["error"]["type"] == "NOT_FOUND"
    assert "cms-facility" in result["error"]["data"]["available_options"]
    assert result["error"]["data"]["suggested_tool_calls"][0]["tool"] == "search"
    assert "cms-facility" in result["suggestions"]
