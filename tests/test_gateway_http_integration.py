"""Streamable HTTP auth integration tests for gateway servers."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def _running_gateway(module: str, env_updates: dict[str, str]) -> Iterator[int]:
    port = _free_port()
    env = os.environ.copy()
    env.update(
        {
            "MCP_TRANSPORT": "streamable-http",
            "MCP_HOST": "127.0.0.1",
            "MCP_PORT": str(port),
            "SEC_USER_AGENT": "healthcare-data-mcp tests@example.com",
            **env_updates,
        }
    )
    process = subprocess.Popen(
        [sys.executable, "-m", module],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_port(port, process)
        yield port
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _wait_for_port(port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.time() + 15
    while time.time() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(f"gateway process exited early: stdout={stdout[-500:]} stderr={stderr[-500:]}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(f"gateway process did not listen on port {port}")


async def _list_tools(url: str, *, token: str | None = None) -> list[str]:
    headers = {"Authorization": f"Bearer {token}"} if token else None
    async with httpx.AsyncClient(headers=headers, timeout=5) as http_client:
        async with streamable_http_client(url, http_client=http_client) as (
            read_stream,
            write_stream,
            _get_session_id,
        ):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                tools = await session.list_tools()
                return sorted(tool.name for tool in tools.tools)


async def _call_tool(url: str, tool_name: str, arguments: dict | None = None, *, token: str | None = None) -> dict:
    result = await _call_tool_result(url, tool_name, arguments, token=token)
    return result.structuredContent or {}


async def _call_tool_result(url: str, tool_name: str, arguments: dict | None = None, *, token: str | None = None):
    headers = {"Authorization": f"Bearer {token}"} if token else None
    async with httpx.AsyncClient(headers=headers, timeout=5) as http_client:
        async with streamable_http_client(url, http_client=http_client) as (
            read_stream,
            write_stream,
            _get_session_id,
        ):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                return await session.call_tool(tool_name, arguments or {})


@pytest.mark.asyncio
async def test_metadata_gateway_http_requires_and_accepts_bearer_auth() -> None:
    token = "this-is-a-long-gateway-token"
    with _running_gateway(
        "servers.gateway.server",
        {
            "MCP_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_GATEWAY_BEARER_TOKEN": token,
            "MCP_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
    ) as port:
        url = f"http://127.0.0.1:{port}/mcp"

        with pytest.raises(ExceptionGroup):
            await _list_tools(url)
        with pytest.raises(ExceptionGroup):
            await _list_tools(url, token="this-is-the-wrong-token")

        tools = await _list_tools(url, token=token)

    assert tools == ["fetch", "search"]


@pytest.mark.asyncio
async def test_live_gateway_http_requires_auth_and_exposes_policy_tools() -> None:
    token = "this-is-a-long-live-gateway-token"
    with _running_gateway(
        "servers.live_gateway.server",
        {
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": token,
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
    ) as port:
        url = f"http://127.0.0.1:{port}/mcp"

        with pytest.raises(ExceptionGroup):
            await _list_tools(url)
        with pytest.raises(ExceptionGroup):
            await _list_tools(url, token="this-is-the-wrong-live-token")

        tools = await _list_tools(url, token=token)
        inventory = await _call_tool(url, "list_live_tools", token=token)

    assert "list_live_tools" in tools
    assert "get_live_gateway_audit_events" in tools
    assert "search_provider_enrollment" in tools
    assert inventory["gateway"] == "live-gateway"
    assert inventory["policy"]["gateway_type"] == "live_policy_gateway"
    assert any(tool["name"] == "screen_leie_batch" and "mcp:bulk" in tool["allowed_scopes"] for tool in inventory["tools"])


@pytest.mark.asyncio
async def test_live_gateway_http_blocks_bulk_tool_without_bulk_scope() -> None:
    token = "this-is-a-long-live-gateway-token"
    second_token = "this-is-another-live-gateway-token"
    with _running_gateway(
        "servers.live_gateway.server",
        {
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_LIVE_GATEWAY_BEARER_TOKENS": f"{token},{second_token}",
            "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "mcp:read",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
    ) as port:
        url = f"http://127.0.0.1:{port}/mcp"

        result = await _call_tool_result(url, "screen_leie_batch", {"candidates": []}, token=token)
        second_result = await _call_tool_result(url, "screen_leie_batch", {"candidates": []}, token=second_token)
        audit = await _call_tool(url, "get_live_gateway_audit_events", {"limit": 5}, token=token)

    error_text = " ".join(str(getattr(item, "text", "")) for item in result.content)
    assert result.isError is True
    assert second_result.isError is True
    assert "requires scope" in error_text
    blocked_events = [event for event in audit["events"] if event["tool"] == "screen_leie_batch"]
    assert len(blocked_events) >= 2
    assert blocked_events[-1]["outcome"] == "blocked"
    assert blocked_events[-1]["reason"] == "missing_scope"
    assert blocked_events[-1]["subject"].startswith("healthcare-data-mcp-gateway:")
    assert blocked_events[-2]["subject"].startswith("healthcare-data-mcp-gateway:")
    assert blocked_events[-1]["subject"] != blocked_events[-2]["subject"]


@pytest.mark.asyncio
async def test_live_gateway_http_routes_authenticated_domain_tool_with_provenance() -> None:
    token = "this-is-a-long-live-gateway-token"
    with _running_gateway(
        "servers.live_gateway.server",
        {
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": token,
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
    ) as port:
        url = f"http://127.0.0.1:{port}/mcp"

        result = await _call_tool(url, "get_sam_exclusions_metadata", token=token)
        audit = await _call_tool(url, "get_live_gateway_audit_events", {"limit": 5}, token=token)

    assert result["source_name"] == "SAM.gov Exclusions"
    assert result["source_metadata"]["source_name"] == "SAM.gov Exclusions"
    assert result["evidence"]["dataset_id"] == "sam_gov_exclusions"
    assert result["evidence"]["match_basis"] == "source_metadata_lookup"
    assert result["live_gateway_policy"]["gateway"] == "live-gateway"
    assert result["live_gateway_policy"]["tool"] == "get_sam_exclusions_metadata"
    assert result["live_gateway_policy"]["source_caveat_class"] == "exclusion_screening"
    assert result["live_gateway_policy"]["provenance_status"]["status"] == "evidence_receipt_valid"
    assert result["live_gateway_policy"]["provenance_status"]["source_claim_paths_status"] == "source_claim_paths_valid"
    assert result["live_gateway_policy"]["provenance_status"]["source_claim_paths_valid"] is True
    event = next(event for event in reversed(audit["events"]) if event["tool"] == "get_sam_exclusions_metadata")
    assert event["outcome"] == "allowed"
    assert event["reason"] == "policy_passed"
    assert event["provenance_status"] == "evidence_receipt_valid"
    assert event["source_claim_paths_status"] == "source_claim_paths_valid"
    assert event["source_claim_paths_valid"] is True
    assert event["source_metadata_present"] is True
    assert "source_metadata" not in event
    assert "evidence" not in event


@pytest.mark.asyncio
async def test_live_gateway_http_rejects_sensitive_identifier_arguments_before_routing() -> None:
    token = "this-is-a-long-live-gateway-token"
    with _running_gateway(
        "servers.live_gateway.server",
        {
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN": token,
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "127.0.0.1:*",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://127.0.0.1:*",
        },
    ) as port:
        url = f"http://127.0.0.1:{port}/mcp"

        result = await _call_tool_result(
            url,
            "screen_leie_batch",
            {"candidates": [{"candidate_id": "1", "entity_name": "Example Health", "ssn": "123-45-6789"}]},
            token=token,
        )
        audit = await _call_tool(url, "get_live_gateway_audit_events", {"limit": 5}, token=token)

    error_text = " ".join(str(getattr(item, "text", "")) for item in result.content)
    assert result.isError is True
    assert "sensitive identifier key" in error_text
    assert audit["events"][-1]["tool"] == "screen_leie_batch"
    assert audit["events"][-1]["outcome"] == "blocked"
    assert audit["events"][-1]["reason"] == "sensitive_argument_key_rejected"
    assert audit["events"][-1]["sensitive_argument_keys"] == ["candidates[0].ssn"]
    assert "123-45-6789" not in str(audit["events"][-1])
