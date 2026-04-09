"""Tests for Google CSE quota management in web-intelligence."""

import httpx
import pytest

from servers.web_intelligence import search_client


@pytest.mark.asyncio
async def test_search_uses_in_process_cache(monkeypatch):
    calls = {"count": 0}

    class DummyResponse:
        def json(self):
            return {"items": [{"title": "Result"}]}

    async def fake_request(*args, **kwargs):
        calls["count"] += 1
        return DummyResponse()

    monkeypatch.setenv("GOOGLE_CSE_API_KEY", "key")
    monkeypatch.setenv("GOOGLE_CSE_ID", "cx")
    monkeypatch.setenv("GOOGLE_CSE_SESSION_LIMIT", "5")
    search_client._reset_runtime_state()
    monkeypatch.setattr(search_client, "resilient_request", fake_request)

    first = await search_client.search("cleveland clinic")
    second = await search_client.search("cleveland clinic")

    assert calls["count"] == 1
    assert first["items"][0]["title"] == "Result"
    assert second["_search_meta"]["cached"] is True
    assert search_client.get_quota_status()["cache_hits"] == 1


@pytest.mark.asyncio
async def test_search_enforces_session_limit(monkeypatch):
    class DummyResponse:
        def json(self):
            return {"items": []}

    async def fake_request(*args, **kwargs):
        return DummyResponse()

    monkeypatch.setenv("GOOGLE_CSE_API_KEY", "key")
    monkeypatch.setenv("GOOGLE_CSE_ID", "cx")
    monkeypatch.setenv("GOOGLE_CSE_SESSION_LIMIT", "1")
    search_client._reset_runtime_state()
    monkeypatch.setattr(search_client, "resilient_request", fake_request)

    first = await search_client.search("query one")
    second = await search_client.search("query two")

    assert "items" in first
    assert "session quota reached" in second["error"].lower()
    assert second["quota"]["requests_made"] == 1


@pytest.mark.asyncio
async def test_search_handles_google_quota_error(monkeypatch):
    request = httpx.Request("GET", "https://www.googleapis.com/customsearch/v1")
    response = httpx.Response(
        403,
        request=request,
        json={
            "error": {
                "message": "Daily Limit Exceeded",
                "errors": [{"reason": "dailyLimitExceeded"}],
            }
        },
    )

    async def fake_request(*args, **kwargs):
        raise httpx.HTTPStatusError("quota", request=request, response=response)

    monkeypatch.setenv("GOOGLE_CSE_API_KEY", "key")
    monkeypatch.setenv("GOOGLE_CSE_ID", "cx")
    search_client._reset_runtime_state()
    monkeypatch.setattr(search_client, "resilient_request", fake_request)

    result = await search_client.search("quota me")

    assert "quota exceeded" in result["error"].lower() or "rate-limited" in result["error"].lower()
    assert result["quota"]["quota_errors"] == 1
    assert result["quota"]["blocked_until_epoch"] is not None
