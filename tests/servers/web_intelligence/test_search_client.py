"""Tests for Google CSE quota management in web-intelligence."""

import httpx
import pytest

from servers.web_intelligence import search_client
from servers.web_intelligence import server


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


@pytest.mark.asyncio
async def test_search_web_returns_bounded_results(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {
            "items": [
                {
                    "title": "Example Health",
                    "link": "https://example.org/about",
                    "snippet": "About Example Health",
                    "displayLink": "example.org",
                }
            ],
            "_search_meta": {"cached": False, "requests_made": 1},
        }

    monkeypatch.setattr(search_client, "search", fake_search)

    result = await server.search_web("Example Health", max_results=3, site_search="example.org")

    assert result["query"] == "Example Health"
    assert result["count"] == 1
    assert result["results"][0]["link"] == "https://example.org/about"
    assert result["metadata"]["source"] == "google_cse"


@pytest.mark.asyncio
async def test_fetch_web_page_extracts_static_text(monkeypatch):
    html = """
    <html>
      <head><title>Example</title><meta name="description" content="Example description"></head>
      <body><header>Navigation</header><main><p>Visible healthcare page text.</p></main></body>
    </html>
    """

    async def fake_fetch_and_parse(url):
        from bs4 import BeautifulSoup

        return html, BeautifulSoup(html, "lxml")

    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.fetch_web_page("https://example.org/about", max_chars=1000)

    assert result["url"] == "https://example.org/about"
    assert result["title"] == "Example"
    assert "Visible healthcare page text." in result["text"]
    assert result["meta"]["meta_description"] == "Example description"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/status",
        "http://metadata.google.internal/latest/meta-data/",
        "http://router.lan/status",
        "http://127.0.0.1/status",
        "http://[::1]/status",
        "http://10.0.0.5/status",
        "http://172.16.0.5/status",
        "http://192.168.0.10/status",
        "http://169.254.169.254/latest/meta-data/",
        "http://100.100.100.200/latest/meta-data/",
        "http://224.0.0.1/status",
        "http://0.0.0.0/status",
    ],
)
async def test_fetch_web_page_rejects_private_urls_before_fetch(monkeypatch, url):
    calls = {"count": 0}

    async def fake_fetch_and_parse(fetch_url):
        calls["count"] += 1
        raise AssertionError(f"private URL should not be fetched: {fetch_url}")

    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.fetch_web_page(url)

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
    assert calls["count"] == 0


@pytest.mark.asyncio
async def test_fetch_web_page_rejects_hostname_resolving_to_private_ip(monkeypatch):
    calls = {"count": 0}

    async def fake_fetch_and_parse(fetch_url):
        calls["count"] += 1
        raise AssertionError(f"private URL should not be fetched: {fetch_url}")

    def fake_getaddrinfo(*args, **kwargs):
        return [(server.socket.AF_INET, server.socket.SOCK_STREAM, 6, "", ("192.168.0.10", 443))]

    monkeypatch.setattr(server.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.fetch_web_page("https://public-name.example/page")

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
    assert calls["count"] == 0


@pytest.mark.asyncio
async def test_fetch_web_page_allows_public_url_before_fetch(monkeypatch):
    html = "<html><head><title>Public</title></head><body><main>Public page text.</main></body></html>"
    calls = {"urls": []}

    async def fake_fetch_and_parse(url):
        from bs4 import BeautifulSoup

        calls["urls"].append(url)
        return html, BeautifulSoup(html, "lxml")

    def fake_getaddrinfo(*args, **kwargs):
        return [(server.socket.AF_INET, server.socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]

    monkeypatch.setattr(server.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.fetch_web_page("https://example.org/public")

    assert "error" not in result
    assert result["url"] == "https://example.org/public"
    assert result["title"] == "Public"
    assert calls["urls"] == ["https://example.org/public"]
