"""Tests for Google CSE quota management in web-intelligence."""

import httpx
import pytest

from servers.web_intelligence import search_client
from servers.web_intelligence import server
from shared.utils.mcp_response import validate_evidence_receipt


def _assert_web_identity_map(
    identity_map: dict,
    *,
    expected_name: str = "",
    expected_ccn: str = "",
    expected_domain: str = "",
    expected_query: str = "",
    expected_source_url: str = "",
    expected_result_url: str = "",
    expected_sources: set[str] | None = None,
) -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "web_osint"
    assert identity_map["source_claims"]
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No-hit or not-evaluated web-intelligence responses")
    if expected_name:
        assert expected_name in by_field["canonical_name"]["values"]
    if expected_ccn:
        assert expected_ccn in by_field["ccn"]["values"]
    if expected_domain:
        assert expected_domain in by_field["system_domain"]["values"]
    if expected_query:
        assert expected_query in by_field["query_text"]["values"]
    if expected_source_url:
        assert expected_source_url in by_field["source_url"]["values"]
    if expected_result_url:
        assert expected_result_url in by_field["result_url"]["values"]
    if expected_sources:
        assert {claim["collection"] for claim in identity_map["source_claims"]} >= expected_sources


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
    _assert_web_evidence(result["results"][0]["evidence"], dataset_id="google_cse_public_web_search")
    assert result["results"][0]["evidence"]["source_url"] == "https://example.org/about"
    assert result["results"][0]["evidence"]["match_basis"] == "google_cse_result_row"
    assert result["results"][0]["evidence"]["query"]["row_url"] == "https://example.org/about"
    assert result["results"][0]["evidence"]["query"]["row_display_link"] == "example.org"
    assert result["results"][0]["evidence"]["query"]["row_snippet"] == "About Example Health"
    assert result["metadata"]["source"] == "google_cse"
    _assert_web_evidence(result["evidence"], dataset_id="google_cse_public_web_search")
    _assert_web_source_metadata(result, dataset_id="google_cse_public_web_search")
    assert result["evidence"]["match_basis"] == "google_cse_query"
    _assert_web_identity_map(
        result["identity_map"],
        expected_domain="example.org",
        expected_query="Example Health",
        expected_source_url="https://programmablesearchengine.google.com/",
        expected_result_url="https://example.org/about",
        expected_sources={"google_cse_public_web_search"},
    )
    assert result["identity_map"]["source_claims"][0]["row_evidence_paths"] == ["results[].evidence"]


@pytest.mark.asyncio
async def test_search_web_zero_results_have_scoped_no_match_evidence(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {"items": [], "_search_meta": {"cached": False, "requests_made": 1}}

    monkeypatch.setattr(search_client, "search", fake_search)

    result = await server.search_web("Example Health", max_results=3)

    assert result["count"] == 0
    _assert_web_evidence(result["evidence"], dataset_id="google_cse_public_web_search")
    assert result["evidence"]["match_basis"] == "google_cse_query_no_match"
    assert result["evidence"]["confidence"] == "no_google_cse_public_web_results"
    assert "zero-result response" in result["evidence"]["caveat"]
    _assert_web_identity_map(
        result["identity_map"],
        expected_query="Example Health",
        expected_source_url="https://programmablesearchengine.google.com/",
        expected_sources={"google_cse_public_web_search"},
    )


@pytest.mark.asyncio
async def test_search_web_source_error_has_evidence(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {
            "error": "GOOGLE_CSE_API_KEY not set",
            "instructions": "Set GOOGLE_CSE_API_KEY.",
            "quota": {"backend": "google_cse"},
        }

    monkeypatch.setattr(search_client, "search", fake_search)

    result = await server.search_web("Example Health")

    assert result["ok"] is False
    assert result["error"]["code"] == "source_unavailable"
    _assert_web_evidence(result["evidence"], dataset_id="google_cse_public_web_search")
    _assert_web_source_metadata(result, dataset_id="google_cse_public_web_search")
    assert result["evidence"]["match_basis"] == "google_cse_query_source_unavailable"
    assert result["evidence"]["confidence"] == "not_evaluated_search_unavailable"
    _assert_web_identity_map(
        result["identity_map"],
        expected_query="Example Health",
        expected_source_url="https://programmablesearchengine.google.com/",
        expected_sources={"google_cse_public_web_search"},
    )


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
    _assert_web_evidence(result["evidence"], dataset_id="public_web_page_fetch")
    _assert_web_source_metadata(result, dataset_id="public_web_page_fetch")
    assert result["evidence"]["source_url"] == "https://example.org/about"
    _assert_web_identity_map(
        result["identity_map"],
        expected_source_url="https://example.org/about",
        expected_sources={"public_web_page_fetch"},
    )


@pytest.mark.asyncio
async def test_fetch_web_page_failure_has_evidence(monkeypatch):
    async def fake_fetch_and_parse(url):
        return "", None

    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.fetch_web_page("https://example.org/missing", max_chars=1000)

    assert result["ok"] is False
    assert result["error"]["code"] == "source_unavailable"
    _assert_web_evidence(result["evidence"], dataset_id="public_web_page_fetch")
    _assert_web_source_metadata(result, dataset_id="public_web_page_fetch")
    assert result["evidence"]["match_basis"] == "direct_public_http_fetch_source_unavailable"
    assert result["evidence"]["confidence"] == "not_evaluated_fetch_failed"
    _assert_web_identity_map(
        result["identity_map"],
        expected_source_url="https://example.org/missing",
        expected_sources={"public_web_page_fetch"},
    )


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
    _assert_web_evidence(result["evidence"], dataset_id="public_web_page_fetch")
    _assert_web_source_metadata(result, dataset_id="public_web_page_fetch")
    assert result["evidence"]["match_basis"] == "direct_public_http_fetch_rejected_non_public_host"
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
async def test_fetch_and_parse_rejects_redirect_to_private_host(monkeypatch):
    calls = []

    class FakeResponse:
        def __init__(self, status_code: int, *, location: str = "", text: str = "") -> None:
            self.status_code = status_code
            self.headers = {"location": location} if location else {}
            self.text = text
            self.content = text.encode()

    async def fake_resilient_request(method, url, **kwargs):
        calls.append((url, kwargs.get("follow_redirects")))
        return FakeResponse(302, location="http://127.0.0.1/private")

    def fake_getaddrinfo(host, *args, **kwargs):
        if host == "example.org":
            return [(server.socket.AF_INET, server.socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]
        return [(server.socket.AF_INET, server.socket.SOCK_STREAM, 6, "", ("127.0.0.1", 80))]

    monkeypatch.setattr(server, "resilient_request", fake_resilient_request)
    monkeypatch.setattr(server.socket, "getaddrinfo", fake_getaddrinfo)

    html, soup = await server._fetch_and_parse("https://example.org/redirect")

    assert html == ""
    assert soup is None
    assert calls == [("https://example.org/redirect", False)]


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


@pytest.mark.asyncio
async def test_scrape_system_profile_includes_canonical_evidence(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {
            "items": [
                {
                    "title": "About Example Health",
                    "link": "https://example.org/about",
                    "snippet": "Example Health mission page",
                    "displayLink": "example.org",
                }
            ],
            "_search_meta": {"cached": False},
        }

    async def fake_fetch_and_parse(url):
        from bs4 import BeautifulSoup

        html = (
            "<html><body><p>Our mission is to improve public health through "
            "community care, education, and reliable public reporting for every "
            "patient and neighborhood we serve.</p></body></html>"
        )
        return html, BeautifulSoup(html, "lxml")

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(search_client, "search", fake_search)
    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.scrape_system_profile("Example Health")

    assert result["system_name"] == "Example Health"
    assert result["source_urls"] == ["https://example.org/about"]
    assert result["locations"][0]["name"] == "About Example Health"
    _assert_web_evidence(result["locations"][0]["evidence"], dataset_id="public_web_system_profile")
    assert result["locations"][0]["evidence"]["source_url"] == "https://example.org/about"
    assert result["locations"][0]["evidence"]["match_basis"] == "system_profile_location_row"
    assert result["locations"][0]["evidence"]["query"]["row_url"] == "https://example.org/about"
    _assert_web_evidence(result["evidence"], dataset_id="public_web_system_profile")
    _assert_web_source_metadata(result, dataset_id="public_web_system_profile")
    assert result["evidence"]["match_basis"] == "about_page_search_and_fetch"
    assert result["evidence"]["confidence"] == "full_parse"
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_domain="example.org",
        expected_source_url="https://example.org/about",
        expected_sources={"public_web_system_profile"},
    )
    assert result["identity_map"]["source_claims"][0]["row_evidence_paths"] == ["locations[].evidence"]


@pytest.mark.asyncio
async def test_scrape_system_profile_cached_locations_get_row_receipts(monkeypatch):
    cached_profile = {
        "system_name": "Example Health",
        "domain": "example.org",
        "mission": "Cached mission",
        "location_count": 1,
        "locations": [{"name": "Example Health Main", "address": "123 Main St"}],
        "source_urls": ["https://example.org/locations"],
        "data_quality": "full_parse",
    }

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: cached_profile)

    result = await server.scrape_system_profile("Example Health", system_domain="example.org")

    assert result["evidence"]["cache_status"] == "cache_hit"
    _assert_web_source_metadata(result, dataset_id="public_web_system_profile")
    assert result["locations"][0]["name"] == "Example Health Main"
    _assert_web_evidence(result["locations"][0]["evidence"], dataset_id="public_web_system_profile")
    assert result["locations"][0]["evidence"]["source_url"] == "https://example.org/locations"
    assert result["locations"][0]["evidence"]["match_basis"] == "system_profile_location_row"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_domain="example.org",
        expected_result_url="https://example.org/locations",
        expected_sources={"public_web_system_profile"},
    )


@pytest.mark.asyncio
async def test_scrape_system_profile_search_error_has_evidence_and_identity(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {"error": "Google CSE session quota reached", "quota": {"backend": "google_cse"}}

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(search_client, "search", fake_search)

    result = await server.scrape_system_profile("Example Health")

    assert result["ok"] is False
    assert result["error"]["code"] == "source_unavailable"
    _assert_web_evidence(result["evidence"], dataset_id="public_web_system_profile")
    _assert_web_source_metadata(result, dataset_id="public_web_system_profile")
    assert result["evidence"]["match_basis"] == "about_page_search_source_unavailable"
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_sources={"public_web_system_profile"},
    )


@pytest.mark.asyncio
async def test_get_executive_profiles_row_receipts_for_report_rows(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {
            "items": [
                {
                    "title": "Leadership Example Health",
                    "link": "https://example.org/leadership",
                    "snippet": "Example Health leadership",
                    "displayLink": "example.org",
                }
            ],
            "_search_meta": {"cached": False},
        }

    async def fake_fetch_and_parse(url):
        from bs4 import BeautifulSoup

        html = (
            "<html><body><h2>Jane Doe</h2><p>Chief Executive Officer</p>"
            "<h2>About Leadership</h2><p>Overview text.</p></body></html>"
        )
        return html, BeautifulSoup(html, "lxml")

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(search_client, "search", fake_search)
    monkeypatch.setattr(server, "_fetch_and_parse", fake_fetch_and_parse)

    result = await server.get_executive_profiles("Example Health", include_linkedin=False, max_results=5)

    assert result["total_results"] == 1
    assert result["executives"][0]["name"] == "Jane Doe"
    _assert_web_evidence(result["executives"][0]["evidence"], dataset_id="public_web_executive_profiles")
    assert result["executives"][0]["evidence"]["source_url"] == "https://example.org/leadership"
    assert result["executives"][0]["evidence"]["match_basis"] == "leadership_profile_row"
    assert result["executives"][0]["evidence"]["query"]["row_url"] == "https://example.org/leadership"
    assert result["executives"][0]["evidence"]["query"]["row_title"] == "Jane Doe"


@pytest.mark.asyncio
async def test_get_executive_profiles_zero_results_have_no_match_evidence(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {"items": [], "_search_meta": {"cached": False}}

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(search_client, "search", fake_search)

    result = await server.get_executive_profiles("Example Health", include_linkedin=False)

    assert result["total_results"] == 0
    _assert_web_evidence(result["evidence"], dataset_id="public_web_executive_profiles")
    assert result["evidence"]["match_basis"] == "leadership_page_search_and_parse_no_match"
    assert result["evidence"]["confidence"] == "no_public_executive_profile_match"
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_sources={"public_web_executive_profiles"},
    )


@pytest.mark.asyncio
async def test_get_executive_profiles_cached_rows_get_report_receipts(monkeypatch):
    cached = {
        "system_name": "Example Health",
        "total_results": 1,
        "executives": [
            {
                "name": "Jane Doe",
                "title": "Chief Executive Officer",
                "source_url": "https://example.org/leadership",
            }
        ],
        "source_urls": ["https://example.org/leadership"],
    }

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: cached)

    result = await server.get_executive_profiles("Example Health", system_domain="example.org")

    assert result["evidence"]["cache_status"] == "cache_hit"
    _assert_web_evidence(result["evidence"], dataset_id="public_web_executive_profiles")
    _assert_web_evidence(result["executives"][0]["evidence"], dataset_id="public_web_executive_profiles")
    assert result["executives"][0]["evidence"]["source_url"] == "https://example.org/leadership"
    assert result["executives"][0]["evidence"]["match_basis"] == "leadership_profile_row"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_domain="example.org",
        expected_result_url="https://example.org/leadership",
        expected_sources={"public_web_executive_profiles"},
    )
    assert result["identity_map"]["source_claims"][0]["row_evidence_paths"] == ["executives[].evidence"]


@pytest.mark.asyncio
async def test_detect_ehr_vendor_pi_result_includes_canonical_evidence(monkeypatch):
    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "ensure_pi_cached", lambda: _async_value(True))
    monkeypatch.setattr(
        server.data_loaders,
        "query_pi_for_ehr",
        lambda **kwargs: [
            {
                "facility_name": "Example Hospital",
                "ccn": "390001",
                "ehr_developer": "Epic Systems Corporation",
                "ehr_product_name": "EpicCare",
                "cehrt_id": "15.04.04.3007.Epic.01.00.1.221231",
            }
        ],
    )

    result = await server.detect_ehr_vendor("Example Hospital", ccn="390001")

    assert result["vendor_name"] == "Epic Systems"
    assert result["confidence"] == "PI_DATA"
    _assert_web_evidence(result["evidence"], dataset_id="cms_promoting_interoperability_hospital")
    assert result["evidence"]["match_basis"] == "cms_pi_ccn_or_name_lookup"
    assert result["evidence"]["confidence"] == "PI_DATA"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HOSPITAL",
        expected_ccn="390001",
        expected_source_url="https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
        expected_sources={"cms_promoting_interoperability_hospital"},
    )


@pytest.mark.asyncio
async def test_detect_ehr_vendor_cached_preserves_cached_source_dataset(monkeypatch):
    cached = {
        "system_name": "Example Hospital",
        "vendor_name": "Epic Systems",
        "confidence": "PI_DATA",
        "source_url": "https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
        "evidence": {
            "source_name": "CMS Promoting Interoperability Hospital",
            "source_url": "https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
            "dataset_id": "cms_promoting_interoperability_hospital",
            "source_period": "current cached CMS Promoting Interoperability public file",
        },
    }

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: cached)

    result = await server.detect_ehr_vendor("Example Hospital", ccn="390001")

    assert result["evidence"]["cache_status"] == "cache_hit"
    _assert_web_evidence(result["evidence"], dataset_id="cms_promoting_interoperability_hospital")
    assert result["evidence"]["source_name"] == "CMS Promoting Interoperability Hospital"
    assert result["evidence"]["source_url"] == (
        "https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability"
    )
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HOSPITAL",
        expected_ccn="390001",
        expected_source_url="https://data.cms.gov/provider-data/topics/hospitals/promoting-interoperability",
        expected_sources={"cms_promoting_interoperability_hospital"},
    )


@pytest.mark.asyncio
async def test_monitor_newsroom_and_gpo_affiliation_include_evidence(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {
            "items": [
                {
                    "title": "Example Health joins Premier",
                    "link": "https://example.org/news",
                    "snippet": "Example Health selected Premier for group purchasing.",
                    "displayLink": "example.org",
                }
            ],
            "_search_meta": {"cached": False},
        }

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "load_gpo_directory", lambda: [{"gpo_name": "Premier", "aliases": "", "gpo_type": "GPO"}])
    monkeypatch.setattr(search_client, "search", fake_search)
    monkeypatch.setattr(server, "_fetch_google_news_rss", lambda *args, **kwargs: _async_value([]))

    news = await server.monitor_newsroom("Example Health", days_back=30, max_results=5)
    gpo = await server.detect_gpo_affiliation("Example Health")

    assert news["total_results"] == 1
    _assert_web_evidence(news["items"][0]["evidence"], dataset_id="public_news_search")
    assert news["items"][0]["evidence"]["source_url"] == "https://example.org/news"
    assert news["items"][0]["evidence"]["match_basis"] == "public_news_result_row"
    assert news["items"][0]["evidence"]["query"]["row_url"] == "https://example.org/news"
    assert news["items"][0]["evidence"]["query"]["row_source"] == "example.org"
    _assert_web_evidence(news["evidence"], dataset_id="public_news_search")
    _assert_web_identity_map(
        news["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_result_url="https://example.org/news",
        expected_sources={"public_news_search"},
    )
    assert news["identity_map"]["source_claims"][0]["row_evidence_paths"] == ["items[].evidence"]
    assert gpo["matches"][0]["gpo_name"] == "Premier"
    _assert_web_evidence(gpo["matches"][0]["evidence"], dataset_id="bundled_gpo_directory_public_web")
    assert gpo["matches"][0]["evidence"]["source_url"] == "https://example.org/news"
    assert gpo["matches"][0]["evidence"]["match_basis"] == "gpo_directory_match_row"
    assert gpo["matches"][0]["evidence"]["query"]["row_url"] == "https://example.org/news"
    assert gpo["matches"][0]["evidence"]["query"]["row_evidence_snippet"] == (
        "Example Health selected Premier for group purchasing."
    )
    _assert_web_evidence(gpo["evidence"], dataset_id="bundled_gpo_directory_public_web")
    _assert_web_identity_map(
        gpo["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_result_url="https://example.org/news",
        expected_sources={"bundled_gpo_directory_public_web"},
    )
    assert gpo["identity_map"]["source_claims"][0]["row_evidence_paths"] == ["matches[].evidence"]


@pytest.mark.asyncio
async def test_monitor_newsroom_zero_results_have_no_match_evidence(monkeypatch):
    async def fake_search(query, num=5, site_search="", date_restrict="", start=1):
        return {"items": [], "_search_meta": {"cached": False}}

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "cache_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(search_client, "search", fake_search)
    monkeypatch.setattr(server, "_fetch_google_news_rss", lambda *args, **kwargs: _async_value([]))

    result = await server.monitor_newsroom("Example Health", days_back=30, max_results=5)

    assert result["total_results"] == 0
    _assert_web_evidence(result["evidence"], dataset_id="public_news_search")
    assert result["evidence"]["match_basis"] == "google_cse_news_or_rss_no_match"
    assert result["evidence"]["confidence"] == "no_public_news_results"
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_sources={"public_news_search"},
    )


@pytest.mark.asyncio
async def test_monitor_newsroom_cached_items_get_report_receipts(monkeypatch):
    cached = {
        "system_name": "Example Health",
        "days_back": 30,
        "total_results": 1,
        "items": [
            {
                "headline": "Example Health expands public reporting",
                "source": "Example News",
                "snippet": "Example Health announced a public reporting update.",
                "url": "https://example.org/news",
            }
        ],
    }

    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: cached)

    result = await server.monitor_newsroom("Example Health", days_back=30, max_results=5)

    assert result["evidence"]["cache_status"] == "cache_hit"
    _assert_web_evidence(result["evidence"], dataset_id="public_news_search")
    _assert_web_evidence(result["items"][0]["evidence"], dataset_id="public_news_search")
    assert result["items"][0]["evidence"]["source_url"] == "https://example.org/news"
    assert result["items"][0]["evidence"]["match_basis"] == "public_news_result_row"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_result_url="https://example.org/news",
        expected_sources={"public_news_search"},
    )


@pytest.mark.asyncio
async def test_gpo_missing_directory_returns_evidence(monkeypatch):
    monkeypatch.setattr(server.data_loaders, "load_cached_response", lambda *args, **kwargs: None)
    monkeypatch.setattr(server.data_loaders, "load_gpo_directory", lambda: [])

    result = await server.detect_gpo_affiliation("Example Health")

    assert result["ok"] is False
    assert result["error"]["code"] == "source_unavailable"
    _assert_web_evidence(result["evidence"], dataset_id="bundled_gpo_directory_public_web")
    assert result["evidence"]["match_basis"] == "bundled_gpo_directory_missing"
    assert result["evidence"]["confidence"] == "not_evaluated_source_missing"
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_web_identity_map(
        result["identity_map"],
        expected_name="EXAMPLE HEALTH",
        expected_sources={"bundled_gpo_directory_public_web"},
    )


async def _async_value(value):
    return value


def _assert_web_evidence(evidence: dict, *, dataset_id: str) -> None:
    validate_evidence_receipt(evidence, require_content=True)
    assert evidence["dataset_id"] == dataset_id
    assert evidence["source_period"]
    assert evidence["retrieved_at"]
    assert evidence["cache_status"]
    assert evidence["cache_freshness"]
    assert evidence["entity_scope"] == "web_osint"
    assert evidence["caveat"]
    assert evidence["next_step"]


def _assert_web_source_metadata(result: dict, *, dataset_id: str) -> None:
    metadata = result["source_metadata"]
    evidence = result["evidence"]

    assert metadata["dataset_id"] == dataset_id
    assert metadata["source_name"] == evidence["source_name"]
    assert metadata["source_url"] == evidence["source_url"]
    assert metadata["source_period"] == evidence["source_period"]
    assert metadata["retrieved_at"] == evidence["retrieved_at"]
    assert metadata["cache_status"] == evidence["cache_status"]
    assert metadata["cache_freshness"] == evidence["cache_freshness"]
    assert metadata["entity_scope"] == "web_osint"
    assert metadata["source_type"] == "public_web_osint"
