"""Google Custom Search API client.

Wraps the CSE JSON API v1 for web search, site-scoped search, and
news-style search. All 5 tools route through this module.

API docs: https://developers.google.com/custom-search/v1/reference/rest/v1/cse/list
"""

import asyncio
import copy
import json
import logging
import os
import time

import httpx

from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.googleapis.com/customsearch/v1"
_TIMEOUT = 20.0
_DEFAULT_CACHE_TTL_SECONDS = 6 * 60 * 60
_DEFAULT_DAILY_LIMIT = 100
_DEFAULT_SESSION_LIMIT = 40

_search_cache: dict[str, dict] = {}
_runtime_state = {
    "requests_made": 0,
    "cache_hits": 0,
    "quota_errors": 0,
    "blocked_until": 0.0,
    "last_error": "",
}
_state_lock = asyncio.Lock()


def _get_credentials() -> tuple[str | None, str | None]:
    """Return (api_key, cse_id) from environment."""
    return (
        os.environ.get("GOOGLE_CSE_API_KEY"),
        os.environ.get("GOOGLE_CSE_ID"),
    )


def _get_cache_ttl_seconds() -> int:
    """Return the in-process cache TTL for identical Google CSE requests."""
    return max(0, int(os.environ.get("GOOGLE_CSE_CACHE_TTL_SECONDS", str(_DEFAULT_CACHE_TTL_SECONDS))))


def _get_daily_limit() -> int:
    """Return the assumed Google CSE daily quota for response metadata."""
    return max(1, int(os.environ.get("GOOGLE_CSE_DAILY_LIMIT", str(_DEFAULT_DAILY_LIMIT))))


def _get_session_limit() -> int:
    """Return the maximum number of live Google CSE requests allowed per process."""
    return max(0, int(os.environ.get("GOOGLE_CSE_SESSION_LIMIT", str(_DEFAULT_SESSION_LIMIT))))


def _build_cache_key(
    query: str,
    *,
    num: int,
    site_search: str,
    date_restrict: str,
    start: int,
) -> str:
    """Build a stable cache key for a Google CSE request."""
    return json.dumps({
        "query": query,
        "num": min(num, 10),
        "site_search": site_search,
        "date_restrict": date_restrict,
        "start": start,
    }, sort_keys=True)


def _quota_snapshot(*, cached: bool = False, backend: str = "google_cse") -> dict:
    """Return current runtime quota state for diagnostics."""
    daily_limit = _get_daily_limit()
    session_limit = _get_session_limit()
    requests_made = int(_runtime_state["requests_made"])
    blocked_until = float(_runtime_state["blocked_until"])
    return {
        "backend": backend,
        "cached": cached,
        "daily_limit": daily_limit,
        "session_limit": session_limit,
        "requests_made": requests_made,
        "remaining_estimate": max(0, daily_limit - requests_made),
        "cache_hits": int(_runtime_state["cache_hits"]),
        "quota_errors": int(_runtime_state["quota_errors"]),
        "blocked_until_epoch": blocked_until or None,
        "retry_after_seconds": max(0, int(blocked_until - time.time())) if blocked_until > time.time() else 0,
        "last_error": str(_runtime_state["last_error"]),
    }


def get_quota_status() -> dict:
    """Return current Google CSE runtime counters."""
    return _quota_snapshot()


def _is_quota_error(status_code: int, payload: dict | None) -> bool:
    """Return True when a Google error payload indicates quota exhaustion."""
    if status_code == 429:
        return True
    if status_code != 403:
        return False

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    errors = error.get("errors", []) if isinstance(error, dict) else []
    reasons = {
        str(item.get("reason", "")).lower()
        for item in errors
        if isinstance(item, dict)
    }
    message = str(error.get("message", "")).lower()

    return any(reason in {"dailylimitexceeded", "quotaexceeded", "userratelimitexceeded"} for reason in reasons) or (
        "quota" in message or "daily limit" in message or "rate limit" in message
    )


def _quota_error_response(message: str, *, backend: str = "google_cse", retry_after: int = 0) -> dict:
    """Build a structured quota-management error response."""
    return {
        "error": message,
        "backend": backend,
        "quota": _quota_snapshot(backend=backend),
        "instructions": (
            "Narrow the query, rely on cached results, or wait for quota reset. "
            "Set GOOGLE_CSE_SESSION_LIMIT=0 to disable the per-process guard."
        ),
        "retry_after_seconds": retry_after,
    }


def _cache_lookup(cache_key: str) -> dict | None:
    """Return a cached response when still valid."""
    entry = _search_cache.get(cache_key)
    if not entry:
        return None

    if entry["expires_at"] <= time.time():
        _search_cache.pop(cache_key, None)
        return None

    _runtime_state["cache_hits"] += 1
    cached_response = copy.deepcopy(entry["response"])
    cached_response["_search_meta"] = _quota_snapshot(cached=True, backend="google_cse")
    return cached_response


def _store_cache(cache_key: str, response: dict) -> None:
    """Store a successful Google CSE response in the in-process cache."""
    ttl_seconds = _get_cache_ttl_seconds()
    if ttl_seconds <= 0:
        return
    _search_cache[cache_key] = {
        "expires_at": time.time() + ttl_seconds,
        "response": copy.deepcopy(response),
    }


def _reset_runtime_state() -> None:
    """Reset cache and counters. Intended for tests."""
    _search_cache.clear()
    _runtime_state.update({
        "requests_made": 0,
        "cache_hits": 0,
        "quota_errors": 0,
        "blocked_until": 0.0,
        "last_error": "",
    })


async def search(
    query: str,
    num: int = 5,
    site_search: str = "",
    date_restrict: str = "",
    start: int = 1,
) -> dict:
    """Execute a Google Custom Search.

    Args:
        query: Search query string.
        num: Number of results (1-10, API maximum).
        site_search: Restrict results to this domain (e.g. "intermountainhealth.org").
        date_restrict: Recency filter (e.g. "d90" for last 90 days, "m6" for 6 months).
        start: Result offset for pagination (1-based).

    Returns:
        Raw API response dict, or error dict on failure.
    """
    api_key, cse_id = _get_credentials()
    if not api_key:
        return {
            "error": "GOOGLE_CSE_API_KEY not set",
            "instructions": (
                "Get a Google Custom Search JSON API key at "
                "https://developers.google.com/custom-search/v1/introduction "
                "(100 free queries/day, $5/1000 after)"
            ),
        }
    if not cse_id:
        return {
            "error": "GOOGLE_CSE_ID not set",
            "instructions": (
                "Create a Programmable Search Engine at "
                "https://programmablesearchengine.google.com/ "
                "and use the cx ID"
            ),
        }

    cache_key = _build_cache_key(
        query,
        num=num,
        site_search=site_search,
        date_restrict=date_restrict,
        start=start,
    )

    async with _state_lock:
        cached = _cache_lookup(cache_key)
        if cached is not None:
            return cached

        blocked_until = float(_runtime_state["blocked_until"])
        if blocked_until > time.time():
            retry_after = max(1, int(blocked_until - time.time()))
            return _quota_error_response(
                "Google CSE temporarily blocked after quota/rate-limit response",
                retry_after=retry_after,
            )

        session_limit = _get_session_limit()
        if session_limit and int(_runtime_state["requests_made"]) >= session_limit:
            return _quota_error_response(
                f"Google CSE session quota reached ({session_limit} live requests this process)",
            )

        _runtime_state["requests_made"] += 1

    params: dict[str, str | int] = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": min(num, 10),
        "start": start,
    }
    if site_search:
        params["siteSearch"] = site_search
        params["siteSearchFilter"] = "i"  # include only results from this site
    if date_restrict:
        params["dateRestrict"] = date_restrict

    try:
        resp = await resilient_request(
            "GET",
            _BASE_URL,
            params=params,
            timeout=_TIMEOUT,
            max_retries=4,
            backoff_base=2.0,
            backoff_max=120.0,
        )
        payload = resp.json()
        payload["_search_meta"] = _quota_snapshot(backend="google_cse")
        async with _state_lock:
            _store_cache(cache_key, payload)
        return payload
    except httpx.HTTPStatusError as e:
        payload: dict | None = None
        try:
            payload = e.response.json()
        except Exception:
            payload = None

        if _is_quota_error(e.response.status_code, payload):
            retry_after = 0
            try:
                retry_after = int(float(e.response.headers.get("Retry-After", "0")))
            except ValueError:
                retry_after = 0
            async with _state_lock:
                _runtime_state["quota_errors"] += 1
                _runtime_state["last_error"] = f"quota:{e.response.status_code}"
                _runtime_state["blocked_until"] = time.time() + max(retry_after, 60 * 60)
            logger.warning("Google CSE quota exceeded: status=%s", e.response.status_code)
            return _quota_error_response(
                "Google CSE daily quota exceeded or rate-limited",
                retry_after=max(retry_after, 0),
            )
        logger.warning("Google CSE HTTP error: %s", e)
        async with _state_lock:
            _runtime_state["last_error"] = f"http:{e.response.status_code}"
        return {
            "error": f"Google CSE request failed: {e.response.status_code}",
            "quota": _quota_snapshot(backend="google_cse"),
        }
    except Exception as e:
        logger.warning("Google CSE request failed: %s", e)
        async with _state_lock:
            _runtime_state["last_error"] = str(e)
        return {"error": str(e), "quota": _quota_snapshot(backend="google_cse")}


def extract_results(raw: dict) -> list[dict]:
    """Extract simplified search results from raw CSE response.

    Returns list of dicts with keys: title, link, snippet, displayLink.
    """
    if "error" in raw:
        return []
    items = raw.get("items", [])
    results = []
    for item in items:
        results.append({
            "title": item.get("title", ""),
            "link": item.get("link", ""),
            "snippet": item.get("snippet", ""),
            "display_link": item.get("displayLink", ""),
        })
    return results
