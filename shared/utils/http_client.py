"""Resilient HTTP client with retry, backoff, connection pooling, and rate limiting.

Provides a module-level pooled httpx.AsyncClient via lazy singleton, plus a
``resilient_request`` helper that retries on transient failures with
exponential backoff and jitter.

Usage
-----
    from shared.utils.http_client import resilient_request, get_client

    # Simple GET with automatic retries
    resp = await resilient_request("GET", url, params={...}, timeout=30.0)

    # Access the pooled client directly (no retry wrapper)
    client = get_client()
    resp = await client.get(url)

OSRM rate limiter
-----------------
    from shared.utils.http_client import osrm_rate_limiter

    async with osrm_rate_limiter:
        resp = await resilient_request("GET", osrm_url)
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy singleton pooled client
# ---------------------------------------------------------------------------

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Return the module-level pooled ``httpx.AsyncClient``.

    Creates the client on first call.  The client uses keep-alive connection
    pooling (default 100 connections, 20 per host) so that repeated calls to
    the same API avoid TCP/TLS handshake overhead.
    """
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0),
        )
    return _client


async def close_client() -> None:
    """Shut down the pooled client (call at process exit if desired)."""
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
        _client = None


# ---------------------------------------------------------------------------
# Retry-eligible status codes
# ---------------------------------------------------------------------------

_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})


# ---------------------------------------------------------------------------
# Resilient request
# ---------------------------------------------------------------------------


async def resilient_request(
    method: str,
    url: str,
    *,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    backoff_max: float = 60.0,
    timeout: float | httpx.Timeout | None = None,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json: Any | None = None,
    content: bytes | None = None,
    data: dict[str, Any] | None = None,
    follow_redirects: bool | None = None,
) -> httpx.Response:
    """Send an HTTP request with automatic retries on transient failures.

    Retries on 429 / 5xx with exponential backoff plus jitter.  Respects the
    ``Retry-After`` header when present.

    Parameters
    ----------
    method : str
        HTTP method (``"GET"``, ``"POST"``, etc.).
    url : str
        Fully qualified URL.
    max_retries : int
        Maximum number of retry attempts (default 3).  Total attempts = 1 + max_retries.
    backoff_base : float
        Base delay in seconds for exponential backoff.
    backoff_max : float
        Maximum delay cap in seconds.
    timeout : float | httpx.Timeout | None
        Per-request timeout override.  A plain ``float`` sets the *read*
        timeout while keeping the default connect timeout.
    headers, params, json, content, data :
        Passed through to ``httpx.AsyncClient.request``.
    follow_redirects : bool | None
        Override client-level redirect following.

    Returns
    -------
    httpx.Response
        The successful response.

    Raises
    ------
    httpx.HTTPStatusError
        If the request fails after all retries.
    httpx.TimeoutException
        If the request times out after all retries.
    """
    client = get_client()

    if isinstance(timeout, (int, float)):
        req_timeout = httpx.Timeout(connect=10.0, read=float(timeout), write=float(timeout), pool=10.0)
    elif timeout is not None:
        req_timeout = timeout
    else:
        req_timeout = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=10.0)

    kwargs: dict[str, Any] = {"timeout": req_timeout}
    if headers is not None:
        kwargs["headers"] = headers
    if params is not None:
        kwargs["params"] = params
    if json is not None:
        kwargs["json"] = json
    if content is not None:
        kwargs["content"] = content
    if data is not None:
        kwargs["data"] = data
    if follow_redirects is not None:
        kwargs["follow_redirects"] = follow_redirects

    last_exc: Exception | None = None

    for attempt in range(1 + max_retries):
        try:
            resp = await client.request(method, url, **kwargs)

            if resp.status_code not in _RETRYABLE_STATUS_CODES:
                resp.raise_for_status()
                return resp

            retry_after = _parse_retry_after(resp)
            delay = retry_after if retry_after is not None else _compute_backoff(attempt, backoff_base, backoff_max)

            if attempt < max_retries:
                logger.warning(
                    "HTTP %d from %s (attempt %d/%d) -- retrying in %.1fs",
                    resp.status_code,
                    url,
                    attempt + 1,
                    1 + max_retries,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            resp.raise_for_status()
            return resp  # unreachable

        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as exc:
            last_exc = exc
            if attempt < max_retries:
                delay = _compute_backoff(attempt, backoff_base, backoff_max)
                logger.warning(
                    "%s for %s (attempt %d/%d) -- retrying in %.1fs",
                    type(exc).__name__,
                    url,
                    attempt + 1,
                    1 + max_retries,
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            raise

    assert last_exc is not None
    raise last_exc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compute_backoff(attempt: int, base: float, maximum: float) -> float:
    """Exponential backoff with full jitter."""
    exp = min(maximum, base * (2**attempt))
    return random.uniform(0, exp)


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Parse the ``Retry-After`` header (seconds or HTTP-date)."""
    value = resp.headers.get("retry-after")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        pass
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(value)
        delta = dt.timestamp() - time.time()
        return max(0.0, delta)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# OSRM rate limiter (max 1 req/sec to public demo server)
# ---------------------------------------------------------------------------


class _OSRMRateLimiter:
    """Async rate limiter enforcing a minimum interval between requests."""

    def __init__(self, min_interval: float = 1.0):
        self._min_interval = min_interval
        self._lock = asyncio.Lock()
        self._last_request: float = 0.0

    async def __aenter__(self) -> "_OSRMRateLimiter":
        await self._lock.acquire()
        now = time.monotonic()
        wait = self._min_interval - (now - self._last_request)
        if wait > 0:
            await asyncio.sleep(wait)
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._last_request = time.monotonic()
        self._lock.release()


osrm_rate_limiter = _OSRMRateLimiter(min_interval=1.0)
