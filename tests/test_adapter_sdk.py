"""Conformance tests for the source-neutral adapter SDK."""

from __future__ import annotations

from dataclasses import replace
import math
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import pytest

from shared.adapters import (
    AdapterCatalogEntry,
    AdapterContractError,
    ConditionalRequest,
    ConditionalResponse,
    CursorConflictError,
    CursorPrecondition,
    Fingerprint,
    InMemoryAdapterCatalog,
    InMemoryCursorStore,
    JsonValue,
    MAX_RATE_REQUESTS,
    MAX_RATE_SECONDS,
    MAX_STREAM_BYTES,
    MAX_STREAM_CHUNKS,
    MAX_STREAM_SECONDS,
    RateLimitError,
    RateLimitPolicy,
    RateLimiter,
    SourceCursor,
    StreamBudget,
    classify_conditional_response,
    fingerprint_bytes,
    fingerprint_json,
    stream_bounded,
)
from shared.acquisition.ahrq_detector import AhrqChangeDetector, ReleaseMetadata


ROOT = Path(__file__).resolve().parents[1]
AHRQ_FIXTURE_ROOT = ROOT / "contracts/healthcare-data-platform/ahrq/v1/fixtures"


def test_fingerprints_are_stable_and_match_the_ahrq_semantic_boundary() -> None:
    value: dict[str, JsonValue] = {
        "release_id": "release:ahrq:2026-08-22",
        "source_id": "source:ahrq:lighthouse",
    }

    assert fingerprint_bytes(b"abc") == "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert fingerprint_json(value) == fingerprint_json(
        {"source_id": value["source_id"], "release_id": value["release_id"]}
    )
    assert Fingerprint.from_bytes(b"abc").digest == fingerprint_bytes(b"abc").removeprefix("sha256:")

    detector = AhrqChangeDetector()
    raw = detector.load_fixture(AHRQ_FIXTURE_ROOT / "official-release-changed.json")["release_metadata"]
    assert isinstance(raw, dict)
    metadata = ReleaseMetadata.from_mapping(raw)
    assert metadata.release_fingerprint.startswith("sha256:")


def test_conditional_request_is_https_and_only_emits_validators() -> None:
    request = ConditionalRequest(
        "https://example.test/releases/latest.json",
        etag='W/"release-1"',
        last_modified="Wed, 22 Apr 2026 00:00:00 GMT",
    )

    assert request.headers == {
        "If-None-Match": 'W/"release-1"',
        "If-Modified-Since": "Wed, 22 Apr 2026 00:00:00 GMT",
    }
    assert request.has_validator is True
    assert ConditionalRequest("https://example.test/releases/latest.json").headers == {}

    with pytest.raises(AdapterContractError, match="HTTPS"):
        ConditionalRequest("http://example.test/releases/latest.json")
    with pytest.raises(AdapterContractError, match="control"):
        ConditionalRequest("https://example.test/releases/latest.json", etag="ok\r\nX-Leak: value")


@pytest.mark.parametrize("etag", ["release-1", '"unterminated', '"contains\nnewline"', "W/abc"])
def test_conditional_validators_reject_malformed_etags(etag: str) -> None:
    with pytest.raises(AdapterContractError, match="entity-tag|control"):
        ConditionalRequest("https://example.test/releases/latest.json", etag=etag)
    with pytest.raises(AdapterContractError, match="entity-tag|control"):
        ConditionalResponse(200, etag=etag)


@pytest.mark.parametrize(
    "last_modified",
    [
        "Wed, 22 Apr 2026 00:00:00 UTC",
        "Wed, 2 Apr 2026 00:00:00 GMT",
        "Wed, 99 Apr 2026 00:00:00 GMT",
        "Tue, 22 Apr 2026 00:00:00 GMT",
        "2026-04-22T00:00:00Z",
    ],
)
def test_conditional_validators_reject_non_imf_fixdate(last_modified: str) -> None:
    with pytest.raises(AdapterContractError, match="IMF-fixdate"):
        ConditionalRequest("https://example.test/releases/latest.json", last_modified=last_modified)
    with pytest.raises(AdapterContractError, match="IMF-fixdate"):
        ConditionalResponse(200, last_modified=last_modified)


def test_conditional_wildcard_is_request_only() -> None:
    assert ConditionalRequest("https://example.test/releases/latest.json", etag="*").headers == {"If-None-Match": "*"}
    with pytest.raises(AdapterContractError, match="entity-tag"):
        ConditionalResponse(200, etag="*")


@pytest.mark.parametrize("url", ["https:///path", "https://:443/path", "https://example.com:invalid/path"])
def test_conditional_url_rejects_malformed_authorities(url: str) -> None:
    with pytest.raises(AdapterContractError, match="authority"):
        ConditionalRequest(url)


def test_conditional_response_classification_preserves_noop_semantics() -> None:
    assert classify_conditional_response(304) == "not_modified"
    assert classify_conditional_response(200) == "changed"
    assert classify_conditional_response(503) == "failed_probe"
    assert ConditionalResponse(304).not_modified is True
    assert ConditionalResponse(200, response_fingerprint=fingerprint_bytes(b"body")).not_modified is False

    with pytest.raises(AdapterContractError, match="status_code"):
        ConditionalResponse(99)


def test_catalog_requires_public_rights_for_enabled_sources_and_is_read_only() -> None:
    entry = AdapterCatalogEntry(
        source_id="source:ahrq:lighthouse",
        source_url="https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
        change_mode="release_metadata",
        release_locator="https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
        rights_status="approved_public",
    )
    catalog = InMemoryAdapterCatalog([entry])

    assert catalog.registration(entry.source_id) == entry
    assert catalog.get(entry.source_id) == entry
    assert catalog.entries[entry.source_id] is entry
    with pytest.raises(TypeError):
        catalog.entries[entry.source_id] = replace(entry, enabled=False)  # type: ignore[index]

    with pytest.raises(AdapterContractError, match="approved public"):
        AdapterCatalogEntry(
            source_id="source:optional:fast",
            source_url="https://fast.example/snapshot.json",
            change_mode="content_hash",
            release_locator="https://fast.example/snapshot.json",
            rights_status="pending_review",
        )
    with pytest.raises(AdapterContractError, match="duplicate"):
        InMemoryAdapterCatalog([entry, entry])


def test_cursor_store_requires_matching_generation_and_replays_idempotently() -> None:
    store = InMemoryCursorStore()
    first = store.compare_and_swap(
        CursorPrecondition(
            source_id="source:ahrq:lighthouse",
            expected_generation=0,
            expected_token=None,
            next_token="release:ahrq:2026-08-22",
        )
    )
    assert first == SourceCursor("source:ahrq:lighthouse", "release:ahrq:2026-08-22", 1)
    replay = store.compare_and_swap(
        CursorPrecondition(
            source_id="source:ahrq:lighthouse",
            expected_generation=0,
            expected_token=None,
            next_token="release:ahrq:2026-08-22",
        )
    )
    assert replay == first

    second = store.compare_and_swap(
        CursorPrecondition(
            source_id="source:ahrq:lighthouse",
            expected_generation=1,
            expected_token=first.token,
            next_token="release:ahrq:2026-08-23",
        )
    )
    assert second.generation == 2
    with pytest.raises(CursorConflictError, match="stale"):
        store.compare_and_swap(
            CursorPrecondition(
                source_id="source:ahrq:lighthouse",
                expected_generation=1,
                expected_token=first.token,
                next_token="release:ahrq:2026-08-24",
            )
        )


def test_streaming_is_bounded_and_reports_digest_without_ack_for_partial_input() -> None:
    delivered: list[bytes] = []
    complete = stream_bounded(
        [b"ab", b"c"],
        StreamBudget(max_bytes=3, max_chunks=2, max_seconds=10),
        on_chunk=delivered.append,
    )
    assert complete.state == "completed"
    assert complete.acknowledged is True
    assert complete.received_bytes == 3
    assert complete.chunk_count == 2
    assert complete.content_sha256 == fingerprint_bytes(b"abc")
    assert delivered == [b"ab", b"c"]

    partial = stream_bounded([b"ab", b"cd"], StreamBudget(max_bytes=3, max_chunks=2), on_chunk=delivered.append)
    assert partial.state == "interrupted"
    assert partial.acknowledged is False
    assert partial.received_bytes == 2
    assert partial.chunk_count == 1
    assert delivered[-1] == b"ab"

    with pytest.raises(AdapterContractError, match="empty"):
        stream_bounded([b""], StreamBudget())


def test_streaming_deadline_is_clock_bounded() -> None:
    current = [0.0]

    def clock() -> float:
        return current[0]

    def write_and_expire(chunk: bytes) -> None:
        current[0] += 2.0

    receipt = stream_bounded(
        [b"one", b"two"],
        StreamBudget(max_bytes=10, max_chunks=2, max_seconds=1),
        on_chunk=write_and_expire,
        clock=clock,
    )
    assert receipt.state == "interrupted"
    assert receipt.acknowledged is False
    assert receipt.received_bytes == 3
    assert receipt.content_sha256 == fingerprint_bytes(b"one")


@pytest.mark.parametrize("seconds", [math.nan, math.inf, -math.inf, MAX_STREAM_SECONDS + 1])
def test_stream_budget_rejects_non_finite_or_unsafe_deadlines(seconds: float) -> None:
    with pytest.raises(AdapterContractError, match="finite"):
        StreamBudget(max_seconds=seconds)


def test_stream_budget_has_explicit_byte_and_chunk_ceilings() -> None:
    with pytest.raises(AdapterContractError, match="max_bytes"):
        StreamBudget(max_bytes=MAX_STREAM_BYTES + 1)
    with pytest.raises(AdapterContractError, match="max_chunks"):
        StreamBudget(max_chunks=MAX_STREAM_CHUNKS + 1)


def test_duration_contracts_reject_huge_integers_without_overflow() -> None:
    huge_duration = 10**1000
    with pytest.raises(AdapterContractError):
        StreamBudget(max_seconds=huge_duration)
    with pytest.raises(AdapterContractError):
        RateLimitPolicy(max_requests=1, window_seconds=huge_duration)
    with pytest.raises(AdapterContractError):
        RateLimitPolicy(max_requests=1, window_seconds=1, min_interval_seconds=huge_duration)
    with pytest.raises(AdapterContractError):
        RateLimitPolicy(max_requests=1, window_seconds=1, max_wait_seconds=huge_duration)
    with pytest.raises(AdapterContractError):
        AdapterCatalogEntry(
            source_id="source:ahrq:lighthouse",
            source_url="https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
            change_mode="release_metadata",
            release_locator="https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
            rights_status="approved_public",
            max_seconds=huge_duration,
        )


def test_rate_limiter_enforces_window_and_bounded_wait_without_payload_state() -> None:
    current = [0.0]
    sleeps: list[float] = []

    def clock() -> float:
        return current[0]

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        current[0] += seconds

    limiter = RateLimiter(
        RateLimitPolicy(max_requests=1, window_seconds=10, max_wait_seconds=10),
        clock=clock,
        sleeper=sleeper,
    )
    assert limiter.acquire().remaining_requests == 0
    second = limiter.acquire()
    assert second.waited_seconds == 10
    assert sleeps == [10]

    stuck = RateLimiter(
        RateLimitPolicy(max_requests=1, window_seconds=10, max_wait_seconds=1),
        clock=lambda: 0.0,
        sleeper=lambda _seconds: None,
    )
    stuck.acquire()
    with pytest.raises(RateLimitError, match="exceeds configured bound") as error:
        stuck.acquire()
    assert error.value.retry_after_seconds == 10


@pytest.mark.parametrize("field", ["window_seconds", "min_interval_seconds", "max_wait_seconds"])
def test_rate_limit_policy_rejects_non_finite_or_unsafe_values(field: str) -> None:
    with pytest.raises(AdapterContractError, match="finite"):
        RateLimitPolicy(max_requests=1, **{"window_seconds": 1, field: math.inf})
    with pytest.raises(AdapterContractError, match="finite"):
        RateLimitPolicy(max_requests=1, **{"window_seconds": 1, field: math.nan})
    with pytest.raises(AdapterContractError, match="finite"):
        RateLimitPolicy(max_requests=1, **{"window_seconds": 1, field: MAX_RATE_SECONDS + 1})
    with pytest.raises(AdapterContractError, match="max_requests"):
        RateLimitPolicy(max_requests=MAX_RATE_REQUESTS + 1, window_seconds=1)


def test_rate_limiter_serializes_check_and_append_for_concurrent_callers() -> None:
    limiter = RateLimiter(
        RateLimitPolicy(max_requests=1, window_seconds=60, max_wait_seconds=0),
        clock=lambda: 0.0,
        sleeper=lambda _seconds: None,
    )

    def acquire() -> str:
        try:
            limiter.acquire()
        except RateLimitError:
            return "rejected"
        return "granted"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _index: acquire(), range(2)))
    assert outcomes.count("granted") == 1
    assert outcomes.count("rejected") == 1
