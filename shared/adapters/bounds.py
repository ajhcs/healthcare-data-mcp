"""Bounded streaming and rate limiting primitives for source adapters.

The helpers in this module operate on caller-owned iterables and clocks. They
do not perform network I/O, retain source payloads, or acknowledge work. A
caller can therefore use the same primitives with an HTTP library, a local
fixture, or a durable custody writer while keeping limits deterministic.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from collections import deque
import time
from typing import Callable, Iterable, Literal

from shared.adapters.contracts import AdapterContractError, fingerprint_bytes


class BoundsError(AdapterContractError):
    """Raised when a bounded adapter helper is configured unsafely."""


class RateLimitError(BoundsError):
    """Raised when a rate-limit wait would exceed the configured bound."""

    def __init__(self, message: str, *, retry_after_seconds: float) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


StreamState = Literal["completed", "interrupted"]


@dataclass(frozen=True, slots=True)
class StreamBudget:
    """Explicit byte, chunk, and monotonic-clock limits for one stream."""

    max_bytes: int = 131_072
    max_chunks: int = 128
    max_seconds: float = 60.0
    max_chunk_bytes: int | None = None

    def __post_init__(self) -> None:
        if isinstance(self.max_bytes, bool) or not isinstance(self.max_bytes, int) or self.max_bytes < 1:
            raise BoundsError("max_bytes must be a positive integer")
        if isinstance(self.max_chunks, bool) or not isinstance(self.max_chunks, int) or self.max_chunks < 1:
            raise BoundsError("max_chunks must be a positive integer")
        if isinstance(self.max_seconds, bool) or not isinstance(self.max_seconds, (int, float)):
            raise BoundsError("max_seconds must be a number")
        if self.max_seconds <= 0:
            raise BoundsError("max_seconds must be greater than zero")
        if self.max_chunk_bytes is not None:
            if (
                isinstance(self.max_chunk_bytes, bool)
                or not isinstance(self.max_chunk_bytes, int)
                or self.max_chunk_bytes < 1
            ):
                raise BoundsError("max_chunk_bytes must be a positive integer when supplied")
            if self.max_chunk_bytes > self.max_bytes:
                raise BoundsError("max_chunk_bytes cannot exceed max_bytes")


@dataclass(frozen=True, slots=True)
class StreamReceipt:
    """Secret-free result of consuming a bounded byte stream."""

    state: StreamState
    acknowledged: bool
    received_bytes: int
    chunk_count: int
    content_sha256: str
    elapsed_seconds: float

    def __post_init__(self) -> None:
        if self.state not in {"completed", "interrupted"}:
            raise BoundsError("stream state is unsupported")
        if self.state == "completed" and not self.acknowledged:
            raise BoundsError("a completed stream must be acknowledged")
        if self.state == "interrupted" and self.acknowledged:
            raise BoundsError("an interrupted stream cannot be acknowledged")
        if self.received_bytes < 0 or self.chunk_count < 0:
            raise BoundsError("stream counters cannot be negative")
        if self.received_bytes == 0 and self.content_sha256 != fingerprint_bytes(b""):
            raise BoundsError("empty stream digest is invalid")
        if not self.content_sha256.startswith("sha256:") or len(self.content_sha256) != 71:
            raise BoundsError("stream digest must be a sha256 fingerprint")


def _stream_receipt(
    *,
    state: StreamState,
    received_bytes: int,
    chunk_count: int,
    digest: str,
    elapsed_seconds: float,
) -> StreamReceipt:
    return StreamReceipt(
        state=state,
        acknowledged=state == "completed",
        received_bytes=received_bytes,
        chunk_count=chunk_count,
        content_sha256=digest,
        elapsed_seconds=max(0.0, elapsed_seconds),
    )


def stream_bounded(
    chunks: Iterable[bytes],
    budget: StreamBudget,
    *,
    on_chunk: Callable[[bytes], None] | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> StreamReceipt:
    """Consume bytes until complete or return an unacknowledged limit result.

    ``on_chunk`` is called only for chunks that fit the byte/chunk budget. It
    may write to caller-owned custody, but this helper never treats that write
    as an acknowledgement. A time or size bound reached after a callback
    yields ``state='interrupted'`` so the caller can quarantine or retry the
    partial work.
    """

    if not isinstance(budget, StreamBudget):
        raise BoundsError("budget must be a StreamBudget")
    started = clock()
    if not isinstance(started, (int, float)):
        raise BoundsError("clock must return a number")
    digest = sha256()
    received_bytes = 0
    chunk_count = 0

    def interrupted() -> StreamReceipt:
        elapsed = max(0.0, float(clock() - started))
        return _stream_receipt(
            state="interrupted",
            received_bytes=received_bytes,
            chunk_count=chunk_count,
            digest="sha256:" + digest.hexdigest(),
            elapsed_seconds=elapsed,
        )

    for chunk in chunks:
        now = clock()
        if now - started > budget.max_seconds:
            return interrupted()
        if chunk_count >= budget.max_chunks:
            return interrupted()
        if not isinstance(chunk, bytes):
            raise BoundsError("stream chunks must be bytes")
        if not chunk:
            raise BoundsError("stream chunks must not be empty")
        if budget.max_chunk_bytes is not None and len(chunk) > budget.max_chunk_bytes:
            return interrupted()
        if received_bytes + len(chunk) > budget.max_bytes:
            return interrupted()
        if on_chunk is not None:
            on_chunk(chunk)
        digest.update(chunk)
        received_bytes += len(chunk)
        chunk_count += 1
        if clock() - started > budget.max_seconds:
            return interrupted()

    return _stream_receipt(
        state="completed",
        received_bytes=received_bytes,
        chunk_count=chunk_count,
        digest="sha256:" + digest.hexdigest(),
        elapsed_seconds=max(0.0, float(clock() - started)),
    )


@dataclass(frozen=True, slots=True)
class RateLimitPolicy:
    """Sliding-window and minimum-spacing limits for one source class."""

    max_requests: int
    window_seconds: float
    min_interval_seconds: float = 0.0
    max_wait_seconds: float = 60.0

    def __post_init__(self) -> None:
        if isinstance(self.max_requests, bool) or not isinstance(self.max_requests, int) or self.max_requests < 1:
            raise BoundsError("max_requests must be a positive integer")
        for label, value in (
            ("window_seconds", self.window_seconds),
            ("min_interval_seconds", self.min_interval_seconds),
            ("max_wait_seconds", self.max_wait_seconds),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
                raise BoundsError(f"{label} must be a non-negative number")
        if self.window_seconds <= 0:
            raise BoundsError("window_seconds must be greater than zero")
        if self.max_wait_seconds > 86_400:
            raise BoundsError("max_wait_seconds must be at most 86400 seconds")


@dataclass(frozen=True, slots=True)
class RateLimitLease:
    """Secret-free evidence for one granted request slot."""

    granted_at: float
    waited_seconds: float
    remaining_requests: int


class RateLimiter:
    """Thread-safe bounded sliding-window limiter with injectable time."""

    def __init__(
        self,
        policy: RateLimitPolicy,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(policy, RateLimitPolicy):
            raise BoundsError("policy must be a RateLimitPolicy")
        self.policy = policy
        self._clock = clock
        self._sleeper = sleeper
        self._grants: deque[float] = deque()

    def acquire(self) -> RateLimitLease:
        """Wait for and grant one slot, failing if the wait exceeds its bound."""

        started = float(self._clock())
        waited = 0.0
        while True:
            now = float(self._clock())
            self._discard_expired(now)
            wait_for = self._required_wait(now)
            if wait_for <= 0:
                self._grants.append(now)
                remaining = max(0, self.policy.max_requests - len(self._grants))
                return RateLimitLease(now, waited, remaining)
            if waited + wait_for > self.policy.max_wait_seconds:
                raise RateLimitError(
                    "rate-limit wait exceeds configured bound",
                    retry_after_seconds=wait_for,
                )
            self._sleeper(wait_for)
            waited += wait_for
            waited = max(waited, float(self._clock()) - started)

    def _discard_expired(self, now: float) -> None:
        cutoff = now - self.policy.window_seconds
        while self._grants and self._grants[0] <= cutoff:
            self._grants.popleft()

    def _required_wait(self, now: float) -> float:
        wait_for = 0.0
        if self._grants:
            wait_for = max(wait_for, self.policy.min_interval_seconds - (now - self._grants[-1]))
        if len(self._grants) >= self.policy.max_requests:
            wait_for = max(wait_for, self._grants[0] + self.policy.window_seconds - now)
        return max(0.0, wait_for)


# Compatibility aliases make the short names convenient without creating a
# second implementation or changing the receipt vocabulary.
BoundedStreamBudget = StreamBudget
BoundedStreamReceipt = StreamReceipt
BoundedRateLimiter = RateLimiter


__all__ = [
    "BoundedRateLimiter",
    "BoundedStreamBudget",
    "BoundedStreamReceipt",
    "BoundsError",
    "RateLimitError",
    "RateLimitLease",
    "RateLimitPolicy",
    "RateLimiter",
    "StreamBudget",
    "StreamReceipt",
    "StreamState",
    "stream_bounded",
]
