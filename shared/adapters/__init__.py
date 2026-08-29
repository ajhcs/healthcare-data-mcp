"""Source-neutral contracts shared by bounded healthcare data adapters.

The adapter SDK is deliberately transport and storage neutral.  It gives
producers one place for conditional request headers, source catalog metadata,
opaque cursor compare-and-swap, and deterministic content fingerprints.  The
bounded stream and rate-limit primitives live in :mod:`shared.adapters.bounds`.
"""

from shared.adapters.contracts import (
    AdapterCatalog,
    AdapterCatalogEntry,
    AdapterChangeMode,
    AdapterContractError,
    ConditionalRequest,
    ConditionalResponse,
    CursorConflictError,
    CursorPrecondition,
    CursorStore,
    Fingerprint,
    InMemoryAdapterCatalog,
    InMemoryCursorStore,
    JsonValue,
    ProbeState,
    SourceCursor,
    classify_conditional_response,
    fingerprint_bytes,
    fingerprint_json,
)
from shared.adapters.bounds import (
    BoundedRateLimiter,
    BoundedStreamBudget,
    BoundedStreamReceipt,
    BoundsError,
    RateLimitError,
    RateLimitLease,
    RateLimitPolicy,
    RateLimiter,
    StreamBudget,
    StreamReceipt,
    stream_bounded,
)

__all__ = [
    "AdapterCatalog",
    "AdapterCatalogEntry",
    "AdapterChangeMode",
    "AdapterContractError",
    "BoundedRateLimiter",
    "BoundedStreamBudget",
    "BoundedStreamReceipt",
    "BoundsError",
    "ConditionalRequest",
    "ConditionalResponse",
    "CursorConflictError",
    "CursorPrecondition",
    "CursorStore",
    "Fingerprint",
    "InMemoryAdapterCatalog",
    "InMemoryCursorStore",
    "JsonValue",
    "ProbeState",
    "RateLimitError",
    "RateLimitLease",
    "RateLimitPolicy",
    "RateLimiter",
    "SourceCursor",
    "StreamBudget",
    "StreamReceipt",
    "classify_conditional_response",
    "fingerprint_bytes",
    "fingerprint_json",
    "stream_bounded",
]
