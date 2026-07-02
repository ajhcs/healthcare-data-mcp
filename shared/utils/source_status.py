"""Normalize source/cache status fields for operator and agent surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

UNAVAILABLE = "unavailable"
SOURCE_STATUS_FIELDS = (
    "source_url",
    "source_period",
    "cache_status",
    "cache_freshness",
    "retrieval_method",
    "caveat",
)


def normalize_source_status(payload: Mapping[str, Any] | None, **defaults: Any) -> dict[str, str]:
    """Return a stable source-status object with explicit unavailable fields."""

    data = dict(payload or {})
    normalized: dict[str, str] = {}
    for field in SOURCE_STATUS_FIELDS:
        value = data.get(field, defaults.get(field, ""))
        normalized[field] = str(value).strip() if value not in (None, "") else UNAVAILABLE
    if normalized["retrieval_method"] == UNAVAILABLE:
        normalized["retrieval_method"] = _infer_retrieval_method(data)
    return normalized


def _infer_retrieval_method(payload: Mapping[str, Any]) -> str:
    cache_status = str(payload.get("cache_status") or payload.get("status") or payload.get("readiness_status") or "")
    if cache_status in {"ready", "stale", "missing", "corrupt", "partial"}:
        return "cache"
    if cache_status in {"live_api", "live_request", "api_metadata"}:
        return "live"
    return UNAVAILABLE


__all__ = ["SOURCE_STATUS_FIELDS", "UNAVAILABLE", "normalize_source_status"]
