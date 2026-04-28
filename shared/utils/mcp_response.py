"""Helpers for FastMCP-friendly structured tool responses.

FastMCP can expose structured output when tools return JSON-compatible Python
objects instead of pre-serialized JSON strings. This module keeps the common
response shapes small and consistent while leaving individual tools free to
define more specific return annotations later.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from json import dumps
from typing import Any, NoReturn

from mcp.server.fastmcp.exceptions import ToolError
from pydantic_core import to_jsonable_python

JsonDict = dict[str, Any]
JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None
REPORT_SOURCE_METADATA_FIELDS = (
    "source_name",
    "source_url",
    "landing_page",
    "retrieved_at",
    "source_modified",
    "entity_scope",
    "query",
    "cache_key",
    "confidence",
)


class ReportIngestContractError(ValueError):
    """Raised when report-ingest fact rows lack source evidence."""


def to_structured(value: Any) -> JsonValue:
    """Convert Pydantic models, dataclasses, dates, and scalars to JSON-safe values."""
    return to_jsonable_python(value, by_alias=True, fallback=str)


def pagination_meta(
    *,
    count: int,
    limit: int | None = None,
    offset: int | None = None,
    total: int | None = None,
) -> JsonDict:
    """Build pagination metadata for list-style tool results."""
    meta: JsonDict = {"count": count}

    if limit is not None:
        meta["limit"] = limit
    if offset is not None:
        meta["offset"] = offset
    if total is not None:
        meta["total"] = total

    if limit is not None and offset is not None:
        next_offset = offset + count
        if total is None:
            has_more = count >= limit
        else:
            has_more = next_offset < total

        meta["has_more"] = has_more
        meta["next_offset"] = next_offset if has_more else None

    return meta


def response_envelope(
    *,
    data: Any | None = None,
    results: Iterable[Any] | None = None,
    meta: Mapping[str, Any] | None = None,
    message: str | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a success envelope suitable for a FastMCP structured tool."""
    envelope: JsonDict = {"ok": True}

    if data is not None:
        envelope["data"] = to_structured(data)

    if results is not None:
        result_list = [to_structured(item) for item in results]
        envelope["results"] = result_list
        envelope["count"] = len(result_list)

    if message is not None:
        envelope["message"] = message

    if meta:
        envelope["meta"] = to_structured(dict(meta))

    for key, value in fields.items():
        if value is not None:
            envelope[key] = to_structured(value)

    return envelope


def collection_response(
    results: Iterable[Any],
    *,
    limit: int | None = None,
    offset: int | None = None,
    total: int | None = None,
    meta: Mapping[str, Any] | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a standard list response with count and pagination metadata."""
    result_list = [to_structured(item) for item in results]
    pagination = pagination_meta(count=len(result_list), limit=limit, offset=offset, total=total)
    merged_meta: JsonDict = {"pagination": pagination}
    if meta:
        merged_meta.update(to_structured(dict(meta)))

    return response_envelope(results=result_list, meta=merged_meta, **fields)


def record_response(record: Any, *, key: str = "result", **fields: Any) -> JsonDict:
    """Return a single-record response under a named key."""
    return response_envelope(**{key: record, **fields})


def empty_response(message: str, **fields: Any) -> JsonDict:
    """Return a successful response with no results."""
    return response_envelope(results=[], message=message, **fields)


def error_response(
    message: str,
    *,
    code: str = "tool_error",
    detail: Any | None = None,
    retryable: bool = False,
    **fields: Any,
) -> JsonDict:
    """Return a structured failure envelope for non-exceptional tool failures."""
    error: JsonDict = {
        "code": code,
        "message": message,
        "retryable": retryable,
    }
    if detail is not None:
        error["detail"] = to_structured(detail)

    response: JsonDict = {"ok": False, "error": error}
    for key, value in fields.items():
        if value is not None:
            response[key] = to_structured(value)
    return response


def validate_report_ingest_payload(payload: Any) -> None:
    """Reject report-ingest fact rows that do not include source evidence fields.

    Report builders pass nested payloads assembled from MCP tool results. Any
    dict row under a ``facts`` or ``fact_rows`` collection is a report-ingest
    fact row and must carry the source evidence contract directly.
    """

    missing: list[str] = []

    def walk(value: Any, path: str, *, in_fact_rows: bool = False) -> None:
        if isinstance(value, Mapping):
            if in_fact_rows:
                absent = [field for field in REPORT_SOURCE_METADATA_FIELDS if field not in value or value[field] in ("", None)]
                if absent:
                    missing.append(f"{path}: {', '.join(absent)}")
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else str(key)
                walk(child, child_path, in_fact_rows=key in {"facts", "fact_rows"})
            return

        if isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]", in_fact_rows=in_fact_rows)

    walk(to_structured(payload), "")
    if missing:
        raise ReportIngestContractError("Report-ingest fact rows missing source metadata: " + "; ".join(missing))


def tool_error(message: str, *, code: str = "tool_error", detail: Any | None = None) -> ToolError:
    """Create a FastMCP ToolError with compact structured context in the message."""
    if detail is None:
        return ToolError(f"{code}: {message}")
    detail_json = dumps(to_structured(detail), separators=(",", ":"), sort_keys=True)
    return ToolError(f"{code}: {message} | detail={detail_json}")


def raise_tool_error(message: str, *, code: str = "tool_error", detail: Any | None = None) -> NoReturn:
    """Raise a FastMCP ToolError for invalid inputs or runtime failures."""
    raise tool_error(message, code=code, detail=detail)


def raise_invalid_params(message: str, *, detail: Any | None = None) -> NoReturn:
    """Raise a ToolError for caller-supplied invalid parameters."""
    raise_tool_error(message, code="invalid_params", detail=detail)


def raise_not_found(message: str, *, detail: Any | None = None) -> NoReturn:
    """Raise a ToolError when the requested entity cannot be found."""
    raise_tool_error(message, code="not_found", detail=detail)


__all__ = [
    "JsonDict",
    "JsonValue",
    "collection_response",
    "empty_response",
    "error_response",
    "pagination_meta",
    "raise_invalid_params",
    "raise_not_found",
    "raise_tool_error",
    "record_response",
    "response_envelope",
    "REPORT_SOURCE_METADATA_FIELDS",
    "ReportIngestContractError",
    "to_structured",
    "tool_error",
    "validate_report_ingest_payload",
]
