"""Helpers for FastMCP-friendly structured tool responses.

FastMCP can expose structured output when tools return JSON-compatible Python
objects instead of pre-serialized JSON strings. This module keeps the common
response shapes small and consistent while leaving individual tools free to
define more specific return annotations later.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from json import dumps
from typing import Any, NoReturn

from mcp.server.fastmcp.exceptions import ToolError
from pydantic_core import to_jsonable_python

JsonDict = dict[str, Any]
JsonValue = dict[str, Any] | list[Any] | str | int | float | bool | None
REPORT_SOURCE_METADATA_FIELDS = (
    "source_name",
    "source_url",
    "dataset_id",
    "source_period",
    "landing_page",
    "retrieved_at",
    "source_modified",
    "cache_status",
    "cache_freshness",
    "entity_scope",
    "query",
    "cache_key",
    "match_basis",
    "confidence",
    "caveat",
    "next_step",
)
EVIDENCE_RECEIPT_FIELDS = REPORT_SOURCE_METADATA_FIELDS
EVIDENCE_RECEIPT_REQUIRED_CONTENT_FIELDS = (
    "source_name",
    "dataset_id",
    "match_basis",
    "confidence",
    "caveat",
    "next_step",
)


@dataclass(frozen=True, slots=True)
class EvidenceReceipt:
    """Canonical source/provenance receipt for healthcare-relevant tool facts."""

    source_name: str = ""
    source_url: str = ""
    dataset_id: str = ""
    source_period: str = ""
    landing_page: str = ""
    retrieved_at: str = ""
    source_modified: str = ""
    cache_status: str = ""
    cache_freshness: str = ""
    entity_scope: str = ""
    query: Any = None
    cache_key: str = ""
    match_basis: str = ""
    confidence: str = ""
    caveat: str = ""
    next_step: str = ""

    def to_dict(self) -> JsonDict:
        return to_structured(asdict(self))  # type: ignore[return-value]


class ReportIngestContractError(ValueError):
    """Raised when report-ingest fact rows lack source evidence."""


@dataclass(frozen=True, slots=True)
class ToolExecutionError(Exception):
    """Machine-parseable MCP tool failure with agent recovery guidance."""

    error_type: str
    message: str
    recoverable: bool = True
    data: Mapping[str, Any] | None = None

    def __str__(self) -> str:
        return self.message

    def to_payload(self) -> JsonDict:
        return {
            "ok": False,
            "error": {
                "type": self.error_type,
                "code": self.error_type.lower(),
                "message": self.message,
                "recoverable": self.recoverable,
                "retryable": self.recoverable,
                "data": to_structured(dict(self.data or {})),
            }
        }


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


def evidence_receipt(
    *,
    source_name: str = "",
    source_url: str = "",
    dataset_id: str = "",
    source_period: str = "",
    landing_page: str = "",
    retrieved_at: str = "",
    source_modified: str = "",
    cache_status: str = "",
    cache_freshness: str = "",
    entity_scope: str = "",
    query: Any = None,
    cache_key: str = "",
    match_basis: str = "",
    confidence: str = "",
    caveat: str = "",
    next_step: str = "",
    source_metadata: Mapping[str, Any] | None = None,
) -> JsonDict:
    """Build the shared source/provenance receipt used by healthcare facts.

    Callers may pass explicit fields, a source metadata mapping, or both.
    Explicit fields win over source metadata aliases.
    """

    metadata = dict(source_metadata or {})
    receipt = EvidenceReceipt(
        source_name=source_name or str(metadata.get("source_name") or metadata.get("source") or ""),
        source_url=source_url or str(metadata.get("source_url") or ""),
        dataset_id=dataset_id or str(metadata.get("dataset_id") or metadata.get("id") or ""),
        source_period=source_period or str(metadata.get("source_period") or metadata.get("period") or ""),
        landing_page=landing_page
        or str(metadata.get("landing_page") or metadata.get("landing_page_url") or metadata.get("docs_url") or ""),
        retrieved_at=retrieved_at
        or str(metadata.get("retrieved_at") or metadata.get("downloaded_at") or metadata.get("queried_at") or ""),
        source_modified=source_modified
        or str(metadata.get("source_modified") or metadata.get("source_last_modified") or metadata.get("modified") or ""),
        cache_status=cache_status or str(metadata.get("cache_status") or ""),
        cache_freshness=cache_freshness or _cache_freshness(metadata),
        entity_scope=entity_scope or str(metadata.get("entity_scope") or ""),
        query=query if query is not None else metadata.get("query"),
        cache_key=cache_key or str(metadata.get("cache_key") or metadata.get("cache_path") or ""),
        match_basis=match_basis or str(metadata.get("match_basis") or ""),
        confidence=confidence or str(metadata.get("confidence") or ""),
        caveat=caveat or str(metadata.get("caveat") or metadata.get("source_caveat") or ""),
        next_step=next_step or str(metadata.get("next_step") or ""),
    )
    return receipt.to_dict()


def _cache_freshness(metadata: Mapping[str, Any]) -> str:
    status = str(metadata.get("cache_status") or "")
    age = metadata.get("cache_age_days")
    if age in ("", None):
        return status
    return f"{status}; age_days={age}" if status else f"age_days={age}"


def validate_evidence_receipt(receipt: Any, *, require_content: bool = False) -> None:
    """Validate that a result exposes the canonical evidence receipt fields."""

    payload = to_structured(receipt)
    if not isinstance(payload, Mapping):
        raise ReportIngestContractError("Evidence receipt must be an object")
    missing = [field for field in EVIDENCE_RECEIPT_FIELDS if field not in payload]
    if missing:
        raise ReportIngestContractError("Evidence receipt missing fields: " + ", ".join(missing))
    if require_content:
        _validate_evidence_receipt_content(payload)


def _validate_evidence_receipt_content(payload: Mapping[str, Any]) -> None:
    missing_content = [
        field for field in EVIDENCE_RECEIPT_REQUIRED_CONTENT_FIELDS if not str(payload.get(field) or "").strip()
    ]
    if not (str(payload.get("source_url") or "").strip() or str(payload.get("landing_page") or "").strip()):
        missing_content.append("source_url_or_landing_page")
    if not (
        str(payload.get("source_period") or "").strip()
        or str(payload.get("retrieved_at") or "").strip()
        or str(payload.get("source_modified") or "").strip()
    ):
        missing_content.append("source_period_or_retrieved_at_or_source_modified")
    if not (str(payload.get("cache_status") or "").strip() or str(payload.get("cache_freshness") or "").strip()):
        missing_content.append("cache_status_or_cache_freshness")
    if missing_content:
        raise ReportIngestContractError(
            "Evidence receipt missing required content: " + ", ".join(missing_content)
        )


def evidence_receipts_in_payload(
    payload: Any,
    *,
    keys: Iterable[str] | None = None,
    path: str = "result",
) -> list[tuple[str, Any]]:
    """Return every nested evidence receipt candidate with its dotted/list path."""

    receipt_keys = {str(key) for key in (keys or ("evidence", "evidence_receipt"))}
    return _nested_values_for_keys(to_structured(payload), receipt_keys, path=path)


def evidence_receipt_validation_summary(
    payload: Any,
    *,
    require_content: bool = False,
    keys: Iterable[str] | None = None,
    path: str = "result",
) -> JsonDict:
    """Summarize nested evidence receipt validity without exposing receipt content."""

    evidence_items = evidence_receipts_in_payload(payload, keys=keys, path=path)
    invalid_evidence_paths = []
    for receipt_path, receipt in evidence_items:
        try:
            validate_evidence_receipt(receipt, require_content=require_content)
        except ReportIngestContractError as exc:
            invalid_evidence_paths.append({"path": receipt_path, "error": str(exc)})

    evidence_present = bool(evidence_items)
    evidence_valid = evidence_present and not invalid_evidence_paths
    status = (
        "evidence_receipt_valid"
        if evidence_valid
        else "evidence_receipt_invalid"
        if evidence_present
        else "evidence_receipt_missing"
    )
    summary: JsonDict = {
        "status": status,
        "receipt_count": len(evidence_items),
        "evidence_present": evidence_present,
        "evidence_valid": evidence_valid,
    }
    if invalid_evidence_paths:
        summary["invalid_evidence_paths"] = invalid_evidence_paths
    return summary


def _nested_values_for_keys(value: Any, keys: set[str], *, path: str) -> list[tuple[str, Any]]:
    matches: list[tuple[str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if str(key) in keys:
                matches.append((child_path, child))
            matches.extend(_nested_values_for_keys(child, keys, path=child_path))
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for index, child in enumerate(value):
            matches.extend(_nested_values_for_keys(child, keys, path=f"{path}[{index}]"))
    return matches


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
    error_type: str | None = None,
    detail: Any | None = None,
    retryable: bool = False,
    recoverable: bool | None = None,
    data: Mapping[str, Any] | None = None,
    fix_hint: str | None = None,
    available_options: Sequence[Any] | None = None,
    suggested_tool_calls: Sequence[Mapping[str, Any]] | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a structured failure envelope for non-exceptional tool failures."""
    resolved_type = (error_type or code or "tool_error").upper()
    resolved_recoverable = retryable if recoverable is None else recoverable
    recovery_data: JsonDict = dict(data or {})
    if detail is not None:
        recovery_data["detail"] = to_structured(detail)
    if fix_hint:
        recovery_data["fix_hint"] = fix_hint
    if available_options is not None:
        recovery_data["available_options"] = to_structured(list(available_options))
    if suggested_tool_calls is not None:
        recovery_data["suggested_tool_calls"] = to_structured(list(suggested_tool_calls))

    error: JsonDict = {
        "code": code,
        "type": resolved_type,
        "message": message,
        "retryable": retryable,
        "recoverable": resolved_recoverable,
        "data": recovery_data,
    }
    if detail is not None:
        error["detail"] = to_structured(detail)

    response: JsonDict = {"ok": False, "error": error}
    for key, value in fields.items():
        if value is not None:
            response[key] = to_structured(value)
    return response


def invalid_argument_response(
    message: str,
    *,
    detail: Any | None = None,
    fix_hint: str | None = None,
    available_options: Sequence[Any] | None = None,
    suggested_tool_calls: Sequence[Mapping[str, Any]] | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a structured invalid-argument failure."""

    return error_response(
        message,
        code="invalid_params",
        error_type="INVALID_ARGUMENT",
        detail=detail,
        recoverable=True,
        data={"fix_hint": fix_hint} if fix_hint else None,
        fix_hint=fix_hint,
        available_options=available_options,
        suggested_tool_calls=suggested_tool_calls,
        **fields,
    )


def not_found_response(
    message: str,
    *,
    detail: Any | None = None,
    fix_hint: str | None = None,
    available_options: Sequence[Any] | None = None,
    suggested_tool_calls: Sequence[Mapping[str, Any]] | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a structured not-found failure with recovery options."""

    return error_response(
        message,
        code="not_found",
        error_type="NOT_FOUND",
        detail=detail,
        recoverable=True,
        fix_hint=fix_hint,
        available_options=available_options,
        suggested_tool_calls=suggested_tool_calls,
        **fields,
    )


def source_unavailable_response(
    message: str,
    *,
    detail: Any | None = None,
    retryable: bool = True,
    fix_hint: str | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a structured source-unavailable failure."""

    return error_response(
        message,
        code="source_unavailable",
        error_type="SOURCE_UNAVAILABLE",
        detail=detail,
        retryable=retryable,
        recoverable=retryable,
        fix_hint=fix_hint,
        **fields,
    )


def policy_denied_response(
    message: str,
    *,
    detail: Any | None = None,
    fix_hint: str | None = None,
    **fields: Any,
) -> JsonDict:
    """Return a structured policy-denied failure."""

    return error_response(
        message,
        code="policy_denied",
        error_type="POLICY_DENIED",
        detail=detail,
        recoverable=True,
        fix_hint=fix_hint,
        **fields,
    )


def validate_report_ingest_payload(
    payload: Any,
    *,
    require_content: bool = False,
    allow_placeholders: bool = True,
    require_identity_context: bool = False,
) -> None:
    """Reject report-ingest fact rows that do not include source evidence fields.

    Report builders pass nested payloads assembled from MCP tool results. Any
    dict row under a ``facts`` or ``fact_rows`` collection is a report-ingest
    fact row and must carry the source evidence contract directly.
    Workflow planner templates can keep ``copy_from_tool_evidence.*``
    placeholders. Final cited report rows should pass ``require_content=True``
    and ``allow_placeholders=False`` after tool evidence has been copied in.
    Workflow-based final reports can also set
    ``require_identity_context=True`` to require identity fields plus either
    copied identity objects or the planner paths for identity and identity_map.
    """

    missing: list[str] = []
    invalid: list[str] = []
    placeholders: list[str] = []
    missing_identity: list[str] = []

    def walk(value: Any, path: str, *, in_fact_rows: bool = False) -> None:
        if isinstance(value, Mapping):
            if in_fact_rows:
                absent = [
                    field
                    for field in REPORT_SOURCE_METADATA_FIELDS
                    if field not in value or (not require_content and value[field] in ("", None))
                ]
                if absent:
                    missing.append(f"{path}: {', '.join(absent)}")
                if not allow_placeholders:
                    placeholder_fields = [
                        field
                        for field in REPORT_SOURCE_METADATA_FIELDS
                        if isinstance(value.get(field), str)
                        and value[field].startswith("copy_from_tool_evidence.")
                    ]
                    if placeholder_fields:
                        placeholders.append(f"{path}: {', '.join(placeholder_fields)}")
                if require_content and not absent:
                    try:
                        validate_evidence_receipt(value, require_content=True)
                    except ReportIngestContractError as exc:
                        invalid.append(f"{path}: {exc}")
                if require_identity_context:
                    identity_absent = _missing_report_identity_context(value)
                    if identity_absent:
                        missing_identity.append(f"{path}: {', '.join(identity_absent)}")
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
    if placeholders:
        raise ReportIngestContractError(
            "Report-ingest fact rows still contain workflow evidence placeholders: " + "; ".join(placeholders)
        )
    if invalid:
        raise ReportIngestContractError("Report-ingest fact rows have invalid evidence content: " + "; ".join(invalid))
    if missing_identity:
        raise ReportIngestContractError(
            "Report-ingest fact rows missing identity context: " + "; ".join(missing_identity)
        )


def _missing_report_identity_context(row: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    identity_fields = row.get("identity_fields")
    if not isinstance(identity_fields, list | tuple) or not identity_fields:
        missing.append("identity_fields")
    if not _has_value(row.get("identity_path")) and not isinstance(row.get("identity"), Mapping):
        missing.append("identity_path_or_identity")
    if not _has_value(row.get("identity_map_path")) and not isinstance(row.get("identity_map"), Mapping):
        missing.append("identity_map_path_or_identity_map")
    return missing


def _has_value(value: Any) -> bool:
    return value not in ("", None, [], {})


def tool_error(message: str, *, code: str = "tool_error", detail: Any | None = None) -> ToolError:
    """Create a FastMCP ToolError with compact structured context in the message."""
    payload = error_response(message, code=code, error_type=code.upper(), detail=detail)
    payload_json = dumps(to_structured(payload), separators=(",", ":"), sort_keys=True)
    return ToolError(payload_json)


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
    "evidence_receipt",
    "EVIDENCE_RECEIPT_FIELDS",
    "EVIDENCE_RECEIPT_REQUIRED_CONTENT_FIELDS",
    "EvidenceReceipt",
    "ToolExecutionError",
    "evidence_receipts_in_payload",
    "evidence_receipt_validation_summary",
    "error_response",
    "invalid_argument_response",
    "not_found_response",
    "policy_denied_response",
    "pagination_meta",
    "raise_invalid_params",
    "raise_not_found",
    "raise_tool_error",
    "record_response",
    "response_envelope",
    "source_unavailable_response",
    "REPORT_SOURCE_METADATA_FIELDS",
    "ReportIngestContractError",
    "to_structured",
    "tool_error",
    "validate_evidence_receipt",
    "validate_report_ingest_payload",
]
