# Structured MCP Tool Results

FastMCP can emit `structuredContent` and output schemas when tools return Python
objects instead of `json.dumps(...)` strings. Use
`shared.utils.mcp_response` as the migration layer for consistent shapes while
server tools move one at a time.

## Helper Shapes

- `response_envelope(...)` returns `{"ok": true, ...}` with JSON-compatible
  payload fields.
- `collection_response(results, limit=..., offset=..., total=...)` returns
  `ok`, `results`, `count`, and `meta.pagination`.
- `record_response(record, key="facility")` returns one model or dict under a
  stable top-level key.
- `empty_response(message)` returns an empty successful result set.
- `error_response(message, code=..., detail=...)` returns a structured failure
  envelope for recoverable, non-exceptional failures.
- `raise_invalid_params`, `raise_not_found`, and `raise_tool_error` raise
  FastMCP `ToolError` for invalid inputs or runtime failures.

The helpers convert Pydantic models, dates, dataclasses, and other JSON-like
values to JSON-compatible Python objects. Do not pre-serialize with
`json.dumps`; let FastMCP serialize the returned object.

## Search Tool Migration

Before:

```python
@mcp.tool()
async def search_facilities(state: str | None = None, limit: int = 50) -> str:
    facilities = [_row_to_facility(row).model_dump() for _, row in results.iterrows()]
    return json.dumps({"count": len(facilities), "results": facilities})
```

After:

```python
from typing import Any

from shared.utils.mcp_response import collection_response, raise_invalid_params


@mcp.tool(structured_output=True)
async def search_facilities(state: str | None = None, limit: int = 50) -> dict[str, Any]:
    if limit < 1:
        raise_invalid_params("limit must be positive", detail={"limit": limit})

    facilities = [_row_to_facility(row) for _, row in results.iterrows()]
    return collection_response(facilities, limit=limit, offset=0)
```

## Record Tool Migration

Before:

```python
@mcp.tool()
async def get_facility(ccn: str) -> str:
    if matches.empty:
        return json.dumps({"error": f"No facility found with CCN: {ccn}"})

    return json.dumps(_row_to_facility(matches.iloc[0]).model_dump())
```

After:

```python
from typing import Any

from shared.utils.mcp_response import raise_not_found, record_response


@mcp.tool(structured_output=True)
async def get_facility(ccn: str) -> dict[str, Any]:
    if matches.empty:
        raise_not_found(f"No facility found with CCN: {ccn}", detail={"ccn": ccn})

    return record_response(_row_to_facility(matches.iloc[0]), key="facility")
```

## Recoverable Failure Migration

Use a raised `ToolError` when the tool cannot satisfy the request because inputs
are invalid, an expected record is absent, or execution failed. Use
`error_response(...)` only when a failure is part of a normal response contract
and clients should continue processing the response.

```python
from typing import Any

from shared.utils.mcp_response import error_response


@mcp.tool(structured_output=True)
async def optional_upstream_status() -> dict[str, Any]:
    if upstream_disabled:
        return error_response(
            "Optional upstream is disabled",
            code="upstream_disabled",
            retryable=False,
        )

    return {"ok": True, "status": "available"}
```

## Migration Notes

1. Change tool return annotations from `str` to `dict[str, Any]` or a concrete
   Pydantic response model.
2. Add `structured_output=True` to `@mcp.tool(...)` when you want FastMCP to
   require a structured-compatible return annotation.
3. Replace `json.dumps({"count": len(results), "results": results})` with
   `collection_response(results, ...)`.
4. Replace record-level JSON strings with `record_response(...)` or a concrete
   Pydantic response model.
5. Replace error JSON payloads for invalid inputs and runtime failures with
   `raise_invalid_params`, `raise_not_found`, or `raise_tool_error`.
