# MR-Explore MCP Contract v1

**Version**: 1.0.0-draft
**Date**: February 17, 2026
**Status**: Contract freeze candidate for implementation

## Purpose
This document defines the v1 Model Context Protocol (MCP) contract for MR-Explore.

Primary goals:
- Make local MRF datasets queryable by AI agents.
- Keep responses token-safe and deterministic.
- Prevent unbounded scans and table dumps.
- Standardize tool behavior across MCP clients.

## Scope
In scope for v1:
- MCP server over `stdio` transport.
- Read-only tools for dataset discovery and querying.
- Deterministic paging (`page` + `limit`).
- Structured response envelope with limits and warnings.

Out of scope for v1:
- Streamable HTTP transport.
- Write or mutation tools.
- Raw SQL execution tools.
- Cursor/token pagination.

## Transport and Runtime Model
- Transport: `stdio` only.
- Server lifecycle: standalone CLI command (`mr-explore-mcp`) is the primary mode.
- GUI integration: optional helper that launches the standalone command as a subprocess.

## Dataset Model
A dataset is a validated local parquet pack under `data/packs/<dataset_id>/` with metadata.

Dataset identity rules:
- `dataset_id` is stable and filesystem-safe.
- All query tools require an explicit `dataset_id`.
- No implicit cross-dataset joins in v1.

## Global Contract Rules
1. All tools are read-only.
2. All responses are structured JSON objects.
3. All responses include a metadata envelope.
4. Server-side limits are always enforced.
5. If the server trims results, it must set `has_more=true` and provide `next_page`.
6. Unknown fields in request payloads are rejected.
7. Empty result sets are valid and not treated as errors.

## Response Envelope (All Tools)
All tool responses must include:

```json
{
  "request_id": "req_01J...",
  "dataset_id": "main",
  "row_count": 42,
  "has_more": false,
  "next_page": null,
  "applied_limits": {
    "max_rows_per_call": 50,
    "max_group_keys": 500,
    "max_response_bytes": 200000,
    "max_query_seconds": 5.0
  },
  "warnings": [],
  "data": {}
}
```

Field rules:
- `request_id`: unique per tool call.
- `row_count`: number of rows returned in `data` payload (not total dataset rows).
- `has_more`: true when additional pages are available or results were truncated.
- `next_page`: integer page number for next request or `null`.
- `applied_limits`: actual limits applied for this request.
- `warnings`: list of non-fatal warning objects.
- `data`: tool-specific payload.

## Warning Object
```json
{
  "code": "RESULT_TRUNCATED",
  "message": "Response capped by max_rows_per_call",
  "details": {
    "returned_rows": 50,
    "estimated_total": 327
  }
}
```

## Error Envelope
Errors must be machine-readable and consistent.

```json
{
  "error": {
    "code": "POLICY_LIMIT_EXCEEDED",
    "message": "Requested group_by cardinality exceeds max_group_keys",
    "details": {
      "max_group_keys": 500,
      "requested": 1700
    },
    "retryable": false
  }
}
```

## Standard Error Codes
- `INVALID_ARGUMENT`
- `DATASET_NOT_FOUND`
- `DATASET_INVALID`
- `UNSUPPORTED_VALUE`
- `POLICY_LIMIT_EXCEEDED`
- `QUERY_TIMEOUT`
- `INTERNAL_ERROR`

## Shared Types

### `DetailLevel`
- `"rows"` (default in v1)
- `"aggregate"`

### `RateFilters`
All fields are optional unless stated.

```json
{
  "hospital_ids": [1, 2],
  "payer_name": "Blue Cross",
  "plan_name": "PPO",
  "setting": "outpatient",
  "code": "27447",
  "code_type": "CPT",
  "billing_class": "professional",
  "min_negotiated_dollar": 1000.0,
  "max_negotiated_dollar": 5000.0,
  "query": "knee arthroplasty"
}
```

### `Metrics`
Allowed values in v1:
- `count`
- `min_negotiated_dollar`
- `max_negotiated_dollar`
- `avg_negotiated_dollar`
- `median_negotiated_dollar`

### `GroupBy`
Allowed fields in v1:
- `hospital_id`
- `hospital_name`
- `payer_name`
- `plan_name`
- `code_1`
- `code_1_type`
- `setting`
- `billing_class`

## Tool Contracts

## 1) `list_datasets`
Returns discoverable local datasets and basic health status.

Input schema:
```json
{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "include_inactive": {"type": "boolean", "default": false}
  }
}
```

Output `data` shape:
```json
{
  "datasets": [
    {
      "dataset_id": "main",
      "display_name": "main",
      "status": "ready",
      "pack_path": "data/packs/main",
      "schema_version": "1.0",
      "imported_at": "2026-02-17T13:10:05Z",
      "charges_count": 834221,
      "hospitals_count": 12,
      "quality_flags": []
    }
  ]
}
```

Behavior:
- `row_count` equals `datasets.length`.
- `has_more` is always `false` in v1.

## 2) `describe_dataset`
Returns schema, quality, and readiness details for one dataset.

Input schema:
```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["dataset_id"],
  "properties": {
    "dataset_id": {"type": "string", "minLength": 1}
  }
}
```

Output `data` shape:
```json
{
  "dataset": {
    "dataset_id": "main",
    "status": "ready",
    "schema_version": "1.0",
    "imported_at": "2026-02-17T13:10:05Z",
    "tables": {
      "charges": 834221,
      "hospitals": 12,
      "descriptions": 14217,
      "payers": 217,
      "plans": 490,
      "algorithms": 23,
      "methodologies": 8
    },
    "quality_report": {
      "missing_payer_rate": 0.004,
      "missing_plan_rate": 0.031,
      "missing_negotiated_dollar_rate": 0.112,
      "validation_errors": []
    }
  }
}
```

Behavior:
- `row_count` is `1` for success.
- Unknown `dataset_id` returns `DATASET_NOT_FOUND`.

## 3) `search_codes`
Performs text/code lookup with strict result caps.

Input schema:
```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["dataset_id", "text"],
  "properties": {
    "dataset_id": {"type": "string", "minLength": 1},
    "text": {"type": "string", "minLength": 1},
    "code_type": {"type": "string"},
    "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 25},
    "page": {"type": "integer", "minimum": 1, "default": 1}
  }
}
```

Output `data` shape:
```json
{
  "matches": [
    {
      "code_1": "27447",
      "code_1_type": "CPT",
      "description": "Arthroplasty, knee, condyle and plateau",
      "match_score": 0.93,
      "sample_count": 184
    }
  ]
}
```

Behavior:
- Stable ordering: `match_score DESC`, then `code_1 ASC`.
- `row_count` is number of `matches` returned.
- `has_more` and `next_page` follow normal paging rules.

## 4) `query_rates`
Core analytics tool for rate retrieval and aggregation.

Input schema:
```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["dataset_id"],
  "properties": {
    "dataset_id": {"type": "string", "minLength": 1},
    "filters": {"type": "object"},
    "detail_level": {
      "type": "string",
      "enum": ["rows", "aggregate"],
      "default": "rows"
    },
    "metrics": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "count",
          "min_negotiated_dollar",
          "max_negotiated_dollar",
          "avg_negotiated_dollar",
          "median_negotiated_dollar"
        ]
      },
      "default": ["count", "avg_negotiated_dollar"]
    },
    "group_by": {
      "type": "array",
      "items": {
        "type": "string",
        "enum": [
          "hospital_id",
          "hospital_name",
          "payer_name",
          "plan_name",
          "code_1",
          "code_1_type",
          "setting",
          "billing_class"
        ]
      },
      "default": []
    },
    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
    "page": {"type": "integer", "minimum": 1, "default": 1}
  }
}
```

Output `data` shape for `detail_level="rows"`:
```json
{
  "rows": [
    {
      "hospital_id": 1,
      "hospital_name": "General Hospital",
      "description": "Arthroplasty, knee, condyle and plateau",
      "code_1": "27447",
      "code_1_type": "CPT",
      "payer_name": "Blue Cross",
      "plan_name": "PPO",
      "setting": "outpatient",
      "negotiated_dollar": 3125.44,
      "min_charge": 2500.0,
      "max_charge": 5100.0,
      "billing_class": "professional"
    }
  ]
}
```

Output `data` shape for `detail_level="aggregate"`:
```json
{
  "groups": [
    {
      "group": {
        "code_1": "27447",
        "payer_name": "Blue Cross"
      },
      "metrics": {
        "count": 184,
        "min_negotiated_dollar": 2500.0,
        "max_negotiated_dollar": 5100.0,
        "avg_negotiated_dollar": 3342.17,
        "median_negotiated_dollar": 3290.0
      }
    }
  ]
}
```

Behavior and policy requirements:
- `detail_level="rows"` is enabled in v1.
- Effective row cap defaults to 50 unless stricter limits apply.
- Aggregate/group mode hard cap is 500 group keys.
- If `group_by` is empty and `detail_level="aggregate"`, aggregate over filtered dataset as a single group.
- Stable ordering:
  - Row mode: `description ASC`, `code_1 ASC`, `hospital_id ASC`.
  - Aggregate mode: by first `group_by` field ASC, then remaining fields ASC.
- If request exceeds policy, return `POLICY_LIMIT_EXCEEDED`.

## Paging Contract (v1)
- Inputs: `page` (1-based), `limit`.
- Server computes offset as `(page - 1) * limit`.
- Response fields:
  - `has_more = true` when more results are available.
  - `next_page = page + 1` when `has_more=true`, else `null`.
- No cursor tokens in v1.

## Limit Application Order
1. Validate argument schema.
2. Validate allowed fields and enum values.
3. Apply policy caps (rows, groups, response bytes, query time).
4. Execute query with deterministic ordering.
5. Trim payload if byte/time budget requires additional truncation.
6. Emit warnings and paging fields.

## Security and Safety Rules
- No mutation statements.
- No user-provided SQL fragments.
- Only allowlisted fields for `filters`, `group_by`, and `metrics`.
- Log only sanitized arguments.
- Reject unknown input keys with `INVALID_ARGUMENT`.

## Example Calls and Responses

### Example A: list datasets
Request arguments:
```json
{}
```

Response data (abridged):
```json
{
  "request_id": "req_001",
  "dataset_id": "_system",
  "row_count": 2,
  "has_more": false,
  "next_page": null,
  "applied_limits": {"max_rows_per_call": 50},
  "warnings": [],
  "data": {
    "datasets": [
      {"dataset_id": "main", "status": "ready"},
      {"dataset_id": "northwest_2025", "status": "ready"}
    ]
  }
}
```

### Example B: query rows for CPT
Request arguments:
```json
{
  "dataset_id": "main",
  "detail_level": "rows",
  "filters": {"code": "27447", "code_type": "CPT"},
  "limit": 50,
  "page": 1
}
```

Response behavior:
- Returns first 50 matching rows.
- Sets `has_more` and `next_page` if more matches exist.

### Example C: aggregate by payer
Request arguments:
```json
{
  "dataset_id": "main",
  "detail_level": "aggregate",
  "filters": {"code": "27447", "code_type": "CPT"},
  "group_by": ["payer_name"],
  "metrics": ["count", "avg_negotiated_dollar"],
  "limit": 100,
  "page": 1
}
```

Response behavior:
- Returns grouped metrics per payer.
- Enforces max 500 groups.

## Implementation Mapping (Code Targets)
- Server bootstrap: `src/mcp/server.py`
- Tool handlers: `src/mcp/tools/datasets.py`, `src/mcp/tools/query.py`
- Error envelope: `src/mcp/errors.py`
- Query core: `src/core/query_service.py`
- Policy engine: `src/core/query_limits.py`
- Dataset discovery: `src/data/dataset_registry.py`

## Compatibility Notes
- Contract is optimized for MCP clients that consume structured tool outputs.
- Fields in `applied_limits` may grow over time; clients should ignore unknown keys.
- Future v2 changes (planned): streamable-http transport and optional cursor pagination.

## Change Control
- Any breaking change requires contract version bump.
- Non-breaking additions (new optional fields) must preserve existing defaults and semantics.
