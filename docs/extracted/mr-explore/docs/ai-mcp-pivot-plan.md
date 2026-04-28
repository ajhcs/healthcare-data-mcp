# Plan: AI-First MCP Pivot for MR-Explore

**Generated**: February 17, 2026
**Estimated Complexity**: High
**Planning Status**: Draft v2 (implementation-ready, key architecture decisions locked)

## Overview
Pivot MR-Explore from a GUI-first analytics app into a local AI data platform where:
- The GUI is optimized for import, data quality checks, and dataset management.
- The primary end-user interface is an MCP server for AI agents (Claude, Codex, etc.).
- Query output is constrained to token-safe, typed, paginated responses.

This plan is incremental and preserves current value: existing import and DuckDB foundations remain, while the UI-heavy exploration workflow is gradually deemphasized.

## Business Goals and Success Criteria

### Product Goals
- Enable non-technical users to ingest large MRF files locally.
- Make imported data reliably queryable by AI agents via MCP.
- Prevent context-window overflow with strict server-side budgets.
- Keep all sensitive data local by default.

### Success Metrics (Launch Gate)
- 95%+ successful imports on representative MRF sample set.
- p95 query latency < 2.5s for indexed/filter-first MCP queries.
- 0 MCP tool responses exceeding configured byte/token budget limits.
- At least 3 validated client integrations (Claude Desktop, Codex-compatible client, one generic MCP client).
- No direct raw-table dump tool exposed.

## Scope and Non-Goals

### In Scope
- MCP server implementation and tool contract design.
- Query engine extraction from UI-specific pathways.
- Import wizard refinements for AI-readiness checks.
- Response budgeting, pagination, and summarization controls.
- Packaging and docs for local installation/use.

### Out of Scope (for this pivot)
- Cloud-hosted multi-tenant server.
- User auth system beyond local machine trust boundary.
- Full replacement of existing GUI tabs in initial release.
- Autonomous AI workflow orchestration inside MR-Explore.

## Current State Assessment (Codebase Anchors)
- Import pipeline exists and is robust:
  - `src/ui/mrf_import_wizard.py`
  - `src/data/importer.py`
  - `src/data/normalizer.py`
- Efficient local storage/query foundation exists:
  - `src/data/duckdb_store.py`
  - `src/data/duckdb_adapter.py`
  - `src/data/database_protocol.py`
- App entry and UX are GUI-centric:
  - `src/main.py`
  - `src/ui/main_window.py`
- No MCP transport/server implementation exists yet.
- No API/MCP dependencies currently in `pyproject.toml`.

## Target Architecture

### Layer 1: Ingestion and Dataset Management (GUI)
Purpose: help users import and validate datasets.
- Import CSV/JSON MRF files.
- Normalize to canonical parquet pack schema.
- Store dataset metadata, provenance, and quality diagnostics.
- Manage dataset lifecycle (active/inactive, aliases, tags).

### Layer 2: Query Core (Headless Domain Layer)
Purpose: provide stable query primitives independent of GUI and MCP.
- Filtered retrieval and aggregations.
- Query planning with guardrails.
- Deterministic output shaping and pagination.
- Shared by GUI and MCP.

### Layer 3: MCP Server (Primary End-User Interface)
Purpose: expose AI-safe tools for interactive analysis.
- Stdio transport only in v1; streamable-http deferred to v2.
- Typed input/output schemas.
- Read-only toolset with strict budgets.
- Offset/limit pagination in v1 with deterministic ordering.

### Layer 4: Integrations and Packaging
Purpose: make usage operationally simple.
- Standalone CLI entrypoint to run server (independent from GUI lifecycle).
- Client setup docs/snippets.
- Windows packaging updates.

## MCP Contract Design (Core of the Pivot)

### Design Principles
- Return aggregates first, then drill-down.
- Never return full-table results.
- Enforce hard limits server-side.
- Use deterministic, schema-validated JSON.
- Prefer small payloads and deterministic paging.

### Phase-1 Tool Set
1. `list_datasets()`
- Returns available local dataset ids and summary stats.

2. `describe_dataset(dataset_id)`
- Returns schema version, row counts, import timestamp, quality flags.

3. `search_codes(dataset_id, text, code_type, limit)`
- Fast text/code lookup with capped response.

4. `query_rates(dataset_id, filters, metrics, group_by, detail_level, limit, page)`
- Aggregation + constrained row retrieval with pagination.
- `detail_level="rows"` is enabled in v1 and capped by policy.

### Phase-2 Tool Set
1. `get_provider_context(dataset_id, provider_id)`
2. `compare_rates(dataset_id, cohort_a, cohort_b, metric)`
3. `explain_query_plan(dataset_id, filters, group_by)`

### Response Shape Requirements
- Mandatory metadata envelope:
  - `request_id`
  - `dataset_id`
  - `row_count`
  - `has_more`
  - `next_page`
  - `applied_limits`
  - `warnings`
- Data payload field is predictable by tool name.

## Token, Payload, and Runtime Guardrails

### Hard Limits (Configurable)
- `max_rows_per_call`
- `max_rows_detail_default` (v1 default: 50 when `detail_level="rows"`)
- `max_response_bytes`
- `max_query_seconds`
- `max_group_keys` (v1 default: 500)
- `max_page_number`

### Enforcement Model
- Validate request complexity before query execution.
- Reject dangerous or unbounded requests with explicit error reasons.
- Truncate in deterministic order and set `has_more=true`.
- Offer continuation via `next_page` when truncated.

### Output Policies
- Aggregated/statistical output preferred by default.
- Raw row mode is enabled in v1 via `detail_level="rows"` and capped.
- Include compact value dictionaries for repeated text fields to reduce size.

## Sprint Plan

## Sprint 0: Contract Freeze (Lean)
**Goal**: publish the MCP contract and move immediately into implementation.
**Demo/Validation**:
- Contract draft committed and implementation-ready.

### Task 0.1: Publish MCP contract draft
- **Location**: `docs/mcp-contract-v1.md`
- **Description**: Define tool names, request schemas, response schemas, error envelope.
- **Dependencies**: None
- **Acceptance Criteria**:
  - Every tool has schema and examples.
  - Offset pagination and budget fields standardized.
- **Validation**:
  - JSON schema lint + manual scenario walkthrough.

## Sprint 1: Data Model and Dataset Registry Foundation
**Goal**: ensure imported data can be safely addressed as MCP datasets.
**Demo/Validation**:
- Import produces dataset metadata and quality report.
- Multiple packs can be discovered and listed.

### Task 1.1: Introduce dataset registry schema
- **Location**: `src/data/dataset_registry.py` (new), `data/packs/*/metadata.json`
- **Description**: Add registry abstraction for dataset IDs, status, version, and provenance.
- **Dependencies**: Task 0.1
- **Acceptance Criteria**:
  - Registry lists all valid packs.
  - Invalid/corrupt packs flagged without crash.
- **Validation**:
  - `tests/test_dataset_registry.py` (new)

### Task 1.2: Extend metadata with AI-readiness fields
- **Location**: `src/data/normalizer.py`
- **Description**: Add stats (distinct payer count, missing-rate metrics, import diagnostics).
- **Dependencies**: Task 1.1
- **Acceptance Criteria**:
  - Metadata fields generated on import.
  - Backward compatible with older metadata.
- **Validation**:
  - `tests/test_normalizer.py`

### Task 1.3: Dataset health check utility
- **Location**: `src/data/pack_validator.py` (new)
- **Description**: Validate required parquet tables and schema compatibility.
- **Dependencies**: Task 1.1
- **Acceptance Criteria**:
  - Returns typed pass/fail result with reasons.
- **Validation**:
  - `tests/test_pack_validator.py` (new)

## Sprint 2: Query Core Extraction and Standardization
**Goal**: isolate reusable query logic from GUI and expose stable domain APIs.
**Demo/Validation**:
- Query core can serve existing UI and non-UI callers.
- All results return a shared typed envelope.

### Task 2.1: Create query service abstraction
- **Location**: `src/core/query_service.py` (new)
- **Description**: Encapsulate filtered queries, aggregates, and paging.
- **Dependencies**: Sprint 1
- **Acceptance Criteria**:
  - GUI search path migrated to query service wrapper.
- **Validation**:
  - `tests/test_query_service.py` (new)

### Task 2.2: Implement query limit policy engine
- **Location**: `src/core/query_limits.py` (new)
- **Description**: Enforce row/time/grouping constraints pre- and post-query.
- **Dependencies**: Task 2.1
- **Acceptance Criteria**:
  - Rejects unbounded queries.
  - Returns explicit policy violation details.
- **Validation**:
  - `tests/test_query_limits.py` (new)

### Task 2.3: Add deterministic offset pagination contract
- **Location**: `src/core/query_service.py`, `src/mcp/tools/query.py`
- **Description**: Implement `page` + `limit` handling with stable ordering and max page guards.
- **Dependencies**: Task 2.1
- **Acceptance Criteria**:
  - Paging is deterministic and reproducible for identical filters.
  - Out-of-bounds pages return explicit errors.
- **Validation**:
  - `tests/test_query_service.py`, `tests/test_mcp_query_tools.py`

## Sprint 3: MCP Server Bootstrap (Read-Only)
**Goal**: ship a working MCP server with core dataset and query tools.
**Demo/Validation**:
- Local MCP client can connect via stdio and run 4 core tools.
- All tool outputs are schema-valid and budget-respecting.

### Task 3.1: Add MCP dependencies and entrypoint
- **Location**: `pyproject.toml`, `src/mcp/__init__.py`, `src/mcp/server.py` (new)
- **Description**: Add MCP Python SDK, create runnable server module.
- **Dependencies**: Sprint 2
- **Acceptance Criteria**:
  - `python -m src.mcp.server` starts successfully.
- **Validation**:
  - `tests/test_mcp_smoke.py` (new)

### Task 3.2: Implement `list_datasets` and `describe_dataset`
- **Location**: `src/mcp/tools/datasets.py` (new)
- **Description**: Connect registry to MCP tool contract.
- **Dependencies**: Task 3.1
- **Acceptance Criteria**:
  - Returns expected metadata envelope.
- **Validation**:
  - `tests/test_mcp_datasets.py` (new)

### Task 3.3: Implement `search_codes` and `query_rates`
- **Location**: `src/mcp/tools/query.py` (new)
- **Description**: Bridge query core to MCP tools with strict filters and limits.
- **Dependencies**: Task 3.1, Task 2.1, Task 2.2
- **Acceptance Criteria**:
  - Filtered queries only; no full dump path.
  - `query_rates` supports `detail_level="rows"` in v1 with default row cap 50.
  - Aggregate/grouped mode cap enforced at 500 group keys.
- **Validation**:
  - `tests/test_mcp_query_tools.py` (new)

### Task 3.4: Standardize MCP error envelope
- **Location**: `src/mcp/errors.py` (new)
- **Description**: Define machine-readable errors for invalid input, policy violation, and internal failure.
- **Dependencies**: Task 3.1
- **Acceptance Criteria**:
  - Error responses are deterministic and actionable.
- **Validation**:
  - `tests/test_mcp_errors.py` (new)

## Sprint 4: Token-Aware Output Shaping and Query Optimization
**Goal**: make large data interactions reliable in constrained model contexts.
**Demo/Validation**:
- Large queries produce bounded, concise responses with continuation paths.
- Payload overflows are prevented by server, not client behavior.

### Task 4.1: Implement response budget estimator
- **Location**: `src/mcp/response_budget.py` (new)
- **Description**: Estimate serialized payload bytes/tokens and trim safely.
- **Dependencies**: Sprint 3
- **Acceptance Criteria**:
  - `applied_limits` populated on every response.
- **Validation**:
  - `tests/test_response_budget.py` (new)

### Task 4.2: Extend query limit engine with adaptive downgrade rules
- **Location**: `src/core/query_limits.py`
- **Description**: Merge aggregate-first behavior into policy engine for risky/high-cardinality requests.
- **Dependencies**: Task 2.2
- **Acceptance Criteria**:
  - Risky row-level requests are downgraded or rejected with warnings.
- **Validation**:
  - `tests/test_query_limits.py`

### Task 4.3: Query profile and index tuning
- **Location**: `src/data/duckdb_adapter.py`, `src/data/duckdb_store.py`
- **Description**: Tune ordering/filter predicates and hot-path joins.
- **Dependencies**: Task 4.2
- **Acceptance Criteria**:
  - p95 meets target on benchmark dataset.
- **Validation**:
  - `tests/benchmark_import.py` + new query benchmarks.

## Sprint 5: GUI Refocus to Import + Dataset Operations
**Goal**: align GUI with new product role (setup and data operations only).
**Demo/Validation**:
- User can import, validate, and activate dataset for MCP use.
- GUI provides simple MCP setup guidance.

### Task 5.1: Add dataset management panel
- **Location**: `src/ui/main_window.py`, `src/ui/` (new panel)
- **Description**: List datasets, statuses, row counts, validation state.
- **Dependencies**: Sprint 1
- **Acceptance Criteria**:
  - Active dataset can be switched without manual file edits.
- **Validation**:
  - `tests/test_dataset_panel.py` (new)

### Task 5.2: Add MCP connection helper UI
- **Location**: `src/ui/main_window.py`, `src/ui/help.py`
- **Description**: Show copy-ready config snippets for common clients.
- **Dependencies**: Sprint 3
- **Acceptance Criteria**:
  - User can launch/test standalone `mr-explore-mcp` server from GUI helper actions.
- **Validation**:
  - Manual UX verification + smoke test.

### Task 5.3: De-emphasize legacy analytics tabs
- **Location**: `src/ui/main_window.py`
- **Description**: Keep compatibility but mark chart/export workflows as secondary.
- **Dependencies**: Task 5.1
- **Acceptance Criteria**:
  - Dataset management is tab position 1 (default landing view).
  - Legacy chart/export tabs remain visible in later tab positions.
- **Validation**:
  - UX acceptance walkthrough.

## Sprint 6: Integration Testing and Client Compatibility
**Goal**: ensure real AI clients can operate safely and effectively.
**Demo/Validation**:
- End-to-end MCP interactions validated for multiple clients.
- Failure modes are explicit and recoverable.

### Task 6.1: Build MCP integration test harness
- **Location**: `tests/integration/test_mcp_e2e.py` (new)
- **Description**: Simulate representative agent workflows end-to-end.
- **Dependencies**: Sprint 4
- **Acceptance Criteria**:
  - Covers dataset discovery, query, offset pagination, and error cases.
- **Validation**:
  - CI test run with seeded packs.

### Task 6.2: Add large-dataset stress suite
- **Location**: `tests/benchmark_mcp_queries.py` (new)
- **Description**: Validate budgets and latency under heavy cardinality.
- **Dependencies**: Task 6.1
- **Acceptance Criteria**:
  - No budget bypass observed.
- **Validation**:
  - Benchmark report artifact.

### Task 6.3: Compatibility matrix and playbooks
- **Location**: `docs/mcp-clients.md` (new)
- **Description**: Client-specific setup and known caveats.
- **Dependencies**: Task 6.1
- **Acceptance Criteria**:
  - Verified instructions for at least 3 clients.
- **Validation**:
  - Reproducible setup by second operator.

## Sprint 7: Packaging, Release, and Transition
**Goal**: deliver production-ready release and migration guidance.
**Demo/Validation**:
- Installer contains all needed components.
- Existing users can migrate without data loss.

### Task 7.1: Add MCP script entrypoint
- **Location**: `pyproject.toml`
- **Description**: Add `mr-explore-mcp` console command.
- **Dependencies**: Sprint 3
- **Acceptance Criteria**:
  - Command runs in dev + packaged env.
- **Validation**:
  - Local and installer smoke tests.

### Task 7.2: Update installer/build scripts
- **Location**: `build_app.bat`, `MR-Explore.spec`, `build_installer.bat`
- **Description**: Include MCP module/config/docs in distribution.
- **Dependencies**: Task 7.1
- **Acceptance Criteria**:
  - Fresh install can run GUI and MCP server.
- **Validation**:
  - Clean VM installation test.

### Task 7.3: Migration and deprecation docs
- **Location**: `README.md`, `CHANGELOG.md`, `docs/migration-to-ai-first.md` (new)
- **Description**: Document behavior changes and migration steps.
- **Dependencies**: Sprint 5
- **Acceptance Criteria**:
  - Legacy and new workflows clearly documented.
- **Validation**:
  - Docs walkthrough from zero context.

## Parallelization Tracks

### Track A: Data Foundations
Sprint 1 + Task 2.1 can progress early.

### Track B: MCP Interface
Sprint 3 can begin once query service contracts are stable.

### Track C: UX Refocus
Sprint 5 can start after registry is done, independent from response budget internals.

### Track D: Integration and Packaging
Sprint 6/7 begin once core MCP tools and envelopes are stable.

## Testing Strategy
- Unit tests for registry, limits, paging, response shaping.
- Contract tests for MCP tool schemas and error envelopes.
- Integration tests for end-to-end client flows.
- Performance tests for latency and payload bounds.
- Regression tests ensuring existing import path still works.

## Operational Concerns

### Security and Safety
- Read-only tool surface.
- Table/column allowlist for any dynamic query component.
- No file-system traversal from tool inputs.
- Sanitized logging for tool arguments and errors.

### Observability
- Structured logs with request IDs.
- Metrics: query duration, truncation count, policy violations, page depth.
- Optional local debug endpoint for diagnostics (phase 2).

### Configuration
- Extend `config.yaml` with MCP and policy sections.
- Reasonable defaults for local machines.
- Environment variable overrides for automation.

## Risks and Mitigations
- Risk: schema drift across imported MRFs.
  - Mitigation: schema versioning + validator + compatibility adapters.
- Risk: runaway query cardinality.
  - Mitigation: preflight query complexity checks and hard caps.
- Risk: MCP client differences.
  - Mitigation: compatibility matrix + strict response contract.
- Risk: user confusion during transition.
  - Mitigation: explicit in-app messaging and migration docs.

## Rollback Plan
- Keep existing GUI search path behind feature flag during transition.
- Maintain legacy data access APIs for one release cycle.
- Ship MCP as additive capability before making it default-first in docs/UI.
- If MCP issues emerge, disable MCP entrypoint in packaging while preserving data and GUI import.

## Decision Log
### Locked Decisions
1. `query_rates` supports row-level output in v1.
2. `query_rates` default is `detail_level="rows"` with cap 50 rows.
3. Grouped/aggregate responses are capped at 500 group keys.
4. v1 transport is stdio-only.
5. Chart/export tabs remain visible; dataset management is the default landing tab.
6. MCP server is standalone command-first; GUI may optionally launch it as a subprocess helper.

### Open Decisions
1. Which single dataset is active by default when multiple packs exist?
2. Should older packs be auto-migrated on load or only warned?
3. What is the initial max response budget target (bytes/tokens)?
4. Which 3 client integrations are release blockers?
5. Is per-query audit logging required for compliance workflows?
6. Do you want feature flags in `config.yaml` for phased rollout?

## Documentation References Used for this Plan
- MCP Python SDK documentation (Context7): `/modelcontextprotocol/python-sdk/v1.12.4`
- Existing project architecture and modules under `src/data/` and `src/ui/`
