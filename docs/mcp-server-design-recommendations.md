# MCP Server Design Recommendations

Date: 2026-05-29

This review applies the local `mcp-server-design` skill to the whole `healthcare-data-mcp` codebase. The repo already has strong agent-oriented foundations: registry-backed server metadata, task-first workflow plans, evidence receipts, identity maps, gateway auth controls, source caveats, and live-gateway policy enforcement. The recommendations below focus on the gaps that still make the tools harder for agents to discover, recover from, and use correctly.

## Priority Ranking

| Rank | Priority | Recommendation | Why now |
|---:|---|---|---|
| 1 | P0 | Standardize structured tool errors and recovery hints | Agents need machine-parseable failures to recover safely; current failures are inconsistent. |
| 2 | P0 | Add agent-grade tool documentation and enforce it in CI | Inventory found 144 tools and 0 tools using the full Discovery / Do-Don't / Examples / Common mistakes template. |
| 3 | P1 | Split or capability-gate oversized tool clusters | Several servers exceed the <=7 tool design target and will confuse tool selection. |
| 4 | P1 | Add per-server resources for local discovery | Only `discovery` exposes MCP resources; single-server clients lack resource-based parameter discovery. |
| 5 | P1 | Centralize input normalization, mistake detection, and suggestions | Normalization is useful but ad hoc; not-found paths rarely suggest recoveries. |
| 6 | P1 | Harden public web fetch against redirect and DNS-rebinding SSRF | Initial URL host validation exists, but redirects use the shared client with `follow_redirects=True`. |
| 7 | P2 | Promote common workflows into macro tools | Workflow plans exist, but agents still need to orchestrate multi-step workflows manually. |
| 8 | P2 | Use atomic writes and metadata sidecars consistently for caches | Some loaders use shared atomic writes; many still write cache artifacts directly. |
| 9 | P2 | Add shared tool-call observability outside the live gateway | Live-gateway calls are audited, but local stdio/HTTP tools lack uniform duration/error/output metrics. |
| 10 | P2 | Add MCP UX validation tests | Existing tests cover data contracts well; add tests for doc contracts, error payloads, and mistake recovery. |

## 1. Standardize Structured Tool Errors And Recovery Hints

**Priority:** P0

**Justification:** `shared/utils/mcp_response.py` has useful response helpers, but `error_response()` emits `ok: false` with `code`, `message`, and `retryable`, while `tool_error()` embeds details into a plain `ToolError` string. Tool implementations also mix returned error envelopes and raised errors. Agents need stable fields like `error.type`, `recoverable`, `data.fix_hint`, `available_options`, and `suggested_tool_calls` to recover from `NOT_FOUND`, `INVALID_ARGUMENT`, `SOURCE_UNAVAILABLE`, and `POLICY_DENIED`.

**Small fix plan:**

1. Add a shared `ToolExecutionError` dataclass/exception with `error_type`, `message`, `recoverable`, and `data`.
2. Extend `error_response()` to keep backward-compatible `code` while also emitting `type`, `recoverable`, and structured `data`.
3. Add helpers for not-found, invalid-argument, source-unavailable, and policy-denied responses.
4. Update high-traffic surfaces first: `gateway`, `discovery`, `cms-facility`, `public-records`, `hospital-quality`, `provider-enrollment`, and `live_gateway`.
5. Add tests that assert the structured payload and recovery hints for each error class.

## 2. Add Agent-Grade Tool Documentation And Enforce It In CI

**Priority:** P0

**Justification:** AST inventory found 144 MCP tools and 0 tools with the full required sections from the skill: `Discovery`, `When to use`, `Do / Don't`, `Examples`, and `Common mistakes`. Current docstrings are mostly concise human API notes. Agents choose tools from docstrings, so this guidance needs to live in the MCP tool surface, not only in docs and workflow plans.

**Small fix plan:**

1. Create a repo documentation contract for MCP tool docstrings.
2. Add a pytest AST check that every `@mcp.tool` docstring includes the required sections or an explicit allowlist reason.
3. Convert externally exposed surfaces first: `gateway`, `discovery`, `live_gateway`, `cms-facility`, `public-records`, `hospital-quality`, and `provider-enrollment`.
4. Include JSON-RPC examples with realistic public identifiers such as CCN, NPI, ZCTA, state, dataset ID, and workflow ID.
5. Document common healthcare-data mistakes: exact-vs-candidate identity, missing API keys, source caveats, public-data-not-PHI, and preserving evidence receipts.

## 3. Split Or Capability-Gate Oversized Tool Clusters

**Priority:** P1

**Justification:** Most servers land in the desired 4-7 tool range, but several exceed it: `public_records` has 25 tools, `workforce_analytics` has 19, `financial_intelligence` has 11, `discovery` has 12, and `research_trials` has 8. The skill's target is <=7 tools per cluster because large menus degrade tool selection.

**Small fix plan:**

1. Split `public-records` into focused clusters such as exclusion screening, state public records, cyber records, and accreditation/interop.
2. Split or capability-gate `workforce-analytics` into workforce supply, training programs, labor activity, and operations throughput.
3. Split or capability-gate `financial-intelligence` into nonprofit finance, SEC/municipal finance, and public financial health.
4. Keep backward-compatible launcher aliases for one release if external users depend on current server IDs.
5. Update `SERVER_REGISTRY`, presets, gateway metadata, workflow plans, Compose files, generated configs, and distribution tests together.

## 4. Add Per-Server Resources For Local Discovery

**Priority:** P1

**Justification:** Only `servers/discovery/server.py` defines MCP resources and prompts. All operational data servers expose tools but no resources. If an agent connects only to `cms-facility` or `public-records`, it cannot use resources to discover valid identifiers, cache/source state, evidence paths, or recommended next calls.

**Small fix plan:**

1. Add a shared `register_standard_resources(mcp, server_id)` helper.
2. Expose metadata-only resources such as `healthcare-data://server/{server_id}/capabilities`, `/datasets`, `/sources`, `/cache-status`, `/examples`, and `/identity-contract`.
3. Add useful templates where feasible, such as `healthcare-data://cms-facility/{ccn}` and `healthcare-data://public-records/exclusions/source-status`.
4. Keep resource handlers side-effect free and avoid importing heavy loaders.
5. Add smoke tests that each server exposes its standard resources.

## 5. Centralize Input Normalization, Mistake Detection, And Suggestions

**Priority:** P1

**Justification:** Some tools normalize well in place, such as ZCTA zero-fill in `geo-demographics`, while exact lookups like CCN often strip and compare before returning a plain not-found response. There is no shared detector for placeholders, malformed CCNs/NPIs/ZCTAs/FIPS values, names passed where exact IDs are required, unknown dataset/workflow IDs, or workflow placeholders copied into direct calls.

**Small fix plan:**

1. Add `shared/utils/input_normalization.py` for CCN, NPI, state, ZCTA, FIPS, dataset ID, workflow ID, and URL normalization.
2. Add `shared/utils/mistake_detection.py` with error types like `PLACEHOLDER_INPUT`, `NAME_USED_FOR_EXACT_ID`, `INVALID_IDENTIFIER_FORMAT`, `UNKNOWN_DATASET_ID`, and `UNSAFE_URL`.
3. Use registry and dataset catalogs for fuzzy suggestions on server IDs, tool IDs, workflow IDs, and dataset IDs.
4. For exact healthcare identifiers, return `suggested_tool_calls`, such as calling `search_facilities` with name/state before `get_facility` with a returned CCN.
5. Add tests for malformed identifiers and placeholder values.

## 6. Harden Public Web Fetch Against Redirect And DNS-Rebinding SSRF

**Priority:** P1

**Justification:** `web_intelligence.fetch_web_page()` validates the initial URL and rejects private hosts, but `_fetch_and_parse()` uses the shared HTTP client, which follows redirects by default. A public URL can redirect to localhost, a private RFC1918 address, or `169.254.169.254`. Existing tests cover direct private URLs and private DNS resolution, but not redirect-chain SSRF.

**Small fix plan:**

1. Change `_fetch_and_parse()` to use `follow_redirects=False` and manually follow a small bounded redirect chain.
2. Re-run public-host validation for every `Location` target after resolving relative redirects.
3. Reject non-http(s) redirects, excessive redirects, private targets, and blocked metadata hostnames.
4. Add tests for 301/302/307 redirects to localhost, RFC1918 addresses, `169.254.169.254`, and blocked metadata hostnames.
5. Add an explicit max-response-bytes guard for fetched HTML.

## 7. Promote Common Workflows Into Macro Tools

**Priority:** P2

**Justification:** `shared/utils/workflows.py` defines strong task-first plans and `discovery` exposes them, but the plans are read-only. Agents still have to execute several calls and preserve identity/evidence contracts manually.

**Small fix plan:**

1. Implement a small set of macro tools rather than all workflows at once.
2. Start with `macro_quality_measure_lookup`, `macro_compliance_exclusion_screening`, and `macro_facility_profile_readiness`.
3. Return component call results, evidence receipts, identity maps, skipped/blocked steps, and `next_actions`.
4. Keep macros read-only and bounded; do not hide source caveats or unresolved identifiers.
5. Add tests that macro outputs satisfy report-ingest evidence requirements where report rows are emitted.

## 8. Use Atomic Writes And Metadata Sidecars Consistently For Caches

**Priority:** P2

**Justification:** `shared/utils/cache.py` provides atomic byte writes and cache metadata helpers, but many loaders still write directly with `write_bytes()`, `write_text()`, `to_csv()`, or `to_parquet()`. Interrupted downloads or concurrent runs can leave truncated cache files that later tools treat as valid public-source data.

**Small fix plan:**

1. Add shared helpers for atomic JSON, CSV, and Parquet writes.
2. Migrate cache-producing direct write sites across CMS facility, financial intelligence, claims analytics, health-system-profiler, web intelligence, workforce, public records, price transparency, and state-health loaders.
3. Ensure cache sidecars include source URL, fetched timestamp, content length, checksum or ETag when available, and source period.
4. Add tests that simulate interrupted writes and verify the previous good cache remains intact.
5. Update `doctor` to flag missing sidecars where provenance is required.

## 9. Add Shared Tool-Call Observability Outside The Live Gateway

**Priority:** P2

**Justification:** `live_gateway` records audit events, request/result sizes, rate-limit decisions, provenance status, and policy outcomes. Local stdio and individual HTTP servers do not appear to have a shared per-tool timing, result-size, error-type, or slow-call tracking layer.

**Small fix plan:**

1. Add a lightweight shared decorator or FastMCP wrapper helper for duration, error type, result size, cache status, and source dataset ID.
2. Log structured JSON without PHI, secrets, or raw bearer tokens.
3. Expose `healthcare-data://tooling/metrics` on each server or through discovery.
4. Add slow-call thresholds by source family.
5. Add tests for redaction and event shape.

## 10. Add MCP UX Validation Tests

**Priority:** P2

**Justification:** The suite has good data-contract, gateway-auth, packaging, workflow, and source-metadata coverage. It does not yet enforce the MCP-design-specific UX contracts: documentation sections, structured error shape, mistake detection, suggested recovery calls, and resource availability per server.

**Small fix plan:**

1. Add `tests/test_mcp_tool_contracts.py` to parse all `@mcp.tool` functions and validate doc sections.
2. Add `tests/test_mcp_error_contracts.py` for canonical error shapes and suggested recovery fields.
3. Add mistake-detection tests for malformed CCN/NPI/ZCTA/state/FIPS/dataset/workflow values.
4. Extend smoke tests to assert each server has standard discovery resources.
5. Add one MCP Inspector or protocol smoke fixture for a high-traffic server and the discovery server.

## Suggested Implementation Order

1. Ship the shared error contract and update the highest-traffic not-found and invalid-input paths.
2. Add the documentation linter in warning/allowlist mode, then convert key servers before making it strict.
3. Add shared normalization and suggestions for CCN, NPI, ZCTA, dataset ID, workflow ID, and server ID.
4. Fix web redirect validation and add SSRF regression tests.
5. Add per-server standard resources.
6. Split or capability-gate large clusters after docs/resources make the new surfaces easy to discover.
