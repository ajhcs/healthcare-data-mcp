# Source-Backed Result Post-Migration Plan

This plan extends the completed Wave 1-5 source-backed result migration. The
first migration made report-ready and live-gateway-bound facts traceable. These
post-migration waves keep that boundary reliable as agents add tools, caches,
and downstream integrations.

Parallel agents may work within one wave when they own disjoint files. Do not
skip a wave's gate before starting dependent work.

## Invariants

- Preserve the source-backed result contract documented in
  `docs/SOURCE_BACKED_RESULT_CONTRACT.md`.
- Keep `docs/SOURCE_CAPABILITY_LEDGER.md` discoverable from metadata resources.
- Treat the live gateway provenance boundary as stricter than local exploratory
  tool output.
- Prefer small shared seams, tests, and registry-backed metadata over parallel
  markdown ledgers.
- If a tool is not fully integrated with downstream persistence, expose a
  traceable link to the source capability, source metadata, identity policy, and
  evidence receipts.

## Wave 6 - Source Capability Ledger Integration

Purpose:

- Make the source capability ledger discoverable from every MCP server's
  standard metadata resources.
- Give coding agents a stable link from capabilities to the source-backed
  contract, source ledger, datasets, and identity rules.

Gate:

```bash
pytest tests/test_mcp_resources_and_observability.py tests/test_distribution_artifacts.py
```

## Wave 7 - Workflow Fact Manifest Hardening

Purpose:

- Add a shared manifest for workflow fact rows that names value paths, evidence
  paths, source metadata paths, identity paths, and owning workflow steps.
- Validate all workflow report-ingest templates against the manifest.
- Keep workflow interactions safe when multiple servers pass facts to one
  another.

Gate:

```bash
pytest tests/test_workflows.py tests/test_distribution_artifacts.py tests/test_source_backed_result.py
```

## Wave 8 - Server Source-Status Normalization

Purpose:

- Normalize cache/source status payloads used by live and metadata servers.
- Ensure source period, cache freshness, source URL, retrieval method, and
  caveat fields are present or explicitly unavailable.
- Preserve existing tool response shapes while adding shared helpers behind
  them.

Gate:

```bash
pytest tests/test_doctor.py tests/test_discovery_metadata.py tests/servers/test_smoke_servers.py
```

## Wave 9 - Identity Conflict And Review Routing

Purpose:

- Make cross-server identity conflicts explicit when exact identifiers disagree
  or candidate context is insufficient.
- Add review-routing fields that downstream agents can use without making
  unsupported joins.
- Cover workflows that combine facility, system, enrollment, financial,
  public-record, and web context.

Gate:

```bash
pytest tests/test_workflows.py tests/servers/health_system_profiler/test_generic_reconciliation.py tests/servers/provider_enrollment/test_server.py tests/servers/public_records/test_regulatory_records.py
```

## Wave 10 - Live Gateway Audit Evidence Export

Purpose:

- Add a compact, non-secret audit evidence export for live-gateway policy
  decisions.
- Include source-claim validation status, blocked or degraded provenance
  reasons, requested tool scope, and trace IDs.
- Keep auth, rate-limit, and sensitive-identifier behavior unchanged.

Gate:

```bash
pytest tests/servers/live_gateway/test_server.py tests/test_gateway_http_integration.py tests/test_source_backed_result.py
```

## Wave 11 - Agent Evaluation Scenarios

Purpose:

- Add deterministic eval scenarios for common source-substitution failures,
  missing cache/source-status recovery, workflow handoffs, and live-gateway
  provenance refusals.
- Make failures actionable for coding agents with expected remediation hints.

Gate:

```bash
pytest tests/test_cache_manager_evals.py tests/test_workflows.py tests/test_mcp_response.py
```

## Wave 12 - Operational Rollout And Regression Closure

Purpose:

- Update operator-facing docs, generated artifacts, doctor output, and release
  notes so the post-migration contract is discoverable.
- Run the full cross-server and packaging regression suite.
- Close the post-migration epic only when every Wave 6-12 child is complete.

Gate:

```bash
pytest
```

## Cross-Wave Regression Gate

Run after each completed wave:

```bash
pytest tests/test_source_backed_result.py \
  tests/test_mcp_response.py \
  tests/test_workflows.py \
  tests/test_mcp_resources_and_observability.py \
  tests/servers/test_smoke_servers.py
```
