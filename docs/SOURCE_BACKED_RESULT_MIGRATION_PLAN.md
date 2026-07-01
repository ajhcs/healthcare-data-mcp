# Source-Backed Result Migration Plan

This plan migrates Healthcare Data MCP to the source-backed result contract without breaking cross-server workflows or the live gateway provenance boundary.

The migration is intentionally sequenced. Parallel agents can work inside a wave, but each wave has an integration gate before the next wave starts.

## Migration Invariants

- Keep `shared.utils.source_backed_result` stable during server migration waves.
- Add traceability before moving large implementation modules.
- Keep compatibility mode for exploratory/local tool output.
- Require boundary traceability for report-ready facts and live-gateway-bound results.
- Preserve existing `identity_map.source_claims`, `evidence_path`, and `row_evidence_path` until the owning tool is migrated.
- Prefer plural `row_evidence_paths` in new or touched code.
- Do not merge identity and evidence; link them with source claim paths.

## Contract Gate

Every migrated report-ready output must pass:

```python
validate_source_claim_paths(payload, require_boundary_traceability=True)
```

The gate verifies:

- `identity_map` exists.
- `identity_map.source_claims[]` exists.
- each source claim has `evidence_path`.
- each source claim has `source_metadata_path`.
- row collections use `row_evidence_paths[]`.
- referenced evidence receipts exist and have boundary-quality content.
- referenced source metadata paths exist.

## Wave 0 - Runway

Status: started.

Owned files:

- `CONTEXT.md`
- `docs/SOURCE_BACKED_RESULT_CONTRACT.md`
- `shared/utils/source_backed_result.py`
- `tests/test_source_backed_result.py`

Purpose:

- Establish canonical language and validation helpers.
- Give future agents a stable seam.
- Avoid changing any server behavior yet.

Gate:

```bash
pytest tests/test_source_backed_result.py tests/test_mcp_response.py
```

## Wave 1 - Pilot Server

Recommended owner: one agent.

Server:

- `cms-facility`

Why:

- Compact result-shaping module.
- Participates in many workflows but is not itself live-gateway exposed.
- Already has `_cms_facility_identity_map`.
- Uses singular `row_evidence_path`, making it a good compatibility-to-contract example.

Scope:

- Update `_cms_facility_identity_map` to use `source_claim(...)`.
- Add `source_metadata_path`.
- Emit plural `row_evidence_paths`.
- Keep legacy `row_evidence_path` only if tests or existing consumers require it.
- Add boundary traceability tests for search and exact lookup outputs.

Gate:

```bash
pytest tests/test_source_backed_result.py tests/servers/cms_facility/test_server.py tests/test_workflows.py
```

Integration checks:

- `facility_profile`
- `quality_profile`
- `hospital_competitive_profile`
- `ownership_chow_trace`
- `system_reconciliation`

## Wave 2 - Workflow Spine

Recommended owner: one agent.

Modules:

- `shared/utils/workflows.py`
- `tests/test_workflows.py`
- `tests/test_distribution_artifacts.py`
- `tests/test_discovery_metadata.py`

Why:

- Workflows are the integration contract between servers.
- Report fact rows already contain `evidence_path`, `source_metadata_path`, and `identity_map_path`.
- This wave turns existing planner contracts into source-claim-path validation.

Scope:

- Add source claim path validation to workflow contract tests.
- Ensure report-ingest templates distinguish exploratory placeholders from report-ready facts.
- Verify every workflow report fact row points at an advertised evidence path and identity map path.

Gate:

```bash
pytest tests/test_workflows.py tests/test_distribution_artifacts.py tests/test_discovery_metadata.py tests/test_source_backed_result.py
```

## Wave 3 - Parallel Domain Server Groups

Agents can work in parallel if they own disjoint server groups and do not edit shared contract modules.

### Group A - Quality And Facility Facts

Servers:

- `hospital-quality`
- `service-area`
- `geo-demographics`
- `drive-time`

Shared workflows:

- `quality_profile`
- `quality_measure_lookup`
- `facility_profile`
- `market_community_health_scan`

Gate:

```bash
pytest tests/servers/hospital_quality/test_server.py \
  tests/servers/service_area/test_server.py \
  tests/servers/geo_demographics/test_server.py \
  tests/servers/drive_time/test_drive_time_server.py \
  tests/test_workflows.py
```

### Group B - Ownership, Compliance, And Public Records

Servers:

- `provider-enrollment`
- `public-records`

Shared workflows:

- `compliance_exclusion_screening`
- `ownership_chow_trace`
- `system_reconciliation`
- `profile_evidence_pack`

Gate:

```bash
pytest tests/servers/provider_enrollment/test_server.py \
  tests/servers/public_records/test_leie_server.py \
  tests/servers/public_records/test_sam_exclusions_server.py \
  tests/servers/public_records/test_regulatory_records.py \
  tests/servers/public_records/test_cyber_enrichment.py \
  tests/test_workflows.py
```

### Group C - Financial, Claims, Workforce

Servers:

- `financial-intelligence`
- `claims-analytics`
- `workforce-analytics`

Shared workflows:

- `finance_profile`
- `hospital_competitive_profile`
- `referral_leakage_readiness`

Gate:

```bash
pytest tests/servers/financial_intelligence/test_server.py \
  tests/servers/claims_analytics/test_server.py \
  tests/servers/workforce_analytics/test_workforce_data.py \
  tests/test_workflows.py
```

### Group D - Health System, Web, Research

Servers:

- `health-system-profiler`
- `web-intelligence`
- `research-trials`
- `physician-referral-network`

Shared workflows:

- `system_reconciliation`
- `profile_evidence_pack`
- `health_system_metrics`
- `research_trials_activity_profile`
- `referral_leakage_readiness`

Gate:

```bash
pytest tests/servers/health_system_profiler/test_server.py \
  tests/servers/health_system_profiler/test_profile_evidence_pack.py \
  tests/servers/health_system_profiler/test_system_metrics.py \
  tests/servers/web_intelligence/test_search_client.py \
  tests/servers/research_trials/test_server.py \
  tests/servers/physician_referral_network/test_server.py \
  tests/test_workflows.py
```

## Wave 4 - Live Gateway Boundary

Recommended owner: one agent.

Modules:

- `servers/live_gateway/server.py`
- `tests/servers/live_gateway/test_server.py`
- `shared/utils/source_backed_result.py` only if the previous waves found a missing generic validator need.

Why:

- The live gateway provenance boundary is the strictest external seam.
- It should enforce source claim path traceability after enough live-exposed tools have migrated.

Scope:

- Integrate `validate_source_claim_paths(..., require_boundary_traceability=True)` into live gateway provenance checks.
- Keep evidence receipt validation.
- Preserve existing rate limit, scope, sensitive-identifier, result-size, and audit behavior.
- Add audit fields for source-claim-path status.

Gate:

```bash
pytest tests/servers/live_gateway/test_server.py tests/test_gateway_http_integration.py tests/test_source_backed_result.py
```

Live-exposed servers to confirm:

- `hospital-quality`
- `financial-intelligence`
- `workforce-analytics`
- `claims-analytics`
- `public-records`
- `provider-enrollment`
- `community-health`
- `research-trials`

## Wave 5 - Public Source Catalog And Large Module Deepening

Start only after traceability gates are reliable.

Targets:

- Public source catalog module.
- Health system profiling module.
- Public records implementation split.
- Live gateway policy runner module.
- Tabular source normalization module.

Rule:

Deepen modules behind the now-stable source-backed result contract. Do not create new result shapes while moving implementation code.

## Cross-Server Regression Suite

Run after each completed wave:

```bash
pytest tests/test_source_backed_result.py \
  tests/test_mcp_response.py \
  tests/test_workflows.py \
  tests/test_mcp_resources_and_observability.py \
  tests/servers/test_smoke_servers.py
```

Run before merging the whole migration:

```bash
pytest
```

## Parallel Agent Rules

- One agent may own one server group in Wave 3.
- No two agents edit `shared/utils/source_backed_result.py` at the same time.
- No two agents edit `shared/utils/workflows.py` at the same time.
- Each agent adds source claim path tests for its owned server group.
- Each agent runs its server gate and the cross-server regression suite.
- If a server participates in a shared workflow with another group, the later-finishing agent owns the workflow-level integration fix.
- If a migration needs a new domain term, update `CONTEXT.md` before changing code.
