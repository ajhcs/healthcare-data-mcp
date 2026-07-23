# Health System Cache Management MCP Plan

Status: initial implementation plan after repo archaeology, external source research, and specialist review.

## Executive Summary

Healthcare Data MCP is already a real multi-server MCP project: it has 19 registered servers, structured responses, workflow plans, discovery/gateway surfaces, live-gateway policy, client packaging, and a passing test baseline. The next phase should not add another domain server. The next phase should add an intelligent cache-management MCP control plane so an LLM can safely inspect, plan, acquire, refresh, validate, and use the public data caches required to answer national health-system questions.

Target behavior:

1. An agent asks for a national health-system or hospital competitive workflow.
2. The MCP server identifies every required public source, cache artifact, environment key, import requirement, validation gate, and source caveat.
3. The agent can request a dry-run refresh plan before any write happens.
4. Approved local cache tools can acquire or refresh registered public sources only, write atomically, validate content, and promote only passing artifacts.
5. Workflow tools refuse or degrade explicitly when caches are missing, stale, corrupt, state-limited, manually licensed, or unsupported.
6. Final report rows preserve evidence, source metadata, identity maps, source periods, cache freshness, and caveats.

This plan addresses the five current comments:

1. Repair and reconcile Beads tracking.
2. Build workflow readiness around intelligent cache management.
3. Add MCP/agent evaluations.
4. Make one flagship workflow excellent.
5. Finish distribution/client validation.

## Grounding

### Current Repo Reality

- The README promises 19 local MCP servers for public healthcare market intelligence, plus structured responses, source metadata, recovery hints, per-server resources, discovery, gateway, live-gateway, workflows, presets, and packaging.
- The CLI reports all 19 servers.
- The test baseline is clean: 676 tests passed.
- Protocol smoke passed for discovery workflow planning and gateway search.
- Doctor runs, but reports many missing cache artifacts.
- Only facility_profile, research_trials_activity_profile, and referral_leakage_readiness are cache-ready on the current machine.
- Core workflows such as hospital_competitive_profile, quality_measure_lookup, quality_profile, ownership_chow_trace, compliance_exclusion_screening, finance_profile, market_community_health_scan, and system_reconciliation are blocked by missing or incomplete source caches.
- Beads exists on disk but normal issue listing fails with an embedded Dolt database-name error. Backup JSONL files contain recoverable issue history.

### External Source Facts

Public healthcare data sources have different access and freshness models:

- CMS Provider Data Catalog exposes row data through dataset-specific API and download endpoints under data.cms.gov/provider-data/api/1/datastore/query.
- CMS data.json exposes current catalog metadata and dataset landing pages; it should be used to avoid hard-coded date and UUID download URLs where possible.
- CMS national sources relevant to health-system intelligence include Hospital General Information, Provider of Services, Hospital Service Area, quality files, cost reports, inpatient/outpatient PUFs, PECOS provider enrollment, hospital owners, and CHOW files.
- NPPES API supports organization search for NPI-2 records, but NPPES is an identity and provider-record source, not proof of ownership, affiliation, or referral relationships.
- CDC PLACES data is available through Socrata-style public datasets and is geography/population context, not facility performance.
- NIH RePORTER and ClinicalTrials.gov expose public research/trial records, but sponsor and site names are aliases unless exact identifiers support joins.
- AHRQ Compendium is the closest public national health-system affiliation starting point, but local cache row counts and source period must be validated before treating it as national-ready.

### Specialist Review Themes

Clinical data guidance:

- National readiness must be a source-governance contract, not a file-exists check.
- Every source-backed fact needs source period, provenance, identity boundary, caveat, and validation status.
- Exact CMS quality measure rows should win over adjacent summaries.

Interoperability guidance:

- Add a first-class cache MCP surface instead of relying on side effects in setup scripts or first tool calls.
- Promote dataset metadata into executable cache contracts.
- Keep mutating cache operations explicit and separate from read-only planning.

Compliance guidance:

- Cache orchestration must be registry-backed, not arbitrary file/network fetch.
- Remote clients should inspect and plan by default; refresh/promote requires a separate scope and local-safe deployment posture.
- Cache actions need non-secret audit events and rollback evidence.

Data engineering guidance:

- Use idempotent cache runs with staging, locks, checksums, validation, atomic promotion, and previous-good rollback.
- Consider a local bronze/silver/gold layout while preserving existing cache paths for compatibility.

Tool evaluation guidance:

- Evaluations must grade whether agents check readiness, refresh only needed sources, preserve evidence, refuse unsupported substitutions, and avoid turning missing data into negative factual claims.

## Architecture Decision

Add a new MCP server: cache-manager.

Rationale:

- discovery is currently metadata-first and read-only. It should remain safe for broad client use.
- Cache acquisition and refresh write to disk and may use network/disk budgets. A separate server lets local operators enable write-capable cache management without expanding the remote metadata gateway.
- The new server can reuse the canonical registry, discovery catalog, workflow definitions, source catalog, cache utilities, and doctor checks.

Read-only cache-readiness helpers can be exposed through discovery where useful, but mutating tools should live in cache-manager.

## Proposed MCP Surface

### Read-Only Tools

list_cache_sources:

- Lists registered dataset/cache sources.
- Filters by server, workflow, preset, profile, status, source system, or acquisition mode.
- Returns bounded summaries with dataset ID, title, owning server, source authority, cache status, validation status, freshness, and next action.

inspect_cache_source:

- Returns the full cache contract for one dataset.
- Includes expected artifacts, current artifacts, manifests, source URLs, source period semantics, TTL, validation checks, source caveats, owning tools/workflows, and report eligibility.

get_workflow_cache_readiness:

- Given workflow_id and optional workflow inputs, resolves required sources and source aliases into canonical dataset IDs.
- Reports per-step and per-dataset readiness as ready, missing, stale, invalid, partial, pattern, manual_import_required, env_required, unsupported, or optional_unavailable.
- Returns exact next tool calls or CLI commands an agent can use.

plan_cache_refresh:

- Dry-run by default.
- Produces an ordered acquisition/refresh plan with dependencies, disk estimate where known, network/source caveats, required environment variables, expected artifacts, and validation gates.
- Does not download or write.

get_cache_manifest:

- Returns one cache artifact manifest or the latest promoted run manifest.
- Must be safe and bounded; it should not return raw large source data.

get_cache_lineage:

- Shows how a source artifact moved from acquisition to normalized cache to workflow-ready use.
- Includes run IDs, source metadata, validation outcomes, promoted artifact paths, and downstream workflow/tool references.

### Mutating Local Tools

All mutating tools must reject unknown dataset IDs, arbitrary URLs, absolute paths outside the configured cache root, unsafe redirects, private/link-local targets, oversized downloads, unsupported file types, and broad deletes.

start_cache_refresh:

- Starts a bounded refresh job for one registered dataset ID or a small explicit dataset list.
- Accepts dry_run, force, max_bytes, and allow_stale_fallback.
- Returns a job ID, not raw data.

get_cache_job:

- Polls refresh/import/validation job status.
- Returns progress, current phase, bytes read, records parsed when available, warnings, validation summary, and next action.

validate_cache_source:

- Runs source-specific validators against current or staged artifacts.
- Does not promote by itself unless explicitly called as part of a refresh pipeline with promotion enabled.

promote_cache_artifact:

- Atomically promotes a staged artifact only after validation passes.
- Records previous promoted artifact so rollback is possible.

quarantine_cache_artifact:

- Marks an artifact invalid or quarantined without deleting broad directories.
- Used for corrupt, truncated, or wrong-schema caches.

rollback_cache_artifact:

- Restores the previous promoted artifact for one dataset when available.

## Cache Contract Model

Create a shared package, likely shared/cache_manager, with these core types.

CacheDatasetSpec:

- dataset_id
- title
- source_system
- source_authority
- landing_page
- resolver: CMS data.json, CMS metastore, stable URL, Socrata API, NPPES API, NIH API, ClinicalTrials API, manual import, licensed import, local fixture
- acquisition_mode: catalog_resolved_download, stable_download, paged_api, live_api_cache, manual_import, licensed_import, pattern
- owning_servers
- owning_tools
- workflow_roles
- source_period_semantics
- ttl_days
- expected_artifacts
- required_env
- optional_env
- expected_grain
- primary_keys
- join_keys
- required_columns
- recommended_indexes
- min_row_count
- expected_state_coverage
- validation_profile
- source_caveat
- missing_data_policy
- report_eligibility_rules

CacheArtifactManifest:

- dataset_id
- artifact_id
- run_id
- artifact_role: bronze, silver, gold, compatibility
- path
- source_url
- landing_page
- retrieved_at
- source_modified
- etag
- last_modified
- checksum_sha256
- content_length
- row_count
- schema_fingerprint
- source_period
- cache_status
- validation_status
- validator_version
- loader_version
- promoted_at
- previous_artifact_id
- caveat
- next_step

CacheValidationResult:

- status: pass, warn, fail
- defects: machine-readable list with severity, field, expected, observed, recovery hint
- metrics: row count, distinct CCNs, distinct NPIs, distinct states, duplicate keys, null rates, schema hash, source period
- report_eligible: boolean

CacheRun:

- run_id
- dataset_id
- requested_by
- request_source: CLI, MCP stdio, MCP HTTP, setup wizard, CI
- started_at
- completed_at
- status
- phase
- dry_run
- force
- input_manifest
- output_manifests
- audit_event_ids
- error
- recovery_hint

## Storage Layout

Keep existing paths working, but introduce a structured cache layout:

- manifests/datasets/{dataset_id}.json
- manifests/artifacts/{artifact_id}.json
- manifests/runs/{run_id}.json
- bronze/{dataset_id}/{run_id}/...
- silver/{dataset_id}/current.parquet
- gold/{workflow_id}/{dataset_id}/...
- legacy compatibility paths

Existing server loaders can keep reading legacy paths during migration. New cache-manager refreshes should write manifests and promote compatibility copies or aliases for legacy paths until all servers read manifests directly.

## National Health-System Dataset Bundle

The cache manager must be able to reason about all health systems in the United States. That does not mean every data source has national system-level facts. It means the MCP readiness layer must know which sources are national, which are state-limited, which are manual or licensed, and which are only adjacent context.

Core national identity sources:

- AHRQ Compendium system file and hospital linkage.
- CMS Hospital General Information.
- CMS Provider of Services.
- CMS PECOS hospital enrollments.
- CMS PECOS hospital owners and CHOW.
- NPPES organization search/cache for NPI-2 identity enrichment.

National performance and operations sources:

- CMS hospital quality files: ratings, HRRP, HAC, HCAHPS, complications/deaths, HAI, unplanned visits, exact measure rows.
- CMS Hospital Cost Report / HCRIS.
- CMS inpatient and outpatient hospital PUFs.
- CMS Hospital Service Area File.
- Medicare Geographic Variation.

National market and community context:

- Census ACS.
- HUD ZIP crosswalk.
- CDC PLACES.
- Dartmouth HSA/HRR crosswalk.
- Routing backends and local facility/geography caches.

Public records and compliance sources:

- HHS OIG LEIE.
- SAM.gov Exclusions where API key is configured.
- USAspending and SAM opportunities.
- OCR breach portal and other public cyber/breach imports where available.
- CMS accreditation and Promoting Interoperability files.

Research and web context:

- NIH RePORTER.
- ClinicalTrials.gov.
- Public web search/fetch, GPO directory, EHR vendor detection, executive/news OSINT.

State-limited supplementary sources:

- PA, NJ, DE, PHC4, and other state artifacts are useful but must be marked as state-specific supplements. They must never be presented as all-US coverage.

## Workstream 1: Repair and Reconcile Beads

Goal: make project tracking reliable before converting this plan into tasks.

Tasks:

1. Inspect .beads backup config, issues, dependencies, and Dolt state.
2. Fix or recreate the Beads config so embedded Dolt has a non-empty database name, expected issue prefix HDM, and repo-local context.
3. Recover or import backup issues if direct config repair is not enough.
4. Verify status, list, ready, stats, and export commands.
5. Reconcile stale issues against current reality.
6. Create new epics for cache-manager, manifest/validation framework, national health-system readiness bundle, flagship hospital_competitive_profile, MCP/agent evaluations, and distribution/client validation.

Dependencies:

- Blocks implementation tracking and bead conversion.
- Does not block continued plan refinement.

Acceptance:

- Beads list succeeds from repo root.
- Prior HDM issue history is visible or archived with a documented recovery reason.
- New plan epics exist with dependency edges.

## Workstream 2: Intelligent Cache Management MCP Server

Goal: give an LLM safe, explicit tools to manage public healthcare caches.

Phase 2.1: registry and spec unification.

Tasks:

1. Build shared/cache_manager/specs.py.
2. Generate CacheDatasetSpec objects from server registry, discovery dataset catalog, source catalog, workflow source aliases, and setup/cache runbook metadata.
3. Add missing fields for expected artifacts, TTL, validation rules, source period, coverage, and source caveats.
4. Keep a compatibility adapter for existing doctor and discovery cache status structures.

Rationale:

The repo currently has cache definitions split across loader modules, doctor, setup wizard, discovery docs, source catalog, and server registry. Intelligent agents need one contract.

Phase 2.2: manifest and validation framework.

Tasks:

1. Implement CacheArtifactManifest, CacheRun, and CacheValidationResult.
2. Add atomic JSON writers for manifests and run records.
3. Implement validators for common file checks, schema checks, identity checks, coverage checks, source checks, and report-eligibility checks.
4. Start with the national flagship bundle: CMS hospital general info, CMS provider of services, AHRQ compendium, CMS hospital quality, CMS cost report, CMS claims PUFs, CMS HSAF, CMS PECOS datasets, HHS OIG LEIE, and CDC PLACES.

Rationale:

File presence is not enough. The agent needs to know whether the local artifact is current, valid, national enough, and usable for report-backed claims.

Phase 2.3: idempotent acquisition pipeline.

Tasks:

1. Implement per-dataset locks.
2. Download or import into staging directories.
3. Use content hashes and HTTP validators when available.
4. Normalize into Parquet or DuckDB-friendly silver artifacts.
5. Run validation before promotion.
6. Atomically promote to current artifacts.
7. Preserve previous-good artifacts for rollback.
8. Clean stale staging directories safely.
9. Add disk and network guardrails.

Rationale:

Healthcare public files are large and source URLs change. Interrupted or partially successful refreshes must not poison downstream workflow facts.

Phase 2.4: MCP server.

Tasks:

1. Add servers/cache_manager/server.py.
2. Register cache-manager in the server registry, CLI launcher, Docker compose rendering, client config rendering, and docs.
3. Implement read-only tools first.
4. Implement mutating tools with local-safe policy and dry-run defaults.
5. Add standard resources for capabilities, datasets, workflows, policy, and metrics.

Rationale:

Separating cache-manager from discovery keeps metadata safe while still making local agent-managed cache workflows possible.

Phase 2.5: workflow integration.

Tasks:

1. Extend workflow plans with cache_readiness.
2. Resolve workflow aliases into canonical datasets.
3. Make readiness per required artifact, not per dataset family only.
4. Add next actions to workflow plans.
5. Block report-ready fact rows when required evidence is missing, stale, invalid, or placeholder-only.

## Workstream 3: MCP and Agent Evaluations

Goal: prove real agents can manage caches and use workflow data correctly.

Tasks:

1. Add tests/evals or scripts/evals with scenario definitions.
2. Add deterministic fixture cache roots for empty, ready, stale, corrupt, wrong-schema, partial multi-file, manual import, and state-limited cases.
3. Build an evaluation runner that can start MCP stdio servers, run scripted tool-call traces, optionally run model-in-the-loop traces later, and record transcript, structured content, score, and remediation hint.
4. Add healthcare reasoning assertions: exact identifiers before joins, no name/address-only fact merges, no missing-cache-as-zero interpretation, no adjacent-source substitution for exact measures, source period and caveat preservation, and no PHI/sensitive tax identifier handling.
5. Add eval scenarios for cache preflight, targeted stale refresh, corrupt cache recovery, missing env/API key, exact CLABSI lookup, state-limited source refusal, and final report ingest validation.

Acceptance:

- Deterministic no-network evals pass in CI.
- Optional scheduled live-source smoke can run separately.
- Critical safety assertions are 100 percent.
- Provenance preservation for cited facts is 100 percent.

## Workstream 4: Flagship Workflow

Goal: make hospital_competitive_profile the proof that the project delivers.

Target scenario:

- Public hospital CCN 390223 as the default Jefferson/TJUH test case unless replaced by a better stable fixture.
- Required explicit quality measure input for exact-measure steps, such as CLABSI SIR mapped to the CMS measure row.

Minimum workflow:

1. Resolve facility identity by exact CCN.
2. Preserve canonical CCN, facility name, address, state, ZIP, and NPI where available.
3. Resolve system affiliation only with exact AHRQ/system identifiers or reviewed reconciliation. Marketing names and web pages remain aliases.
4. Pull CMS quality summary and exact measure rows.
5. Pull finance/public cost-report context with metric-level evidence.
6. Pull workforce/operations context with source period and denominator caveats.
7. Pull claims/service-line market context if claims-analytics remains in the preset. If claims is not included, remove it from the preset and docs.
8. Optionally add service-area/community context with clear source boundaries.
9. Emit report fact rows that pass strict report-ingest validation.

Important current gap:

The current preset recommends claims-analytics for competitive profile, but the workflow needs to be checked for a real claims step and report rows. Either add claims/service-line steps or narrow the workflow/preset.

Acceptance:

- The workflow plan for hospital_competitive_profile with CCN 390223 and measure clabsi_sir includes all expected steps and cache readiness.
- Missing caches block or degrade explicitly.
- Exact quality measure facts come from exact CMS rows, not adjacent summaries.
- Final report rows include evidence/source/identity paths and no placeholders.

## Workstream 5: Distribution and Client Validation

Goal: prove a fresh operator or MCP client can install, inspect readiness, and run the flagship path without hidden local assumptions.

Tasks:

1. Update registry-rendered configs for cache-manager.
2. Update Docker compose renderers.
3. Update project MCP config, Codex examples, Claude Desktop examples, generic HTTP client configs, and MCPB manifest generation.
4. Update installer, setup scripts, Codex registration, and setup wizard to expose cache-manager and cache readiness.
5. Add clean install checks for editable install, clean virtualenv, pipx or uvx once packaging supports it, Docker zero-config, MCPB skeleton, stdio, Streamable HTTP local, discovery/gateway metadata, and live-gateway allowlist where applicable.
6. Ensure remote metadata gateway stays read-only.
7. Ensure cache mutating tools are unavailable remotely unless explicitly deployed as local-safe authenticated cache-manager with a refresh scope.

Acceptance:

- MCP smoke covers cache-manager read-only tools.
- MCP Inspector smoke covers discovery, gateway, live-gateway, cache-manager, and flagship readiness.
- Generated configs pass check mode.
- Docker and MCPB checks pass.
- Remote gateway metadata does not expose write-capable cache operations.

## Security and Compliance Controls

Cache-manager must enforce:

- Registered datasets only.
- Registered source URLs and resolvers only.
- No arbitrary caller URLs.
- No arbitrary filesystem paths.
- Cache root confinement.
- Private, loopback, link-local, metadata service, and non-http(s) URL rejection.
- Redirect revalidation.
- File extension and content-type allowlists.
- Download byte limits.
- Disk free-space guard.
- Per-dataset locks.
- Atomic promotion.
- Previous-good rollback.
- Non-secret audit events.
- Token fingerprint only, never token values.
- No raw payloads or evidence bodies in audit logs.
- Read-only inspection by default.
- Separate cache refresh scope for mutating HTTP/SSE use.
- No PHI positioning: this project handles public aggregate/admin data only.

## Dependency Graph

- Beads repair blocks Beads epics/tasks.
- Cache spec unification blocks manifest schema, validators, acquisition pipeline, cache-manager tools, and workflow cache readiness.
- Workflow cache readiness blocks the flagship workflow, MCP/agent eval scenarios, and distribution/client validation.
- Security policy blocks mutating cache tools, HTTP/SSE deployment validation, and live/remote gateway boundaries.

## Implementation Order

1. Repair Beads.
2. Create cache-manager epic/task graph.
3. Implement cache spec model and manifest schema.
4. Add validators for national flagship datasets.
5. Add read-only cache-manager MCP tools.
6. Wire workflow cache readiness.
7. Add idempotent refresh/promote pipeline.
8. Add mutating cache-manager tools with local-safe policy.
9. Make hospital_competitive_profile executable and report-ready.
10. Add deterministic MCP/agent evals.
11. Update distribution and client validation.
12. Run full test, smoke, packaging, and eval gates.

## Initial Bead Epics To Create After Repair

Epic: Repair Beads workspace.

- Diagnose .beads config and embedded Dolt database-name failure.
- Restore/import backup issue history.
- Reconcile stale issues.
- Add new epics and dependency edges.

Epic: Cache manager core.

- Dataset spec model.
- Manifest/run schemas.
- Validators.
- Acquisition pipeline.
- Audit events.

Epic: Cache-manager MCP server.

- Server registration.
- Read-only tools.
- Mutating local tools.
- Resources.
- Policy tests.

Epic: National health-system cache bundle.

- AHRQ.
- CMS HGI/POS.
- PECOS enrollment/ownership/CHOW.
- CMS quality.
- CMS cost report/HCRIS.
- CMS claims/HSAF.
- CDC/Census geography.
- LEIE/SAM where available.

Epic: Flagship competitive profile.

- Cache readiness integration.
- Claims/service-line contract.
- Exact quality measure contract.
- Report fact-row validation.
- Jefferson/TJUH fixture scenario.

Epic: MCP/agent evaluations.

- Scenario schema.
- Fixture cache roots.
- Transcript/scoring runner.
- Safety/provenance assertions.

Epic: Distribution validation.

- Config rendering.
- Docker.
- MCPB.
- Installer/setup.
- Stdio and Streamable HTTP smoke.
- Gateway/live-gateway boundaries.

## Definition of Done

The project is ready for the next release when:

- Beads works and contains this plan as task epics.
- cache-manager is registered and discoverable.
- An agent can call read-only cache tools to understand exactly what is missing for hospital_competitive_profile.
- A local authorized agent can refresh/validate required public caches without arbitrary file/network access.
- Cache manifests prove source period, checksum, row count, schema, and validation status.
- hospital_competitive_profile is executable against deterministic fixtures and a local ready cache.
- Final report rows preserve evidence, source metadata, identity, identity map, and caveats.
- CI includes deterministic evals for cache readiness and agent behavior.
- Distribution artifacts expose the same cache-readiness semantics.
