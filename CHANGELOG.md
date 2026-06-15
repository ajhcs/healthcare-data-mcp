# Changelog

## v0.4.0 - Source-disciplined health-system metrics

Released: 2026-06-15

- Added health-system-profiler.list_health_system_metrics and
  health-system-profiler.get_health_system_metrics using the AHRQ Compendium
  2023 health-system universe as the canonical public snapshot.
- Added explicit source-vintage metadata, data modes, deterministic snapshot
  IDs, cursor pagination, state-filter caveats, and coverage summaries.
- Returns AHRQ hospital counts, nonfederal general acute hospital counts,
  linked hospital rows, system and hospital bed counts, and AHRQ Compendium
  physician counts without silently replacing them with current CMS overlays.
- Adds latest_public_overlay candidates for CMS HGI/POS/HCRIS/state values,
  hospital address/type conflict reporting, CCN identity caveats, and optional
  Medicare public clinician roster NPI-deduped estimates.
- Hardened nullable AHRQ parsing, legacy profiler compatibility, ambiguous
  system-name resolution, cursor filter validation, and snapshot ID hashing.
- Added QA artifacts and regression coverage for source-vintage discipline,
  row-count reconciliation, nullable metrics, cursor mismatches, duplicate
  names, leading-zero identifiers, and clinician roster deduplication.

## v0.3.1 - Profile evidence pack workflow

Released: 2026-05-30

- Added health-system-profiler.build_profile_evidence_pack, a read-only MCP
  workflow output for Healthcare Toolkit health-system profile population.
- Added source-backed profile candidates for system identity, aliases, hospital
  rosters, CCNs, source-local identifiers, addresses, county/GEOID,
  coordinates, facility bed counts, system bed rollup guidance, affiliations,
  official count claims, conflicts, and unavailable-public findings.
- Fixed workflow cache preflight planning so
  cache-manager.get_workflow_cache_readiness receives concrete workflow_id and
  inputs arguments.
- Moved PECOS/HCRIS/state/official enrichment after roster resolution so
  state + system_name calls use discovered CCNs for source lookups.
- Wired HCRIS/cost-report, PA state bed, and reviewed official-evidence cache
  loaders instead of advertising stubbed source families.
- Added evidence-pack documentation and regression tests for JSON contract,
  cache readiness, PA fixtures, bed/geography/affiliation resolution, and
  exact-vs-vague official count handling.

## v0.3.0 - MCP design hardening

Released: 2026-05-30

- Added standardized MCP error payloads with machine-readable error types,
  recoverability flags, fix hints, available options, and suggested follow-up
  tool calls.
- Added an enforced MCP tool documentation contract covering discovery,
  when-to-use guidance, parameters, returns, do/don't rules, examples, and
  common mistakes for every checked-in tool.
- Added per-server metadata resources for capabilities, datasets, examples,
  identity rules, and local non-secret metrics.
- Added capability clusters for broad servers so agents can choose smaller,
  task-aligned tool groups.
- Added shared input normalization and mistake detection for exact public
  identifiers such as CCN, NPI, state, ZCTA, FIPS, and catalog IDs.
- Hardened public web fetching against redirect-chain SSRF, private/metadata
  hosts, excessive redirects, and oversized HTML responses.
- Added read-only discovery macro tools for quality measure lookup, compliance
  exclusion screening, and facility profile readiness.
- Migrated runtime public-source cache writes to atomic replacement helpers for
  bytes, JSON, CSV, and Parquet artifacts.
- Added shared tool-call observability for local/HTTP MCP servers with
  non-secret duration, outcome, result-size, dataset, and cache-status metrics.
- Added MCP UX regression tests for doc contracts, resources, structured
  errors, mistake recovery, observability, cache writes, and web SSRF behavior.

## v0.2.0 - Operator-ready productization

Released: 2026-05-22

- Added `hc-mcp doctor` as a single read-only readiness command covering package/version, Python environment, server importability, port conflicts, client config hints, API-key posture, cache readiness, source freshness, registry artifact drift, evidence contract coverage, workflow validation, live-gateway policy checks, and distribution readiness.
- Added a canonical typed server/capability registry that drives or validates the launcher, Compose files, installer/setup surfaces, Codex/Claude/client configs, MCPB/Desktop Extension choices, gateway/discovery metadata, docs tables, and release gates.
- Standardized evidence/provenance receipts across the major healthcare and workflow surfaces, including source metadata, cache/freshness status, match basis, confidence, caveats, missing-data next steps, and shared validation helpers.
- Added executable task-first workflows and curated presets for compliance screening, facility/system reconciliation, ownership/CHOW tracing, hospital competitive profiles, market/community scans, quality lookup, research activity, referral/leakage readiness, finance, and metadata-only usage.
- Added a shared healthcare identity map foundation for CCN, NPI, PECOS enrollment, AHRQ system IDs, ownership/source aliases, match decisions, conflicts, and unresolved identifiers, with cross-server workflow smoke coverage.
- Hardened metadata gateway and live-gateway separation with explicit live tool policy metadata, scopes, size bounds, rate-limit classes, audit event shape, source caveat classes, auth guards, and unsafe exposure tests.
- Improved distribution and onboarding with versioned install guidance, package/container metadata checks, localhost-bound Compose output, dry-run setup/register scripts, MCP Inspector smoke, MCPB skeleton validation, and registry-rendered client artifacts.
- Added product/security CI gates for secret scanning, dependency audit, installer dry runs, registry-rendered artifact checks, doctor readiness, workflow/preset smoke, MCP protocol smoke, Docker zero-config startup, gateway auth integration, and Python distribution checks.

- Added public healthcare data cache acquisition and status coverage for PHC4,
  AHRQ HFMD, PA hospital reports, NJ hospital public data, DE discharge data,
  and public cyber/breach sources.
- Added PHC4 public report indexing and extracted-table provenance for Hospital
  Performance, Financial Analysis, Common Procedures, and special public
  reports without using paid PHC4 datasets.
- Added normalized PA/NJ/DE public state-health artifact models and query
  helpers that feed public financial health, staffing, productivity, and
  throughput profiles with confidence and source metadata.
- Added high-confidence public financial health/community-benefit outputs from
  HCRIS, IRS 990 Schedule H, and AHRQ HFMD while explicitly excluding HFMA MAP
  revenue-cycle KPI derivations and HCRIS days-in-A/R proxies.
- Added public staffing, productivity, throughput, ED volume, and procedure
  volume tools with provider-year filtering, peer-group metadata, source
  ranking, and PA admissions enhancement where public rows are available.
- Added cyber incident enrichment from OCR enforcement actions, SEC disclosures,
  searchable state breach notices, and CISA KEV vulnerability context with
  explicit incident confidence fields.
- Expanded regression coverage for new public caches, PHC4/report provenance,
  financial metric confidence, MAP KPI exclusion, cyber enrichment, and
  state-health normalization.

## v0.1.2 - Gateway and discovery hardening

- Generalized health-system facility reconciliation beyond Jefferson by adding
  generic AHRQ/CMS facility ledgers for non-curated systems while preserving the
  Jefferson/LVHN merger resolver.
- Fixed stale gateway dataset metadata so advertised tools now match real MCP
  tools, and added the April 2026 service-area, provider-enrollment,
  community-health, and research-trials servers to remote metadata discovery.
- Added tool-callable discovery helpers for dataset catalog, schema, source,
  cache status, and runbook access for clients that do not reliably consume MCP
  resources.
- Added generic `search_web` and `fetch_web_page` tools to `web-intelligence`,
  with bounded outputs and explicit public-web caveats.
- Added `live-gateway` on port `8020` as a separate authenticated router for
  approved live provider, quality, claims, exclusion-screening, PLACES, NIH, and
  ClinicalTrials.gov tools.
- Removed checked-in local issue-tracker artifacts from the public source tree
  and ignored future Beads/Dolt exports.

## v0.1.1 - Development readiness cleanup

- Consolidated the open release-prep branches into a single candidate branch.
- Added release metadata, community docs, and baseline CI automation.
- Corrected shipped configuration templates so `financial-intelligence` no
  longer pretends to run without a real `SEC_USER_AGENT`.
- Cleaned public-release artifacts and removed extracted planning/prototype
  material from the packaged repository.
- Added development-readiness support for Jefferson reporting workflows,
  source-evidence validation, public cache acquisition, and bounded financial
  disclosure parsing.
- Fixed MRF registry CCNs for Jefferson/LVHN facilities so price-transparency
  entries match the canonical facility ledger.
