# Changelog

## Unreleased

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
