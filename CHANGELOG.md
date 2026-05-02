# Changelog

## Unreleased

No unreleased changes.

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
