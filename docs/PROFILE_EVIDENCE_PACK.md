# Profile Evidence Pack Workflow

health-system-profiler.build_profile_evidence_pack is a read-only MCP tool for
Healthcare Toolkit profile population. It returns public-data evidence
candidates only; it does not write to Healthcare Toolkit.

## Inputs

- state: required two-letter state abbreviation, for example PA.
- system_slug: optional reviewed system slug.
- system_name: optional public system name.
- ccns: optional exact CMS Certification Numbers.
- required_fields: optional fields that must be present or returned as
  unavailable_public, source_conflict, or needs_review.

## Output Fields

- system_identity_aliases: AHRQ/system identity and alias candidates.
- current_hospital_roster: candidate hospital roster rows anchored by CCN when available.
- source_identifiers: CCNs and source-local identifiers such as POS PRVDR_NUM and AHRQ system IDs.
- addresses: source-scoped facility address candidates.
- geography_candidates: Census Geocoder or reviewed OSM fallback coordinate, county, and county GEOID candidates.
- hospital_bed_counts: facility bed-count resolver output with POS plus configured HCRIS/state cache candidates when available.
- system_bed_count_candidates: additive CCN-scope bed rollup candidates when source rows are compatible.
- bed_rollup_guidance: additive versus non-additive rollup rules.
- affiliation_evidence: AHRQ linkage, PECOS/CHOW/provider-enrollment, and reviewed official current-operator evidence when available.
- facility_site_count_evidence: exact reviewed official count claims when available; vague claims remain needs_review.
- conflicts: explicit source conflicts such as duplicate campuses, material bed-source variance, current-operator mismatch, or count mismatch.
- unavailable_public_findings: searched public sources where evidence was insufficient. These are not negative factual claims.
- cache_preflight: cache-manager readiness for the workflow source set.
- suggested_next_calls: MCP calls that can recover missing or conflicted evidence.

Every candidate row carries evidence, source_metadata, dataset_id or
source_family, source period/date, retrieval/access date, cache
status/freshness, confidence, match basis, caveat, and metadata.mcp_server /
metadata.mcp_tool.

## Source Precedence

1. CMS POS/HGI for facility identity, addresses, CCNs, source-local identifiers, and certified bed candidates.
2. AHRQ as discovery/linkage spine, not final ownership authority.
3. HCRIS and state report caches for bed-count corroboration; reviewed official evidence rows only when configured.
4. Census Geocoder first for coordinates, county, and county GEOID.
5. OSM/Nominatim only as an acceptable-quality fallback.
6. Reviewed official system pages/reports for exact facility/site counts.
7. PECOS/provider-enrollment/CHOW and reviewed official rows for current-affiliation review.

## Healthcare Toolkit Persistence Guidance

Persist:

- Supported source rows into profile_sources.
- Supported exact metric values, such as selected CCN-scope bed candidates or exact official counts, into profile_metric_values.
- Source-backed narrative or rollup guidance into profile_knowledge_objects when the caveat and evidence receipt are preserved.

Review manually:

- source_conflict, needs_review, and unavailable_public rows.
- Vague count claims such as "more than", "over", "about", or plus-sign counts.
- OSM fallback geography.
- Duplicate campus or incompatible bed-scope rollups.
- AHRQ/current official operator mismatches.

Do not estimate missing fields. If evidence is insufficient, keep the returned
finding and use suggested_next_calls for recovery.

The profiler does not scrape official pages directly. Use
web-intelligence.scrape_system_profile to collect official/public page evidence,
review it, then provide it through the local profile-evidence cache before
treating official counts or operator claims as source-backed candidates.
