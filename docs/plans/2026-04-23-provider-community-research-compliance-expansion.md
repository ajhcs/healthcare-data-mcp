# Provider, Community, Research, and Compliance Expansion Plan

Date: 2026-04-23
Bead: HDM-b2m
Status: planned

## Goal

Extend `healthcare-data-mcp` with four high-value public-data capabilities:

- CMS PECOS-derived provider enrollment, chain, and ownership intelligence.
- CDC PLACES community health profiles.
- NIH RePORTER awards and ClinicalTrials.gov study search/profile tools.
- Exclusions screening tooling, starting with the existing HHS OIG LEIE plan and expanding to SAM.gov Exclusions.

The target is not a separate application. These should become part of this MCP server collection, using the existing FastMCP, structured response, cache, discovery, Docker, and `.mcp.json` patterns.

## Existing System Fit

The repo already has the right shape for this expansion:

- `servers/_launcher.py` registers focused MCP servers on predictable ports.
- Existing servers use `FastMCP`, `@mcp.tool(structured_output=True)`, Pydantic models, and `shared.utils.mcp_response`.
- Large public files are cached under `~/.healthcare-data-mcp/cache` and queried with DuckDB/Parquet.
- `shared.utils.http_client.resilient_request` provides retry/backoff and pooled HTTP.
- `servers/discovery/server.py` exposes dataset metadata, source URLs, cache status, and workflow prompts.
- `public-records` already owns regulatory/compliance data such as USAspending, SAM opportunities, 340B, HIPAA breaches, accreditation, and interoperability.

The major gap to fix before adding more CMS datasets is source freshness. Bead `HDM-d0i` already tracks hardcoded CMS URLs that will rot when CMS publishes new releases. This expansion should depend on a shared source resolver instead of embedding one-off CMS file URLs in each new server.

## Source Findings

Primary sources checked on 2026-04-23:

- CMS Provider Enrollment and Certification / PECOS overview: `https://www.cms.gov/medicare/enrollment-renewal/providers-suppliers/chain-ownership-system-pecos`
- CMS data catalog `data.json`: `https://data.cms.gov/data.json`
- CMS Provider Enrollment Hospital Data Guidance: `https://data.cms.gov/sites/default/files/2024-10/Hospital_Data_Guidance.pdf`
- Hospital All Owners data dictionary: `https://data.cms.gov/sites/default/files/2024-10/Hospital_All_Owners_Data_Dictionary.pdf`
- CDC PLACES portal: `https://www.cdc.gov/places/tools/data-portal.html`
- NIH RePORTER API: `https://api.reporter.nih.gov/`
- ClinicalTrials.gov API: `https://clinicaltrials.gov/data-api/api`
- HHS OIG LEIE downloadable database: `https://www.oig.hhs.gov/exclusions/exclusions_list.asp`
- HHS OIG LEIE CSV FAQ: `https://www.oig.hhs.gov/exclusions/transition-faq.asp`
- SAM.gov Exclusions API: `https://open.gsa.gov/api/exclusions-api/`
- SAM Exclusions public extract layout: `https://open.gsa.gov/api/sam-entity-extracts-api/v1/SAM_Exclusions_Public_Extract_Layout_V2.pdf`

Observed API/source state:

- CMS `data.json` currently lists PECOS-derived datasets modified `2026-04-20`, including `Hospital Enrollments`, `Hospital All Owners`, `Hospital Change of Ownership`, `Hospital Change of Ownership - Owner Information`, `Medicare Fee-For-Service Public Provider Enrollment`, and owner files for FQHC, HHA, hospice, RHC, and SNF.
- CMS data API sample endpoints worked for `Hospital All Owners` and `Medicare Fee-For-Service Public Provider Enrollment`.
- CDC Socrata catalog currently exposes `PLACES: Local Data for Better Health, County Data, 2025 release` as dataset `swc5-untb`; the metadata endpoint confirms fields including `stateabbr`, `locationname`, `measureid`, `datavaluetypeid`, `data_value`, confidence limits, population, and geolocation.
- NIH RePORTER `POST /v2/projects/search` worked with fiscal year 2026 award results and organization fields including org name, UEI, DUNS, IPF, state, ZIP, PIs, award amount, and project title.
- ClinicalTrials.gov `GET /api/v2/version` returned API version `2.0.5` and data timestamp `2026-04-23T09:00:05`; `GET /api/v2/studies` worked with field selection and pagination.
- HHS OIG LEIE page shows last update `04-10-2026` and links `03-2026 Updated LEIE Database`; OIG explicitly says there is no public LEIE API, so local CSV download/cache is the right implementation model.
- SAM.gov Exclusions API v4 is available at `https://api.sam.gov/entity-information/v4/exclusions?api_key=...`; CSV export uses a tokenized download endpoint.

## Architecture Decision

Add three new focused servers and extend one existing server:

| Capability | Server | Port | Rationale |
| --- | --- | ---: | --- |
| PECOS enrollment and ownership | `provider-enrollment` | 8017 | Distinct data domain with provider/entity identity, PAC IDs, enrollment IDs, ownership graph, and CHOW history. |
| CDC PLACES | `community-health` | 8018 | Community health is broader than demographics and should serve county/place/tract/ZCTA profiles without bloating `geo-demographics`. |
| NIH RePORTER + ClinicalTrials.gov | `research-trials` | 8019 | Research awards and studies share organization/PI/topic workflows and should be queryable together. |
| Exclusions | `public-records` first | 8013 | Existing plan `2026-04-23-hhs-oig-leie-exclusions-tooling.md` correctly keeps LEIE in `public-records`; add SAM Exclusions there after LEIE lands. |

Do not add a separate `exclusions` server in the first pass. Revisit only if LEIE + SAM + future state Medicaid lists make `public-records` too broad.

## Shared Foundation

### Source resolver

Create a shared source discovery layer before adding the new datasets:

- Add `shared/utils/source_catalog.py`.
- Resolve CMS datasets from `https://data.cms.gov/data.json` by title and optionally landing page slug.
- Resolve CDC Socrata datasets from `https://api.us.socrata.com/api/catalog/v1`.
- Persist a small source manifest beside cached files with `source_url`, `landing_page`, `dataset_id`, `title`, `modified`, `fetched_at`, `etag`, `last_modified`, `record_count`, and checksum where available.
- Support deterministic fallbacks for tests through fixture manifests.

This directly addresses `HDM-d0i`: new code should not introduce more hardcoded CMS release URLs.

### Cache conventions

Use one cache root per capability:

- `~/.healthcare-data-mcp/cache/provider-enrollment/`
- `~/.healthcare-data-mcp/cache/community-health/`
- `~/.healthcare-data-mcp/cache/research-trials/`
- Existing `~/.healthcare-data-mcp/cache/public-records/` for LEIE and SAM exclusions.

Large datasets should be normalized to Parquet with zstd compression. Per-query API responses can use JSON cache files with short TTLs. Every tool response that depends on cached bulk data should include source metadata.

### Identity normalization

Add `shared/utils/identity.py` for common matching helpers:

- NPI validation and normalization.
- CCN normalization.
- UEI normalization.
- PAC ID and enrollment ID normalization.
- Organization/person name normalization.
- Address/state/ZIP normalization.
- Conservative fuzzy matching wrappers using `rapidfuzz`.

This avoids each server inventing slightly different logic for Johns Hopkins, NPIs, CCNs, UEIs, owner names, and PI names.

## Server 1: `provider-enrollment`

### Data sources

Initial CMS datasets:

- `Medicare Fee-For-Service Public Provider Enrollment`
- `Hospital Enrollments`
- `Hospital All Owners`
- `Hospital Change of Ownership`
- `Hospital Change of Ownership - Owner Information`
- `Skilled Nursing Facility All Owners`
- `Skilled Nursing Facility Change of Ownership`
- `Skilled Nursing Facility Change of Ownership - Owner Information`

Second wave:

- `Federally Qualified Health Center All Owners`
- `Home Health Agency All Owners`
- `Hospice All Owners`
- `Rural Health Clinic All Owners`

### Proposed files

- `servers/provider_enrollment/__init__.py`
- `servers/provider_enrollment/server.py`
- `servers/provider_enrollment/models.py`
- `servers/provider_enrollment/data_loaders.py`
- `servers/provider_enrollment/ownership_graph.py`
- `tests/servers/provider_enrollment/test_data_loaders.py`
- `tests/servers/provider_enrollment/test_server.py`
- `tests/servers/provider_enrollment/test_ownership_graph.py`

### Tools

`search_provider_enrollment`

- Inputs: `npi`, `provider_name`, `state`, `provider_type`, `limit`.
- Uses Medicare FFS Public Provider Enrollment.
- Returns provider enrollment rows with NPI, PAC ID, enrollment ID, provider type, state, individual/org name, and source metadata.

`get_provider_enrollment_detail`

- Inputs: `npi`, `enrollment_id`, or `associate_id`.
- Returns all known enrollment rows plus linked hospital/SNF/FQHC/HHA/hospice/RHC enrollment details when available.

`get_facility_ownership`

- Inputs: `ccn`, `facility_name`, `state`, `provider_category`, `include_indirect`, `limit`.
- Joins enrollment files to all-owners files through `ENROLLMENT ID`.
- Returns ownership/management rows with owner PAC ID, owner type, role code/text, association date, percentage ownership, private equity/REIT/holding company flags, and owner address.

`trace_owner_network`

- Inputs: `owner_name`, `owner_associate_id`, `state`, `provider_category`, `depth`, `limit`.
- Builds a NetworkX graph from owner-to-enrollment relationships.
- Returns owner nodes, facility nodes, edges, shared owners, and repeated management-control relationships.

`search_change_of_ownership`

- Inputs: `ccn`, `facility_name`, `state`, `start_date`, `end_date`, `provider_category`, `limit`.
- Uses CHOW datasets to surface ownership transitions.

`profile_provider_control`

- Inputs: `ccn` or `npi`.
- Produces an agent-friendly profile combining enrollment status, owners, CHOW events, and join keys for NPPES, facility, quality, and exclusions tools.

### Implementation notes

- Use CMS data API pagination for small/detail queries and bulk download to Parquet for owner graph queries.
- Treat ownership percentages and role flags as strings until normalized, because CMS fields can be blank.
- Preserve both original CMS column names and normalized snake-case aliases in internal models.
- Keep graph output bounded; default `depth=1`, cap `depth=3`.

## Server 2: `community-health`

### Data sources

CDC PLACES 2025 release through Socrata:

- County Open Data: currently `swc5-untb`
- Place Open Data: currently `eav7-hnsx`
- Census Tract Open Data: currently `cwsq-ngmh`
- ZCTA Open Data: currently `qnzd-25i4`
- GIS-friendly equivalents where geometry is needed.
- Data dictionary: currently `m35w-spkz`
- Non-medical factor measures: county/place/tract/ZCTA ACS 2017-2021 datasets.

The implementation must discover datasets by title/release, not hardcode these IDs as permanent truth.

### Proposed files

- `servers/community_health/__init__.py`
- `servers/community_health/server.py`
- `servers/community_health/models.py`
- `servers/community_health/places_client.py`
- `servers/community_health/data_loaders.py`
- `servers/community_health/profiles.py`
- `tests/servers/community_health/test_places_client.py`
- `tests/servers/community_health/test_profiles.py`
- `tests/servers/community_health/test_server.py`

### Tools

`list_places_measures`

- Inputs: `category`, `release`, `search`.
- Returns measure IDs, labels, categories, value types, and data dictionary metadata.

`search_places`

- Inputs: `geography`, `state`, `county_fips`, `tract_fips`, `zcta`, `place`, `measure_id`, `category`, `value_type`, `limit`.
- Returns normalized PLACES rows with confidence intervals and population.

`get_places_profile`

- Inputs: `geography`, `location_id`, `measure_ids`, `categories`.
- Returns a compact profile for one county/tract/ZCTA/place.

`compare_places`

- Inputs: list of locations plus measures/categories.
- Returns side-by-side values, ranks within state when feasible, and missing-data notes.

`get_market_community_profile`

- Inputs: `ccn` or `zip_codes`.
- Uses existing service-area/geo-demographics join keys where available.
- Returns community health measures for a hospital service area or ZIP list.

### Implementation notes

- Socrata JSON API is enough for filtered queries; bulk Parquet caching is better for compare/ranking workflows.
- Normalize value types (`CrdPrv`, age-adjusted where present) and make confidence intervals explicit.
- Do not expose PLACES estimates as patient-level facts. They are community estimates and should be described that way in response metadata.

## Server 3: `research-trials`

### Data sources

- NIH RePORTER API v2 project search and publication search.
- ClinicalTrials.gov API v2 studies, study detail, stats/metadata, and version endpoint.

### Proposed files

- `servers/research_trials/__init__.py`
- `servers/research_trials/server.py`
- `servers/research_trials/models.py`
- `servers/research_trials/reporter_client.py`
- `servers/research_trials/clinical_trials_client.py`
- `servers/research_trials/org_matching.py`
- `servers/research_trials/profiles.py`
- `tests/servers/research_trials/test_reporter_client.py`
- `tests/servers/research_trials/test_clinical_trials_client.py`
- `tests/servers/research_trials/test_profiles.py`
- `tests/servers/research_trials/test_server.py`

### Tools

`search_nih_projects`

- Inputs: `org_name`, `org_uei`, `pi_name`, `text`, `fiscal_years`, `activity_codes`, `agencies`, `limit`, `offset`.
- Returns projects with project number, title, fiscal year, award amount, PIs, org identifiers, funding mechanism, terms, and RePORTER URL.

`get_nih_project`

- Inputs: `project_num` or `appl_id`.
- Returns project detail and selected publications when available.

`profile_research_funding`

- Inputs: `org_name`, `org_uei`, `years`.
- Aggregates projects by fiscal year, institute/center, PI, activity code, and terms.

`search_clinical_trials`

- Inputs: `query`, `sponsor`, `condition`, `intervention`, `location`, `status`, `phase`, `fields`, `page_size`, `page_token`.
- Uses ClinicalTrials.gov v2 fields and pagination.

`get_clinical_trial`

- Inputs: `nct_id`.
- Returns study detail with sponsor, collaborators, status, conditions, interventions, contacts/locations, dates, enrollment, phases, and links.

`profile_research_activity`

- Inputs: `organization_name`, `uei`, `facility_name`, `state`, `years`.
- Combines NIH funding and ClinicalTrials.gov study activity into one organization profile.

### Implementation notes

- RePORTER is POST/JSON with rich criteria. Keep a thin client and build higher-level profile tools separately.
- ClinicalTrials.gov API v2 supports field selection and page tokens; preserve `nextPageToken` in responses.
- Check `GET /api/v2/version` in metadata and expose `dataTimestamp` so users know the freshness of trial data.
- Organization matching should be conservative; do not silently merge hospitals, universities, foundations, and health systems with similar names.

## Exclusions Tooling

### LEIE

Implement the existing plan first:

- `docs/plans/2026-04-23-hhs-oig-leie-exclusions-tooling.md`

Keep LEIE inside `public-records`, with tools such as:

- `check_leie_npi`
- `search_leie_individual`
- `search_leie_entity`
- `screen_leie_batch`
- `get_leie_metadata`

Important constraints from the existing plan:

- Use the downloadable OIG CSV, because OIG states there is no public API.
- Avoid "cleared" or "verified not excluded" language.
- Do not accept or store SSNs/EINs.
- Include source/version metadata and match basis in every response.

### SAM Exclusions

Add after LEIE:

- `servers/public_records/sam_exclusions_client.py`
- Models for `SAMExclusionRecord`, `SAMExclusionSearchResponse`, and shared batch screening output.
- `search_sam_exclusions`
- `check_sam_exclusion_identifier`
- `screen_sam_exclusions_batch`
- `get_sam_exclusions_metadata`

Inputs should support `entity_name`, `first_name`, `last_name`, `uei`, `cage_code`, `npi`, `state`, `country`, `classification`, `exclusion_type`, `excluding_agency`, and `limit`.

Use SAM.gov API v4 JSON for search/detail workflows. Use the CSV tokenized export path only for future bulk mirror mode, because it introduces asynchronous file availability.

## Discovery, Gateway, and Docs

Update `servers/discovery/server.py` with dataset catalog entries for:

- `cms_pecos_public_provider_enrollment`
- `cms_pecos_hospital_enrollments`
- `cms_pecos_hospital_owners`
- `cms_pecos_hospital_chow`
- `cdc_places`
- `nih_reporter_projects`
- `clinicaltrials_gov`
- `hhs_oig_leie`
- `sam_gov_exclusions`

Update:

- `README.md`
- `.mcp.json`
- `docker-compose.yml`
- `docs/DISCOVERY_SERVER.md`
- `docs/MCP_CLIENTS.md` if local client setup changes
- `docs/REMOTE_GATEWAY.md` only if gateway metadata includes the new datasets
- `smoke_test.py` with optional live checks guarded by env vars/API keys

The metadata gateway should remain remote-safe. It can expose metadata about these datasets, but it should not proxy full live queries or exclusion screening unless a separate authenticated gateway design is approved.

## Deployment on Plumbob

For local Docker Compose:

- Add `provider-enrollment` on port `8017`.
- Add `community-health` on port `8018`.
- Add `research-trials` on port `8019`.
- Keep exclusions in `public-records` on port `8013`.
- Reuse the `healthcare-cache` Docker volume.
- Add `SAM_GOV_API_KEY` to the public-records environment if SAM exclusions are implemented.

No Caddy or OPNsense changes are needed for local-only MCP use. If these are exposed beyond localhost later, put them behind the existing HTTPS/auth pattern and prefer the gateway or an authenticated reverse proxy rather than direct unauthenticated server exposure.

## Security and Compliance

- All data sources are public datasets, but user-provided batch screening inputs can still include sensitive identifiers. Do not log raw DOBs, SSNs, EINs, or batch payloads.
- Do not add `verify=False` for federal sites. Fix local CA trust if certificate validation fails.
- Cap query sizes and batch sizes. Default to small MCP responses with pagination.
- Use env vars for API keys. Do not add client-side/public env names for secrets.
- Exclusion results are screening support, not legal/compliance determinations. Responses must preserve match basis, source freshness, and verification caveats.
- Avoid cross-source identity merges unless the join key is explicit or the response labels it as a candidate match.

## Testing Plan

Unit tests:

- Parser normalization for CMS, PLACES, LEIE, and SAM responses.
- Source resolver fixtures for CMS `data.json` and Socrata catalog responses.
- Query builders for RePORTER and ClinicalTrials.gov.
- Identity normalization and fuzzy matching edge cases.
- MCP tool validation and structured response shape tests.

Integration tests:

- Parquet cache creation/query with small fixture CSV/JSON files.
- Discovery metadata completeness.
- Import tests for new servers.
- Docker command/launcher registration tests.

Optional live smoke tests:

- CMS data API one-row query.
- CDC PLACES one-row query.
- NIH RePORTER one-project query.
- ClinicalTrials.gov version and one-study query.
- OIG LEIE metadata refresh.
- SAM Exclusions query only when `SAM_GOV_API_KEY` is set.

Expected local gate after implementation:

```bash
ruff check .
pytest -q
python3 -m compileall -q servers shared tests smoke_test.py
```

## Implementation Sequence

1. Build shared source resolver and identity normalization.
2. Implement LEIE in `public-records` from the existing LEIE plan.
3. Implement `provider-enrollment` with CMS public provider enrollment, hospital enrollments, owners, and CHOW.
4. Implement `community-health` with CDC PLACES measure search and geography profiles.
5. Implement `research-trials` with NIH RePORTER and ClinicalTrials.gov clients/tools.
6. Add SAM Exclusions to `public-records`.
7. Update discovery metadata, docs, Docker Compose, launcher, `.mcp.json`, and smoke tests.
8. Run full quality gates and create follow-up beads for any deferred provider categories or workflow polish.

## Bead Breakdown

Recommended implementation beads:

- Foundation: source resolver and identity normalization.
- LEIE public-records tooling.
- Provider enrollment CMS data loaders and cache manifests.
- Provider enrollment MCP tools and ownership graph.
- CDC PLACES loader/client and measure metadata.
- Community health MCP tools and service-area profile.
- NIH RePORTER client/tools.
- ClinicalTrials.gov client/tools.
- Research activity profile tool.
- SAM Exclusions client/tools.
- Discovery/docs/Docker/client registration.
- Smoke tests and live source contract checks.

## Open Questions

- Should owner graph traversal include only currently active ownership, or should CHOW history be represented as temporal edges in the first release?
- Should PLACES service-area profiles initially aggregate by county/ZCTA only, or should they support tract-weighted rollups from hospital ZIP/service-area inputs?
- Should research/trials matching prefer organization name search first or require user-provided UEI/IPF when available to avoid false merges?
- Should SAM Exclusions batch screening share one combined response shape with LEIE, or stay source-specific and add a later aggregate `screen_exclusions_batch` tool?
