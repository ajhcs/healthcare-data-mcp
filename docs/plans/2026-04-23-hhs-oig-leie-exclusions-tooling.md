# HHS OIG LEIE Exclusions Tooling Plan

Date: 2026-04-23
Bead: HDM-c9t
Status: planned

## Problem Frame

Add HHS OIG List of Excluded Individuals/Entities (LEIE) screening to this MCP server collection so users can check providers, entities, and batches against the current federal exclusion list from the same public-records/regulatory tool surface they already use.

The tool must be explicit that downloadable LEIE matches are screening results, not final SSN/EIN identity verification. OIG states the downloadable database does not include SSNs or EINs, and potential name matches should be verified through the Online Searchable Database when that level of verification is required.

## External Source Findings

Primary sources reviewed:

- HHS OIG LEIE downloadable database page: `https://www.oig.hhs.gov/exclusions/exclusions_list.asp`
- HHS OIG LEIE help: `https://oig.hhs.gov/exclusions/exclusions-help.asp`
- HHS OIG monthly supplements: `https://oig.hhs.gov/exclusions/supplements.asp`
- HHS OIG exclusions FAQ: `https://oig.hhs.gov/faqs/exclusions-faq/`
- HHS OIG state Medicaid agency guidance: `https://oig.hhs.gov/exclusions/state-agencies.asp`
- Current record layout PDF: `https://www.oig.hhs.gov/exclusions/files/leie_record_layout.pdf`

Current observed source state on 2026-04-23:

- The LEIE downloadable page showed `Last Update` as `04-10-2026` and linked `03-2026 Updated LEIE Database`.
- The stable CSV URL `https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv` returned `200`, `content-type: text/csv`, `content-length: 15420582`, `last-modified: Fri, 10 Apr 2026 11:00:39 GMT`, and `content-disposition: attachment; filename="UPDATED.csv"`.
- The file header is:

```text
LASTNAME,FIRSTNAME,MIDNAME,BUSNAME,GENERAL,SPECIALTY,UPIN,NPI,DOB,ADDRESS,CITY,STATE,ZIP,EXCLTYPE,EXCLDATE,REINDATE,WAIVERDATE,WVRSTATE
```

OIG guidance that affects implementation:

- The updated LEIE database is a complete file of exclusions currently in effect and is replaced monthly.
- Reinstated individuals/entities are not included in the updated LEIE file.
- Profile updates are not for verifying exclusion status.
- Monthly supplements contain only one month of exclusions or reinstatements and OIG archives only the previous 12 months.
- The downloadable database does not contain SSNs or EINs.
- OIG recommends maintaining documentation of initial searches and follow-up verification.
- OIG says state Medicaid agencies should check LEIE monthly and on new enrollments.

## Scope

Implement LEIE in the existing `public-records` server, not a new server.

In scope:

- Download/cache the complete LEIE `UPDATED.csv`.
- Normalize and store it as Parquet under the existing public-records cache root.
- Expose individual/entity/NPI search tools with structured responses.
- Expose a batch screening tool for small to medium candidate lists.
- Return source/version metadata and match rationale in every screening response.
- Add discovery metadata so the dataset is visible through the discovery server.
- Add focused unit tests for parsing, matching, cache metadata, and tool responses.

Out of scope for the first implementation:

- Automating the online SSN/EIN verification workflow.
- Storing SSNs, EINs, or other user-provided sensitive identity numbers.
- Building a historical exclusion timeline from supplements.
- Treating profile correction files as authoritative exclusion checks.
- State Medicaid exclusion lists beyond HHS OIG LEIE.

## Architecture Decision

Extend `servers/public_records/` because this server already owns compliance/regulatory public data: USAspending, SAM.gov, 340B, HIPAA breach history, accreditation, and interoperability. LEIE fits that boundary, avoids a new port, and reuses the existing public-records cache, model, and structured FastMCP response conventions.

The first implementation should not add a separate `exclusions` MCP server. If public-records grows too broad later, split regulatory screening into its own server as a follow-up after LEIE and any future SAM/NPDB screening use cases are clear.

## Proposed File Changes

Modify:

- `servers/public_records/data_loaders.py`
- `servers/public_records/models.py`
- `servers/public_records/server.py`
- `servers/discovery/server.py`
- `docs/plans/2026-03-05-public-records-design.md`
- `README.md`
- `docs/DISCOVERY_SERVER.md` if cache-status output is documented there

Add:

- `tests/servers/public_records/__init__.py`
- `tests/servers/public_records/test_leie_data_loaders.py`
- `tests/servers/public_records/test_leie_server.py`

Potentially modify:

- `tests/test_discovery_metadata.py`
- `tests/test_structured_server_imports.py` only if public-records import behavior changes

No dependency changes are expected. `pandas`, `duckdb`, `pyarrow`, and `rapidfuzz` already exist in `pyproject.toml`.

## Data Model

Add Pydantic models in `servers/public_records/models.py`:

- `LEIESourceMetadata`
- `LEIEExclusionRecord`
- `LEIESearchResponse`
- `LEIEBatchCandidate`
- `LEIEBatchResult`
- `LEIEBatchResponse`

Recommended `LEIEExclusionRecord` fields:

- `entity_type`: `"individual"` or `"entity"`
- `display_name`
- `last_name`
- `first_name`
- `middle_name`
- `business_name`
- `general_category`
- `specialty`
- `upin`
- `npi`
- `dob`
- `address`
- `city`
- `state`
- `zip_code`
- `exclusion_type`
- `exclusion_date`
- `reinstatement_date`
- `waiver_date`
- `waiver_state`
- `match_basis`
- `match_score`
- `verification_status`

Recommended source metadata fields:

- `source_name`: `"HHS OIG LEIE"`
- `source_url`
- `downloaded_at`
- `source_last_modified`
- `source_etag`
- `record_count`
- `cache_path`
- `cache_status`: `"fresh"`, `"refreshed"`, or `"stale"`

Normalize `0000000000` NPI, `00000000` dates, and blank UPIN/DOB values to empty strings or `None` consistently in API responses. Preserve original string values internally only if needed for audit/debug.

## Loader Design

Add constants in `servers/public_records/data_loaders.py`:

- `LEIE_URL = "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"`
- `_LEIE_PARQUET = _CACHE_DIR / "leie_current.parquet"`
- `_LEIE_META = _CACHE_DIR / "leie_current.meta.json"`
- `_LEIE_CSV = _CACHE_DIR / "leie_current.csv"`

Add functions:

- `async ensure_leie_cached(force_refresh: bool = False) -> dict`
- `parse_leie_csv(csv_path: Path) -> pd.DataFrame`
- `query_leie_by_npi(npi: str) -> list[dict]`
- `query_leie_by_individual(last_name: str, first_name: str = "", state: str = "", dob: str = "", limit: int = 25) -> list[dict]`
- `query_leie_by_entity(entity_name: str, state: str = "", npi: str = "", limit: int = 25) -> list[dict]`
- `screen_leie_candidates(candidates: list[dict], limit_per_candidate: int = 5) -> list[dict]`
- `get_leie_source_metadata() -> dict`

Refresh rules:

- Prefer a `HEAD` request to compare `Last-Modified`/`ETag` with `_LEIE_META`.
- Refresh when metadata differs, when `force_refresh=True`, or when the cache is older than 31 days.
- If `HEAD` fails but the Parquet cache exists and is less than 45 days old, serve cached data with `cache_status="stale"` and include the failure in metadata.
- If no cache exists and download fails, return a structured recoverable error from the tool.

Parsing rules:

- Read all columns as strings.
- Strip whitespace and normalize column names to lowercase snake case.
- Validate all layout columns are present.
- Add computed columns: `entity_type`, `display_name`, `normalized_individual_name`, `normalized_business_name`, `normalized_state`, `normalized_npi`, and parsed date strings.
- Do not drop rows because NPI/DOB are missing; OIG notes many records do not include those identifiers.
- Write Parquet with zstd compression.

Certificate note:

- On 2026-04-23, local `curl` failed certificate verification for `oig.hhs.gov`, while browser fetch succeeded. Implementation should test through `shared.utils.http_client.resilient_request`/`httpx` and fix local CA trust if needed. Do not add `verify=False`.

## Tool Design

Add tools to `servers/public_records/server.py` with `@mcp.tool(structured_output=True)`.

### `check_leie_npi`

Parameters:

- `npi: str`
- `limit: int = 25`
- `force_refresh: bool = False`

Behavior:

- Validate NPI as 10 digits.
- Normalize placeholder `0000000000` as invalid input.
- Return exact NPI matches with `match_basis="npi_exact"` and `verification_status="strong_potential_match"`.
- Include source metadata and OIG verification warning.

### `search_leie_individual`

Parameters:

- `last_name: str`
- `first_name: str = ""`
- `state: str = ""`
- `dob: str = ""`
- `limit: int = 25`
- `force_refresh: bool = False`

Behavior:

- Require at least `last_name`.
- Candidate generation should use prefix or exact last-name matching, then score first/middle name similarity when supplied.
- If DOB is supplied, use it only as an additional match criterion; never require DOB because OIG may not have it.
- Return `verification_status="potential_match"` unless an exact NPI was also involved.
- Include `match_basis` values such as `last_name_prefix`, `name_state`, `name_dob`, or `name_fuzzy`.

### `search_leie_entity`

Parameters:

- `entity_name: str`
- `state: str = ""`
- `npi: str = ""`
- `limit: int = 25`
- `force_refresh: bool = False`

Behavior:

- Require `entity_name` or `npi`.
- If NPI is provided, perform exact NPI matching first.
- For name matching, generate candidates by normalized business-name prefix/contains and score with `rapidfuzz`.
- Return entity records only unless NPI exact match finds a row classified as individual.

### `screen_leie_batch`

Parameters:

- `candidates: list[dict[str, str]]`
- `limit_per_candidate: int = 5`
- `force_refresh: bool = False`

Candidate fields:

- `candidate_id`
- `entity_type`
- `npi`
- `first_name`
- `last_name`
- `entity_name`
- `state`
- `dob`

Behavior:

- Cap batch size initially at 100 candidates to keep MCP calls responsive.
- Process NPI exact matches before name matching.
- Return one `LEIEBatchResult` per candidate with `match_count`, `best_match_score`, `matches`, `screened_at`, and source metadata.
- Include enough input echo to make audit logs useful without storing sensitive identifiers beyond fields already in the request.

### `get_leie_metadata`

Parameters:

- `force_refresh: bool = False`

Behavior:

- Ensure or inspect cache and return source metadata, record count, cache age, source URL, and layout columns.
- This supports operational validation without screening a real person/entity.

## Matching Approach

Use deterministic matching before fuzzy scoring:

- NPI exact match is highest confidence.
- Individual last name prefix/exact with optional first name/state/DOB narrows the candidate set.
- Entity name normalized prefix/contains handles OIG guidance that entity searches should begin with first letters but also makes local batch screening more useful.
- Fuzzy scoring should only rank candidates already narrowed by name tokens/state. Avoid scanning every row with fuzzy matching for every request.

Response language should avoid saying “cleared” or “verified not excluded.” Use:

- `"no_current_leie_match_found"` for zero results.
- `"potential_match"` for name/entity matches.
- `"strong_potential_match"` for exact NPI matches.

## Discovery Metadata

Add `hhs_oig_leie` to `DATASET_CATALOG` in `servers/discovery/server.py`:

- `title`: `HHS OIG List of Excluded Individuals/Entities`
- `server`: `["public-records"]`
- `category`: `regulatory_compliance`
- `grain`: `one row per currently excluded individual or entity`
- `source_system`: `HHS Office of Inspector General`
- `source_urls`: LEIE page, `UPDATED.csv`, record layout PDF
- `cache_files`: `public-records/leie_current.parquet`, `public-records/leie_current.meta.json`
- `schema.identity_fields`: `LASTNAME`, `FIRSTNAME`, `BUSNAME`, `NPI`, `DOB`
- `schema.common_fields`: `GENERAL`, `SPECIALTY`, `EXCLTYPE`, `EXCLDATE`, `WAIVERDATE`, `WVRSTATE`
- `schema.join_keys`: `npi`, `name`, `state`
- `workflows`: `provider screening`, `vendor screening`, `enrollment checks`, `monthly exclusion monitoring`

If cache status currently enumerates expected files manually, add the two LEIE cache files there too.

## Tests

Add `tests/servers/public_records/test_leie_data_loaders.py`:

- `test_parse_leie_csv_normalizes_layout_columns`: fixture CSV with individual/entity rows produces normalized columns.
- `test_parse_leie_csv_normalizes_placeholder_values`: `0000000000` NPI and `00000000` dates become empty/null response values.
- `test_parse_leie_csv_rejects_missing_required_columns`: missing layout column raises or returns false in a controlled way.
- `test_query_leie_by_npi_exact_match`: exact NPI returns only matching rows.
- `test_query_leie_by_individual_uses_name_state_filters`: fixture Parquet returns expected individual candidates.
- `test_query_leie_by_entity_scores_business_name_candidates`: entity name finds expected business record.
- `test_get_leie_source_metadata_reads_meta_json`: metadata includes source URL, last modified, and count.

Add `tests/servers/public_records/test_leie_server.py`:

- `test_check_leie_npi_rejects_invalid_npi`.
- `test_check_leie_npi_returns_structured_exact_match`.
- `test_search_leie_individual_requires_last_name`.
- `test_search_leie_entity_requires_name_or_npi`.
- `test_screen_leie_batch_caps_batch_size`.
- `test_screen_leie_batch_prioritizes_npi_exact_match`.
- `test_get_leie_metadata_does_not_require_real_download`.

Update `tests/test_discovery_metadata.py`:

- Assert `hhs_oig_leie` has source/schema/cache payloads and appears in dataset search.

Regression gate:

```bash
pytest tests/servers/public_records tests/test_discovery_metadata.py tests/test_structured_server_imports.py
```

## Sequencing

1. Add LEIE models and fixture-based tests first.
2. Implement parser/cache metadata/query functions in `data_loaders.py`.
3. Add server tools and tool tests with monkeypatched loader functions.
4. Add discovery metadata and update docs.
5. Run the focused pytest command.
6. Perform one manual live-cache smoke test of `get_leie_metadata` or `check_leie_npi` after confirming `httpx` can validate `oig.hhs.gov`.

## Risks And Mitigations

- False positives from name matching: expose match basis and score, avoid “verified” language, and preserve OIG verification warning.
- Stale data: refresh by source `Last-Modified`/`ETag` and monthly TTL, not the existing 90-day bulk default.
- Missing NPIs/DOBs: keep name-based screening useful and make missing identifiers explicit in records.
- Sensitive identifiers: do not accept or store SSNs/EINs.
- Upstream page/link changes: use the stable `UPDATED.csv` URL, but keep the LEIE landing page URL in metadata for human fallback.
- Local CA issue: verify with the project HTTP client during implementation and fix host trust separately if needed.

## Open Decisions

- Whether batch screening should accept more than 100 candidates in-process or require a future file-backed workflow.
- Whether public-records should eventually be renamed/split into a broader compliance server once LEIE joins SAM, HIPAA, 340B, and accreditation.
- Whether to add state Medicaid exclusion lists after federal LEIE is stable.
