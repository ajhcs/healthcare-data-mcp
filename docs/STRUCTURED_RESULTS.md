# Structured MCP Tool Results

FastMCP can emit `structuredContent` and output schemas when tools return Python
objects instead of `json.dumps(...)` strings. Use
`shared.utils.mcp_response` as the migration layer for consistent shapes while
server tools move one at a time.

## Helper Shapes

- `response_envelope(...)` returns `{"ok": true, ...}` with JSON-compatible
  payload fields.
- `collection_response(results, limit=..., offset=..., total=...)` returns
  `ok`, `results`, `count`, and `meta.pagination`.
- `record_response(record, key="facility")` returns one model or dict under a
  stable top-level key.
- `empty_response(message)` returns an empty successful result set.
- `error_response(message, code=..., detail=...)` returns a structured failure
  envelope for recoverable, non-exceptional failures.
- `evidence_receipt(...)` returns the canonical source/provenance contract for
  healthcare-relevant facts.
- `raise_invalid_params`, `raise_not_found`, and `raise_tool_error` raise
  FastMCP `ToolError` for invalid inputs or runtime failures.

The helpers convert Pydantic models, dates, dataclasses, and other JSON-like
values to JSON-compatible Python objects. Do not pre-serialize with
`json.dumps`; let FastMCP serialize the returned object.

## Evidence Receipt Contract

Healthcare-relevant tool results should expose an `evidence` object with these
fields: `source_name`, `source_url`, `dataset_id`, `source_period`,
`landing_page`, `retrieved_at`, `source_modified`, `cache_status`,
`cache_freshness`, `entity_scope`, `query`, `cache_key`, `match_basis`,
`confidence`, `caveat`, and `next_step`.

The contract is implemented in `shared.utils.mcp_response.evidence_receipt` and
validated by `validate_evidence_receipt`. Use
`validate_evidence_receipt(receipt, require_content=True)` for live-gateway
or report-ready result paths; that stricter mode requires source identity,
either `source_url` or `landing_page`, source period or retrieval/modified
metadata, cache status/freshness, match basis, confidence, caveat, and next
step. Template receipts may keep placeholder values while workflow planning,
but cited facts should pass the stricter content check after the owning tool
has run.

The contract is currently used across priority hospital quality, provider enrollment,
health-system profiler, financial intelligence, workforce analytics,
public-record cyber/breach, web intelligence, exclusion-screening, and
cms-facility facility/NPPES/cost-report surfaces. Claims analytics outputs
also expose receipts for CMS Medicare Provider Utilization PUF aggregate
volumes and market-share facts. Research/trials outputs expose receipts for
NIH RePORTER, ClinicalTrials.gov, and combined public research-activity
profiles. Community-health outputs expose receipts for CDC PLACES measure
discovery, geography profiles, comparisons, and market community profiles.
CDC PLACES measure catalog rows, location search rows, profile/comparison
estimate rows, market service-area geography rows, and market aggregate rows
also carry row-level receipts so extracted report facts preserve geography ID,
measure ID, year/value basis, source vintage, and the community-estimate caveat.
Physician referral/network outputs expose receipts for NPPES physician search
and profiles, DocGraph cache imports, referral network graphs, physician mix,
and leakage-readiness outputs. NPPES physician search rows, DocGraph graph
nodes and edges, physician-mix sample classification rows, leakage destination
rows, and leakage specialty-summary rows also carry row-level receipts so
report extraction preserves exact NPI, edge, classification, specialty,
source-vintage, and readiness caveat context. Service-area outputs expose receipts for CMS
Hospital Service Area File PSA/SSA and market-share facts, Dartmouth Atlas
HSA/HRR crosswalks, and HSAF-to-Dartmouth overlap context.
Geo-demographics outputs expose receipts for Census ACS ZCTA estimates, CMS
Medicare Geographic Variation geography aggregates, Census TIGER/ZCTA
adjacency, and HUD USPS ZIP crosswalk allocations. ACS batch rows, TIGER
adjacent-ZCTA rows, and HUD ZIP crosswalk allocation rows carry row-level
receipts for report extraction. Price-transparency outputs
expose receipts for CMS/MRF discovery, hospital MRF cache rows, rate
dispersion, candidate system comparisons, and Medicare/peer benchmark context.
Drive-time outputs expose receipts for OSRM routes/matrices, OpenRouteService
isochrones, CMS Hospital General Information facility candidates, Census ZIP
centroid fallback geocoding, and E2SFCA modeled accessibility scores. Drive-time
matrix cells, competing-facility rows, and E2SFCA demand-point score rows carry
row-level receipts so extracted access facts preserve the origin/destination or
demand-point basis, routing backend, source caveat, and caller-supplied input
dependency.
Empty strings are acceptable when a source does not expose a field, but the
field should still be present so report builders can validate the shape
consistently.

Source-backed no-data results should keep the same receipt shape. For example,
health-system profiler and hospital-quality no-match responses include
`ok: false`, an `error` object, the relevant public-source evidence receipt,
and seed `identity` / `identity_map` fields for the unresolved system or
hospital identifier. Hospital-quality summary and exact-measure tools expose a
CCN-centered `identity_map` with source-claim paths for CMS quality, HRRP, HAC,
HCAHPS, cost-report, and measure-ID joins plus top-level `source_metadata`
paired with the canonical evidence receipt on success, no-data, exact-measure,
and comparison responses. HRRP condition rows, HAC `domain_evidence` rows, and
HCAHPS domain rows also carry row-level evidence receipts so extracted summary facts
preserve the CCN, row kind, measure/domain basis, cache metadata, caveat, and
next step to fetch exact CMS measure rows when needed. `hospital-quality.compare_hospitals`
includes a top-level composite comparison receipt, source metadata, and a CCN-keyed
`identity_map`; report builders should still cite the nested quality, safety,
readmission, and HCAHPS evidence receipts for source-specific facts.
Provider-enrollment zero-result searches remain successful bounded search
responses, but their receipts use explicit `_no_match` match bases and seed the
query identifier or owner-name identity. Provider-enrollment responses also
include top-level `source_metadata` paired with the canonical evidence receipt,
plus an `identity_map` with NPI, CCN, PECOS enrollment ID, owner associate ID,
and PAC join keys and source-claim paths for enrollment, ownership, CHOW, and
bounded owner-network facts. Enrollment, ownership, and CHOW rows also carry
row-level evidence receipts so extracted report facts preserve the exact CMS
public dataset, identifier basis, cache metadata, caveat, and source-row
confidence after the row is separated from the parent response.
Health-system-profiler responses include top-level `source_metadata` paired
with the canonical evidence receipt plus an `identity_map` with AHRQ system IDs
or reviewed system slugs, linked CCNs, NPIs, source names, ZIPs, and
source-claim paths for system and facility reconciliation facts. Search result
rows, inpatient facility rows, facility
reconciliation rows, and reviewed merger-evidence rows also carry row-level
evidence receipts so extracted system facts preserve the AHRQ/reviewed source,
identifier basis, source refs, caveat, and row confidence. NPPES-discovered
outpatient site rows carry NPPES-specific row receipts with NPI, taxonomy,
category, live API freshness, and a caveat that system affiliation remains
candidate context. Health-system source claims expose `row_evidence_paths` for
`results[].evidence`, `inpatient_facilities[].evidence`,
`outpatient_sites[].evidence`, `facilities[].evidence`, and
`facility_reconciliation.facilities[].evidence` so workflow fact rows can keep
the cited system/facility receipt. Financial-
intelligence profile, filing-search,
filing-detail, municipal-bond, and audited-PDF responses include an
`source_metadata` object paired with the canonical evidence receipt plus an
`identity_map` that separates CCN-anchored HCRIS/HFMD hospital joins,
EIN-anchored IRS Form 990 joins, SEC CIK/accession joins, source URLs, issuer
or company names, states, and user-supplied document names. Financial-
intelligence public financial-health, uncompensated-care, charity-care, and
bad-debt profile tools also expose nested HCRIS, IRS Schedule H/Form 990, and
AHRQ HFMD source blocks with their own `evidence`, `source_metadata`, and
per-metric `metric_evidence` receipts, so report builders can cite the exact
source field, metric confidence, source period, and source domain behind each
metric instead of only the composite profile receipt. The task-specific
uncompensated-care, charity-care, and bad-debt profiles also promote selected
source receipts into top-level `metric_evidence.*` entries with
`promoted_metric_name` and `selected_source_metric` query fields for the profile
metric actually returned. Financial-
intelligence Form 990 organization rows, SEC filing rows, municipal bond rows,
and municipal disclosure document rows also carry row-level evidence receipts
so extracted report facts preserve exact EIN, CIK, accession number, source
URL, match basis, confidence, and caveat. Financial-intelligence zero-result
searches and source-unparsed SEC detail lookups keep
the same evidence and identity-map shape so report builders can cite the
searched public source, accession/EIN/query basis, and no-match or
unsupported-document status without treating it as proof of no filing or no
public debt disclosure. Claims-analytics inpatient, outpatient, trend,
case-mix, and market-volume responses expose public CMS PUF evidence receipts
and CCN-centered identity maps where provider identities participate in market
comparisons. DRG detail rows, APC detail rows, service-line summaries,
service-line/APC trend rows, case-mix contribution rows, provider market-share
rows, provider service-line breakdown rows, and service-line market totals also
carry row-level receipts so extracted claims facts preserve the CMS public PUF
dataset, CCN/provider set, service-line or code basis, source period, and
public-aggregate caveat. Workforce-analytics public operations
responses for HCRIS productivity, HCRIS/PBJ staffing, GME/teaching intensity,
bed-source resolution, ACGME program lookups, and public throughput/ED/procedure
profiles include top-level `source_metadata` paired with the canonical evidence
receipt plus an `identity_map` that preserves CCN, state facility ID,
facility name, state, year, facility type, program ID, occupation, area code,
discipline, and employer-name join boundaries where applicable.
Public throughput comparison rows keep their per-profile evidence receipt, and
ED/procedure focused views expose `source_profile_evidence` plus the relevant
`metric_confidence` and `metric_evidence` entries so extracted volume facts
retain the exact public field/source basis, derived-metric caveat, source
period, and parent identity context.
HRSA HPSA rows, ACGME program rows, NLRB election rows, BLS work-stoppage rows,
PBJ daily staffing rows, and HCRIS department staffing rows also carry row-level
evidence receipts so extracted workforce facts preserve the source row kind,
identifier basis, public source/cache class, confidence, and caveat.
Workforce-analytics BLS employment, HRSA HPSA, and NLRB/BLS labor activity
responses frame occupation, geography, discipline, employer name, and year as
source-scoped workforce search keys, not facility identity proof.
Workforce-analytics no-data responses for BLS, HCRIS GME/staffing,
and PBJ staffing expose receipts with the queried occupation, CCN, year,
facility type, and source/cache class. This lets report builders cite the
verified source scope of the no-match result without converting it into an
unsupported fact such as current affiliation, absence of facilities, absence of
quality measures, absence of ownership/control, absence of public financial
filings, or zero staffing/workforce activity. Public-record
cyber/breach responses for OCR breach history, OCR enforcement search, SEC
cyber disclosures, state AG breach notices, CISA KEV context, unsupported
attestation status, and combined incident profiles include top-level
`source_metadata` paired with the canonical evidence receipt plus an
`identity_map` that keeps entity name, state, CIK, accession number, source
URL, and source status joins source-scoped. Public-record cyber/breach no-data
responses also expose receipts for missing OCR breach caches, missing OCR
enforcement indexes, missing SEC user-agent configuration, missing state notice
imports, zero SEC/state/OCR search results, and empty combined cyber profiles;
their caveats explicitly prevent treating a no-hit result as proof of no breach,
incident, enforcement action, disclosure, state notice, cybersecurity issue, or
cybersecurity attestation. State AG breach notice receipts use the configured
state source URL when an exact state is known and a public state-AG locator when
the status or missing-import response spans multiple states. OCR breach rows,
OCR enforcement rows, SEC cyber disclosure rows, state breach notice rows, and
aggregated cyber incident rows
also carry row-level evidence receipts so report extraction preserves the
source URL, source type, incident type, affected-individual count when the
source provides it, match/confidence fields, match basis, confidence, and
no-assurance caveat for each public record. Their source claims expose
`row_evidence_paths` for `breaches[].evidence`, `records[].evidence`, and
`incidents[].evidence` so report builders can cite the exact row receipt.
Public-record PHC4 report search
and report-profile tools include PHC4 public-report receipts plus an
`identity_map` for hospital
name/query, report type, year/fiscal year, procedure, and source URL. Those
outputs are public-report context only; they are not paid PHC4 discharge
datasets and should not be substituted for exact CMS quality, cost-report, or
enrollment facts. Public-record CMS Provider of Services accreditation and CMS
Promoting Interoperability outputs expose source/cache metadata, evidence
receipts, row-level receipts for returned POS/PI records, and facility identity
maps keyed by CCN with name/state as candidate filters; the PI response and row
receipts explicitly prevent treating EHR/CEHRT fields as broad cybersecurity
attestation. Public-record LEIE and SAM.gov Exclusions metadata
tools also expose standard evidence receipts so cache/API readiness checks can
be preserved in screening reports without implying a screening result.
Public-record USAspending and SAM.gov Opportunities
searches expose API/cache metadata, evidence receipts, and candidate
organization identity maps; their receipts distinguish federal awards or
procurement notices from exclusions, enrollment, ownership, or facility facts.
Returned award and opportunity rows also carry row-level evidence receipts with
the row award ID, notice ID, solicitation number, parent query, match basis,
confidence, and source caveat so extracted report facts do not lose the
API/source scope.
Price-transparency MRF discovery, negotiated-rate, rate-dispersion,
system-comparison, and Medicare benchmark responses expose source metadata,
canonical evidence receipts, and facility identity fields where CCNs or hospital
IDs participate. Negotiated-rate rows, rate-dispersion rows, system-comparison
hospital/rate rows, and benchmark CPT rows also carry row-level evidence
receipts so extracted price facts preserve the hospital ID, CPT/HCPCS code,
payer/plan or benchmark basis, Medicare locality when used, MRF cache status,
and caveat that MRF rates are not patient out-of-pocket costs or complete payer
contracts.
Web-intelligence responses for Google CSE search,
direct public page fetches, system profile scraping, CMS PI/public-web EHR
detection, executive profiles, news monitoring, and GPO detection include
top-level `source_metadata` paired with the canonical evidence receipt plus an
`identity_map` that keeps organization name, CCN, domain, query text, source
URLs, and result URLs as OSINT source-boundary fields. Search result rows,
system-profile location rows, executive rows, news items, and GPO match rows
also carry row-level evidence receipts so report extraction can preserve the
exact source URL and candidate match basis after a row is separated from the
parent response. Cached web-intelligence responses are rewrapped with
dataset-specific receipts rather than generic web provenance, and cached
executive/news rows are backfilled with row-level receipts before report use.
Web-intelligence no-data and not-evaluated responses expose receipts for Google CSE missing
configuration/quota failures, rejected non-public fetch URLs, failed page
fetches, zero public web/news/executive/GPO search results, and missing bundled
GPO directory data; those receipts and identity maps frame web output as OSINT
leads rather than proof of current affiliation, leadership, vendor, GPO
relationship, web presence, or news absence.

## Identity Map Contract

Healthcare workflows should also expose or preserve a normalized identity object
from `shared.utils.healthcare_identity` when a result participates in
cross-server analysis. The identity map carries canonical public identifiers
and source-specific aliases:

- `ccn`, `npi`, `pecos_enrollment_id`, `ahrq_system_id`, and `owner_id`
- normalized `canonical_name`, `address`, and `zip_code`
- `aliases`, `match_decisions`, `conflicts`, and `unresolved_identifiers`

`hc-mcp workflow <name> --json` returns an `identity_map` plan with join keys,
source-claim extraction steps, exact/candidate match policy, and conflict rules.
Report builders should keep `identity_fields`, `identity_path`, and
`identity_map_path` from workflow fact rows next to each tool result's
`evidence` receipt. Workflow fact rows also expose `evidence_path` and
`source_metadata_path` so a report builder can copy the exact receipt from the
owning tool result rather than guessing whether a fact is result-level or
row-level. Workflow report contracts require both `identity_path` and
`identity_map_path`; the planner validates those paths against the owning
step's advertised `identity_contract.output_paths` before the workflow is
reported as contract-valid.
Where a source claim produces reportable row or metric collections, its identity
map should also expose `row_evidence_paths` or `metric_evidence_paths` such as
`enrollments[].evidence`, `owner_network.nodes[].evidence`, `results[].evidence`,
`items[].evidence`, or `metric_evidence.*` so report builders can cite the row
or metric receipt after extracting the value from its parent response.

See [Source Capability Ledger](SOURCE_CAPABILITY_LEDGER.md) for the maintained
source-boundary contract covering exact, adjacent, unsupported, import-required,
and screening-only result classes.

Workflow plans also include `report_ingest_contract.fact_rows`. These rows
already satisfy the required source metadata field names so report builders can
validate the shape before execution, but they contain
`copy_from_tool_evidence.*` placeholders. Replace those placeholders with the
owning tool result's `evidence_path` receipt before treating a row as a cited
fact, and preserve the row's `identity_path` and `identity_map_path` alongside
the cited value. Use
`validate_report_ingest_payload(payload, require_content=True, allow_placeholders=False, require_identity_context=True)`
for final workflow-derived report rows after tool execution; this rejects
workflow placeholders, applies the report-ready evidence content checks, and
verifies each fact row retained identity fields plus either identity objects or
the planner identity paths. The same call is exposed in workflow JSON at
`report_ingest_contract.validation_modes.final_report.python_call`, while the
template mode remains available at `report_ingest_contract.validation_modes.template`.
Each workflow step also includes an `evidence_contract` with
`result_evidence_path`, `row_evidence_paths`, and the required canonical
receipt fields. `workflow_contract_validation` checks fact-row paths against
the owning step, so stale report paths fail before the workflow is presented as
contract-valid.

## Search Tool Migration

Before:

```python
@mcp.tool()
async def search_facilities(state: str | None = None, limit: int = 50) -> str:
    facilities = [_row_to_facility(row).model_dump() for _, row in results.iterrows()]
    return json.dumps({"count": len(facilities), "results": facilities})
```

After:

```python
from typing import Any

from shared.utils.mcp_response import collection_response, raise_invalid_params


@mcp.tool(structured_output=True)
async def search_facilities(state: str | None = None, limit: int = 50) -> dict[str, Any]:
    if limit < 1:
        raise_invalid_params("limit must be positive", detail={"limit": limit})

    facilities = [_row_to_facility(row) for _, row in results.iterrows()]
    return collection_response(facilities, limit=limit, offset=0)
```

## Record Tool Migration

Before:

```python
@mcp.tool()
async def get_facility(ccn: str) -> str:
    if matches.empty:
        return json.dumps({"error": f"No facility found with CCN: {ccn}"})

    return json.dumps(_row_to_facility(matches.iloc[0]).model_dump())
```

After:

```python
from typing import Any

from shared.utils.mcp_response import raise_not_found, record_response


@mcp.tool(structured_output=True)
async def get_facility(ccn: str) -> dict[str, Any]:
    if matches.empty:
        raise_not_found(f"No facility found with CCN: {ccn}", detail={"ccn": ccn})

    return record_response(_row_to_facility(matches.iloc[0]), key="facility")
```

## Recoverable Failure Migration

Use a raised `ToolError` when the tool cannot satisfy the request because inputs
are invalid, an expected record is absent, or execution failed. Use
`error_response(...)` only when a failure is part of a normal response contract
and clients should continue processing the response.

```python
from typing import Any

from shared.utils.mcp_response import error_response


@mcp.tool(structured_output=True)
async def optional_upstream_status() -> dict[str, Any]:
    if upstream_disabled:
        return error_response(
            "Optional upstream is disabled",
            code="upstream_disabled",
            retryable=False,
        )

    return {"ok": True, "status": "available"}
```

## Migration Notes

1. Change tool return annotations from `str` to `dict[str, Any]` or a concrete
   Pydantic response model.
2. Add `structured_output=True` to `@mcp.tool(...)` when you want FastMCP to
   require a structured-compatible return annotation.
3. Replace `json.dumps({"count": len(results), "results": results})` with
   `collection_response(results, ...)`.
4. Replace record-level JSON strings with `record_response(...)` or a concrete
   Pydantic response model.
5. Replace error JSON payloads for invalid inputs and runtime failures with
   `raise_invalid_params`, `raise_not_found`, or `raise_tool_error`.
