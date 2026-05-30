# Task-First Workflows

These workflows are public-data workflows for operators, analysts, and agent
users. They do not handle PHI, do not establish HIPAA readiness, and do not
replace source-specific verification steps.

Run readiness first:

```bash
hc-mcp doctor
```

List and inspect executable workflow plans:

```bash
hc-mcp workflow
hc-mcp workflow compliance_exclusion_screening
hc-mcp workflow hospital_competitive_profile --json
hc-mcp workflow quality_measure_lookup --input ccn=390223 --inputs-json '{"measure":"clabsi_sir"}' --json
```

The bare `hc-mcp workflow` list is task-first: each row includes required
identifiers, required public sources, recommended servers, tool-step count,
report-row count, and validation status before you open a full plan.

Workflow CLI inputs are read-only planner inputs. Use repeatable
`--input key=value` for quick identifiers or `--inputs-json '{...}'` for a
template object. When both are present, explicit `--input` values override the
JSON object.

List curated install/use presets:

```bash
hc-mcp preset
hc-mcp preset compliance
hc-mcp preset market-strategy --json
```

The same workflows and curated presets are exposed by the `discovery` MCP
server through `list_workflows`, `get_workflow_plan`, `list_presets`, and
`get_preset_plan`. Plans are read-only: they report required identifiers,
recommended tool order, source caveats, cache/API-key readiness, identity-map
starter fields, report-ready fact row templates, and a
machine-checkable `report_ingest_contract` with template and final-report
validation modes.

Preset plans are task bundles, not just server lists. `hc-mcp preset <id>
--json` and `discovery.get_preset_plan` include `workflow_summaries` for each
preset workflow: required identifiers, required sources, `source_resolution`,
recommended servers, examples, validation status, step count, report-row
count, and the exact `hc-mcp workflow <workflow_id> --json` plan command.
Use these summaries to choose the workflow before opening the full plan; use
the full workflow plan for step-level cache checks, identity-map actions,
tool calls, and report fact-row paths.

Text output from `hc-mcp workflow <workflow_id>` also includes a workflow-scope
summary before the tool sequence: required identifiers, required public
sources, recommended servers, and planner validation status for tool
references and report contracts.
Every workflow plan also includes an `examples` block with public-data or
valid-format planner inputs, a direct CLI command, a JSON CLI command, and the
equivalent `discovery.get_workflow_plan` MCP tool call. Client UIs and agents
should use this block for runnable examples instead of copying examples from
prose docs. The example inputs are tested against the planner's required
identifier checks so every workflow example clears `readiness.missing_inputs`.
Every plan also includes `tool_reference_validation`, a registry-backed AST
check proving that each step names a real top-level tool function in the
registered server module without importing servers or requiring API keys.
Plans also include `workflow_contract_validation`, which checks that every
report-ready fact-row template points at a tool in the same workflow and only
uses declared identity join keys plus standard alias fields. It also checks
that each fact row's `evidence_path`, `identity_path`, `identity_map_path`,
and `source_metadata_path` reference the same workflow step as `value_path`,
that `evidence_path` is advertised by that step's `evidence_contract`, and
that identity paths are advertised by `identity_contract.output_paths`.
Every step carries the corresponding `tool_reference` status plus an executable
call descriptor:

```json
{
  "stdio_command": "hc-mcp public-records",
  "tool_reference": {
    "status": "ok",
    "module": "servers.public_records.server"
  },
  "mcp_call": {
    "server": "public-records",
    "tool": "check_leie_npi",
    "qualified_tool": "public-records.check_leie_npi",
    "arguments_template": {
      "npi": "<npi>"
    }
  }
}
```

`required_sources` names the public source dependencies for the workflow. A
source entry can be a canonical registry dataset ID, such as
`cms_hospital_quality`, or a planner alias used when the task spans several
source-specific datasets. The plan's top-level `source_resolution` array
resolves every entry before execution: registry dataset IDs are marked
`registry_dataset`, while aliases are marked `workflow_alias` and list the
canonical dataset IDs, source type, and caveat that should travel with report
rows.

Common workflow aliases include:

| Alias | Resolves to | Caveat |
|---|---|---|
| `public_web` | `web_intelligence` | Public website facts are context and alias evidence, not authoritative enrollment, ownership, or quality facts. |
| `public_financial_health` | `ahrq_hfmd`, `cms_cost_report`, `nj_hospital_public_data`, `state_health_data` | Financial metrics need the source-period and metric-level receipt that produced the value. |
| `routing` | `cms_hospital_general_info` | Routing/access calculations are modeled context and must keep configured service assumptions separate from source facts. |
| `nppes` | `nppes_registry` | NPI/name/address joins are source-scoped unless an exact identifier supports the merge. |

Agents and report builders should preserve `source_resolution` with the
workflow scope and consult each step's `source_resolution` before treating a
required source as executed evidence. Alias resolution explains which source
families are expected; it does not authorize substituting adjacent public
records for exact source-backed facts.

`arguments_template` preserves placeholders for optional alternatives an agent
may choose to fill. Use `mcp_call.resolved_arguments` when a client needs the
directly callable subset of arguments with placeholders removed. Step
`execution_readiness.missing_inputs` reports blocking tool inputs that remain
unresolved after applying the planner inputs.

Plans also include registry-backed execution readiness. Workflow-level
`readiness.missing_required_env` blocks execution when a required key such as
`SEC_USER_AGENT` is absent. Optional or source-conditional steps are reported
without blocking the whole plan:

```json
{
  "execution_readiness": {
    "status": "optional_unavailable",
    "blocking": false,
    "required_env": ["SAM_GOV_API_KEY"],
    "missing_env": ["SAM_GOV_API_KEY"],
    "source_checks": [
      {"dataset_id": "sam_gov_exclusions", "status": "not_checked"}
    ],
    "notes": ["Optional federal exclusions screen; unavailable without SAM_GOV_API_KEY."]
  }
}
```

Every step also carries an `identity_contract` so agents can preserve the
identity spine while moving between servers:

```json
{
  "identity_contract": {
    "consumes": ["ccn"],
    "produces": ["ccn", "measure_id"],
    "output_paths": ["result.identity", "result.records[].identity"],
    "match_policy": "exact_identifier_required_for_report_fact",
    "evidence_required": true,
    "preserve_with_fact_rows": ["ccn", "measure_id"]
  }
}
```

Presets group registry-backed servers and workflows for common job families:
`compliance`, `market-strategy`, `research`, and `metadata-only`.

Each workflow plan now includes an `identity_map` section. It lists the join
keys the workflow expects, which tools consume each key, source-claim extraction
steps, exact-vs-candidate match policy, unresolved identifiers, and conflict
handling rules. The `identity_map.resolution_plan` gives the ordered merge
action for each tool: merge only on exact identifiers, preserve aliases as
candidates requiring source review, or keep context without merging. Use this
section as the handoff contract between facility profile, ownership trace,
quality, finance, workforce, system reconciliation, and compliance screening
steps.

Workflow plans also expose `identity_map.merge_policy`, which points agents to
the shared conservative helper
`shared.utils.healthcare_identity.merge_healthcare_identities`. That helper
merges CCN, NPI, PECOS enrollment IDs, AHRQ system IDs, and owner IDs only when
they are non-conflicting; names, addresses, ZIPs, and state values remain alias
or conflict context unless an exact identifier supports the join.

The machine-checkable `report_ingest_contract.fact_rows` also names
`identity_path` for the owning tool's normalized identity object and requires
`identity_map_path` for the owning tool's richer source-boundary map. It also
names `evidence_path` and `source_metadata_path` so report builders can copy
the exact result-level or row-level receipt that supports the cited value.
Those paths are validated against the owning workflow step before the plan is
reported as contract-valid: evidence paths must be advertised by the step's
`evidence_contract`, and identity paths must be advertised by the step's
`identity_contract.output_paths`.
Report builders should preserve both `identity_path` and `identity_map_path`
with every cited workflow fact row so cross-server joins retain source-claim,
merge-policy, missing-data, and conflict-policy context.
For finance and public-throughput workflows, metric-level facts should cite
the tool's `metric_evidence` entry instead of only the composite profile
receipt. The executable `finance_profile` plan includes report-row templates
for `financial_intelligence.get_public_financial_health_profile.*.metric_evidence`
and `workforce_analytics.get_public_throughput_profile.metric_evidence` so
source field, metric confidence, period, caveat, and identity context stay
attached to extracted metric values.
Facility and ownership workflows cite
`health_system_profiler.get_system_facilities.inpatient_facilities[].evidence`
for system-affiliation fact rows, and `system_reconciliation` cites
`web_intelligence.scrape_system_profile.locations[].evidence` for public-web
alias context so report builders preserve row-level receipts instead of only
the composite profile receipt.
The executable `quality_profile` plan also cites HRRP
`get_readmission_data.conditions[].evidence`, HAC
`get_safety_scores.domain_evidence[].evidence`, HCAHPS
`get_patient_experience.domains[].evidence`, and exact CMS measure
`get_quality_measure_rows.rows[].evidence` paths for report-ready quality
facts.

## Compliance And Exclusion Screening

Use this when an agent needs to screen a provider, owner, vendor, or organization
against public exclusion sources and preserve source-backed receipts.

Known public test inputs:

| Input | Purpose |
|---|---|
| `npi=1234567893` | Valid-format NPI for tool-shape testing; not a recommended real-world clearance example. |
| `entity_name=Thomas Jefferson University Hospitals` | Public hospital entity name for name-search workflow tests. |
| `ccn=390223` | Thomas Jefferson University Hospital public CMS CCN used in examples. |

Local stdio:

```bash
hc-mcp public-records
```

Recommended agent steps:

1. Call `public_records.get_leie_metadata` and confirm `cache_status` is fresh or acceptable for the task.
2. Call `public_records.check_leie_npi` for exact NPI screening when an NPI is available.
3. Call `public_records.search_leie_entity` or `search_leie_individual` only as a name-based potential-match screen.
4. Call `public_records.search_sam_exclusions` or `check_sam_exclusion_identifier` when `SAM_GOV_API_KEY` is configured.
5. Preserve each tool's `evidence` receipt, `source_metadata`, match basis, confidence, and verification caveat in the final report.
6. Preserve each screening result's `identity` object and `identity_map` when present with each report fact row.
7. Preserve the workflow `identity_map` join keys with each report fact row, especially `npi`, `ccn`, `entity_name`, `state`, SAM.gov `uei`/`cage_code`, PECOS enrollment IDs, and owner associate IDs.

The executable workflow includes separate report fact-row templates for exact
LEIE NPI screening, LEIE entity-name potential matches, SAM.gov exclusion
records, and PECOS enrollment join keys. PECOS join-key rows cite
`provider_enrollment.search_provider_enrollment.enrollments[].evidence` and
preserve `provider_enrollment.search_provider_enrollment.identity_map` so NPI,
CCN, and enrollment identifiers remain source-backed. Entity-only screening
must cite the name-search receipt and keep state/UEI/CAGE values source-scoped;
do not reuse an exact-NPI fact row for a name-only result.

The corresponding planner step exposes the concrete MCP call:

```json
{
  "server": "public-records",
  "tool": "check_leie_npi",
  "qualified_tool": "public-records.check_leie_npi",
  "arguments_template": {
    "npi": "1234567893"
  }
}
```

Expected output shape:

```json
{
  "status": "no_current_leie_match_found",
  "total_results": 0,
  "source_metadata": {
    "source_name": "HHS OIG LEIE",
    "cache_status": "fresh"
  },
  "evidence": {
    "dataset_id": "hhs_oig_leie",
    "match_basis": "npi_exact_no_current_match",
    "confidence": "high_identifier_no_match_in_current_file",
    "caveat": "HHS OIG LEIE downloadable data is a screening source..."
  },
  "identity": {
    "canonical_name": "",
    "npi": "1234567893",
    "match_decisions": [
      {
        "basis": "npi_exact_no_current_match",
        "confidence": "high_identifier_no_match_in_current_file"
      }
    ]
  }
}
```

Decision caveat:

- A zero-result LEIE or SAM.gov response is not legal clearance.
- Name matches are potential matches until verified through the official source
  and documented follow-up process.
- Do not submit SSN, EIN, TIN, or other sensitive tax identifiers to these tools.

## Other Canonical Workflow Presets

The canonical workflow registry is rendered from
`shared.utils.server_registry.WORKFLOW_PRESETS`.

| Workflow | Primary servers |
|---|---|
| `compliance_exclusion_screening` | `public-records`, `provider-enrollment`, `live-gateway` |
| `facility_profile` | `cms-facility`, `health-system-profiler`, `service-area`, `workforce-analytics` |
| `quality_profile` | `hospital-quality`, `cms-facility` |
| `finance_profile` | `financial-intelligence`, `hospital-quality`, `workforce-analytics` |
| `hospital_competitive_profile` | `cms-facility`, `health-system-profiler`, `hospital-quality`, `financial-intelligence`, `workforce-analytics`, `claims-analytics` |
| `ownership_chow_trace` | `provider-enrollment`, `cms-facility`, `health-system-profiler`, `public-records` |
| `market_community_health_scan` | `geo-demographics`, `community-health`, `service-area`, `drive-time` |
| `quality_measure_lookup` | `hospital-quality`, `discovery` |
| `research_trials_activity_profile` | `research-trials` |
| `referral_leakage_readiness` | `physician-referral-network`, `claims-analytics`, `drive-time` |
| `system_reconciliation` | `health-system-profiler`, `cms-facility`, `provider-enrollment`, `web-intelligence` |

Use `hc-mcp doctor --json` to inspect machine-readable workflow readiness and
missing requirements before handing a workflow to an agent. Doctor uses the
same workflow planner contract, so each workflow entry includes missing
inputs, missing required environment variables, cache/source checks, optional
unavailable steps, per-step readiness counts, and the exact `hc-mcp workflow
<workflow_id> --json` command to inspect the full plan.
Use `hc-mcp doctor --check --json` when a runbook or release gate should exit
non-zero unless all doctor readiness checks are ready.
Doctor also reports the workflow planner's static validation gates for report
contracts and tool references so adoption drift is visible before a workflow
is handed to an agent.
Doctor also reports priority evidence-contract readiness for hospital-quality,
provider-enrollment, health-system-profiler, financial-intelligence,
workforce-analytics, public-record cyber/breach, web-intelligence,
research-trials, community-health, claims-analytics, and physician-referral
surfaces. It also checks the facility/geography/routing/service-area,
price-transparency, PHC4, and federal/regulatory public-record receipt
contracts that feed workflow context.
In source checkouts, doctor also compares registry-rendered artifacts such as
Compose files, client configs, the MCPB/Desktop Extension manifest, `.env.example`, and registry-backed docs tables
against their renderers so setup drift is visible from the same read-only
readiness command.
Workflow tool references are validated against registry modules and public tool
signatures, so `arguments_template` keys are intended to be directly callable
MCP arguments rather than loose aliases. Workflow report contracts are also
validated so nested report facts cannot drift back to result-level receipts
when the owning tool advertises a matching row-level evidence path.

`ownership_chow_trace` resolves CMS facility identity first, adds optional AHRQ
system-affiliation context only when an exact `system_id` is supplied, fetches
PECOS owner/CHOW rows, and optionally screens the owner/entity name against
SAM.gov Exclusions when `SAM_GOV_API_KEY` is configured. It also includes the
non-blocking `provider_enrollment.profile_provider_control` composite profile
for consolidated enrollment, ownership, CHOW, and owner-network receipts; use
the atomic owner and CHOW calls for exact source-backed assertions. Treat AHRQ
affiliation and SAM no-results as context, not ownership proof or legal
clearance.

`market_community_health_scan` keeps geography and facility identity separate:
CDC PLACES market aggregates cite
`community_health.get_market_community_profile.market_profile.aggregated_measures[].evidence`,
ZCTA topology cites `geo_demographics.get_zcta_adjacency.adjacent_zcta_rows[].evidence`,
CMS HSAF/service-area facts remain CCN-scoped, and drive-time access facts cite
`drive_time.compute_accessibility_score.results[].evidence` for each modeled
demand-point score. Adjacent ZCTAs are topology context only; do not treat them
as service-area membership, patient-flow evidence, or network adequacy without a
source-backed service-area, utilization, or access row.
Planner aliases map single `zcta` inputs into the PLACES `zctas` argument and
single `ccn` inputs into claims `provider_ccns` when those tool signatures
expect arrays.

`finance_profile` separates composite profile evidence from metric receipts:
public financial-health source metrics cite source-block `metric_evidence`
from HCRIS, IRS Schedule H/Form 990, or AHRQ HFMD, while public throughput
denominators cite `workforce_analytics.get_public_throughput_profile.metric_evidence`.
Task-specific uncompensated-care, charity-care, and bad-debt tools also expose
top-level `metric_evidence.*` receipts for the promoted profile metrics, and
the executable finance workflow includes non-blocking steps plus report fact
rows for those promoted receipts.
Do not cite a derived rate or financial metric without copying its metric
receipt and preserving the workflow identity map.

`hospital_competitive_profile` uses the same row-level convention for
system and workforce context: system affiliation facts cite
`health_system_profiler.get_system_facilities.inpatient_facilities[].evidence`,
and staffing facts cite
`workforce_analytics.get_hospital_staffing_productivity.departments[].evidence`
with the workforce identity map preserved.

`research_trials_activity_profile` maps a single organization-style input into
the actual research-trials tool arguments: `org_name` for NIH RePORTER funding,
`sponsor` for ClinicalTrials.gov sponsor inventory, and `location` for
ClinicalTrials.gov site inventory. Its report fact rows cite
`profile_research_funding.projects[].evidence`,
`inventory_clinical_trial_sponsors.records[].evidence`, and
`inventory_clinical_trial_sites.records[].evidence` so project, sponsor, and
site facts stay tied to source rows.

## Identity Map Contract

Workflow plans are not identity resolution engines by themselves. They define
the expected identity spine for the job so an agent can preserve source-backed
facts without merging adjacent records.

Key rules:

- Use exact public identifiers first: CCN for facilities, NPI for providers, PECOS enrollment IDs for enrollment rows, owner associate IDs for ownership, AHRQ system IDs for system affiliation, and measure IDs for CMS quality rows.
- Treat names, addresses, ZIPs, sponsor names, owner names, and market labels as aliases or scope fields unless an exact identifier also supports the join.
- Follow each step's `identity_map.resolution_plan.merge_action`; web aliases and name-only matches are candidates, while CCN/NPI/PECOS/AHRQ identifiers can support exact merges.
- Use `identity_map.merge_policy.helper` for cross-server report builders that need to merge normalized tool identities; exact identifier disagreements must become conflicts, not overwritten values.
- Carry conflicts forward in `identity_map.conflict_policy`; do not overwrite a canonical identifier because another public source uses a different name.
- Every report-ready fact row should retain its `identity_fields`, the originating tool `evidence` receipt, and the tool `identity_map` from `identity_map_path`.
- `report_ingest_contract.fact_rows` are templates, not executed source facts.
  Replace each `copy_from_tool_evidence.*` placeholder with the owning tool's
  receipt from `evidence_path` before citing the row in a report, then preserve
  both `identity_map_path` and `identity_path` next to the cited value.
  Final report rows should pass
  `validate_report_ingest_payload(payload, require_content=True, allow_placeholders=False, require_identity_context=True)`.
