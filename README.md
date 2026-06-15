# Healthcare Data MCP

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![MCP](https://img.shields.io/badge/MCP-stdio%20%7C%20streamable--http-0f766e)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Public healthcare market intelligence for AI agents: 20 local MCP servers covering hospitals, ownership, quality, claims, price transparency, workforce, finance, public state-health reporting, community health, research activity, web intelligence, federal exclusion screening, and local-safe cache management.

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
hc-mcp --version
hc-mcp-setup --interactive
hc-mcp doctor
hc-mcp --list
```

Prefer an installer script?

```bash
curl -fsSL https://raw.githubusercontent.com/ajhcs/healthcare-data-mcp/main/install.sh | bash
```

## TL;DR

Healthcare data is public, but it is scattered across CMS, CDC, NIH, HHS OIG, SAM.gov, Census, HUD, SEC, IRS, hospital MRFs, and other systems. Healthcare Data MCP turns those sources into focused MCP tools with local caches, structured responses, source metadata, recovery hints, per-server discovery resources, and client-friendly server discovery.

Latest release: `v0.4.0` adds source-disciplined AHRQ Compendium 2023 health-system metrics, including hospital counts, bed counts, physician counts, hospital-level bed/address/type details, snapshot metadata, coverage summaries, cursor pagination, and explicit current-CMS overlay candidates. See [v0.4.0 release notes](docs/release-notes/2026-06-15-v0.4.0-health-system-metrics.md).

| If your agent needs to... | Use these servers |
| --- | --- |
| Find facilities, ownership, CHOW history, quality, claims, service areas, or pricing | `cms-facility`, `provider-enrollment`, `hospital-quality`, `claims-analytics`, `service-area`, `price-transparency` |
| Understand a market, community, workforce, or access footprint | `geo-demographics`, `community-health`, `workforce-analytics`, `drive-time` |
| Research health systems, referrals, finance, trials, publications, or web presence | `health-system-profiler`, `physician-referral-network`, `financial-intelligence`, `research-trials`, `web-intelligence` |
| Screen organizations against public compliance signals | `public-records` |
| Let remote MCP clients search dataset metadata safely | `gateway`, `discovery` |
| Give one authenticated local/remote endpoint access to approved live tools | `live-gateway` |

## Quick Example

```bash
# Install locally and configure optional API keys
python -m pip install -e ".[dev]"
hc-mcp-setup --interactive

# See every available server and its default HTTP port
hc-mcp --list

# Check install, importability, API keys, cache status, ports, and workflows
hc-mcp doctor
hc-mcp doctor --check --json

# Show task-first plans an operator or agent can run
hc-mcp workflow
hc-mcp workflow compliance_exclusion_screening
hc-mcp workflow quality_measure_lookup --input ccn=390223 --inputs-json '{"measure":"clabsi_sir"}' --json
hc-mcp workflow system_reconciliation --input query="Jefferson Health" --input system_slug=jefferson-health --json
hc-mcp workflow profile_evidence_pack --input state=PA --input system_name="Jefferson Health" --json

# Connect to discovery to expose read-only macro tools for common workflows
hc-mcp discovery
# MCP tools: macro_quality_measure_lookup, macro_compliance_exclusion_screening,
# and macro_facility_profile_readiness return bounded workflow plans.

# Show curated install/use presets by job family
hc-mcp preset
hc-mcp preset market-strategy

# Run one server over stdio for Claude Desktop, Claude Code, Codex, or another local MCP client
hc-mcp public-records

# Run the same server over local Streamable HTTP
hc-mcp public-records --transport streamable-http --port 8013

# Start all HTTP servers in containers
docker compose up --build
```

Workflow plans are read-only and include concrete MCP call templates,
registry-backed source/API-key readiness, identity-map handoff rules, caveats,
and report fact-row templates.

The `v0.3.0` MCP UX release also makes individual servers self-describing:
each server exposes standard resources for capabilities, datasets, examples,
identity rules, and non-secret local metrics. Unknown IDs and malformed exact
identifier inputs now return machine-parseable recovery payloads where the
surface has been migrated.

## Preset Catalog

Curated presets group registry-backed servers and workflows for common operator
jobs. They are inspectable with `hc-mcp preset <preset-id> --json` and are
used by setup, docs, and distribution checks.

| Preset | Servers | Workflows |
| --- | ---: | --- |
| `compliance` | 4 | `compliance_exclusion_screening`, `ownership_chow_trace` |
| `market-strategy` | 10 | `facility_profile`, `quality_profile`, `finance_profile`, `hospital_competitive_profile`, `system_reconciliation`, `profile_evidence_pack`, `health_system_metrics`, `market_community_health_scan`, `referral_leakage_readiness` |
| `metadata-only` | 2 | `quality_measure_lookup` |
| `research` | 3 | `research_trials_activity_profile` |

## Why Use It?

| Capability | What you get | Example source families |
| --- | --- | --- |
| Agent-ready tools | Narrow MCP servers instead of one huge ambiguous tool surface | Facility lookup, LEIE screening, trial search, ownership graph expansion |
| Self-describing MCP UX | Tool docstrings include discovery, when-to-use, examples, do/don't guidance, and common mistakes | Every checked-in `@mcp.tool` surface |
| Structured responses | Bounded JSON-compatible results with provenance fields, error types, recovery hints, and suggested next calls where available | CMS, CDC, NIH, ClinicalTrials.gov, SAM.gov, HHS OIG |
| Per-server resources | Metadata resources expose capabilities, datasets, examples, identity rules, and local metrics even when a client connects to one server | `healthcare-data://server/{server}/capabilities` |
| Local-first operation | Stdio and localhost HTTP by default, with cache reuse across sessions | `~/.healthcare-data-mcp/cache` or Docker `healthcare-cache` |
| Remote-safe metadata mode | A gateway that exposes `search` and `fetch` for dataset metadata only | OpenAI and Claude remote MCP connector shapes |
| Authenticated live router | One allowlisted gateway for provider, quality, claims, compliance, PLACES, NIH, and ClinicalTrials tools | Local stdio, or Streamable HTTP behind bearer auth/HTTPS |
| Practical client packaging | Examples for Codex, Claude Code, Claude Desktop, generic MCP clients, Docker, and MCPB | `examples/`, `configs/`, `desktop-extension/` |

## Server Catalog

| Server | Port | Domain | Dataset IDs |
| --- | ---: | --- | --- |
| `service-area` | 8002 | CMS hospital service areas and market share | `cms_hospital_general_info`, `cms_hsaf`, `dartmouth_hsa_hrr` |
| `geo-demographics` | 8003 | Census, ZCTA, Medicare, and HUD geography | `census_acs`, `cms_geographic_variation` |
| `drive-time` | 8004 | Routing, drive-time matrices, and access scoring | `cms_hospital_general_info` |
| `hospital-quality` | 8005 | CMS quality, readmission, and safety data | `cms_cost_report`, `cms_hospital_general_info`, `cms_hospital_quality` |
| `cms-facility` | 8006 | CMS facility master data and NPPES lookup | `cms_hospital_general_info`, `cms_provider_of_services`, `nppes_registry` |
| `health-system-profiler` | 8007 | Health system discovery and facility enrichment | `ahrq_health_system_compendium`, `cms_hospital_general_info`, `cms_provider_of_services`, `cms_doctors_clinicians_national_downloadable_file`, `nppes_registry` |
| `financial-intelligence` | 8008 | IRS 990, SEC EDGAR, and nonprofit finance intelligence | `ahrq_hfmd`, `nj_hospital_public_data`, `state_health_data` |
| `price-transparency` | 8009 | Hospital MRF and benchmark pricing | `cms_price_transparency_mrf` |
| `physician-referral-network` | 8010 | NPPES, physician mix, referral network, and leakage analysis | `dartmouth_hsa_hrr`, `docgraph_referrals`, `nppes_registry`, `physician_compare_utilization` |
| `workforce-analytics` | 8011 | BLS and ACGME workforce analytics | `cms_cost_report`, `de_hospital_discharge`, `nj_hospital_public_data`, `pa_hospital_reports`, `state_health_data`, `workforce_labor` |
| `claims-analytics` | 8012 | DRG, service-line, and claims analytics | `cms_medicare_claims_pufs` |
| `public-records` | 8013 | SAM.gov, USAspending, CHPL, accreditation, and exclusion screening | `cms_provider_of_services`, `hhs_oig_leie`, `phc4_public_reports`, `public_records`, `sam_gov_exclusions`, `state_health_data` |
| `web-intelligence` | 8014 | Web search and health system OSINT | `web_intelligence` |
| `discovery` | 8015 | Dataset catalog resources, cache status, and prompts | `mcp_metadata_surfaces` |
| `gateway` | 8016 | Remote-safe metadata gateway with search/fetch | `mcp_metadata_surfaces` |
| `provider-enrollment` | 8017 | CMS PECOS-derived provider enrollment, ownership, and CHOW | `cms_pecos_hospital_chow`, `cms_pecos_hospital_enrollments`, `cms_pecos_hospital_owners`, `cms_pecos_public_provider_enrollment`, `cms_pecos_snf_chow`, `cms_pecos_snf_enrollments`, `cms_pecos_snf_owners` |
| `community-health` | 8018 | CDC PLACES community-health estimates for counties, places, tracts, and ZCTAs | `cdc_places` |
| `research-trials` | 8019 | NIH RePORTER funding and ClinicalTrials.gov study activity | `clinicaltrials_gov`, `nih_reporter_projects` |
| `live-gateway` | 8020 | Authenticated live router for approved provider, quality, claims, compliance, community, and research tools | none |
| `cache-manager` | 8021 | Local-safe cache inspection, planning, validation, refresh, promotion, rollback, and lineage control plane | none |

## Installation

### Versioned Python Tools

For tagged releases, prefer an isolated tool install once packages are published:

```bash
pipx install healthcare-data-mcp
hc-mcp --version
hc-mcp doctor
# or for one-off execution with uv:
uvx --from healthcare-data-mcp hc-mcp doctor
```

Until PyPI publishing is enabled, install from a tagged Git URL:

```bash
pipx install git+https://github.com/ajhcs/healthcare-data-mcp@<tag>
```

`hc-mcp --version` should match the tagged release or container image label you
intend to run. After installing, use the read-only doctor first, then run the
setup wizard only when you are ready to write a local `.env` or acquire caches:

```bash
hc-mcp doctor
hc-mcp-setup --interactive
hc-mcp --list
```

### Local Python Development

Use an editable install when developing the package or validating unpublished
changes locally:

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
hc-mcp doctor
```

### Docker Compose

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
cp .env.example .env
docker compose up --build
```

Each server is exposed at `http://localhost:<port>/mcp`. Compose publishes ports on `127.0.0.1` by default.

Generated Compose files assign the local build a package-versioned image tag
such as `healthcare-data-mcp:0.4.0`. To run a published image instead of
building locally, set `HC_MCP_IMAGE` to the trusted image reference before
starting Compose; Compose uses `pull_policy: missing` so normal startup can
reuse or pull the tagged image unless you explicitly pass `--build`.

For release builds, tag images from the package version:

```bash
docker build $(python3 scripts/docker_image_tags.py --format docker-build-args --image ghcr.io/ajhcs/healthcare-data-mcp) .
```

Published tagged images and Compose defaults should use the same package
version shown by `hc-mcp doctor`.

### Universal Installer

```bash
curl -fsSL https://raw.githubusercontent.com/ajhcs/healthcare-data-mcp/main/install.sh | bash
```

The installer detects common MCP clients and can install via local Python or Docker.
Its server list, zero-config selection, port hints, and environment-key prompts
come from the canonical registry so installer behavior stays aligned with
`hc-mcp --list`, Compose files, client configs, and MCPB packaging.
Use `bash install.sh --dry-run --no-register` for a read-only preview of the
registry entry count, zero-config server IDs, and environment-key names; unknown
options fail before prerequisite checks or any write-capable setup path.

## MCP Client Setup

| Client/system | Recommended mode | Setup |
| --- | --- | --- |
| Codex CLI / Codex IDE / Codex App | Local stdio, or HTTP when Docker is running | `examples/codex-config.toml` or `codex mcp add ...` |
| Claude Code | Project HTTP via `.mcp.json`, or stdio for selected servers | `.mcp.json` or `claude mcp add ...` |
| Claude Desktop / shared desktops | Stdio JSON, Desktop Extension/MCPB, or local HTTP | `examples/claude-desktop-stdio.json`, `.mcp.json`, or `scripts/build_mcpb.py` |
| Generic MCP clients | Stdio command or local Streamable HTTP URL | `hc-mcp <server>` or `http://localhost:<port>/mcp` |
| OpenAI/Anthropic remote MCP metadata integrations | HTTPS metadata gateway | `hc-mcp gateway --transport streamable-http` behind HTTPS/auth |
| OpenAI/Anthropic remote MCP live integrations | HTTPS live gateway with auth | `hc-mcp live-gateway --transport streamable-http` behind HTTPS/auth |

Local stdio examples:

```bash
scripts/register-codex.sh --dry-run
codex mcp add cmsFacility -- hc-mcp cms-facility
codex mcp add publicRecords --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp public-records

claude mcp add provider-enrollment --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp provider-enrollment
```

`scripts/register-codex.sh --dry-run --http` previews registry-backed Docker/HTTP Codex registrations without writing client config.

Local HTTP examples:

```bash
docker compose up --build
# then point a client at http://localhost:8006/mcp, http://localhost:8013/mcp, etc.
```

More detail:

- [MCP client notes](docs/MCP_CLIENTS.md)
- [Client packaging and MCPB](docs/CLIENT_PACKAGING.md)
- [Remote gateway](docs/REMOTE_GATEWAY.md)
- [Structured MCP results](docs/STRUCTURED_RESULTS.md)
- [Task-first workflows](docs/TASK_WORKFLOWS.md)
- [Profile evidence pack workflow](docs/PROFILE_EVIDENCE_PACK.md)
- [Source capability ledger](docs/SOURCE_CAPABILITY_LEDGER.md)

## Configuration

Use the setup wizard instead of hand-editing secrets:

```bash
hc-mcp-setup --interactive
hc-mcp-setup --validate-only
hc-mcp-setup --print-client-snippets
hc-mcp-setup --cache-status
hc-mcp-setup --cache-guide
hc-mcp-setup --acquire-public-caches
hc-mcp-setup --agent-cache-instructions
```

`hc-mcp` loads `.env` from the current working directory before starting a server. For GUI clients launched from another directory, set `HC_MCP_ENV_FILE=/absolute/path/to/.env` or pass `--env-file /absolute/path/to/.env`.

| Variable | Used by | Required? |
| --- | --- | --- |
| `SAM_GOV_API_KEY` | SAM.gov Exclusions and opportunity tools in `public-records` | Required for SAM API tools |
| `SEC_USER_AGENT` | SEC EDGAR tools in `financial-intelligence` | Required for SEC tools |
| `CHPL_API_KEY` | ONC CHPL enrichment in `public-records` | Optional |
| `CENSUS_API_KEY` | Census-backed `geo-demographics` tools | Optional |
| `HUD_API_TOKEN` | HUD ZIP crosswalk tools | Optional |
| `ORS_API_KEY` | OpenRouteService isochrones in `drive-time` | Optional |
| `BLS_API_KEY` | Higher BLS API limits in `workforce-analytics` | Optional |
| `GOOGLE_CSE_API_KEY`, `GOOGLE_CSE_ID` | Google Custom Search in `web-intelligence` | Optional |
| `PROXYCURL_API_KEY` | Web intelligence enrichment | Optional |

No key is required for HHS OIG LEIE, CMS PECOS/provider enrollment, CDC PLACES, NIH RePORTER, or ClinicalTrials.gov.

Run `hc-mcp doctor` after setup. It is read-only and reports package/version, Python environment, server importability, port conflicts, key configuration, cache readiness, source freshness where known, client config hints, gateway posture, workflow readiness from the same planner used by `hc-mcp workflow <name> --json`, workflow planner validation status for report contracts/tool references, priority evidence-contract readiness for the major healthcare and workflow surfaces, registry-rendered artifact drift for checked-in Compose/env/MCPB/Desktop Extension/client config/docs-table surfaces, and distribution readiness for package metadata, console entry points, wheel module aliases, and versioned container metadata.
Use `hc-mcp doctor --check --json` when a release script or operator runbook should fail fast unless the readiness status is `ready`.

Some tools also depend on local cache files. The setup CLI fetches sources that expose a stable unauthenticated acquisition path and leaves the rest as explicit imports:

```bash
hc-mcp-setup --cache-status
hc-mcp-setup --acquire-public-caches
hc-mcp-setup --acquire-hipaa-breaches
hc-mcp-setup --acquire-phc4-public-reports
hc-mcp-setup --acquire-ahrq-hfmd
hc-mcp-setup --acquire-pa-hospital-reports
hc-mcp-setup --acquire-nj-hospital-public-data
hc-mcp-setup --acquire-de-hospital-discharge
hc-mcp-setup --cache-guide
hc-mcp-setup --import-breach-csv /path/to/hipaa_breaches.csv
hc-mcp-setup --import-state-breach-notices PA /path/to/state_notices.csv
hc-mcp-setup --import-docgraph-csv /path/to/docgraph_shared_patients.csv
# or, if already converted:
hc-mcp-setup --import-docgraph-parquet /path/to/shared_patients.parquet
python3 scripts/import_acgme_programs.py /path/to/acgme-program-search-export.csv
```

The default cache root is `~/.healthcare-data-mcp/cache`. The affected tools include `public_records.search_phc4_public_reports`, `public_records.get_breach_history`, `public_records.get_cyber_incident_profile`, `hospital_quality.get_quality_measure_rows`, `financial_intelligence.get_public_financial_health_profile`, `workforce_analytics.get_public_throughput_profile`, `workforce_analytics.compare_hospital_staffing_productivity`, `physician_referral_network.map_referral_network`, and `physician_referral_network.detect_leakage`.

`hc-mcp-setup --acquire-public-caches` fetches or indexes public caches with stable unauthenticated acquisition paths, including the national all-state hospital/county backbone (CMS Hospital General Information, HSAF, Dartmouth ZIP-HSA-HRR, and CMS Geographic Variation), OCR HIPAA breaches, PHC4 public reports, AHRQ HFMD when a direct public artifact is available, and PA/NJ/DE public hospital source indexes. PA/NJ/DE are state-specific hospital-report enhancements; they are not the product boundary for national hospital, state, or county coverage. DocGraph/CareSet shared-patient data is separately licensed and is import-only.

Exact-source tools do not substitute adjacent public records. Use `hospital_quality.get_quality_measure_rows` for exact CMS measure IDs such as `MORT_30_AMI`, `READM_30_HOSP_WIDE`, and `HAI_1_SIR`; use HRRP/HAC/PHC4 tools only as separate adjacent context. Use `workforce_analytics.get_acgme_source_status` before ACGME program ID lookup, `research_trials.inventory_clinical_trial_sponsors` or `inventory_clinical_trial_sites` for deduped ClinicalTrials.gov inventories, and `public_records.get_cyber_attestation_source_status` for unsupported broad cybersecurity-attestation claims. See `docs/SOURCE_CAPABILITY_LEDGER.md`.

## Command Reference

```bash
# List available servers
hc-mcp --list

# Read-only readiness check
hc-mcp doctor
hc-mcp doctor --json
hc-mcp doctor --check --json

# Start a server over stdio
hc-mcp cms-facility

# Start a server over local Streamable HTTP
hc-mcp cms-facility --transport streamable-http --port 8006

# Load secrets from a specific dotenv file
hc-mcp public-records --env-file /absolute/path/to/.env

# Configure and validate environment variables
hc-mcp-setup --interactive
hc-mcp-setup --validate-only
hc-mcp-setup --print-client-snippets
hc-mcp-setup --cache-status
hc-mcp-setup --acquire-public-caches
hc-mcp-setup --acquire-hipaa-breaches
hc-mcp-setup --acquire-phc4-public-reports
hc-mcp-setup --acquire-ahrq-hfmd
hc-mcp-setup --acquire-pa-hospital-reports
hc-mcp-setup --acquire-nj-hospital-public-data
hc-mcp-setup --acquire-de-hospital-discharge
hc-mcp-setup --cache-guide
hc-mcp-setup --agent-cache-instructions
hc-mcp-setup --import-breach-csv /path/to/hipaa_breaches.csv
hc-mcp-setup --import-docgraph-csv /path/to/docgraph_shared_patients.csv

# Build the Claude Desktop extension package
python3 scripts/build_mcpb.py --check
python3 scripts/build_mcpb.py
```

## Architecture

```text
MCP clients
  ├─ Claude Desktop / Claude Code / Codex / OpenCode / generic local clients
  ├─ stdio: hc-mcp <server>
  └─ HTTP:  http://localhost:<port>/mcp
          │
          ▼
Healthcare Data MCP launch layer
  ├─ canonical server registry in shared/utils/server_registry.py
  ├─ dotenv loading through HC_MCP_ENV_FILE or --env-file
  ├─ read-only readiness checks through hc-mcp doctor
  └─ stdio, SSE, or Streamable HTTP transport
          │
          ▼
Focused FastMCP servers
  ├─ structured_output=True tools
  ├─ agent-facing docstrings, capability clusters, and MCP resources
  ├─ structured errors with recovery hints and suggested calls
  ├─ local non-secret tool timing/result metrics
  ├─ shared HTTP retry/client helpers
  ├─ source catalog, evidence receipt, and identity-map helpers
  └─ bounded responses with source metadata and caveats
          │
          ▼
Public sources + local cache
  ├─ CMS, CDC, NIH, ClinicalTrials.gov
  ├─ HHS OIG LEIE, SAM.gov, USAspending, CHPL
  ├─ Census, HUD, BLS, SEC, IRS, OSRM
  └─ ~/.healthcare-data-mcp/cache or Docker healthcare-cache volume
```

## Comparison

| Option | Best for | Tradeoff |
| --- | --- | --- |
| Healthcare Data MCP | AI agents that need public healthcare intelligence as MCP tools | Alpha project; some datasets require first-run downloads or source-specific keys |
| Direct public APIs | Custom applications with narrow, known data needs | Every agent/client must learn each source schema, auth rule, pagination model, and caveat |
| Data warehouse or BI stack | Internal reporting on curated tables | Strong for dashboards, weaker for ad hoc agent workflows and source-aware discovery |
| One-off notebooks/scripts | Analyst experiments | Harder to share with MCP clients, cache consistently, or reuse across agents |

## MCP Design Guarantees

The tool surface is tested for agent usability, not only Python correctness:

| Contract | What is enforced |
| --- | --- |
| Tool documentation | Every `@mcp.tool` docstring includes discovery, when-to-use, parameters, returns, do/don't, examples, and common mistakes sections. |
| Error recovery | Shared errors include `error.type`, `recoverable`, `data.fix_hint`, `available_options`, and `suggested_tool_calls` where applicable. |
| Capability clusters | Broad servers publish small capability clusters so clients can choose the narrowest relevant group. |
| Resources | Every server registers standard metadata resources under `healthcare-data://server/{server_id}/...`. |
| Web safety | Public web fetches revalidate redirect targets and reject private, metadata, non-http(s), excessive, and oversized HTML targets. |
| Cache durability | Runtime public-source cache writes use atomic replacement helpers for bytes, JSON, CSV, and Parquet paths. |

## Cache and Compliance Notes

Dataset caches live under `~/.healthcare-data-mcp/cache` and the Docker `healthcare-cache` volume.

LEIE stores `public-records/leie_current.csv`, `leie_current.parquet`, and `leie_current.meta.json` with a 31-day freshness target and stale-cache fallback. LEIE and SAM.gov Exclusions responses are screening support only. Name matches are potential matches; do not treat a zero-result response as legal clearance.

The remote `gateway` is intentionally metadata-only. It exposes dataset search/fetch records for remote MCP clients and does not proxy live exclusion screening, provider-enrollment queries, PLACES queries, RePORTER, or ClinicalTrials.gov.

Use `live-gateway` when a client needs one allowlisted endpoint for live provider enrollment, quality, claims, LEIE/SAM, PLACES, NIH, or ClinicalTrials.gov calls. HTTP/SSE live-gateway deployments require bearer-token or token-hash configuration through `MCP_LIVE_GATEWAY_*` environment variables; local stdio use does not. Live-gateway calls preserve owning-tool evidence/source metadata, then add policy metadata with request/result bounds, per-tool scopes, rate-limit classes, source caveat classes, registry dataset IDs/cache needs/safety notes, provenance status, malformed or content-empty top-level/nested row evidence-receipt blocking, sensitive SSN/EIN/TIN-style argument-key rejection, non-secret audit events, optional JSONL audit retention through `MCP_LIVE_GATEWAY_AUDIT_LOG_PATH`, and wildcard network-bind guards. The live allowlist is validated against canonical registry `gateway_exposure="live"` metadata at startup. Batch exclusion screening requires `mcp:bulk` in addition to `mcp:read`; prefer `MCP_LIVE_GATEWAY_TOKEN_SCOPES=<sha256>=mcp:read+mcp:bulk` for selected bulk-screening tokens. HTTP/SSE startup still rejects global `mcp:bulk` unless `MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE=true`.

Docker Compose is local-first. Host ports bind to `127.0.0.1`; if you need remote access, keep `MCP_HOST=127.0.0.1` at the process level and put `gateway` or `live-gateway` behind a trusted HTTPS reverse proxy with auth. The generated `live-gateway` service also inherits registry-defined environment keys for every live-routed server, including `SEC_USER_AGENT`, `SAM_GOV_API_KEY`, optional BLS/CHPL keys, PLACES cache overrides, and ClinicalTrials.gov inventory limits.

## Development

```bash
python -m pip install -e ".[dev]"
ruff check .
pytest -q
pip-audit . --strict
git ls-files -z | xargs -0 detect-secrets-hook --baseline .secrets.baseline --exclude-files '(^\\.git/|^\\.venv/|^build/|^dist/|^\\.pytest_cache/|^\\.ruff_cache/|^\\.secrets\\.baseline$)'
python3 -m compileall -q servers shared scripts tests smoke_test.py
```

CI also treats product-readiness as a first-class gate: installer and Codex
registration dry-runs, registry-rendered artifact checks, `hc-mcp doctor --check`,
workflow/preset smoke commands, MCP protocol and Inspector smoke, MCPB skeleton
build, Python package metadata checks, dependency audit, secret scanning, and
Docker zero-config startup must stay green.

Manual live-data smoke tests are in `smoke_test.py`. They call public APIs and may require environment variables from `.env.example`; they are intentionally excluded from normal pytest discovery.

Registry-backed docs and distribution checks:

```bash
python scripts/render_registry_docs.py server-catalog
python scripts/render_registry_docs.py env-catalog
python scripts/render_registry_docs.py server-catalog --check
python scripts/render_registry_docs.py preset-catalog --check
python scripts/render_registry_docs.py workflow-catalog --check
python scripts/render_registry_docs.py env-catalog --check
python scripts/render_env_example.py --check
python scripts/render_compose.py full --check
python scripts/render_compose.py zero-config --check
python scripts/render_client_configs.py codex --check
python scripts/render_client_configs.py http-clients --check
python scripts/render_client_configs.py project-mcp --check
python scripts/render_client_configs.py claude-desktop-stdio --check
python scripts/render_client_configs.py claude-desktop --check
python -m build --sdist --wheel --outdir dist/python-package
python -m twine check dist/python-package/*
python -m pytest tests/test_distribution_artifacts.py -q  # includes wheel install + hc-mcp smoke
python scripts/docker_image_tags.py --format json --image ghcr.io/ajhcs/healthcare-data-mcp
```

```bash
HC_MCP_LIVE_EXPANSION=1 HC_MCP_LIVE_LEIE=1 SAM_GOV_API_KEY=... python smoke_test.py
```

## Maintainer Merge Flow

Do not push directly to `main`. For routine maintainer changes, commit locally and let the helper create the PR, push the branch, and queue squash auto-merge:

```bash
git checkout -b docs/readme-update
# edit files
git add README.md
git commit -m "Refresh README"
scripts/pr-merge.sh docs/readme-update "Refresh README"
```

The script requires a clean worktree and an authenticated `gh` CLI. It enables branch deletion after merge, so the normal happy path is one command after the commit.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| GUI client cannot find `hc-mcp` | Use the absolute path from `which hc-mcp`, or activate the project venv and reinstall with `python -m pip install -e .` |
| API-backed tool says a key is missing | Run `hc-mcp-setup --interactive`, then point the client at `.env` with `HC_MCP_ENV_FILE` |
| Docker port conflict | Check `docker-compose.yml` ports and run `ss -tlnp` before changing ports |
| Claude Code ignores project config | Approve the project `.mcp.json`, or add servers with `claude mcp add --scope local ...` |
| Codex does not see a server | Check `~/.codex/config.toml` or `.codex/config.toml`; Codex CLI and IDE share that config |
| Remote MCP client cannot use localhost | Deploy `gateway` or `live-gateway` behind HTTPS/auth; stdio and localhost HTTP are local-only |
| Need a protocol smoke check | Run `scripts/mcp_inspector_smoke.sh` for MCP Inspector, or use `python scripts/mcp_smoke.py --server discovery --expect-tool get_workflow_plan --call-tool get_workflow_plan --tool-args '{"workflow_id":"quality_measure_lookup","inputs":{"ccn":"390223","measure":"clabsi_sir"}}' --expect-structured-key workflow_id` to verify executable workflow plans |

## Limitations

- This is an alpha project. Source schemas, API behavior, and MCP client compatibility can change.
- Some source APIs require keys for reliable or full access.
- Large public datasets are cached locally; first use can take longer while files download and normalize.
- Public exclusion screening is not final SSN/EIN identity verification or legal clearance.
- The remote `gateway` is metadata-only. Live calls belong on the separate authenticated `live-gateway`.
- The MCPB Desktop Extension skeleton should be validated in Claude Desktop before broad distribution.

## FAQ

**Does this store PHI?**
No. The project is built around public datasets and local cache files. Do not put PHI into prompts, logs, config, or cache paths.

**Do I need API keys?**
Not for every server. Many tools work with public downloads or unauthenticated public APIs, but SAM.gov, SEC EDGAR, Census, HUD, ORS, BLS, Google CSE, CHPL, and Proxycurl features may need keys or contact metadata.

Registry-backed environment key catalog:

| Key | Required | Servers | Purpose |
| --- | ---: | --- | --- |
| `ACGME_PROGRAMS_CSV` | no | `workforce-analytics` | Optional normalized ACGME Program Search export path. |
| `BLS_API_KEY` | no | `workforce-analytics` | Optional BLS key for higher API limits. |
| `CENSUS_API_KEY` | no | `geo-demographics` | Optional Census key for higher API limits. |
| `CHPL_API_KEY` | no | `public-records` | Optional ONC CHPL enrichment key. |
| `CLINICAL_TRIALS_INVENTORY_HARD_MAX` | no | `research-trials` | Maximum ClinicalTrials.gov records scanned by inventory tools. |
| `DOCGRAPH_CSV_PATH` | no | `physician-referral-network` | Optional licensed CareSet DocGraph import path. |
| `GOOGLE_CSE_API_KEY` | no | `web-intelligence` | Optional Google Custom Search API key. |
| `GOOGLE_CSE_CACHE_TTL_SECONDS` | no | `web-intelligence` | Google Custom Search cache TTL in seconds. |
| `GOOGLE_CSE_DAILY_LIMIT` | no | `web-intelligence` | Google Custom Search daily request guardrail. |
| `GOOGLE_CSE_ID` | no | `web-intelligence` | Optional Google Custom Search Engine ID. |
| `GOOGLE_CSE_SESSION_LIMIT` | no | `web-intelligence` | Google Custom Search per-session request guardrail. |
| `HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS` | no | `cache-manager` | Explicit opt-in for mutating HTTP deployments. |
| `HC_MCP_CACHE_ROOT` | no | `cache-manager` | Optional cache root override for cache-manager operations. |
| `HUD_API_TOKEN` | no | `geo-demographics` | Optional HUD USPS ZIP crosswalk API token. |
| `MCP_GATEWAY_ALLOWED_HOSTS` | no | `gateway` | Allowed Host headers for metadata gateway HTTP/SSE. |
| `MCP_GATEWAY_ALLOWED_ORIGINS` | no | `gateway` | Allowed Origin headers for metadata gateway HTTP/SSE. |
| `MCP_GATEWAY_AUTH_REQUIRED` | no | `gateway` | Whether metadata gateway HTTP/SSE auth is required. |
| `MCP_GATEWAY_BEARER_TOKEN` | no | `gateway` | Optional local bearer token. |
| `MCP_GATEWAY_BEARER_TOKENS` | no | `gateway` | Comma-separated metadata gateway bearer tokens. |
| `MCP_GATEWAY_BEARER_TOKEN_SHA256` | no | `gateway` | Recommended token hash for remote deployments. |
| `MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST` | no | `gateway` | Comma-separated metadata gateway bearer token SHA-256 hashes. |
| `MCP_GATEWAY_ISSUER_URL` | no | `gateway` | OAuth/OIDC issuer URL advertised by the metadata gateway. |
| `MCP_GATEWAY_PUBLIC_URL` | no | `gateway` | Public HTTPS MCP URL for metadata gateway deployments. |
| `MCP_GATEWAY_REQUIRED_SCOPES` | no | `gateway` | Required metadata gateway auth scopes; defaults to mcp:read. |
| `MCP_GATEWAY_TOKEN_SCOPES` | no | `gateway` | Optional semicolon-separated SHA-256 token-hash scope overrides. |
| `MCP_LIVE_GATEWAY_ALLOWED_HOSTS` | no | `live-gateway` | Allowed Host headers for live-gateway HTTP/SSE. |
| `MCP_LIVE_GATEWAY_ALLOWED_ORIGINS` | no | `live-gateway` | Allowed Origin headers for live-gateway HTTP/SSE. |
| `MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE` | no | `live-gateway` | Explicit opt-in to grant mcp:bulk to every valid live-gateway token. |
| `MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND` | no | `live-gateway` | Explicit opt-in for live-gateway wildcard network binds behind HTTPS and locked allow-lists. |
| `MCP_LIVE_GATEWAY_AUDIT_LOG_PATH` | no | `live-gateway` | Optional JSONL file path for non-secret live-gateway audit events. |
| `MCP_LIVE_GATEWAY_AUTH_REQUIRED` | no | `live-gateway` | Whether live-gateway HTTP/SSE auth is required; cannot disable remote auth. |
| `MCP_LIVE_GATEWAY_BEARER_TOKEN` | no | `live-gateway` | Bearer token for live HTTP/SSE deployments. |
| `MCP_LIVE_GATEWAY_BEARER_TOKENS` | no | `live-gateway` | Comma-separated live-gateway bearer tokens. |
| `MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256` | no | `live-gateway` | Recommended token hash for live HTTP/SSE deployments. |
| `MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256_LIST` | no | `live-gateway` | Comma-separated live-gateway bearer token SHA-256 hashes. |
| `MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND` | no | `live-gateway` | Docker-only marker for container wildcard binds published to localhost. |
| `MCP_LIVE_GATEWAY_ISSUER_URL` | no | `live-gateway` | OAuth/OIDC issuer URL advertised by live-gateway. |
| `MCP_LIVE_GATEWAY_PUBLIC_URL` | no | `live-gateway` | Public HTTPS MCP URL for live-gateway deployments. |
| `MCP_LIVE_GATEWAY_REQUIRED_SCOPES` | no | `live-gateway` | Required auth scopes; defaults to mcp:read. |
| `MCP_LIVE_GATEWAY_TOKEN_SCOPES` | no | `live-gateway` | Optional semicolon-separated SHA-256 token-hash scope overrides; use this to grant mcp:bulk to selected tokens. |
| `MRF_DOWNLOAD_PROGRESS_INTERVAL_BYTES` | no | `price-transparency` | Progress logging interval for large MRF downloads. |
| `MRF_MAX_DOWNLOAD_BYTES` | no | `price-transparency` | Maximum hospital MRF download size in bytes. |
| `MRF_MIN_FREE_BYTES` | no | `price-transparency` | Minimum free disk bytes required before MRF downloads. |
| `ORS_API_KEY` | no | `drive-time` | Optional OpenRouteService key for isochrones. |
| `OSRM_BASE_URL` | no | `drive-time` | Optional OSRM endpoint; defaults to the public demo server. |
| `PLACES_CACHE_DIR` | no | `community-health` | Optional CDC PLACES cache directory override. |
| `PROXYCURL_API_KEY` | no | `web-intelligence` | Optional Proxycurl enrichment key. |
| `SAM_GOV_API_KEY` | no | `public-records` | Required for SAM.gov opportunity and Exclusions API tools. |
| `SEC_USER_AGENT` | yes | `financial-intelligence` | Required for SEC EDGAR-backed tools. |

**Should I use stdio or HTTP?**
Use stdio for local desktop/CLI agents. Use local Streamable HTTP when Docker is already running or when multiple local clients share the same server process. Use the HTTPS `gateway` for remote metadata integrations and HTTPS `live-gateway` only when live-tool auth is configured.

**Can I expose every server to ChatGPT or another remote MCP client?**
Use the metadata-only `gateway` for discovery. Use `live-gateway` only behind HTTPS with bearer auth/OIDC-equivalent edge policy, Host/Origin validation, rate limits, and source-specific compliance controls. Do not expose cache-manager mutating tools remotely unless the deployment is explicitly scoped, authenticated, loopback-safe, and `HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS=true` is intentionally set.

**Where do cached datasets live?**
Local Python runs use `~/.healthcare-data-mcp/cache` by default. Docker Compose uses the `healthcare-cache` volume unless `HC_MCP_CACHE_ROOT` is set.

## About Contributions

*About Contributions:* Please don't take this the wrong way, but I do not accept outside contributions for any of my projects. I simply don't have the mental bandwidth to review anything, and it's my name on the thing, so I'm responsible for any problems it causes; thus, the risk-reward is highly asymmetric from my perspective. I'd also have to worry about other "stakeholders," which seems unwise for tools I mostly make for myself for free. Feel free to submit issues, and even PRs if you want to illustrate a proposed fix, but know I won't merge them directly. Instead, I'll have Claude or Codex review submissions via `gh` and independently decide whether and how to address them. Bug reports in particular are welcome. Sorry if this offends, but I want to avoid wasted time and hurt feelings. I understand this isn't in sync with the prevailing open-source ethos that seeks community contributions, but it's the only way I can move at this velocity and keep my sanity.

## License

MIT
