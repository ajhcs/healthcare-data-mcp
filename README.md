# Healthcare Data MCP

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![MCP](https://img.shields.io/badge/MCP-stdio%20%7C%20streamable--http-0f766e)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Public healthcare market intelligence for AI agents: 18 local MCP servers covering hospitals, ownership, quality, claims, price transparency, workforce, finance, community health, research activity, web intelligence, and federal exclusion screening.

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
hc-mcp-setup --interactive
hc-mcp --list
```

Prefer an installer script?

```bash
curl -fsSL https://raw.githubusercontent.com/ajhcs/healthcare-data-mcp/main/install.sh | bash
```

## TL;DR

Healthcare data is public, but it is scattered across CMS, CDC, NIH, HHS OIG, SAM.gov, Census, HUD, SEC, IRS, hospital MRFs, and other systems. Healthcare Data MCP turns those sources into focused MCP tools with local caches, structured responses, source metadata, and client-friendly server discovery.

| If your agent needs to... | Use these servers |
| --- | --- |
| Find facilities, ownership, CHOW history, quality, claims, service areas, or pricing | `cms-facility`, `provider-enrollment`, `hospital-quality`, `claims-analytics`, `service-area`, `price-transparency` |
| Understand a market, community, workforce, or access footprint | `geo-demographics`, `community-health`, `workforce-analytics`, `drive-time` |
| Research health systems, referrals, finance, trials, publications, or web presence | `health-system-profiler`, `physician-referral-network`, `financial-intelligence`, `research-trials`, `web-intelligence` |
| Screen organizations against public compliance signals | `public-records` |
| Let remote MCP clients search dataset metadata safely | `gateway`, `discovery` |

## Quick Example

```bash
# Install locally and configure optional API keys
python -m pip install -e ".[dev]"
hc-mcp-setup --interactive

# See every available server and its default HTTP port
hc-mcp --list

# Run one server over stdio for Claude Desktop, Claude Code, Codex, or another local MCP client
hc-mcp public-records

# Run the same server over local Streamable HTTP
hc-mcp public-records --transport streamable-http --port 8013

# Start all HTTP servers in containers
docker compose up --build
```

## Why Use It?

| Capability | What you get | Example source families |
| --- | --- | --- |
| Agent-ready tools | Narrow MCP servers instead of one huge ambiguous tool surface | Facility lookup, LEIE screening, trial search, ownership graph expansion |
| Structured responses | Bounded JSON-compatible results with provenance fields where available | CMS, CDC, NIH, ClinicalTrials.gov, SAM.gov, HHS OIG |
| Local-first operation | Stdio and localhost HTTP by default, with cache reuse across sessions | `~/.healthcare-data-mcp/cache` or Docker `healthcare-cache` |
| Remote-safe metadata mode | A gateway that exposes `search` and `fetch` for dataset metadata only | OpenAI and Claude remote MCP connector shapes |
| Practical client packaging | Examples for Codex, Claude Code, Claude Desktop, generic MCP clients, Docker, and MCPB | `examples/`, `configs/`, `desktop-extension/` |

## Server Catalog

| Server | Port | Domain |
| --- | ---: | --- |
| `service-area` | 8002 | CMS hospital service areas and market share |
| `geo-demographics` | 8003 | Census, ZCTA, Medicare, and HUD geography |
| `drive-time` | 8004 | Routing, drive-time matrices, and access scoring |
| `hospital-quality` | 8005 | CMS quality, readmission, and safety data |
| `cms-facility` | 8006 | CMS facility master data and NPPES lookup |
| `health-system-profiler` | 8007 | Health system discovery and facility enrichment |
| `financial-intelligence` | 8008 | IRS 990, SEC EDGAR, and nonprofit finance intelligence |
| `price-transparency` | 8009 | Hospital MRF and benchmark pricing |
| `physician-referral-network` | 8010 | NPPES, physician mix, referral network, and leakage analysis |
| `workforce-analytics` | 8011 | BLS and ACGME workforce analytics |
| `claims-analytics` | 8012 | DRG, service-line, and claims analytics |
| `public-records` | 8013 | USAspending, SAM.gov, CHPL, accreditation, 340B, HIPAA breaches, LEIE, and SAM Exclusions |
| `web-intelligence` | 8014 | Web search and health system OSINT |
| `discovery` | 8015 | Dataset catalog resources, cache status, runbooks, and prompts |
| `gateway` | 8016 | Remote-safe metadata gateway with `search` and `fetch` |
| `provider-enrollment` | 8017 | CMS PECOS-derived enrollment, ownership graph, and CHOW history |
| `community-health` | 8018 | CDC PLACES county, place, tract, and ZCTA estimates |
| `research-trials` | 8019 | NIH RePORTER funding and ClinicalTrials.gov study activity |

HTTP servers bind to `127.0.0.1` by default. Use `MCP_HOST=0.0.0.0` only in containers or behind a trusted reverse proxy with authentication.

## Installation

### Local Python

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
hc-mcp-setup --interactive
hc-mcp --list
```

### Docker Compose

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
cp .env.example .env
docker compose up --build
```

Each server is exposed at `http://localhost:<port>/mcp`.

### Universal Installer

```bash
curl -fsSL https://raw.githubusercontent.com/ajhcs/healthcare-data-mcp/main/install.sh | bash
```

The installer detects common MCP clients and can install via local Python or Docker.

## MCP Client Setup

| Client/system | Recommended mode | Setup |
| --- | --- | --- |
| Codex CLI / Codex IDE / Codex App | Local stdio, or HTTP when Docker is running | `examples/codex-config.toml` or `codex mcp add ...` |
| Claude Code | Project HTTP via `.mcp.json`, or stdio for selected servers | `.mcp.json` or `claude mcp add ...` |
| Claude Desktop / shared desktops | Stdio JSON, Desktop Extension/MCPB, or local HTTP | `examples/claude-desktop-stdio.json`, `.mcp.json`, or `scripts/build_mcpb.py` |
| Generic MCP clients | Stdio command or local Streamable HTTP URL | `hc-mcp <server>` or `http://localhost:<port>/mcp` |
| OpenAI/Anthropic remote MCP integrations | HTTPS gateway only | `hc-mcp gateway --transport streamable-http` behind HTTPS/auth |

Local stdio examples:

```bash
codex mcp add cmsFacility -- hc-mcp cms-facility
codex mcp add publicRecords --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp public-records

claude mcp add provider-enrollment --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp provider-enrollment
```

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

Some tools also depend on local cache files. The setup CLI fetches sources that expose a stable unauthenticated acquisition path and leaves the rest as explicit imports:

```bash
hc-mcp-setup --cache-status
hc-mcp-setup --acquire-public-caches
hc-mcp-setup --acquire-hipaa-breaches
hc-mcp-setup --cache-guide
hc-mcp-setup --import-340b-json /path/to/340b_covered_entities.json
hc-mcp-setup --import-docgraph-csv /path/to/docgraph_shared_patients.csv
# or, if already converted:
hc-mcp-setup --import-docgraph-parquet /path/to/shared_patients.parquet
```

The default cache root is `~/.healthcare-data-mcp/cache`. The affected tools are `public_records.get_340b_status`, `public_records.get_breach_history`, `physician_referral_network.map_referral_network`, and `physician_referral_network.detect_leakage`.

`hc-mcp-setup --acquire-public-caches` currently fetches the public HHS OCR HIPAA breach table. HRSA 340B still requires importing the OPAIS Covered Entity Daily Export JSON because the public reports page does not expose a stable unauthenticated file URL for the CLI. DocGraph/CareSet shared-patient data is separately licensed and is import-only.

## Command Reference

```bash
# List available servers
hc-mcp --list

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
hc-mcp-setup --cache-guide
hc-mcp-setup --agent-cache-instructions
hc-mcp-setup --import-340b-json /path/to/340b_covered_entities.json
hc-mcp-setup --import-breach-csv /path/to/hipaa_breaches.csv
hc-mcp-setup --import-docgraph-csv /path/to/docgraph_shared_patients.csv

# Build the Claude Desktop extension package
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
  ├─ server registry in servers/_launcher.py
  ├─ dotenv loading through HC_MCP_ENV_FILE or --env-file
  └─ stdio, SSE, or Streamable HTTP transport
          │
          ▼
Focused FastMCP servers
  ├─ structured_output=True tools
  ├─ shared HTTP retry/client helpers
  ├─ source catalog and identity normalization helpers
  └─ bounded responses with source metadata
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

## Cache and Compliance Notes

Dataset caches live under `~/.healthcare-data-mcp/cache` and the Docker `healthcare-cache` volume.

LEIE stores `public-records/leie_current.csv`, `leie_current.parquet`, and `leie_current.meta.json` with a 31-day freshness target and stale-cache fallback. LEIE and SAM.gov Exclusions responses are screening support only. Name matches are potential matches; do not treat a zero-result response as legal clearance.

The remote `gateway` is intentionally metadata-only. It exposes dataset search/fetch records for remote MCP clients and does not proxy live exclusion screening, provider-enrollment queries, PLACES queries, RePORTER, or ClinicalTrials.gov.

## Development

```bash
python -m pip install -e ".[dev]"
ruff check .
pytest -q
python3 -m compileall -q servers shared scripts tests smoke_test.py
```

Manual live-data smoke tests are in `smoke_test.py`. They call public APIs and may require environment variables from `.env.example`; they are intentionally excluded from normal pytest discovery.

```bash
HC_MCP_LIVE_EXPANSION=1 HC_MCP_LIVE_LEIE=1 SAM_GOV_API_KEY=... python smoke_test.py
```

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| GUI client cannot find `hc-mcp` | Use the absolute path from `which hc-mcp`, or activate the project venv and reinstall with `python -m pip install -e .` |
| API-backed tool says a key is missing | Run `hc-mcp-setup --interactive`, then point the client at `.env` with `HC_MCP_ENV_FILE` |
| Docker port conflict | Check `docker-compose.yml` ports and run `ss -tlnp` before changing ports |
| Claude Code ignores project config | Approve the project `.mcp.json`, or add servers with `claude mcp add --scope local ...` |
| Codex does not see a server | Check `~/.codex/config.toml` or `.codex/config.toml`; Codex CLI and IDE share that config |
| Remote MCP client cannot use localhost | Deploy `gateway` behind HTTPS/auth; stdio and localhost HTTP are local-only |

## Limitations

- This is an alpha project. Source schemas, API behavior, and MCP client compatibility can change.
- Some source APIs require keys for reliable or full access.
- Large public datasets are cached locally; first use can take longer while files download and normalize.
- Public exclusion screening is not final SSN/EIN identity verification or legal clearance.
- The remote gateway is metadata-only unless a separate authenticated live-data gateway is designed and approved.
- The MCPB Desktop Extension skeleton should be validated in Claude Desktop before broad distribution.

## FAQ

**Does this store PHI?**
No. The project is built around public datasets and local cache files. Do not put PHI into prompts, logs, config, or cache paths.

**Do I need API keys?**
Not for every server. Many tools work with public downloads or unauthenticated public APIs, but SAM.gov, SEC EDGAR, Census, HUD, ORS, BLS, Google CSE, CHPL, and Proxycurl features may need keys or contact metadata.

**Should I use stdio or HTTP?**
Use stdio for local desktop/CLI agents. Use local Streamable HTTP when Docker is already running or when multiple local clients share the same server process. Use the HTTPS `gateway` for remote MCP integrations.

**Can I expose every server to ChatGPT or another remote MCP client?**
Not directly. Use the metadata-only `gateway` unless you have designed authentication, authorization, rate limits, Host/Origin validation, and source-specific compliance controls for live tools.

**Where do cached datasets live?**
Local Python runs use `~/.healthcare-data-mcp/cache`. Docker Compose uses the `healthcare-cache` volume.

## License

MIT
