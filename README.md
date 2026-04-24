# healthcare-data-mcp

Local MCP servers for public healthcare market intelligence: CMS facilities, service areas, quality, claims, workforce, financials, provider enrollment and ownership, CDC PLACES community health, NIH/ClinicalTrials.gov research activity, and federal exclusion screening.

Use it from Codex, Claude Code, Claude Desktop, Claude Desktop cowork/shared-machine setups, or any MCP client that can launch stdio servers or connect to local Streamable HTTP.

## Why This Exists

Healthcare data is public, but it is scattered across CMS, CDC, NIH, HHS OIG, SAM.gov, Census, HUD, SEC, IRS, and other systems. This repo turns those sources into focused MCP tools with structured responses, local caches, provenance metadata, and agent-friendly discovery.

| Need | Server |
| --- | --- |
| Find hospitals, ownership, quality, service areas, claims, or pricing | `cms-facility`, `hospital-quality`, `service-area`, `claims-analytics`, `price-transparency` |
| Understand markets and communities | `geo-demographics`, `drive-time`, `community-health` |
| Analyze health systems, referrals, workforce, finance, or web presence | `health-system-profiler`, `physician-referral-network`, `workforce-analytics`, `financial-intelligence`, `web-intelligence` |
| Screen providers/vendors against federal exclusions | `public-records` |
| Explore available datasets and cache state | `discovery` or the remote-safe `gateway` |

## Quick Start

```bash
python3 -m pip install -e ".[dev]"
hc-mcp-setup --interactive
hc-mcp --list
```

Run a server over stdio:

```bash
hc-mcp public-records
```

Run a server over local Streamable HTTP:

```bash
hc-mcp public-records --transport streamable-http --port 8013
```

Run every HTTP server with Docker Compose:

```bash
docker compose up --build
```

## API Keys and `.env`

Use the setup wizard instead of hand-editing secrets:

```bash
hc-mcp-setup --interactive
hc-mcp-setup --validate-only
hc-mcp-setup --print-client-snippets
```

`hc-mcp` loads `.env` from the current working directory before starting a server. For GUI clients that launch from another directory, set `HC_MCP_ENV_FILE=/absolute/path/to/.env` or pass `--env-file /absolute/path/to/.env`.

New April 2026 tools only require one new key:

| Variable | Used by | Required? |
| --- | --- | --- |
| `SAM_GOV_API_KEY` | SAM.gov Exclusions and opportunities in `public-records` | Required for SAM API tools |
| `SEC_USER_AGENT` | SEC EDGAR in `financial-intelligence` | Required for SEC tools |
| `CHPL_API_KEY` | ONC CHPL enrichment in `public-records` | Optional |
| `CENSUS_API_KEY`, `HUD_API_TOKEN`, `ORS_API_KEY`, `BLS_API_KEY`, `GOOGLE_CSE_API_KEY`, `GOOGLE_CSE_ID`, `PROXYCURL_API_KEY` | Existing source-specific tools | Optional or feature-specific |

No key is required for HHS OIG LEIE, CMS PECOS/provider enrollment, CDC PLACES, NIH RePORTER, or ClinicalTrials.gov.

## MCP Clients

| Client/system | Recommended mode | Setup |
| --- | --- | --- |
| Codex CLI / Codex IDE / Codex App | Local stdio, or HTTP when Docker is running | `examples/codex-config.toml` or `codex mcp add ...` |
| Claude Code | Project HTTP via `.mcp.json`, or stdio for selected servers | `.mcp.json` or `claude mcp add ...` |
| Claude Desktop / cowork desktops | Stdio JSON, Desktop Extension/MCPB, or local HTTP for shared machines | `examples/claude-desktop-stdio.json`, `.mcp.json`, or `scripts/build_mcpb.py` |
| Generic MCP clients | Stdio command or local Streamable HTTP URL | `hc-mcp <server>` or `http://localhost:<port>/mcp` |
| OpenAI/Anthropic remote MCP API integrations | HTTPS gateway only | `hc-mcp gateway --transport streamable-http` behind HTTPS/auth |

Examples:

```bash
codex mcp add publicRecords --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp public-records
claude mcp add provider-enrollment --env HC_MCP_ENV_FILE=/absolute/path/to/.env -- hc-mcp provider-enrollment
```

More detail:

- [MCP client notes](docs/MCP_CLIENTS.md)
- [Client packaging and MCPB](docs/CLIENT_PACKAGING.md)
- [Remote gateway](docs/REMOTE_GATEWAY.md)

## Server Catalog

| Server | Port | Domain |
| --- | ---: | --- |
| `service-area` | 8002 | CMS hospital service areas and market share |
| `geo-demographics` | 8003 | Census, ZCTA, Medicare, HUD geography |
| `drive-time` | 8004 | Routing, drive-time matrices, access scoring |
| `hospital-quality` | 8005 | CMS quality, readmission, safety data |
| `cms-facility` | 8006 | CMS facility master data and NPPES lookup |
| `health-system-profiler` | 8007 | Health system discovery and facility enrichment |
| `financial-intelligence` | 8008 | IRS 990, SEC EDGAR, nonprofit finance intelligence |
| `price-transparency` | 8009 | Hospital MRF and benchmark pricing |
| `physician-referral-network` | 8010 | NPPES, physician mix, referral network, leakage analysis |
| `workforce-analytics` | 8011 | BLS and ACGME workforce analytics |
| `claims-analytics` | 8012 | DRG, service-line, and claims analytics |
| `public-records` | 8013 | USAspending, SAM.gov, CHPL, 340B, HIPAA breaches, LEIE, SAM Exclusions |
| `web-intelligence` | 8014 | Web search and health system OSINT |
| `discovery` | 8015 | Dataset catalog, cache status, runbooks, prompts |
| `gateway` | 8016 | Remote-safe metadata search/fetch only |
| `provider-enrollment` | 8017 | CMS PECOS-derived hospital/SNF enrollment, ownership graph, CHOW history |
| `community-health` | 8018 | CDC PLACES county/place/tract/ZCTA community estimates |
| `research-trials` | 8019 | NIH RePORTER funding and ClinicalTrials.gov studies |

HTTP servers bind to `127.0.0.1` by default. Use `MCP_HOST=0.0.0.0` only in containers or behind a trusted reverse proxy with auth.

## Architecture

```text
MCP client
  ├─ stdio: hc-mcp <server>
  └─ HTTP:  http://localhost:<port>/mcp
          │
          ▼
FastMCP server modules
  ├─ focused tools with structured_output=True
  ├─ shared HTTP retry/client helpers
  ├─ source catalog + identity normalization helpers
  └─ bounded responses with source metadata
          │
          ▼
Public sources + local cache
  ├─ CMS, CDC, NIH, ClinicalTrials.gov
  ├─ HHS OIG LEIE, SAM.gov, USAspending, CHPL
  ├─ Census, HUD, BLS, SEC, IRS, OSRM
  └─ ~/.healthcare-data-mcp/cache or Docker healthcare-cache volume
```

## Cache and Compliance Notes

Dataset caches live under `~/.healthcare-data-mcp/cache` and the Docker `healthcare-cache` volume. April 2026 caches use `provider-enrollment/`, `community-health/`, and `public-records/` subdirectories.

LEIE stores `public-records/leie_current.csv`, `leie_current.parquet`, and `leie_current.meta.json` with a 31-day freshness target and stale-cache fallback. LEIE and SAM.gov Exclusions responses are screening support only. Name matches are potential matches; do not treat a zero-result response as legal clearance.

## Development

```bash
python3 -m pip install -e ".[dev]"
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
| GUI client cannot find `hc-mcp` | Use the absolute path from `which hc-mcp`, or reinstall with `python3 -m pip install -e .` |
| API-backed tool says a key is missing | Run `hc-mcp-setup --interactive`, then point the client at `.env` with `HC_MCP_ENV_FILE` |
| Docker port conflict | Check `docker-compose.yml` ports and run `ss -tlnp` before changing ports |
| Claude Code ignores project config | Approve the project `.mcp.json`, or add servers with `claude mcp add --scope local ...` |
| Codex does not see a server | Check `~/.codex/config.toml` or `.codex/config.toml`; Codex CLI and IDE share that config |
| Remote MCP client cannot use localhost | Deploy `gateway` behind HTTPS/auth; stdio and localhost HTTP are local-only |

## Limitations

- The remote gateway is metadata-only. It does not proxy live exclusion screening, provider-enrollment queries, PLACES queries, RePORTER, or ClinicalTrials.gov.
- Some source APIs require keys for reliable or full access.
- Public exclusion screening is not final SSN/EIN identity verification.
- Large public datasets are cached locally; first use can take longer while files download and normalize.
- The MCPB Desktop Extension skeleton should be validated in Claude Desktop before broad distribution.

## About Contributions

Contributions are welcome. Please open an issue for larger changes before investing heavily, keep pull requests focused, and include tests or fixture updates for behavior changes. Bug reports, source URL fixes, new public dataset integrations, client packaging improvements, and documentation cleanup are especially useful.

## License

MIT
