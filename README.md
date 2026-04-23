# healthcare-data-mcp

MCP servers for public healthcare data: CMS facility files, hospital service areas, demographics, drive times, hospital quality, financial intelligence, price transparency, physician referral networks, workforce analytics, claims analytics, public records and exclusions, web intelligence, provider enrollment and ownership, community health, research/trials, dataset discovery, and remote-safe metadata gateway access.

## Install

```bash
python3 -m pip install -e ".[dev]"
hc-mcp --list
```

Run one server over stdio for local agent clients:

```bash
hc-mcp cms-facility
```

Run the discovery resources/prompts server or remote-safe metadata gateway:

```bash
hc-mcp discovery
hc-mcp gateway --transport streamable-http --port 8016
```

Run one server over Streamable HTTP:

```bash
hc-mcp cms-facility --transport streamable-http --port 8006
```

Current local HTTP ports:

| Server | Port | Domain |
| --- | ---: | --- |
| `public-records` | 8013 | SAM.gov, USAspending, CHPL, 340B, HIPAA breaches, HHS OIG LEIE, and SAM exclusions |
| `discovery` | 8015 | Metadata catalog, cache status, runbooks, and prompts |
| `gateway` | 8016 | Remote-safe metadata search/fetch only |
| `provider-enrollment` | 8017 | CMS PECOS-derived hospital/SNF enrollment, ownership graph, and CHOW history |
| `community-health` | 8018 | CDC PLACES county/place/tract/ZCTA community estimates |
| `research-trials` | 8019 | NIH RePORTER funding and ClinicalTrials.gov studies |

HTTP servers bind to `127.0.0.1` by default. Set `MCP_HOST=0.0.0.0` only behind a trusted network boundary or reverse proxy with auth.

Run every HTTP server with Docker Compose:

```bash
docker compose up --build
```

## Client Configuration

This repo includes a project-scoped `.mcp.json` for local HTTP servers. Start the servers with Docker Compose first, then use the project config from Claude Code or another client that supports `.mcp.json`.

For stdio clients, add individual servers with the launcher:

```bash
claude mcp add cms-facility -- hc-mcp cms-facility
codex mcp add cms-facility -- hc-mcp cms-facility
```

See [docs/MCP_CLIENTS.md](docs/MCP_CLIENTS.md) for Claude Desktop, Claude Code, Codex, OpenAI remote MCP, and packaging guidance.

## Cache and API Notes

Dataset caches live under `~/.healthcare-data-mcp/cache` and the Docker `healthcare-cache` volume. New April 2026 caches use `provider-enrollment/`, `community-health/`, and `public-records/` subdirectories. LEIE stores `public-records/leie_current.csv`, `leie_current.parquet`, and `leie_current.meta.json` with a 31-day freshness target and stale-cache fallback in the public-records server.

LEIE and SAM.gov Exclusions responses are screening support only. Name matches are potential matches; do not treat a zero-result response as a legal clearance. SAM.gov API-backed tools require `SAM_GOV_API_KEY` in the public-records server environment.

## Development

```bash
python3 -m pip install -e ".[dev]"
ruff check .
pytest -q
python3 -m compileall -q servers shared tests smoke_test.py
```

`smoke_test.py` is a manual live-data script. It calls public APIs and may require environment variables from `.env.example`; it is intentionally excluded from normal pytest discovery.
