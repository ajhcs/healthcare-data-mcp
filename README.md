# healthcare-data-mcp

MCP servers for public healthcare data: CMS facility files, hospital service areas, demographics, drive times, hospital quality, financial intelligence, price transparency, physician referral networks, workforce analytics, claims analytics, public records, and web intelligence.

## Install

```bash
python3 -m pip install -e ".[dev]"
hc-mcp --list
```

Run one server over stdio for local agent clients:

```bash
hc-mcp cms-facility
```

Run one server over Streamable HTTP:

```bash
hc-mcp cms-facility --transport streamable-http --port 8006
```

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

## Development

```bash
python3 -m pip install -e ".[dev]"
ruff check .
pytest -q
python3 -m compileall -q servers shared tests smoke_test.py
```

`smoke_test.py` is a manual live-data script. It calls public APIs and may require environment variables from `.env.example`; it is intentionally excluded from normal pytest discovery.
