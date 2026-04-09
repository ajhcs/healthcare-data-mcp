# Contributing

## Development Setup

```bash
git clone https://github.com/ajhcs/healthcare-data-mcp.git
cd healthcare-data-mcp
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Before Opening a Pull Request

Run the checks that exist in the repo today:

```bash
ruff check .
pytest tests/servers/health_system_profiler -q
```

If you touched server startup or shared infrastructure, also smoke-import the
affected modules or run the Docker build locally.

## Change Expectations

- Keep server outputs JSON-serializable and MCP-friendly.
- Prefer shared utilities in `shared/utils/` over copying logic across servers.
- Do not commit secrets, `.env`, cache files, or downloaded datasets.
- If a source requires attribution or fair-access headers, preserve those
  requirements in code and docs.

## Issue Tracking

This repo uses `bd` (beads) for local task tracking. See [AGENTS.md](AGENTS.md)
for the workflow used inside the repository.
