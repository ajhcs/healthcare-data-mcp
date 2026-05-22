# Client Packaging

This repo supports local stdio clients directly and includes a starter Claude Desktop MCPB package layout for one-click desktop installs.

## Prerequisites

For operator and analyst installs, prefer the release-style console command that
Claude Desktop, Codex, and other MCP clients can reach without activating this
repository:

```bash
pipx install healthcare-data-mcp
hc-mcp --version
hc-mcp doctor
hc-mcp doctor --check --json

uvx --from healthcare-data-mcp hc-mcp doctor
```

Until PyPI publishing is enabled, use a tagged Git URL such as
`pipx install git+https://github.com/ajhcs/healthcare-data-mcp@<tag>`.
The reported `hc-mcp --version` should match the tagged release or container
image label you intend to run.
`hc-mcp doctor --json` also exposes a `distribution` section for source
checkouts. It verifies package metadata, `hc-mcp`/`hc-mcp-setup` entry points,
wheel force-include aliases for registry server modules, and versioned Docker
metadata before a release is cut. Use `hc-mcp doctor --check --json` in release
automation when a non-ready report should stop the job.

Use an editable install only when developing the package or validating
unpublished changes locally:

```bash
python3 -m pip install -e .
hc-mcp --version
hc-mcp doctor
hc-mcp --list
```

If a GUI client cannot find `hc-mcp`, replace `command = "hc-mcp"` or `"command": "hc-mcp"` with the absolute path from `which hc-mcp`.
If the GUI client launches outside this repository, set `HC_MCP_ENV_FILE=/absolute/path/to/.env` in that server's MCP config.

Checked-in client config examples are registry-rendered. Regenerate them after
changing `shared/utils/server_registry.py`:

```bash
python scripts/render_client_configs.py project-mcp > .mcp.json
python scripts/render_client_configs.py claude-desktop-stdio > examples/claude-desktop-stdio.json
python scripts/render_client_configs.py codex > examples/codex-config.toml
python scripts/render_client_configs.py http-clients > configs/http-clients.json
python scripts/render_client_configs.py claude-desktop > configs/claude-desktop.json
```

Validate the checked-in examples without writing files:

```bash
python scripts/render_client_configs.py project-mcp --check
python scripts/render_client_configs.py claude-desktop-stdio --check
python scripts/render_client_configs.py codex --check
python scripts/render_client_configs.py http-clients --check
python scripts/render_client_configs.py claude-desktop --check
```

The universal installer uses the same registry for server registration,
zero-config Docker selection, port hints, and interactive environment-key
prompts. Run `bash install.sh --dry-run --no-register` to verify the local
registry can be loaded and to preview the registry-derived zero-config server
IDs and environment-key names without cloning, installing, writing config, or
registering clients. Unknown installer options fail early with usage output
instead of falling through to install or registration logic.

## Docker Compose HTTP

`docker-compose.yml` and `docker-compose.zero-config.yml` are generated from
`shared/utils/server_registry.py` by `scripts/render_compose.py`. Each service
keeps its registry module, port, and environment keys, publishes only to
`127.0.0.1`, and tags the local build as `healthcare-data-mcp:<package-version>`.
Set `HC_MCP_IMAGE` when you want Compose to use a trusted published image
reference instead of the default local version tag; the generated services use
`pull_policy: missing` so startup can reuse or pull that image unless `--build`
is passed explicitly.

Validate the checked-in Compose files without writing files:

```bash
python scripts/render_compose.py full --check
python scripts/render_compose.py zero-config --check
```

## Claude Desktop Stdio

Use `examples/claude-desktop-stdio.json` as a concrete `claude_desktop_config.json` example. Keep only the servers you need; each entry starts one local stdio MCP process:

```json
{
  "mcpServers": {
    "cms-facility": {
      "command": "hc-mcp",
      "args": ["cms-facility"]
    }
  }
}
```

This mode does not require Docker or HTTP ports.

## Codex CLI / Codex IDE / Codex App

Use `examples/codex-config.toml` for `~/.codex/config.toml` entries. Codex CLI and the IDE/App share `config.toml`, so one setup works across both local clients. Local stdio is the default recommendation:

The registry-backed helper can preview or apply the same server list through the Codex CLI. Use `scripts/register-codex.sh --dry-run` first; it prints the planned `codex mcp add`/`remove` commands without writing Codex config. Add `--http` to preview Docker/HTTP registrations.

```toml
[mcp_servers.cmsFacility]
command = "hc-mcp"
args = ["cms-facility"]

[mcp_servers.publicRecords]
command = "hc-mcp"
args = ["public-records"]
env = { HC_MCP_ENV_FILE = "/absolute/path/to/healthcare-data-mcp/.env" }
```

When Docker Compose Streamable HTTP servers are already running, Codex can use localhost URLs instead:

```toml
[mcp_servers.cmsFacilityHttp]
url = "http://127.0.0.1:8006/mcp"
```

## Claude Desktop MCPB

The MCPB skeleton lives under `desktop-extension/`:

- `desktop-extension/manifest.json` declares the extension metadata, Python runtime, stdio launch command, and configurable `server_name`.
- `desktop-extension/server/launcher.py` adds the bundled Python target directory to `sys.path` and delegates to `servers._launcher`.
- `scripts/build_mcpb.py` stages files under `build/mcpb/healthcare-data-mcp` and writes a `.mcpb` archive under `dist/`.

The `server_name` selector is registry-backed. The source manifest carries an
explicit enum of canonical server IDs, and the builder rewrites the staged enum,
description, and default from `shared/utils/server_registry.py` before packaging.

Build a local bundle with vendored Python dependencies:

```bash
python3 scripts/build_mcpb.py \
  --server-name cms-facility \
  --output dist/healthcare-data-mcp-cms-facility.mcpb
```

The Desktop Extension manifest exposes registry-backed optional GUI fields for
server environment keys, including SAM.gov, CHPL, SEC EDGAR contact metadata,
Census/HUD/routing/workforce/search keys, import-path settings, and gateway
token settings, plus the selected `server_name`. The builder rewrites these
fields from `shared/utils/server_registry.py` when staging a package. This is
the easiest path for non-technical Claude Desktop users who should not edit
JSON by hand.

Validate that the checked-in source manifest is still aligned with the registry
without writing build artifacts:

```bash
python3 scripts/build_mcpb.py --check
```

For a fast manifest and launcher smoke test that does not install dependencies:

```bash
python3 scripts/build_mcpb.py --skip-dependency-install --force
```

The generated `.mcpb` is a zip archive with `manifest.json`, `server/launcher.py`, and, unless skipped, `server/lib` installed with `pip --target`. The script only writes inside `build/` and `dist/` and refuses stage/output paths outside the repository.
CI and `tests/test_client_packaging.py` also run the read-only registry check,
open the generated skeleton archive, and verify the staged manifest has
registry-backed server choices before it is treated as a valid desktop package
artifact.

## Current Limits

- The MCPB manifest packages one selected server name at a time. Build separate artifacts for different default servers.
- The classic Python MCPB path expects Python 3.11+ to be available to Claude Desktop.
- The skeleton has not been submitted to a connector directory and should be validated in Claude Desktop before distribution.

## References

- OpenAI Codex MCP configuration: https://developers.openai.com/codex/mcp
- Claude Code MCP configuration: https://docs.anthropic.com/en/docs/claude-code/mcp
- Anthropic remote MCP connector: https://docs.anthropic.com/en/docs/agents-and-tools/mcp-connector
