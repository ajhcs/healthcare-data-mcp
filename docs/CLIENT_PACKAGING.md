# Client Packaging

This repo supports local stdio clients directly and includes a starter Claude Desktop MCPB package layout for one-click desktop installs.

## Prerequisites

Install the package in the Python environment Claude Desktop or Codex can reach:

```bash
python3 -m pip install -e .
hc-mcp-setup --interactive
hc-mcp --list
```

If a GUI client cannot find `hc-mcp`, replace `command = "hc-mcp"` or `"command": "hc-mcp"` with the absolute path from `which hc-mcp`.
If the GUI client launches outside this repository, set `HC_MCP_ENV_FILE=/absolute/path/to/.env` in that server's MCP config.

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

Build a local bundle with vendored Python dependencies:

```bash
python3 scripts/build_mcpb.py \
  --server-name cms-facility \
  --output dist/healthcare-data-mcp-cms-facility.mcpb
```

The Desktop Extension manifest exposes optional GUI fields for `SAM_GOV_API_KEY`, `CHPL_API_KEY`, `SEC_USER_AGENT`, Census/HUD/routing/workforce/search keys, and the selected `server_name`. This is the easiest path for non-technical Claude Desktop users who should not edit JSON by hand.

For a fast manifest and launcher smoke test that does not install dependencies:

```bash
python3 scripts/build_mcpb.py --skip-dependency-install --force
```

The generated `.mcpb` is a zip archive with `manifest.json`, `server/launcher.py`, and, unless skipped, `server/lib` installed with `pip --target`. The script only writes inside `build/` and `dist/` and refuses stage/output paths outside the repository.

## Current Limits

- The MCPB manifest packages one selected server name at a time. Build separate artifacts for different default servers.
- The classic Python MCPB path expects Python 3.11+ to be available to Claude Desktop.
- The skeleton has not been submitted to a connector directory and should be validated in Claude Desktop before distribution.

## References

- OpenAI Codex MCP configuration: https://developers.openai.com/codex/mcp
- Claude Code MCP configuration: https://docs.anthropic.com/en/docs/claude-code/mcp
- Anthropic remote MCP connector: https://docs.anthropic.com/en/docs/agents-and-tools/mcp-connector
