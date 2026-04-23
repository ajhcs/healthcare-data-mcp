# Client Packaging

This repo supports local stdio clients directly and includes a starter Claude Desktop MCPB package layout for one-click desktop installs.

## Prerequisites

Install the package in the Python environment Claude Desktop or Codex can reach:

```bash
python3 -m pip install -e .
hc-mcp --list
```

If a GUI client cannot find `hc-mcp`, replace `command = "hc-mcp"` or `"command": "hc-mcp"` with the absolute path from `which hc-mcp`.

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

## Codex

Use `examples/codex-config.toml` for `~/.codex/config.toml` entries. Local stdio is the default recommendation:

```toml
[mcp_servers.cmsFacility]
command = "hc-mcp"
args = ["cms-facility"]
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

For a fast manifest and launcher smoke test that does not install dependencies:

```bash
python3 scripts/build_mcpb.py --skip-dependency-install --force
```

The generated `.mcpb` is a zip archive with `manifest.json`, `server/launcher.py`, and, unless skipped, `server/lib` installed with `pip --target`. The script only writes inside `build/` and `dist/` and refuses stage/output paths outside the repository.

## Current Limits

- The MCPB manifest packages one selected server name at a time. Build separate artifacts for different default servers.
- The classic Python MCPB path expects Python 3.11+ to be available to Claude Desktop.
- The skeleton has not been submitted to a connector directory and should be validated in Claude Desktop before distribution.
