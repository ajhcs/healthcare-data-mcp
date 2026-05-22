"""Render registry-backed MCP client configuration examples."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from shared.utils.server_registry import SERVER_REGISTRY

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_PLACEHOLDER = "/absolute/path/to/healthcare-data-mcp/.env"


def codex_key(server_id: str, *, http: bool = False) -> str:
    """Return the camelCase Codex config key for a server id."""

    parts = server_id.split("-")
    key = parts[0] + "".join(part.capitalize() for part in parts[1:])
    return f"{key}Http" if http else key


def render_codex_config() -> str:
    """Render example ~/.codex/config.toml entries for all registry servers."""

    lines = [
        "# Example ~/.codex/config.toml entries for healthcare-data-mcp.",
        "# Keep only the servers you actually want Codex to start.",
        "# Run `hc-mcp-setup --interactive` first to create .env, then either start",
        "# Codex from the repo root or set HC_MCP_ENV_FILE below.",
        "",
    ]
    for spec in SERVER_REGISTRY:
        lines.extend(
            [
                f"[mcp_servers.{codex_key(spec.server_id)}]",
                'command = "hc-mcp"',
                f'args = ["{spec.server_id}"]',
            ]
        )
        if spec.server_id == "public-records":
            lines.extend(
                [
                    "# Optional when Codex is not launched from the repo root:",
                    f'# env = {{ HC_MCP_ENV_FILE = "{ENV_PLACEHOLDER}" }}',
                ]
            )
        lines.append("")

    lines.extend(
        [
            "# If the Docker Compose Streamable HTTP servers are already running, Codex can",
            "# connect to localhost instead of spawning stdio processes.",
        ]
    )
    for spec in SERVER_REGISTRY:
        lines.extend(
            [
                f"[mcp_servers.{codex_key(spec.server_id, http=True)}]",
                f'url = "http://127.0.0.1:{spec.port}/mcp"',
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_http_clients_config() -> str:
    """Render generic HTTP MCP client configuration for all registry servers."""

    payload: dict[str, Any] = {
        "_comment": "HTTP transport config for any MCP client. Requires: docker compose up -d",
        "mcpServers": {
            f"hc-{spec.server_id}": {
                "type": "http",
                "url": f"http://localhost:{spec.port}/mcp",
            }
            for spec in SERVER_REGISTRY
        },
    }
    return json.dumps(payload, indent=2) + "\n"


def render_project_mcp_config() -> str:
    """Render project-scoped Claude Code .mcp.json for all registry servers."""

    payload: dict[str, Any] = {
        "mcpServers": {
            spec.server_id: {
                "type": "stdio",
                "command": "hc-mcp",
                "args": [spec.server_id],
            }
            for spec in SERVER_REGISTRY
        }
    }
    return json.dumps(payload, indent=2) + "\n"


def render_claude_desktop_stdio_example() -> str:
    """Render bare Claude Desktop stdio example with registry server ids."""

    mcp_servers: dict[str, Any] = {}
    for spec in SERVER_REGISTRY:
        entry: dict[str, Any] = {
            "command": "hc-mcp",
            "args": [spec.server_id],
        }
        if spec.server_id == "public-records":
            entry["env"] = {"HC_MCP_ENV_FILE": ENV_PLACEHOLDER}
        mcp_servers[spec.server_id] = entry
    return json.dumps({"mcpServers": mcp_servers}, indent=2) + "\n"


def render_claude_desktop_config() -> str:
    """Render shared Claude Desktop stdio config for all registry servers."""

    payload = {
        "mcpServers": {
            f"hc-{spec.server_id}": {
                "command": "hc-mcp",
                "args": [spec.server_id],
                "env": {"HC_MCP_ENV_FILE": ENV_PLACEHOLDER},
            }
            for spec in SERVER_REGISTRY
        }
    }
    return json.dumps(payload, indent=2) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Render registry-backed MCP client configs.")
    parser.add_argument(
        "target",
        choices=("codex", "http-clients", "project-mcp", "claude-desktop-stdio", "claude-desktop"),
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the checked-in config target matches the registry renderer without printing.",
    )
    args = parser.parse_args()

    renderers: dict[str, Callable[[], str]] = {
        "codex": render_codex_config,
        "http-clients": render_http_clients_config,
        "project-mcp": render_project_mcp_config,
        "claude-desktop-stdio": render_claude_desktop_stdio_example,
        "claude-desktop": render_claude_desktop_config,
    }
    target_paths = {
        "codex": REPO_ROOT / "examples" / "codex-config.toml",
        "http-clients": REPO_ROOT / "configs" / "http-clients.json",
        "project-mcp": REPO_ROOT / ".mcp.json",
        "claude-desktop-stdio": REPO_ROOT / "examples" / "claude-desktop-stdio.json",
        "claude-desktop": REPO_ROOT / "configs" / "claude-desktop.json",
    }
    rendered = renderers[args.target]()
    if args.check:
        target_path = target_paths[args.target]
        current = target_path.read_text(encoding="utf-8")
        if current != rendered:
            print(
                f"{target_path} is not current; regenerate it with scripts/render_client_configs.py {args.target}",
                file=sys.stderr,
            )
            raise SystemExit(1)
        print(f"Registry-rendered client config is current: {target_path}")
        return
    print(rendered, end="")


if __name__ == "__main__":
    main()
