"""Render registry-backed Docker Compose files."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path

from shared.utils.server_registry import SERVER_REGISTRY, EnvKey, ServerCapability

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_VOLUME = "healthcare-cache:/root/.healthcare-data-mcp/cache"
DEFAULT_IMAGE_NAME = "healthcare-data-mcp"
TARGET_PATHS = {
    "full": REPO_ROOT / "docker-compose.yml",
    "zero-config": REPO_ROOT / "docker-compose.zero-config.yml",
}

ENV_DEFAULTS = {
    "OSRM_BASE_URL": "http://router.project-osrm.org",
    "MCP_GATEWAY_AUTH_REQUIRED": "false",
    "MCP_GATEWAY_REQUIRED_SCOPES": "mcp:read",
    "MCP_GATEWAY_ALLOWED_HOSTS": "localhost:8016,127.0.0.1:8016",
    "MCP_GATEWAY_ALLOWED_ORIGINS": "http://localhost:*,http://127.0.0.1:*",
    "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
    "MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND": "true",
    "MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND": "false",
    "MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE": "false",
    "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "mcp:read",
    "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "localhost:8020,127.0.0.1:8020",
    "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://localhost:*,http://127.0.0.1:*",
}

EXTRA_ENV_BY_SERVER = {
    "gateway": (
        EnvKey("MCP_GATEWAY_AUTH_REQUIRED"),
        EnvKey("MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST"),
        EnvKey("MCP_GATEWAY_REQUIRED_SCOPES"),
        EnvKey("MCP_GATEWAY_ALLOWED_HOSTS"),
        EnvKey("MCP_GATEWAY_ALLOWED_ORIGINS"),
        EnvKey("MCP_GATEWAY_PUBLIC_URL"),
        EnvKey("MCP_GATEWAY_ISSUER_URL"),
    ),
    "live-gateway": (
        EnvKey("MCP_LIVE_GATEWAY_AUTH_REQUIRED"),
        EnvKey("MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND"),
        EnvKey("MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256_LIST"),
        EnvKey("MCP_LIVE_GATEWAY_REQUIRED_SCOPES"),
        EnvKey("MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE"),
        EnvKey("MCP_LIVE_GATEWAY_ALLOWED_HOSTS"),
        EnvKey("MCP_LIVE_GATEWAY_ALLOWED_ORIGINS"),
        EnvKey("MCP_LIVE_GATEWAY_PUBLIC_URL"),
        EnvKey("MCP_LIVE_GATEWAY_ISSUER_URL"),
        EnvKey("MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND"),
        EnvKey("MCP_LIVE_GATEWAY_AUDIT_LOG_PATH"),
    ),
}


def render_compose(*, zero_config_only: bool = False) -> str:
    """Render docker-compose YAML from the canonical server registry."""

    specs = [spec for spec in SERVER_REGISTRY if not zero_config_only or spec.zero_config]
    lines: list[str] = []
    if zero_config_only:
        lines.extend(
            [
                "# Healthcare Data MCP - Zero-Config Docker Compose",
                "# Registry-backed local profile for servers that do not require API keys.",
                "#",
                "# Usage:",
                "#   docker compose -f docker-compose.zero-config.yml up -d",
                "#",
                "# Host ports bind to 127.0.0.1. Put a gateway behind HTTPS/auth for remote access.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "# Healthcare Data MCP - Docker Compose",
                "# Generated from shared.utils.server_registry via scripts/render_compose.py.",
                "# Host ports bind to 127.0.0.1. Put gateway/live-gateway behind HTTPS/auth for remote access.",
                "",
            ]
        )
    lines.append("services:")
    for index, spec in enumerate(specs):
        if index:
            lines.append("")
        lines.extend(_service_lines(spec))
    lines.extend(["", "volumes:", "  healthcare-cache:", "    driver: local"])
    return "\n".join(lines) + "\n"


def package_version(pyproject_path: Path = REPO_ROOT / "pyproject.toml") -> str:
    """Return the package version used for local Compose image tags."""

    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return str(data["project"]["version"])


def compose_image_reference(image_name: str = DEFAULT_IMAGE_NAME) -> str:
    """Return the default versioned image reference with an operator override."""

    return "$" + "{" + f"HC_MCP_IMAGE:-{image_name}:{package_version()}" + "}"


def _service_lines(spec: ServerCapability) -> list[str]:
    lines = [
        f"  {spec.server_id}:",
        "    build: .",
        f'    image: "{compose_image_reference()}"',
        "    pull_policy: missing",
        f"    command: python -m {spec.module}",
    ]
    if spec.server_id == "live-gateway":
        lines.append('    profiles: ["live-gateway"]')
    lines.extend(
        [
            "    ports:",
            f'      - "127.0.0.1:{spec.port}:{spec.port}"',
            "    environment:",
            "      - MCP_TRANSPORT=streamable-http",
            "      - MCP_HOST=0.0.0.0",
            f"      - MCP_PORT={spec.port}",
        ]
    )
    for env_key in _compose_env_keys(spec):
        lines.append(f"      - {env_key.name}=${{{env_key.name}:-{ENV_DEFAULTS.get(env_key.name, '')}}}")
    lines.extend(
        [
            "    volumes:",
            f"      - {CACHE_VOLUME}",
            "    restart: unless-stopped",
            "    healthcheck:",
            f'      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection((\'localhost\',{spec.port}),5); s.close()"]',
            "      interval: 60s",
            "      timeout: 10s",
            "      retries: 3",
            "      start_period: 30s",
        ]
    )
    return lines


def _compose_env_keys(spec: ServerCapability) -> tuple[EnvKey, ...]:
    ordered: list[EnvKey] = []
    seen: set[str] = set()
    for env_key in (
        *spec.required_env,
        *spec.optional_env,
        *_live_gateway_routed_env_keys(spec),
        *EXTRA_ENV_BY_SERVER.get(spec.server_id, ()),
    ):
        if env_key.name not in seen:
            ordered.append(env_key)
            seen.add(env_key.name)
    return tuple(ordered)


def _live_gateway_routed_env_keys(spec: ServerCapability) -> tuple[EnvKey, ...]:
    if spec.server_id != "live-gateway":
        return ()
    return tuple(
        env_key
        for routed_spec in SERVER_REGISTRY
        if routed_spec.server_id != spec.server_id and "live" in routed_spec.gateway_exposure
        for env_key in (*routed_spec.required_env, *routed_spec.optional_env)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Render registry-backed Docker Compose YAML.")
    parser.add_argument("target", choices=("full", "zero-config"))
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the checked-in Compose file matches the registry renderer without printing.",
    )
    args = parser.parse_args()

    rendered = render_compose(zero_config_only=args.target == "zero-config")
    if args.check:
        target_path = TARGET_PATHS[args.target]
        current = target_path.read_text(encoding="utf-8")
        if current != rendered:
            print(f"{target_path} is not current; regenerate it with scripts/render_compose.py {args.target}", file=sys.stderr)
            raise SystemExit(1)
        print(f"Registry-rendered Compose file is current: {target_path}")
        return
    print(rendered, end="")


if __name__ == "__main__":
    main()
