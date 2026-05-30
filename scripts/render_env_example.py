"""Render the registry-backed .env.example template."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from shared.utils.server_registry import SERVER_REGISTRY

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_EXAMPLE = REPO_ROOT / ".env.example"


@dataclass(frozen=True, slots=True)
class EnvTemplateEntry:
    """One environment variable rendered in .env.example."""

    name: str
    default: str = ""
    comment: str = ""


OPERATIONAL_ENV = {
    "HC_MCP_ENV_FILE": "Optional explicit dotenv path for stdio clients launched outside the repo root.",
    "MCP_HOST": "MCP HTTP bind host. Local runs default to 127.0.0.1.",
}

DEFAULTS = {
    "OSRM_BASE_URL": "http://router.project-osrm.org",
    "GOOGLE_CSE_SESSION_LIMIT": "40",
    "GOOGLE_CSE_CACHE_TTL_SECONDS": "21600",
    "GOOGLE_CSE_DAILY_LIMIT": "100",
    "CLINICAL_TRIALS_INVENTORY_HARD_MAX": "5000",
    "MCP_HOST": "127.0.0.1",
    "MCP_GATEWAY_AUTH_REQUIRED": "false",
    "MCP_GATEWAY_REQUIRED_SCOPES": "mcp:read",
    "MCP_GATEWAY_TOKEN_SCOPES": "",
    "MCP_GATEWAY_ALLOWED_HOSTS": "localhost:8016,127.0.0.1:8016",
    "MCP_GATEWAY_ALLOWED_ORIGINS": "http://localhost:*,http://127.0.0.1:*",
    "MCP_LIVE_GATEWAY_AUTH_REQUIRED": "true",
    "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "mcp:read",
    "MCP_LIVE_GATEWAY_TOKEN_SCOPES": "",
    "MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE": "false",
    "MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND": "false",
    "MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND": "false",
    "MCP_LIVE_GATEWAY_ALLOWED_HOSTS": "localhost:8020,127.0.0.1:8020",
    "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS": "http://localhost:*,http://127.0.0.1:*",
    "HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS": "false",
    "MRF_MAX_DOWNLOAD_BYTES": "10737418240",
    "MRF_MIN_FREE_BYTES": "2147483648",
    "MRF_DOWNLOAD_PROGRESS_INTERVAL_BYTES": "104857600",
}

SECTIONS = (
    (
        "Optional API keys, public source limits, and local imports",
        (
            "CENSUS_API_KEY",
            "HUD_API_TOKEN",
            "ORS_API_KEY",
            "OSRM_BASE_URL",
            "BLS_API_KEY",
            "ACGME_PROGRAMS_CSV",
            "DOCGRAPH_CSV_PATH",
            "SAM_GOV_API_KEY",
            "CHPL_API_KEY",
            "GOOGLE_CSE_API_KEY",
            "GOOGLE_CSE_ID",
            "GOOGLE_CSE_SESSION_LIMIT",
            "GOOGLE_CSE_CACHE_TTL_SECONDS",
            "GOOGLE_CSE_DAILY_LIMIT",
            "PROXYCURL_API_KEY",
            "PLACES_CACHE_DIR",
            "CLINICAL_TRIALS_INVENTORY_HARD_MAX",
        ),
    ),
    (
        "SEC EDGAR User-Agent",
        ("SEC_USER_AGENT",),
    ),
    (
        "Local launcher and client helpers",
        ("HC_MCP_ENV_FILE", "MCP_HOST", "HC_MCP_CACHE_ROOT"),
    ),
    (
        "Remote metadata gateway",
        (
            "MCP_GATEWAY_AUTH_REQUIRED",
            "MCP_GATEWAY_BEARER_TOKEN",
            "MCP_GATEWAY_BEARER_TOKENS",
            "MCP_GATEWAY_BEARER_TOKEN_SHA256",
            "MCP_GATEWAY_BEARER_TOKEN_SHA256_LIST",
            "MCP_GATEWAY_REQUIRED_SCOPES",
            "MCP_GATEWAY_TOKEN_SCOPES",
            "MCP_GATEWAY_ALLOWED_HOSTS",
            "MCP_GATEWAY_ALLOWED_ORIGINS",
            "MCP_GATEWAY_PUBLIC_URL",
            "MCP_GATEWAY_ISSUER_URL",
        ),
    ),
    (
        "Authenticated live gateway",
        (
            "MCP_LIVE_GATEWAY_AUTH_REQUIRED",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN",
            "MCP_LIVE_GATEWAY_BEARER_TOKENS",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256",
            "MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256_LIST",
            "MCP_LIVE_GATEWAY_REQUIRED_SCOPES",
            "MCP_LIVE_GATEWAY_TOKEN_SCOPES",
            "MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE",
            "MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND",
            "MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND",
            "MCP_LIVE_GATEWAY_ALLOWED_HOSTS",
            "MCP_LIVE_GATEWAY_ALLOWED_ORIGINS",
            "MCP_LIVE_GATEWAY_PUBLIC_URL",
            "MCP_LIVE_GATEWAY_ISSUER_URL",
            "MCP_LIVE_GATEWAY_AUDIT_LOG_PATH",
        ),
    ),
    (
        "Price transparency download guardrails",
        (
            "MRF_MAX_DOWNLOAD_BYTES",
            "MRF_MIN_FREE_BYTES",
            "MRF_DOWNLOAD_PROGRESS_INTERVAL_BYTES",
        ),
    ),
    (
        "Cache manager guardrails",
        (
            "HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS",
        ),
    ),
)


def registry_env_descriptions() -> dict[str, str]:
    descriptions: dict[str, str] = {}
    for spec in SERVER_REGISTRY:
        for key in (*spec.required_env, *spec.optional_env):
            if key.description:
                descriptions[key.name] = key.description
            else:
                descriptions.setdefault(key.name, "")
    return descriptions


def rendered_env_names() -> set[str]:
    return {name for _, names in SECTIONS for name in names}


def expected_env_names() -> set[str]:
    return set(registry_env_descriptions()) | set(OPERATIONAL_ENV)


def render_env_example() -> str:
    """Render .env.example from registry environment metadata."""

    descriptions = {**registry_env_descriptions(), **OPERATIONAL_ENV}
    missing = expected_env_names() - rendered_env_names()
    extra = rendered_env_names() - expected_env_names()
    if missing or extra:
        raise RuntimeError(
            "render_env_example section drift: "
            f"missing={sorted(missing)} extra={sorted(extra)}"
        )

    lines = [
        "# Healthcare Data MCP environment template.",
        "# Generated from shared.utils.server_registry via scripts/render_env_example.py.",
        "# Leave optional keys blank. Do not put PHI, SSNs, EINs, TINs, or patient data here.",
        "",
    ]
    for section_index, (title, names) in enumerate(SECTIONS):
        if section_index:
            lines.append("")
        lines.append(f"# {title}")
        if title == "SEC EDGAR User-Agent":
            lines.append("# Required for financial-intelligence SEC EDGAR-backed tools.")
            lines.append("# Format: YourAppName your-real-email@domain.com")
        elif title == "Local launcher and client helpers":
            lines.append("# Docker Compose sets MCP_HOST=0.0.0.0 inside containers while published ports bind to 127.0.0.1.")
            lines.append("# HC_MCP_CACHE_ROOT defaults to ~/.healthcare-data-mcp/cache when unset.")
        elif title == "Remote metadata gateway":
            lines.append("# Leave auth disabled for local-only development.")
            lines.append("# For production, terminate HTTPS at a reverse proxy and prefer SHA-256 token hashes.")
        elif title == "Authenticated live gateway":
            lines.append("# HTTP/SSE requires auth by default. Add mcp:bulk only for deployments that should expose batch screening.")
            lines.append("# Prefer MCP_LIVE_GATEWAY_TOKEN_SCOPES=<sha256>=mcp:read+mcp:bulk for selected bulk-screening tokens.")
            lines.append("# Global mcp:bulk is rejected unless MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE=true.")
            lines.append("# MCP_LIVE_GATEWAY_ALLOW_NETWORK_BIND=true also requires HTTPS public URL and explicit Host/Origin allow-lists.")
        elif title == "Cache manager guardrails":
            lines.append("# Keep remote cache mutations disabled unless cache-manager is deployed behind explicit local-safe auth.")

        for name in names:
            comment = descriptions.get(name, "")
            if comment and title not in {"SEC EDGAR User-Agent", "Local launcher and client helpers"}:
                lines.append(f"# {comment}")
            lines.append(f"{name}={DEFAULTS.get(name, '')}")
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render registry-backed .env.example.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the checked-in .env.example matches the registry renderer without printing.",
    )
    parser.add_argument("--path", type=Path, default=DEFAULT_ENV_EXAMPLE, help="Path to compare when using --check.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rendered = render_env_example()
    if args.check:
        current = args.path.read_text(encoding="utf-8")
        if current != rendered:
            print(f"{args.path} is not current; regenerate it with scripts/render_env_example.py", file=sys.stderr)
            raise SystemExit(1)
        print(f"Registry-rendered env template is current: {args.path}")
        return
    print(rendered, end="")


if __name__ == "__main__":
    main()
