"""Interactive setup wizard for healthcare-data-mcp environment values."""

from __future__ import annotations

import argparse
import getpass
import hashlib
import re
import secrets
from dataclasses import dataclass
from pathlib import Path

from shared.utils.env_file import read_env_file, write_env_file


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV = Path.cwd() / ".env"
DEFAULT_TEMPLATE = REPO_ROOT / ".env.example"


@dataclass(frozen=True)
class ConfigKey:
    name: str
    prompt: str
    required: bool = False
    secret: bool = True
    help_text: str = ""


CONFIG_KEYS: tuple[ConfigKey, ...] = (
    ConfigKey("SAM_GOV_API_KEY", "SAM.gov API key for Exclusions and opportunities", help_text="Required for SAM.gov API-backed tools."),
    ConfigKey("CHPL_API_KEY", "ONC CHPL API key for EHR enrichment", help_text="Optional public-records enrichment."),
    ConfigKey("SEC_USER_AGENT", "SEC EDGAR User-Agent", required=True, secret=False, help_text='Required format: "AppName email@example.com".'),
    ConfigKey("CENSUS_API_KEY", "Census API key", help_text="Optional geo-demographics rate-limit improvement."),
    ConfigKey("HUD_API_TOKEN", "HUD USPS Crosswalk token", help_text="Optional ZIP crosswalk support."),
    ConfigKey("ORS_API_KEY", "OpenRouteService API key", help_text="Optional drive-time isochrones."),
    ConfigKey("BLS_API_KEY", "BLS API key", help_text="Optional workforce analytics rate-limit improvement."),
    ConfigKey("GOOGLE_CSE_API_KEY", "Google Custom Search API key", help_text="Optional web-intelligence search."),
    ConfigKey("GOOGLE_CSE_ID", "Google Custom Search Engine ID", secret=False, help_text="Used with GOOGLE_CSE_API_KEY."),
    ConfigKey("PROXYCURL_API_KEY", "Proxycurl API key", help_text="Optional web-intelligence enrichment."),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="hc-mcp-setup",
        description="Create, update, and validate healthcare-data-mcp .env configuration.",
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV, help="Path to write/read. Default: .env in the repo.")
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE, help="Template dotenv file.")
    parser.add_argument("--set", action="append", default=[], metavar="KEY=VALUE", help="Set a value non-interactively.")
    parser.add_argument("--interactive", action="store_true", help="Prompt for missing or changed values.")
    parser.add_argument("--skip-optional", action="store_true", help="Only prompt for required values in interactive mode.")
    parser.add_argument("--validate-only", action="store_true", help="Validate the selected env file without writing changes.")
    parser.add_argument("--generate-gateway-token", action="store_true", help="Generate a gateway bearer token and store only its SHA-256 hash.")
    parser.add_argument("--print-client-snippets", action="store_true", help="Print install snippets for common MCP clients.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    current = read_env_file(args.env_file)
    updates = dict(current)
    updates.update(_parse_set_values(args.set))

    if args.generate_gateway_token:
        token = secrets.token_urlsafe(32)
        updates["MCP_GATEWAY_AUTH_REQUIRED"] = "true"
        updates["MCP_GATEWAY_BEARER_TOKEN_SHA256"] = hashlib.sha256(token.encode()).hexdigest()
        print("Generated gateway bearer token. Store this now; only its SHA-256 hash is written to .env:")
        print(token)

    if args.interactive:
        updates.update(prompt_for_values(updates, required_only=args.skip_optional))

    errors = validate_env(updates)
    if errors:
        print("Configuration warnings/errors:")
        for error in errors:
            print(f"- {error}")

    if not args.validate_only:
        write_env_file(args.env_file, updates, template_path=args.template)
        print(f"Wrote {args.env_file}")

    if args.print_client_snippets:
        print_client_snippets(args.env_file)


def prompt_for_values(current: dict[str, str], *, required_only: bool = False) -> dict[str, str]:
    """Prompt for config values. Existing values are kept on blank input."""
    updates: dict[str, str] = {}
    for item in CONFIG_KEYS:
        if required_only and not item.required:
            continue

        existing = current.get(item.name, "")
        required_marker = " required" if item.required else " optional"
        print(f"\n{item.name} ({required_marker})")
        if item.help_text:
            print(item.help_text)
        if existing:
            print("Current value: [set]" if item.secret else f"Current value: {existing}")

        prompt = f"{item.prompt} (blank keeps current): "
        value = getpass.getpass(prompt) if item.secret else input(prompt)
        if value:
            updates[item.name] = value.strip()
        elif item.name not in current:
            updates[item.name] = ""
    return updates


def validate_env(values: dict[str, str]) -> list[str]:
    """Return validation messages for important configuration values."""
    messages: list[str] = []
    sec_user_agent = values.get("SEC_USER_AGENT", "").strip()
    if not sec_user_agent:
        messages.append("SEC_USER_AGENT is required for financial-intelligence SEC EDGAR tools.")
    elif "@" not in sec_user_agent or "example.com" in sec_user_agent.lower():
        messages.append("SEC_USER_AGENT should include a real contact email and must not use example.com.")

    sam_key = values.get("SAM_GOV_API_KEY", "").strip()
    if not sam_key:
        messages.append("SAM_GOV_API_KEY is empty; SAM.gov Exclusions tools will return a missing-key response.")

    gateway_hash = values.get("MCP_GATEWAY_BEARER_TOKEN_SHA256", "").strip()
    if gateway_hash and not re.fullmatch(r"[A-Fa-f0-9]{64}", gateway_hash):
        messages.append("MCP_GATEWAY_BEARER_TOKEN_SHA256 must be a 64-character SHA-256 hex digest.")

    google_key = values.get("GOOGLE_CSE_API_KEY", "").strip()
    google_id = values.get("GOOGLE_CSE_ID", "").strip()
    if bool(google_key) != bool(google_id):
        messages.append("GOOGLE_CSE_API_KEY and GOOGLE_CSE_ID should be set together.")

    return messages


def print_client_snippets(env_file: Path) -> None:
    """Print concise setup snippets for common MCP clients."""
    env_path = env_file.resolve()
    print(
        f"""
Client snippets

Codex CLI / Codex IDE:
  codex mcp add publicRecords --env HC_MCP_ENV_FILE={env_path} -- hc-mcp public-records
  codex mcp add providerEnrollment --env HC_MCP_ENV_FILE={env_path} -- hc-mcp provider-enrollment

Claude Code:
  claude mcp add public-records --env HC_MCP_ENV_FILE={env_path} -- hc-mcp public-records
  claude mcp add provider-enrollment --env HC_MCP_ENV_FILE={env_path} -- hc-mcp provider-enrollment

Claude Desktop stdio JSON:
  {{
    "mcpServers": {{
      "public-records": {{
        "command": "hc-mcp",
        "args": ["public-records"],
        "env": {{"HC_MCP_ENV_FILE": "{env_path}"}}
      }}
    }}
  }}
"""
    )


def _parse_set_values(items: list[str]) -> dict[str, str]:
    updates: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"--set must be KEY=VALUE, got: {item}")
        key, value = item.split("=", 1)
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise SystemExit(f"Invalid environment key: {key}")
        updates[key] = value
    return updates


if __name__ == "__main__":
    main()
