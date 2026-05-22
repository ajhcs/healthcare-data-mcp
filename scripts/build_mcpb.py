"""Build a local Claude Desktop MCPB package for healthcare-data-mcp.

The script stages files under build/mcpb and writes a .mcpb zip archive. It
does not modify project source files or global client configuration.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from shared.utils.server_registry import SERVER_REGISTRY, EnvKey, server_ids


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "desktop-extension" / "manifest.json"
DEFAULT_OUTPUT = REPO_ROOT / "dist" / "healthcare-data-mcp.mcpb"
DEFAULT_STAGE = REPO_ROOT / "build" / "mcpb" / "healthcare-data-mcp"
LAUNCHER_SOURCE = REPO_ROOT / "desktop-extension" / "server" / "launcher.py"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"

SERVER_NAMES = set(server_ids())
SERVER_CHOICE_VALUES = tuple(server_ids())
MCPB_EXCLUDED_ENV_KEYS = frozenset({"MCP_LIVE_GATEWAY_CONTAINER_LOCAL_BIND"})

EXCLUDED_PARTS = {"__pycache__", ".pytest_cache"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a Claude Desktop .mcpb package.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Source manifest.json path.")
    parser.add_argument("--stage-dir", type=Path, default=DEFAULT_STAGE, help="Temporary staging directory.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output .mcpb path.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate that the source manifest registry-derived fields are current without writing files.",
    )
    parser.add_argument(
        "--server-name",
        default="cms-facility",
        choices=sorted(SERVER_NAMES),
        help="Default healthcare-data-mcp server to write into the staged manifest.",
    )
    parser.add_argument(
        "--skip-dependency-install",
        action="store_true",
        help="Create a manifest/launcher skeleton without vendoring Python dependencies.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite an existing output file.")
    return parser.parse_args()


def server_choice_description() -> str:
    """Return a registry-derived description for the MCPB server selector."""

    choices = "; ".join(f"{spec.server_id}: {spec.description}" for spec in SERVER_REGISTRY)
    return f"Registry-backed server to run. Choices: {choices}."


def apply_registry_server_choices(server_config: dict[str, object], server_name: str) -> None:
    """Mutate a manifest server_name user config with canonical registry choices."""

    server_config["type"] = "string"
    server_config["title"] = "Healthcare Data MCP server"
    server_config["description"] = server_choice_description()
    server_config["required"] = True
    server_config["default"] = server_name
    server_config["enum"] = list(SERVER_CHOICE_VALUES)


def registry_env_keys_for_mcpb() -> tuple[EnvKey, ...]:
    """Return registry env keys exposed in the Desktop Extension UI."""

    by_name: dict[str, EnvKey] = {}
    for spec in SERVER_REGISTRY:
        for key in (*spec.required_env, *spec.optional_env):
            if key.name not in MCPB_EXCLUDED_ENV_KEYS:
                by_name[key.name] = key
    return tuple(by_name[name] for name in sorted(by_name))


def env_config_for_key(key: EnvKey) -> dict[str, object]:
    """Return one Desktop Extension user_config field from registry metadata."""

    config: dict[str, object] = {
        "type": "string",
        "title": _env_title(key.name),
        "description": key.description or f"Optional {key.name} value.",
        "required": False,
        "default": "",
    }
    if _env_is_sensitive(key.name):
        config["sensitive"] = True
    return config


def validate_manifest_registry_sync(manifest: dict[str, object], server_name: str) -> list[str]:
    """Return registry drift errors for a source Desktop Extension manifest."""

    errors: list[str] = []
    expected_version = project_version()
    if manifest.get("version") != expected_version:
        errors.append(f"manifest version {manifest.get('version')!r} does not match pyproject version {expected_version!r}")

    user_config = manifest.get("user_config")
    if not isinstance(user_config, dict):
        return ["manifest user_config must be an object"]

    expected_server_config: dict[str, object] = {}
    apply_registry_server_choices(expected_server_config, server_name)
    if user_config.get("server_name") != expected_server_config:
        errors.append("user_config.server_name does not match the canonical server registry choices")

    expected_env = {key.name: key for key in registry_env_keys_for_mcpb()}
    actual_user_env = {key for key in user_config if key != "server_name"}
    expected_env_names = set(expected_env)
    missing_user_config = sorted(expected_env_names - actual_user_env)
    extra_user_config = sorted(actual_user_env - expected_env_names)
    if missing_user_config:
        errors.append("user_config is missing registry env keys: " + ", ".join(missing_user_config))
    if extra_user_config:
        errors.append("user_config contains non-registry env keys: " + ", ".join(extra_user_config))
    for name, key in expected_env.items():
        if name in user_config and user_config.get(name) != env_config_for_key(key):
            errors.append(f"user_config.{name} does not match registry env metadata")

    mcp_config = _manifest_mcp_config(manifest)
    errors.extend(_env_placeholder_errors(mcp_config, expected_env_names, label="server.mcp_config.env"))
    platform_overrides = mcp_config.get("platform_overrides", {})
    if isinstance(platform_overrides, dict):
        for platform, override in platform_overrides.items():
            if isinstance(override, dict):
                errors.extend(
                    _env_placeholder_errors(
                        override,
                        expected_env_names,
                        label=f"server.mcp_config.platform_overrides.{platform}.env",
                    )
                )
    return errors


def project_version() -> str:
    """Return the canonical Python package version."""

    with PYPROJECT_PATH.open("rb") as handle:
        pyproject = tomllib.load(handle)
    return str(pyproject.get("project", {}).get("version") or "")


def _env_placeholder_errors(config: dict[str, object], expected_env_names: set[str], *, label: str) -> list[str]:
    env = config.get("env")
    if not isinstance(env, dict):
        return [f"{label} must be an object"]
    actual_env_names = set(env) - {"PYTHONPATH"}
    errors: list[str] = []
    missing_env = sorted(expected_env_names - actual_env_names)
    extra_env = sorted(actual_env_names - expected_env_names)
    if missing_env:
        errors.append(f"{label} is missing registry env placeholders: " + ", ".join(missing_env))
    if extra_env:
        errors.append(f"{label} contains non-registry env placeholders: " + ", ".join(extra_env))
    for name in sorted(expected_env_names & actual_env_names):
        expected = f"${{user_config.{name}}}"
        if env.get(name) != expected:
            errors.append(f"{label}.{name} should be {expected}")
    return errors


def apply_registry_env_config(manifest: dict[str, object]) -> None:
    """Mutate manifest env placeholders and user_config from registry env keys."""

    user_config = manifest.setdefault("user_config", {})
    if not isinstance(user_config, dict):
        raise SystemExit("manifest user_config must be an object")

    env_keys = registry_env_keys_for_mcpb()
    for key in env_keys:
        user_config[key.name] = env_config_for_key(key)

    mcp_config = _manifest_mcp_config(manifest)
    _apply_env_placeholders(mcp_config, env_keys)
    platform_overrides = mcp_config.get("platform_overrides", {})
    if isinstance(platform_overrides, dict):
        for override in platform_overrides.values():
            if isinstance(override, dict):
                _apply_env_placeholders(override, env_keys)


def _manifest_mcp_config(manifest: dict[str, object]) -> dict[str, object]:
    server = manifest.get("server")
    if not isinstance(server, dict):
        raise SystemExit("manifest server must be an object")
    mcp_config = server.get("mcp_config")
    if not isinstance(mcp_config, dict):
        raise SystemExit("manifest server.mcp_config must be an object")
    return mcp_config


def _apply_env_placeholders(config: dict[str, object], env_keys: tuple[EnvKey, ...]) -> None:
    existing = config.get("env")
    env_map = {"PYTHONPATH": "${__dirname}/server/lib"}
    if isinstance(existing, dict) and "PYTHONPATH" in existing:
        env_map["PYTHONPATH"] = str(existing["PYTHONPATH"])
    for key in env_keys:
        env_map[key.name] = f"${{user_config.{key.name}}}"
    config["env"] = env_map


def _env_title(name: str) -> str:
    overrides = {
        "ACGME_PROGRAMS_CSV": "ACGME Programs CSV path",
        "BLS_API_KEY": "BLS API key",  # pragma: allowlist secret
        "CENSUS_API_KEY": "Census API key",  # pragma: allowlist secret
        "CHPL_API_KEY": "ONC CHPL API key",  # pragma: allowlist secret
        "DOCGRAPH_CSV_PATH": "DocGraph CSV path",
        "GOOGLE_CSE_API_KEY": "Google Custom Search API key",  # pragma: allowlist secret
        "GOOGLE_CSE_ID": "Google Custom Search Engine ID",
        "HUD_API_TOKEN": "HUD API token",
        "MCP_GATEWAY_BEARER_TOKEN": "Metadata gateway bearer token",
        "MCP_GATEWAY_BEARER_TOKEN_SHA256": "Metadata gateway bearer token SHA-256",
        "MCP_LIVE_GATEWAY_BEARER_TOKEN": "Live gateway bearer token",
        "MCP_LIVE_GATEWAY_BEARER_TOKEN_SHA256": "Live gateway bearer token SHA-256",
        "MCP_LIVE_GATEWAY_ALLOW_GLOBAL_BULK_SCOPE": "Live gateway global bulk scope opt-in",
        "MCP_LIVE_GATEWAY_REQUIRED_SCOPES": "Live gateway required scopes",
        "ORS_API_KEY": "OpenRouteService API key",  # pragma: allowlist secret
        "OSRM_BASE_URL": "OSRM base URL",
        "PROXYCURL_API_KEY": "Proxycurl API key",  # pragma: allowlist secret
        "SAM_GOV_API_KEY": "SAM.gov API key",  # pragma: allowlist secret
        "SEC_USER_AGENT": "SEC EDGAR User-Agent",
    }
    return overrides.get(name, name.replace("_", " ").title())


def _env_is_sensitive(name: str) -> bool:
    return any(token in name for token in ("API_KEY", "TOKEN", "SHA256"))


def require_within_repo(path: Path, label: str) -> Path:
    resolved = path.resolve()
    if not resolved.is_relative_to(REPO_ROOT):
        raise SystemExit(f"{label} must stay inside the repository: {resolved}")
    return resolved


def load_manifest(path: Path, server_name: str) -> dict[str, object]:
    with path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)

    required = {"manifest_version", "name", "version", "description", "author", "server"}
    missing = sorted(required - set(manifest))
    if missing:
        raise SystemExit(f"manifest missing required fields: {', '.join(missing)}")

    user_config = manifest.setdefault("user_config", {})
    if not isinstance(user_config, dict):
        raise SystemExit("manifest user_config must be an object")

    server_config = user_config.setdefault("server_name", {})
    if not isinstance(server_config, dict):
        raise SystemExit("manifest user_config.server_name must be an object")
    apply_registry_server_choices(server_config, server_name)
    apply_registry_env_config(manifest)

    return manifest


def reset_stage(stage_dir: Path) -> None:
    stage_dir = require_within_repo(stage_dir, "stage-dir")
    build_root = (REPO_ROOT / "build").resolve()
    if not stage_dir.is_relative_to(build_root):
        raise SystemExit(f"stage-dir must stay under {build_root}")

    if stage_dir.exists():
        shutil.rmtree(stage_dir)

    (stage_dir / "server").mkdir(parents=True)


def install_dependencies(stage_dir: Path) -> None:
    target = stage_dir / "server" / "lib"
    target.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--target",
        str(target),
        str(REPO_ROOT),
    ]
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def copy_runtime_files(stage_dir: Path, manifest: dict[str, object]) -> None:
    shutil.copy2(LAUNCHER_SOURCE, stage_dir / "server" / "launcher.py")
    with (stage_dir / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")


def should_zip(path: Path) -> bool:
    if any(part in EXCLUDED_PARTS for part in path.parts):
        return False
    return path.suffix not in EXCLUDED_SUFFIXES


def write_archive(stage_dir: Path, output: Path, force: bool) -> None:
    output = require_within_repo(output, "output")
    if output.exists() and not force:
        raise SystemExit(f"output already exists; pass --force to overwrite: {output}")

    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(stage_dir.rglob("*")):
            if path.is_file() and should_zip(path.relative_to(stage_dir)):
                archive.write(path, path.relative_to(stage_dir).as_posix())


def main() -> None:
    args = parse_args()
    manifest_path = require_within_repo(args.manifest, "manifest")

    if args.check:
        with manifest_path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
        errors = validate_manifest_registry_sync(manifest, args.server_name)
        if errors:
            for error in errors:
                print(f"manifest registry drift: {error}", file=sys.stderr)
            raise SystemExit(1)
        print(f"Manifest registry fields are current: {manifest_path}")
        return

    stage_dir = require_within_repo(args.stage_dir, "stage-dir")
    manifest = load_manifest(manifest_path, args.server_name)
    reset_stage(stage_dir)
    copy_runtime_files(stage_dir, manifest)

    if args.skip_dependency_install:
        print("Skipping dependency install; package is a manifest/launcher skeleton only.")
    else:
        install_dependencies(stage_dir)

    write_archive(stage_dir, args.output, args.force)
    print(f"Wrote {args.output.resolve()}")


if __name__ == "__main__":
    main()
