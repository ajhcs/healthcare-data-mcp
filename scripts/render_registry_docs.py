"""Render registry-backed documentation snippets."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterable
from pathlib import Path

from shared.utils.server_registry import CURATED_PRESETS, SERVER_REGISTRY, WORKFLOW_PRESETS, EnvKey, ServerCapability

REPO_ROOT = Path(__file__).resolve().parents[1]


def render_server_catalog(specs: Iterable[ServerCapability] = SERVER_REGISTRY) -> str:
    """Render the README server catalog table from the canonical registry."""

    rows = ["| Server | Port | Domain | Dataset IDs |", "| --- | ---: | --- | --- |"]
    for spec in specs:
        datasets = ", ".join(f"`{dataset_id}`" for dataset_id in spec.dataset_ids) or "none"
        rows.append(f"| `{spec.server_id}` | {spec.port} | {spec.description} | {datasets} |")
    return "\n".join(rows)


def render_preset_catalog() -> str:
    """Render a compact curated preset table."""

    rows = ["| Preset | Servers | Workflows |", "| --- | ---: | --- |"]
    for preset in sorted(CURATED_PRESETS.values(), key=lambda item: item.preset_id):
        workflows = ", ".join(f"`{workflow_id}`" for workflow_id in preset.workflow_ids) or "none"
        rows.append(f"| `{preset.preset_id}` | {len(preset.server_ids)} | {workflows} |")
    return "\n".join(rows)


def render_workflow_catalog() -> str:
    """Render the task workflow table from the canonical workflow registry."""

    rows = ["| Workflow | Primary servers |", "|---|---|"]
    for workflow_id, server_ids in WORKFLOW_PRESETS.items():
        servers = ", ".join(f"`{server_id}`" for server_id in server_ids)
        rows.append(f"| `{workflow_id}` | {servers} |")
    return "\n".join(rows)


def render_live_gateway_catalog() -> str:
    """Render the live-gateway routed server table from the canonical registry."""

    rows = ["| Live-routed server | Domain | Required/optional env keys |", "| --- | --- | --- |"]
    for spec in SERVER_REGISTRY:
        if spec.server_id == "live-gateway" or "live" not in spec.gateway_exposure:
            continue
        env_keys = ", ".join(f"`{name}`" for name in spec.all_env_names) or "none"
        rows.append(f"| `{spec.server_id}` | {spec.description} | {env_keys} |")
    return "\n".join(rows)


def render_source_ledger_registry(specs: Iterable[ServerCapability] = SERVER_REGISTRY) -> str:
    """Render source-ledger server/source coverage from the canonical registry."""

    rows = [
        "| Server | Dataset IDs | Cache/API readiness | Gateway exposure | Safety notes |",
        "| --- | --- | --- | --- | --- |",
    ]
    for spec in specs:
        datasets = _code_list(spec.dataset_ids)
        readiness = _registry_readiness_summary(spec)
        exposure = _code_list(spec.gateway_exposure)
        safety = "; ".join(spec.safety_notes) or "Follow tool evidence receipts, source metadata, and workflow caveats."
        rows.append(
            f"| `{spec.server_id}` | {datasets} | {_escape_table_cell(readiness)} | "
            f"{exposure} | {_escape_table_cell(safety)} |"
        )
    return "\n".join(rows)


def render_http_client_catalog() -> str:
    """Render local HTTP endpoints from the canonical server registry."""

    rows = ["| Server | Local HTTP URL | Gateway exposure |", "| --- | --- | --- |"]
    for spec in SERVER_REGISTRY:
        exposure = ", ".join(f"`{item}`" for item in spec.gateway_exposure) or "none"
        rows.append(f"| `{spec.server_id}` | `http://localhost:{spec.port}/mcp` | {exposure} |")
    return "\n".join(rows)


def _code_list(values: Iterable[str]) -> str:
    items = tuple(values)
    return ", ".join(f"`{value}`" for value in items) if items else "none"


def _registry_readiness_summary(spec: ServerCapability) -> str:
    parts = [f"zero-config: {'yes' if spec.zero_config else 'no'}"]
    if spec.cache_needs:
        parts.append("cache: " + ", ".join(spec.cache_needs))
    if spec.required_env:
        parts.append("required env: " + ", ".join(key.name for key in spec.required_env))
    if spec.optional_env:
        parts.append("optional env: " + ", ".join(key.name for key in spec.optional_env))
    return "; ".join(parts)


def render_env_catalog() -> str:
    """Render registry-backed environment key documentation."""

    env_rows: dict[str, dict[str, object]] = {}
    for spec in SERVER_REGISTRY:
        for key in (*spec.required_env, *spec.optional_env):
            row = env_rows.setdefault(
                key.name,
                {
                    "key": key,
                    "required": False,
                    "servers": [],
                    "description": key.description,
                },
            )
            row["required"] = bool(row["required"]) or key.required
            row["servers"].append(spec.server_id)  # type: ignore[union-attr]
            if key.description and len(key.description) > len(str(row["description"])):
                row["description"] = key.description

    rows = ["| Key | Required | Servers | Purpose |", "| --- | ---: | --- | --- |"]
    for key_name in sorted(env_rows):
        row = env_rows[key_name]
        env_key = row["key"]
        assert isinstance(env_key, EnvKey)
        required = "yes" if row["required"] else "no"
        servers = ", ".join(f"`{server_id}`" for server_id in sorted(set(row["servers"])))  # type: ignore[arg-type]
        rows.append(f"| `{env_key.name}` | {required} | {servers} | {_escape_table_cell(str(row['description']))} |")
    return "\n".join(rows)


def _escape_table_cell(value: str) -> str:
    return value.replace("|", "\\|")


def checked_in_snippet(snippet: str) -> tuple[Path, str]:
    """Return the checked-in docs table corresponding to a rendered snippet."""

    if snippet == "server-catalog":
        path = REPO_ROOT / "README.md"
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r"## Server Catalog\n\n(?P<table>\| Server \| Port \| Domain \| Dataset IDs \|\n(?:\|.*\|\n)+)",
            text,
        )
        if not match:
            raise RuntimeError("README server catalog table not found")
        return path, match.group("table").strip()

    if snippet == "preset-catalog":
        path = REPO_ROOT / "README.md"
        text = path.read_text(encoding="utf-8")
        try:
            section = text.split("## Preset Catalog\n\n", 1)[1].split("\n\n## Why Use It?", 1)[0]
        except IndexError as exc:
            raise RuntimeError("README preset catalog table not found") from exc
        return path, section.split("\n\n", 1)[1].strip()

    if snippet == "workflow-catalog":
        path = REPO_ROOT / "docs" / "TASK_WORKFLOWS.md"
        text = path.read_text(encoding="utf-8")
        try:
            section = text.split("## Other Canonical Workflow Presets\n\n", 1)[1].split(
                "\n\nUse `hc-mcp doctor --json`",
                1,
            )[0]
        except IndexError as exc:
            raise RuntimeError("TASK_WORKFLOWS workflow catalog table not found") from exc
        return path, section.split("\n\n", 1)[1].strip()

    if snippet == "live-gateway-catalog":
        path = REPO_ROOT / "docs" / "REMOTE_GATEWAY.md"
        text = path.read_text(encoding="utf-8")
        try:
            section = text.split("Registry-backed live-gateway server catalog:\n\n", 1)[1].split(
                "\n\n## Security Configuration",
                1,
            )[0]
        except IndexError as exc:
            raise RuntimeError("REMOTE_GATEWAY live-gateway catalog table not found") from exc
        return path, section.strip()

    if snippet == "source-ledger-registry":
        path = REPO_ROOT / "docs" / "SOURCE_CAPABILITY_LEDGER.md"
        text = path.read_text(encoding="utf-8")
        try:
            section = text.split("Registry-rendered server/source coverage:\n\n", 1)[1].split(
                "\n\n## Source Boundaries",
                1,
            )[0]
        except IndexError as exc:
            raise RuntimeError("SOURCE_CAPABILITY_LEDGER registry coverage table not found") from exc
        return path, section.strip()

    if snippet == "http-client-catalog":
        path = REPO_ROOT / "docs" / "MCP_CLIENTS.md"
        text = path.read_text(encoding="utf-8")
        try:
            section = text.split("Local HTTP ports are rendered into `configs/http-clients.json`:\n\n", 1)[1].split(
                "\n\nRegenerate checked-in client configs",
                1,
            )[0]
        except IndexError as exc:
            raise RuntimeError("MCP_CLIENTS local HTTP catalog table not found") from exc
        return path, section.strip()

    if snippet == "env-catalog":
        path = REPO_ROOT / "README.md"
        text = path.read_text(encoding="utf-8")
        match = re.search(
            r"Registry-backed environment key catalog:\n\n(?P<table>\| Key \| Required \| Servers \| Purpose \|\n(?:\|.*\|\n?)+)",
            text,
        )
        if not match:
            raise RuntimeError("README environment key catalog table not found")
        return path, match.group("table").strip()

    raise RuntimeError(f"Unknown docs snippet: {snippet}")


def render_snippet(snippet: str) -> str:
    if snippet == "server-catalog":
        return render_server_catalog()
    if snippet == "preset-catalog":
        return render_preset_catalog()
    if snippet == "workflow-catalog":
        return render_workflow_catalog()
    if snippet == "live-gateway-catalog":
        return render_live_gateway_catalog()
    if snippet == "source-ledger-registry":
        return render_source_ledger_registry()
    if snippet == "http-client-catalog":
        return render_http_client_catalog()
    if snippet == "env-catalog":
        return render_env_catalog()
    raise RuntimeError(f"Unknown docs snippet: {snippet}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render registry-backed docs snippets.")
    parser.add_argument(
        "snippet",
        choices=(
            "server-catalog",
            "preset-catalog",
            "workflow-catalog",
            "live-gateway-catalog",
            "source-ledger-registry",
            "http-client-catalog",
            "env-catalog",
        ),
        help="Snippet to render.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the checked-in docs section matches the registry renderer without printing the snippet.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rendered = render_snippet(args.snippet).strip()
    if args.check:
        try:
            target_path, current = checked_in_snippet(args.snippet)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(1) from exc
        if current != rendered:
            print(
                f"{target_path} {args.snippet} is not current; regenerate it with scripts/render_registry_docs.py {args.snippet}",
                file=sys.stderr,
            )
            raise SystemExit(1)
        print(f"Registry-rendered docs snippet is current: {target_path} ({args.snippet})")
        return
    print(rendered)


if __name__ == "__main__":
    main()
