"""Curated task-first install/use presets for healthcare-data-mcp."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from shared.utils.mcp_response import to_structured
from shared.utils.server_registry import CURATED_PRESETS, SERVER_BY_ID
from shared.utils.workflows import list_workflow_plans


def list_presets() -> dict[str, Any]:
    """List curated server presets."""

    return {
        "preset_count": len(CURATED_PRESETS),
        "presets": [
            {
                "preset_id": preset.preset_id,
                "title": preset.title,
                "description": preset.description,
                "server_count": len(preset.server_ids),
                "workflow_ids": list(preset.workflow_ids),
            }
            for preset in sorted(CURATED_PRESETS.values(), key=lambda item: item.preset_id)
        ],
    }


def build_preset_plan(preset_id: str) -> dict[str, Any]:
    """Return one curated preset with commands, HTTP URLs, env keys, and caveats."""

    key = preset_id.strip().lower().replace("_", "-")
    preset = CURATED_PRESETS.get(key)
    if preset is None:
        return {
            "error": "preset_not_found",
            "preset_id": preset_id,
            "available_presets": sorted(CURATED_PRESETS),
        }

    server_rows = []
    required_env: dict[str, str] = {}
    optional_env: dict[str, str] = {}
    for server_id in preset.server_ids:
        spec = SERVER_BY_ID[server_id]
        for env_key in spec.required_env:
            required_env[env_key.name] = env_key.description
        for env_key in spec.optional_env:
            optional_env[env_key.name] = env_key.description
        server_rows.append(
            {
                "server_id": spec.server_id,
                "description": spec.description,
                "stdio_command": f"hc-mcp {spec.server_id}",
                "http_url": f"http://127.0.0.1:{spec.port}/mcp",
                "port": spec.port,
                "zero_config": spec.zero_config,
                "gateway_exposure": list(spec.gateway_exposure),
                "dataset_ids": list(spec.dataset_ids),
                "safety_notes": list(spec.safety_notes),
            }
        )

    workflow_catalog = {
        workflow["workflow_id"]: workflow
        for workflow in list_workflow_plans()["workflows"]
    }
    workflow_rows = []
    for workflow_id in preset.workflow_ids:
        workflow = workflow_catalog.get(workflow_id)
        if workflow is None:
            workflow_rows.append(
                {
                    "workflow_id": workflow_id,
                    "status": "workflow_not_found",
                    "plan_command": f"hc-mcp workflow {workflow_id} --json",
                }
            )
            continue
        workflow_rows.append(
            {
                "workflow_id": workflow["workflow_id"],
                "title": workflow["title"],
                "description": workflow["description"],
                "required_identifiers": workflow["required_identifiers"],
                "identity_join_keys": workflow["identity_join_keys"],
                "identity_strategy": workflow["identity_strategy"],
                "required_sources": workflow["required_sources"],
                "source_resolution": workflow["source_resolution"],
                "recommended_servers": workflow["recommended_servers"],
                "step_count": workflow["step_count"],
                "report_fact_row_count": workflow["report_fact_row_count"],
                "validation": workflow["validation"],
                "examples": workflow["examples"],
                "plan_command": f"hc-mcp workflow {workflow_id} --json",
            }
        )

    return to_structured(
        {
            **asdict(preset),
            "servers": server_rows,
            "workflow_summaries": workflow_rows,
            "required_env": required_env,
            "optional_env": optional_env,
            "docker_compose_hint": "docker compose up " + " ".join(preset.server_ids),
            "mcp_client_hint": "Use stdio commands locally; use localhost HTTP only when Docker Compose is running.",
        }
    )


def format_preset_plan(plan: dict[str, Any]) -> str:
    """Format a preset plan for CLI users."""

    if "error" in plan:
        return f"Unknown preset: {plan['preset_id']}\nAvailable presets: {', '.join(plan['available_presets'])}\n"

    lines = [
        f"{plan['title']} ({plan['preset_id']})",
        plan["description"],
        "",
        "Servers:",
    ]
    for server in plan["servers"]:
        lines.append(f"  {server['server_id']:<28} {server['stdio_command']:<32} {server['http_url']}")
    if plan.get("required_env"):
        lines.append("")
        lines.append("Required env:")
        for name, description in plan["required_env"].items():
            lines.append(f"  {name}: {description}")
    if plan.get("optional_env"):
        lines.append("")
        lines.append("Optional env:")
        for name, description in plan["optional_env"].items():
            lines.append(f"  {name}: {description}")
    if plan.get("workflow_ids"):
        lines.append("")
        lines.append("Workflows:")
        workflow_summaries = {workflow["workflow_id"]: workflow for workflow in plan.get("workflow_summaries", [])}
        for workflow_id in plan["workflow_ids"]:
            workflow = workflow_summaries.get(workflow_id, {})
            source_count = len(workflow.get("source_resolution", []))
            report_rows = workflow.get("report_fact_row_count", 0)
            suffix = f" ({source_count} sources, {report_rows} report rows)" if workflow else ""
            lines.append(f"  hc-mcp workflow {workflow_id}{suffix}")
            if workflow.get("identity_join_keys"):
                lines.append(f"      identity keys: {', '.join(workflow['identity_join_keys'])}")
            aliases = [
                source
                for source in workflow.get("source_resolution", [])
                if source.get("status") == "alias"
            ]
            if aliases:
                alias_text = ", ".join(
                    f"{source['source_id']}->{'+'.join(source.get('canonical_dataset_ids', []))}"
                    for source in aliases[:3]
                )
                lines.append(f"      source aliases: {alias_text}")
    if plan.get("safety_notes"):
        lines.append("")
        lines.append("Safety notes:")
        for note in plan["safety_notes"]:
            lines.append(f"  - {note}")
    return "\n".join(lines) + "\n"


def print_preset_plan(preset_id: str | None, *, json_output: bool = False) -> None:
    """Print preset list or one preset plan."""

    payload = build_preset_plan(preset_id) if preset_id else list_presets()
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True), end="")
    elif preset_id:
        print(format_preset_plan(payload), end="")
    else:
        print(_format_preset_list(payload), end="")


def _format_preset_list(payload: dict[str, Any]) -> str:
    lines = ["Available healthcare-data-mcp presets:"]
    for preset in payload["presets"]:
        lines.append(f"  {preset['preset_id']:<18} {preset['title']} ({preset['server_count']} servers)")
    lines.append("Run: hc-mcp preset <preset_id>")
    return "\n".join(lines) + "\n"


__all__ = ["build_preset_plan", "format_preset_plan", "list_presets", "print_preset_plan"]
