"""Console launcher for the healthcare-data-mcp server collection."""

from __future__ import annotations

import argparse
import os
import runpy
from dataclasses import dataclass

from shared.utils.doctor import package_version, print_doctor
from shared.utils.env_file import load_env_file
from shared.utils.presets import print_preset_plan
from shared.utils.server_registry import SERVER_REGISTRY
from shared.utils.workflows import parse_workflow_inputs, print_workflow_plan


@dataclass(frozen=True)
class ServerSpec:
    module: str
    port: int
    description: str


SERVERS: dict[str, ServerSpec] = {
    spec.server_id: ServerSpec(spec.module, spec.port, spec.description)
    for spec in SERVER_REGISTRY
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="hc-mcp", description="Run one healthcare-data-mcp server.")
    parser.add_argument("server", nargs="?", help="Server to run, or 'doctor', 'workflow', or 'preset'.")
    parser.add_argument("target", nargs="?", help="Workflow or preset name for task-first commands.")
    parser.add_argument(
        "--transport",
        choices=("stdio", "sse", "streamable-http"),
        default=None,
        help="MCP transport. Defaults to MCP_TRANSPORT or stdio.",
    )
    parser.add_argument("--port", type=int, default=None, help="HTTP/SSE port. Defaults to the server's standard port.")
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional dotenv file to load before starting the server. Defaults to HC_MCP_ENV_FILE or ./.env.",
    )
    parser.add_argument("--list", action="store_true", help="List available servers and ports.")
    parser.add_argument("--version", action="store_true", help="Print the installed healthcare-data-mcp version.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON for doctor, workflow, or preset.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="With 'doctor', exit non-zero when readiness status is not ready.",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Workflow input identifier. Repeat for multiple inputs. Only valid with 'workflow'.",
    )
    parser.add_argument(
        "--inputs-json",
        default=None,
        help="Workflow input identifiers as a JSON object. Only valid with 'workflow'.",
    )
    args = parser.parse_args()

    if args.version:
        if args.server or args.target:
            parser.error("--version does not accept positional arguments")
        print(f"healthcare-data-mcp {package_version()}")
        return

    load_env_file(args.env_file)

    if args.server == "doctor":
        if args.target:
            parser.error("doctor does not accept an extra positional argument")
        report = print_doctor(json_output=args.json)
        if args.check and report.get("status") != "ready":
            raise SystemExit(1)
        return
    if args.check:
        parser.error("--check is only valid with 'doctor'")
    if args.server == "workflow":
        if not args.target and (args.input or args.inputs_json):
            parser.error("--input and --inputs-json require a workflow name")
        try:
            workflow_inputs = parse_workflow_inputs(input_items=args.input, inputs_json=args.inputs_json)
        except ValueError as exc:
            parser.error(str(exc))
        print_workflow_plan(args.target, json_output=args.json, inputs=workflow_inputs or None)
        return
    if args.server == "preset":
        if args.input or args.inputs_json:
            parser.error("--input and --inputs-json are only valid with 'workflow'")
        print_preset_plan(args.target, json_output=args.json)
        return

    if args.list:
        if args.target:
            parser.error("--list does not accept an extra positional argument")
        for name, spec in sorted(SERVERS.items(), key=lambda item: item[1].port):
            print(f"{name:28} {spec.port}  {spec.description}")
        return

    if not args.server:
        parser.error("choose a server or pass --list")
    if args.input or args.inputs_json:
        parser.error("--input and --inputs-json are only valid with 'workflow'")
    if args.target:
        parser.error(f"unexpected extra argument {args.target!r}")
    if args.server not in SERVERS:
        parser.error(
            f"unknown server {args.server!r}; choose one of {', '.join(sorted(SERVERS))}, doctor, workflow, or preset"
        )

    spec = SERVERS[args.server]
    os.environ["MCP_TRANSPORT"] = args.transport or os.environ.get("MCP_TRANSPORT", "stdio")
    os.environ["MCP_PORT"] = str(args.port or int(os.environ.get("MCP_PORT", spec.port)))
    runpy.run_module(spec.module, run_name="__main__")


if __name__ == "__main__":
    main()
