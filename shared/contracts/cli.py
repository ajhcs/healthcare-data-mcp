"""CLI for versioned Healthcare Data MCP transport contracts."""

from __future__ import annotations

import argparse
from pathlib import Path

from shared.contracts.public_evidence import PublicEvidenceBundleInput, build_public_evidence_bundle


def main() -> None:
    parser = argparse.ArgumentParser(prog="hc-mcp-contract")
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build-public-evidence", help="Build a canonical Public Evidence Bundle v1.")
    build.add_argument("--input", type=Path, required=True, help="PublicEvidenceBundleInput JSON path.")
    build.add_argument("--output", type=Path, help="Output JSON path. Defaults to stdout.")
    args = parser.parse_args()

    if args.command == "build-public-evidence":
        value = PublicEvidenceBundleInput.model_validate_json(args.input.read_text(encoding="utf-8"))
        payload = build_public_evidence_bundle(value).model_dump_json(indent=2) + "\n"
        if args.output is None:
            print(payload, end="")
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(payload, encoding="utf-8")


if __name__ == "__main__":
    main()
