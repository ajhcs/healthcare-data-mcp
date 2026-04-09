"""Launcher for healthcare-data-mcp servers.

Usage:
    hc-mcp-all                    # Start all servers via Docker Compose
    hc-mcp-all --list             # List available servers
    hc-mcp-all --server cms-facility  # Start a single server (stdio)
    python -m servers._launcher   # Same as hc-mcp-all
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

SERVERS = {
    "service-area":              ("servers.service_area.server",              8002),
    "geo-demographics":          ("servers.geo_demographics.server",          8003),
    "drive-time":                ("servers.drive_time.server",                8004),
    "hospital-quality":          ("servers.hospital_quality.server",          8005),
    "cms-facility":              ("servers.cms_facility.server",              8006),
    "health-system-profiler":    ("servers.health_system_profiler.server",    8007),
    "financial-intelligence":    ("servers.financial_intelligence.server",    8008),
    "price-transparency":        ("servers.price_transparency.server",        8009),
    "physician-referral-network":("servers.physician_referral_network.server",8010),
    "workforce-analytics":       ("servers.workforce_analytics.server",       8011),
    "claims-analytics":          ("servers.claims_analytics.server",          8012),
    "public-records":            ("servers.public_records.server",            8013),
    "web-intelligence":          ("servers.web_intelligence.server",          8014),
}


def find_project_root() -> Path:
    """Walk up from this file to find docker-compose.yml."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "docker-compose.yml").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(
        description="Healthcare Data MCP server launcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--list", action="store_true", help="List all servers")
    parser.add_argument("--server", "-s", help="Start a single server by name (stdio)")
    parser.add_argument("--http", action="store_true", help="Use HTTP transport instead of stdio")
    parser.add_argument("--port", type=int, help="Override port (HTTP mode)")
    args = parser.parse_args()

    if args.list:
        print("Available servers:\n")
        for name, (module, port) in sorted(SERVERS.items()):
            print(f"  {name:30s}  module={module}  port={port}")
        print(f"\nTotal: {len(SERVERS)} servers")
        return

    if args.server:
        name = args.server
        if name.startswith("hc-"):
            name = name[3:]  # Strip prefix
        if name not in SERVERS:
            print(f"Unknown server: {name}")
            print(f"Available: {', '.join(sorted(SERVERS))}")
            sys.exit(1)

        module, default_port = SERVERS[name]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(find_project_root())

        if args.http:
            port = args.port or default_port
            env["MCP_TRANSPORT"] = "streamable-http"
            env["MCP_PORT"] = str(port)
            print(f"Starting {name} on http://0.0.0.0:{port}/mcp")

        print(f"Starting {name} (module: {module})...")
        sys.exit(subprocess.call([sys.executable, "-m", module], env=env))

    # Default: try docker compose
    root = find_project_root()
    compose_file = root / "docker-compose.yml"
    if compose_file.exists():
        print("Starting all servers via Docker Compose...")
        print(f"  compose file: {compose_file}")
        sys.exit(subprocess.call(
            ["docker", "compose", "-f", str(compose_file), "up", "-d"],
            cwd=str(root),
        ))
    else:
        print("No docker-compose.yml found.")
        print("Start a single server: hc-mcp-all --server cms-facility")
        print("List servers:          hc-mcp-all --list")
        sys.exit(1)


if __name__ == "__main__":
    main()
