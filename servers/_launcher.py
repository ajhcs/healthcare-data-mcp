"""Console launcher for the healthcare-data-mcp server collection."""

from __future__ import annotations

import argparse
import os
import runpy
from dataclasses import dataclass

from shared.utils.env_file import load_env_file


@dataclass(frozen=True)
class ServerSpec:
    module: str
    port: int
    description: str


SERVERS: dict[str, ServerSpec] = {
    "service-area": ServerSpec("servers.service_area.server", 8002, "CMS hospital service areas and market share"),
    "geo-demographics": ServerSpec("servers.geo_demographics.server", 8003, "Census, ZCTA, Medicare, and HUD geography"),
    "drive-time": ServerSpec("servers.drive_time.server", 8004, "Routing, drive-time matrices, and access scoring"),
    "hospital-quality": ServerSpec("servers.hospital_quality.server", 8005, "CMS quality, readmission, and safety data"),
    "cms-facility": ServerSpec("servers.cms_facility.server", 8006, "CMS facility master data and NPPES lookup"),
    "health-system-profiler": ServerSpec(
        "servers.health_system_profiler.server",
        8007,
        "Health system discovery and facility enrichment",
    ),
    "financial-intelligence": ServerSpec(
        "servers.financial_intelligence.server",
        8008,
        "IRS 990, SEC EDGAR, and nonprofit finance intelligence",
    ),
    "price-transparency": ServerSpec("servers.price_transparency.server", 8009, "Hospital MRF and benchmark pricing"),
    "physician-referral-network": ServerSpec(
        "servers.physician_referral_network.server",
        8010,
        "NPPES, physician mix, referral network, and leakage analysis",
    ),
    "workforce-analytics": ServerSpec("servers.workforce_analytics.server", 8011, "BLS and ACGME workforce analytics"),
    "claims-analytics": ServerSpec("servers.claims_analytics.server", 8012, "DRG, service-line, and claims analytics"),
    "public-records": ServerSpec(
        "servers.public_records.server",
        8013,
        "SAM.gov, USAspending, CHPL, accreditation, and exclusion screening",
    ),
    "web-intelligence": ServerSpec("servers.web_intelligence.server", 8014, "Web search and health system OSINT"),
    "discovery": ServerSpec("servers.discovery.server", 8015, "Dataset catalog resources, cache status, and prompts"),
    "gateway": ServerSpec("servers.gateway.server", 8016, "Remote-safe metadata gateway with search/fetch"),
    "live-gateway": ServerSpec(
        "servers.live_gateway.server",
        8020,
        "Authenticated live router for approved provider, quality, claims, compliance, community, and research tools",
    ),
    "provider-enrollment": ServerSpec(
        "servers.provider_enrollment.server",
        8017,
        "CMS PECOS-derived provider enrollment, ownership, and CHOW",
    ),
    "community-health": ServerSpec(
        "servers.community_health.server",
        8018,
        "CDC PLACES community-health estimates for counties, places, tracts, and ZCTAs",
    ),
    "research-trials": ServerSpec(
        "servers.research_trials.server",
        8019,
        "NIH RePORTER funding and ClinicalTrials.gov study activity",
    ),
}


def main() -> None:
    parser = argparse.ArgumentParser(prog="hc-mcp", description="Run one healthcare-data-mcp server.")
    parser.add_argument("server", nargs="?", choices=sorted(SERVERS), help="Server to run")
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
    args = parser.parse_args()

    load_env_file(args.env_file)

    if args.list:
        for name, spec in sorted(SERVERS.items(), key=lambda item: item[1].port):
            print(f"{name:28} {spec.port}  {spec.description}")
        return

    if not args.server:
        parser.error("choose a server or pass --list")

    spec = SERVERS[args.server]
    os.environ["MCP_TRANSPORT"] = args.transport or os.environ.get("MCP_TRANSPORT", "stdio")
    os.environ["MCP_PORT"] = str(args.port or int(os.environ.get("MCP_PORT", spec.port)))
    runpy.run_module(spec.module, run_name="__main__")


if __name__ == "__main__":
    main()
