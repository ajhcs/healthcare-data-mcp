"""Health Intel MCP Server.

Stdio-transport MCP server providing tools for health system intelligence
data collection from public APIs:
- CMS Provider Data Catalog (quality, safety, HCAHPS, spending)
- ProPublica Nonprofit Explorer (IRS 990 financials)
- NPPES NPI Registry (provider network data)
- Report rendering (HTML → PDF via WeasyPrint)

A product of Open-Informatics.org
"""

import asyncio
import json
from pathlib import Path
from datetime import datetime
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from tools.systems import SYSTEMS, get_system, list_systems
from tools.cms_quality import (
    get_hospital_general_info,
    get_quality_measures,
    get_readmission_measures,
    get_patient_satisfaction,
    get_medicare_spending,
    get_timely_effective_care,
    get_healthcare_infections,
    get_all_cms_data,
)
from tools.propublica import (
    get_organization_990,
    search_organizations as search_nonprofits,
    get_health_system_financials,
)
from tools.nppes import (
    lookup_npi,
    search_organizations as search_npi_orgs,
    get_provider_taxonomy_summary,
    get_health_system_providers,
)

server = Server("health-intel")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # --- System Lookup ---
        Tool(
            name="list_health_systems",
            description="List all available health systems in the database with their identifiers.",
            inputSchema={"type": "object", "properties": {}, "required": []},
        ),
        Tool(
            name="get_health_system_info",
            description="Get basic information and all identifiers (CMS ID, EIN, NPI) for a health system.",
            inputSchema={
                "type": "object",
                "properties": {
                    "system_key": {
                        "type": "string",
                        "description": "System key (e.g. 'jefferson_health', 'cooper_health', 'temple_health')",
                    }
                },
                "required": ["system_key"],
            },
        ),
        # --- CMS Quality Data ---
        Tool(
            name="get_cms_hospital_info",
            description="Get hospital general information including overall star rating, type, ownership, emergency services, and measure group scores from CMS Provider Data Catalog.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {
                        "type": "string",
                        "description": "CMS Facility ID (CCN). E.g. '390174' for Jefferson.",
                    }
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_cms_quality_measures",
            description="Get complications and deaths (mortality) measures from CMS. Includes mortality rates for heart attack, heart failure, pneumonia, COPD, stroke, CABG.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_cms_readmissions",
            description="Get hospital readmission rates and Hospital Readmissions Reduction Program data from CMS.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_cms_patient_satisfaction",
            description="Get HCAHPS patient satisfaction survey results from CMS. Covers 10 dimensions including communication, responsiveness, cleanliness, discharge info.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_cms_spending",
            description="Get Medicare spending per beneficiary data from CMS. Shows cost efficiency compared to national average.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_cms_timely_care",
            description="Get timely and effective care measures from CMS. Covers ED wait times, immunizations, preventive care.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_cms_infections",
            description="Get healthcare-associated infection measures from CMS. Covers CLABSI, CAUTI, SSI, MRSA, C.diff rates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        Tool(
            name="get_all_cms_data",
            description="Fetch ALL CMS datasets for a hospital at once (general info, quality, readmissions, HCAHPS, spending, timely care, infections). Use for comprehensive analysis.",
            inputSchema={
                "type": "object",
                "properties": {
                    "facility_id": {"type": "string", "description": "CMS Facility ID"},
                },
                "required": ["facility_id"],
            },
        ),
        # --- Financial Data (990s) ---
        Tool(
            name="get_nonprofit_990",
            description="Get IRS Form 990 filings for a nonprofit hospital by EIN. Returns revenue, expenses, assets, executive compensation, employee counts across all available filing years.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ein": {
                        "type": "string",
                        "description": "EIN without dashes (e.g. '232829095' for Jefferson)",
                    }
                },
                "required": ["ein"],
            },
        ),
        Tool(
            name="search_nonprofits",
            description="Search for nonprofit organizations by name and state. Useful for finding related entities in a health system.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query (organization name)"},
                    "state": {"type": "string", "description": "Two-letter state code (optional)"},
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="get_health_system_financials",
            description="Get comprehensive financial data for a health system including all related entities. Returns 5 years of 990 data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "system_key": {"type": "string", "description": "System key (e.g. 'jefferson_health')"},
                },
                "required": ["system_key"],
            },
        ),
        # --- Provider Network (NPI) ---
        Tool(
            name="lookup_npi",
            description="Look up a specific NPI number. Returns organization details, addresses, and taxonomy/specialty.",
            inputSchema={
                "type": "object",
                "properties": {
                    "npi": {"type": "string", "description": "NPI number"},
                },
                "required": ["npi"],
            },
        ),
        Tool(
            name="search_npi_organizations",
            description="Search NPI registry for organizations by name, state, and city. Returns up to 200 matching NPIs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "organization_name": {"type": "string", "description": "Organization name to search"},
                    "state": {"type": "string", "description": "State abbreviation (optional)"},
                    "city": {"type": "string", "description": "City name (optional)"},
                },
                "required": ["organization_name"],
            },
        ),
        Tool(
            name="get_provider_specialties",
            description="Get a summary of provider types and specialties for an organization from NPI data.",
            inputSchema={
                "type": "object",
                "properties": {
                    "organization_name": {"type": "string", "description": "Organization name"},
                    "state": {"type": "string", "description": "State abbreviation (optional)"},
                    "city": {"type": "string", "description": "City name (optional)"},
                },
                "required": ["organization_name"],
            },
        ),
        Tool(
            name="get_health_system_providers",
            description="Get comprehensive provider network data for a health system including org NPIs and specialty distribution.",
            inputSchema={
                "type": "object",
                "properties": {
                    "system_key": {"type": "string", "description": "System key (e.g. 'jefferson_health')"},
                },
                "required": ["system_key"],
            },
        ),
        # --- Data Collection ---
        Tool(
            name="collect_all_data",
            description="Collect ALL data from ALL sources (CMS, 990, NPI) for a health system and save to a JSON file. Returns the file path and a summary. Use this before generating a report.",
            inputSchema={
                "type": "object",
                "properties": {
                    "system_key": {"type": "string", "description": "System key (e.g. 'jefferson_health')"},
                },
                "required": ["system_key"],
            },
        ),
        # --- Report Rendering ---
        Tool(
            name="render_report_pdf",
            description=(
                "Render a health system intelligence report as PDF. Takes the system key "
                "and a list of report sections (each with title and HTML content). "
                "Returns the file path to the generated PDF. The HTML content should be "
                "well-formatted with tables, headings, and data. Claude provides the analysis "
                "content; this tool handles the PDF rendering with professional styling."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "system_key": {
                        "type": "string",
                        "description": "System key for the report",
                    },
                    "sections": {
                        "type": "array",
                        "description": "Report sections in order",
                        "items": {
                            "type": "object",
                            "properties": {
                                "title": {"type": "string", "description": "Section title"},
                                "content": {"type": "string", "description": "HTML content for this section"},
                            },
                            "required": ["title", "content"],
                        },
                    },
                },
                "required": ["system_key", "sections"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        result = await _dispatch(name, arguments)
        return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
    except Exception as e:
        error = {"error": str(e), "tool": name, "arguments": arguments}
        return [TextContent(type="text", text=json.dumps(error, indent=2))]


async def _dispatch(name: str, args: dict):
    # System lookup
    if name == "list_health_systems":
        return list_systems()
    if name == "get_health_system_info":
        sys = get_system(args["system_key"])
        return {
            "key": sys.key,
            "name": sys.name,
            "short_name": sys.short_name,
            "city": sys.city,
            "state": sys.state,
            "system_type": sys.system_type,
            "description": sys.description,
            "identifiers": {
                "cms_facility_id": sys.cms_facility_id,
                "ein": sys.ein,
                "primary_npi": sys.primary_npi,
            },
        }

    # CMS Quality
    if name == "get_cms_hospital_info":
        return await get_hospital_general_info(args["facility_id"])
    if name == "get_cms_quality_measures":
        return await get_quality_measures(args["facility_id"])
    if name == "get_cms_readmissions":
        return await get_readmission_measures(args["facility_id"])
    if name == "get_cms_patient_satisfaction":
        return await get_patient_satisfaction(args["facility_id"])
    if name == "get_cms_spending":
        return await get_medicare_spending(args["facility_id"])
    if name == "get_cms_timely_care":
        return await get_timely_effective_care(args["facility_id"])
    if name == "get_cms_infections":
        return await get_healthcare_infections(args["facility_id"])
    if name == "get_all_cms_data":
        return await get_all_cms_data(args["facility_id"])

    # Financial
    if name == "get_nonprofit_990":
        return await get_organization_990(args["ein"])
    if name == "search_nonprofits":
        return await search_nonprofits(args["query"], args.get("state", ""))
    if name == "get_health_system_financials":
        return await get_health_system_financials(args["system_key"])

    # NPI
    if name == "lookup_npi":
        return await lookup_npi(args["npi"])
    if name == "search_npi_organizations":
        return await search_npi_orgs(
            args["organization_name"],
            args.get("state", ""),
            args.get("city", ""),
        )
    if name == "get_provider_specialties":
        return await get_provider_taxonomy_summary(
            args["organization_name"],
            args.get("state", ""),
            args.get("city", ""),
        )
    if name == "get_health_system_providers":
        return await get_health_system_providers(args["system_key"])

    # Data collection
    if name == "collect_all_data":
        from collector import collect_and_save
        path = await collect_and_save(args["system_key"], "output")
        # Return summary
        with open(path) as f:
            data = json.load(f)
        return {
            "file_path": str(path.resolve()),
            "file_size_kb": round(path.stat().st_size / 1024, 1),
            "system": data.get("system_profile", {}).get("name"),
            "sections_collected": list(data.keys()),
            "message": f"Data collected and saved to {path}. Use render_report_pdf to generate the report.",
        }

    # Report rendering
    if name == "render_report_pdf":
        return _render_report(args["system_key"], args["sections"])

    raise ValueError(f"Unknown tool: {name}")


def _render_report(system_key: str, sections: list[dict]) -> dict:
    """Render report sections as a professional PDF."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from markupsafe import Markup

    system = get_system(system_key)
    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=select_autoescape(default=True),
    )
    template = env.get_template("report.html")

    today = datetime.now().strftime("%B %d, %Y")

    section_data = []
    for i, sec in enumerate(sections):
        section_data.append({
            "number": i + 1,
            "title": sec["title"],
            "key": sec["title"].lower().replace(" ", "_").replace("&", "and"),
            # Content is trusted HTML written by Claude — mark safe for Jinja2
            "content": Markup(sec["content"]),
        })

    html = template.render(
        system_name=system.name,
        short_name=system.short_name,
        report_date=today,
        sections=section_data,
        metadata={
            "system_key": system_key,
            "version": "1.0.0",
            "product": "Open-Informatics Health System Intelligence Report",
        },
    )

    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Try PDF, fall back to HTML
    try:
        from weasyprint import HTML as WeasyprintHTML
        pdf_path = output_dir / f"{system_key}_report.pdf"
        WeasyprintHTML(string=html).write_pdf(str(pdf_path))
        return {
            "format": "pdf",
            "file_path": str(pdf_path.resolve()),
            "file_size_kb": round(pdf_path.stat().st_size / 1024, 1),
            "sections_rendered": len(sections),
            "message": f"PDF report saved to {pdf_path.resolve()}",
        }
    except ImportError:
        html_path = output_dir / f"{system_key}_report.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        return {
            "format": "html",
            "file_path": str(html_path.resolve()),
            "file_size_kb": round(html_path.stat().st_size / 1024, 1),
            "sections_rendered": len(sections),
            "message": f"HTML report saved to {html_path.resolve()} (install weasyprint for PDF)",
        }


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
