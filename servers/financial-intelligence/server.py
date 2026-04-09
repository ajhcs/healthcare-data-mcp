"""Financial Intelligence MCP Server.

Provides tools for IRS Form 990 nonprofit financials, SEC EDGAR corporate
filings, and municipal bond data from public APIs.
"""

import asyncio
import json
import logging
import os as _os

from mcp.server.fastmcp import FastMCP

from . import edgar_client, propublica_client
from .irs990_parser import download_990_xml, lookup_xml_url, parse_990_xml
from .models import (
    Form990Details,
    Form990Summary,
    MuniBond,
    MuniBondDetails,
    Officer,
    SecFiling,
    SecFilingDetail,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "financial-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8008"))
mcp = FastMCP(**_mcp_kwargs)


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _first_present(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _build_form990_summary(search_org: dict, org_data: dict | None) -> dict:
    details = org_data or {}
    organization = details.get("organization", {})
    filings = details.get("filings_with_data", [])
    latest_filing = filings[0] if filings else {}

    return Form990Summary(
        ein=str(_first_present(search_org.get("ein"), organization.get("ein"), "")),
        name=_first_present(search_org.get("name"), organization.get("name"), "") or "",
        city=_first_present(search_org.get("city"), organization.get("city"), "") or "",
        state=_first_present(search_org.get("state"), organization.get("state"), "") or "",
        ntee_code=_first_present(search_org.get("ntee_code"), organization.get("ntee_code"), "") or "",
        total_revenue=_safe_float(
            _first_present(
                latest_filing.get("totrevenue"),
                organization.get("revenue_amount"),
                search_org.get("revenue_amount"),
            )
        ),
        total_expenses=_safe_float(
            _first_present(
                latest_filing.get("totfuncexpns"),
                organization.get("expenses_amount"),
                search_org.get("expenses_amount"),
            )
        ),
        net_assets=_safe_float(
            _first_present(
                latest_filing.get("totnetassetend"),
                latest_filing.get("totassetsend"),
                organization.get("asset_amount"),
                search_org.get("asset_amount"),
            )
        ),
        tax_period=str(
            _first_present(
                latest_filing.get("tax_prd"),
                latest_filing.get("tax_prd_yr"),
                search_org.get("tax_period"),
                organization.get("tax_period"),
            )
            or ""
        ),
    ).model_dump()


# ---------------------------------------------------------------------------
# Tool 1: search_form990
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_form990(query: str, state: str = "", ntee_code: str = "") -> str:
    """Search IRS Form 990 filings by organization name or EIN.

    Returns nonprofit organizations with revenue, expenses, and net assets
    from the most recent filing.

    Args:
        query: Organization name or EIN to search for.
        state: Two-letter state code filter (e.g. "OH").
        ntee_code: NTEE category code filter (1-10).
    """
    try:
        data = await propublica_client.search_organizations(query, state=state, ntee_code=ntee_code)
        orgs = data.get("organizations", [])

        limited_orgs = orgs[:25]
        org_details = await asyncio.gather(
            *(propublica_client.get_organization(str(org.get("ein", ""))) for org in limited_orgs)
        )
        results = [
            _build_form990_summary(org, detail)
            for org, detail in zip(limited_orgs, org_details, strict=False)
        ]

        return json.dumps({"total_results": data.get("total_results", 0), "organizations": results})
    except Exception as e:
        logger.exception("search_form990 failed")
        return json.dumps({"error": f"search_form990 failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_form990_details
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_form990_details(ein: str) -> str:
    """Get detailed Form 990 data for a nonprofit by EIN.

    Returns revenue breakdown, functional expenses (Part IX), Schedule H
    community benefit (hospitals), officer compensation, and program descriptions.
    Parses the full IRS e-file XML when available; falls back to ProPublica summary.

    Args:
        ein: Employer Identification Number (e.g. "341323166").
    """
    try:
        org_data = await propublica_client.get_organization(ein)
        if not org_data:
            return json.dumps({"error": f"Organization not found for EIN: {ein}"})

        org = org_data.get("organization", {})
        filings = org_data.get("filings_with_data", [])

        if not filings:
            return json.dumps({"error": f"No filings with data found for EIN: {ein}"})

        latest = filings[0]
        tax_period = str(latest.get("tax_prd", latest.get("tax_prd_yr", "")))

        # Try to get XML URL — ProPublica may or may not include it
        xml_url = latest.get("xml_url", "")

        # If ProPublica doesn't provide XML URL, try IRS e-file index
        if not xml_url:
            xml_url = await lookup_xml_url(ein, tax_period) or ""

        if xml_url:
            xml_path = await download_990_xml(xml_url, ein, tax_period)
            if xml_path:
                parsed = parse_990_xml(xml_path)
                result = Form990Details(
                    ein=ein,
                    name=org.get("name", ""),
                    tax_period=tax_period,
                    source="xml",
                    contributions=parsed.get("contributions"),
                    program_service_revenue=parsed.get("program_service_revenue"),
                    investment_income=parsed.get("investment_income"),
                    other_revenue=parsed.get("other_revenue"),
                    total_revenue=parsed.get("total_revenue"),
                    total_expenses=parsed.get("total_expenses"),
                    program_expenses=parsed.get("program_expenses"),
                    management_expenses=parsed.get("management_expenses"),
                    fundraising_expenses=parsed.get("fundraising_expenses"),
                    community_benefit_total=parsed.get("community_benefit_total"),
                    community_benefit_pct=parsed.get("community_benefit_pct"),
                    officers=[Officer(**o) for o in parsed.get("officers", [])],
                    program_descriptions=parsed.get("program_descriptions", []),
                )
                return json.dumps(result.model_dump())

        # Fallback: ProPublica summary data only
        result = Form990Details(
            ein=ein,
            name=org.get("name", ""),
            tax_period=tax_period,
            source="propublica",
            total_revenue=_safe_float(latest.get("totrevenue")),
            total_expenses=_safe_float(latest.get("totfuncexpns")),
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_form990_details failed")
        return json.dumps({"error": f"get_form990_details failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: search_sec_filings
# Uses ACTUAL EFTS response structure: hits.hits[]._source with fields:
# adsh, display_names[], ciks[], form, file_date
# Deduplicates by adsh (each file in a filing is a separate hit)
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_sec_filings(query: str, filing_type: str = "10-K", date_from: str = "", date_to: str = "") -> str:
    """Search SEC EDGAR filings by company name, CIK, or keyword.

    Returns a list of filings with accession numbers, filing dates, and links.

    Args:
        query: Company name, CIK number, or keyword to search.
        filing_type: SEC form type filter (e.g. "10-K", "10-Q", "8-K"). Default "10-K".
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).
    """
    try:
        data = await edgar_client.search_filings(query, forms=filing_type, date_from=date_from, date_to=date_to)

        hits_obj = data.get("hits", {})
        raw_hits = hits_obj.get("hits", [])
        total_obj = hits_obj.get("total", {})
        total_count = total_obj.get("value", 0) if isinstance(total_obj, dict) else 0

        # Deduplicate by accession number (adsh) — EFTS returns one hit per file, not per filing
        seen_adsh = set()
        results = []
        for hit in raw_hits:
            source = hit.get("_source", {})
            adsh = source.get("adsh", "")
            if not adsh or adsh in seen_adsh:
                continue
            seen_adsh.add(adsh)

            # Extract company name from display_names array
            display_names = source.get("display_names", [])
            company_name = display_names[0] if display_names else ""

            # Extract CIK from ciks array
            ciks = source.get("ciks", [])
            cik = ciks[0] if ciks else ""

            # Construct filing URL from accession number and CIK
            acc_no_hyphens = adsh.replace("-", "")
            unpadded_cik = cik.lstrip("0") or "0"
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{unpadded_cik}/{acc_no_hyphens}/{adsh}-index.htm"

            results.append(SecFiling(
                accession_number=adsh,
                company_name=company_name,
                cik=cik,
                form_type=source.get("form", filing_type),
                filing_date=source.get("file_date", ""),
                filing_url=filing_url,
            ).model_dump())

            if len(results) >= 25:
                break

        return json.dumps({"total_results": total_count, "filings": results})
    except Exception as e:
        logger.exception("search_sec_filings failed")
        return json.dumps({"error": f"search_sec_filings failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: get_sec_filing
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_sec_filing(accession_number: str, sections: list[str] | None = None) -> str:
    """Get detailed data from a specific SEC filing.

    Retrieves structured XBRL financial data and/or narrative sections (MD&A,
    Risk Factors) from 10-K/10-Q filings.

    Args:
        accession_number: EDGAR accession number (e.g. "0000320193-24-000058").
        sections: Which sections to retrieve. Options: "financials", "debt", "mda", "risk_factors". Default ["financials"].
    """
    if sections is None:
        sections = ["financials"]

    try:
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return json.dumps({"error": f"Could not determine CIK from accession number: {accession_number}"})

        submissions = await edgar_client.get_company_submissions(cik)
        company_name = submissions.get("name", "")
        form_type = ""
        filing_date = ""

        recent = submissions.get("filings", {}).get("recent", {})
        accession_numbers = recent.get("accessionNumber", [])
        for i, acc in enumerate(accession_numbers):
            if acc == accession_number:
                form_type = recent.get("form", [])[i] if i < len(recent.get("form", [])) else ""
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        result = SecFilingDetail(
            accession_number=accession_number,
            company_name=company_name,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
        )

        if "financials" in sections or "debt" in sections:
            facts = await edgar_client.get_company_facts(cik)
            if "financials" in sections:
                result.financials = edgar_client.extract_financials(facts)
            if "debt" in sections:
                result.debt_summary = edgar_client.extract_debt_summary(facts)

        if "mda" in sections or "risk_factors" in sections:
            html = await edgar_client.download_filing_html(cik, accession_number)
            if html:
                if "mda" in sections:
                    result.mda_text = edgar_client.extract_section(html, "mda")
                if "risk_factors" in sections:
                    result.risk_factors_text = edgar_client.extract_section(html, "risk_factors")

        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_sec_filing failed")
        return json.dumps({"error": f"get_sec_filing failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: search_muni_bonds
# Same EFTS structure, but with forms="OS"
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_muni_bonds(query: str, state: str = "", date_from: str = "", date_to: str = "") -> str:
    """Search municipal bond offerings via SEC EDGAR Official Statements.

    Returns municipal bond filings with issuer name, filing date, and accession number.

    Args:
        query: Issuer name or keyword to search.
        state: Two-letter state code filter (e.g. "CA").
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).
    """
    try:
        search_query = query
        if state:
            search_query = f"{query} {state}"

        data = await edgar_client.search_filings(search_query, forms="OS", date_from=date_from, date_to=date_to)

        hits_obj = data.get("hits", {})
        raw_hits = hits_obj.get("hits", [])
        total_obj = hits_obj.get("total", {})
        total_count = total_obj.get("value", 0) if isinstance(total_obj, dict) else 0

        seen_adsh = set()
        results = []
        for hit in raw_hits:
            source = hit.get("_source", {})
            adsh = source.get("adsh", "")
            if not adsh or adsh in seen_adsh:
                continue
            seen_adsh.add(adsh)

            display_names = source.get("display_names", [])
            issuer_name = display_names[0] if display_names else ""

            ciks = source.get("ciks", [])
            cik = ciks[0] if ciks else ""
            acc_no_hyphens = adsh.replace("-", "")
            unpadded_cik = cik.lstrip("0") or "0"
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{unpadded_cik}/{acc_no_hyphens}/{adsh}-index.htm"

            # Try to extract state from biz_locations or biz_states
            biz_states = source.get("biz_states", [])
            hit_state = biz_states[0] if biz_states else state

            results.append(MuniBond(
                accession_number=adsh,
                issuer_name=issuer_name,
                state=hit_state,
                filing_date=source.get("file_date", ""),
                filing_url=filing_url,
            ).model_dump())

            if len(results) >= 25:
                break

        return json.dumps({"total_results": total_count, "bonds": results})
    except Exception as e:
        logger.exception("search_muni_bonds failed")
        return json.dumps({"error": f"search_muni_bonds failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 6: get_muni_bond_details
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_muni_bond_details(accession_number: str) -> str:
    """Get details for a specific municipal bond filing from EDGAR.

    Returns the issuer information, filing documents list, and links to
    the Official Statement PDF.

    Args:
        accession_number: EDGAR accession number for the Official Statement.
    """
    try:
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return json.dumps({"error": f"Could not determine CIK from accession number: {accession_number}"})

        submissions = await edgar_client.get_company_submissions(cik)
        issuer_name = submissions.get("name", "")
        filing_date = ""

        recent = submissions.get("filings", {}).get("recent", {})
        for i, acc in enumerate(recent.get("accessionNumber", [])):
            if acc == accession_number:
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        index_data = await edgar_client.get_filing_index(cik, accession_number)

        result = MuniBondDetails(
            accession_number=accession_number,
            issuer_name=issuer_name,
            filing_date=filing_date,
            documents=index_data.get("documents", []),
            description=index_data.get("description", ""),
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_muni_bond_details failed")
        return json.dumps({"error": f"get_muni_bond_details failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
