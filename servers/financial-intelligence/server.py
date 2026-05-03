"""Financial Intelligence MCP Server.

Provides tools for IRS Form 990 nonprofit financials, SEC EDGAR corporate
filings, and municipal bond data from public APIs.
"""

from typing import Any
import asyncio
import logging
import os as _os

from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, to_structured
from shared.utils.cost_report import load_cost_report_row

from . import edgar_client, propublica_client
from .audited_financial_pdf import parse_audited_financial_pdf as _parse_audited_financial_pdf
from .financial_health import load_ahrq_hfmd_profile, normalize_hcris_public_metrics
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

from servers.hospital_quality import data_loaders as hospital_quality_data_loaders

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "financial-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
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


def _reported_metric(value: Any, confidence: str, source_field: str) -> dict[str, Any]:
    return {
        "value": value,
        "confidence": confidence if value is not None else "not_available",
        "source_field": source_field if value is not None else "",
    }


async def _latest_990_schedule_h(ein: str) -> dict[str, Any]:
    if not ein:
        return {}
    org_data = await propublica_client.get_organization(ein)
    filings = org_data.get("filings_with_data", []) if org_data else []
    if not filings:
        return {"ein": ein, "source_status": "no_990_filing_found"}
    latest = filings[0]
    tax_period = str(latest.get("tax_prd", latest.get("tax_prd_yr", "")))
    xml_url = latest.get("xml_url", "") or await lookup_xml_url(ein, tax_period) or ""
    parsed: dict[str, Any] = {}
    if xml_url:
        xml_path = await download_990_xml(xml_url, ein, tax_period)
        if xml_path:
            parsed = parse_990_xml(xml_path)
    metrics = {
        "charity_care_cost": _reported_metric(
            parsed.get("charity_care_cost") or parsed.get("community_benefit_total"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "CharityCareAtCostAmt" if parsed.get("charity_care_cost") else "TotalCommunityBenefitExpnsAmt",
        ),
        "bad_debt_expense": _reported_metric(
            parsed.get("bad_debt_expense"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "BadDebtExpenseAmt",
        ),
        "medicare_shortfall": _reported_metric(
            parsed.get("medicare_shortfall"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "MedicareShortfallAmt",
        ),
        "medicaid_shortfall": _reported_metric(
            parsed.get("medicaid_shortfall"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "MedicaidShortfallAmt",
        ),
        "community_benefit_total": _reported_metric(
            parsed.get("community_benefit_total"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "TotalCommunityBenefitExpnsAmt",
        ),
        "community_benefit_pct": _reported_metric(
            parsed.get("community_benefit_pct"),
            "medium_derived_from_schedule_h_total_expenses" if parsed.get("community_benefit_pct") is not None else "not_available",
            "TotalCommunityBenefitExpnsAmt / CYTotalExpensesAmt",
        ),
        "total_revenue": _reported_metric(
            parsed.get("total_revenue") or _safe_float(latest.get("totrevenue")),
            "high_reported_irs_xml_or_propublica_summary",
            "TotalRevenueAmt or totrevenue",
        ),
        "total_expenses": _reported_metric(
            parsed.get("total_expenses") or _safe_float(latest.get("totfuncexpns")),
            "high_reported_irs_xml_or_propublica_summary",
            "CYTotalExpensesAmt or totfuncexpns",
        ),
    }
    return {
        "ein": ein,
        "tax_period": tax_period,
        "source": "IRS Form 990 Schedule H XML" if parsed else "ProPublica Form 990 summary",
        "xml_url": xml_url,
        "charity_care": metrics["charity_care_cost"]["value"],
        "bad_debt_expense": parsed.get("bad_debt_expense"),
        "medicare_shortfall": parsed.get("medicare_shortfall"),
        "medicaid_shortfall": parsed.get("medicaid_shortfall"),
        "community_benefit_total": parsed.get("community_benefit_total"),
        "community_benefit_pct": parsed.get("community_benefit_pct"),
        "total_revenue": parsed.get("total_revenue") or _safe_float(latest.get("totrevenue")),
        "total_expenses": parsed.get("total_expenses") or _safe_float(latest.get("totfuncexpns")),
        "source_status": "ready" if parsed else "summary_only",
        "metrics": metrics,
        "metric_confidence": {name: metric["confidence"] for name, metric in metrics.items()},
    }


async def _cost_report_public_metrics(ccn: str) -> dict[str, Any]:
    if not ccn:
        return {}
    row, error = await load_cost_report_row(hospital_quality_data_loaders, ccn)
    if error:
        return {"ccn": ccn, "source_status": "unavailable", "detail": error}
    return normalize_hcris_public_metrics(row, requested_ccn=ccn)


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
@mcp.tool(structured_output=True)
async def search_form990(query: str, state: str = "", ntee_code: str = "") -> dict[str, Any]:
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

        return to_structured({"total_results": data.get("total_results", 0), "organizations": results})
    except Exception as e:
        logger.exception("search_form990 failed")
        return error_response(f"search_form990 failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_form990_details
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_form990_details(ein: str) -> dict[str, Any]:
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
            return error_response(f"Organization not found for EIN: {ein}")

        org = org_data.get("organization", {})
        filings = org_data.get("filings_with_data", [])

        if not filings:
            return error_response(f"No filings with data found for EIN: {ein}")

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
                return to_structured(result.model_dump())

        # Fallback: ProPublica summary data only
        result = Form990Details(
            ein=ein,
            name=org.get("name", ""),
            tax_period=tax_period,
            source="propublica",
            total_revenue=_safe_float(latest.get("totrevenue")),
            total_expenses=_safe_float(latest.get("totfuncexpns")),
        )
        return to_structured(result.model_dump())
    except Exception as e:
        logger.exception("get_form990_details failed")
        return error_response(f"get_form990_details failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: search_sec_filings
# Uses ACTUAL EFTS response structure: hits.hits[]._source with fields:
# adsh, display_names[], ciks[], form, file_date
# Deduplicates by adsh (each file in a filing is a separate hit)
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_sec_filings(query: str, filing_type: str = "10-K", date_from: str = "", date_to: str = "") -> dict[str, Any]:
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

        return to_structured({"total_results": total_count, "filings": results})
    except Exception as e:
        logger.exception("search_sec_filings failed")
        return error_response(f"search_sec_filings failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: get_sec_filing
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_sec_filing(accession_number: str, sections: list[str] | None = None) -> dict[str, Any]:
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
            return error_response(f"Could not determine CIK from accession number: {accession_number}")

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

        return to_structured(result.model_dump())
    except Exception as e:
        logger.exception("get_sec_filing failed")
        return error_response(f"get_sec_filing failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: search_muni_bonds
# Same EFTS structure, but with forms="OS"
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_muni_bonds(query: str, state: str = "", date_from: str = "", date_to: str = "") -> dict[str, Any]:
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
                source_url=filing_url,
            ).model_dump())

            if len(results) >= 25:
                break

        return to_structured({"total_results": total_count, "bonds": results})
    except Exception as e:
        logger.exception("search_muni_bonds failed")
        return error_response(f"search_muni_bonds failed: {e}")


# ---------------------------------------------------------------------------
# Tool 6: get_muni_bond_details
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_muni_bond_details(accession_number: str) -> dict[str, Any]:
    """Get details for a specific municipal bond filing from EDGAR.

    Returns the issuer information, filing documents list, and links to
    the Official Statement PDF.

    Args:
        accession_number: EDGAR accession number for the Official Statement.
    """
    try:
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return error_response(f"Could not determine CIK from accession number: {accession_number}")

        submissions = await edgar_client.get_company_submissions(cik)
        issuer_name = submissions.get("name", "")
        filing_date = ""

        recent = submissions.get("filings", {}).get("recent", {})
        for i, acc in enumerate(recent.get("accessionNumber", [])):
            if acc == accession_number:
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        index_data = await edgar_client.get_filing_index(cik, accession_number)
        source_url = index_data.get("source_url", "")
        documents = _bounded_disclosure_documents(index_data.get("documents", []), source_url=source_url)
        if not documents:
            return error_response(
                "No parseable disclosure documents found for municipal bond filing.",
                code="source_unparsed",
                detail={"accession_number": accession_number, "source_url": source_url},
            )
        official_statement_url = _official_statement_url(documents)

        result = MuniBondDetails(
            accession_number=accession_number,
            issuer_name=issuer_name,
            filing_date=filing_date,
            documents=documents,
            source_url=source_url,
            official_statement_url=official_statement_url,
            disclosure_count=len(documents),
            description=index_data.get("description", ""),
        )
        return to_structured(result.model_dump())
    except Exception as e:
        logger.exception("get_muni_bond_details failed")
        return error_response(f"get_muni_bond_details failed: {e}")


def _bounded_disclosure_documents(documents: list[dict], limit: int = 25, source_url: str = "") -> list[dict]:
    parseable_suffixes = (".pdf", ".txt", ".xml", ".xbrl")
    bounded: list[dict] = []
    for document in documents:
        url = str(document.get("url", ""))
        name = str(document.get("name", ""))
        if not url:
            continue
        lower_url = url.lower()
        lower_name = name.lower()
        if not (lower_url.endswith(parseable_suffixes) or lower_name.endswith(parseable_suffixes)):
            continue
        normalized = dict(document)
        normalized.setdefault("source_url", source_url)
        bounded.append(normalized)
        if len(bounded) >= limit:
            break
    return bounded


def _official_statement_url(documents: list[dict]) -> str:
    for document in documents:
        haystack = " ".join(
            str(document.get(key, "")) for key in ("name", "type", "description", "url")
        ).lower()
        if "official" in haystack and "statement" in haystack:
            return str(document.get("url", ""))
    for document in documents:
        if str(document.get("url", "")).lower().endswith(".pdf"):
            return str(document.get("url", ""))
    return str(documents[0].get("url", "")) if documents else ""


# ---------------------------------------------------------------------------
# Tool 7: parse_audited_financial_pdf
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def parse_audited_financial_pdf(url_or_path: str, entity_name: str, fiscal_year: int | str) -> dict[str, Any]:
    """Parse headline financial metrics from an audited health-system PDF.

    Extracts common balance sheet, operations, and cash-flow metrics with page
    anchors and source citation locators. Values in PDFs labeled "In Thousands"
    are returned in whole dollars.
    """
    try:
        return to_structured(_parse_audited_financial_pdf(url_or_path, entity_name, fiscal_year))
    except Exception as e:
        logger.exception("parse_audited_financial_pdf failed")
        return error_response(f"parse_audited_financial_pdf failed: {e}")


@mcp.tool(structured_output=True)
async def get_public_financial_health_profile(ccn: str = "", ein: str = "", state: str = "") -> dict[str, Any]:
    """Return high-confidence public financial health fields from HCRIS, 990 Schedule H, and HFMD.

    This intentionally excludes HFMA MAP KPIs and public accounts-receivable proxies.
    """
    try:
        hcris = await _cost_report_public_metrics(ccn)
        form990 = await _latest_990_schedule_h(ein)
        hfmd = load_ahrq_hfmd_profile(ccn=ccn, state=state)
        joined_on = "ccn" if ccn and hfmd.get("matched_on") == "ccn" else ""
        return to_structured(
            {
                "ccn": ccn,
                "ein": ein,
                "state": state.upper() if state else "",
                "hcris": hcris,
                "form990_schedule_h": form990,
                "ahrq_hfmd": hfmd,
                "join_summary": {
                    "hcris_hfmd_joined": bool(joined_on),
                    "joined_on": joined_on,
                    "ccn": ccn,
                    "hfmd_provider_id": hfmd.get("join_keys", {}).get("hfmd_provider_id", ""),
                },
                "metric_confidence": {
                    "hcris": hcris.get("metric_confidence", {}),
                    "form990_schedule_h": form990.get("metric_confidence", {}),
                    "ahrq_hfmd": hfmd.get("metric_confidence", {}),
                },
                "source_policy": "reported_public_fields_only_no_revenue_cycle_map_kpi_derivations",
            }
        )
    except Exception as e:
        logger.exception("get_public_financial_health_profile failed")
        return error_response(f"get_public_financial_health_profile failed: {e}")


@mcp.tool(structured_output=True)
async def get_uncompensated_care_profile(ccn: str = "", ein: str = "") -> dict[str, Any]:
    """Return public uncompensated-care fields from CMS S-10/HCRIS and IRS Schedule H."""
    try:
        hcris = await _cost_report_public_metrics(ccn)
        form990 = await _latest_990_schedule_h(ein)
        return to_structured(
            {
                "ccn": ccn,
                "ein": ein,
                "uncompensated_care_cost": hcris.get("uncompensated_care_cost"),
                "charity_care_cost": hcris.get("charity_care_cost") or form990.get("charity_care"),
                "bad_debt_expense": hcris.get("bad_debt_expense"),
                "medicare_shortfall": hcris.get("medicare_shortfall"),
                "medicaid_shortfall": hcris.get("medicaid_shortfall"),
                "sources": {"hcris": hcris, "form990_schedule_h": form990},
                "metric_confidence": {
                    "uncompensated_care_cost": hcris.get("metric_confidence", {}).get("uncompensated_care_cost", "not_available"),
                    "charity_care_cost": (
                        hcris.get("metric_confidence", {}).get("charity_care_cost")
                        or form990.get("metric_confidence", {}).get("charity_care_cost")
                        or "not_available"
                    ),
                    "bad_debt_expense": hcris.get("metric_confidence", {}).get("bad_debt_expense", "not_available"),
                    "medicare_shortfall": hcris.get("metric_confidence", {}).get("medicare_shortfall", "not_available"),
                    "medicaid_shortfall": hcris.get("metric_confidence", {}).get("medicaid_shortfall", "not_available"),
                },
                "confidence": "high_when_source_field_present",
            }
        )
    except Exception as e:
        logger.exception("get_uncompensated_care_profile failed")
        return error_response(f"get_uncompensated_care_profile failed: {e}")


@mcp.tool(structured_output=True)
async def get_charity_care_profile(ein: str = "", ccn: str = "") -> dict[str, Any]:
    """Return public charity-care fields without deriving revenue-cycle MAP KPIs."""
    try:
        hcris = await _cost_report_public_metrics(ccn)
        form990 = await _latest_990_schedule_h(ein)
        return to_structured(
            {
                "ein": ein,
                "ccn": ccn,
                "charity_care_cost": hcris.get("charity_care_cost") or form990.get("charity_care"),
                "community_benefit_pct": form990.get("community_benefit_pct"),
                "total_expenses": form990.get("total_expenses"),
                "sources": {"hcris": hcris, "form990_schedule_h": form990},
                "metric_confidence": {
                    "charity_care_cost": (
                        hcris.get("metric_confidence", {}).get("charity_care_cost")
                        or form990.get("metric_confidence", {}).get("charity_care_cost")
                        or "not_available"
                    ),
                    "community_benefit_pct": form990.get("metric_confidence", {}).get("community_benefit_pct", "not_available"),
                    "total_expenses": form990.get("metric_confidence", {}).get("total_expenses", "not_available"),
                },
                "confidence": "high_when_schedule_h_or_s10_field_present",
            }
        )
    except Exception as e:
        logger.exception("get_charity_care_profile failed")
        return error_response(f"get_charity_care_profile failed: {e}")


@mcp.tool(structured_output=True)
async def get_bad_debt_profile(ccn: str = "", ein: str = "") -> dict[str, Any]:
    """Return public bad-debt disclosures from CMS S-10/HCRIS and 990 context."""
    try:
        hcris = await _cost_report_public_metrics(ccn)
        form990 = await _latest_990_schedule_h(ein)
        return to_structured(
            {
                "ccn": ccn,
                "ein": ein,
                "bad_debt_expense": hcris.get("bad_debt_expense"),
                "uncompensated_care_cost": hcris.get("uncompensated_care_cost"),
                "sources": {"hcris": hcris, "form990_schedule_h": form990},
                "metric_confidence": {
                    "bad_debt_expense": hcris.get("metric_confidence", {}).get("bad_debt_expense", "not_available"),
                    "uncompensated_care_cost": hcris.get("metric_confidence", {}).get("uncompensated_care_cost", "not_available"),
                },
                "confidence": "high_when_source_field_present",
            }
        )
    except Exception as e:
        logger.exception("get_bad_debt_profile failed")
        return error_response(f"get_bad_debt_profile failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
