"""Public Records & Regulatory MCP Server.

Provides tools for federal spending, 340B status, HIPAA breaches,
accreditation, and interoperability data. Port 8013.
"""

import csv
import json
import logging
import os as _os
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

from . import data_loaders, usaspending_client, sam_client
from .models import (
    USAspendingAward,
    USAspendingResponse,
    SAMOpportunity,
    SAMResponse,
    CoveredEntity340B,
    Status340BResponse,
    BreachRecord,
    BreachHistoryResponse,
    AccreditationRecord,
    AccreditationResponse,
    InteropRecord,
    InteropResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "public-records"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8013"))
mcp = FastMCP(**_mcp_kwargs)

# Load accreditation code lookup once at module level
_ACCR_CODES: dict[str, str] = {}
_codes_csv = Path(__file__).parent / "data" / "accreditation_codes.csv"
if _codes_csv.exists():
    with open(_codes_csv, newline="") as _f:
        for _row in csv.DictReader(_f):
            _ACCR_CODES[_row["code"].strip()] = _row["organization"].strip()


# ---------------------------------------------------------------------------
# CHPL API helper (optional enrichment for interop tool)
# ---------------------------------------------------------------------------

async def _lookup_chpl(cehrt_id: str, api_key: str) -> dict:
    """Look up EHR certification details from ONC CHPL API.

    Returns dict with ehr_product_name and ehr_developer, or empty dict on failure.
    """
    url = f"https://chpl.healthit.gov/rest/certification_ids/{cehrt_id}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, headers={"API-key": api_key})
            resp.raise_for_status()
            data = resp.json()
            products = data.get("products", [])
            if products:
                product = products[0]
                return {
                    "ehr_product_name": product.get("name", ""),
                    "ehr_developer": product.get("developer", ""),
                }
            return {}
    except Exception as e:
        logger.debug("CHPL lookup failed for %s: %s", cehrt_id, e)
        return {}


# ---------------------------------------------------------------------------
# Tool 1: search_usaspending
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_usaspending(
    recipient_name: str, award_type: str = "", fiscal_year: str = "", limit: int = 25,
) -> str:
    """Search federal spending awarded to a health system or hospital.

    Uses USAspending.gov (fully open, no auth needed).

    Args:
        recipient_name: Health system or hospital name to search.
        award_type: Filter: "contracts", "grants", "direct_payments", or "" for all.
        fiscal_year: e.g. "2024". Default: current fiscal year.
        limit: Max results (default 25, max 100).
    """
    try:
        cache_params = {
            "recipient_name": recipient_name,
            "award_type": award_type,
            "fiscal_year": fiscal_year,
            "limit": limit,
        }
        cached = data_loaders.load_cached_api_response("usaspending", cache_params)
        if cached is not None:
            raw = cached
        else:
            raw = await usaspending_client.search_awards(
                recipient_name, award_type, fiscal_year, limit,
            )
            if "error" in raw:
                return json.dumps(raw)
            data_loaders.cache_api_response("usaspending", cache_params, raw)

        results = raw.get("results", [])
        awards: list[USAspendingAward] = []
        for r in results:
            awards.append(USAspendingAward(
                award_id=str(r.get("Award ID", "")),
                recipient_name=str(r.get("Recipient Name", "")),
                awarding_agency=str(r.get("Awarding Agency", "")),
                awarding_sub_agency=str(r.get("Awarding Sub Agency", "")),
                award_type=str(r.get("Award Type", "")),
                total_obligation=float(r.get("Award Amount", 0) or 0),
                description=str(r.get("Description", "")),
                start_date=str(r.get("Start Date", "")),
                end_date=str(r.get("End Date", "")),
                naics_code=str(r.get("NAICS Code", "")),
                naics_description=str(r.get("NAICS Description", "")),
            ))

        total_obligation = sum(a.total_obligation for a in awards)
        total_awards = raw.get("page_metadata", {}).get("total", len(awards))

        response = USAspendingResponse(
            recipient_search=recipient_name,
            fiscal_year=fiscal_year,
            total_awards=total_awards,
            total_obligation=total_obligation,
            awards=awards,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("search_usaspending failed")
        return json.dumps({"error": f"search_usaspending failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: search_sam_gov
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_sam_gov(
    keyword: str, posted_from: str = "", posted_to: str = "", ptype: str = "", limit: int = 25,
) -> str:
    """Search federal contract opportunities and solicitations.

    Uses SAM.gov Opportunities API. Requires SAM_GOV_API_KEY env var.

    Args:
        keyword: Search keyword (organization name, service type, NAICS code).
        posted_from: Start date (MM/DD/YYYY). Default: 1 year ago.
        posted_to: End date. Default: today.
        ptype: Procurement type: "o" (solicitation), "p" (presolicitation), "k" (combined), or "" for all.
        limit: Max results (default 25).
    """
    try:
        cache_params = {
            "keyword": keyword,
            "posted_from": posted_from,
            "posted_to": posted_to,
            "ptype": ptype,
            "limit": limit,
        }
        cached = data_loaders.load_cached_api_response("sam_gov", cache_params)
        if cached is not None:
            raw = cached
        else:
            raw = await sam_client.search_opportunities(
                keyword, posted_from, posted_to, ptype, limit,
            )
            if "error" in raw:
                return json.dumps(raw)
            data_loaders.cache_api_response("sam_gov", cache_params, raw)

        opp_data = raw.get("opportunitiesData", [])
        opportunities: list[SAMOpportunity] = []
        for r in opp_data:
            opportunities.append(SAMOpportunity(
                notice_id=str(r.get("noticeId", "")),
                title=str(r.get("title", "")),
                solicitation_number=str(r.get("solicitationNumber", "")),
                department=str(r.get("department", "")),
                sub_tier=str(r.get("subTier", "")),
                posted_date=str(r.get("postedDate", "")),
                response_deadline=str(r.get("responseDeadLine", "")),
                naics_code=str(r.get("naicsCode", "")),
                set_aside_type=str(r.get("typeOfSetAsideDescription", "")),
                description=str(r.get("description", "")),
                active=bool(r.get("active", True)),
            ))

        total_results = raw.get("totalRecords", len(opportunities))

        response = SAMResponse(
            keyword=keyword,
            total_results=total_results,
            opportunities=opportunities,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("search_sam_gov failed")
        return json.dumps({"error": f"search_sam_gov failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: get_340b_status
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_340b_status(
    entity_name: str = "", entity_id: str = "", state: str = "",
) -> str:
    """Look up 340B Drug Pricing Program enrollment and contract pharmacy data.

    Requires manual download of the HRSA 340B OPAIS daily JSON export.

    Args:
        entity_name: Search by covered entity name.
        entity_id: Search by 340B ID.
        state: Filter by state abbreviation (e.g. "PA").
    """
    try:
        if not entity_name and not entity_id:
            return json.dumps({"error": "At least one of entity_name or entity_id is required."})

        if not data_loaders.ensure_340b_loaded():
            return json.dumps({
                "error": "340B data not available",
                "instructions": (
                    "Download the HRSA 340B OPAIS daily JSON export and place it at: "
                    f"{data_loaders._340B_JSON}"
                ),
            })

        rows = data_loaders.query_340b(
            entity_name=entity_name, entity_id=entity_id, state=state,
        )

        entities: list[CoveredEntity340B] = []
        for r in rows:
            entities.append(CoveredEntity340B(
                **{k: v for k, v in r.items() if k in CoveredEntity340B.model_fields},
            ))

        search_term = entity_name or entity_id
        response = Status340BResponse(
            search_term=search_term,
            total_results=len(entities),
            entities=entities,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_340b_status failed")
        return json.dumps({"error": f"get_340b_status failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: get_breach_history
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_breach_history(
    entity_name: str, state: str = "", min_individuals: int = 0,
) -> str:
    """Look up HIPAA breach reports for an organization.

    Requires manual download of breach data CSV from HHS OCR portal.

    Args:
        entity_name: Organization name to search.
        state: Filter by state abbreviation.
        min_individuals: Minimum individuals affected (default 0).
    """
    try:
        if not data_loaders.ensure_breach_loaded():
            return json.dumps({
                "error": "HIPAA breach data not available",
                "instructions": (
                    "Download the breach report CSV from "
                    "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf "
                    f"and place it at: {data_loaders._BREACH_CSV}"
                ),
            })

        rows = data_loaders.query_breaches(
            entity_name=entity_name, state=state, min_individuals=min_individuals,
        )

        breaches: list[BreachRecord] = []
        for r in rows:
            # Extract individuals_affected — look for key containing "individuals" or "affected"
            individuals = r.get("individuals_affected", 0)
            if not individuals:
                for k, v in r.items():
                    if "individuals" in k.lower() or "affected" in k.lower():
                        try:
                            individuals = int(float(v)) if v else 0
                        except (ValueError, TypeError):
                            individuals = 0
                        break

            breaches.append(BreachRecord(
                entity_name=r.get("entity_name", ""),
                state=r.get("state", ""),
                covered_entity_type=r.get("covered_entity_type", ""),
                individuals_affected=int(float(individuals)) if individuals else 0,
                breach_submission_date=r.get("breach_submission_date", ""),
                breach_type=r.get("breach_type", ""),
                location_of_breached_info=r.get("location_of_breached_info", ""),
                business_associate_present=r.get("business_associate_present", ""),
                web_description=r.get("web_description", ""),
            ))

        total_individuals = sum(b.individuals_affected for b in breaches)

        response = BreachHistoryResponse(
            search_entity=entity_name,
            total_breaches=len(breaches),
            total_individuals_affected=total_individuals,
            breaches=breaches,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_breach_history failed")
        return json.dumps({"error": f"get_breach_history failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: get_accreditation
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_accreditation(
    ccn: str = "", provider_name: str = "", state: str = "",
) -> str:
    """Look up hospital accreditation and certification status.

    Uses CMS Provider of Services (POS) file. Includes Joint Commission,
    DNV, CIHQ, and other CMS-approved accrediting organizations.

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        provider_name: Search by provider name (partial match).
        state: Filter by state abbreviation.
    """
    try:
        if not ccn and not provider_name:
            return json.dumps({"error": "At least one of ccn or provider_name is required."})

        await data_loaders.ensure_pos_cached()

        rows = data_loaders.query_pos(
            ccn=ccn, provider_name=provider_name, state=state,
        )

        providers: list[AccreditationRecord] = []
        for r in rows:
            # Map accreditation_type_code to org name
            code = r.get("accreditation_type_code", "")
            accred_org = _ACCR_CODES.get(code.strip(), code)

            providers.append(AccreditationRecord(
                ccn=r.get("ccn", ""),
                provider_name=r.get("provider_name", ""),
                state=r.get("state", ""),
                city=r.get("city", ""),
                accreditation_org=accred_org,
                accreditation_type_code=code,
                accreditation_effective_date=r.get("accreditation_effective_date", ""),
                accreditation_expiration_date=r.get("accreditation_expiration_date", ""),
                certification_date=r.get("certification_date", ""),
                ownership_type=r.get("ownership_type", ""),
                bed_count=int(float(r.get("bed_count", 0) or 0)),
                medicare_medicaid=r.get("medicare_medicaid", ""),
                compliance_status=r.get("compliance_status", ""),
            ))

        search_term = ccn or provider_name
        response = AccreditationResponse(
            search_term=search_term,
            total_results=len(providers),
            providers=providers,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_accreditation failed")
        return json.dumps({"error": f"get_accreditation failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 6: get_interop_status
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_interop_status(
    ccn: str = "", facility_name: str = "", state: str = "",
) -> str:
    """Check Promoting Interoperability attestation and EHR certification.

    Uses CMS Promoting Interoperability dataset. Optionally enriches with
    ONC CHPL API for EHR product details (requires CHPL_API_KEY env var).

    Args:
        ccn: CMS Certification Number.
        facility_name: Search by facility name (partial match).
        state: Filter by state abbreviation.
    """
    try:
        if not ccn and not facility_name:
            return json.dumps({"error": "At least one of ccn or facility_name is required."})

        await data_loaders.ensure_pi_cached()

        rows = data_loaders.query_pi(
            ccn=ccn, facility_name=facility_name, state=state,
        )

        chpl_api_key = _os.environ.get("CHPL_API_KEY", "")

        records: list[InteropRecord] = []
        for r in rows:
            ehr_product = r.get("ehr_product_name", "")
            ehr_developer = r.get("ehr_developer", "")
            cehrt_id = r.get("cehrt_id", "")

            # Optionally enrich with CHPL data
            if chpl_api_key and cehrt_id and not ehr_product:
                chpl_data = await _lookup_chpl(cehrt_id, chpl_api_key)
                if chpl_data:
                    ehr_product = chpl_data.get("ehr_product_name", ehr_product)
                    ehr_developer = chpl_data.get("ehr_developer", ehr_developer)

            records.append(InteropRecord(
                facility_name=r.get("facility_name", ""),
                ccn=r.get("ccn", ""),
                state=r.get("state", ""),
                city=r.get("city", ""),
                meets_pi_criteria=r.get("meets_pi_criteria", ""),
                cehrt_id=cehrt_id,
                reporting_period_start=r.get("reporting_period_start", ""),
                reporting_period_end=r.get("reporting_period_end", ""),
                ehr_product_name=ehr_product,
                ehr_developer=ehr_developer,
            ))

        search_term = ccn or facility_name
        response = InteropResponse(
            search_term=search_term,
            total_results=len(records),
            records=records,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_interop_status failed")
        return json.dumps({"error": f"get_interop_status failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
