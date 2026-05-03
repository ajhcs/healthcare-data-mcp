"""Public Records & Regulatory MCP Server.

Provides tools for federal spending, 340B status, HIPAA breaches,
accreditation, and interoperability data. Port 8013.
"""

from typing import Any
import csv
from datetime import UTC, datetime
import json
import logging
import os as _os
from pathlib import Path
import re


from shared.utils.http_client import resilient_request
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_response import error_response, to_structured
from shared import state_health_data

from . import data_loaders, usaspending_client, sam_client, sam_exclusions_client  # pyright: ignore[reportAttributeAccessIssue]
from .models import (
    OIG_LEIE_CAVEAT,
    SAM_EXCLUSIONS_CAVEAT,
    USAspendingAward,
    USAspendingResponse,
    SAMOpportunity,
    SAMResponse,
    CoveredEntity340B,
    CISAKevContext,
    CyberIncidentRecord,
    CyberSourceStatus,
    Status340BResponse,
    BreachRecord,
    BreachHistoryResponse,
    AccreditationRecord,
    AccreditationResponse,
    InteropRecord,
    InteropResponse,
    LEIEBatchResponse,
    LEIEBatchResult,
    LEIEBatchCandidate,
    LEIEExclusionRecord,
    LEIESearchResponse,
    LEIESourceMetadata,
    SAMExclusionBatchCandidate,
    SAMExclusionBatchResponse,
    SAMExclusionBatchResult,
    SAMExclusionRecord,
    SAMExclusionSearchResponse,
    SAMExclusionsSourceMetadata,
)
from shared.utils.identity import normalize_npi

logger = logging.getLogger(__name__)

_SENSITIVE_IDENTIFIER_KEYS = {
    "ssn",
    "social_security_number",
    "social_security_num",
    "social_security",
    "ein",
    "fein",
    "tin",
    "tax_id",
    "tax_identifier",
    "taxpayer_id",
    "taxpayer_identifier",
    "taxpayer_identification_number",
    "employer_identification_number",
    "federal_tax_id",
    "federal_tax_identifier",
}

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "public-records"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
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
        resp = await resilient_request("GET", url, headers={"API-key": api_key}, timeout=15.0)
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
# LEIE response helpers
# ---------------------------------------------------------------------------

def _leie_source_metadata(data: dict) -> LEIESourceMetadata:
    return LEIESourceMetadata(
        **{k: v for k, v in data.items() if k in LEIESourceMetadata.model_fields}
    )


def _leie_records(rows: list[dict]) -> list[LEIEExclusionRecord]:
    return [
        LEIEExclusionRecord(**{k: v for k, v in row.items() if k in LEIEExclusionRecord.model_fields})
        for row in rows
    ]


def _leie_status(records: list[LEIEExclusionRecord]) -> str:
    if not records:
        return "no_current_leie_match_found"
    if any(record.verification_status == "strong_potential_match" for record in records):
        return "strong_potential_match"
    return "potential_match"


def _normalized_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _contains_sensitive_identifier_keys(payload: dict[str, Any]) -> bool:
    return bool(_SENSITIVE_IDENTIFIER_KEYS & {_normalized_key(key) for key in payload})


# ---------------------------------------------------------------------------
# Cyber incident enrichment helpers
# ---------------------------------------------------------------------------

_STATE_AG_BREACH_NOTICE_SOURCE_STATUSES: dict[str, CyberSourceStatus] = {
    "PA": CyberSourceStatus(
        source_name="Pennsylvania Office of Attorney General breach notices",
        source_type="state_ag_breach_notice",
        status="ready",
        reason="Pennsylvania notices can be represented as public-page records in the local cyber index.",
        source_url="https://www.attorneygeneral.gov/",
        next_step="Seed public notice pages or structured fixture rows into the public-records cyber cache.",
    ),
    "NJ": CyberSourceStatus(
        source_name="New Jersey Division of Consumer Affairs breach notices",
        source_type="state_ag_breach_notice",
        status="not_searchable",
        reason="The public source does not expose a stable structured search/export endpoint for automated entity lookup.",
        source_url="https://www.njconsumeraffairs.gov/",
        next_step="Use manual source review or add curated public notice fixtures when available.",
    ),
    "DE": CyberSourceStatus(
        source_name="Delaware Department of Justice breach notices",
        source_type="state_ag_breach_notice",
        status="not_automatable",
        reason="The public source should not be automated without a stable unauthenticated index or documented export path.",
        source_url="https://attorneygeneral.delaware.gov/",
        next_step="Treat Delaware AG breach notices as manual-review evidence until a stable public feed exists.",
    ),
}


def _cyber_source_statuses(states: list[str] | None = None) -> dict[str, dict[str, Any]]:
    requested = [state.strip().upper() for state in states or [] if state.strip()]
    keys = requested or ["PA", "NJ", "DE"]
    statuses: dict[str, dict[str, Any]] = {}
    for state in keys:
        status = _STATE_AG_BREACH_NOTICE_SOURCE_STATUSES.get(
            state,
            CyberSourceStatus(
                source_name=f"{state} state AG breach notices",
                source_type="state_ag_breach_notice",
                status="not_searchable",
                reason="No state-specific public breach notice search status has been configured.",
                next_step="Add a reviewed source status before using this state for automated enrichment.",
            ),
        )
        statuses[state] = status.model_dump()
    return statuses


def _breach_incident_type(breach: dict[str, Any]) -> str:
    haystack = json.dumps(breach).lower()
    if "ransomware" in haystack:
        return "ransomware"
    if "malware" in haystack:
        return "malware"
    if "phishing" in haystack:
        return "phishing"
    if "hacking" in haystack or "unauthorized access" in haystack:
        return "unauthorized_access"
    if "network" in haystack or "it incident" in haystack or "cyber" in haystack:
        return "cybersecurity_incident"
    return ""


def _breach_incident_type_confidence(breach: dict[str, Any]) -> str:
    incident_type = _breach_incident_type(breach)
    if incident_type in {"ransomware", "malware", "phishing", "unauthorized_access"}:
        return "high"
    if incident_type:
        return "medium"
    return "low"


def _breach_entity_match_confidence(entity_name: str, breach: dict[str, Any]) -> str:
    query_name = entity_name.strip()
    breach_name = str(breach.get("entity_name", "")).strip()
    if not query_name:
        return "not_requested"
    if breach_name and (query_name.lower() in breach_name.lower() or breach_name.lower() in query_name.lower()):
        return "high"
    if query_name.lower() in json.dumps(breach).lower():
        return "medium"
    return "low"


def _cyber_incident_from_breach(entity_name: str, breach: dict[str, Any]) -> dict[str, Any]:
    incident_type_confidence = _breach_incident_type_confidence(breach)
    entity_match_confidence = _breach_entity_match_confidence(entity_name, breach)
    return CyberIncidentRecord(
        entity_name=str(breach.get("entity_name", "")),
        state=str(breach.get("state", "")),
        incident_type=_breach_incident_type(breach),
        incident_date="",
        disclosure_date=str(breach.get("breach_submission_date", "")),
        date=str(breach.get("breach_submission_date", "")),
        title=str(breach.get("entity_name", "")),
        summary=str(breach.get("web_description", "")),
        source_type="hhs_ocr_breach_portal",
        entity_match_confidence=entity_match_confidence,
        incident_type_confidence=incident_type_confidence,
        timeline_disclosed=bool(breach.get("breach_submission_date")),
        timeline_inferred=False,
        confidence=(
            "high"
            if entity_match_confidence == "high" and incident_type_confidence == "high"
            else "medium"
            if incident_type_confidence in {"high", "medium"}
            else "low"
        ),
    ).model_dump()


# ---------------------------------------------------------------------------
# SAM.gov Exclusions response helpers
# ---------------------------------------------------------------------------

def _sam_source_metadata(data: dict[str, Any]) -> SAMExclusionsSourceMetadata:
    return SAMExclusionsSourceMetadata(
        **{k: v for k, v in data.items() if k in SAMExclusionsSourceMetadata.model_fields}
    )


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _first_sam_action(raw: dict[str, Any]) -> dict[str, Any]:
    actions = raw.get("exclusionActions", {}).get("listOfActions", [])
    if isinstance(actions, list) and actions:
        first = actions[0]
        if isinstance(first, dict):
            return first
    return {}


def _sam_references(raw: dict[str, Any]) -> list[dict[str, str]]:
    refs = (
        raw.get("exclusionOtherInformation", {})
        .get("references", {})
        .get("referencesList", [])
    )
    if not isinstance(refs, list):
        return []
    normalized: list[dict[str, str]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        exclusion_name = _text(ref.get("exclusionName") or ref.get("name"))
        ref_type = _text(ref.get("type"))
        if exclusion_name or ref_type:
            normalized.append({"exclusion_name": exclusion_name, "type": ref_type})
    return normalized


def _sam_display_name(identification: dict[str, Any]) -> str:
    entity_name = _text(identification.get("entityName") or identification.get("name")).strip()
    if entity_name:
        return entity_name
    name_parts = [
        _text(identification.get("prefix")).strip(),
        _text(identification.get("firstName")).strip(),
        _text(identification.get("middleName")).strip(),
        _text(identification.get("lastName")).strip(),
        _text(identification.get("suffix")).strip(),
    ]
    return " ".join(part for part in name_parts if part)


def _sam_match_basis(record: SAMExclusionRecord, query: dict[str, Any]) -> tuple[str, int, str]:
    if query.get("ueiSAM") and record.uei.upper() == _text(query.get("ueiSAM")).upper():
        return "uei_exact", 100, "strong_potential_match"
    if query.get("cageCode") and record.cage_code.upper() == _text(query.get("cageCode")).upper():
        return "cage_code_exact", 100, "strong_potential_match"
    query_npi = normalize_npi(query.get("npi"))
    record_npi = normalize_npi(record.npi)
    if query_npi and record_npi == query_npi:
        return "npi_exact", 100, "strong_potential_match"
    if query.get("exclusionName"):
        return "name_search", 70, "potential_match"
    return "filtered_search", 50, "potential_match"


def _sam_records(rows: list[dict[str, Any]], query: dict[str, Any] | None = None) -> list[SAMExclusionRecord]:
    records: list[SAMExclusionRecord] = []
    query = query or {}
    for row in rows:
        details = row.get("exclusionDetails", {})
        identification = row.get("exclusionIdentification", {})
        address = row.get("exclusionPrimaryAddress") or row.get("exclusionAddress") or {}
        action = _first_sam_action(row)
        other = row.get("exclusionOtherInformation", {})
        record = SAMExclusionRecord(
            classification=_text(details.get("classificationType")),
            exclusion_type=_text(details.get("exclusionType")),
            exclusion_program=_text(details.get("exclusionProgram")),
            excluding_agency_code=_text(details.get("excludingAgencyCode")),
            excluding_agency_name=_text(details.get("excludingAgencyName")),
            uei=_text(identification.get("ueiSAM")),
            cage_code=_text(identification.get("cageCode")),
            npi=_text(identification.get("npi")),
            prefix=_text(identification.get("prefix")),
            first_name=_text(identification.get("firstName")),
            middle_name=_text(identification.get("middleName")),
            last_name=_text(identification.get("lastName")),
            suffix=_text(identification.get("suffix")),
            entity_name=_text(identification.get("entityName") or identification.get("name")),
            display_name=_sam_display_name(identification),
            address_line_1=_text(address.get("addressLine1")),
            address_line_2=_text(address.get("addressLine2")),
            city=_text(address.get("city")),
            state=_text(address.get("stateOrProvinceCode")),
            zip_code=_text(address.get("zipCode")),
            zip_code_plus_4=_text(address.get("zipCodePlus4")),
            country=_text(address.get("countryCode")),
            create_date=_text(action.get("createDate")),
            update_date=_text(action.get("updateDate")),
            activation_date=_text(action.get("activateDate")),
            termination_date=_text(action.get("terminationDate")),
            termination_type=_text(action.get("terminationType")),
            record_status=_text(action.get("recordStatus")),
            ct_code=_text(other.get("ctCode")),
            fascsa_order=_text(other.get("isFASCSAOrder")),
            additional_comments=_text(other.get("additionalComments")),
            references=_sam_references(row),
        )
        match_basis, match_score, verification_status = _sam_match_basis(record, query)
        record.match_basis = match_basis
        record.match_score = match_score
        record.verification_status = verification_status
        records.append(record)
    return records


def _sam_status(records: list[SAMExclusionRecord]) -> str:
    if not records:
        return "no_current_sam_exclusion_found"
    if any(record.verification_status == "strong_potential_match" for record in records):
        return "strong_potential_match"
    return "potential_match"


def _sam_query_payload(**kwargs: str) -> dict[str, str]:
    return {key: value for key, value in kwargs.items() if value}


def _sam_error_response(raw: dict[str, Any], tool_name: str) -> dict[str, Any]:
    return error_response(
        raw.get("error", f"{tool_name} failed."),
        code=raw.get("code", "source_unavailable"),
        detail=raw.get("detail") or raw.get("instructions"),
        retryable=bool(raw.get("retryable", False)),
        source_metadata=raw.get("source_metadata"),
    )


# ---------------------------------------------------------------------------
# Tool 1: search_usaspending
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_usaspending(
    recipient_name: str, award_type: str = "", fiscal_year: str = "", limit: int = 25,
) -> dict[str, Any]:
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
                return to_structured(raw)
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

        from datetime import datetime as _dt
        fy = fiscal_year or str(_dt.now().year)
        response = USAspendingResponse(
            recipient_search=recipient_name,
            fiscal_year=fy,
            total_awards=total_awards,
            total_obligation=total_obligation,
            awards=awards,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_usaspending failed")
        return error_response(f"search_usaspending failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: search_sam_gov
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_sam_gov(
    keyword: str, posted_from: str = "", posted_to: str = "", ptype: str = "", limit: int = 25,
) -> dict[str, Any]:
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
                return to_structured(raw)
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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_sam_gov failed")
        return error_response(f"search_sam_gov failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: get_340b_status
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_340b_status(
    entity_name: str = "", entity_id: str = "", state: str = "",
) -> dict[str, Any]:
    """Look up 340B Drug Pricing Program enrollment and contract pharmacy data.

    Uses the cached HRSA 340B OPAIS daily JSON export when available. If the
    public reports page does not expose a stable direct JSON URL, the response
    includes a not_automatable source status with precise next steps.

    Args:
        entity_name: Search by covered entity name.
        entity_id: Search by 340B ID.
        state: Filter by state abbreviation (e.g. "PA").
    """
    try:
        if not entity_name and not entity_id:
            return error_response("At least one of entity_name or entity_id is required.")

        if not data_loaders.ensure_340b_loaded():
            source_status = await state_health_data.acquire_340b_opais()
            return error_response(
                "340B data not available",
                code="source_not_automatable" if source_status.status == "not_automatable" else "source_unavailable",
                instructions=(
                    source_status.next_step
                    or f"Download the HRSA 340B OPAIS daily JSON export and place it at: {data_loaders._340B_JSON}"
                ),
                source_status=source_status.to_dict(),
            )

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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_340b_status failed")
        return error_response(f"get_340b_status failed: {e}")


@mcp.tool(structured_output=True)
async def check_340b_status(entity_name: str = "", entity_id: str = "", state: str = "") -> dict[str, Any]:
    """Alias for get_340b_status."""
    return await get_340b_status(entity_name=entity_name, entity_id=entity_id, state=state)


@mcp.tool(structured_output=True)
async def get_340b_profile(entity_name: str = "", entity_id: str = "", state: str = "") -> dict[str, Any]:
    """Return a normalized 340B profile with parent/child and pharmacy context."""
    result = await get_340b_status(entity_name=entity_name, entity_id=entity_id, state=state)
    if result.get("ok") is False:
        return result
    entities = result.get("entities", [])
    if not entities:
        return to_structured({"search_term": entity_name or entity_id, "total_results": 0, "profiles": []})
    profiles = []
    for entity in entities:
        profiles.append(
            {
                "entity_id": entity.get("entity_id", ""),
                "entity_name": entity.get("entity_name", ""),
                "entity_type": entity.get("entity_type", ""),
                "parent_child_relation": entity.get("parent_child_relation", ""),
                "parent_entity_id": entity.get("parent_entity_id", ""),
                "parent_entity_name": entity.get("parent_entity_name", ""),
                "contract_pharmacy_count": entity.get("contract_pharmacy_count", 0),
                "participation_status": entity.get("participation_status", ""),
                "participating": entity.get("participating", True),
                "effective_date": entity.get("effective_date", ""),
                "termination_date": entity.get("termination_date", ""),
                "source_report_date": entity.get("source_report_date", ""),
                "address": entity.get("address", ""),
                "city": entity.get("city", ""),
                "state": entity.get("state", ""),
                "zip_code": entity.get("zip_code", ""),
            }
        )
    return to_structured({"search_term": entity_name or entity_id, "total_results": len(profiles), "profiles": profiles})


@mcp.tool(structured_output=True)
async def find_340b_entities_near_facility(
    facility_name: str = "",
    state: str = "",
    city: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Find 340B covered entities by facility/city/state public-record proximity filters.

    This is textual proximity only; it does not infer geographic radius unless
    coordinates are present in the OPAIS cache.
    """
    result = await get_340b_status(entity_name=facility_name, state=state)
    if result.get("ok") is False:
        return result
    entities = result.get("entities", [])
    if city:
        entities = [entity for entity in entities if str(entity.get("city", "")).lower() == city.lower()]
    return to_structured(
        {
            "facility_name": facility_name,
            "state": state.upper() if state else "",
            "city": city,
            "match_method": "opais_name_city_state_text_filter",
            "total_results": min(len(entities), max(1, min(limit, 100))),
            "entities": entities[: max(1, min(limit, 100))],
        }
    )


# ---------------------------------------------------------------------------
# Tool 4: get_breach_history
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_breach_history(
    entity_name: str, state: str = "", min_individuals: int = 0,
) -> dict[str, Any]:
    """Look up HIPAA breach reports for an organization.

    Requires manual download of breach data CSV from HHS OCR portal.

    Args:
        entity_name: Organization name to search.
        state: Filter by state abbreviation.
        min_individuals: Minimum individuals affected (default 0).
    """
    try:
        if not data_loaders.ensure_breach_loaded():
            return error_response(
                "HIPAA breach data not available",
                instructions=(
                    "Download the breach report CSV from "
                    "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf "
                    f"and place it at: {data_loaders._BREACH_CSV}"
                ),
            )

        rows = data_loaders.query_breaches(
            entity_name=entity_name, state=state, min_individuals=min_individuals,
        )

        breaches: list[BreachRecord] = []
        for r in rows:
            individuals = r.get("individuals_affected", 0)
            try:
                individuals = int(float(individuals)) if individuals else 0
            except (ValueError, TypeError):
                individuals = 0

            breaches.append(BreachRecord(
                entity_name=r.get("entity_name", ""),
                state=r.get("state", ""),
                covered_entity_type=r.get("covered_entity_type", ""),
                individuals_affected=individuals,
                breach_submission_date=r.get("breach_submission_date", ""),
                breach_type=r.get("breach_type", ""),
                location_of_breached_info=r.get("location_of_breached_info", ""),
                business_associate_present=r.get("business_associate_present", ""),
                web_description=r.get("web_description", ""),
                entity_match_confidence=_breach_entity_match_confidence(entity_name, r),
                incident_type_confidence=_breach_incident_type_confidence(r),
                timeline_disclosed=bool(r.get("breach_submission_date")),
                timeline_inferred=False,
                source_type="hhs_ocr_breach_portal",
            ))

        total_individuals = sum(b.individuals_affected for b in breaches)

        response = BreachHistoryResponse(
            search_entity=entity_name,
            total_breaches=len(breaches),
            total_individuals_affected=total_individuals,
            breaches=breaches,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_breach_history failed")
        return error_response(f"get_breach_history failed: {e}")


@mcp.tool(structured_output=True)
async def search_phc4_public_reports(query: str, year: str = "", report_type: str = "") -> dict[str, Any]:
    """Search the indexed PHC4 public report library without using paid PHC4 datasets."""
    try:
        return to_structured(await state_health_data.search_phc4_reports(query, year=year, report_type=report_type))
    except Exception as e:
        logger.exception("search_phc4_public_reports failed")
        return error_response(f"search_phc4_public_reports failed: {e}")


@mcp.tool(structured_output=True)
async def get_phc4_hospital_performance(hospital_name: str = "", year: int = 0) -> dict[str, Any]:
    """Return PHC4 public Hospital Performance report matches and provenance."""
    try:
        return to_structured(await state_health_data.phc4_report_profile(hospital_name=hospital_name, year=year, report_type="hospital_performance"))
    except Exception as e:
        logger.exception("get_phc4_hospital_performance failed")
        return error_response(f"get_phc4_hospital_performance failed: {e}")


@mcp.tool(structured_output=True)
async def get_phc4_financial_analysis(hospital_name: str = "", fiscal_year: int = 0) -> dict[str, Any]:
    """Return PHC4 public Financial Analysis report matches and provenance."""
    try:
        return to_structured(await state_health_data.phc4_report_profile(hospital_name=hospital_name, fiscal_year=fiscal_year, report_type="financial_analysis"))
    except Exception as e:
        logger.exception("get_phc4_financial_analysis failed")
        return error_response(f"get_phc4_financial_analysis failed: {e}")


@mcp.tool(structured_output=True)
async def get_phc4_common_procedure_profile(
    hospital_name: str = "", procedure: str = "", year: int = 0,
) -> dict[str, Any]:
    """Return PHC4 public Common Procedures report matches and provenance."""
    try:
        return to_structured(
            await state_health_data.phc4_report_profile(
                hospital_name=hospital_name,
                procedure=procedure,
                year=year,
                report_type="common_procedure",
            )
        )
    except Exception as e:
        logger.exception("get_phc4_common_procedure_profile failed")
        return error_response(f"get_phc4_common_procedure_profile failed: {e}")


@mcp.tool(structured_output=True)
async def search_ocr_enforcement_actions(
    query: str = "",
    entity_name: str = "",
    state: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search locally indexed HHS OCR enforcement action public pages."""
    try:
        result = data_loaders.search_ocr_enforcement_actions(
            query=query,
            entity_name=entity_name,
            state=state,
            limit=limit,
        )
        return to_structured(
            {
                "query": query,
                "entity_name": entity_name,
                "state": state.upper() if state else "",
                "total_results": len(result.get("records", [])),
                "source_status": result.get("source_status", {}),
                "records": result.get("records", []),
            }
        )
    except Exception as e:
        logger.exception("search_ocr_enforcement_actions failed")
        return error_response(f"search_ocr_enforcement_actions failed: {e}")


@mcp.tool(structured_output=True)
async def search_sec_cyber_disclosures(
    entity_name: str = "",
    cik: str = "",
    query: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search locally indexed SEC cyber disclosures.

    SEC_USER_AGENT is required for this search path so callers do not build SEC
    workflows that ignore EDGAR's contactable user-agent policy.
    """
    try:
        sec_user_agent = _os.environ.get("SEC_USER_AGENT", "").strip()
        if not sec_user_agent:
            return error_response(
                "SEC_USER_AGENT is required for SEC cyber disclosure search.",
                code="invalid_config",
                instructions="Set SEC_USER_AGENT to a contactable application name and email before using SEC paths.",
            )

        result = data_loaders.search_sec_cyber_disclosures(
            entity_name=entity_name,
            cik=cik,
            query=query,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        records = result.get("records", [])
        return to_structured(
            {
                "entity_name": entity_name,
                "cik": cik,
                "query": query,
                "start_date": start_date,
                "end_date": end_date,
                "total_results": len(records),
                "source_status": result.get("source_status", {}),
                "records": records,
            }
        )
    except Exception as e:
        logger.exception("search_sec_cyber_disclosures failed")
        return error_response(f"search_sec_cyber_disclosures failed: {e}")


@mcp.tool(structured_output=True)
async def get_state_ag_breach_notice_sources(states: list[str] | None = None) -> dict[str, Any]:
    """Return PA/NJ/DE state AG breach notice source statuses and reasons."""
    return to_structured({"sources": _cyber_source_statuses(states)})


@mcp.tool(structured_output=True)
async def get_cisa_kev_context_status() -> dict[str, Any]:
    """Return CISA KEV context-only status; this source is not attribution evidence."""
    return to_structured(CISAKevContext().model_dump())


@mcp.tool(structured_output=True)
async def get_cyber_incident_profile(entity_name: str, state: str = "") -> dict[str, Any]:
    """Enrich public cyber incident history from OCR breach data and public-source flags.

    The tool returns confidence flags and does not infer response timelines
    unless a source row explicitly contains timing fields.
    """
    try:
        breach_result = await get_breach_history(entity_name=entity_name, state=state)
        if breach_result.get("ok") is False:
            breaches: list[dict[str, Any]] = []
            ocr_status = breach_result.get("error", {}).get("code", "source_unavailable")
        else:
            breaches = breach_result.get("breaches", [])
            ocr_status = "ready"

        incidents = [_cyber_incident_from_breach(entity_name, breach) for breach in breaches]

        ocr_enforcement = data_loaders.search_ocr_enforcement_actions(entity_name=entity_name, state=state, limit=10)
        incidents.extend(ocr_enforcement.get("records", []))

        sec_status: dict[str, Any]
        if _os.environ.get("SEC_USER_AGENT", "").strip():
            sec_result = data_loaders.search_sec_cyber_disclosures(entity_name=entity_name, limit=10)
            sec_status = sec_result.get("source_status", {})
            incidents.extend(sec_result.get("records", []))
        else:
            sec_status = CyberSourceStatus(
                source_name="SEC EDGAR cyber disclosures",
                source_type="sec_cyber_disclosure",
                status="not_searchable",
                reason="SEC_USER_AGENT is not set; SEC search paths require a contactable user agent.",
                source_url="https://www.sec.gov/edgar/search/",
                next_step="Set SEC_USER_AGENT before searching SEC cyber disclosures.",
            ).model_dump()

        cisa_context = CISAKevContext().model_dump()
        return to_structured(
            {
                "entity_name": entity_name,
                "state": state.upper() if state else "",
                "sources": {
                    "hhs_ocr_breach_portal": ocr_status,
                    "ocr_enforcement_actions": ocr_enforcement.get("source_status", {}),
                    "sec_cyber_disclosures": sec_status,
                    "state_ag_breach_notices": _cyber_source_statuses([state] if state else None),
                    "cisa_kev": cisa_context,
                },
                "incident_count": len(incidents),
                "incidents": incidents,
                "confidence_flags": {
                    "hhs_ocr_name_match": "medium" if breaches else "none",
                    "timeline_inferred": False,
                    "cisa_kev_used_for_attribution": cisa_context["attribution_used"],
                },
            }
        )
    except Exception as e:
        logger.exception("get_cyber_incident_profile failed")
        return error_response(f"get_cyber_incident_profile failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: get_accreditation
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_accreditation(
    ccn: str = "", provider_name: str = "", state: str = "",
) -> dict[str, Any]:
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
            return error_response("At least one of ccn or provider_name is required.")

        await data_loaders.ensure_pos_cached()

        rows = data_loaders.query_pos(
            ccn=ccn, provider_name=provider_name, state=state,
        )

        providers: list[AccreditationRecord] = []
        for r in rows:
            # Map accreditation_type_code to org name
            code = r.get("accreditation_type_code", "")
            accred_org = _ACCR_CODES.get(code.strip()) or code

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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_accreditation failed")
        return error_response(f"get_accreditation failed: {e}")


# ---------------------------------------------------------------------------
# Tool 6: get_interop_status
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_interop_status(
    ccn: str = "", facility_name: str = "", state: str = "",
) -> dict[str, Any]:
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
            return error_response("At least one of ccn or facility_name is required.")

        await data_loaders.ensure_pi_cached()

        rows = data_loaders.query_pi(
            ccn=ccn, facility_name=facility_name, state=state,
        )

        chpl_api_key = _os.environ.get("CHPL_API_KEY", "")

        # Deduplicate CHPL lookups: fetch each unique cehrt_id only once
        chpl_cache: dict[str, dict] = {}
        if chpl_api_key:
            unique_ids = {r.get("cehrt_id", "") for r in rows} - {""}
            for cid in list(unique_ids)[:10]:  # cap at 10 lookups
                chpl_data = await _lookup_chpl(cid, chpl_api_key)
                if chpl_data:
                    chpl_cache[cid] = chpl_data

        records: list[InteropRecord] = []
        for r in rows:
            ehr_product = r.get("ehr_product_name", "")
            ehr_developer = r.get("ehr_developer", "")
            cehrt_id = r.get("cehrt_id", "")

            # Enrich from pre-fetched CHPL data
            if cehrt_id in chpl_cache and not ehr_product:
                chpl_data = chpl_cache[cehrt_id]
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
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("get_interop_status failed")
        return error_response(f"get_interop_status failed: {e}")


# ---------------------------------------------------------------------------
# Tool 7: HHS OIG LEIE screening
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def check_leie_npi(
    npi: str,
    limit: int = 25,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Check a provider NPI against the current HHS OIG LEIE exclusion file.

    Exact NPI matches are strong potential matches. The downloadable LEIE file
    does not include SSNs/EINs, so this tool does not provide final identity
    verification.
    """
    try:
        normalized_npi = normalize_npi(npi)
        if not normalized_npi:
            return error_response(
                "A valid non-placeholder 10-digit NPI is required.",
                code="invalid_params",
            )

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        rows = data_loaders.query_leie_by_npi(normalized_npi)[:max(1, min(limit, 100))]
        records = _leie_records(rows)
        response = LEIESearchResponse(
            search_type="npi",
            query={"npi": normalized_npi},
            status=_leie_status(records),
            total_results=len(records),
            records=records,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("check_leie_npi failed")
        return error_response(f"check_leie_npi failed: {e}")


@mcp.tool(structured_output=True)
async def search_leie_individual(
    last_name: str,
    first_name: str = "",
    state: str = "",
    dob: str = "",
    limit: int = 25,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Search the current HHS OIG LEIE file for an excluded individual name."""
    try:
        if not last_name.strip():
            return error_response("last_name is required.", code="invalid_params")

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        rows = data_loaders.query_leie_by_individual(
            last_name=last_name,
            first_name=first_name,
            state=state,
            dob=dob,
            limit=limit,
        )
        records = _leie_records(rows)
        response = LEIESearchResponse(
            search_type="individual",
            query={
                "last_name": last_name,
                "first_name": first_name,
                "state": state,
                "dob": dob,
            },
            status=_leie_status(records),
            total_results=len(records),
            records=records,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_leie_individual failed")
        return error_response(f"search_leie_individual failed: {e}")


@mcp.tool(structured_output=True)
async def search_leie_entity(
    entity_name: str = "",
    state: str = "",
    npi: str = "",
    limit: int = 25,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Search the current HHS OIG LEIE file for an excluded business/entity."""
    try:
        if not entity_name.strip() and not npi.strip():
            return error_response("entity_name or npi is required.", code="invalid_params")
        normalized_npi = normalize_npi(npi) if npi.strip() else None
        if npi.strip() and not normalized_npi:
            return error_response(
                "When provided, npi must be a valid non-placeholder 10-digit NPI.",
                code="invalid_params",
            )

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        rows = data_loaders.query_leie_by_entity(
            entity_name=entity_name,
            state=state,
            npi=normalized_npi or "",
            limit=limit,
        )
        records = _leie_records(rows)
        response = LEIESearchResponse(
            search_type="entity",
            query={"entity_name": entity_name, "state": state, "npi": normalized_npi or ""},
            status=_leie_status(records),
            total_results=len(records),
            records=records,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_leie_entity failed")
        return error_response(f"search_leie_entity failed: {e}")


@mcp.tool(structured_output=True)
async def screen_leie_batch(
    candidates: list[dict[str, str]],
    limit_per_candidate: int = 5,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Screen up to 100 people/entities against the current HHS OIG LEIE file."""
    try:
        if len(candidates) > 100:
            return error_response(
                "screen_leie_batch accepts at most 100 candidates per call.",
                code="invalid_params",
            )
        if any(_contains_sensitive_identifier_keys(dict(candidate)) for candidate in candidates):
            return error_response(
                "LEIE screening does not accept SSN, EIN, TIN, or tax identifier fields.",
                code="invalid_params",
            )

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        raw_results = data_loaders.screen_leie_candidates(
            [dict(candidate) for candidate in candidates],
            limit_per_candidate=limit_per_candidate,
        )
        results: list[LEIEBatchResult] = []
        for raw in raw_results:
            result_metadata = raw.get("source_metadata") or metadata
            results.append(LEIEBatchResult(
                candidate=LEIEBatchCandidate(
                    **{
                        k: v
                        for k, v in raw.get("candidate", {}).items()
                        if k in LEIEBatchCandidate.model_fields
                    }
                ),
                status=raw.get("status", "no_current_leie_match_found"),
                match_count=int(raw.get("match_count", 0) or 0),
                best_match_score=int(raw.get("best_match_score", 0) or 0),
                matches=_leie_records(raw.get("matches", [])),
                screened_at=str(raw.get("screened_at", "")),
                source_metadata=_leie_source_metadata(result_metadata),
                oig_verification_caveat=OIG_LEIE_CAVEAT,
            ))

        response = LEIEBatchResponse(
            total_candidates=len(candidates),
            results=results,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        return to_structured(response.model_dump())
    except ValueError as e:
        return error_response(str(e), code="invalid_params")
    except Exception as e:
        logger.exception("screen_leie_batch failed")
        return error_response(f"screen_leie_batch failed: {e}")


@mcp.tool(structured_output=True)
async def get_leie_metadata(force_refresh: bool = False) -> dict[str, Any]:
    """Return HHS OIG LEIE source/cache metadata without screening a person."""
    try:
        metadata = (
            await data_loaders.ensure_leie_cached(force_refresh=True)
            if force_refresh
            else data_loaders.get_leie_source_metadata()
        )
        return to_structured(_leie_source_metadata(metadata).model_dump())
    except Exception as e:
        logger.exception("get_leie_metadata failed")
        return error_response(f"get_leie_metadata failed: {e}")


# ---------------------------------------------------------------------------
# Tool 8: SAM.gov Exclusions screening
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_sam_exclusions(
    entity_name: str = "",
    first_name: str = "",
    last_name: str = "",
    uei: str = "",
    cage_code: str = "",
    npi: str = "",
    state: str = "",
    country: str = "",
    classification: str = "",
    exclusion_type: str = "",
    excluding_agency: str = "",
    limit: int = 10,
) -> dict[str, Any]:
    """Search active SAM.gov Exclusions records through the v4 JSON API."""
    try:
        query = _sam_query_payload(
            entity_name=entity_name,
            first_name=first_name,
            last_name=last_name,
            uei=uei,
            cage_code=cage_code,
            npi=npi,
            state=state,
            country=country,
            classification=classification,
            exclusion_type=exclusion_type,
            excluding_agency=excluding_agency,
        )
        if not query:
            return error_response(
                "At least one SAM.gov Exclusions search parameter is required.",
                code="invalid_params",
            )

        normalized_npi = normalize_npi(npi) if npi.strip() else ""
        if npi.strip() and not normalized_npi:
            return error_response(
                "When provided, npi must be a valid non-placeholder 10-digit NPI.",
                code="invalid_params",
            )

        raw = await sam_exclusions_client.search_exclusions(
            entity_name=entity_name,
            first_name=first_name,
            last_name=last_name,
            uei=uei,
            cage_code=cage_code,
            npi=normalized_npi or npi,
            state=state,
            country=country,
            classification=classification,
            exclusion_type=exclusion_type,
            excluding_agency=excluding_agency,
            limit=limit,
        )
        if "error" in raw:
            return _sam_error_response(raw, "search_sam_exclusions")

        metadata = _sam_source_metadata(raw.get("source_metadata", {}))
        records = _sam_records(raw.get("excludedEntity", []), metadata.query)
        response = SAMExclusionSearchResponse(
            search_type="search",
            query=query,
            status=_sam_status(records),
            total_results=int(raw.get("totalRecords", len(records)) or 0),
            records=records,
            source_metadata=metadata,
            sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("search_sam_exclusions failed")
        return error_response(f"search_sam_exclusions failed: {e}")


@mcp.tool(structured_output=True)
async def check_sam_exclusion_identifier(
    uei: str = "",
    cage_code: str = "",
    npi: str = "",
    limit: int = 10,
) -> dict[str, Any]:
    """Check public identifiers against active SAM.gov Exclusions records."""
    try:
        if not uei.strip() and not cage_code.strip() and not npi.strip():
            return error_response(
                "At least one of uei, cage_code, or npi is required.",
                code="invalid_params",
            )
        normalized_npi = normalize_npi(npi) if npi.strip() else ""
        if npi.strip() and not normalized_npi:
            return error_response(
                "When provided, npi must be a valid non-placeholder 10-digit NPI.",
                code="invalid_params",
            )

        raw = await sam_exclusions_client.check_identifier(
            uei=uei,
            cage_code=cage_code,
            npi=normalized_npi or npi,
            limit=limit,
        )
        if "error" in raw:
            return _sam_error_response(raw, "check_sam_exclusion_identifier")

        metadata = _sam_source_metadata(raw.get("source_metadata", {}))
        records = _sam_records(raw.get("excludedEntity", []), metadata.query)
        response = SAMExclusionSearchResponse(
            search_type="identifier",
            query=_sam_query_payload(uei=uei, cage_code=cage_code, npi=normalized_npi or npi),
            status=_sam_status(records),
            total_results=int(raw.get("totalRecords", len(records)) or 0),
            records=records,
            source_metadata=metadata,
            sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("check_sam_exclusion_identifier failed")
        return error_response(f"check_sam_exclusion_identifier failed: {e}")


@mcp.tool(structured_output=True)
async def screen_sam_exclusions_batch(
    candidates: list[dict[str, str]],
    limit_per_candidate: int = 5,
) -> dict[str, Any]:
    """Screen up to 100 candidates against active SAM.gov Exclusions records."""
    try:
        if len(candidates) > sam_exclusions_client.MAX_BATCH_SIZE:
            return error_response(
                f"screen_sam_exclusions_batch accepts at most {sam_exclusions_client.MAX_BATCH_SIZE} "
                "candidates per call.",
                code="invalid_params",
            )
        if any(_contains_sensitive_identifier_keys(dict(candidate)) for candidate in candidates):
            return error_response(
                "SAM.gov Exclusions screening does not accept SSN, EIN, TIN, or tax identifier fields.",
                code="invalid_params",
            )

        safe_limit = max(1, min(int(limit_per_candidate), 10))
        results: list[SAMExclusionBatchResult] = []
        response_metadata = sam_exclusions_client.source_metadata(limit=safe_limit)
        for candidate_payload in candidates:
            candidate_data = {
                key: value
                for key, value in dict(candidate_payload).items()
                if key in SAMExclusionBatchCandidate.model_fields
            }
            candidate = SAMExclusionBatchCandidate(**candidate_data)
            if candidate.npi.strip():
                normalized_npi = normalize_npi(candidate.npi)
                if not normalized_npi:
                    return error_response(
                        "When provided in a SAM.gov Exclusions batch candidate, "
                        "npi must be a valid non-placeholder 10-digit NPI.",
                        code="invalid_params",
                    )
                candidate.npi = normalized_npi
            raw = await sam_exclusions_client.search_exclusions(
                entity_name=candidate.entity_name,
                first_name=candidate.first_name,
                last_name=candidate.last_name,
                uei=candidate.uei,
                cage_code=candidate.cage_code,
                npi=candidate.npi,
                state=candidate.state,
                country=candidate.country,
                classification=candidate.classification,
                limit=safe_limit,
            )
            if "error" in raw:
                metadata = _sam_source_metadata(raw.get("source_metadata", {}))
                results.append(SAMExclusionBatchResult(
                    candidate=candidate,
                    status="source_error",
                    match_count=0,
                    matches=[],
                    match_basis="source_error",
                    best_match_score=0,
                    screened_at=datetime.now(UTC).isoformat(),
                    source_metadata=metadata,
                    sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
                ))
                response_metadata = metadata.model_dump()
                continue

            metadata = _sam_source_metadata(raw.get("source_metadata", {}))
            records = _sam_records(raw.get("excludedEntity", []), metadata.query)
            status = _sam_status(records)
            results.append(SAMExclusionBatchResult(
                candidate=candidate,
                status=status,
                match_count=len(records),
                matches=records,
                match_basis=records[0].match_basis if records else "",
                best_match_score=max((record.match_score for record in records), default=0),
                screened_at=datetime.now(UTC).isoformat(),
                source_metadata=metadata,
                sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
            ))
            response_metadata = metadata.model_dump()

        response = SAMExclusionBatchResponse(
            total_candidates=len(candidates),
            results=results,
            source_metadata=_sam_source_metadata(response_metadata),
            sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
        )
        return to_structured(response.model_dump())
    except Exception as e:
        logger.exception("screen_sam_exclusions_batch failed")
        return error_response(f"screen_sam_exclusions_batch failed: {e}")


@mcp.tool(structured_output=True)
async def get_sam_exclusions_metadata() -> dict[str, Any]:
    """Return SAM.gov Exclusions API metadata without running a search."""
    try:
        metadata = sam_exclusions_client.source_metadata()
        return to_structured(_sam_source_metadata(metadata).model_dump())
    except Exception as e:
        logger.exception("get_sam_exclusions_metadata failed")
        return error_response(f"get_sam_exclusions_metadata failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
