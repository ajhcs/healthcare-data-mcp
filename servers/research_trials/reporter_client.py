"""NIH RePORTER API v2 client and response normalization."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from shared.utils.http_client import resilient_request

from .models import (
    NIHFundingInstitute,
    NIHOrganization,
    NIHPrincipalInvestigator,
    NIHProject,
    NIHPublication,
    SourceMetadata,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.reporter.nih.gov/v2"
PROJECT_SEARCH_URL = f"{BASE_URL}/projects/search"
PUBLICATION_SEARCH_URL = f"{BASE_URL}/publications/search"
TIMEOUT = 30.0


def _clean_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    return [str(value).strip() for value in values or [] if str(value).strip()]


def _clean_years(values: list[int] | tuple[int, ...] | None) -> list[int]:
    years: list[int] = []
    for value in values or []:
        try:
            years.append(int(value))
        except (TypeError, ValueError):
            continue
    return years


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _str(value: Any) -> str:
    return "" if value is None else str(value).strip()


def build_project_search_payload(
    *,
    org_name: str = "",
    org_uei: str = "",
    pi_name: str = "",
    text: str = "",
    fiscal_years: list[int] | None = None,
    activity_codes: list[str] | None = None,
    agencies: list[str] | None = None,
    project_nums: list[str] | None = None,
    appl_ids: list[int] | None = None,
    limit: int = 25,
    offset: int = 0,
) -> dict[str, Any]:
    """Build a NIH RePORTER project search payload."""
    criteria: dict[str, Any] = {}

    # RePORTER returns UEI fields but currently documents them as response
    # attributes rather than project-search payload criteria, so UEI is applied
    # as a conservative post-filter in server/profile code.
    if org_name:
        criteria["org_names"] = [_str(org_name).upper()]

    if pi_name:
        criteria["pi_names"] = [{"any_name": _str(pi_name)}]
    if text:
        criteria["advanced_text_search"] = {
            "operator": "and",
            "search_field": "all",
            "search_text": _str(text),
        }

    years = _clean_years(fiscal_years)
    if years:
        criteria["fiscal_years"] = years

    activities = _clean_list(activity_codes)
    if activities:
        criteria["activity_codes"] = activities

    agency_values = _clean_list(agencies)
    if agency_values:
        criteria["agencies"] = agency_values

    project_values = _clean_list(project_nums)
    if project_values:
        criteria["project_nums"] = project_values

    if appl_ids:
        criteria["appl_ids"] = [int(value) for value in appl_ids]

    return {
        "criteria": criteria,
        "limit": _bounded_int(limit, default=25, minimum=1, maximum=500),
        "offset": _bounded_int(offset, default=0, minimum=0, maximum=10_000_000),
        "sort_field": "fiscal_year",
        "sort_order": "desc",
    }


async def search_projects(
    *,
    org_name: str = "",
    org_uei: str = "",
    pi_name: str = "",
    text: str = "",
    fiscal_years: list[int] | None = None,
    activity_codes: list[str] | None = None,
    agencies: list[str] | None = None,
    limit: int = 25,
    offset: int = 0,
) -> dict[str, Any]:
    """Search NIH RePORTER projects and return the raw JSON response."""
    payload = build_project_search_payload(
        org_name=org_name,
        org_uei=org_uei,
        pi_name=pi_name,
        text=text,
        fiscal_years=fiscal_years,
        activity_codes=activity_codes,
        agencies=agencies,
        limit=limit,
        offset=offset,
    )
    try:
        resp = await resilient_request("POST", PROJECT_SEARCH_URL, json=payload, timeout=TIMEOUT)
        return resp.json()
    except Exception as exc:
        logger.warning("NIH RePORTER search failed: %s", exc)
        return {"error": str(exc), "request": payload}


async def get_project(project_num: str = "", appl_id: str = "") -> dict[str, Any]:
    """Fetch a single NIH RePORTER project by project number or application ID."""
    if not project_num and not appl_id:
        return {"error": "Either project_num or appl_id is required."}

    payload = build_project_search_payload(
        project_nums=[project_num] if project_num else None,
        appl_ids=[int(appl_id)] if appl_id else None,
        limit=1,
        offset=0,
    )
    try:
        resp = await resilient_request("POST", PROJECT_SEARCH_URL, json=payload, timeout=TIMEOUT)
        return resp.json()
    except Exception as exc:
        logger.warning("NIH RePORTER project detail failed: %s", exc)
        return {"error": str(exc), "request": payload}


async def search_publications_by_appl_id(appl_id: str, limit: int = 10) -> dict[str, Any]:
    """Search publications associated with a NIH application ID."""
    if not appl_id:
        return {"results": [], "meta": {"total": 0, "offset": 0, "limit": 0}}
    payload = {"criteria": {"appl_ids": [int(appl_id)]}, "limit": _bounded_int(limit, default=10, minimum=1, maximum=100), "offset": 0}
    try:
        resp = await resilient_request("POST", PUBLICATION_SEARCH_URL, json=payload, timeout=TIMEOUT)
        return resp.json()
    except Exception as exc:
        logger.info("NIH RePORTER publication search failed for %s: %s", appl_id, exc)
        return {"results": [], "meta": {"total": 0, "offset": 0, "limit": limit}, "warning": str(exc)}


def normalize_project(raw: dict[str, Any]) -> NIHProject:
    """Normalize one NIH RePORTER project result into the public model."""
    org = raw.get("organization") or {}
    org_ueis = org.get("org_ueis") or []
    org_duns = org.get("org_duns") or []

    pis: list[NIHPrincipalInvestigator] = []
    for pi in raw.get("principal_investigators") or []:
        pis.append(
            NIHPrincipalInvestigator(
                profile_id=_str(pi.get("profile_id")),
                full_name=_str(pi.get("full_name")),
                first_name=_str(pi.get("first_name")),
                middle_name=_str(pi.get("middle_name")),
                last_name=_str(pi.get("last_name")),
                title=_str(pi.get("title")),
                is_contact_pi=bool(pi.get("is_contact_pi")),
            )
        )

    fundings: list[NIHFundingInstitute] = []
    for funding in raw.get("agency_ic_fundings") or []:
        fiscal_year = funding.get("fy")
        fundings.append(
            NIHFundingInstitute(
                fiscal_year=int(fiscal_year) if fiscal_year is not None else None,
                code=_str(funding.get("code")),
                name=_str(funding.get("name")),
                abbreviation=_str(funding.get("abbreviation")),
                total_cost=_float(funding.get("total_cost")),
                direct_cost=_float(funding.get("direct_cost_ic")),
                indirect_cost=_float(funding.get("indirect_cost_ic")),
            )
        )

    terms = _split_terms(raw.get("pref_terms") or raw.get("terms") or "")

    fiscal_year = raw.get("fiscal_year")
    return NIHProject(
        appl_id=_str(raw.get("appl_id")),
        project_num=_str(raw.get("project_num")),
        core_project_num=_str(raw.get("core_project_num")),
        title=_str(raw.get("project_title")),
        abstract=_str(raw.get("abstract_text")),
        public_health_relevance=_str(raw.get("phr_text")),
        fiscal_year=int(fiscal_year) if fiscal_year is not None else None,
        award_amount=_float(raw.get("award_amount")),
        activity_code=_str(raw.get("activity_code")),
        agency_code=_str(raw.get("agency_code")),
        funding_mechanism=_str(raw.get("funding_mechanism")),
        award_notice_date=_str(raw.get("award_notice_date")),
        project_start_date=_str(raw.get("project_start_date")),
        project_end_date=_str(raw.get("project_end_date")),
        project_detail_url=_str(raw.get("project_detail_url")),
        organization=NIHOrganization(
            name=_str(org.get("org_name")),
            department=_str(org.get("dept_type")),
            city=_str(org.get("org_city") or org.get("city")),
            state=_str(org.get("org_state")),
            country=_str(org.get("org_country") or org.get("country")),
            zip_code=_str(org.get("org_zipcode")),
            uei=_str(org.get("primary_uei") or (org_ueis[0] if org_ueis else "")),
            duns=_str(org.get("primary_duns") or (org_duns[0] if org_duns else "")),
            ipf_code=_str(org.get("org_ipf_code") or org.get("external_org_id")),
        ),
        principal_investigators=pis,
        institute_fundings=fundings,
        terms=terms,
    )


def normalize_publication(raw: dict[str, Any]) -> NIHPublication:
    """Normalize one NIH RePORTER publication result."""
    return NIHPublication(
        pmid=_str(raw.get("pmid") or raw.get("PMID")),
        title=_str(raw.get("title") or raw.get("article_title")),
        journal=_str(raw.get("journal") or raw.get("journal_title")),
        publication_year=_str(raw.get("publication_year") or raw.get("pub_year")),
        core_project_num=_str(raw.get("core_project_num") or raw.get("coreproject")),
        appl_id=_str(raw.get("appl_id") or raw.get("latest_appl_id")),
    )


def metadata_from_response(raw: dict[str, Any]) -> SourceMetadata:
    """Build source metadata from a RePORTER search response."""
    meta = raw.get("meta") or {}
    properties = meta.get("properties") or {}
    return SourceMetadata(
        source_name="NIH RePORTER",
        source_url=PROJECT_SEARCH_URL,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        search_id=_str(meta.get("search_id")),
        source_detail_url=_str(properties.get("URL") or properties.get("url")),
    )


def _split_terms(value: str) -> list[str]:
    if not value:
        return []
    if "<" in value and ">" in value:
        return [term.strip() for term in value.replace(">", "<").split("<") if term.strip()]
    return [term.strip() for term in value.split(";") if term.strip()]


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))
