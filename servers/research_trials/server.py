"""Research Funding & Clinical Trials MCP Server.

Provides NIH RePORTER funding search/profile tools and ClinicalTrials.gov v2
study search/detail tools. Port 8019 when run over HTTP transports.
"""

from __future__ import annotations

from typing import Any
import logging
import os as _os
import re

from mcp.server.fastmcp import FastMCP

from shared.utils.mcp_response import error_response, to_structured

from . import clinical_trials_client, profiles, reporter_client
from .models import (
    ClinicalTrialDetailResponse,
    ClinicalTrialSearchResponse,
    NIHProjectDetailResponse,
    NIHProjectSearchResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict[str, Any] = {"name": "research-trials"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8019"))
mcp = FastMCP(**_mcp_kwargs)


def _clean_int_list(values: list[int] | None) -> list[int]:
    cleaned: list[int] = []
    for value in values or []:
        try:
            cleaned.append(int(value))
        except (TypeError, ValueError):
            continue
    return cleaned


def _clean_str_list(values: list[str] | None) -> list[str]:
    return [str(value).strip() for value in values or [] if str(value).strip()]


def _validate_limit(limit: int, maximum: int = 100) -> int:
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        parsed = 25
    return max(1, min(parsed, maximum))


def _validate_offset(offset: int) -> int:
    try:
        parsed = int(offset)
    except (TypeError, ValueError):
        parsed = 0
    return max(0, parsed)


@mcp.tool(structured_output=True)
async def search_nih_projects(
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
    """Search NIH RePORTER projects by organization, PI, topic, year, activity, or agency."""
    try:
        if not any([org_name, pi_name, text]):
            return error_response(
                "At least one of org_name, pi_name, or text is required. NIH RePORTER does not document UEI as a search criterion.",
                code="invalid_params",
            )

        request_limit = _validate_limit(limit, 500)
        request_offset = _validate_offset(offset)
        raw = await reporter_client.search_projects(
            org_name=org_name,
            org_uei=org_uei,
            pi_name=pi_name,
            text=text,
            fiscal_years=_clean_int_list(fiscal_years),
            activity_codes=_clean_str_list(activity_codes),
            agencies=_clean_str_list(agencies),
            limit=request_limit,
            offset=request_offset,
        )
        if "error" in raw:
            return to_structured(raw)

        projects = [reporter_client.normalize_project(item) for item in raw.get("results") or []]
        if org_uei:
            projects = profiles.filter_projects_by_uei(projects, org_uei)

        meta = raw.get("meta") or {}
        raw_limit = int(meta.get("limit") or request_limit)
        raw_offset = int(meta.get("offset") or request_offset)
        raw_total = int(meta.get("total") or len(raw.get("results") or []))
        total_results = len(projects) if org_uei else raw_total
        next_offset = raw_offset + raw_limit if raw_offset + raw_limit < raw_total else None
        response = NIHProjectSearchResponse(
            total_results=total_results,
            limit=raw_limit,
            offset=raw_offset,
            next_offset=next_offset,
            projects=projects,
            metadata=reporter_client.metadata_from_response(raw),
        )
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("search_nih_projects failed")
        return error_response(f"search_nih_projects failed: {exc}")


@mcp.tool(structured_output=True)
async def get_nih_project(project_num: str = "", appl_id: str = "") -> dict[str, Any]:
    """Fetch one NIH RePORTER project by project number or application ID."""
    try:
        if not project_num and not appl_id:
            return error_response("Either project_num or appl_id is required.", code="invalid_params")
        if appl_id and not appl_id.isdigit():
            return error_response("appl_id must be numeric.", code="invalid_params")

        raw = await reporter_client.get_project(project_num=project_num, appl_id=appl_id)
        if "error" in raw:
            return to_structured(raw)

        project = None
        results = raw.get("results") or []
        if results:
            project = reporter_client.normalize_project(results[0])

        publications = []
        lookup_appl_id = appl_id or (project.appl_id if project else "")
        if lookup_appl_id:
            publication_raw = await reporter_client.search_publications_by_appl_id(lookup_appl_id, limit=10)
            publications = [reporter_client.normalize_publication(item) for item in publication_raw.get("results") or []]

        response = NIHProjectDetailResponse(
            project=project,
            publications=publications,
            metadata=reporter_client.metadata_from_response(raw),
        )
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("get_nih_project failed")
        return error_response(f"get_nih_project failed: {exc}")


@mcp.tool(structured_output=True)
async def profile_research_funding(
    org_name: str = "",
    org_uei: str = "",
    years: list[int] | None = None,
) -> dict[str, Any]:
    """Aggregate NIH RePORTER funding by year, institute, PI, activity code, and terms."""
    try:
        if not org_name:
            return error_response(
                "org_name is required. org_uei is supported as a post-search filter when org_name is supplied.",
                code="invalid_params",
            )

        response = await profiles.build_funding_profile(org_name=org_name, org_uei=org_uei, years=_clean_int_list(years))
        return to_structured(response.model_dump() if hasattr(response, "model_dump") else response)
    except Exception as exc:
        logger.exception("profile_research_funding failed")
        return error_response(f"profile_research_funding failed: {exc}")


@mcp.tool(structured_output=True)
async def search_clinical_trials(
    query: str = "",
    sponsor: str = "",
    condition: str = "",
    intervention: str = "",
    location: str = "",
    status: str = "",
    phase: str = "",
    fields: list[str] | None = None,
    page_size: int = 25,
    page_token: str = "",
) -> dict[str, Any]:
    """Search ClinicalTrials.gov v2 studies with metadata and page-token pagination."""
    try:
        if not any([query, sponsor, condition, intervention, location]):
            return error_response(
                "At least one of query, sponsor, condition, intervention, or location is required.",
                code="invalid_params",
            )

        raw = await clinical_trials_client.search_studies(
            query=query,
            sponsor=sponsor,
            condition=condition,
            intervention=intervention,
            location=location,
            status=status,
            phase=phase,
            fields=_clean_str_list(fields),
            page_size=_validate_limit(page_size, 100),
            page_token=page_token,
        )
        if "error" in raw:
            return to_structured(raw)

        trials = [clinical_trials_client.normalize_study(item) for item in raw.get("studies") or []]
        response = ClinicalTrialSearchResponse(
            total_results=int(raw.get("totalCount") or len(trials)),
            trials=trials,
            metadata=clinical_trials_client.metadata_from_response(raw, page_size=_validate_limit(page_size, 100)),
        )
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("search_clinical_trials failed")
        return error_response(f"search_clinical_trials failed: {exc}")


@mcp.tool(structured_output=True)
async def get_clinical_trial(nct_id: str) -> dict[str, Any]:
    """Fetch one ClinicalTrials.gov v2 study by NCT ID."""
    try:
        normalized = nct_id.strip().upper()
        if not re.fullmatch(r"NCT\d{8}", normalized):
            return error_response("nct_id must look like NCT########.", code="invalid_params")

        raw = await clinical_trials_client.get_study(normalized)
        if "error" in raw:
            return to_structured(raw)

        response = ClinicalTrialDetailResponse(
            trial=clinical_trials_client.normalize_study(raw),
            metadata=clinical_trials_client.metadata_from_response(raw, page_size=1),
        )
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("get_clinical_trial failed")
        return error_response(f"get_clinical_trial failed: {exc}")


@mcp.tool(structured_output=True)
async def profile_research_activity(
    organization_name: str,
    uei: str = "",
    facility_name: str = "",
    state: str = "",
    years: list[int] | None = None,
) -> dict[str, Any]:
    """Combine NIH funding and ClinicalTrials.gov activity without silently merging ambiguous organizations."""
    try:
        if not organization_name:
            return error_response("organization_name is required.", code="invalid_params")

        response = await profiles.build_research_activity_profile(
            organization_name=organization_name,
            uei=uei,
            facility_name=facility_name,
            state=state,
            years=_clean_int_list(years),
        )
        return to_structured(response.model_dump() if hasattr(response, "model_dump") else response)
    except Exception as exc:
        logger.exception("profile_research_activity failed")
        return error_response(f"profile_research_activity failed: {exc}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
