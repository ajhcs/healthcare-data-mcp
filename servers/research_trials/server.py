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
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources

from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured

from . import clinical_trials_client, profiles, reporter_client
from .models import (
    ClinicalTrialDetailResponse,
    ClinicalTrialSearchResponse,
    TrialInventoryResponse,
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
register_standard_resources(mcp, "research-trials")


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


def _metadata_dict(metadata: Any) -> dict[str, Any]:
    if hasattr(metadata, "model_dump"):
        return metadata.model_dump()
    if isinstance(metadata, dict):
        return dict(metadata)
    return {}


def _research_identity(*, name: str = "", uei: str = "", facility_name: str = "", state: str = "") -> dict[str, Any]:
    identity = identity_from_public_record(
        name=facility_name or name,
        entity_type="research_organization" if not facility_name else "facility",
        source_name="NIH RePORTER and ClinicalTrials.gov public registries",
    ).to_dict()
    if uei:
        identity["unresolved_identifiers"].append({"type": "uei", "value": uei.strip().upper()})
    if state:
        identity["state"] = state.strip().upper()
    return identity


def _research_evidence(
    *,
    source_metadata: dict[str, Any],
    dataset_id: str,
    source_period: str = "",
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=source_metadata,
        source_name=str(source_metadata.get("source_name") or "NIH RePORTER and ClinicalTrials.gov"),
        source_url=str(source_metadata.get("source_url") or "https://reporter.nih.gov/"),
        dataset_id=dataset_id,
        source_period=source_period or str(source_metadata.get("data_timestamp") or ""),
        landing_page=str(source_metadata.get("source_detail_url") or ""),
        retrieved_at=str(source_metadata.get("retrieved_at") or ""),
        cache_status=str(source_metadata.get("cache_status") or "live_api"),
        entity_scope="research_public_registry",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _attach_research_context(
    payload: dict[str, Any],
    *,
    dataset_id: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    identity: dict[str, Any] | None = None,
    identity_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = _metadata_dict(payload.get("metadata") or payload.get("funding", {}).get("metadata"))
    payload["source_metadata"] = metadata
    payload["evidence"] = _research_evidence(
        source_metadata=metadata,
        dataset_id=dataset_id,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )
    if identity is not None:
        payload["identity"] = identity
    if identity_map is not None:
        payload["identity_map"] = identity_map
    return payload


def _attach_nih_project_row_evidence(payload: dict[str, Any], *, query: dict[str, Any]) -> None:
    metadata = _metadata_dict(payload.get("metadata"))
    projects = payload.get("projects") if isinstance(payload.get("projects"), list) else []
    if isinstance(payload.get("project"), dict):
        projects = [payload["project"], *projects]
    for project in projects:
        if not isinstance(project, dict):
            continue
        row_query = {
            **query,
            "appl_id": project.get("appl_id") or "",
            "project_num": project.get("project_num") or "",
            "core_project_num": project.get("core_project_num") or "",
            "organization_name": (project.get("organization") or {}).get("name") if isinstance(project.get("organization"), dict) else "",
            "organization_uei": (project.get("organization") or {}).get("uei") if isinstance(project.get("organization"), dict) else "",
            "fiscal_year": project.get("fiscal_year") or "",
        }
        project["evidence"] = _research_evidence(
            source_metadata=metadata,
            dataset_id="nih_reporter_projects",
            query={key: value for key, value in row_query.items() if value},
            match_basis="nih_reporter_project_row",
            confidence="source_row",
            caveat=(
                "NIH RePORTER project rows are public registry records for research funding; "
                "organization names and UEIs remain source-specific identifiers until reconciled."
            ),
            next_step="Use appl_id or project_num for exact follow-up before citing a project fact.",
        )


def _attach_nih_publication_row_evidence(payload: dict[str, Any], *, query: dict[str, Any]) -> None:
    metadata = _metadata_dict(payload.get("metadata"))
    for publication in payload.get("publications") or []:
        if not isinstance(publication, dict):
            continue
        row_query = {
            **query,
            "pmid": publication.get("pmid") or "",
            "core_project_num": publication.get("core_project_num") or "",
            "publication_year": publication.get("publication_year") or "",
        }
        publication["evidence"] = _research_evidence(
            source_metadata=metadata,
            dataset_id="nih_reporter_publications",
            query={key: value for key, value in row_query.items() if value},
            match_basis="nih_reporter_publication_row",
            confidence="source_row",
            caveat="NIH RePORTER publication rows are linked public bibliography records, not direct funding amounts or clinical outcome evidence.",
            next_step="Use PMID and appl_id/core_project_num to verify the publication-project linkage before citing.",
        )


def _attach_funding_profile_row_evidence(payload: dict[str, Any], *, query: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    _attach_nih_project_row_evidence(payload, query=query)
    metadata = _metadata_dict(payload.get("metadata"))
    aggregate_specs = {
        "by_fiscal_year": ("fiscal_year", "nih_reporter_fiscal_year_aggregate"),
        "by_institute": ("institute", "nih_reporter_institute_aggregate"),
        "by_pi": ("pi_name", "nih_reporter_pi_aggregate"),
        "by_activity_code": ("activity_code", "nih_reporter_activity_code_aggregate"),
        "top_terms": ("term", "nih_reporter_term_aggregate"),
    }
    for collection, (group_field, match_basis) in aggregate_specs.items():
        for row in payload.get(collection) or []:
            if not isinstance(row, dict):
                continue
            row["evidence"] = _research_evidence(
                source_metadata=metadata,
                dataset_id="nih_reporter_projects",
                query={
                    **{key: value for key, value in query.items() if value},
                    group_field: row.get(group_field) or "",
                },
                match_basis=match_basis,
                confidence="aggregate_from_returned_project_rows",
                caveat="NIH funding profile aggregates summarize returned RePORTER project rows and inherit the search/filter limitations.",
                next_step="Review the underlying project row receipts before citing an aggregate.",
            )


def _attach_clinical_trial_row_evidence(payload: dict[str, Any], *, query: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    metadata = _metadata_dict(payload.get("metadata"))
    trials = payload.get("trials") if isinstance(payload.get("trials"), list) else []
    if isinstance(payload.get("trial"), dict):
        trials = [payload["trial"], *trials]
    for trial in trials:
        if not isinstance(trial, dict):
            continue
        row_query = {
            **query,
            "nct_id": trial.get("nct_id") or "",
            "lead_sponsor": (trial.get("lead_sponsor") or {}).get("name") if isinstance(trial.get("lead_sponsor"), dict) else "",
            "organization": trial.get("organization") or "",
            "overall_status": trial.get("overall_status") or "",
        }
        trial["evidence"] = _research_evidence(
            source_metadata=metadata,
            dataset_id="clinicaltrials_gov",
            query={key: value for key, value in row_query.items() if value},
            match_basis="clinicaltrials_study_row",
            confidence="source_row",
            caveat=(
                "ClinicalTrials.gov study rows are public registry records; sponsor and site names are source-specific aliases "
                "unless reconciled with exact identifiers or reviewed source context."
            ),
            next_step="Use the NCT ID for exact trial follow-up and preserve sponsor/site role context.",
        )


def _attach_clinical_trial_inventory_row_evidence(
    payload: dict[str, Any],
    *,
    kind: str,
    query: dict[str, Any],
) -> None:
    metadata = _metadata_dict(payload.get("metadata"))
    for record in payload.get("records") or []:
        if not isinstance(record, dict):
            continue
        if kind == "site":
            row_query = {
                **query,
                "normalized_facility_name": record.get("normalized_facility_name") or "",
                "city": record.get("city") or "",
                "state": record.get("state") or "",
                "zip_code": record.get("zip_code") or "",
                "nct_ids": record.get("nct_ids") or [],
            }
            match_basis = "clinicaltrials_site_inventory_row"
            caveat = "Site inventory rows group public ClinicalTrials.gov locations by facility and geography; blank facilities are excluded."
        else:
            row_query = {
                **query,
                "normalized_sponsor_name": record.get("normalized_sponsor_name") or "",
                "display_names": record.get("display_names") or [],
                "nct_ids": record.get("nct_ids") or [],
            }
            match_basis = "clinicaltrials_sponsor_inventory_row"
            caveat = "Sponsor inventory rows group public ClinicalTrials.gov names by role counts; grouping is not proof of common control."
        record["evidence"] = _research_evidence(
            source_metadata=metadata,
            dataset_id="clinicaltrials_gov",
            query={key: value for key, value in row_query.items() if value},
            match_basis=match_basis,
            confidence=str(record.get("match_confidence") or "conservative_public_registry_inventory"),
            caveat=caveat,
            next_step="Review NCT IDs, display names, roles, and geography before citing the inventory row.",
        )


def _inventory_identity_map(records: list[dict[str, Any]], *, kind: str) -> dict[str, Any]:
    entities = []
    for row in records:
        if kind == "site":
            entities.append(
                _research_identity(
                    facility_name=str(row.get("display_names", [""])[0] if row.get("display_names") else row.get("normalized_facility_name", "")),
                    state=str(row.get("state") or ""),
                )
                | {
                    "city": str(row.get("city") or ""),
                    "country": str(row.get("country") or ""),
                    "zip_code": str(row.get("zip_code") or ""),
                    "nct_ids": list(row.get("nct_ids") or []),
                    "match_basis": str(row.get("match_basis") or ""),
                    "confidence": str(row.get("match_confidence") or ""),
                }
            )
        else:
            entities.append(
                _research_identity(
                    name=str(row.get("display_names", [""])[0] if row.get("display_names") else row.get("normalized_sponsor_name", "")),
                )
                | {
                    "nct_ids": list(row.get("nct_ids") or []),
                    "match_basis": str(row.get("match_basis") or ""),
                    "confidence": str(row.get("match_confidence") or ""),
                }
            )
    return {
        "entities": entities,
        "match_basis": "clinicaltrials_public_inventory_grouping",
        "source_claims": [
            {
                "collection": f"clinicaltrials_{kind}_inventory",
                "identity_paths": ["evidence.query"],
                "evidence_path": "evidence",
                "source_metadata_path": "source_metadata",
                "row_evidence_paths": ["records[].evidence"] if records else [],
                "match_policy": "clinicaltrials_inventory_grouping_requires_source_row_review",
            }
        ],
        "conflict_policy": "Do not merge sponsors or sites without matching role/geography identifiers and source review.",
    }


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
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
    """Search NIH RePORTER projects by organization, PI, topic, year, activity, or agency.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_nih_projects","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
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
        payload = response.model_dump()
        _attach_nih_project_row_evidence(
            payload,
            query={
                "org_name": org_name,
                "org_uei": org_uei,
                "pi_name": pi_name,
                "text": text,
                "fiscal_years": _clean_int_list(fiscal_years),
                "activity_codes": _clean_str_list(activity_codes),
                "agencies": _clean_str_list(agencies),
            },
        )
        payload = _attach_research_context(
            payload,
            dataset_id="nih_reporter_projects",
            query={
                "org_name": org_name,
                "org_uei": org_uei,
                "pi_name": pi_name,
                "text": text,
                "fiscal_years": _clean_int_list(fiscal_years),
                "activity_codes": _clean_str_list(activity_codes),
                "agencies": _clean_str_list(agencies),
                "limit": request_limit,
                "offset": request_offset,
            },
            match_basis="nih_reporter_search_filters",
            confidence="candidate_public_registry_matches_require_review",
            caveat="NIH RePORTER search results are public registry records; organization-name matches are candidates unless UEI or exact source identifiers support the join.",
            next_step="Use project_num/appl_id for exact project follow-up and preserve organization aliases separately.",
            identity=_research_identity(name=org_name, uei=org_uei),
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("search_nih_projects failed")
        return error_response(f"search_nih_projects failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
async def get_nih_project(project_num: str = "", appl_id: str = "") -> dict[str, Any]:
    """Fetch one NIH RePORTER project by project number or application ID.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_nih_project","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
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
        payload = response.model_dump()
        _attach_nih_project_row_evidence(payload, query={"project_num": project_num, "appl_id": appl_id})
        _attach_nih_publication_row_evidence(payload, query={"project_num": project_num, "appl_id": lookup_appl_id})
        payload = _attach_research_context(
            payload,
            dataset_id="nih_reporter_projects",
            query={"project_num": project_num, "appl_id": appl_id},
            match_basis="appl_id_or_project_num_lookup",
            confidence="high_for_exact_nih_project_identifier" if (project_num or appl_id) else "none",
            caveat="NIH RePORTER project details are public registry facts tied to the requested project/application identifier.",
            next_step="Use organization UEI/name from the project record only as a source-specific alias unless independently reconciled.",
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("get_nih_project failed")
        return error_response(f"get_nih_project failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
async def profile_research_funding(
    org_name: str = "",
    org_uei: str = "",
    years: list[int] | None = None,
) -> dict[str, Any]:
    """Aggregate NIH RePORTER funding by year, institute, PI, activity code, and terms.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"profile_research_funding","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not org_name:
            return error_response(
                "org_name is required. org_uei is supported as a post-search filter when org_name is supplied.",
                code="invalid_params",
            )

        response = await profiles.build_funding_profile(org_name=org_name, org_uei=org_uei, years=_clean_int_list(years))
        payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
        _attach_funding_profile_row_evidence(payload, query={"org_name": org_name, "org_uei": org_uei, "years": _clean_int_list(years)})
        payload = _attach_research_context(
            payload,
            dataset_id="nih_reporter_projects",
            query={"org_name": org_name, "org_uei": org_uei, "years": _clean_int_list(years)},
            match_basis="organization_name_search_with_optional_uei_filter",
            confidence="high_for_uei_filter" if org_uei else "candidate_public_registry_matches_require_review",
            caveat="NIH funding profiles are public RePORTER aggregates by search criteria; they are not a complete internal research portfolio.",
            next_step="Review included project records and unresolved aliases before aggregating across organizations.",
            identity=_research_identity(name=org_name, uei=org_uei),
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("profile_research_funding failed")
        return error_response(f"profile_research_funding failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
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
    """Search ClinicalTrials.gov v2 studies with metadata and page-token pagination.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_clinical_trials","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
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
        payload = response.model_dump()
        _attach_clinical_trial_row_evidence(
            payload,
            query={
                "query": query,
                "sponsor": sponsor,
                "condition": condition,
                "intervention": intervention,
                "location": location,
                "status": status,
                "phase": phase,
            },
        )
        payload = _attach_research_context(
            payload,
            dataset_id="clinicaltrials_gov",
            query={
                "query": query,
                "sponsor": sponsor,
                "condition": condition,
                "intervention": intervention,
                "location": location,
                "status": status,
                "phase": phase,
                "fields": _clean_str_list(fields),
                "page_size": _validate_limit(page_size, 100),
                "page_token": page_token,
            },
            match_basis="clinicaltrials_search_filters",
            confidence="candidate_public_registry_matches_require_review",
            caveat="ClinicalTrials.gov search results are public registry records; sponsor/site matches require source review before entity aggregation.",
            next_step="Use NCT IDs for exact trial follow-up and preserve sponsor/site role context.",
            identity=_research_identity(name=sponsor or query or location),
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("search_clinical_trials failed")
        return error_response(f"search_clinical_trials failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
async def get_clinical_trial(nct_id: str) -> dict[str, Any]:
    """Fetch one ClinicalTrials.gov v2 study by NCT ID.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_clinical_trial","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
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
        payload = response.model_dump()
        _attach_clinical_trial_row_evidence(payload, query={"nct_id": normalized})
        payload = _attach_research_context(
            payload,
            dataset_id="clinicaltrials_gov",
            query={"nct_id": normalized},
            match_basis="nct_id_exact",
            confidence="high_for_exact_clinicaltrials_identifier",
            caveat="ClinicalTrials.gov trial details are public registry facts tied to the requested NCT ID.",
            next_step="Use sponsor, collaborator, and site records as source-specific aliases unless reconciled by workflow identity.",
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("get_clinical_trial failed")
        return error_response(f"get_clinical_trial failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
async def inventory_clinical_trial_sponsors(
    sponsor: str,
    status: str = "",
    scan_limit: int = 500,
) -> dict[str, Any]:
    """Build a conservative ClinicalTrials.gov sponsor inventory with explicit role counts.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"inventory_clinical_trial_sponsors","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not sponsor:
            return error_response("sponsor is required.", code="invalid_params")
        hard_max = int(_os.environ.get("CLINICAL_TRIALS_INVENTORY_HARD_MAX", "5000"))
        response = await profiles.inventory_clinical_trial_sponsors(
            sponsor=sponsor,
            status=status,
            scan_limit=max(1, min(int(scan_limit or 500), hard_max)),
            hard_max=hard_max,
        )
        payload = TrialInventoryResponse(**response).model_dump()
        _attach_clinical_trial_inventory_row_evidence(
            payload,
            kind="sponsor",
            query={"sponsor": sponsor, "status": status},
        )
        payload = _attach_research_context(
            payload,
            dataset_id="clinicaltrials_gov",
            query={"sponsor": sponsor, "status": status, "scan_limit": payload.get("filters", {}).get("scan_limit")},
            match_basis="clinicaltrials_sponsor_inventory_grouping",
            confidence="conservative_public_registry_inventory",
            caveat="Sponsor inventory groups public registry names by role counts; it is not proof of common control or a complete research portfolio.",
            next_step="Review display_names, role_counts, and NCT IDs before using sponsor aggregates in reports.",
            identity=_research_identity(name=sponsor),
            identity_map=_inventory_identity_map(list(payload.get("records") or []), kind="sponsor"),
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("inventory_clinical_trial_sponsors failed")
        return error_response(f"inventory_clinical_trial_sponsors failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
async def inventory_clinical_trial_sites(
    location: str,
    status: str = "",
    scan_limit: int = 500,
) -> dict[str, Any]:
    """Build a conservative ClinicalTrials.gov site inventory keyed by facility plus geography.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"inventory_clinical_trial_sites","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not location:
            return error_response("location is required.", code="invalid_params")
        hard_max = int(_os.environ.get("CLINICAL_TRIALS_INVENTORY_HARD_MAX", "5000"))
        response = await profiles.inventory_clinical_trial_sites(
            location=location,
            status=status,
            scan_limit=max(1, min(int(scan_limit or 500), hard_max)),
            hard_max=hard_max,
        )
        payload = TrialInventoryResponse(**response).model_dump()
        _attach_clinical_trial_inventory_row_evidence(
            payload,
            kind="site",
            query={"location": location, "status": status},
        )
        payload = _attach_research_context(
            payload,
            dataset_id="clinicaltrials_gov",
            query={"location": location, "status": status, "scan_limit": payload.get("filters", {}).get("scan_limit")},
            match_basis="clinicaltrials_site_inventory_grouping",
            confidence="conservative_public_registry_inventory",
            caveat="Site inventory groups public registry locations by facility and geography; unresolved locations are excluded from exact site counts.",
            next_step="Review facility/geography keys and unresolved_location_count before using site aggregates in reports.",
            identity=_research_identity(facility_name=location),
            identity_map=_inventory_identity_map(list(payload.get("records") or []), kind="site"),
        )
        return to_structured(payload)
    except Exception as exc:
        logger.exception("inventory_clinical_trial_sites failed")
        return error_response(f"inventory_clinical_trial_sites failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("research-trials")
async def profile_research_activity(
    organization_name: str,
    uei: str = "",
    facility_name: str = "",
    state: str = "",
    years: list[int] | None = None,
) -> dict[str, Any]:
    """Combine NIH funding and ClinicalTrials.gov activity without silently merging ambiguous organizations.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"profile_research_activity","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
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
        payload = response.model_dump() if hasattr(response, "model_dump") else dict(response)
        _attach_funding_profile_row_evidence(
            payload.get("funding", {}),
            query={"organization_name": organization_name, "uei": uei, "years": _clean_int_list(years)},
        )
        _attach_clinical_trial_row_evidence(
            payload.get("trials", {}),
            query={"sponsor": organization_name, "location": " ".join(part for part in [facility_name, state] if part)},
        )
        metadata = _metadata_dict(payload.get("funding", {}).get("metadata") or payload.get("trials", {}).get("metadata"))
        if not metadata:
            metadata = {
                "source_name": "NIH RePORTER and ClinicalTrials.gov",
                "source_url": "https://reporter.nih.gov/",
                "source_detail_url": "https://clinicaltrials.gov/",
                "source_period": "live public registry profile request",
                "cache_status": "live_api",
            }
        payload["source_metadata"] = metadata
        payload["evidence"] = _research_evidence(
            source_metadata=metadata,
            dataset_id="research_activity_profile",
            query={
                "organization_name": organization_name,
                "uei": uei,
                "facility_name": facility_name,
                "state": state,
                "years": _clean_int_list(years),
            },
            match_basis=str(payload.get("match_decision", {}).get("status") or "organization_profile_request"),
            confidence=str(payload.get("match_decision", {}).get("confidence") or "candidate_public_registry_profile"),
            caveat="Combined research activity profiles preserve NIH and ClinicalTrials public registry caveats; ambiguous organizations are not silently merged.",
            next_step="Inspect match_decision, warnings, and component evidence before citing profile-level claims.",
        )
        payload["identity"] = _research_identity(name=organization_name, uei=uei, facility_name=facility_name, state=state)
        return to_structured(payload)
    except Exception as exc:
        logger.exception("profile_research_activity failed")
        return error_response(f"profile_research_activity failed: {exc}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
