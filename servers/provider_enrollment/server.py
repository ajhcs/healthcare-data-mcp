"""CMS Provider Enrollment & Ownership MCP Server.

Provides PECOS-derived provider enrollment, facility ownership, and change of
ownership lookups from cached CMS public datasets. Port 8017.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any

from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources

from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_enrollment_id, normalize_npi, normalize_state
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured

from . import data_loaders, ownership_graph
from .models import (
    ChangeOfOwnershipRecord,
    ChangeOfOwnershipSearchResponse,
    EnrollmentRecord,
    FacilityOwnershipResponse,
    GraphEdge,
    GraphNode,
    OwnerNetworkResponse,
    OwnershipRecord,
    ProviderControlProfileResponse,
    ProviderEnrollmentDetailResponse,
    ProviderEnrollmentSearchResponse,
    SourceMetadata,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict[str, Any] = {"name": "provider-enrollment"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8017"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "provider-enrollment")


@mcp.tool(structured_output=True)
@observe_tool("provider-enrollment")
async def search_provider_enrollment(
    npi: str = "",
    provider_name: str = "",
    state: str = "",
    provider_type: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search CMS Medicare FFS and facility provider enrollment rows.

    Use NPI for exact identity searches. Use provider_name/state/provider_type
    for broader discovery. Results are bounded and include source metadata for
    cached CMS files.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_provider_enrollment","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    try:
        if not any([npi, provider_name, state, provider_type]):
            return error_response(
                "At least one of npi, provider_name, state, or provider_type is required.",
                code="invalid_params",
            )
        if npi and not normalize_npi(npi):
            return error_response("npi must be a valid 10-digit NPI.", code="invalid_params")

        bounded_limit = _bounded_limit(limit, 100)
        rows = data_loaders.search_enrollments(
            npi=npi,
            provider_name=provider_name,
            state=state,
            provider_type=provider_type,
            limit=bounded_limit,
        )
        response = ProviderEnrollmentSearchResponse(
            total_results=len(rows),
            limit=bounded_limit,
            enrollments=[_enrollment(row) for row in rows],
            metadata=_metadata(data_loaders.ENROLLMENT_DATASET_KEYS),
        )
        return to_structured(
            _with_provider_evidence(
                response.model_dump(),
                query={"npi": npi, "provider_name": provider_name, "state": state, "provider_type": provider_type},
                rows=rows,
                match_basis="npi_exact" if npi else "filtered_public_enrollment_search",
                confidence="high_identifier_match" if npi else "candidate_provider_matches",
            )
        )
    except Exception as exc:
        logger.exception("search_provider_enrollment failed")
        return error_response(f"search_provider_enrollment failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("provider-enrollment")
async def get_provider_enrollment_detail(
    npi: str = "",
    enrollment_id: str = "",
    associate_id: str = "",
) -> dict[str, Any]:
    """Fetch enrollment detail plus linked owners and CHOW history.

    Provide NPI, enrollment_id, or associate_id. The tool uses exact normalized
    identifiers and does not attempt fuzzy identity merges.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_provider_enrollment_detail","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    try:
        if not any([npi, enrollment_id, associate_id]):
            return error_response("npi, enrollment_id, or associate_id is required.", code="invalid_params")
        if npi and not normalize_npi(npi):
            return error_response("npi must be a valid 10-digit NPI.", code="invalid_params")

        enrollment_rows = data_loaders.get_enrollment_detail(
            npi=npi,
            enrollment_id=enrollment_id,
            associate_id=associate_id,
        )
        enrollment_ids = sorted({row.get("enrollment_id", "") for row in enrollment_rows if row.get("enrollment_id")})
        ownership_rows = data_loaders.query_ownership(enrollment_ids=enrollment_ids, limit=100) if enrollment_ids else []
        chow_rows = data_loaders.query_chow(enrollment_ids=enrollment_ids, limit=100) if enrollment_ids else []
        response = ProviderEnrollmentDetailResponse(
            query={"npi": npi, "enrollment_id": enrollment_id, "associate_id": associate_id},
            enrollments=[_enrollment(row) for row in enrollment_rows],
            ownership=[_ownership(row) for row in ownership_rows],
            chow_history=[_chow(row) for row in chow_rows],
            metadata=_metadata((*data_loaders.ENROLLMENT_DATASET_KEYS, *data_loaders.OWNER_DATASET_KEYS, *data_loaders.CHOW_DATASET_KEYS)),
        )
        return to_structured(
            _with_provider_evidence(
                response.model_dump(),
                query=response.query,
                rows=[*enrollment_rows, *ownership_rows, *chow_rows],
                match_basis="exact_public_identifier",
                confidence="high_when_identifier_matches_source_row",
            )
        )
    except Exception as exc:
        logger.exception("get_provider_enrollment_detail failed")
        return error_response(f"get_provider_enrollment_detail failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("provider-enrollment")
async def get_facility_ownership(
    ccn: str = "",
    facility_name: str = "",
    state: str = "",
    provider_category: str = "",
    include_indirect: bool = True,
    limit: int = 50,
) -> dict[str, Any]:
    """Return active CMS owner/managing-control rows for a facility.

    Use ccn for exact facility lookup. facility_name/state can be used when CCN
    is unknown, but names are conservative substring filters, not identity proof.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_facility_ownership","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    try:
        if not any([ccn, facility_name]):
            return error_response("ccn or facility_name is required.", code="invalid_params")
        if ccn and not normalize_ccn(ccn):
            return error_response("ccn must normalize to a six-character CMS Certification Number.", code="invalid_params")

        bounded_limit = _bounded_limit(limit, 200)
        rows = data_loaders.query_ownership(
            ccn=ccn,
            facility_name=facility_name,
            state=state,
            provider_category=provider_category,
            include_indirect=include_indirect,
            limit=bounded_limit,
        )
        response = FacilityOwnershipResponse(
            query={"ccn": ccn, "facility_name": facility_name, "state": state, "provider_category": provider_category},
            total_results=len(rows),
            limit=bounded_limit,
            owners=[_ownership(row) for row in rows],
            metadata=_metadata(data_loaders.OWNER_DATASET_KEYS),
        )
        return to_structured(
            _with_provider_evidence(
                response.model_dump(),
                query=response.query,
                rows=rows,
                match_basis="ccn_exact" if ccn else "facility_name_state_filter",
                confidence="high_identifier_match" if ccn else "candidate_facility_matches",
            )
        )
    except Exception as exc:
        logger.exception("get_facility_ownership failed")
        return error_response(f"get_facility_ownership failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("provider-enrollment")
async def trace_owner_network(
    owner_name: str = "",
    owner_associate_id: str = "",
    state: str = "",
    provider_category: str = "",
    depth: int = 1,
    limit: int = 100,
) -> dict[str, Any]:
    """Trace a bounded active ownership network around an owner.

    Depth defaults to 1 and is capped at 3. Output is capped to keep agent
    context bounded and includes only active all-owner relationships; CHOW
    records remain separate history available through search_change_of_ownership.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"trace_owner_network","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    try:
        if not any([owner_name, owner_associate_id]):
            return error_response("owner_name or owner_associate_id is required.", code="invalid_params")
        bounded_depth = _bounded_limit(depth, 3, default=1)
        bounded_limit = _bounded_limit(limit, 250)
        rows = data_loaders.query_ownership(
            owner_name=owner_name,
            owner_associate_id=owner_associate_id,
            state=state,
            provider_category=provider_category,
            limit=bounded_limit,
        )
        graph = ownership_graph.trace_owner_network(
            rows,
            owner_name=owner_name,
            owner_associate_id=owner_associate_id,
            state=normalize_state(state) or state,
            provider_category=data_loaders.snake_case(provider_category) if provider_category else "",
            depth=bounded_depth,
            limit=bounded_limit,
        )
        query = {
            "owner_name": owner_name,
            "owner_associate_id": owner_associate_id,
            "state": state,
            "provider_category": provider_category,
        }
        graph = _owner_graph_with_evidence(graph, rows, query=query)
        response = OwnerNetworkResponse(
            query=query,
            depth=bounded_depth,
            limit=bounded_limit,
            nodes=[GraphNode(**node) for node in graph["nodes"]],
            edges=[GraphEdge(**edge) for edge in graph["edges"]],
            shared_owners=graph["shared_owners"],
            metadata=_metadata(data_loaders.OWNER_DATASET_KEYS),
        )
        return to_structured(
            _with_provider_evidence(
                response.model_dump(),
                query=response.query,
                rows=rows,
                match_basis="owner_associate_id_seed" if owner_associate_id else "owner_name_seed",
                confidence="bounded_public_ownership_graph",
            )
        )
    except Exception as exc:
        logger.exception("trace_owner_network failed")
        return error_response(f"trace_owner_network failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("provider-enrollment")
async def search_change_of_ownership(
    ccn: str = "",
    facility_name: str = "",
    state: str = "",
    start_date: str = "",
    end_date: str = "",
    provider_category: str = "",
    limit: int = 50,
) -> dict[str, Any]:
    """Search CMS Change of Ownership history records for hospitals and SNFs.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_change_of_ownership","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    try:
        if not any([ccn, facility_name, state, provider_category]):
            return error_response(
                "At least one of ccn, facility_name, state, or provider_category is required.",
                code="invalid_params",
            )
        if ccn and not normalize_ccn(ccn):
            return error_response("ccn must normalize to a six-character CMS Certification Number.", code="invalid_params")

        bounded_limit = _bounded_limit(limit, 200)
        rows = data_loaders.query_chow(
            ccn=ccn,
            facility_name=facility_name,
            state=state,
            provider_category=provider_category,
            start_date=start_date,
            end_date=end_date,
            limit=bounded_limit,
        )
        response = ChangeOfOwnershipSearchResponse(
            query={
                "ccn": ccn,
                "facility_name": facility_name,
                "state": state,
                "start_date": start_date,
                "end_date": end_date,
                "provider_category": provider_category,
            },
            total_results=len(rows),
            limit=bounded_limit,
            events=[_chow(row) for row in rows],
            metadata=_metadata(data_loaders.CHOW_DATASET_KEYS),
        )
        return to_structured(
            _with_provider_evidence(
                response.model_dump(),
                query=response.query,
                rows=rows,
                match_basis="ccn_exact" if ccn else "filtered_chow_search",
                confidence="high_identifier_match" if ccn else "candidate_chow_matches",
            )
        )
    except Exception as exc:
        logger.exception("search_change_of_ownership failed")
        return error_response(f"search_change_of_ownership failed: {exc}")


@mcp.tool(structured_output=True)
@observe_tool("provider-enrollment")
async def profile_provider_control(ccn: str = "", npi: str = "") -> dict[str, Any]:
    """Build a compact control profile combining enrollment, owners, and CHOW.

    Use ccn for facilities or npi for Medicare FFS enrollment. The profile
    exposes join keys for other healthcare-data-mcp tools.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"profile_provider_control","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """

    try:
        if not any([ccn, npi]):
            return error_response("ccn or npi is required.", code="invalid_params")
        if ccn and not normalize_ccn(ccn):
            return error_response("ccn must normalize to a six-character CMS Certification Number.", code="invalid_params")
        if npi and not normalize_npi(npi):
            return error_response("npi must be a valid 10-digit NPI.", code="invalid_params")

        enrollment_rows = data_loaders.get_enrollment_detail(npi=npi) if npi else []
        owner_rows = data_loaders.query_ownership(ccn=ccn, limit=100) if ccn else []
        if enrollment_rows and not owner_rows:
            enrollment_ids = sorted({row.get("enrollment_id", "") for row in enrollment_rows if row.get("enrollment_id")})
            owner_rows = data_loaders.query_ownership(enrollment_ids=enrollment_ids, limit=100)
        chow_rows = data_loaders.query_chow(ccn=ccn, limit=100) if ccn else []
        if enrollment_rows and not chow_rows:
            enrollment_ids = sorted({row.get("enrollment_id", "") for row in enrollment_rows if row.get("enrollment_id")})
            chow_rows = data_loaders.query_chow(enrollment_ids=enrollment_ids, limit=100)

        network_seed = owner_rows[0].get("owner_associate_id", "") if owner_rows else ""
        graph_payload = ownership_graph.trace_owner_network(
            owner_rows,
            owner_associate_id=network_seed,
            depth=1,
            limit=100,
        ) if network_seed else {"nodes": [], "edges": [], "shared_owners": []}
        graph_payload = _owner_graph_with_evidence(
            graph_payload,
            owner_rows,
            query={"ccn": ccn, "npi": npi, "owner_associate_id": network_seed},
        )
        network = OwnerNetworkResponse(
            query={"owner_associate_id": network_seed},
            nodes=[GraphNode(**node) for node in graph_payload["nodes"]],
            edges=[GraphEdge(**edge) for edge in graph_payload["edges"]],
            shared_owners=graph_payload["shared_owners"],
            metadata=_metadata(data_loaders.OWNER_DATASET_KEYS),
        )
        response = ProviderControlProfileResponse(
            query={"ccn": ccn, "npi": npi},
            enrollment=[_enrollment(row) for row in enrollment_rows],
            ownership=[_ownership(row) for row in owner_rows],
            chow_history=[_chow(row) for row in chow_rows],
            owner_network=network,
            join_keys=_join_keys(enrollment_rows, owner_rows, chow_rows),
            metadata=_metadata((*data_loaders.ENROLLMENT_DATASET_KEYS, *data_loaders.OWNER_DATASET_KEYS, *data_loaders.CHOW_DATASET_KEYS)),
        )
        return to_structured(
            _with_provider_evidence(
                response.model_dump(),
                query=response.query,
                rows=[*enrollment_rows, *owner_rows, *chow_rows],
                match_basis="ccn_exact" if ccn else "npi_exact",
                confidence="high_when_identifier_matches_source_row",
            )
        )
    except Exception as exc:
        logger.exception("profile_provider_control failed")
        return error_response(f"profile_provider_control failed: {exc}")


def _enrollment(row: dict[str, Any]) -> EnrollmentRecord:
    return EnrollmentRecord(
        dataset_key=str(row.get("source_dataset_key") or row.get("dataset_key") or ""),
        provider_category=str(row.get("provider_category") or ""),
        npi=str(row.get("npi") or ""),
        pac_id=str(row.get("pac_id") or ""),
        enrollment_id=str(row.get("enrollment_id") or ""),
        associate_id=str(row.get("associate_id") or ""),
        ccn=str(row.get("ccn") or ""),
        state=str(row.get("state") or ""),
        provider_type=str(row.get("provider_type") or ""),
        provider_name=str(row.get("provider_name") or ""),
        facility_name=str(row.get("facility_name") or ""),
        **data_loaders.source_evidence_for_row(row),
        evidence=_provider_row_evidence(row, match_basis="cms_provider_enrollment_row"),
        raw=data_loaders.row_to_raw(row),
    )


def _ownership(row: dict[str, Any]) -> OwnershipRecord:
    return OwnershipRecord(
        dataset_key=str(row.get("source_dataset_key") or row.get("dataset_key") or ""),
        provider_category=str(row.get("provider_category") or ""),
        enrollment_id=str(row.get("enrollment_id") or ""),
        ccn=str(row.get("ccn") or ""),
        facility_name=str(row.get("facility_name") or ""),
        state=str(row.get("state") or ""),
        owner_name=str(row.get("owner_name") or ""),
        owner_associate_id=str(row.get("owner_associate_id") or ""),
        owner_pac_id=str(row.get("owner_pac_id") or ""),
        owner_type=str(row.get("owner_type") or ""),
        role_code=str(row.get("role_code") or ""),
        role_text=str(row.get("role_text") or ""),
        percentage_ownership=str(row.get("percentage_ownership") or ""),
        association_date=str(row.get("association_date") or ""),
        association_end_date=str(row.get("association_end_date") or ""),
        is_active=bool(row.get("is_active", True)),
        private_equity=str(row.get("private_equity") or ""),
        reit=str(row.get("reit") or ""),
        holding_company=str(row.get("holding_company") or ""),
        **data_loaders.source_evidence_for_row(row),
        evidence=_provider_row_evidence(row, match_basis="cms_provider_ownership_row"),
        raw=data_loaders.row_to_raw(row),
    )


def _chow(row: dict[str, Any]) -> ChangeOfOwnershipRecord:
    return ChangeOfOwnershipRecord(
        dataset_key=str(row.get("source_dataset_key") or row.get("dataset_key") or ""),
        provider_category=str(row.get("provider_category") or ""),
        enrollment_id=str(row.get("enrollment_id") or ""),
        ccn=str(row.get("ccn") or ""),
        facility_name=str(row.get("facility_name") or ""),
        state=str(row.get("state") or ""),
        owner_name=str(row.get("owner_name") or ""),
        owner_associate_id=str(row.get("owner_associate_id") or ""),
        transaction_date=str(row.get("transaction_date") or ""),
        effective_date=str(row.get("effective_date") or ""),
        change_type=str(row.get("change_type") or ""),
        **data_loaders.source_evidence_for_row(row),
        evidence=_provider_row_evidence(row, match_basis="cms_provider_chow_row"),
        raw=data_loaders.row_to_raw(row),
    )


def _metadata(dataset_keys: tuple[str, ...] | list[str]) -> list[SourceMetadata]:
    return [SourceMetadata(**payload) for payload in data_loaders.source_metadata_for_keys(dataset_keys)]


def _provider_row_evidence(row: dict[str, Any], *, match_basis: str) -> dict[str, Any]:
    metadata = data_loaders.source_evidence_for_row(row)
    dataset_key = str(metadata.get("dataset_id") or row.get("source_dataset_key") or row.get("dataset_key") or "")
    query = {
        "dataset_key": dataset_key,
        "npi": row.get("npi") or "",
        "ccn": row.get("ccn") or "",
        "enrollment_id": row.get("enrollment_id") or "",
        "owner_associate_id": row.get("owner_associate_id") or "",
        "transaction_date": row.get("transaction_date") or row.get("effective_date") or "",
    }
    return evidence_receipt(
        source_metadata=metadata,
        dataset_id=dataset_key or "cms_provider_enrollment",
        entity_scope="provider_enrollment_ownership_chow",
        query={key: value for key, value in query.items() if value},
        match_basis=match_basis,
        confidence="source_row",
        caveat=(
            "CMS PECOS public enrollment, ownership, and CHOW rows are source-backed public records; "
            "row names are not identity proof without exact identifiers."
        ),
        next_step="Preserve this row receipt with the exact NPI, CCN, enrollment ID, owner ID, or CHOW date before citing.",
    )


def _provider_source_metadata(evidence: dict[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a provider-enrollment receipt."""

    return {
        "source_name": evidence.get("source_name", ""),
        "source_url": evidence.get("source_url", ""),
        "dataset_id": evidence.get("dataset_id", ""),
        "source_period": evidence.get("source_period", ""),
        "landing_page": evidence.get("landing_page", ""),
        "retrieved_at": evidence.get("retrieved_at", ""),
        "source_modified": evidence.get("source_modified", ""),
        "cache_status": evidence.get("cache_status", ""),
        "cache_freshness": evidence.get("cache_freshness", ""),
        "entity_scope": evidence.get("entity_scope", "provider_enrollment_ownership_chow"),
        "query": evidence.get("query", {}),
        "cache_key": evidence.get("cache_key", ""),
        "source_type": "cms_pecos_provider_enrollment_public_file",
    }


def _owner_graph_with_evidence(
    graph_payload: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    query: dict[str, Any],
) -> dict[str, Any]:
    """Attach source-row receipts to ownership graph nodes and edges."""

    node_rows, edge_rows = _owner_graph_source_rows(rows)
    nodes = []
    for node in graph_payload.get("nodes", []):
        node_payload = dict(node)
        node_id = str(node_payload.get("id") or "")
        node_kind = str(node_payload.get("kind") or "")
        node_payload["evidence"] = _owner_graph_evidence(
            node_rows.get(node_id),
            match_basis=_owner_graph_node_match_basis(node_kind),
            query={
                **query,
                "graph_node_id": node_id,
                "graph_node_kind": node_kind,
                "graph_node_label": node_payload.get("label") or "",
            },
        )
        nodes.append(node_payload)

    edges = []
    for edge in graph_payload.get("edges", []):
        edge_payload = dict(edge)
        source = str(edge_payload.get("source") or "")
        target = str(edge_payload.get("target") or "")
        edge_payload["evidence"] = _owner_graph_evidence(
            edge_rows.get((source, target)),
            match_basis="cms_provider_owner_graph_edge_row",
            query={
                **query,
                "graph_edge_source": source,
                "graph_edge_target": target,
                "graph_edge_relationship": edge_payload.get("relationship") or "",
            },
        )
        edges.append(edge_payload)

    return {
        **graph_payload,
        "nodes": nodes,
        "edges": edges,
    }


def _owner_graph_source_rows(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    node_rows: dict[str, dict[str, Any]] = {}
    edge_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        owner_id, facility_id = ownership_graph.source_graph_ids(row)
        if owner_id:
            node_rows.setdefault(owner_id, row)
        if facility_id:
            node_rows.setdefault(facility_id, row)
        if owner_id and facility_id:
            edge_rows.setdefault((owner_id, facility_id), row)
    return node_rows, edge_rows


def _owner_graph_node_match_basis(node_kind: str) -> str:
    if node_kind == "owner":
        return "cms_provider_owner_graph_owner_node_row"
    if node_kind == "facility":
        return "cms_provider_owner_graph_facility_node_row"
    return "cms_provider_owner_graph_node_row"


def _owner_graph_evidence(
    row: dict[str, Any] | None,
    *,
    match_basis: str,
    query: dict[str, Any],
) -> dict[str, Any]:
    compact_query = {key: value for key, value in query.items() if value}
    if row:
        receipt = _provider_row_evidence(row, match_basis=match_basis)
        receipt["query"] = {**receipt.get("query", {}), **compact_query}
        receipt["next_step"] = (
            "Use this graph element only with its source CMS ownership row; review exact CCN, "
            "enrollment ID, owner ID, and role fields before citing control."
        )
        return receipt

    metadata = data_loaders.source_metadata_for_keys(data_loaders.OWNER_DATASET_KEYS)
    first_metadata = metadata[0] if metadata else {}
    return evidence_receipt(
        source_metadata=first_metadata,
        dataset_id=str(first_metadata.get("dataset_key") or first_metadata.get("dataset_id") or "cms_provider_ownership"),
        entity_scope="provider_enrollment_ownership_chow",
        query=compact_query,
        match_basis=f"{match_basis}_source_row_missing",
        confidence="graph_element_without_matching_source_row",
        caveat=(
            "CMS ownership graph nodes and edges must be traceable to PECOS public ownership rows; "
            "a missing row receipt means this graph element should not be cited as a source-backed fact."
        ),
        next_step="Rebuild the graph from current cached ownership rows before using this element in a report.",
    )


def _with_provider_evidence(
    payload: dict[str, Any],
    *,
    query: dict[str, Any],
    rows: list[dict[str, Any]],
    match_basis: str,
    confidence: str,
) -> dict[str, Any]:
    metadata = payload.get("metadata") or []
    first_metadata = metadata[0] if metadata else {}
    effective_match_basis = match_basis
    effective_confidence = confidence
    next_step = "Use exact NPI, CCN, enrollment_id, or owner_associate_id where available and preserve source rows for review."
    if not rows:
        effective_match_basis = _no_match_basis(match_basis)
        effective_confidence = "no_matching_rows_in_loaded_cms_provider_enrollment_public_files"
        next_step = (
            "Verify the identifier, name filters, and cache/source freshness before reporting no enrollment, "
            "ownership, or CHOW rows. Do not infer absence of ownership or control from adjacent sources."
        )
    payload["evidence"] = evidence_receipt(
        source_metadata=first_metadata,
        dataset_id=str(first_metadata.get("dataset_key") or first_metadata.get("dataset_id") or "cms_provider_enrollment"),
        entity_scope="provider_enrollment_ownership_chow",
        query=query,
        match_basis=effective_match_basis,
        confidence=effective_confidence,
        caveat="CMS PECOS public enrollment and ownership files are source-backed public records; name filters are candidate matches, not identity proof.",
        next_step=next_step,
    )
    payload["source_metadata"] = _provider_source_metadata(payload["evidence"])
    payload["identity"] = _identity_from_rows(rows, query).to_dict()
    payload["identity_map"] = _provider_identity_map(payload=payload, rows=rows, query=query)
    return payload


def _no_match_basis(match_basis: str) -> str:
    if match_basis.endswith("_no_match"):
        return match_basis
    return f"{match_basis}_no_match"


def _identity_from_rows(rows: list[dict[str, Any]], query: dict[str, Any]):
    first = rows[0] if rows else {}
    return identity_from_public_record(
        name=(
            first.get("facility_name")
            or first.get("provider_name")
            or first.get("owner_name")
            or query.get("facility_name")
            or query.get("provider_name")
            or query.get("owner_name")
            or ""
        ),
        entity_type=str(first.get("provider_category") or ""),
        ccn=first.get("ccn") or query.get("ccn") or "",
        npi=first.get("npi") or query.get("npi") or "",
        pecos_enrollment_id=first.get("enrollment_id") or query.get("enrollment_id") or "",
        owner_id=first.get("owner_associate_id") or query.get("owner_associate_id") or "",
        source_name=str(first.get("source_name") or "CMS Provider Enrollment"),
        source_url=str(first.get("source_url") or ""),
    )


def _provider_identity_map(*, payload: dict[str, Any], rows: list[dict[str, Any]], query: dict[str, Any]) -> dict[str, Any]:
    """Return the provider-enrollment identity spine for cross-server workflows."""

    row_keys = _join_keys(rows)
    field_values = {
        "npi": _identity_values("npi", row_keys.get("npi", []), query.get("npi")),
        "ccn": _identity_values("ccn", row_keys.get("ccn", []), query.get("ccn")),
        "pecos_enrollment_id": _identity_values(
            "pecos_enrollment_id",
            row_keys.get("enrollment_id", []),
            query.get("enrollment_id"),
        ),
        "owner_id": _identity_values(
            "owner_id",
            row_keys.get("associate_id", []),
            query.get("owner_associate_id") or query.get("associate_id"),
        ),
        "pac_id": _identity_values("pac_id", row_keys.get("pac_id", []), query.get("pac_id")),
    }
    source_claims = _provider_source_claims(payload)
    return {
        "entity_scope": "provider_enrollment_ownership_chow",
        "join_keys": [
            {
                "field": field,
                "values": values,
                "status": "provided" if values else "missing",
                "used_by": _provider_join_key_usage(field, source_claims),
            }
            for field, values in field_values.items()
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Use NPI, CCN, PECOS enrollment IDs, and owner associate IDs as exact public-record join keys.",
            "Treat provider, facility, and owner names as aliases or candidate filters unless an exact identifier also matches.",
            "Keep enrollment rows, ownership rows, and CHOW events source-scoped; do not infer current control from CHOW history alone.",
            "Carry identifier conflicts forward instead of overwriting canonical identity fields.",
        ],
        "missing_data_policy": (
            "No-match provider-enrollment responses identify the searched CMS public-source scope; "
            "they are not proof of no ownership, control, enrollment, CHOW history, or exclusion status."
        ),
    }


def _identity_values(field: str, row_values: list[str], *query_values: Any) -> list[str]:
    values: set[str] = set()
    for value in (*row_values, *query_values):
        normalized = _normalize_identity_value(field, value)
        if normalized:
            values.add(normalized)
    return sorted(values)


def _normalize_identity_value(field: str, value: Any) -> str:
    if value in ("", None):
        return ""
    if field == "npi":
        return normalize_npi(value) or ""
    if field == "ccn":
        return normalize_ccn(value) or ""
    if field in {"pecos_enrollment_id", "owner_id"}:
        return normalize_enrollment_id(value) or ""
    return str(value).strip()


def _provider_source_claims(payload: dict[str, Any]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    if "enrollments" in payload:
        claim = _provider_source_claim(
            collection="enrollments",
            match_policy="exact_identifier_required_for_report_fact",
        )
        if payload.get("enrollments"):
            claim["row_evidence_paths"] = ["enrollments[].evidence"]
        claims.append(claim)
    if "enrollment" in payload:
        claim = _provider_source_claim(
            collection="enrollment",
            match_policy="exact_identifier_required_for_report_fact",
        )
        if payload.get("enrollment"):
            claim["row_evidence_paths"] = ["enrollment[].evidence"]
        claims.append(claim)
    if "ownership" in payload or "owners" in payload:
        collection = "owners" if "owners" in payload else "ownership"
        claim = _provider_source_claim(
            collection=collection,
            match_policy="owner_identifier_required_for_owner_merge",
        )
        if payload.get(collection):
            claim["row_evidence_paths"] = [f"{collection}[].evidence"]
        claims.append(claim)
    if "chow_history" in payload or "events" in payload:
        collection = "events" if "events" in payload else "chow_history"
        claim = _provider_source_claim(
            collection=collection,
            match_policy="exact_identifier_plus_event_date_for_chow_fact",
        )
        if payload.get(collection):
            claim["row_evidence_paths"] = [f"{collection}[].evidence"]
        claims.append(claim)
    if "nodes" in payload or "owner_network" in payload:
        prefix = "owner_network." if "owner_network" in payload else ""
        claim = _provider_source_claim(
            collection="owner_network",
            match_policy="bounded_graph_context_requires_source_row_review",
        )
        rows = payload.get("owner_network") if prefix else payload
        if rows.get("nodes") or rows.get("edges"):
            claim["row_evidence_paths"] = [f"{prefix}nodes[].evidence", f"{prefix}edges[].evidence"]
        claims.append(claim)
    return claims


def _provider_source_claim(*, collection: str, match_policy: str) -> dict[str, Any]:
    return {
        "collection": collection,
        "identity_paths": ["identity", "evidence.query"],
        "evidence_path": "evidence",
        "source_metadata_path": "source_metadata",
        "match_policy": match_policy,
    }


def _provider_join_key_usage(field: str, source_claims: list[dict[str, Any]]) -> list[str]:
    collections_by_field = {
        "npi": {"enrollments", "enrollment"},
        "ccn": {"enrollments", "enrollment", "owners", "ownership", "events", "chow_history"},
        "pecos_enrollment_id": {"enrollments", "enrollment", "owners", "ownership", "events", "chow_history"},
        "owner_id": {"owners", "ownership", "owner_network"},
    }.get(field, set())
    path_tokens = {
        "npi": ("npi",),
        "ccn": ("ccn",),
        "pecos_enrollment_id": ("enrollment_id",),
        "owner_id": ("owner_associate_id", "owner_network"),
        "pac_id": ("pac_id",),
    }.get(field, (field,))
    used_by = []
    for claim in source_claims:
        collection = str(claim.get("collection") or "")
        if collection in collections_by_field:
            used_by.append(collection)
            continue
        paths = " ".join(str(path) for path in claim.get("identity_paths", []))
        if any(token in paths for token in path_tokens):
            used_by.append(collection)
    return sorted(item for item in used_by if item)


def _join_keys(*row_groups: list[dict[str, Any]]) -> dict[str, list[str]]:
    keys = {"npi": set(), "ccn": set(), "pac_id": set(), "enrollment_id": set(), "associate_id": set()}
    for rows in row_groups:
        for row in rows:
            for key in keys:
                value = str(row.get(key) or row.get(f"owner_{key}") or "")
                if key == "npi":
                    value = normalize_npi(value) or ""
                elif key == "ccn":
                    value = normalize_ccn(value) or ""
                elif key in {"enrollment_id", "associate_id"}:
                    value = normalize_enrollment_id(value) or ""
                if value:
                    keys[key].add(value)
    return {key: sorted(values) for key, values in keys.items()}


def _bounded_limit(limit: int, maximum: int, *, default: int = 25) -> int:
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        parsed = default
    return max(1, min(parsed, maximum))


if __name__ == "__main__":
    mcp.run(transport=_transport)
