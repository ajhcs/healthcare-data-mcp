"""CMS Provider Enrollment & Ownership MCP Server.

Provides PECOS-derived provider enrollment, facility ownership, and change of
ownership lookups from cached CMS public datasets. Port 8017.
"""

from __future__ import annotations

import logging
import os as _os
from typing import Any

from mcp.server.fastmcp import FastMCP

from shared.utils.identity import normalize_ccn, normalize_enrollment_id, normalize_npi, normalize_state
from shared.utils.mcp_response import error_response, to_structured

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


@mcp.tool(structured_output=True)
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
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("search_provider_enrollment failed")
        return error_response(f"search_provider_enrollment failed: {exc}")


@mcp.tool(structured_output=True)
async def get_provider_enrollment_detail(
    npi: str = "",
    enrollment_id: str = "",
    associate_id: str = "",
) -> dict[str, Any]:
    """Fetch enrollment detail plus linked owners and CHOW history.

    Provide NPI, enrollment_id, or associate_id. The tool uses exact normalized
    identifiers and does not attempt fuzzy identity merges.
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
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("get_provider_enrollment_detail failed")
        return error_response(f"get_provider_enrollment_detail failed: {exc}")


@mcp.tool(structured_output=True)
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
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("get_facility_ownership failed")
        return error_response(f"get_facility_ownership failed: {exc}")


@mcp.tool(structured_output=True)
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
        response = OwnerNetworkResponse(
            query={
                "owner_name": owner_name,
                "owner_associate_id": owner_associate_id,
                "state": state,
                "provider_category": provider_category,
            },
            depth=bounded_depth,
            limit=bounded_limit,
            nodes=[GraphNode(**node) for node in graph["nodes"]],
            edges=[GraphEdge(**edge) for edge in graph["edges"]],
            shared_owners=graph["shared_owners"],
            metadata=_metadata(data_loaders.OWNER_DATASET_KEYS),
        )
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("trace_owner_network failed")
        return error_response(f"trace_owner_network failed: {exc}")


@mcp.tool(structured_output=True)
async def search_change_of_ownership(
    ccn: str = "",
    facility_name: str = "",
    state: str = "",
    start_date: str = "",
    end_date: str = "",
    provider_category: str = "",
    limit: int = 50,
) -> dict[str, Any]:
    """Search CMS Change of Ownership history records for hospitals and SNFs."""

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
        return to_structured(response.model_dump())
    except Exception as exc:
        logger.exception("search_change_of_ownership failed")
        return error_response(f"search_change_of_ownership failed: {exc}")


@mcp.tool(structured_output=True)
async def profile_provider_control(ccn: str = "", npi: str = "") -> dict[str, Any]:
    """Build a compact control profile combining enrollment, owners, and CHOW.

    Use ccn for facilities or npi for Medicare FFS enrollment. The profile
    exposes join keys for other healthcare-data-mcp tools.
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
        return to_structured(response.model_dump())
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
        raw=data_loaders.row_to_raw(row),
    )


def _metadata(dataset_keys: tuple[str, ...] | list[str]) -> list[SourceMetadata]:
    return [SourceMetadata(**payload) for payload in data_loaders.source_metadata_for_keys(dataset_keys)]


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
