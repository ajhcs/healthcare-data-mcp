"""Standard metadata resources for healthcare-data-mcp servers."""

from __future__ import annotations

import json
from typing import Any

from shared.utils.mcp_observability import tooling_metrics_payload
from shared.utils.server_registry import SERVER_BY_ID
from shared.utils.tool_clusters import clusters_for_server


SOURCE_BACKED_CONTRACT_DOC = "docs/SOURCE_BACKED_RESULT_CONTRACT.md"
SOURCE_CAPABILITY_LEDGER_DOC = "docs/SOURCE_CAPABILITY_LEDGER.md"


def server_capabilities_payload(server_id: str) -> dict[str, Any]:
    spec = SERVER_BY_ID.get(server_id)
    if spec is None:
        return {"server_id": server_id, "status": "server_not_registered"}
    return {
        "server_id": spec.server_id,
        "module": spec.module,
        "port": spec.port,
        "description": spec.description,
        "profiles": list(spec.profiles),
        "workflow_roles": list(spec.workflow_roles),
        "dataset_ids": list(spec.dataset_ids),
        "cache_needs": list(spec.cache_needs),
        "gateway_exposure": list(spec.gateway_exposure),
        "safety_notes": list(spec.safety_notes),
        "tool_clusters": {
            cluster: list(tools)
            for cluster, tools in clusters_for_server(server_id).items()
        },
        "source_backed_contract": {
            "contract_uri": SOURCE_BACKED_CONTRACT_DOC,
            "ledger_doc": SOURCE_CAPABILITY_LEDGER_DOC,
            "ledger_resource": f"healthcare-data://server/{server_id}/source-ledger",
            "boundary_rule": (
                "Report-ready and live-gateway-bound facts require evidence, source_metadata, "
                "identity_map, and source_claim paths that validate at the provenance boundary."
            ),
        },
        "next_actions": [
            "Read this server's source-ledger resource before citing public facts.",
            "Use the datasets resource before citing public facts.",
            "Use capability clusters to choose the narrowest relevant tool group.",
            "Preserve evidence and identity_map fields from tool results.",
        ],
    }


def server_source_ledger_payload(server_id: str) -> dict[str, Any]:
    spec = SERVER_BY_ID.get(server_id)
    if spec is None:
        return {"server_id": server_id, "status": "server_not_registered"}
    return {
        "server_id": spec.server_id,
        "ledger_doc": SOURCE_CAPABILITY_LEDGER_DOC,
        "contract_doc": SOURCE_BACKED_CONTRACT_DOC,
        "dataset_ids": list(spec.dataset_ids),
        "cache_needs": list(spec.cache_needs),
        "gateway_exposure": list(spec.gateway_exposure),
        "safety_notes": list(spec.safety_notes),
        "operator_rules": [
            "Prefer exact public identifiers before joining source facts.",
            "Treat names, addresses, ZIPs, domains, and search snippets as candidate context.",
            "Preserve each tool's evidence, source_metadata, identity, and identity_map fields.",
            "Return source-status or import-required responses when data is missing.",
            "Do not convert adjacent public records into unsupported assertions.",
        ],
        "resource_links": {
            "capabilities": f"healthcare-data://server/{server_id}/capabilities",
            "datasets": f"healthcare-data://server/{server_id}/datasets",
            "identity_contract": f"healthcare-data://server/{server_id}/identity-contract",
        },
    }


def register_standard_resources(mcp: Any, server_id: str) -> None:
    """Register side-effect-free discovery resources on one FastMCP server."""

    def _json(payload: Any) -> str:
        return json.dumps(payload, indent=2, sort_keys=True, default=str)

    @mcp.resource(
        f"healthcare-data://server/{server_id}/capabilities",
        name=f"{server_id}_capabilities",
        description="Server capability metadata, safety notes, datasets, and tool clusters.",
        mime_type="application/json",
    )
    def capabilities() -> str:
        return _json(server_capabilities_payload(server_id))

    @mcp.resource(
        f"healthcare-data://server/{server_id}/datasets",
        name=f"{server_id}_datasets",
        description="Dataset IDs and cache/source prerequisites for this server.",
        mime_type="application/json",
    )
    def datasets() -> str:
        spec = SERVER_BY_ID.get(server_id)
        return _json(
            {
                "server_id": server_id,
                "dataset_ids": list(spec.dataset_ids) if spec else [],
                "cache_needs": list(spec.cache_needs) if spec else [],
                "required_env": [
                    {"name": key.name, "description": key.description}
                    for key in (spec.required_env if spec else ())
                ],
                "optional_env": [
                    {"name": key.name, "description": key.description}
                    for key in (spec.optional_env if spec else ())
                ],
            }
        )

    @mcp.resource(
        f"healthcare-data://server/{server_id}/examples",
        name=f"{server_id}_examples",
        description="Agent-facing usage examples and common mistakes for this server.",
        mime_type="application/json",
    )
    def examples() -> str:
        return _json(
            {
                "server_id": server_id,
                "examples": [
                    {
                        "purpose": "Inspect capabilities before selecting a tool.",
                        "resource": f"healthcare-data://server/{server_id}/capabilities",
                    },
                    {
                        "purpose": "Check datasets and source prerequisites.",
                        "resource": f"healthcare-data://server/{server_id}/datasets",
                    },
                ],
                "common_mistakes": [
                    "Do not cite candidate search rows as exact source facts without exact identifiers.",
                    "Do not pass workflow placeholders such as <ccn> or YOUR_PROJECT as real arguments.",
                    "Do not merge names or addresses across sources without an exact identifier.",
                ],
            }
        )

    @mcp.resource(
        f"healthcare-data://server/{server_id}/identity-contract",
        name=f"{server_id}_identity_contract",
        description="Default cross-source identity and evidence preservation rules.",
        mime_type="application/json",
    )
    def identity_contract() -> str:
        return _json(
            {
                "server_id": server_id,
                "exact_identifier_policy": "Exact identifiers such as CCN, NPI, PECOS enrollment ID, AHRQ system ID, and owner ID may anchor joins when non-conflicting.",
                "candidate_policy": "Names, addresses, ZIP codes, domains, and search snippets are candidate context until supported by exact identifiers.",
                "preserve_fields": ["identity", "identity_map", "evidence", "source_metadata"],
            }
        )

    @mcp.resource(
        f"healthcare-data://server/{server_id}/source-ledger",
        name=f"{server_id}_source_ledger",
        description="Source capability ledger link, source-backed contract pointer, and operator rules.",
        mime_type="application/json",
    )
    def source_ledger() -> str:
        return _json(server_source_ledger_payload(server_id))

    @mcp.resource(
        f"healthcare-data://server/{server_id}/tooling/metrics",
        name=f"{server_id}_tooling_metrics",
        description="Recent non-secret tool timing and outcome metrics for this process.",
        mime_type="application/json",
    )
    def tooling_metrics() -> str:
        return _json(tooling_metrics_payload(server_id))
