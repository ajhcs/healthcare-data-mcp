"""Local-safe MCP cache-management control plane."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

from shared.cache_manager import core
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.mcp_response import response_envelope

_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict[str, Any] = {"name": "cache-manager"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8021"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "cache-manager")


def _cache_root() -> Path:
    return Path(os.environ.get("HC_MCP_CACHE_ROOT") or core.DEFAULT_CACHE_ROOT).expanduser()


def _mutations_allowed() -> bool:
    if _transport == "stdio":
        return True
    host = str(_mcp_kwargs.get("host") or "127.0.0.1")
    if host in {"127.0.0.1", "localhost"}:
        return True
    return os.environ.get("HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS", "").casefold() in {"1", "true", "yes"}


def _require_mutations_allowed() -> None:
    if not _mutations_allowed():
        raise ValueError(
            "Mutating cache-manager tools require stdio, loopback HTTP, or explicit "
            "HC_MCP_CACHE_MANAGER_ALLOW_REMOTE_MUTATIONS=true."
        )


@mcp.tool()
@observe_tool("cache-manager")
async def list_cache_sources(
    server: str = "",
    workflow: str = "",
    status: str = "",
    source_system: str = "",
    acquisition_mode: str = "",
) -> dict[str, Any]:
    """List registered public healthcare cache sources and readiness.

    Discovery: Enumerates registry-backed cache contracts and current manifest-backed readiness.
    When to use: Use before a workflow to see which public sources are ready, blocked, stale, or import-only.
    Parameters: Optional server, workflow, status, source_system, and acquisition_mode filters.
    Returns: Bounded source summaries with validation status, source period, report eligibility, and next actions.
    Do / Don't: Do filter by exact dataset/workflow IDs; don't treat missing cache data as a negative factual claim.
    Examples: {"workflow":"hospital_competitive_profile"} lists the flagship workflow's cache inputs.
    Common mistakes: Assuming file existence equals validated readiness; using state-limited sources as national coverage.
    """

    return response_envelope(
        data=core.list_cache_sources(
            cache_root=_cache_root(),
            server=server or None,
            workflow=workflow or None,
            status=status or None,
            source_system=source_system or None,
            acquisition_mode=acquisition_mode or None,
        )
    )


@mcp.tool()
@observe_tool("cache-manager")
async def inspect_cache_source(dataset_id: str) -> dict[str, Any]:
    """Inspect one registered dataset's cache contract, status, manifest, and lineage.

    Discovery: Shows the executable cache contract for a dataset_id.
    When to use: Use before citing or refreshing a specific source.
    Parameters: dataset_id must be a registered dataset such as cms_hospital_quality.
    Returns: Spec fields, current readiness, latest manifest, and lineage summary.
    Do / Don't: Do use exact dataset IDs; don't pass arbitrary URLs or paths.
    Examples: {"dataset_id":"cms_hospital_quality"} inspects exact CMS quality measure readiness.
    Common mistakes: Joining facts on names or addresses instead of exact identifiers from the spec.
    """

    return response_envelope(data=core.inspect_cache_source(dataset_id, cache_root=_cache_root()))


@mcp.tool()
@observe_tool("cache-manager")
async def get_workflow_cache_readiness(workflow_id: str, inputs: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return dataset readiness, blockers, and next actions for one workflow.

    Discovery: Resolves workflow source aliases into canonical dataset IDs.
    When to use: Use as the cache preflight for report workflows.
    Parameters: workflow_id plus optional concrete inputs such as ccn and measure.
    Returns: Workflow readiness, step checks, ordered cache plan, blockers, and next actions.
    Do / Don't: Do preserve blockers in the final plan; don't substitute adjacent sources for exact measure rows.
    Examples: {"workflow_id":"hospital_competitive_profile","inputs":{"ccn":"390223","measure":"clabsi_sir"}}.
    Common mistakes: Treating optional unavailable context as evidence against a hospital.
    """

    from shared.utils.workflows import build_workflow_plan

    cache_status = core.cache_status_payload(_cache_root())
    plan = build_workflow_plan(workflow_id, inputs=inputs or {}, cache_status=cache_status)
    refresh_plan = core.plan_cache_refresh(workflow_id=workflow_id, cache_root=_cache_root())
    return response_envelope(
        data={
            "workflow_id": workflow_id,
            "readiness": plan.get("readiness", {}),
            "cache_readiness": plan.get("cache_readiness", {}),
            "steps": plan.get("steps", []),
            "ordered_cache_plan": refresh_plan.get("ordered_plan", []),
            "blockers": refresh_plan.get("blockers", []),
            "next_actions": refresh_plan.get("next_actions", []),
        }
    )


@mcp.tool()
@observe_tool("cache-manager")
async def plan_cache_refresh(
    dataset_ids: list[str] | None = None,
    workflow_id: str = "",
    force: bool = False,
    max_bytes: int = core.DEFAULT_MAX_BYTES,
) -> dict[str, Any]:
    """Dry-run an ordered cache refresh plan without downloading or writing.

    Discovery: Produces the acquisition sequence from registered cache specs.
    When to use: Use before any mutating refresh or import.
    Parameters: dataset_ids, workflow_id, force, and max_bytes.
    Returns: Ordered plan rows with validation gates, missing env, blockers, and source caveats.
    Do / Don't: Do review max_bytes and acquisition_mode; don't call refresh without an explicit dataset list or workflow.
    Examples: {"workflow_id":"hospital_competitive_profile"} plans the flagship bundle.
    Common mistakes: Refreshing licensed/manual imports as if they were public downloads.
    """

    return response_envelope(
        data=core.plan_cache_refresh(
            dataset_ids=dataset_ids or None,
            workflow_id=workflow_id or None,
            cache_root=_cache_root(),
            force=force,
            max_bytes=max_bytes,
        )
    )


@mcp.tool()
@observe_tool("cache-manager")
async def get_cache_manifest(dataset_id: str) -> dict[str, Any]:
    """Return the latest promoted artifact manifest for one dataset without raw data.

    Discovery: Reads the bounded promoted manifest for a dataset.
    When to use: Use when report evidence needs checksum, source period, or validation status.
    Parameters: dataset_id must be registered.
    Returns: Manifest metadata or a missing-manifest status.
    Do / Don't: Do cite manifest metadata; don't request raw source rows from this tool.
    Examples: {"dataset_id":"cms_hospital_general_info"} returns the current promoted artifact receipt.
    Common mistakes: Treating a missing manifest as proof the public source has no data.
    """

    return response_envelope(data=core.get_cache_manifest(dataset_id, cache_root=_cache_root()))


@mcp.tool()
@observe_tool("cache-manager")
async def get_cache_lineage(dataset_id: str) -> dict[str, Any]:
    """Return bounded lineage for one dataset's cache runs and promoted artifacts.

    Discovery: Shows recent run records and the current promoted artifact.
    When to use: Use to explain how a cache moved from acquisition to report-ready use.
    Parameters: dataset_id must be registered.
    Returns: Current artifact ID, manifest, recent runs, and downstream workflow references.
    Do / Don't: Do use lineage for recovery; don't expose raw payloads in audit reports.
    Examples: {"dataset_id":"cms_hospital_quality"} traces quality-cache promotion.
    Common mistakes: Ignoring previous-good rollback evidence after a corrupt refresh.
    """

    return response_envelope(data=core.get_cache_lineage(dataset_id, cache_root=_cache_root()))


@mcp.tool()
@observe_tool("cache-manager")
async def start_cache_refresh(
    dataset_ids: list[str],
    dry_run: bool = True,
    force: bool = False,
    max_bytes: int = core.DEFAULT_MAX_BYTES,
    allow_stale_fallback: bool = False,
) -> dict[str, Any]:
    """Start a bounded refresh for registered dataset IDs only.

    Discovery: Starts or dry-runs guarded acquisition jobs for allowlisted datasets.
    When to use: Use after plan_cache_refresh identifies a needed public refresh.
    Parameters: dataset_ids, dry_run, force, max_bytes, and allow_stale_fallback.
    Returns: Run IDs, job records, validation summaries, manifests, or recovery hints.
    Do / Don't: Do keep dry_run true until approved; don't pass arbitrary URLs, broad paths, or large unbounded downloads.
    Examples: {"dataset_ids":["cms_hospital_quality"],"dry_run":true} creates a no-write plan record.
    Common mistakes: Expecting licensed imports or state manual files to download automatically.
    """

    _require_mutations_allowed()
    return response_envelope(
        data=core.start_cache_refresh(
            dataset_ids,
            cache_root=_cache_root(),
            dry_run=dry_run,
            force=force,
            max_bytes=max_bytes,
            allow_stale_fallback=allow_stale_fallback,
            request_source=f"MCP {_transport}",
        )
    )


@mcp.tool()
@observe_tool("cache-manager")
async def get_cache_job(run_id: str) -> dict[str, Any]:
    """Poll a cache refresh/import/validation job.

    Discovery: Reads one bounded cache run record.
    When to use: Use after start_cache_refresh returns a run_id.
    Parameters: run_id from a previous cache job response.
    Returns: Status, phase, error, manifests, and recovery hint when available.
    Do / Don't: Do poll by exact run_id; don't infer success without validation_status.
    Examples: {"run_id":"cms_hospital_quality-20260530T000000Z-abcd1234"}.
    Common mistakes: Treating a planned dry-run job as a promoted cache artifact.
    """

    return response_envelope(data=core.get_cache_job(run_id, cache_root=_cache_root()))


@mcp.tool()
@observe_tool("cache-manager")
async def validate_cache_source(dataset_id: str, staged_path: str = "", relative_path: str = "") -> dict[str, Any]:
    """Validate current or staged artifacts for one registered dataset without promotion.

    Discovery: Runs dataset-specific validation against current or staged cache artifacts.
    When to use: Use before promotion or after manual import.
    Parameters: dataset_id, optional staged_path confined to the cache root, and optional relative_path.
    Returns: Validation status, defects, metrics, and report eligibility.
    Do / Don't: Do validate before citing; don't validate paths outside the configured cache root.
    Examples: {"dataset_id":"cms_hospital_general_info","staged_path":"bronze/cms_hospital_general_info/run/source.csv"}.
    Common mistakes: Promoting corrupt or wrong-schema files because a filename exists.
    """

    _require_mutations_allowed()
    return response_envelope(
        data=core.validate_cache_source(
            dataset_id,
            cache_root=_cache_root(),
            staged_path=staged_path or None,
            relative_path=relative_path,
        )
    )


@mcp.tool()
@observe_tool("cache-manager")
async def promote_cache_artifact(
    dataset_id: str,
    staged_path: str,
    run_id: str = "",
    source_url: str = "",
    relative_path: str = "",
) -> dict[str, Any]:
    """Atomically promote one staged artifact after validation passes.

    Discovery: Promotes one staged, validated artifact into the current cache slot.
    When to use: Use after a staged artifact passes validate_cache_source.
    Parameters: dataset_id, staged_path, optional run_id, optional registered source_url, and relative_path for multi-artifact datasets.
    Returns: Promoted artifact manifest with checksum, validation status, and previous artifact ID.
    Do / Don't: Do promote one dataset at a time; don't use path traversal, arbitrary URLs, or failed validation.
    Examples: {"dataset_id":"cms_hospital_quality","relative_path":"hospital_quality_hac.csv","staged_path":"bronze/cms_hospital_quality/run/hac.csv"}.
    Common mistakes: Overwriting a previous-good cache without rollback metadata.
    """

    _require_mutations_allowed()
    return response_envelope(
        data=core.promote_cache_artifact(
            dataset_id,
            staged_path,
            cache_root=_cache_root(),
            run_id=run_id or None,
            source_url=source_url,
            relative_path=relative_path,
        )
    )


@mcp.tool()
@observe_tool("cache-manager")
async def quarantine_cache_artifact(dataset_id: str, reason: str = "operator_requested") -> dict[str, Any]:
    """Quarantine the current artifact for one dataset without broad deletes.

    Discovery: Marks one promoted artifact invalid and moves it to quarantine.
    When to use: Use when validation or downstream checks detect corrupt/truncated data.
    Parameters: dataset_id and a non-secret reason.
    Returns: Quarantine status, artifact ID, and moved path.
    Do / Don't: Do quarantine one dataset; don't delete broad cache directories.
    Examples: {"dataset_id":"cms_hospital_quality","reason":"wrong_schema"}.
    Common mistakes: Deleting evidence needed for recovery analysis.
    """

    _require_mutations_allowed()
    return response_envelope(data=core.quarantine_cache_artifact(dataset_id, cache_root=_cache_root(), reason=reason))


@mcp.tool()
@observe_tool("cache-manager")
async def rollback_cache_artifact(dataset_id: str) -> dict[str, Any]:
    """Restore the previous promoted artifact for one dataset when available.

    Discovery: Restores previous-good cache metadata and artifact for one dataset.
    When to use: Use after a bad promotion or quarantine when previous_artifact_id is available.
    Parameters: dataset_id must be registered.
    Returns: Rolled-back artifact status or rollback-unavailable recovery guidance.
    Do / Don't: Do preserve audit lineage; don't fabricate readiness when previous artifacts are missing.
    Examples: {"dataset_id":"cms_hospital_general_info"} restores the prior promoted artifact.
    Common mistakes: Continuing to use corrupt current artifacts after rollback is available.
    """

    _require_mutations_allowed()
    return response_envelope(data=core.rollback_cache_artifact(dataset_id, cache_root=_cache_root()))


def _json(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.resource(
    "healthcare-data://cache-manager/policy",
    name="cache_manager_policy",
    description="Cache-manager read/write safety policy and allowed readiness states.",
    mime_type="application/json",
)
def cache_manager_policy() -> str:
    return _json(
        {
            "server_id": "cache-manager",
            "mode": "local_safe_cache_control_plane",
            "mutations_allowed": _mutations_allowed(),
            "allowed_readiness_states": sorted(core.ALLOWED_READINESS_STATES),
            "guards": [
                "registered dataset IDs only",
                "registered source URLs only",
                "cache-root path confinement",
                "private-network and unsafe redirect rejection",
                "download byte limits",
                "validation before promotion",
                "previous-good rollback",
                "non-secret audit events",
            ],
            "remote_gateway_boundary": "discovery and gateway remain metadata-only/read-only",
        }
    )


@mcp.resource(
    "healthcare-data://cache-manager/sources",
    name="cache_manager_sources",
    description="Registered cache-source specs and readiness summaries.",
    mime_type="application/json",
)
def cache_manager_sources() -> str:
    return _json(core.list_cache_sources(cache_root=_cache_root()))


if __name__ == "__main__":
    mcp.run(transport=_transport)
