"""Claims & Service Line Analytics MCP Server.

Provides tools for inpatient discharge volumes, outpatient procedure volumes,
multi-year service line trends, case mix computation, and market volume analysis.
All data sourced from CMS Medicare Provider Utilization PUFs.
"""

from typing import Any
import logging
import os as _os
from datetime import datetime, timezone
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured

from . import data_loaders, service_lines  # pyright: ignore[reportAttributeAccessIssue]
from .models import (
    APCDetail,
    CaseMixResponse,
    DRGDetail,
    DRGWeightContribution,
    InpatientVolumesResponse,
    MarketVolumesResponse,
    OutpatientTrend,
    OutpatientVolumesResponse,
    ProviderMarketShare,
    ServiceLineAcuity,
    ServiceLineMarketTotal,
    ServiceLineShare,
    ServiceLineSummary,
    ServiceLineTrend,
    ServiceLineTrendResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "claims-analytics"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8012"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "claims-analytics")


def _validate_year(year: str) -> dict[str, Any] | None:
    """Validate year parameter. Returns an error response or None if valid."""
    if year and year not in data_loaders.AVAILABLE_YEARS:
        return error_response(f"Invalid year: {year}. Available: {data_loaders.AVAILABLE_YEARS}")
    return None


def _claims_source_metadata(dataset: str, year: str) -> dict[str, Any]:
    """Return source/cache metadata for CMS Medicare Provider Utilization PUFs."""

    if dataset == "inpatient":
        source_name = "CMS Medicare Inpatient Hospitals by Provider and Service PUF"
        source_url = data_loaders.INPATIENT_URLS.get(year, "")
        dataset_id = "cms_medicare_inpatient_puf"
        cache_paths = [data_loaders._cache_path("inpatient", year)]
    elif dataset == "outpatient":
        source_name = "CMS Medicare Outpatient Hospitals by Provider and Service PUF"
        source_url = data_loaders.OUTPATIENT_URLS.get(year, "")
        dataset_id = "cms_medicare_outpatient_puf"
        cache_paths = [data_loaders._cache_path("outpatient", year)]
    else:
        source_name = "CMS Medicare Provider Utilization PUF"
        source_url = "; ".join(
            value
            for value in (
                data_loaders.INPATIENT_URLS.get(year, ""),
                data_loaders.OUTPATIENT_URLS.get(year, ""),
            )
            if value
        )
        dataset_id = "cms_medicare_provider_utilization_puf"
        cache_paths = [data_loaders._cache_path("inpatient", year), data_loaders._cache_path("outpatient", year)]

    metadata: dict[str, Any] = {
        "source_name": source_name,
        "source_url": source_url,
        "dataset_id": dataset_id,
        "source_period": year,
        "landing_page": "https://data.cms.gov/provider-summary-by-type-of-service/medicare-inpatient-hospitals",
        "cache_status": "missing",
        "cache_freshness": "missing",
        "cache_key": "; ".join(str(path) for path in cache_paths),
    }
    existing = [path for path in cache_paths if path.exists()]
    if existing:
        newest = max(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc) for path in existing)
        age_days = (datetime.now(timezone.utc) - newest).total_seconds() / 86400
        metadata.update(
            {
                "cache_status": "ready" if len(existing) == len(cache_paths) else "partial",
                "cache_freshness": f"ready; age_days={age_days:.1f}" if len(existing) == len(cache_paths) else f"partial; age_days={age_days:.1f}",
                "source_modified": newest.isoformat(),
                "cache_age_days": round(age_days, 1),
            }
        )
    return metadata


def _claims_evidence(
    *,
    dataset: str,
    year: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
) -> dict[str, Any]:
    metadata = _claims_source_metadata(dataset, year)
    return evidence_receipt(
        source_metadata=metadata,
        entity_scope="claims_public_aggregate",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=(
            "CMS Medicare Provider Utilization PUF data is public aggregate provider/service data. "
            "It is not patient-level data, not PHI, and may lag current operations."
        ),
        next_step=next_step,
    )


def _claims_row_evidence(
    *,
    dataset: str,
    year: str,
    parent_query: dict[str, Any],
    row: dict[str, Any],
    row_kind: str,
    match_basis: str,
    confidence: str = "source_row_aggregate",
    next_step: str = "Preserve this row receipt with the parent claims evidence before citing the aggregate fact.",
) -> dict[str, Any]:
    row_query = {
        **parent_query,
        "row_kind": row_kind,
        "row_ccn": row.get("ccn") or parent_query.get("ccn") or "",
        "row_provider_name": row.get("provider_name") or "",
        "row_state": row.get("state") or "",
        "row_service_line": row.get("service_line") or "",
        "row_drg_code": row.get("drg_code") or "",
        "row_apc_code": row.get("apc_code") or "",
        "row_top_provider_ccn": row.get("top_provider_ccn") or "",
    }
    return _claims_evidence(
        dataset=dataset,
        year=year,
        query=row_query,
        match_basis=match_basis,
        confidence=confidence,
        next_step=next_step,
    )


def _facility_identity(ccn: str, provider_name: str = "", state: str = "") -> dict[str, Any]:
    return identity_from_public_record(
        name=provider_name,
        entity_type="facility",
        ccn=ccn,
        source_name="CMS Medicare Provider Utilization PUF",
    ).to_dict() | ({"state": state} if state else {})


def _attach_claims_context(
    payload: dict[str, Any],
    *,
    dataset: str,
    year: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
    provider_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload["source_metadata"] = _claims_source_metadata(dataset, year)
    payload["evidence"] = _claims_evidence(
        dataset=dataset,
        year=year,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        next_step=next_step,
    )
    if payload.get("ccn"):
        payload["identity"] = _facility_identity(
            str(payload.get("ccn") or ""),
            str(payload.get("provider_name") or ""),
            str(payload.get("state") or ""),
        )
    if provider_rows is not None:
        payload["identity_map"] = {
            "entities": [
                _facility_identity(
                    str(row.get("ccn") or ""),
                    str(row.get("provider_name") or ""),
                    str(row.get("state") or ""),
                )
                for row in provider_rows
            ],
            "match_basis": "ccn_exact_provider_set",
            "conflict_policy": "Do not merge providers by name alone; use CCN as the market-share entity key.",
        }
    return payload


# ---------------------------------------------------------------------------
# Tool 1: get_inpatient_volumes
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("claims-analytics")
async def get_inpatient_volumes(
    ccn: str, drg_code: str = "", service_line: str = "", year: str = "",
) -> dict[str, Any]:
    """Get inpatient discharge volumes by DRG and service line for a hospital.

    Uses CMS Medicare Inpatient Hospitals PUF (by Provider and Service).

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        drg_code: Filter to a specific MS-DRG code (e.g. "470").
        service_line: Filter to a service line (e.g. "Cardiovascular", "Orthopedics").
        year: Discharge year ("2021", "2022", "2023"). Default: latest available.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_inpatient_volumes","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if err := _validate_year(year):
            return err
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccn=ccn, drg_code=drg_code)
        if not rows:
            return error_response(f"No inpatient data found for CCN: {ccn}")

        # Map DRGs to service lines
        for r in rows:
            r["service_line"] = service_lines.map_drg_to_service_line(r["drg_code"])

        # Apply service line filter
        if service_line:
            rows = [r for r in rows if r["service_line"].lower() == service_line.lower()]
            if not rows:
                return error_response(f"No data for service line '{service_line}' at CCN: {ccn}")

        # Build DRG details
        drg_details = [
            DRGDetail(
                drg_code=r["drg_code"],
                drg_description=r["drg_desc"],
                service_line=r["service_line"],
                discharges=r["discharges"],
                avg_charges=r["avg_charges"],
                avg_total_payment=r["avg_total_payment"],
                avg_medicare_payment=r["avg_medicare_payment"],
            )
            for r in rows
        ]

        # Aggregate by service line
        sl_totals: dict[str, dict] = {}
        total_discharges = 0
        for r in rows:
            sl = r["service_line"]
            total_discharges += r["discharges"]
            if sl not in sl_totals:
                sl_totals[sl] = {"discharges": 0, "charge_sum": 0.0, "payment_sum": 0.0, "count": 0}
            sl_totals[sl]["discharges"] += r["discharges"]
            sl_totals[sl]["charge_sum"] += r["avg_charges"] * r["discharges"]
            sl_totals[sl]["payment_sum"] += r["avg_medicare_payment"] * r["discharges"]
            sl_totals[sl]["count"] += 1

        sl_summary = []
        for sl, t in sorted(sl_totals.items(), key=lambda x: x[1]["discharges"], reverse=True):
            sl_summary.append(ServiceLineSummary(
                service_line=sl,
                discharges=t["discharges"],
                pct_of_total=round(t["discharges"] / total_discharges * 100, 1) if total_discharges else 0,
                avg_charges=round(t["charge_sum"] / t["discharges"], 2) if t["discharges"] else 0,
                avg_medicare_payment=round(t["payment_sum"] / t["discharges"], 2) if t["discharges"] else 0,
            ))

        response = InpatientVolumesResponse(
            ccn=ccn,
            provider_name=rows[0]["provider_name"] if rows else "",
            state=rows[0]["state"] if rows else "",
            year=yr,
            total_discharges=total_discharges,
            total_drgs=len(drg_details),
            service_line_summary=sl_summary,
            drg_details=sorted(drg_details, key=lambda d: d.discharges, reverse=True),
        )
        payload = _attach_claims_context(
            response.model_dump(),
            dataset="inpatient",
            year=yr,
            query={"ccn": ccn, "drg_code": drg_code, "service_line": service_line, "year": yr},
            match_basis="ccn_exact_inpatient_provider_service_rows",
            confidence="high_for_public_cms_provider_service_aggregate",
            next_step="Preserve DRG/service-line filters and this evidence receipt when citing inpatient aggregate volumes.",
        )
        for summary in payload["service_line_summary"]:
            summary["evidence"] = _claims_row_evidence(
                dataset="inpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=summary,
                row_kind="inpatient_service_line_summary",
                match_basis="inpatient_service_line_summary_row",
            )
        for detail in payload["drg_details"]:
            detail["evidence"] = _claims_row_evidence(
                dataset="inpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=detail,
                row_kind="inpatient_drg_detail",
                match_basis="inpatient_drg_detail_row",
            )
        return to_structured(payload)

    except Exception as e:
        logger.exception("get_inpatient_volumes failed")
        return error_response(f"get_inpatient_volumes failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_outpatient_volumes
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("claims-analytics")
async def get_outpatient_volumes(
    ccn: str, apc_code: str = "", year: str = "",
) -> dict[str, Any]:
    """Get outpatient procedure volumes by APC for a hospital.

    Uses CMS Medicare Outpatient Hospitals PUF (by Provider and Service).

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        apc_code: Filter to a specific APC code.
        year: Discharge year ("2021", "2022", "2023"). Default: latest available.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_outpatient_volumes","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if err := _validate_year(year):
            return err
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_outpatient_cached(yr)

        rows = data_loaders.query_outpatient(year=yr, ccn=ccn, apc_code=apc_code)
        if not rows:
            return error_response(f"No outpatient data found for CCN: {ccn}")

        total_services = sum(r["services"] for r in rows)

        apc_details = [
            APCDetail(
                apc_code=r["apc_code"],
                apc_description=r["apc_desc"],
                services=r["services"],
                avg_charges=r["avg_charges"],
                avg_total_payment=r["avg_total_payment"],
                avg_medicare_payment=r["avg_medicare_payment"],
            )
            for r in rows
        ]

        response = OutpatientVolumesResponse(
            ccn=ccn,
            provider_name=rows[0]["provider_name"] if rows else "",
            state=rows[0]["state"] if rows else "",
            year=yr,
            total_services=total_services,
            total_apcs=len(apc_details),
            apc_details=sorted(apc_details, key=lambda a: a.services, reverse=True),
        )
        payload = _attach_claims_context(
            response.model_dump(),
            dataset="outpatient",
            year=yr,
            query={"ccn": ccn, "apc_code": apc_code, "year": yr},
            match_basis="ccn_exact_outpatient_provider_service_rows",
            confidence="high_for_public_cms_provider_service_aggregate",
            next_step="Preserve APC filters and this evidence receipt when citing outpatient aggregate volumes.",
        )
        for detail in payload["apc_details"]:
            detail["evidence"] = _claims_row_evidence(
                dataset="outpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=detail,
                row_kind="outpatient_apc_detail",
                match_basis="outpatient_apc_detail_row",
            )
        return to_structured(payload)

    except Exception as e:
        logger.exception("get_outpatient_volumes failed")
        return error_response(f"get_outpatient_volumes failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: trend_service_lines
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("claims-analytics")
async def trend_service_lines(
    ccn: str, service_line: str = "", include_outpatient: bool = True,
) -> dict[str, Any]:
    """Get multi-year volume trends by service line for a hospital (3-year).

    Shows year-over-year volume changes and compound annual growth rates.

    Args:
        ccn: CMS Certification Number (6-digit).
        service_line: Filter to one service line (e.g. "Cardiovascular").
        include_outpatient: Include outpatient APC trends (default True).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"trend_service_lines","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        cached_years = await data_loaders.ensure_all_years_cached(include_outpatient)
        if not cached_years:
            return error_response("Failed to download PUF data for trend analysis")

        years_sorted = sorted(cached_years)
        provider_name = ""

        # Inpatient trends by service line
        sl_by_year: dict[str, dict[str, int]] = {}
        for yr in years_sorted:
            rows = data_loaders.query_inpatient(year=yr, ccn=ccn)
            if rows and not provider_name:
                provider_name = rows[0]["provider_name"]
            for r in rows:
                sl = service_lines.map_drg_to_service_line(r["drg_code"])
                if service_line and sl.lower() != service_line.lower():
                    continue
                if sl not in sl_by_year:
                    sl_by_year[sl] = {}
                sl_by_year[sl][yr] = sl_by_year[sl].get(yr, 0) + r["discharges"]

        inpatient_trends = []
        for sl, volumes in sorted(sl_by_year.items()):
            yoy: dict[str, float] = {}
            sorted_yrs = sorted(volumes.keys())
            for i in range(1, len(sorted_yrs)):
                prev = volumes[sorted_yrs[i - 1]]
                curr = volumes[sorted_yrs[i]]
                if prev > 0:
                    yoy[sorted_yrs[i]] = round((curr - prev) / prev * 100, 1)

            # CAGR
            cagr = 0.0
            if len(sorted_yrs) >= 2:
                first_vol = volumes[sorted_yrs[0]]
                last_vol = volumes[sorted_yrs[-1]]
                n_years = int(sorted_yrs[-1]) - int(sorted_yrs[0])
                if first_vol > 0 and n_years > 0:
                    cagr = round(((last_vol / first_vol) ** (1 / n_years) - 1) * 100, 1)

            inpatient_trends.append(ServiceLineTrend(
                service_line=sl,
                volumes_by_year=volumes,
                yoy_change_pct=yoy,
                cagr_pct=cagr,
            ))

        # Outpatient trends by APC
        outpatient_trends = None
        if include_outpatient:
            apc_by_year: dict[str, dict] = {}
            for yr in years_sorted:
                rows = data_loaders.query_outpatient(year=yr, ccn=ccn)
                for r in rows:
                    apc = r["apc_code"]
                    if apc not in apc_by_year:
                        apc_by_year[apc] = {"desc": r["apc_desc"], "volumes": {}}
                    apc_by_year[apc]["volumes"][yr] = (
                        apc_by_year[apc]["volumes"].get(yr, 0) + r["services"]
                    )

            outpatient_trends = []
            for apc, apc_data in sorted(apc_by_year.items()):
                volumes = apc_data["volumes"]
                apc_yoy: dict[str, float] = {}
                sorted_yrs = sorted(volumes.keys())
                for i in range(1, len(sorted_yrs)):
                    prev = volumes[sorted_yrs[i - 1]]
                    curr = volumes[sorted_yrs[i]]
                    if prev > 0:
                        apc_yoy[sorted_yrs[i]] = round((curr - prev) / prev * 100, 1)

                apc_cagr = 0.0
                if len(sorted_yrs) >= 2:
                    first_vol = volumes[sorted_yrs[0]]
                    last_vol = volumes[sorted_yrs[-1]]
                    n_years = int(sorted_yrs[-1]) - int(sorted_yrs[0])
                    if first_vol > 0 and n_years > 0:
                        apc_cagr = round(((last_vol / first_vol) ** (1 / n_years) - 1) * 100, 1)

                outpatient_trends.append(OutpatientTrend(
                    apc_code=apc,
                    apc_description=apc_data["desc"],
                    volumes_by_year=volumes,
                    yoy_change_pct=apc_yoy,
                    cagr_pct=apc_cagr,
                ))

        response = ServiceLineTrendResponse(
            ccn=ccn,
            provider_name=provider_name,
            years=years_sorted,
            inpatient_trends=sorted(inpatient_trends, key=lambda t: sum(t.volumes_by_year.values()), reverse=True),
            outpatient_trends=(
                sorted(outpatient_trends, key=lambda t: sum(t.volumes_by_year.values()), reverse=True)[:50]
                if outpatient_trends else None
            ),
        )
        payload = _attach_claims_context(
            response.model_dump(),
            dataset="combined" if include_outpatient else "inpatient",
            year=years_sorted[-1] if years_sorted else data_loaders.LATEST_YEAR,
            query={"ccn": ccn, "service_line": service_line, "include_outpatient": include_outpatient},
            match_basis="ccn_exact_multi_year_provider_service_rows",
            confidence="high_for_public_cms_provider_service_aggregate",
            next_step="Use the years field and evidence receipt when citing trends; do not compare against nonmatching periods.",
        )
        trend_dataset = "combined" if include_outpatient else "inpatient"
        trend_year = years_sorted[-1] if years_sorted else data_loaders.LATEST_YEAR
        for trend in payload["inpatient_trends"]:
            trend["evidence"] = _claims_row_evidence(
                dataset=trend_dataset,
                year=trend_year,
                parent_query=payload["evidence"]["query"],
                row=trend,
                row_kind="inpatient_service_line_trend",
                match_basis="inpatient_service_line_trend_row",
            )
        for trend in payload.get("outpatient_trends") or []:
            trend["evidence"] = _claims_row_evidence(
                dataset=trend_dataset,
                year=trend_year,
                parent_query=payload["evidence"]["query"],
                row=trend,
                row_kind="outpatient_apc_trend",
                match_basis="outpatient_apc_trend_row",
            )
        return to_structured(payload)

    except Exception as e:
        logger.exception("trend_service_lines failed")
        return error_response(f"trend_service_lines failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: compute_case_mix
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("claims-analytics")
async def compute_case_mix(ccn: str, year: str = "") -> dict[str, Any]:
    """Compute case mix index and acuity analysis by service line for a hospital.

    Uses inpatient discharge data with CMS IPPS DRG relative weights.

    Args:
        ccn: CMS Certification Number (6-digit).
        year: Discharge year. Default: latest available.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"compute_case_mix","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if err := _validate_year(year):
            return err
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccn=ccn)
        if not rows:
            return error_response(f"No inpatient data found for CCN: {ccn}")

        # Compute overall CMI
        drg_discharges = [(r["drg_code"], r["discharges"]) for r in rows]
        cmi = service_lines.compute_cmi(drg_discharges)

        total_discharges = sum(r["discharges"] for r in rows)

        # Service line acuity
        sl_data: dict[str, dict] = {}
        total_weight = 0.0
        for r in rows:
            sl = service_lines.map_drg_to_service_line(r["drg_code"])
            weight = service_lines.get_drg_weight(r["drg_code"])
            contrib = weight * r["discharges"]
            total_weight += contrib

            if sl not in sl_data:
                sl_data[sl] = {"discharges": 0, "weighted_sum": 0.0}
            sl_data[sl]["discharges"] += r["discharges"]
            sl_data[sl]["weighted_sum"] += contrib

        sl_acuity = []
        for sl, d in sorted(sl_data.items(), key=lambda x: x[1]["weighted_sum"], reverse=True):
            sl_acuity.append(ServiceLineAcuity(
                service_line=sl,
                discharges=d["discharges"],
                avg_drg_weight=round(d["weighted_sum"] / d["discharges"], 4) if d["discharges"] else 0,
                pct_of_total_weight=round(d["weighted_sum"] / total_weight * 100, 1) if total_weight else 0,
            ))

        # Top DRGs by weight contribution
        drg_contribs = []
        for r in rows:
            weight = service_lines.get_drg_weight(r["drg_code"])
            contrib = weight * r["discharges"]
            drg_contribs.append(DRGWeightContribution(
                drg_code=r["drg_code"],
                drg_description=r["drg_desc"],
                service_line=service_lines.map_drg_to_service_line(r["drg_code"]),
                discharges=r["discharges"],
                drg_weight=weight,
                total_weight_contribution=round(contrib, 2),
                pct_of_total_weight=round(contrib / total_weight * 100, 1) if total_weight else 0,
            ))

        response = CaseMixResponse(
            ccn=ccn,
            provider_name=rows[0]["provider_name"] if rows else "",
            year=yr,
            case_mix_index=cmi,
            total_discharges=total_discharges,
            service_line_acuity=sl_acuity,
            top_drgs_by_weight=sorted(drg_contribs, key=lambda d: d.total_weight_contribution, reverse=True)[:25],
        )
        payload = _attach_claims_context(
            response.model_dump(),
            dataset="inpatient",
            year=yr,
            query={"ccn": ccn, "year": yr},
            match_basis="ccn_exact_inpatient_drg_rows_with_public_weights",
            confidence="derived_from_public_cms_provider_service_aggregate",
            next_step="Preserve DRG weight assumptions and source period when citing case-mix-derived facts.",
        )
        for acuity in payload["service_line_acuity"]:
            acuity["evidence"] = _claims_row_evidence(
                dataset="inpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=acuity,
                row_kind="case_mix_service_line_acuity",
                match_basis="case_mix_service_line_acuity_row",
                confidence="derived_source_row_aggregate",
            )
        for contribution in payload["top_drgs_by_weight"]:
            contribution["evidence"] = _claims_row_evidence(
                dataset="inpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=contribution,
                row_kind="case_mix_drg_weight_contribution",
                match_basis="case_mix_drg_weight_contribution_row",
                confidence="derived_source_row_aggregate",
            )
        return to_structured(payload)

    except Exception as e:
        logger.exception("compute_case_mix failed")
        return error_response(f"compute_case_mix failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: analyze_market_volumes
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("claims-analytics")
async def analyze_market_volumes(
    provider_ccns: list[str], service_line: str = "", year: str = "",
) -> dict[str, Any]:
    """Analyze service-line market share among a set of providers.

    Compare inpatient volumes across providers within a defined market area.
    Use with service-area or geo-demographics tools to identify competitor CCNs.

    Args:
        provider_ccns: List of CCNs for providers in the market (e.g. ["390223", "390111"]).
        service_line: Filter to one service line.
        year: Discharge year. Default: latest available.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"analyze_market_volumes","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if err := _validate_year(year):
            return err
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccns=provider_ccns)
        if not rows:
            return error_response("No inpatient data found for the provided CCNs")

        # Map service lines
        for r in rows:
            r["service_line"] = service_lines.map_drg_to_service_line(r["drg_code"])

        # Apply service line filter
        if service_line:
            rows = [r for r in rows if r["service_line"].lower() == service_line.lower()]

        # Aggregate by provider
        provider_data: dict[str, dict] = {}
        for r in rows:
            ccn = r["ccn"]
            if ccn not in provider_data:
                provider_data[ccn] = {
                    "provider_name": r["provider_name"],
                    "state": r["state"],
                    "total_discharges": 0,
                    "by_sl": {},
                }
            provider_data[ccn]["total_discharges"] += r["discharges"]
            sl = r["service_line"]
            if sl not in provider_data[ccn]["by_sl"]:
                provider_data[ccn]["by_sl"][sl] = 0
            provider_data[ccn]["by_sl"][sl] += r["discharges"]

        total_market = sum(p["total_discharges"] for p in provider_data.values())

        # Market totals by service line
        sl_market: dict[str, dict] = {}
        for ccn, p in provider_data.items():
            for sl, vol in p["by_sl"].items():
                if sl not in sl_market:
                    sl_market[sl] = {"total": 0, "top_ccn": "", "top_name": "", "top_vol": 0}
                sl_market[sl]["total"] += vol
                if vol > sl_market[sl]["top_vol"]:
                    sl_market[sl]["top_ccn"] = ccn
                    sl_market[sl]["top_name"] = p["provider_name"]
                    sl_market[sl]["top_vol"] = vol

        # Build provider shares
        provider_shares = []
        for ccn, p in sorted(provider_data.items(), key=lambda x: x[1]["total_discharges"], reverse=True):
            sl_breakdown = []
            for sl, vol in sorted(p["by_sl"].items(), key=lambda x: x[1], reverse=True):
                sl_total = sl_market[sl]["total"]
                sl_breakdown.append(ServiceLineShare(
                    service_line=sl,
                    discharges=vol,
                    market_share_pct=round(vol / sl_total * 100, 1) if sl_total else 0,
                ))

            provider_shares.append(ProviderMarketShare(
                ccn=ccn,
                provider_name=p["provider_name"],
                state=p["state"],
                total_discharges=p["total_discharges"],
                market_share_pct=round(p["total_discharges"] / total_market * 100, 1) if total_market else 0,
                service_line_breakdown=sl_breakdown,
            ))

        sl_totals = [
            ServiceLineMarketTotal(
                service_line=sl,
                total_discharges=d["total"],
                pct_of_market=round(d["total"] / total_market * 100, 1) if total_market else 0,
                top_provider_ccn=d["top_ccn"],
                top_provider_name=d["top_name"],
            )
            for sl, d in sorted(sl_market.items(), key=lambda x: x[1]["total"], reverse=True)
        ]

        response = MarketVolumesResponse(
            year=yr,
            total_market_discharges=total_market,
            total_providers=len(provider_data),
            provider_shares=provider_shares,
            service_line_totals=sl_totals,
        )
        provider_rows = [
            {
                "ccn": share.ccn,
                "provider_name": share.provider_name,
                "state": share.state,
            }
            for share in provider_shares
        ]
        payload = _attach_claims_context(
            response.model_dump(),
            dataset="inpatient",
            year=yr,
            query={"provider_ccns": provider_ccns, "service_line": service_line, "year": yr},
            match_basis="ccn_exact_provider_set_inpatient_rows",
            confidence="high_for_public_cms_provider_service_market_aggregate",
            next_step="Use the same provider CCN set and service-line filter when reproducing market-share facts.",
            provider_rows=provider_rows,
        )
        for share in payload["provider_shares"]:
            share["evidence"] = _claims_row_evidence(
                dataset="inpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=share,
                row_kind="provider_market_share",
                match_basis="provider_market_share_row",
                confidence="derived_provider_set_market_aggregate",
            )
            for breakdown in share.get("service_line_breakdown") or []:
                breakdown["evidence"] = _claims_row_evidence(
                    dataset="inpatient",
                    year=yr,
                    parent_query={**payload["evidence"]["query"], "ccn": share.get("ccn") or ""},
                    row=breakdown,
                    row_kind="provider_service_line_market_share",
                    match_basis="provider_service_line_market_share_row",
                    confidence="derived_provider_set_market_aggregate",
                )
        for total in payload["service_line_totals"]:
            total["evidence"] = _claims_row_evidence(
                dataset="inpatient",
                year=yr,
                parent_query=payload["evidence"]["query"],
                row=total,
                row_kind="service_line_market_total",
                match_basis="service_line_market_total_row",
                confidence="derived_provider_set_market_aggregate",
            )
        return to_structured(payload)

    except Exception as e:
        logger.exception("analyze_market_volumes failed")
        return error_response(f"analyze_market_volumes failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
