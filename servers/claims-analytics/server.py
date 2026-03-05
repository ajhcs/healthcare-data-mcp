"""Claims & Service Line Analytics MCP Server.

Provides tools for inpatient discharge volumes, outpatient procedure volumes,
multi-year service line trends, case mix computation, and market volume analysis.
All data sourced from CMS Medicare Provider Utilization PUFs.
"""

import json
import logging
import os as _os
from mcp.server.fastmcp import FastMCP

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
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8012"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Tool 1: get_inpatient_volumes
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_inpatient_volumes(
    ccn: str, drg_code: str = "", service_line: str = "", year: str = "",
) -> str:
    """Get inpatient discharge volumes by DRG and service line for a hospital.

    Uses CMS Medicare Inpatient Hospitals PUF (by Provider and Service).

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        drg_code: Filter to a specific MS-DRG code (e.g. "470").
        service_line: Filter to a service line (e.g. "Cardiovascular", "Orthopedics").
        year: Discharge year ("2021", "2022", "2023"). Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccn=ccn, drg_code=drg_code)
        if not rows:
            return json.dumps({"error": f"No inpatient data found for CCN: {ccn}"})

        # Map DRGs to service lines
        for r in rows:
            r["service_line"] = service_lines.map_drg_to_service_line(r["drg_code"])

        # Apply service line filter
        if service_line:
            rows = [r for r in rows if r["service_line"].lower() == service_line.lower()]
            if not rows:
                return json.dumps({"error": f"No data for service line '{service_line}' at CCN: {ccn}"})

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
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("get_inpatient_volumes failed")
        return json.dumps({"error": f"get_inpatient_volumes failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_outpatient_volumes
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_outpatient_volumes(
    ccn: str, apc_code: str = "", year: str = "",
) -> str:
    """Get outpatient procedure volumes by APC for a hospital.

    Uses CMS Medicare Outpatient Hospitals PUF (by Provider and Service).

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        apc_code: Filter to a specific APC code.
        year: Discharge year ("2021", "2022", "2023"). Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_outpatient_cached(yr)

        rows = data_loaders.query_outpatient(year=yr, ccn=ccn, apc_code=apc_code)
        if not rows:
            return json.dumps({"error": f"No outpatient data found for CCN: {ccn}"})

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
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("get_outpatient_volumes failed")
        return json.dumps({"error": f"get_outpatient_volumes failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: trend_service_lines
# ---------------------------------------------------------------------------
@mcp.tool()
async def trend_service_lines(
    ccn: str, service_line: str = "", include_outpatient: bool = True,
) -> str:
    """Get multi-year volume trends by service line for a hospital (3-year).

    Shows year-over-year volume changes and compound annual growth rates.

    Args:
        ccn: CMS Certification Number (6-digit).
        service_line: Filter to one service line (e.g. "Cardiovascular").
        include_outpatient: Include outpatient APC trends (default True).
    """
    try:
        cached_years = await data_loaders.ensure_all_years_cached(include_outpatient)
        if not cached_years:
            return json.dumps({"error": "Failed to download PUF data for trend analysis"})

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
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("trend_service_lines failed")
        return json.dumps({"error": f"trend_service_lines failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: compute_case_mix
# ---------------------------------------------------------------------------
@mcp.tool()
async def compute_case_mix(ccn: str, year: str = "") -> str:
    """Compute case mix index and acuity analysis by service line for a hospital.

    Uses inpatient discharge data with CMS IPPS DRG relative weights.

    Args:
        ccn: CMS Certification Number (6-digit).
        year: Discharge year. Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccn=ccn)
        if not rows:
            return json.dumps({"error": f"No inpatient data found for CCN: {ccn}"})

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
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("compute_case_mix failed")
        return json.dumps({"error": f"compute_case_mix failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: analyze_market_volumes
# ---------------------------------------------------------------------------
@mcp.tool()
async def analyze_market_volumes(
    provider_ccns: list[str], service_line: str = "", year: str = "",
) -> str:
    """Analyze service-line market share among a set of providers.

    Compare inpatient volumes across providers within a defined market area.
    Use with service-area or geo-demographics tools to identify competitor CCNs.

    Args:
        provider_ccns: List of CCNs for providers in the market (e.g. ["390223", "390111"]).
        service_line: Filter to one service line.
        year: Discharge year. Default: latest available.
    """
    try:
        yr = year or data_loaders.LATEST_YEAR
        await data_loaders.ensure_inpatient_cached(yr)

        rows = data_loaders.query_inpatient(year=yr, ccns=provider_ccns)
        if not rows:
            return json.dumps({"error": "No inpatient data found for the provided CCNs"})

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
        return json.dumps(response.model_dump())

    except Exception as e:
        logger.exception("analyze_market_volumes failed")
        return json.dumps({"error": f"analyze_market_volumes failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
