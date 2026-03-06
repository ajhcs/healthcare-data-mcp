"""Geographic Demographics MCP Server.

Provides Census ACS demographics, ZCTA geography/adjacency,
Medicare enrollment data, and ZIP-to-geography crosswalks.
"""

import json
import logging
import os

import httpx
from mcp.server.fastmcp import FastMCP

from .census_client import get_demographics_batch, get_demographics_for_zcta
from .geography import get_adjacent_zctas
from .models import (
    CrosswalkResponse,
    CrosswalkResult,
    GeographicVariation,
    MedicareEnrollment,
    ZctaAdjacency,
    ZctaDemographics,
)

logger = logging.getLogger(__name__)

import os as _os

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "geo-demographics"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8003"))
mcp = FastMCP(**_mcp_kwargs)

from . import data_loaders as gv_loaders

# HUD USPS Crosswalk API
HUD_CROSSWALK_BASE = "https://www.huduser.gov/hudapi/public/usps"
HUD_CROSSWALK_TYPES = {"tract": 1, "county": 2, "cbsa": 3, "cd": 4}


@mcp.tool()
async def get_zcta_demographics(zcta: str, year: int = 2023) -> str:
    """Get Census ACS demographics for a single ZCTA (ZIP Code Tabulation Area).

    Returns population, age distribution, median income, and health insurance coverage.

    Args:
        zcta: 5-digit ZCTA code (e.g., "60614")
        year: ACS 5-Year data year (default 2023)
    """
    zcta = zcta.strip().zfill(5)
    try:
        data = await get_demographics_for_zcta(zcta, year)
        result = ZctaDemographics(**data)
        return result.model_dump_json(indent=2)
    except httpx.HTTPStatusError as e:
        return json.dumps({"error": f"Census API error: {e.response.status_code}", "detail": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_zcta_demographics_batch(zctas: list[str], year: int = 2023) -> str:
    """Get Census ACS demographics for multiple ZCTAs in a single efficient query.

    Args:
        zctas: List of 5-digit ZCTA codes (e.g., ["60614", "60657", "60613"])
        year: ACS 5-Year data year (default 2023)
    """
    zctas = [z.strip().zfill(5) for z in zctas]
    try:
        data_list = await get_demographics_batch(zctas, year)
        results = [ZctaDemographics(**d) for d in data_list]
        return json.dumps([r.model_dump() for r in results], indent=2)
    except httpx.HTTPStatusError as e:
        return json.dumps({"error": f"Census API error: {e.response.status_code}", "detail": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_zcta_adjacency(zcta: str) -> str:
    """Find all ZCTAs geographically adjacent to the given ZCTA.

    Uses TIGER/Line ZCTA shapefiles. The adjacency graph is computed once
    and cached for subsequent calls.

    NOTE: First call requires downloading the ZCTA shapefile (~800MB) and
    computing adjacency, which may take several minutes.

    Args:
        zcta: 5-digit ZCTA code (e.g., "60614")
    """
    zcta = zcta.strip().zfill(5)
    try:
        neighbors = await get_adjacent_zctas(zcta)
        result = ZctaAdjacency(zcta=zcta, adjacent_zctas=neighbors, count=len(neighbors))
        return result.model_dump_json(indent=2)
    except FileNotFoundError as e:
        return json.dumps({"error": f"Shapefile not available: {e}"})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_medicare_enrollment(state: str | None = None, county_fips: str | None = None) -> str:
    """Get Medicare enrollment and spending data from the CMS Geographic Variation PUF.

    Provide either a state abbreviation or county FIPS code.

    Args:
        state: Two-letter state abbreviation (e.g., "IL"). Returns state-level data.
        county_fips: 5-digit county FIPS code (e.g., "17031" for Cook County, IL). Returns county-level data.
    """
    if not state and not county_fips:
        return json.dumps({"error": "Provide either 'state' or 'county_fips'"})

    try:
        await gv_loaders.ensure_gv_cached()

        if county_fips:
            data = gv_loaders.query_gv("County", county_fips)
            geo_type, geo_code = "county", county_fips
        else:
            data = gv_loaders.query_gv("State", state.upper())
            geo_type, geo_code = "state", state.upper()

        if not data:
            return json.dumps({"error": f"No Medicare data found for {geo_type} {geo_code}"})

        result = MedicareEnrollment(
            geography_type=geo_type,
            geography_code=geo_code,
            geography_name=data.get("geo_desc", ""),
            total_beneficiaries=data.get("total_beneficiaries"),
            ma_penetration_pct=data.get("ma_penetration_pct"),
            avg_age=data.get("avg_age"),
            pct_female=data.get("pct_female"),
            pct_dual_eligible=data.get("pct_dual_eligible"),
            pct_a_b_coverage=None,
            per_capita_spending=data.get("per_capita_spending"),
        )
        return result.model_dump_json(indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_geographic_variation(geography_type: str = "county", geography_code: str | None = None) -> str:
    """Get CMS Geographic Variation PUF data including spending and utilization.

    Returns demographics, per-capita spending breakdown (IP, OP, physician, SNF),
    utilization rates (discharges, ER visits per 1000), and readmission rate.

    Args:
        geography_type: "county" (FIPS code) or "state" (abbreviation)
        geography_code: County FIPS (e.g., "17031") or state abbreviation (e.g., "IL")
    """
    if not geography_code:
        return json.dumps({"error": "geography_code is required"})

    try:
        await gv_loaders.ensure_gv_cached()

        geo_level = "County" if geography_type == "county" else "State"
        code = geography_code.upper() if geography_type != "county" else geography_code
        data = gv_loaders.query_gv(geo_level, code)

        if not data:
            return json.dumps({"error": f"No data found for {geography_type} {geography_code}"})

        result = GeographicVariation(
            geography_type=geography_type,
            geography_code=geography_code,
            geography_name=data.get("geo_desc", ""),
            total_beneficiaries=data.get("total_beneficiaries"),
            avg_age=data.get("avg_age"),
            pct_female=data.get("pct_female"),
            pct_dual_eligible=data.get("pct_dual_eligible"),
            per_capita_spending=data.get("per_capita_spending"),
            ip_spending_per_capita=data.get("ip_spending_per_capita"),
            op_spending_per_capita=data.get("op_spending_per_capita"),
            physician_spending_per_capita=data.get("physician_spending_per_capita"),
            snf_spending_per_capita=data.get("snf_spending_per_capita"),
            discharges_per_1000=data.get("discharges_per_1000"),
            er_visits_per_1000=data.get("er_visits_per_1000"),
            readmission_rate=data.get("readmission_rate"),
        )
        return result.model_dump_json(indent=2)

    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def crosswalk_zip(zip_code: str, target: str = "county") -> str:
    """Crosswalk a ZIP code to another geography using the HUD USPS Crosswalk API.

    Maps ZIP codes to Census tracts, counties, or CBSAs with allocation ratios.

    Requires HUD_API_TOKEN environment variable.

    Args:
        zip_code: 5-digit ZIP code (e.g., "60614")
        target: Target geography — "tract", "county", or "cbsa" (default "county")
    """
    hud_token = os.environ.get("HUD_API_TOKEN")
    if not hud_token:
        return json.dumps({"error": "HUD_API_TOKEN environment variable not set"})

    target_lower = target.lower()
    type_code = HUD_CROSSWALK_TYPES.get(target_lower)
    if type_code is None:
        return json.dumps({
            "error": f"Invalid target '{target}'. Must be one of: {', '.join(HUD_CROSSWALK_TYPES.keys())}"
        })

    zip_code = zip_code.strip().zfill(5)

    try:
        url = HUD_CROSSWALK_BASE
        params = {"type": type_code, "query": zip_code}
        headers = {"Authorization": f"Bearer {hud_token}"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        results_data = data.get("data", {}).get("results", data) if isinstance(data, dict) else data

        # HUD API returns a list of crosswalk records
        if isinstance(results_data, dict):
            results_data = results_data.get("results", [results_data])

        crosswalk_results = []
        for item in results_data if isinstance(results_data, list) else [results_data]:
            # HUD response field names vary by target type
            if target_lower == "county":
                target_code = item.get("county", item.get("geoid", ""))
            elif target_lower == "tract":
                target_code = item.get("tract", item.get("geoid", ""))
            elif target_lower == "cbsa":
                target_code = item.get("cbsa", item.get("geoid", ""))
            else:
                target_code = item.get("geoid", "")

            crosswalk_results.append(CrosswalkResult(
                zip_code=zip_code,
                target_type=target_lower,
                target_code=str(target_code),
                residential_ratio=_to_float(item.get("res_ratio")),
                business_ratio=_to_float(item.get("bus_ratio")),
                other_ratio=_to_float(item.get("oth_ratio")),
                total_ratio=_to_float(item.get("tot_ratio")),
            ))

        response = CrosswalkResponse(
            zip_code=zip_code,
            target_type=target_lower,
            results=crosswalk_results,
        )
        return response.model_dump_json(indent=2)

    except httpx.HTTPStatusError as e:
        return json.dumps({"error": f"HUD API error: {e.response.status_code}", "detail": str(e)})
    except Exception as e:
        return json.dumps({"error": str(e)})


# --- Utility functions ---

def _to_int(value) -> int | None:
    """Safely convert a value to int."""
    if value is None:
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _to_float(value) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    mcp.run(transport=_transport)
