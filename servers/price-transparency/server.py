"""Price Transparency / MRF Engine MCP Server.

Provides tools for discovering hospital machine-readable files (MRFs),
querying negotiated rates from cached Parquet, computing rate dispersion
statistics, cross-hospital comparisons, and Medicare benchmark analysis.
"""

import json
import logging
import os as _os
import statistics as _stats

from mcp.server.fastmcp import FastMCP

from . import benchmark_client, mrf_processor, mrf_registry
from .models import (
    BenchmarkComparison,
    BenchmarkResponse,
    HospitalRateComparison,
    MRFIndexResult,
    MRFLocation,
    NegotiatedRate,
    NegotiatedRatesResponse,
    RateDispersion,
    SystemComparisonResponse,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "price-transparency"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8009"))
mcp = FastMCP(**_mcp_kwargs)


# ---------------------------------------------------------------------------
# Tool 1: search_mrf_index
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_mrf_index(query: str, state: str = "") -> str:
    """Search for hospital MRF (machine-readable file) URLs by name, CCN, or EIN.

    Discovers hospitals via the CMS Provider Data Catalog and curated registry,
    then checks whether cached Parquet data already exists for each result.

    Args:
        query: Hospital name, CCN (6-digit), or EIN to search for.
        state: Two-letter state code filter (e.g. "OH").
    """
    try:
        hospitals = await mrf_registry.discover_mrf_urls(query, state=state)

        results = []
        for hosp in hospitals:
            ccn = hosp.get("ccn", "")
            hospital_id = ccn or hosp.get("ein", "")

            # Check cache status
            cached = False
            cache_date = ""
            row_count = None
            if hospital_id:
                cached = mrf_processor.is_cached(hospital_id)
                if cached:
                    meta = mrf_processor.get_cache_metadata(hospital_id)
                    cache_date = meta.get("cached_at", "")
                    row_count = meta.get("row_count")

            results.append(MRFIndexResult(
                hospital_name=hosp.get("name", ""),
                ccn=ccn,
                ein=hosp.get("ein", ""),
                city=hosp.get("city", ""),
                state=hosp.get("state", ""),
                mrf_urls=[MRFLocation(**u) for u in hosp.get("mrf_urls", [])],
                cached=cached,
                cache_date=cache_date,
                row_count=row_count,
            ).model_dump())

        return json.dumps({"total_results": len(results), "hospitals": results})
    except Exception as e:
        logger.exception("search_mrf_index failed")
        return json.dumps({"error": f"search_mrf_index failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_negotiated_rates
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_negotiated_rates(
    hospital_id: str,
    cpt_codes: list[str],
    payer: str = "",
    mrf_url: str = "",
) -> str:
    """Get negotiated rates for CPT codes at a hospital from cached Parquet data.

    If the hospital is not yet cached and an mrf_url is provided, triggers
    an MRF download and processing pipeline before querying.

    Args:
        hospital_id: Hospital identifier (CCN or filesystem-safe ID).
        cpt_codes: List of CPT/HCPCS codes to look up (e.g. ["99213", "27447"]).
        payer: Optional payer name filter (case-insensitive partial match).
        mrf_url: Optional MRF file URL to trigger download if not cached.
    """
    try:
        cached = mrf_processor.is_cached(hospital_id)

        if not cached:
            if mrf_url:
                # Auto-download and process the MRF
                logger.info(
                    "Hospital %s not cached; downloading from %s",
                    hospital_id, mrf_url,
                )
                await mrf_processor.process_mrf(mrf_url, hospital_id)
            else:
                return json.dumps({
                    "error": (
                        f"No cached data for hospital '{hospital_id}'. "
                        "Use search_mrf_index to find the MRF URL, then pass it "
                        "as mrf_url to trigger automatic download and processing."
                    )
                })

        # Query rates from Parquet cache
        raw_rates = mrf_processor.get_rates(hospital_id, cpt_codes, payer=payer)

        meta = mrf_processor.get_cache_metadata(hospital_id)
        hospital_name = meta.get("hospital_name", hospital_id)

        rates = [NegotiatedRate(**r) for r in raw_rates]
        source = "live_download" if mrf_url and not cached else "parquet_cache"

        response = NegotiatedRatesResponse(
            hospital_name=hospital_name,
            hospital_id=hospital_id,
            cpt_codes_requested=cpt_codes,
            rates=rates,
            total_rates=len(rates),
            source=source,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("get_negotiated_rates failed")
        return json.dumps({"error": f"get_negotiated_rates failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: compute_rate_dispersion
# ---------------------------------------------------------------------------
@mcp.tool()
async def compute_rate_dispersion(hospital_id: str, cpt_codes: list[str]) -> str:
    """Compute rate dispersion statistics for CPT codes across payers at a hospital.

    Shows min, max, median, mean, IQR, coefficient of variation, and standard
    deviation of negotiated dollar amounts for each requested CPT code.

    Args:
        hospital_id: Hospital identifier (CCN or filesystem-safe ID).
        cpt_codes: List of CPT/HCPCS codes to analyze.
    """
    try:
        if not mrf_processor.is_cached(hospital_id):
            return json.dumps({
                "error": (
                    f"No cached data for hospital '{hospital_id}'. "
                    "Use get_negotiated_rates with an mrf_url first."
                )
            })

        raw_stats = mrf_processor.get_rate_stats(hospital_id, cpt_codes)

        results = [RateDispersion(**s).model_dump() for s in raw_stats]
        return json.dumps({"hospital_id": hospital_id, "dispersion": results})
    except Exception as e:
        logger.exception("compute_rate_dispersion failed")
        return json.dumps({"error": f"compute_rate_dispersion failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: compare_rates_system
# ---------------------------------------------------------------------------
@mcp.tool()
async def compare_rates_system(system_name: str, cpt_codes: list[str]) -> str:
    """Compare negotiated rates across hospitals in a health system.

    Queries all cached hospitals whose names match the system name, then
    retrieves rates for the requested CPT codes at each matching hospital.

    Args:
        system_name: Health system name to match (case-insensitive partial match).
        cpt_codes: List of CPT/HCPCS codes to compare.
    """
    try:
        all_hospitals = mrf_processor.get_all_cached_hospitals()

        # Filter to hospitals matching system name
        system_lower = system_name.lower()
        matching = [
            h for h in all_hospitals
            if system_lower in h.get("hospital_name", "").lower()
        ]

        if not matching:
            return json.dumps({
                "error": (
                    f"No cached hospitals match system name '{system_name}'. "
                    f"Found {len(all_hospitals)} cached hospitals total."
                )
            })

        hospitals = []
        for hosp_meta in matching:
            hid = hosp_meta["hospital_id"]
            hname = hosp_meta.get("hospital_name", hid)
            try:
                raw_rates = mrf_processor.get_rates(hid, cpt_codes)
                rates = [NegotiatedRate(**r) for r in raw_rates]
            except Exception as exc:
                logger.warning("Failed to query rates for %s: %s", hid, exc)
                rates = []

            hospitals.append(HospitalRateComparison(
                hospital_name=hname,
                hospital_id=hid,
                rates=rates,
            ))

        response = SystemComparisonResponse(
            system_name=system_name,
            cpt_codes=cpt_codes,
            hospitals=hospitals,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("compare_rates_system failed")
        return json.dumps({"error": f"compare_rates_system failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: benchmark_rates
# ---------------------------------------------------------------------------
@mcp.tool()
async def benchmark_rates(
    hospital_id: str,
    cpt_codes: list[str],
    locality: str = "",
) -> str:
    """Benchmark a hospital's negotiated rates against Medicare and peer hospitals.

    For each CPT code, computes:
    - Medicare allowed amount (from CMS Physician Fee Schedule RVUs + GPCI)
    - Medicare actual average payment (from CMS utilization data)
    - Hospital rate as a percentage of Medicare
    - Peer percentile rank across all cached hospitals

    Args:
        hospital_id: Hospital identifier (CCN or filesystem-safe ID).
        cpt_codes: List of CPT/HCPCS codes to benchmark.
        locality: Medicare GPCI locality code (e.g. "0100000"). Defaults to national.
    """
    try:
        if not mrf_processor.is_cached(hospital_id):
            return json.dumps({
                "error": (
                    f"No cached data for hospital '{hospital_id}'. "
                    "Use get_negotiated_rates with an mrf_url first."
                )
            })

        meta = mrf_processor.get_cache_metadata(hospital_id)
        hospital_name = meta.get("hospital_name", hospital_id)

        # Fetch GPCI data for the locality (or national default)
        gpci_data = await benchmark_client.get_locality_gpci(
            locality if locality else None
        )
        locality_used = (
            gpci_data.get("locality_name", locality) if gpci_data else locality
        )

        # Get hospital's own rate stats for median rates
        hospital_stats = mrf_processor.get_rate_stats(hospital_id, cpt_codes)
        stats_by_code = {s["cpt_code"]: s for s in hospital_stats}

        # Get cross-hospital peer rates for percentile computation
        peer_rates_raw = mrf_processor.get_cross_hospital_rates(cpt_codes)
        # Group peer median rates by CPT code (one median per hospital)
        peer_medians_by_code: dict[str, list[float]] = {}
        # First group raw rates by (cpt_code, hospital_id)
        hosp_rates: dict[str, dict[str, list[float]]] = {}
        for r in peer_rates_raw:
            code = r.get("cpt_code", "")
            hid = r.get("hospital_id", "")
            dollar = r.get("negotiated_dollar")
            if code and hid and dollar is not None:
                hosp_rates.setdefault(code, {}).setdefault(hid, []).append(
                    float(dollar)
                )
        # Compute median per hospital per code
        for code, hospitals_map in hosp_rates.items():
            medians: list[float] = []
            for hid, vals in hospitals_map.items():
                medians.append(_stats.median(vals))
            peer_medians_by_code[code] = medians

        benchmarks = []
        for code in cpt_codes:
            stat = stats_by_code.get(code, {})
            hospital_median = stat.get("median_rate")
            description = stat.get("description", "")

            # Medicare PFS allowed amount -- no locality param on get_pfs_rate
            pfs_data = await benchmark_client.get_pfs_rate(code)
            medicare_allowed = None
            if pfs_data:
                medicare_allowed = benchmark_client.calculate_medicare_allowed(
                    pfs_data, gpci_data
                )

            # Percentage of Medicare
            pct_of_medicare = None
            if hospital_median is not None and medicare_allowed:
                pct_of_medicare = round(
                    (hospital_median / medicare_allowed) * 100, 1
                )

            # Utilization data for actual average payment
            util_data = await benchmark_client.get_utilization_data(code)
            medicare_actual_avg = None
            if util_data:
                medicare_actual_avg = util_data.get("avg_medicare_payment")

            # Peer percentiles
            peer_medians = peer_medians_by_code.get(code, [])
            peer_pcts = benchmark_client.compute_peer_percentiles(peer_medians)
            peer_rank = None
            if hospital_median is not None and peer_medians:
                peer_rank = benchmark_client.compute_percentile_rank(
                    hospital_median, peer_medians
                )

            benchmarks.append(BenchmarkComparison(
                cpt_code=code,
                description=description,
                hospital_median_rate=hospital_median,
                medicare_allowed_amount=medicare_allowed,
                pct_of_medicare=pct_of_medicare,
                medicare_actual_avg_payment=medicare_actual_avg,
                peer_percentile=peer_rank,
                peer_25th=peer_pcts.get("p25"),
                peer_50th=peer_pcts.get("p50"),
                peer_75th=peer_pcts.get("p75"),
                peer_90th=peer_pcts.get("p90"),
                peer_hospital_count=peer_pcts.get("count", 0),
            ))

        response = BenchmarkResponse(
            hospital_name=hospital_name,
            hospital_id=hospital_id,
            locality=locality_used,
            benchmarks=benchmarks,
        )
        return json.dumps(response.model_dump())
    except Exception as e:
        logger.exception("benchmark_rates failed")
        return json.dumps({"error": f"benchmark_rates failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
