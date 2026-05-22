"""Price Transparency / MRF Engine MCP Server.

Provides tools for discovering hospital machine-readable files (MRFs),
querying negotiated rates from cached Parquet, computing rate dispersion
statistics, cross-hospital comparisons, and Medicare benchmark analysis.
"""

from typing import Any
import logging
import os as _os
import statistics as _stats
from datetime import datetime, timezone

from mcp.server.fastmcp import FastMCP
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured

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
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8009"))
mcp = FastMCP(**_mcp_kwargs)


def _registry_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "CMS Hospital Price Transparency MRF Discovery",
        "source_url": mrf_registry.CMS_PROVIDER_API,
        "dataset_id": "cms_hospital_price_transparency_mrf_discovery",
        "source_period": "live CMS Provider Data Catalog plus curated registry",
        "landing_page": "https://data.cms.gov/provider-data/",
        "cache_status": "registry_ready",
        "cache_key": str(mrf_registry._REGISTRY_PATH),
        "source_caveat": (
            "MRF discovery identifies candidate machine-readable file URLs from CMS/catalog and curated registry sources; "
            "it does not prove file completeness or current negotiated-rate availability."
        ),
    }


def _mrf_source_metadata(hospital_id: str, *, mrf_url: str = "", source: str = "parquet_cache") -> dict[str, Any]:
    meta = mrf_processor.get_cache_metadata(hospital_id)
    cached_at = str(meta.get("cached_at") or "")
    last_updated = str(meta.get("last_updated") or "")
    return {
        "source_name": "Hospital Machine-Readable Price Transparency File",
        "source_url": mrf_url or str(meta.get("source_url") or ""),
        "landing_page": str(meta.get("landing_page") or "https://www.cms.gov/priorities/key-initiatives/hospital-price-transparency"),
        "dataset_id": "hospital_price_transparency_mrf_cache",
        "source_period": last_updated or cached_at or "MRF cache readiness checked at request time",
        "retrieved_at": cached_at or datetime.now(timezone.utc).isoformat(),
        "cache_status": "ready" if mrf_processor.is_cached(hospital_id) else "missing",
        "cache_freshness": cached_at or source,
        "cache_key": hospital_id,
        "source_caveat": (
            "Hospital price-transparency MRF rates are source-file records that may be incomplete, stale, payer-specific, "
            "or difficult to compare across hospitals without normalization and source review."
        ),
    }


def _benchmark_source_metadata() -> dict[str, Any]:
    return {
        "source_name": "CMS PFS, Medicare Utilization, and Local MRF Peer Cache",
        "source_url": benchmark_client.PFS_BASE,
        "dataset_id": "price_transparency_medicare_benchmark_workflow",
        "source_period": "2026 PFS and configured Medicare utilization source",
        "cache_status": "mixed_live_and_local_cache",
        "source_caveat": (
            "Benchmarking combines Medicare public benchmark data with locally cached hospital MRF peer rates; "
            "missing cache rows or locality assumptions can change comparisons."
        ),
    }


def _price_evidence(
    source_metadata: dict[str, Any],
    *,
    dataset_id: str = "",
    entity_scope: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=source_metadata,
        dataset_id=dataset_id,
        entity_scope=entity_scope,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _price_row_evidence(
    source_metadata: dict[str, Any],
    *,
    dataset_id: str = "",
    entity_scope: str,
    parent_query: dict[str, Any],
    row: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
) -> dict[str, Any]:
    row_query = {
        **parent_query,
        "hospital_id": row.get("hospital_id") or parent_query.get("hospital_id") or "",
        "hospital_name": row.get("hospital_name") or "",
        "cpt_code": row.get("cpt_code") or "",
        "description": row.get("description") or "",
        "payer_name": row.get("payer_name") or "",
        "plan_name": row.get("plan_name") or "",
        "methodology": row.get("methodology") or "",
        "peer_hospital_count": row.get("peer_hospital_count") or "",
        "locality": parent_query.get("locality") or "",
    }
    return _price_evidence(
        source_metadata,
        dataset_id=dataset_id,
        entity_scope=entity_scope,
        query={key: value for key, value in row_query.items() if value not in ("", None, [], {})},
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _hospital_identity(record: dict[str, Any]) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=record.get("hospital_name") or record.get("name") or "",
        entity_type="facility",
        ccn=record.get("ccn") or record.get("hospital_id") or "",
        source_name="Hospital price transparency public files",
        source_url=(record.get("mrf_urls") or [{}])[0].get("source_page_url", "") if isinstance(record.get("mrf_urls"), list) else "",
    ).to_dict()
    ein = str(record.get("ein") or "").strip()
    if ein:
        identity["unresolved_identifiers"].append({"type": "ein", "value": ein})
    city = record.get("city")
    state = record.get("state")
    if city:
        identity["city"] = str(city)
    if state:
        identity["state"] = str(state).upper()
    return identity


def _hospital_identity_map(records: list[dict[str, Any]], *, match_basis: str) -> dict[str, Any]:
    return {
        "entities": [_hospital_identity(record) for record in records],
        "match_basis": match_basis,
        "conflict_policy": "Join hospital price data by exact CCN where available; EIN and names are source-specific context unless independently reconciled.",
    }


def _cached_hospital_records(hospital_ids: list[str]) -> list[dict[str, Any]]:
    records = []
    for hospital_id in hospital_ids:
        meta = mrf_processor.get_cache_metadata(hospital_id)
        records.append(
            {
                "hospital_id": hospital_id,
                "ccn": hospital_id if hospital_id.isdigit() and len(hospital_id) == 6 else "",
                "hospital_name": meta.get("hospital_name", hospital_id),
            }
        )
    return records


# ---------------------------------------------------------------------------
# Tool 1: search_mrf_index
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def search_mrf_index(query: str, state: str = "") -> dict[str, Any]:
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
            cache_status = "missing"
            cache_date = ""
            row_count = None
            if hospital_id:
                cached = mrf_processor.is_cached(hospital_id)
                if cached:
                    cache_status = "ready"
                    meta = mrf_processor.get_cache_metadata(hospital_id)
                    cache_date = meta.get("cached_at", "")
                    row_count = meta.get("row_count")

            results.append(MRFIndexResult(
                hospital_name=hosp.get("name", ""),
                hospital_id=hospital_id,
                ccn=ccn,
                ein=hosp.get("ein", ""),
                city=hosp.get("city", ""),
                state=hosp.get("state", ""),
                mrf_urls=[MRFLocation(**u) for u in hosp.get("mrf_urls", [])],
                cached=cached,
                cache_status=cache_status,
                cache_date=cache_date,
                row_count=row_count,
            ).model_dump())

        payload = to_structured({"total_results": len(results), "hospitals": results})
        source_metadata = _registry_source_metadata()
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _price_evidence(
            source_metadata,
            entity_scope="hospital_mrf_discovery",
            query={"query": query, "state": state},
            match_basis="ccn_ein_or_name_mrf_registry_search",
            confidence="candidate_mrf_urls_require_file_validation",
            caveat=source_metadata["source_caveat"],
            next_step="Use a selected hospital_id and MRF URL with get_negotiated_rates to validate cached rates before citing prices.",
        )
        payload["identity_map"] = _hospital_identity_map(results, match_basis="mrf_registry_candidate_hospitals")
        return payload
    except Exception as e:
        logger.exception("search_mrf_index failed")
        return error_response(f"search_mrf_index failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_negotiated_rates
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def get_negotiated_rates(
    hospital_id: str,
    cpt_codes: list[str],
    payer: str = "",
    mrf_url: str = "",
) -> dict[str, Any]:
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
                return error_response(
                    f"No cached data for hospital '{hospital_id}'. "
                    "Use search_mrf_index to find the MRF URL, then pass it "
                    "as mrf_url to trigger automatic download and processing.",
                    source_metadata=_mrf_source_metadata(hospital_id, mrf_url=mrf_url),
                    evidence=_price_evidence(
                        _mrf_source_metadata(hospital_id, mrf_url=mrf_url),
                        entity_scope="hospital_mrf_rates",
                        query={"hospital_id": hospital_id, "cpt_codes": cpt_codes, "payer": payer, "mrf_url": mrf_url},
                        match_basis="mrf_cache_readiness_check",
                        confidence="data_unavailable_until_mrf_cached",
                        caveat=_mrf_source_metadata(hospital_id, mrf_url=mrf_url)["source_caveat"],
                        next_step="Pass a validated mrf_url or process the hospital MRF before querying negotiated rates.",
                    ),
                )

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
        payload = to_structured(response.model_dump())
        source_metadata = _mrf_source_metadata(hospital_id, mrf_url=mrf_url, source=source)
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _price_evidence(
            source_metadata,
            entity_scope="hospital_negotiated_rates",
            query={"hospital_id": hospital_id, "cpt_codes": cpt_codes, "payer": payer, "mrf_url": mrf_url},
            match_basis="hospital_id_exact_mrf_cache_cpt_filter",
            confidence="source_backed_cached_mrf_rows" if rates else "no_matching_mrf_rows",
            caveat=source_metadata["source_caveat"],
            next_step="Preserve hospital_id, CPT/HCPCS code, payer/plan, methodology, and cache metadata with cited rates.",
        )
        for rate in payload["rates"]:
            rate["evidence"] = _price_row_evidence(
                source_metadata,
                entity_scope="hospital_negotiated_rate_row",
                parent_query={"hospital_id": hospital_id, "cpt_codes": cpt_codes, "payer": payer},
                row=rate,
                match_basis="hospital_mrf_negotiated_rate_row",
                confidence="source_backed_cached_mrf_row",
                caveat=source_metadata["source_caveat"],
                next_step="Cite this negotiated-rate row only with its hospital_id, CPT/HCPCS code, payer/plan, methodology, and MRF cache metadata.",
            )
        payload["identity"] = _hospital_identity({"hospital_id": hospital_id, "hospital_name": hospital_name})
        return payload
    except Exception as e:
        logger.exception("get_negotiated_rates failed")
        return error_response(f"get_negotiated_rates failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: compute_rate_dispersion
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def compute_rate_dispersion(hospital_id: str, cpt_codes: list[str]) -> dict[str, Any]:
    """Compute rate dispersion statistics for CPT codes across payers at a hospital.

    Shows min, max, median, mean, IQR, coefficient of variation, and standard
    deviation of negotiated dollar amounts for each requested CPT code.

    Args:
        hospital_id: Hospital identifier (CCN or filesystem-safe ID).
        cpt_codes: List of CPT/HCPCS codes to analyze.
    """
    try:
        if not mrf_processor.is_cached(hospital_id):
            source_metadata = _mrf_source_metadata(hospital_id)
            return error_response(
                f"No cached data for hospital '{hospital_id}'. "
                "Use get_negotiated_rates with an mrf_url first.",
                source_metadata=source_metadata,
                evidence=_price_evidence(
                    source_metadata,
                    entity_scope="hospital_rate_dispersion",
                    query={"hospital_id": hospital_id, "cpt_codes": cpt_codes},
                    match_basis="mrf_cache_readiness_check",
                    confidence="data_unavailable_until_mrf_cached",
                    caveat=source_metadata["source_caveat"],
                    next_step="Cache the hospital MRF before computing dispersion statistics.",
                ),
            )

        raw_stats = mrf_processor.get_rate_stats(hospital_id, cpt_codes)

        results = [RateDispersion(**s).model_dump() for s in raw_stats]
        payload = to_structured({"hospital_id": hospital_id, "dispersion": results})
        source_metadata = _mrf_source_metadata(hospital_id)
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _price_evidence(
            source_metadata,
            entity_scope="hospital_rate_dispersion",
            query={"hospital_id": hospital_id, "cpt_codes": cpt_codes},
            match_basis="hospital_id_exact_mrf_cache_rate_distribution",
            confidence="source_backed_cached_mrf_statistics" if results else "no_matching_mrf_rows",
            caveat=source_metadata["source_caveat"],
            next_step="Inspect payer_count and code-level row coverage before comparing rate spread across hospitals.",
        )
        for dispersion in payload["dispersion"]:
            dispersion["evidence"] = _price_row_evidence(
                source_metadata,
                entity_scope="hospital_rate_dispersion_row",
                parent_query={"hospital_id": hospital_id, "cpt_codes": cpt_codes},
                row=dispersion,
                match_basis="hospital_mrf_rate_dispersion_row",
                confidence="aggregate_from_cached_mrf_rate_rows",
                caveat=source_metadata["source_caveat"],
                next_step="Review payer_count and underlying negotiated-rate row coverage before citing dispersion statistics.",
            )
        payload["identity"] = _hospital_identity({"hospital_id": hospital_id, "hospital_name": source_metadata.get("hospital_name", hospital_id)})
        return payload
    except Exception as e:
        logger.exception("compute_rate_dispersion failed")
        return error_response(f"compute_rate_dispersion failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: compare_rates_system
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def compare_rates_system(system_name: str, cpt_codes: list[str]) -> dict[str, Any]:
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
            return error_response(
                f"No cached hospitals match system name '{system_name}'. "
                f"Found {len(all_hospitals)} cached hospitals total."
            )

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
        payload = to_structured(response.model_dump())
        source_metadata = _mrf_source_metadata("system_comparison")
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _price_evidence(
            source_metadata,
            dataset_id="hospital_price_transparency_system_comparison",
            entity_scope="health_system_cached_mrf_rate_comparison",
            query={"system_name": system_name, "cpt_codes": cpt_codes},
            match_basis="cached_hospital_name_contains_system_name",
            confidence="candidate_system_name_cache_match",
            caveat=(
                "System comparisons use locally cached hospital MRFs whose names contain the requested text; "
                "this is a candidate system grouping unless exact facility/system identity has been reconciled."
            ),
            next_step="Review matched hospitals and reconcile facility CCNs before treating results as a system-level comparison.",
        )
        payload["identity_map"] = _hospital_identity_map(
            _cached_hospital_records([hospital.hospital_id for hospital in hospitals]),
            match_basis="cached_mrf_hospital_name_system_match",
        )
        for hospital in payload["hospitals"]:
            hospital_metadata = _mrf_source_metadata(str(hospital.get("hospital_id") or ""))
            hospital["evidence"] = _price_row_evidence(
                hospital_metadata,
                dataset_id="hospital_price_transparency_system_comparison",
                entity_scope="system_comparison_hospital_row",
                parent_query={"system_name": system_name, "cpt_codes": cpt_codes},
                row=hospital,
                match_basis="cached_mrf_system_comparison_hospital_row",
                confidence="candidate_system_name_cache_match",
                caveat=(
                    "Hospital rows in system comparisons are selected by cached hospital name text; "
                    "reconcile CCNs before citing as a system roster."
                ),
                next_step="Review the hospital CCN/name and source cache metadata before using this row as system comparison evidence.",
            )
            for rate in hospital.get("rates") or []:
                rate["evidence"] = _price_row_evidence(
                    hospital_metadata,
                    dataset_id="hospital_price_transparency_system_comparison",
                    entity_scope="system_comparison_negotiated_rate_row",
                    parent_query={
                        "system_name": system_name,
                        "hospital_id": hospital.get("hospital_id") or "",
                        "cpt_codes": cpt_codes,
                    },
                    row=rate,
                    match_basis="cached_mrf_system_comparison_rate_row",
                    confidence="source_backed_cached_mrf_row",
                    caveat=source_metadata["source_caveat"],
                    next_step="Cite this system-comparison rate only with its hospital_id, CPT/HCPCS code, payer/plan, and MRF cache metadata.",
                )
        return payload
    except Exception as e:
        logger.exception("compare_rates_system failed")
        return error_response(f"compare_rates_system failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: benchmark_rates
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
async def benchmark_rates(
    hospital_id: str,
    cpt_codes: list[str],
    locality: str = "",
) -> dict[str, Any]:
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
            source_metadata = _mrf_source_metadata(hospital_id)
            return error_response(
                f"No cached data for hospital '{hospital_id}'. "
                "Use get_negotiated_rates with an mrf_url first.",
                source_metadata=source_metadata,
                evidence=_price_evidence(
                    source_metadata,
                    entity_scope="hospital_price_benchmark",
                    query={"hospital_id": hospital_id, "cpt_codes": cpt_codes, "locality": locality},
                    match_basis="mrf_cache_readiness_check",
                    confidence="data_unavailable_until_mrf_cached",
                    caveat=source_metadata["source_caveat"],
                    next_step="Cache the hospital MRF before benchmarking rates against Medicare or peers.",
                ),
            )

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
        payload = to_structured(response.model_dump())
        source_metadata = _benchmark_source_metadata()
        payload["source_metadata"] = {"sources": [_mrf_source_metadata(hospital_id), source_metadata]}
        payload["evidence"] = _price_evidence(
            source_metadata,
            entity_scope="hospital_price_benchmark",
            query={"hospital_id": hospital_id, "cpt_codes": cpt_codes, "locality": locality},
            match_basis="hospital_mrf_median_rates_plus_medicare_pfs_and_peer_cache",
            confidence="benchmark_context_requires_cache_and_locality_review",
            caveat=source_metadata["source_caveat"],
            next_step="Review Medicare locality, peer_hospital_count, missing benchmark fields, and source cache coverage before citing benchmark ratios.",
        )
        for benchmark in payload["benchmarks"]:
            benchmark["evidence"] = _price_row_evidence(
                source_metadata,
                entity_scope="price_benchmark_cpt_row",
                parent_query={"hospital_id": hospital_id, "cpt_codes": cpt_codes, "locality": locality},
                row=benchmark,
                match_basis="hospital_mrf_medicare_peer_benchmark_row",
                confidence="benchmark_context_requires_cache_and_locality_review",
                caveat=source_metadata["source_caveat"],
                next_step="Review Medicare locality, peer_hospital_count, and missing benchmark fields before citing this CPT benchmark row.",
            )
        payload["identity"] = _hospital_identity({"hospital_id": hospital_id, "hospital_name": hospital_name})
        return payload
    except Exception as e:
        logger.exception("benchmark_rates failed")
        return error_response(f"benchmark_rates failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
