"""Benchmark data client — CMS Physician Fee Schedule, Medicare utilization, and peer percentiles.

Provides three benchmark sources for comparing hospital negotiated rates:
1. CMS Physician Fee Schedule (PFS) — RVU-based Medicare allowed amounts
2. CMS Medicare Provider Utilization — national average charges/payments by HCPCS
3. Cross-hospital peer percentiles — computed from a list of observed rates
"""

import logging
import statistics

import httpx

from shared.utils.http_client import resilient_request, get_client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# CMS PFS API (Physician Fee Schedule)
PFS_BASE = "https://pfs.data.cms.gov/api/1/datastore/query"
PFS_INDICATORS_DATASET = "7c7df311-5315-4f38-b9ed-fd62f8bebe11"  # 2026 indicators
PFS_LOCALITIES_DATASET = "81f942b8-3f6c-4b36-a151-0888376d9ca0"  # 2026 localities
CONVERSION_FACTOR_2026 = 33.4009

# CMS Provider Utilization API (data-api v1, GET-based)
UTILIZATION_BASE = "https://data.cms.gov/data-api/v1/dataset"
UTILIZATION_DATASET = "92396110-2aed-4d63-a6a2-5d6207d46a29"  # Medicare Physician & Other Practitioners
UTILIZATION_PAGE_SIZE = 1000

# Default locality (national average)
DEFAULT_LOCALITY = "0000000"


# ---------------------------------------------------------------------------
# Source 1: CMS Physician Fee Schedule (PFS)
# ---------------------------------------------------------------------------

async def get_pfs_rate(hcpcs_code: str) -> dict | None:
    """Fetch PFS indicator data for an HCPCS code.

    PFS indicators are national-level (not locality-specific). Use
    get_locality_gpci() separately to apply geographic adjustments.

    Args:
        hcpcs_code: HCPCS/CPT code (e.g. "99213").

    Returns:
        Dict with RVU fields (rvu_work, rvu_mp, full_nfac_pe, full_nfac_total,
        full_fac_total, conv_fact, year, hcpc, modifier, sdesc) or None on failure.
    """
    payload = {
        "conditions": [
            {"property": "hcpc", "value": hcpcs_code.strip(), "operator": "="},
        ],
        "limit": 25,
        "offset": 0,
    }
    url = f"{PFS_BASE}/{PFS_INDICATORS_DATASET}/0"

    try:
        resp = await resilient_request("POST", url, json=payload, timeout=30.0)
        data = resp.json()

        results = data.get("results", [])
        if not results:
            logger.warning("PFS: no results for HCPCS %s", hcpcs_code)
            return None

        # Return first result (modifier-free preferred)
        # Try to find the row with no modifier first
        best = results[0]
        for row in results:
            mod = (row.get("modifier") or "").strip()
            if mod == "" or mod == "0" or mod == "00":
                best = row
                break

        # Coerce numeric fields
        numeric_fields = [
            "rvu_work", "rvu_mp", "full_nfac_pe", "full_nfac_total",
            "full_fac_total", "conv_fact",
        ]
        parsed = {}
        for key, value in best.items():
            if key in numeric_fields:
                parsed[key] = _safe_float(value)
            else:
                parsed[key] = value

        return parsed

    except Exception as e:
        logger.warning("PFS API request failed for HCPCS %s: %s", hcpcs_code, e)
        return None


async def get_locality_gpci(locality: str | None = None) -> dict | None:
    """Fetch GPCI (Geographic Practice Cost Index) values for a Medicare locality.

    Args:
        locality: Medicare locality code. Defaults to national average ("0000000").

    Returns:
        Dict with gpci_work, gpci_pe, gpci_mp or None on failure.
    """
    loc = locality or DEFAULT_LOCALITY
    payload = {
        "conditions": [
            {"property": "locality", "value": loc, "operator": "="},
        ],
        "limit": 5,
        "offset": 0,
    }
    url = f"{PFS_BASE}/{PFS_LOCALITIES_DATASET}/0"

    try:
        resp = await resilient_request("POST", url, json=payload, timeout=30.0)
        data = resp.json()

        results = data.get("results", [])
        if not results:
            logger.warning("PFS localities: no results for locality %s", loc)
            return None

        row = results[0]
        return {
            "locality": row.get("locality", loc),
            "locality_name": row.get("locality_name", ""),
            "gpci_work": _safe_float(row.get("gpci_work")),
            "gpci_pe": _safe_float(row.get("gpci_pe")),
            "gpci_mp": _safe_float(row.get("gpci_mp")),
        }

    except Exception as e:
        logger.warning("PFS localities API failed for locality %s: %s", loc, e)
        return None


def calculate_medicare_allowed(pfs_data: dict, gpci_data: dict | None = None) -> float | None:
    """Calculate the Medicare allowed amount from PFS RVU data.

    With GPCI:
        (Work_RVU * Work_GPCI + PE_RVU * PE_GPCI + MP_RVU * MP_GPCI) * CF

    Without GPCI:
        full_nfac_total * CF

    Args:
        pfs_data: Dict from get_pfs_rate() with RVU fields.
        gpci_data: Optional dict from get_locality_gpci() with GPCI fields.

    Returns:
        Medicare allowed amount in dollars, or None if required data is missing.
    """
    cf = _safe_float(pfs_data.get("conv_fact")) or CONVERSION_FACTOR_2026

    if gpci_data:
        rvu_work = _safe_float(pfs_data.get("rvu_work"))
        rvu_pe = _safe_float(pfs_data.get("full_nfac_pe"))
        rvu_mp = _safe_float(pfs_data.get("rvu_mp"))
        gpci_work = _safe_float(gpci_data.get("gpci_work"))
        gpci_pe = _safe_float(gpci_data.get("gpci_pe"))
        gpci_mp = _safe_float(gpci_data.get("gpci_mp"))

        if (
            rvu_work is not None and rvu_pe is not None and rvu_mp is not None
            and gpci_work is not None and gpci_pe is not None and gpci_mp is not None
        ):
            total_rvu = (rvu_work * gpci_work) + (rvu_pe * gpci_pe) + (rvu_mp * gpci_mp)
            return round(total_rvu * cf, 2)

        logger.warning("Incomplete RVU/GPCI data for Medicare allowed calculation")
        return None

    # Without GPCI — use pre-computed total
    total_rvu = _safe_float(pfs_data.get("full_nfac_total"))
    if total_rvu is not None:
        return round(total_rvu * cf, 2)

    logger.warning("Missing full_nfac_total for Medicare allowed calculation without GPCI")
    return None


# ---------------------------------------------------------------------------
# Source 2: CMS Medicare Provider Utilization Data
# ---------------------------------------------------------------------------

async def get_utilization_data(hcpcs_code: str) -> dict | None:
    """Fetch national-level Medicare utilization data for an HCPCS code.

    Queries the Medicare Physician & Other Practitioners dataset and aggregates
    across all providers to compute national averages.

    Args:
        hcpcs_code: HCPCS/CPT code (e.g. "99213").

    Returns:
        Dict with avg_medicare_allowed, avg_medicare_payment,
        avg_submitted_charge, total_services, provider_count or None on failure.
    """
    url = f"{UTILIZATION_BASE}/{UTILIZATION_DATASET}/data"
    offset = 0
    all_rows: list[dict] = []

    try:
        client = get_client()
        # Paginate to collect provider rows via GET with filter params
        while True:
                params = {
                    "filter[HCPCS_Cd]": hcpcs_code.strip(),
                    "size": UTILIZATION_PAGE_SIZE,
                    "offset": offset,
                }
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                results = resp.json()

                if not isinstance(results, list) or not results:
                    break

                all_rows.extend(results)

                # If we got fewer than page size, we're done
                if len(results) < UTILIZATION_PAGE_SIZE:
                    break

                offset += UTILIZATION_PAGE_SIZE

                # Safety cap: don't pull more than 10k rows
                if len(all_rows) >= 10000:
                    break

        if not all_rows:
            logger.warning("Utilization: no data for HCPCS %s", hcpcs_code)
            return None

        # Aggregate across all providers
        allowed_amounts = []
        payment_amounts = []
        charge_amounts = []
        total_services = 0

        for row in all_rows:
            allowed = _safe_float(row.get("Avg_Mdcr_Alowd_Amt"))
            payment = _safe_float(row.get("Avg_Mdcr_Pymt_Amt"))
            charge = _safe_float(row.get("Avg_Sbmtd_Chrg"))
            services = _safe_float(row.get("Tot_Srvcs"))

            if allowed is not None:
                allowed_amounts.append(allowed)
            if payment is not None:
                payment_amounts.append(payment)
            if charge is not None:
                charge_amounts.append(charge)
            if services is not None:
                total_services += int(services)

        return {
            "hcpcs_code": hcpcs_code,
            "avg_medicare_allowed": round(statistics.mean(allowed_amounts), 2) if allowed_amounts else None,
            "avg_medicare_payment": round(statistics.mean(payment_amounts), 2) if payment_amounts else None,
            "avg_submitted_charge": round(statistics.mean(charge_amounts), 2) if charge_amounts else None,
            "total_services": total_services,
            "provider_count": len(all_rows),
        }

    except Exception as e:
        logger.warning("Utilization API request failed for HCPCS %s: %s", hcpcs_code, e)
        return None


# ---------------------------------------------------------------------------
# Source 3: Cross-Hospital Peer Percentiles
# ---------------------------------------------------------------------------

def compute_peer_percentiles(rates: list[float]) -> dict:
    """Compute percentile distribution from a list of negotiated rates.

    Args:
        rates: List of dollar amounts from peer hospitals.

    Returns:
        Dict with p25, p50, p75, p90, count. Values are None if insufficient data.
    """
    if not rates:
        return {"p25": None, "p50": None, "p75": None, "p90": None, "count": 0}

    sorted_rates = sorted(rates)
    n = len(sorted_rates)

    if n == 1:
        val = sorted_rates[0]
        return {"p25": val, "p50": val, "p75": val, "p90": val, "count": 1}

    return {
        "p25": round(_percentile(sorted_rates, 25), 2),
        "p50": round(_percentile(sorted_rates, 50), 2),
        "p75": round(_percentile(sorted_rates, 75), 2),
        "p90": round(_percentile(sorted_rates, 90), 2),
        "count": n,
    }


def compute_percentile_rank(value: float, rates: list[float]) -> float | None:
    """Compute the percentile rank of a value within a list of rates.

    Args:
        value: The rate to rank.
        rates: List of peer rates to compare against.

    Returns:
        Percentile rank (0-100), or None if rates list is empty.
    """
    if not rates:
        return None

    n = len(rates)
    below = sum(1 for r in rates if r < value)
    equal = sum(1 for r in rates if r == value)

    # Percentile rank using the "mean of inclusive and exclusive" method
    rank = ((below + 0.5 * equal) / n) * 100
    return round(rank, 1)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_float(value) -> float | None:
    """Safely convert a value to float. Returns None for null/empty/invalid."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if value == "" or value.lower() in ("null", "none", "n/a"):
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _percentile(sorted_data: list[float], pct: float) -> float:
    """Compute a percentile from pre-sorted data using linear interpolation.

    Uses the same method as Excel PERCENTILE.INC / numpy's 'linear' interpolation.

    Args:
        sorted_data: Pre-sorted list of floats (ascending).
        pct: Percentile to compute (0-100).

    Returns:
        Interpolated percentile value.
    """
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]

    # Convert percentile to 0-1 scale
    k = pct / 100.0
    # Index in the sorted array (0-based, fractional)
    idx = k * (n - 1)
    floor_idx = int(idx)
    ceil_idx = min(floor_idx + 1, n - 1)
    fraction = idx - floor_idx

    return sorted_data[floor_idx] + fraction * (sorted_data[ceil_idx] - sorted_data[floor_idx])
