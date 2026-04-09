"""BLS OES API v2 client for occupation employment and wage data.

Bureau of Labor Statistics Occupational Employment and Wage Statistics.
API docs: https://www.bls.gov/developers/api_signature_v2.htm
"""

import logging
import os
from pathlib import Path


from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_API_KEY = os.environ.get("BLS_API_KEY", "")

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "workforce"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Healthcare occupation SOC codes (2018 SOC)
HEALTHCARE_SOCS: dict[str, str] = {
    "registered nurses": "291141",
    "nurse practitioners": "291171",
    "physicians": "291210",
    "pharmacists": "291051",
    "physical therapists": "291123",
    "occupational therapists": "291122",
    "respiratory therapists": "291126",
    "medical assistants": "319092",
    "nursing assistants": "311014",
    "home health aides": "311121",
    "medical and health services managers": "119111",
    "licensed practical nurses": "292061",
    "dental hygienists": "292021",
    "radiologic technologists": "292034",
    "clinical laboratory technologists": "292010",
    "emergency medical technicians": "292042",
    "surgeons": "291248",
    "anesthesiologists": "291211",
    "psychiatrists": "291223",
    "dentists": "291020",
}

# BLS OES data type codes
DATATYPE_EMPLOYMENT = "01"
DATATYPE_MEAN_WAGE = "04"
DATATYPE_MEDIAN_WAGE = "13"
DATATYPE_PCT10_WAGE = "07"
DATATYPE_PCT90_WAGE = "11"


def _resolve_soc(occupation: str) -> str | None:
    """Resolve an occupation name or SOC code to a 6-digit SOC code."""
    clean = occupation.strip()

    # Already a SOC code (e.g. "29-1141" or "291141")
    digits = clean.replace("-", "")
    if digits.isdigit() and len(digits) == 6:
        return digits

    # Lookup by name
    key = clean.lower()
    if key in HEALTHCARE_SOCS:
        return HEALTHCARE_SOCS[key]

    # Partial match
    for name, soc in HEALTHCARE_SOCS.items():
        if key in name or name in key:
            return soc

    return None


def _build_series_id(
    soc6: str,
    area_code: str = "0000000",
    industry: str = "000000",
    datatype: str = "01",
) -> str:
    """Build a BLS OES series ID.

    Format: OE U N {area7} {industry6} {soc6} {datatype2}
    """
    return f"OEUN{area_code}{industry}{soc6}{datatype}"


def _state_to_area_code(state: str) -> str:
    """Convert a 2-letter state code to a BLS area code (FIPS + 000)."""
    # State FIPS codes
    fips: dict[str, str] = {
        "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
        "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
        "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
        "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
        "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
        "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
        "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
        "OH": "39", "OK": "40", "OR": "41", "PA": "42", "PR": "72",
        "RI": "44", "SC": "45", "SD": "46", "TN": "47", "TX": "48",
        "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
        "WI": "55", "WY": "56",
    }
    code = fips.get(state.upper(), "")
    return f"{code}00000" if code else "0000000"


async def get_oes_data(
    occupation: str,
    area_code: str = "",
    state: str = "",
    start_year: int = 2020,
    end_year: int = 2024,
) -> dict | None:
    """Query BLS OES API for employment and wage data.

    Args:
        occupation: Occupation name or SOC code.
        area_code: BLS area code (MSA FIPS, state FIPS+000, or "" for national).
        state: Two-letter state code (alternative to area_code).
        start_year: Start year for data.
        end_year: End year for data.

    Returns:
        Dict with employment, wages, and metadata, or None on failure.
    """
    if not BLS_API_KEY:
        return {"error": "BLS_API_KEY environment variable not set. Register free at https://data.bls.gov/registrationEngine/"}

    soc6 = _resolve_soc(occupation)
    if not soc6:
        return {"error": f"Could not resolve occupation '{occupation}' to SOC code. Try a specific name like 'Registered Nurses' or a SOC code like '29-1141'."}

    # Resolve area code
    if not area_code and state:
        area_code = _state_to_area_code(state)
    elif not area_code:
        area_code = "0000000"  # National

    # Build series IDs for all data types
    series_ids = [
        _build_series_id(soc6, area_code, datatype=DATATYPE_EMPLOYMENT),
        _build_series_id(soc6, area_code, datatype=DATATYPE_MEAN_WAGE),
        _build_series_id(soc6, area_code, datatype=DATATYPE_MEDIAN_WAGE),
        _build_series_id(soc6, area_code, datatype=DATATYPE_PCT10_WAGE),
        _build_series_id(soc6, area_code, datatype=DATATYPE_PCT90_WAGE),
    ]

    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "annualaverage": True,
        "registrationkey": BLS_API_KEY,
    }

    try:
        resp = await resilient_request("POST", BLS_API_URL, json=payload, timeout=30.0)
        data = resp.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            msg = "; ".join(data.get("message", []))
            return {"error": f"BLS API error: {msg}"}

        # Parse results by series
        result: dict = {
            "soc_code": f"{soc6[:2]}-{soc6[2:]}",
            "area_code": area_code,
            "data_year": str(end_year),
        }

        for series in data.get("Results", {}).get("series", []):
            sid = series.get("seriesID", "")
            datatype = sid[-2:]  # Last 2 chars = datatype code
            series_data = series.get("data", [])

            # Get the most recent annual average
            annual = [d for d in series_data if d.get("period") == "M13"]
            if not annual:
                annual = series_data[:1]

            if annual:
                val = annual[0].get("value", "0").replace(",", "")
                try:
                    num = float(val)
                except ValueError:
                    num = 0.0

                if datatype == DATATYPE_EMPLOYMENT:
                    result["employment"] = int(num * 1000) if num < 100000 else int(num)
                    result["data_year"] = annual[0].get("year", str(end_year))
                elif datatype == DATATYPE_MEAN_WAGE:
                    result["mean_wage"] = num
                elif datatype == DATATYPE_MEDIAN_WAGE:
                    result["median_wage"] = num
                elif datatype == DATATYPE_PCT10_WAGE:
                    result["pct_10_wage"] = num
                elif datatype == DATATYPE_PCT90_WAGE:
                    result["pct_90_wage"] = num

        # Resolve occupation title from SOC mapping
        for name, code in HEALTHCARE_SOCS.items():
            if code == soc6:
                result["occupation_title"] = name.title()
                break

        return result

    except Exception as e:
        logger.warning("BLS OES query failed: %s", e)
        return {"error": f"BLS API request failed: {e}"}
