"""Census ACS API wrapper for querying demographic data by ZCTA."""

import logging
import os

from shared.utils.http_client import resilient_request

logger = logging.getLogger(__name__)

CENSUS_BASE = "https://api.census.gov/data"

# ACS 5-Year variable mapping
# B01003: Total Population
# B01001: Sex by Age
# B01002: Median Age by Sex
# B19013: Median Household Income
# B27010: Types of Health Insurance Coverage by Age

# Flat variables (single value)
SINGLE_VARS = {
    "total_pop": "B01003_001E",
    "median_age": "B01002_001E",
    "male_total": "B01001_002E",
    "female_total": "B01001_026E",
    "median_income": "B19013_001E",
}

# Age group variables: under 18 (male + female)
# B01001_003E = Male under 5
# B01001_004E = Male 5-9
# B01001_005E = Male 10-14
# B01001_006E = Male 15-17
# B01001_027E = Female under 5
# B01001_028E = Female 5-9
# B01001_029E = Female 10-14
# B01001_030E = Female 15-17
UNDER_18_VARS = [
    "B01001_003E", "B01001_004E", "B01001_005E", "B01001_006E",
    "B01001_027E", "B01001_028E", "B01001_029E", "B01001_030E",
]

# 65+ (male)
# B01001_020E = Male 65-66, _021E = 67-69, _022E = 70-74, _023E = 75-79, _024E = 80-84, _025E = 85+
# 65+ (female)
# B01001_044E = Female 65-66, _045E = 67-69, _046E = 70-74, _047E = 75-79, _048E = 80-84, _049E = 85+
OVER_65_VARS = [
    "B01001_020E", "B01001_021E", "B01001_022E", "B01001_023E", "B01001_024E", "B01001_025E",
    "B01001_044E", "B01001_045E", "B01001_046E", "B01001_047E", "B01001_048E", "B01001_049E",
]

# Insurance variables from B27010 (ages 19-64 civilian noninstitutionalized)
# B27010_003E = With employer-based (under 19)
# B27010_017E = With private health insurance (19-34)
# We use a simplified approach: total with private, total with public, total uninsured
# B27010 table structure is complex. Use B27001 for simpler breakdown:
# Actually let's use the main summary:
#   B27010_002E = Under 19: With one type
#   Complex table — simplify to B27001 or use S2701 subject table
# Better approach: use B27001 (Health Insurance Coverage Status by Age)
#   B27001_001E = Total civilian noninstitutionalized
#   B27001_004E = Under 19 with health insurance
#   B27001_005E = Under 19 no health insurance
# Actually B27010 is what was requested. Let's map it properly:
#   B27010_001E = Total
#   B27010_002E = Under 19
#   B27010_003E = Under 19, with one type of coverage
#   B27010_004E = Under 19, with one type, employer-based alone
#   ...
# This is deeply nested. Better to use:
#   C27006 (Medicare coverage by age) and C27007 (Medicaid coverage by age)
# For simplicity and reliability, use these key variables:
INSURANCE_VARS = {
    # From B27010: Types of Health Insurance Coverage by Age
    "ins_total": "B27010_001E",             # Total population for insurance universe
    "ins_private_under19": "B27010_004E",    # Under 19: employer-based alone
    "ins_private_19_34": "B27010_020E",      # 19-34: employer-based alone
    "ins_private_35_64": "B27010_036E",      # 35-64: employer-based alone
    "ins_private_65plus": "B27010_052E",     # 65+: employer-based alone
    "ins_medicare_under19": "B27010_007E",   # Under 19: Medicare alone (rare)
    "ins_medicare_19_34": "B27010_023E",     # 19-34: Medicare alone
    "ins_medicare_35_64": "B27010_039E",     # 35-64: Medicare alone
    "ins_medicare_65plus": "B27010_055E",    # 65+: Medicare alone
    "ins_medicaid_under19": "B27010_009E",   # Under 19: Medicaid/means-tested alone
    "ins_medicaid_19_34": "B27010_025E",     # 19-34: Medicaid alone
    "ins_medicaid_35_64": "B27010_041E",     # 35-64: Medicaid alone
    "ins_medicaid_65plus": "B27010_057E",    # 65+: Medicaid alone
    "ins_none_under19": "B27010_017E",       # Under 19: no insurance
    "ins_none_19_34": "B27010_033E",         # 19-34: no insurance
    "ins_none_35_64": "B27010_050E",         # 35-64: no insurance
    "ins_none_65plus": "B27010_066E",        # 65+: no insurance
}


def _all_variable_codes() -> list[str]:
    """Return all Census variable codes we need to query."""
    codes = list(SINGLE_VARS.values())
    codes.extend(UNDER_18_VARS)
    codes.extend(OVER_65_VARS)
    codes.extend(INSURANCE_VARS.values())
    return list(dict.fromkeys(codes))  # deduplicate preserving order


def _safe_int(value: str | None) -> int:
    """Safely convert a Census API value to int. Returns 0 for null/negative."""
    if value is None or value == "" or value == "null":
        return 0
    try:
        v = int(float(value))
        return max(v, 0)
    except (ValueError, TypeError):
        return 0


def _safe_float(value: str | None) -> float | None:
    """Safely convert a Census API value to float. Returns None for null/missing."""
    if value is None or value == "" or value == "null":
        return None
    try:
        v = float(value)
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


def parse_demographics(row: dict[str, str], zcta: str, year: int) -> dict:
    """Parse a single row of Census ACS data into a demographics dict."""
    total_pop = _safe_int(row.get("B01003_001E"))

    # Age groups
    under_18 = sum(_safe_int(row.get(v)) for v in UNDER_18_VARS)
    over_65 = sum(_safe_int(row.get(v)) for v in OVER_65_VARS)
    age_18_to_64 = max(total_pop - under_18 - over_65, 0)

    # Insurance
    private = sum(
        _safe_int(row.get(INSURANCE_VARS[k]))
        for k in ["ins_private_under19", "ins_private_19_34", "ins_private_35_64", "ins_private_65plus"]
    )
    medicare = sum(
        _safe_int(row.get(INSURANCE_VARS[k]))
        for k in ["ins_medicare_under19", "ins_medicare_19_34", "ins_medicare_35_64", "ins_medicare_65plus"]
    )
    medicaid = sum(
        _safe_int(row.get(INSURANCE_VARS[k]))
        for k in ["ins_medicaid_under19", "ins_medicaid_19_34", "ins_medicaid_35_64", "ins_medicaid_65plus"]
    )
    uninsured = sum(
        _safe_int(row.get(INSURANCE_VARS[k]))
        for k in ["ins_none_under19", "ins_none_19_34", "ins_none_35_64", "ins_none_65plus"]
    )
    ins_total = _safe_int(row.get("B27010_001E"))
    uninsured_pct = round((uninsured / ins_total * 100), 1) if ins_total > 0 else 0.0

    return {
        "zcta": zcta,
        "year": year,
        "total_population": total_pop,
        "median_age": _safe_float(row.get("B01002_001E")),
        "male_population": _safe_int(row.get("B01001_002E")),
        "female_population": _safe_int(row.get("B01001_026E")),
        "age_distribution": {
            "under_18": under_18,
            "age_18_to_64": age_18_to_64,
            "age_65_plus": over_65,
        },
        "median_household_income": _safe_int(row.get("B19013_001E")) or None,
        "insurance": {
            "private": private,
            "public_medicare": medicare,
            "public_medicaid": medicaid,
            "uninsured": uninsured,
            "uninsured_pct": uninsured_pct,
        },
    }


async def query_acs(
    variables: list[str],
    zcta: str = "*",
    year: int = 2023,
    api_key: str | None = None,
) -> list[dict[str, str]]:
    """Query Census ACS 5-Year API for ZCTA-level data.

    Args:
        variables: List of Census variable codes to retrieve.
        zcta: ZCTA code or "*" for all ZCTAs.
        year: ACS year (default 2023).
        api_key: Census API key (optional but recommended).

    Returns:
        List of dicts mapping variable names to string values.
    """
    if api_key is None:
        api_key = os.environ.get("CENSUS_API_KEY")

    url = f"{CENSUS_BASE}/{year}/acs/acs5"
    var_str = ",".join(["NAME"] + variables)
    params: dict[str, str] = {
        "get": var_str,
        "for": f"zip code tabulation area:{zcta}",
    }
    if api_key:
        params["key"] = api_key

    resp = await resilient_request("GET", url, params=params, timeout=60.0)
    data = resp.json()

    # First row is header, rest is data
    if len(data) < 2:
        return []

    headers = data[0]
    return [dict(zip(headers, row)) for row in data[1:]]


async def get_demographics_for_zcta(zcta: str, year: int = 2023) -> dict:
    """Fetch and parse demographics for a single ZCTA."""
    variables = _all_variable_codes()
    rows = await query_acs(variables, zcta=zcta, year=year)
    if not rows:
        return {"zcta": zcta, "year": year, "error": f"No data found for ZCTA {zcta}"}
    return parse_demographics(rows[0], zcta, year)


async def get_demographics_batch(zctas: list[str], year: int = 2023) -> list[dict]:
    """Fetch demographics for multiple ZCTAs efficiently.

    Queries all ZCTAs at once and filters to the requested set.
    For small batches (<=10), uses comma-separated query.
    For larger batches, queries all ZCTAs and filters.
    """
    variables = _all_variable_codes()
    zcta_set = set(zctas)

    if len(zctas) <= 10:
        # Census API supports comma-separated ZCTA values
        zcta_param = ",".join(zctas)
        rows = await query_acs(variables, zcta=zcta_param, year=year)
    else:
        # Query all ZCTAs and filter
        rows = await query_acs(variables, zcta="*", year=year)

    results = []
    for row in rows:
        row_zcta = row.get("zip code tabulation area", "")
        if row_zcta in zcta_set:
            results.append(parse_demographics(row, row_zcta, year))

    return results
