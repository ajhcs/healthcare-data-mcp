"""CMS Provider Data Catalog tools.

Fetches hospital quality data from data.cms.gov:
- General info & star ratings
- Complications & deaths (mortality)
- Readmissions
- Patient satisfaction (HCAHPS)
- Medicare spending per beneficiary
"""

import httpx
from typing import Any

from tools.validation import validate_facility_id

BASE_URL = "https://data.cms.gov/provider-data/api/1/datastore/query"

# Dataset IDs on data.cms.gov
DATASETS = {
    "general_info": "xubh-q36u",
    "complications_deaths": "ynj2-r877",
    "readmissions": "9n3s-kdb3",
    "patient_satisfaction": "dgck-syfz",
    "spending": "rrqw-56er",
    "spending_by_claim": "nrth-mfg3",
    "imaging_efficiency": "di4y-5kzj",
    "timely_effective_care": "yv7e-xc69",
    "healthcare_infections": "77hc-ibv8",
    "unplanned_visits": "632h-zaca",
}

# CMS Facility IDs for our target systems
FACILITY_IDS = {
    "jefferson_health": "390174",
    "cooper_health": "310014",
    "temple_health": "390027",
}


async def _query_cms(
    dataset_id: str,
    facility_id: str,
    limit: int = 500,
    offset: int = 0,
) -> dict[str, Any]:
    """Query a CMS Provider Data Catalog dataset by facility ID."""
    facility_id = validate_facility_id(facility_id)
    url = f"{BASE_URL}/{dataset_id}/0"
    params = {
        "conditions[0][property]": "facility_id",
        "conditions[0][value]": facility_id,
        "conditions[0][operator]": "=",
        "limit": str(limit),
        "offset": str(offset),
        "count": "true",
        "results": "true",
        "schema": "true",
        "format": "json",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()


async def get_hospital_general_info(facility_id: str) -> dict[str, Any]:
    """Get hospital general information including star ratings, type, ownership."""
    data = await _query_cms(DATASETS["general_info"], facility_id)
    results = data.get("results", [])
    if not results:
        return {"error": f"No hospital found for facility_id {facility_id}"}
    return {
        "facility_id": facility_id,
        "dataset": "general_info",
        "record_count": len(results),
        "data": results,
    }


async def get_quality_measures(facility_id: str) -> dict[str, Any]:
    """Get complications, deaths, and mortality measures."""
    data = await _query_cms(DATASETS["complications_deaths"], facility_id)
    return {
        "facility_id": facility_id,
        "dataset": "complications_deaths",
        "record_count": len(data.get("results", [])),
        "data": data.get("results", []),
    }


async def get_readmission_measures(facility_id: str) -> dict[str, Any]:
    """Get hospital readmission rates and reduction program data."""
    data = await _query_cms(DATASETS["readmissions"], facility_id)
    return {
        "facility_id": facility_id,
        "dataset": "readmissions",
        "record_count": len(data.get("results", [])),
        "data": data.get("results", []),
    }


async def get_patient_satisfaction(facility_id: str) -> dict[str, Any]:
    """Get HCAHPS patient satisfaction survey results."""
    data = await _query_cms(DATASETS["patient_satisfaction"], facility_id, limit=500)
    return {
        "facility_id": facility_id,
        "dataset": "patient_satisfaction_hcahps",
        "record_count": len(data.get("results", [])),
        "data": data.get("results", []),
    }


async def get_medicare_spending(facility_id: str) -> dict[str, Any]:
    """Get Medicare spending per beneficiary data."""
    data = await _query_cms(DATASETS["spending"], facility_id)
    return {
        "facility_id": facility_id,
        "dataset": "medicare_spending",
        "record_count": len(data.get("results", [])),
        "data": data.get("results", []),
    }


async def get_timely_effective_care(facility_id: str) -> dict[str, Any]:
    """Get timely and effective care measures."""
    data = await _query_cms(DATASETS["timely_effective_care"], facility_id, limit=500)
    return {
        "facility_id": facility_id,
        "dataset": "timely_effective_care",
        "record_count": len(data.get("results", [])),
        "data": data.get("results", []),
    }


async def get_healthcare_infections(facility_id: str) -> dict[str, Any]:
    """Get healthcare-associated infection measures."""
    data = await _query_cms(DATASETS["healthcare_infections"], facility_id, limit=500)
    return {
        "facility_id": facility_id,
        "dataset": "healthcare_infections",
        "record_count": len(data.get("results", [])),
        "data": data.get("results", []),
    }


async def get_all_cms_data(facility_id: str) -> dict[str, Any]:
    """Fetch all CMS datasets for a facility. Returns aggregated results."""
    import asyncio

    tasks = {
        "general_info": get_hospital_general_info(facility_id),
        "quality_measures": get_quality_measures(facility_id),
        "readmissions": get_readmission_measures(facility_id),
        "patient_satisfaction": get_patient_satisfaction(facility_id),
        "medicare_spending": get_medicare_spending(facility_id),
        "timely_effective_care": get_timely_effective_care(facility_id),
        "healthcare_infections": get_healthcare_infections(facility_id),
    }

    results = {}
    gathered = await asyncio.gather(
        *tasks.values(), return_exceptions=True
    )
    for key, result in zip(tasks.keys(), gathered):
        if isinstance(result, Exception):
            results[key] = {"error": str(result)}
        else:
            results[key] = result

    return {"facility_id": facility_id, "cms_data": results}
