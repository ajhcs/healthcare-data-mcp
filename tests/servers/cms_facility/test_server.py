"""Tests for the cms-facility MCP server tools.

Uses monkeypatching to avoid real HTTP calls or file downloads.
"""

import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.cms_facility import server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_hospital_df():
    """Minimal Hospital General Info DataFrame."""
    return pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "address": "111 S 11th St",
            "city": "Philadelphia",
            "state": "PA",
            "zip_code": "19107",
            "county_name": "Philadelphia",
            "phone_number": "2155551234",
            "hospital_type": "Acute Care Hospitals",
            "hospital_ownership": "Voluntary non-profit - Private",
            "emergency_services": "Yes",
            "hospital_bed_count": "757",
            "hospital_overall_rating": "4",
            "mortality_national_comparison": "Above the national average",
            "safety_of_care_national_comparison": "Above the national average",
            "readmission_national_comparison": "Below the national average",
            "patient_experience_national_comparison": "Same as the national average",
        },
        {
            "facility_id": "390226",
            "facility_name": "Temple University Hospital",
            "address": "3401 N Broad St",
            "city": "Philadelphia",
            "state": "PA",
            "zip_code": "19140",
            "county_name": "Philadelphia",
            "phone_number": "2155559876",
            "hospital_type": "Acute Care Hospitals",
            "hospital_ownership": "Government - Hospital District or Authority",
            "emergency_services": "Yes",
            "hospital_bed_count": "722",
            "hospital_overall_rating": "3",
            "mortality_national_comparison": "Same as the national average",
            "safety_of_care_national_comparison": "Same as the national average",
            "readmission_national_comparison": "Same as the national average",
            "patient_experience_national_comparison": "Below the national average",
        },
    ])


@pytest.fixture
def mock_nppes_results():
    return [
        {
            "number": "1234567890",
            "enumeration_type": "NPI-2",
            "basic": {"organization_name": "Thomas Jefferson University Hospital"},
            "addresses": [
                {"address_1": "111 S 11th St", "city": "Philadelphia", "state": "PA"}
            ],
            "taxonomies": [{"desc": "General Acute Care Hospital", "primary": True}],
            "other_names": [],
        }
    ]


# ---------------------------------------------------------------------------
# Tests: search_facilities
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_facilities_by_name(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = json.loads(await server.search_facilities(name="Jefferson"))
    assert "results" in result
    assert result["count"] == 1
    assert "Jefferson" in result["results"][0]["facility_name"]


@pytest.mark.asyncio
async def test_search_facilities_by_state(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = json.loads(await server.search_facilities(state="PA"))
    assert result["count"] == 2


@pytest.mark.asyncio
async def test_search_facilities_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = json.loads(await server.search_facilities(name="anything"))
    assert "error" in result
    assert result["results"] == []


@pytest.mark.asyncio
async def test_search_facilities_no_match(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = json.loads(await server.search_facilities(name="Mayo Clinic"))
    assert result["count"] == 0
    assert result["results"] == []


# ---------------------------------------------------------------------------
# Tests: get_facility
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_facility_found(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = json.loads(await server.get_facility("390223"))
    assert result["ccn"] == "390223"
    assert "Jefferson" in result["facility_name"]
    assert result["beds"] == 757
    assert result["emergency_services"] is True


@pytest.mark.asyncio
async def test_get_facility_not_found(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = json.loads(await server.get_facility("999999"))
    assert "error" in result
    assert "999999" in result["error"]


@pytest.mark.asyncio
async def test_get_facility_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = json.loads(await server.get_facility("390223"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: search_npi
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_npi_returns_results(mock_nppes_results):
    with patch.object(server.data_loaders, "search_nppes", new_callable=AsyncMock, return_value=mock_nppes_results):
        result = json.loads(await server.search_npi(organization_name="Jefferson"))
    assert result["count"] == 1
    assert result["results"][0]["npi"] == "1234567890"
    assert result["results"][0]["enumeration_type"] == "NPI-2"


@pytest.mark.asyncio
async def test_search_npi_api_error():
    with patch.object(server.data_loaders, "search_nppes", new_callable=AsyncMock, side_effect=Exception("Network timeout")):
        result = json.loads(await server.search_npi(organization_name="Jefferson"))
    assert "error" in result
    assert result["results"] == []


@pytest.mark.asyncio
async def test_search_npi_empty_results():
    with patch.object(server.data_loaders, "search_nppes", new_callable=AsyncMock, return_value=[]):
        result = json.loads(await server.search_npi(organization_name="NonExistentHospital"))
    assert result["count"] == 0
    assert result["results"] == []
