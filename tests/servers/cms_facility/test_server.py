"""Tests for the cms-facility MCP server tools.

Uses monkeypatching to avoid real HTTP calls or file downloads.
"""

from tests.helpers import parse_tool_result
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.cms_facility import server
from shared.utils.mcp_response import validate_evidence_receipt


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
            "number": "1234567893",
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
        result = parse_tool_result(await server.search_facilities(name="Jefferson"))
    assert "results" in result
    assert result["count"] == 1
    assert "Jefferson" in result["results"][0]["facility_name"]
    assert result["results"][0]["identity"]["ccn"] == "390223"
    assert result["identity_map"]["join_keys"][0]["field"] == "ccn"
    assert result["identity_map"]["join_keys"][0]["values"] == ["390223"]
    assert result["identity_map"]["source_claims"][0]["row_evidence_path"] == "results[].evidence"
    validate_evidence_receipt(result["results"][0]["evidence"], require_content=True)
    assert result["results"][0]["evidence"]["dataset_id"] == "cms_hospital_general_info"
    assert result["results"][0]["evidence"]["match_basis"] == "cms_hospital_general_info_search_row"
    assert result["results"][0]["evidence"]["query"]["ccn"] == "390223"
    assert result["evidence"]["dataset_id"] == "cms_hospital_general_info"
    assert result["evidence"]["match_basis"] == "facility_search_filters"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_search_facilities_by_state(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = parse_tool_result(await server.search_facilities(state="PA"))
    assert result["count"] == 2


@pytest.mark.asyncio
async def test_search_facilities_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = parse_tool_result(await server.search_facilities(name="anything"))
    assert "error" in result
    assert result["results"] == []


@pytest.mark.asyncio
async def test_search_facilities_no_match(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = parse_tool_result(await server.search_facilities(name="Mayo Clinic"))
    assert result["count"] == 0
    assert result["results"] == []


# ---------------------------------------------------------------------------
# Tests: get_facility
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_facility_found(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = parse_tool_result(await server.get_facility("390223"))
    assert result["ccn"] == "390223"
    assert "Jefferson" in result["facility_name"]
    assert result["beds"] == 757
    assert result["emergency_services"] is True
    assert result["identity"]["ccn"] == "390223"
    assert result["identity"]["canonical_name"] == "THOMAS JEFFERSON UNIVERSITY HOSPITAL"
    assert result["identity_map"]["join_keys"][0]["values"] == ["390223"]
    assert "identity.ccn" in result["identity_map"]["source_claims"][0]["identity_paths"]
    assert result["source_metadata"]["dataset_id"] == "cms_hospital_general_info"
    assert result["evidence"]["match_basis"] == "ccn_exact"
    assert result["evidence"]["confidence"] == "high_for_cms_hospital_general_info_row"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_facility_not_found(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = parse_tool_result(await server.get_facility("999999"))
    assert "error" in result
    assert "999999" in result["error"]


@pytest.mark.asyncio
async def test_get_facility_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = parse_tool_result(await server.get_facility("390223"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: search_npi
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_npi_returns_results(mock_nppes_results):
    with patch.object(server.data_loaders, "search_nppes", new_callable=AsyncMock, return_value=mock_nppes_results):
        result = parse_tool_result(await server.search_npi(organization_name="Jefferson"))
    assert result["count"] == 1
    assert result["results"][0]["npi"] == "1234567893"
    assert result["results"][0]["enumeration_type"] == "NPI-2"
    assert result["results"][0]["identity"]["npi"] == "1234567893"
    assert result["identity_map"]["join_keys"][1]["field"] == "npi"
    assert result["identity_map"]["join_keys"][1]["values"] == ["1234567893"]
    assert "results[].identity.npi" in result["identity_map"]["source_claims"][0]["identity_paths"]
    validate_evidence_receipt(result["results"][0]["evidence"], require_content=True)
    assert result["results"][0]["evidence"]["dataset_id"] == "nppes_npi_registry"
    assert result["results"][0]["evidence"]["match_basis"] == "nppes_result_row"
    assert result["results"][0]["evidence"]["query"]["npi"] == "1234567893"
    assert result["evidence"]["dataset_id"] == "nppes_npi_registry"
    assert result["evidence"]["match_basis"] == "nppes_search_filters"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_search_npi_api_error():
    with patch.object(server.data_loaders, "search_nppes", new_callable=AsyncMock, side_effect=Exception("Network timeout")):
        result = parse_tool_result(await server.search_npi(organization_name="Jefferson"))
    assert "error" in result
    assert result["results"] == []


@pytest.mark.asyncio
async def test_search_npi_empty_results():
    with patch.object(server.data_loaders, "search_nppes", new_callable=AsyncMock, return_value=[]):
        result = parse_tool_result(await server.search_npi(organization_name="NonExistentHospital"))
    assert result["count"] == 0
    assert result["results"] == []
    assert result["evidence"]["dataset_id"] == "nppes_npi_registry"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_facility_financials_includes_identity_and_evidence():
    cost_report = pd.DataFrame([
        {
            "provider_number": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "fiscal_year_end": "2023-12-31",
            "hospital_bed_count": "757",
            "total_discharges": "32000",
            "total_patient_days": "214000",
            "net_patient_revenue": "1000000",
            "total_costs": "900000",
            "fte_employees": "4500.5",
        },
    ])
    with patch.object(server.data_loaders, "load_cost_report", new_callable=AsyncMock, return_value=cost_report):
        result = parse_tool_result(await server.get_facility_financials("390223"))

    assert result["ccn"] == "390223"
    assert result["fiscal_year_end"] == "2023-12-31"
    assert result["identity"]["ccn"] == "390223"
    assert result["identity_map"]["join_keys"][0]["values"] == ["390223"]
    assert result["identity_map"]["source_claims"][0]["match_policy"] == "ccn_exact_cost_report_row"
    assert result["evidence"]["dataset_id"] == "cms_cost_report"
    assert result["evidence"]["source_period"] == "2023-12-31"
    assert result["evidence"]["match_basis"] == "ccn_exact_cost_report_row"
    validate_evidence_receipt(result["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_get_hospital_info_includes_identity_and_evidence(mock_hospital_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_df):
        result = parse_tool_result(await server.get_hospital_info("390223"))

    assert result["ccn"] == "390223"
    assert result["identity"]["ccn"] == "390223"
    assert result["identity_map"]["join_keys"][0]["values"] == ["390223"]
    assert result["identity_map"]["source_claims"][0]["collection"] == "cms_hospital_general_info"
    assert result["source_metadata"]["dataset_id"] == "cms_hospital_general_info"
    assert result["evidence"]["match_basis"] == "ccn_exact"
    validate_evidence_receipt(result["evidence"], require_content=True)
