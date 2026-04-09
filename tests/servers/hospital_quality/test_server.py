"""Tests for the hospital-quality MCP server tools.

Uses monkeypatching to avoid real HTTP calls or file downloads.
"""

import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.hospital_quality import server


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_hospital_info_df():
    return pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "hospital_overall_rating": "4",
            "mortality_national_comparison": "Above the national average",
            "safety_of_care_national_comparison": "Above the national average",
            "readmission_national_comparison": "Below the national average",
            "patient_experience_national_comparison": "Same as the national average",
            "timeliness_of_care_national_comparison": "Above the national average",
        },
    ])


@pytest.fixture
def mock_hrrp_df():
    """HRRP dataset with long (tidy) layout — one row per condition per hospital."""
    rows = []
    conditions = [
        ("READM-30-AMI-HRRP", "AMI", "1.05", "15.2", "14.5", "320", "48"),
        ("READM-30-HF-HRRP", "HF", "0.98", "22.1", "22.5", "510", "89"),
        ("READM-30-PN-HRRP", "PN", "1.02", "17.4", "17.1", "280", "42"),
    ]
    for measure_id, _abbr, err, pred, exp, disch, readm in conditions:
        rows.append({
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "measure_id": measure_id,
            "excess_readmission_ratio": err,
            "predicted_readmission_rate": pred,
            "expected_readmission_rate": exp,
            "number_of_discharges": disch,
            "number_of_readmissions": readm,
            "payment_adjustment_factor": "0.9970",
        })
    return pd.DataFrame(rows)


@pytest.fixture
def mock_hac_df():
    return pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "total_hac_score": "6.25",
            "payment_reduction": "No",
            "psi_90_safety": "7.10",
            "clabsi": "0.82",
            "cauti": "1.10",
            "mrsa": "0.50",
            "cdi": "0.90",
        },
    ])


@pytest.fixture
def mock_hcahps_df():
    return pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "hcahps_measure_id": "H_COMP_1_STAR_RATING",
            "patient_survey_star_rating": "4",
            "hcahps_answer_percent": "",
            "hcahps_answer_description": "",
            "survey_response_rate_percent": "28",
            "number_of_completed_surveys": "620",
        },
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "hcahps_measure_id": "H_COMP_1_A_P",
            "patient_survey_star_rating": "",
            "hcahps_answer_percent": "82",
            "hcahps_answer_description": "Always",
            "survey_response_rate_percent": "28",
            "number_of_completed_surveys": "620",
        },
    ])


# ---------------------------------------------------------------------------
# Tests: get_quality_scores
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_quality_scores_found(mock_hospital_info_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_info_df):
        result = json.loads(await server.get_quality_scores("390223"))
    assert result["ccn"] == "390223"
    assert result["overall_rating"] == "4"
    assert result["mortality_national_comparison"] == "Above the national average"
    assert result["readmission_national_comparison"] == "Below the national average"


@pytest.mark.asyncio
async def test_get_quality_scores_not_found(mock_hospital_info_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_info_df):
        result = json.loads(await server.get_quality_scores("999999"))
    assert "error" in result
    assert "999999" in result["error"]


@pytest.mark.asyncio
async def test_get_quality_scores_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = json.loads(await server.get_quality_scores("390223"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_readmission_data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_readmission_data_found(mock_hrrp_df):
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=mock_hrrp_df):
        result = json.loads(await server.get_readmission_data("390223"))
    assert result["ccn"] == "390223"
    assert len(result["conditions"]) == 3
    condition_measures = {c["measure"] for c in result["conditions"]}
    assert "AMI" in condition_measures
    assert "HF" in condition_measures
    # Payment reduction derived from payment_adjustment_factor (0.9970 → 0.30%)
    assert result["payment_reduction_percentage"] == pytest.approx(0.30, abs=0.01)


@pytest.mark.asyncio
async def test_get_readmission_data_not_found(mock_hrrp_df):
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=mock_hrrp_df):
        result = json.loads(await server.get_readmission_data("000000"))
    assert "error" in result


@pytest.mark.asyncio
async def test_get_readmission_data_empty_df():
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = json.loads(await server.get_readmission_data("390223"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_safety_scores
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_safety_scores_found(mock_hac_df):
    with patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=mock_hac_df):
        result = json.loads(await server.get_safety_scores("390223"))
    assert result["ccn"] == "390223"
    assert result["total_hac_score"] == pytest.approx(6.25)
    assert result["payment_reduction"] == "No"


@pytest.mark.asyncio
async def test_get_safety_scores_not_found(mock_hac_df):
    with patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=mock_hac_df):
        result = json.loads(await server.get_safety_scores("000000"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_patient_experience
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_patient_experience_found(mock_hcahps_df):
    with patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=mock_hcahps_df):
        result = json.loads(await server.get_patient_experience("390223"))
    assert result["ccn"] == "390223"
    assert result["survey_response_rate"] == "28"
    assert result["num_completed_surveys"] == "620"
    # nurse_communication domain should be present
    domain_names = {d["domain"] for d in result["domains"]}
    assert "nurse_communication" in domain_names


@pytest.mark.asyncio
async def test_get_patient_experience_not_found(mock_hcahps_df):
    with patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=mock_hcahps_df):
        result = json.loads(await server.get_patient_experience("000000"))
    assert "error" in result
