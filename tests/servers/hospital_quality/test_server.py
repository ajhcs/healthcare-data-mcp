"""Tests for the hospital-quality MCP server tools.

Uses monkeypatching to avoid real HTTP calls or file downloads.
"""

from tests.helpers import parse_tool_result
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
        result = parse_tool_result(await server.get_quality_scores("390223"))
    assert result["ccn"] == "390223"
    assert result["overall_rating"] == "4"
    assert result["mortality_national_comparison"] == "Above the national average"
    assert result["readmission_national_comparison"] == "Below the national average"


@pytest.mark.asyncio
async def test_get_quality_scores_not_found(mock_hospital_info_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_info_df):
        result = parse_tool_result(await server.get_quality_scores("999999"))
    assert "error" in result
    assert "999999" in result["error"]


@pytest.mark.asyncio
async def test_get_quality_scores_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = parse_tool_result(await server.get_quality_scores("390223"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_readmission_data
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_readmission_data_found(mock_hrrp_df):
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=mock_hrrp_df):
        result = parse_tool_result(await server.get_readmission_data("390223"))
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
        result = parse_tool_result(await server.get_readmission_data("000000"))
    assert "error" in result


@pytest.mark.asyncio
async def test_get_readmission_data_empty_df():
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = parse_tool_result(await server.get_readmission_data("390223"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_safety_scores
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_safety_scores_found(mock_hac_df):
    with patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=mock_hac_df):
        result = parse_tool_result(await server.get_safety_scores("390223"))
    assert result["ccn"] == "390223"
    assert result["total_hac_score"] == pytest.approx(6.25)
    assert result["payment_reduction"] == "No"


@pytest.mark.asyncio
async def test_get_safety_scores_not_found(mock_hac_df):
    with patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=mock_hac_df):
        result = parse_tool_result(await server.get_safety_scores("000000"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_patient_experience
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_patient_experience_found(mock_hcahps_df):
    with patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=mock_hcahps_df):
        result = parse_tool_result(await server.get_patient_experience("390223"))
    assert result["ccn"] == "390223"
    assert result["survey_response_rate"] == "28"
    assert result["num_completed_surveys"] == "620"
    # nurse_communication domain should be present
    domain_names = {d["domain"] for d in result["domains"]}
    assert "nurse_communication" in domain_names


@pytest.mark.asyncio
async def test_get_patient_experience_not_found(mock_hcahps_df):
    with patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=mock_hcahps_df):
        result = parse_tool_result(await server.get_patient_experience("000000"))
    assert "error" in result


# ---------------------------------------------------------------------------
# Tests: get_quality_measure_rows
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_quality_measure_rows_hcahps_alias(mock_hcahps_df):
    with patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=mock_hcahps_df):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="hcahps_communication_nurses"))
    assert result["ccn"] == "390223"
    assert result["confidence"] == "high_for_exact_cms_measure_rows"
    assert result["total_rows"] == 2
    assert {row["measure_id"] for row in result["rows"]} == {"H_COMP_1_STAR_RATING", "H_COMP_1_A_P"}
    assert result["rows"][0]["source_name"] == "CMS HCAHPS - Hospital"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_ami_mortality_alias():
    complications = pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "measure_id": "MORT_30_AMI",
            "measure_name": "Acute Myocardial Infarction (AMI) 30-Day Mortality Rate",
            "score": "11.7",
            "start_date": "2021-07-01",
            "end_date": "2024-06-30",
        },
    ])
    with patch.object(server.data_loaders, "load_complications", new_callable=AsyncMock, return_value=complications):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="ami_30_day_mortality"))
    assert result["total_rows"] == 1
    assert result["rows"][0]["measure_id"] == "MORT_30_AMI"
    assert result["rows"][0]["score"] == "11.7"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_ami_mortality_rejects_adjacent_phc4_row():
    complications = pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "measure_id": "PHC4_IN_HOSPITAL_MORTALITY_AMI",
            "measure_name": "PHC4 in-hospital mortality",
            "score": "2.0",
        },
    ])
    with patch.object(server.data_loaders, "load_complications", new_callable=AsyncMock, return_value=complications):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="ami_30_day_mortality"))
    assert result["exact_measure_found"] is False
    assert result["status"] == "exact_measure_not_found"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_hospital_wide_readmission_rejects_hrrp_condition_rows():
    unplanned = pd.DataFrame()
    hrrp = pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "measure_id": "READM-30-AMI-HRRP",
            "excess_readmission_ratio": "1.0123",
        },
    ])
    with (
        patch.object(server.data_loaders, "load_unplanned_visits", new_callable=AsyncMock, return_value=unplanned),
        patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=hrrp),
    ):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="hospital_wide_readmission"))
    assert result["status"] == "exact_measure_not_found"
    assert result["exact_measure_found"] is False
    assert result["datasets_checked"] == ["unplanned_visits"]
    assert result["adjacent_available"] is True
    assert result["adjacent_tool"] == "get_readmission_data"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_hospital_wide_readmission_exact_only():
    unplanned = pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "measure_id": "READM-30-HOSP-WIDE",
            "measure_name": "Rate of readmission after discharge from hospital (hospital-wide)",
            "score": "14.2",
        },
    ])
    with patch.object(server.data_loaders, "load_unplanned_visits", new_callable=AsyncMock, return_value=unplanned):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="hospital_wide_readmission"))
    assert result["exact_measure_found"] is True
    assert result["rows"][0]["measure_id"] == "READM-30-HOSP-WIDE"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_clabsi_uses_exact_hai_dataset():
    hai = pd.DataFrame([
        {
            "facility_id": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "measure_id": "HAI-1",
            "measure_name": "Central line-associated bloodstream infections (CLABSI)",
            "score": "0.82",
        },
    ])
    with patch.object(server.data_loaders, "load_hai", new_callable=AsyncMock, return_value=hai):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="clabsi_sir"))
    assert result["total_rows"] == 1
    assert result["rows"][0]["measure_id"] == "HAI-1"
    assert result["rows"][0]["score"] == "0.82"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_clabsi_does_not_use_hac_total(mock_hac_df):
    with (
        patch.object(server.data_loaders, "load_hai", new_callable=AsyncMock, return_value=pd.DataFrame()),
        patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=mock_hac_df),
    ):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="clabsi_sir"))
    assert result["exact_measure_found"] is False
    assert result["adjacent_available"] is True
    assert result["adjacent_tool"] == "get_safety_scores"


@pytest.mark.asyncio
async def test_get_quality_measure_rows_reports_source_shape_error():
    wide_without_measure = pd.DataFrame([
        {"facility_id": "390223", "facility_name": "Example", "total_hac_score": "4.0"}
    ])
    with patch.object(server.data_loaders, "load_hai", new_callable=AsyncMock, return_value=wide_without_measure):
        result = parse_tool_result(await server.get_quality_measure_rows("390223", measure="clabsi_sir"))
    assert result["status"] == "source_shape_error"
    assert result["dataset_shapes"][0]["dataset_id"] == server._QUALITY_DATASET_IDS["hai"]
    assert "total_hac_score" in result["dataset_shapes"][0]["columns_sample"]
