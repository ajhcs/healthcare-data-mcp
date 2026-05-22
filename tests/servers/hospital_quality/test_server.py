"""Tests for the hospital-quality MCP server tools.

Uses monkeypatching to avoid real HTTP calls or file downloads.
"""

from tests.helpers import parse_tool_result
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.hospital_quality import server
from shared.utils.mcp_response import validate_evidence_receipt


def assert_quality_receipt(result: dict, *, ccn: str = "390223") -> None:
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert result["evidence"]["cache_status"] in {"ready", "missing", "memory_only"}
    assert result["evidence"]["cache_freshness"]
    assert_quality_source_metadata(result)
    assert result["identity"]["ccn"] == ccn
    assert_quality_identity_map(result["identity_map"], ccn=ccn)


def assert_quality_no_data_receipt(result: dict, *, ccn: str, match_basis: str) -> None:
    assert result["ok"] is False
    assert result["error"]["code"] in {"not_found", "source_unavailable"}
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert result["evidence"]["match_basis"] == match_basis
    assert result["evidence"]["query"] == {"ccn": ccn}
    assert result["evidence"]["next_step"]
    assert_quality_source_metadata(result)
    assert result["identity"]["ccn"] == ccn
    assert_quality_identity_map(result["identity_map"], ccn=ccn)


def assert_quality_source_metadata(result: dict) -> None:
    metadata = result["source_metadata"]
    evidence = result["evidence"]

    assert metadata["source_name"] == evidence["source_name"]
    assert metadata["source_url"] == evidence["source_url"]
    assert metadata["dataset_id"] == evidence["dataset_id"]
    assert metadata["source_period"] == evidence["source_period"]
    assert metadata["cache_status"] == evidence["cache_status"]
    assert metadata["cache_freshness"] == evidence["cache_freshness"]
    assert metadata["entity_scope"] == evidence["entity_scope"]
    assert metadata["source_type"] == "cms_hospital_quality_public_file"


def assert_quality_identity_map(identity_map: dict, *, ccn: str) -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "hospital_quality_ccn"
    assert ccn in by_field["ccn"]["values"]
    assert by_field["ccn"]["status"] == "provided"
    assert by_field["ccn"]["used_by"]
    assert "canonical_name" in by_field
    assert "measure_id" in by_field
    assert identity_map["source_claims"]
    assert identity_map["source_claims"][0]["evidence_path"] == "evidence"
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No-match or missing hospital-quality responses")


def source_claim(identity_map: dict, collection: str) -> dict:
    claims = {claim["collection"]: claim for claim in identity_map["source_claims"]}
    return claims[collection]


def assert_quality_row_receipt(receipt: dict, *, dataset_id: str, match_basis: str, row_kind: str) -> None:
    validate_evidence_receipt(receipt, require_content=True)
    assert receipt["dataset_id"] == dataset_id
    assert receipt["entity_scope"] == "hospital_quality_ccn"
    assert receipt["match_basis"] == match_basis
    assert receipt["query"]["row_kind"] == row_kind
    assert receipt["confidence"]
    assert receipt["caveat"].startswith("CMS summary rows")
    assert receipt["next_step"]


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
    assert result["evidence"]["dataset_id"] == server._QUALITY_DATASET_IDS["hospital_info"]
    assert result["evidence"]["match_basis"] == "ccn_exact"
    assert_quality_receipt(result)
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert "THOMAS JEFFERSON UNIVERSITY HOSPITAL" in by_field["canonical_name"]["values"]


@pytest.mark.asyncio
async def test_get_quality_scores_not_found(mock_hospital_info_df):
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=mock_hospital_info_df):
        result = await server.get_quality_scores("390999")
    assert "390999" in result["error"]["message"]
    assert_quality_no_data_receipt(result, ccn="390999", match_basis="ccn_no_match_in_hospital_general_info")


@pytest.mark.asyncio
async def test_get_quality_scores_empty_df():
    with patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = await server.get_quality_scores("390223")
    assert result["error"]["code"] == "source_unavailable"
    assert_quality_no_data_receipt(result, ccn="390223", match_basis="source_cache_unavailable")


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
    assert result["evidence"]["match_basis"] == "ccn_exact_hrrp_condition_rows"
    assert_quality_receipt(result)
    assert_quality_row_receipt(
        result["conditions"][0]["evidence"],
        dataset_id=server._QUALITY_DATASET_IDS["hrrp"],
        match_basis="hrrp_condition_summary_row",
        row_kind="hrrp_condition",
    )
    assert result["conditions"][0]["evidence"]["query"]["ccn"] == "390223"
    assert result["conditions"][0]["evidence"]["query"]["condition"] in condition_measures
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert "READM_30_AMI_HRRP" in by_field["measure_id"]["values"]
    claim = source_claim(result["identity_map"], server._QUALITY_DATASET_IDS["hrrp"])
    assert claim["row_evidence_paths"] == ["conditions[].evidence"]


@pytest.mark.asyncio
async def test_get_readmission_data_not_found(mock_hrrp_df):
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=mock_hrrp_df):
        result = await server.get_readmission_data("390999")
    assert_quality_no_data_receipt(result, ccn="390999", match_basis="ccn_no_match_in_hrrp")


@pytest.mark.asyncio
async def test_get_readmission_data_empty_df():
    with patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=pd.DataFrame()):
        result = await server.get_readmission_data("390223")
    assert result["error"]["code"] == "source_unavailable"
    assert_quality_no_data_receipt(result, ccn="390223", match_basis="source_cache_unavailable")


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
    assert result["evidence"]["match_basis"] == "ccn_exact_hac_summary_row"
    assert_quality_receipt(result)
    by_domain = {row["domain"]: row for row in result["domain_evidence"]}
    assert by_domain["psi90"]["value"] == pytest.approx(7.10)
    assert by_domain["psi90"]["source_column"] == "psi_90_safety"
    assert_quality_row_receipt(
        by_domain["psi90"]["evidence"],
        dataset_id=server._QUALITY_DATASET_IDS["hac"],
        match_basis="hac_domain_summary_field",
        row_kind="hac_domain",
    )
    assert by_domain["psi90"]["evidence"]["query"]["domain"] == "psi90"
    assert by_domain["psi90"]["evidence"]["query"]["source_column"] == "psi_90_safety"
    claim = source_claim(result["identity_map"], server._QUALITY_DATASET_IDS["hac"])
    assert claim["row_evidence_paths"] == ["domain_evidence[].evidence"]


@pytest.mark.asyncio
async def test_get_safety_scores_not_found(mock_hac_df):
    with patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=mock_hac_df):
        result = await server.get_safety_scores("390999")
    assert_quality_no_data_receipt(result, ccn="390999", match_basis="ccn_no_match_in_hac")


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
    assert result["evidence"]["match_basis"] == "ccn_exact_hcahps_rows"
    assert_quality_receipt(result)
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert "H_COMP_1_STAR_RATING" in by_field["measure_id"]["values"]
    # nurse_communication domain should be present
    domain_names = {d["domain"] for d in result["domains"]}
    assert "nurse_communication" in domain_names
    nurse = next(d for d in result["domains"] if d["domain"] == "nurse_communication")
    assert_quality_row_receipt(
        nurse["evidence"],
        dataset_id=server._QUALITY_DATASET_IDS["hcahps"],
        match_basis="hcahps_domain_summary_rows",
        row_kind="hcahps_domain",
    )
    assert nurse["evidence"]["query"]["domain"] == "nurse_communication"
    assert "H_COMP_1_STAR_RATING" in nurse["evidence"]["query"]["source_measure_ids"]
    claim = source_claim(result["identity_map"], server._QUALITY_DATASET_IDS["hcahps"])
    assert claim["row_evidence_paths"] == ["domains[].evidence"]


@pytest.mark.asyncio
async def test_get_patient_experience_not_found(mock_hcahps_df):
    with patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=mock_hcahps_df):
        result = await server.get_patient_experience("390999")
    assert_quality_no_data_receipt(result, ccn="390999", match_basis="ccn_no_match_in_hcahps")


@pytest.mark.asyncio
async def test_get_financial_profile_not_found_has_evidence() -> None:
    cost_report = pd.DataFrame([
        {"provider_number": "390223", "facility_name": "Thomas Jefferson University Hospital"},
    ])
    with patch.object(server.data_loaders, "load_cost_report", new_callable=AsyncMock, return_value=cost_report):
        result = await server.get_financial_profile("390999")

    assert_quality_no_data_receipt(result, ccn="390999", match_basis="ccn_no_match_in_cost_report")


@pytest.mark.asyncio
async def test_get_financial_profile_includes_identity_and_evidence() -> None:
    cost_report = pd.DataFrame([
        {
            "provider_number": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "case_mix_index": "1.85",
            "total_discharges": "32000",
            "number_of_beds": "900",
            "resident_to_bed_ratio": "0.31",
            "dsh_pct": "22.5",
            "wage_index": "1.04",
            "urban_rural": "Urban",
            "fiscal_year_end": "2023-12-31",
        },
    ])
    with patch.object(server.data_loaders, "load_cost_report", new_callable=AsyncMock, return_value=cost_report):
        result = parse_tool_result(await server.get_financial_profile("390223"))

    assert result["case_mix_index"] == pytest.approx(1.85)
    assert result["teaching_status"] == "Major teaching"
    assert result["evidence"]["match_basis"] == "ccn_exact_cost_report_row"
    assert result["evidence"]["source_period"] == "2023-12-31"
    assert_quality_receipt(result)


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
    assert result["evidence"]["match_basis"] == "ccn_exact_measure_id"
    assert result["rows"][0]["evidence"]["caveat"].startswith("Exact row-level CMS")
    assert_quality_receipt(result)
    by_field = {entry["field"]: entry for entry in result["identity_map"]["join_keys"]}
    assert by_field["measure_id"]["status"] == "provided"
    assert "H_COMP_1" in by_field["measure_id"]["values"]
    validate_evidence_receipt(result["rows"][0]["evidence"], require_content=True)
    assert result["rows"][0]["cache_status"] in {"ready", "missing", "memory_only"}
    assert result["rows"][0]["cache_freshness"]
    claim = source_claim(result["identity_map"], server._QUALITY_DATASET_IDS["hcahps"])
    assert claim["row_evidence_paths"] == ["rows[].evidence"]


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
    assert result["evidence"]["match_basis"] == "no_exact_measure_row"
    assert_quality_receipt(result)


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


# ---------------------------------------------------------------------------
# Tests: compare_hospitals
# ---------------------------------------------------------------------------

def _second_hospital_rows(df: pd.DataFrame, *, ccn: str = "390999", name: str = "Example Medical Center") -> pd.DataFrame:
    copy = df.copy()
    if "facility_id" in copy.columns:
        copy["facility_id"] = ccn
    if "provider_number" in copy.columns:
        copy["provider_number"] = ccn
    if "facility_name" in copy.columns:
        copy["facility_name"] = name
    return copy


@pytest.mark.asyncio
async def test_compare_hospitals_returns_composite_evidence_and_identity_map(
    mock_hospital_info_df,
    mock_hrrp_df,
    mock_hac_df,
    mock_hcahps_df,
):
    hospital_info = pd.concat([mock_hospital_info_df, _second_hospital_rows(mock_hospital_info_df)], ignore_index=True)
    hrrp = pd.concat([mock_hrrp_df, _second_hospital_rows(mock_hrrp_df)], ignore_index=True)
    hac = pd.concat([mock_hac_df, _second_hospital_rows(mock_hac_df)], ignore_index=True)
    hcahps = pd.concat([mock_hcahps_df, _second_hospital_rows(mock_hcahps_df)], ignore_index=True)

    with (
        patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=hospital_info),
        patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=hrrp),
        patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=hac),
        patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=hcahps),
    ):
        result = parse_tool_result(await server.compare_hospitals(["390223", "390999"]))

    assert result["hospital_count"] == 2
    assert result["matched_hospital_count"] == 2
    assert result["evidence"]["dataset_id"] == "cms_hospital_quality_comparison"
    assert result["evidence"]["entity_scope"] == "ccn_list"
    assert result["evidence"]["query"] == {"ccns": ["390223", "390999"]}
    assert result["evidence"]["match_basis"] == "ccn_exact_multi_source_summary"
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert_quality_source_metadata(result)
    assert result["source_metadata"]["dataset_id"] == "cms_hospital_quality_comparison"
    assert result["source_metadata"]["entity_scope"] == "ccn_list"
    assert result["identity_map"]["join_key"] == "ccn"
    assert {facility["identity"]["ccn"] for facility in result["identity_map"]["facilities"]} == {"390223", "390999"}
    assert result["identity_map"]["facilities"][0]["available_domains"] == [
        "quality",
        "safety",
        "readmission",
        "patient_experience",
    ]
    validate_evidence_receipt(result["hospitals"][0]["quality"]["evidence"], require_content=True)
    validate_evidence_receipt(result["hospitals"][0]["safety"]["evidence"], require_content=True)
    validate_evidence_receipt(result["hospitals"][0]["readmission"]["evidence"], require_content=True)
    validate_evidence_receipt(result["hospitals"][0]["patient_experience"]["evidence"], require_content=True)


@pytest.mark.asyncio
async def test_compare_hospitals_preserves_nested_no_data_evidence(
    mock_hospital_info_df,
    mock_hac_df,
    mock_hcahps_df,
):
    hospital_info = pd.concat([mock_hospital_info_df, _second_hospital_rows(mock_hospital_info_df)], ignore_index=True)
    hac = pd.concat([mock_hac_df, _second_hospital_rows(mock_hac_df)], ignore_index=True)
    hcahps = pd.concat([mock_hcahps_df, _second_hospital_rows(mock_hcahps_df)], ignore_index=True)

    with (
        patch.object(server.data_loaders, "load_hospital_info", new_callable=AsyncMock, return_value=hospital_info),
        patch.object(server.data_loaders, "load_hrrp", new_callable=AsyncMock, return_value=pd.DataFrame()),
        patch.object(server.data_loaders, "load_hac", new_callable=AsyncMock, return_value=hac),
        patch.object(server.data_loaders, "load_hcahps", new_callable=AsyncMock, return_value=hcahps),
    ):
        result = parse_tool_result(await server.compare_hospitals(["390223", "390999"]))

    readmission = result["hospitals"][0]["readmission"]
    assert readmission["error"] == "HRRP data not available"
    assert readmission["ok"] is False
    assert readmission["error_code"] == "source_unavailable"
    assert readmission["evidence"]["match_basis"] == "source_cache_unavailable"
    assert readmission["evidence"]["dataset_id"] == server._QUALITY_DATASET_IDS["hrrp"]
    assert readmission["identity"]["ccn"] == "390223"
    assert readmission["identity_map"]["entity_scope"] == "hospital_quality_ccn"
    validate_evidence_receipt(readmission["evidence"], require_content=True)
    assert_quality_source_metadata(readmission)
    assert result["identity_map"]["facilities"][0]["missing_domains"] == ["readmission"]
    assert result["identity_map"]["facilities"][0]["available_domains"] == [
        "quality",
        "safety",
        "patient_experience",
    ]


@pytest.mark.asyncio
async def test_compare_hospitals_invalid_params_returns_evidence() -> None:
    result = await server.compare_hospitals(["390223"])

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"
    assert result["evidence"]["match_basis"] == "invalid_comparison_parameters"
    assert result["evidence"]["query"] == {"ccns": ["390223"]}
    validate_evidence_receipt(result["evidence"], require_content=True)
    assert_quality_source_metadata(result)
    assert result["source_metadata"]["dataset_id"] == "cms_hospital_quality_comparison"
    assert result["source_metadata"]["entity_scope"] == "ccn_list"
    assert result["identity_map"]["provided_ccns"] == ["390223"]
    assert result["identity_map"]["facilities"][0]["ccn"] == "390223"
