"""Tests for workforce-analytics ACGME import and query behavior."""

from pathlib import Path

import pandas as pd
import pytest
import duckdb

from servers.workforce_analytics import operations_data, server, workforce_data
from shared.utils.mcp_response import validate_evidence_receipt


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    con = duckdb.connect(":memory:")
    try:
        con.register("df", df)
        con.execute("COPY df TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(path)])
    finally:
        con.close()


def _assert_workforce_evidence(evidence: dict, *, dataset_id: str) -> None:
    validate_evidence_receipt(evidence, require_content=True)
    assert evidence["dataset_id"] == dataset_id
    assert evidence["source_name"]
    assert evidence["source_url"]
    assert evidence["source_period"]
    assert evidence["retrieved_at"]
    assert evidence["cache_status"]
    assert evidence["cache_freshness"]
    assert evidence["entity_scope"] == "workforce_operations"
    assert evidence["match_basis"]
    assert evidence["confidence"]
    assert evidence["caveat"]
    assert evidence["next_step"]


def _assert_workforce_source_metadata(result: dict) -> None:
    metadata = result["source_metadata"]
    evidence = result["evidence"]

    assert metadata["source_name"] == evidence["source_name"]
    assert metadata["source_url"] == evidence["source_url"]
    assert metadata["dataset_id"] == evidence["dataset_id"]
    assert metadata["source_period"] == evidence["source_period"]
    assert metadata["landing_page"] == evidence["landing_page"]
    assert metadata["retrieved_at"] == evidence["retrieved_at"]
    assert metadata["source_modified"] == evidence["source_modified"]
    assert metadata["cache_status"] == evidence["cache_status"]
    assert metadata["cache_freshness"] == evidence["cache_freshness"]
    assert metadata["entity_scope"] == evidence["entity_scope"]
    assert metadata["query"] == evidence["query"]
    assert metadata["cache_key"] == evidence["cache_key"]
    assert metadata["source_type"] == "public_workforce_operations_source"


def _assert_workforce_row_evidence(evidence: dict, *, dataset_id: str, match_basis: str) -> None:
    _assert_workforce_evidence(evidence, dataset_id=dataset_id)
    assert evidence["match_basis"] == match_basis
    assert evidence["query"]["row_kind"]


def _assert_workforce_metric_evidence(
    evidence: dict,
    *,
    dataset_id: str,
    match_basis: str,
    metric_name: str,
) -> None:
    _assert_workforce_evidence(evidence, dataset_id=dataset_id)
    assert evidence["match_basis"] == match_basis
    assert evidence["query"]["metric_name"] == metric_name
    assert "metric_value_present" in evidence["query"]
    assert evidence["query"]["metric_confidence"]
    assert evidence["query"]["source"]
    assert "missing fields are not zero values" in evidence["caveat"]


def _assert_workforce_no_data(result: dict, *, dataset_id: str, match_basis: str, ccn: str = "") -> None:
    assert result["ok"] is False
    assert result["error"]["code"] == "not_found"
    _assert_workforce_evidence(result["evidence"], dataset_id=dataset_id)
    _assert_workforce_source_metadata(result)
    assert result["evidence"]["match_basis"] == match_basis
    assert result["evidence"]["confidence"].startswith("no_matching_")
    assert result["identity_map"]["entity_scope"] == "workforce_operations"
    assert result["identity_map"]["source_claims"]
    assert result["identity_map"]["conflict_policy"]
    assert result["identity_map"]["missing_data_policy"].startswith("No-match or missing workforce/operations responses")
    if ccn:
        assert result["identity"]["ccn"] == ccn
        _assert_workforce_identity_map(result["identity_map"], expected_ccn=ccn)


def _assert_workforce_identity_map(
    identity_map: dict,
    *,
    expected_ccn: str = "",
    expected_state: str = "",
    expected_name: str = "",
    expected_field_values: dict[str, str] | None = None,
    expected_sources: set[str] | None = None,
) -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "workforce_operations"
    assert identity_map["source_claims"]
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No-match or missing workforce/operations responses")
    if expected_ccn:
        assert expected_ccn in by_field["ccn"]["values"]
        assert by_field["ccn"]["status"] == "provided"
    if expected_state:
        assert expected_state in by_field["state"]["values"]
        assert by_field["state"]["status"] == "provided"
    if expected_name:
        assert expected_name in by_field["canonical_name"]["values"]
    for field, value in (expected_field_values or {}).items():
        assert value in by_field[field]["values"]
        assert by_field[field]["status"] == "provided"
    if expected_sources:
        assert {claim["collection"] for claim in identity_map["source_claims"]} >= expected_sources


def _workforce_source_claim(identity_map: dict, collection: str) -> dict:
    claims = {claim["collection"]: claim for claim in identity_map["source_claims"]}
    return claims[collection]


def test_normalize_acgme_dataframe_maps_export_aliases():
    raw = pd.DataFrame(
        [
            {
                "Program ID": "1403521487",
                "Specialty Name": "Internal Medicine",
                "Sponsoring Institution": "Cleveland Clinic Foundation",
                "City": "Cleveland",
                "State": "OH",
                "Approved Positions": "180",
                "On Duty": "176",
                "Status": "Accredited",
            }
        ]
    )

    normalized = workforce_data.normalize_acgme_dataframe(raw)

    assert list(normalized.columns) == [
        "program_id",
        "specialty",
        "institution",
        "city",
        "state",
        "total_positions",
        "filled_positions",
        "accreditation_status",
    ]
    row = normalized.iloc[0]
    assert row["program_id"] == "1403521487"
    assert row["specialty"] == "Internal Medicine"
    assert row["institution"] == "Cleveland Clinic Foundation"
    assert row["state"] == "OH"
    assert row["total_positions"] == 180
    assert row["filled_positions"] == 176
    assert row["accreditation_status"] == "Accredited"


def test_normalize_acgme_dataframe_preserves_leading_zero_program_id():
    raw = pd.DataFrame(
        [
            {
                "Program ID": "0123456789",
                "Specialty Name": "Internal Medicine",
                "Sponsoring Institution": "Example Sponsor",
                "State": "PA",
            }
        ]
    )

    normalized = workforce_data.normalize_acgme_dataframe(raw)

    assert normalized.iloc[0]["program_id"] == "0123456789"


def test_normalize_acgme_dataframe_rejects_invalid_program_id():
    raw = pd.DataFrame(
        [
            {
                "Program ID": "ABC123",
                "Specialty Name": "Internal Medicine",
                "Sponsoring Institution": "Example Sponsor",
                "State": "PA",
            }
        ]
    )

    with pytest.raises(ValueError, match="10-digit"):
        workforce_data.normalize_acgme_dataframe(raw)


def test_query_acgme_programs_reads_env_configured_csv(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "acgme_export.csv"
    pd.DataFrame(
        [
            {
                "program_id": "1403521487",
                "specialty": "Internal Medicine",
                "institution": "Cleveland Clinic Foundation",
                "city": "Cleveland",
                "state": "OH",
                "total_positions": "180",
                "filled_positions": "176",
                "accreditation_status": "Accredited",
            },
            {
                "program_id": "4403521234",
                "specialty": "General Surgery",
                "institution": "Cleveland Clinic Foundation",
                "city": "Cleveland",
                "state": "OH",
                "total_positions": "70",
                "filled_positions": "68",
                "accreditation_status": "Accredited",
            },
        ]
    ).to_csv(csv_path, index=False)

    monkeypatch.setenv(workforce_data._ACGME_ENV_VAR, str(csv_path))
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_CSV", tmp_path / "missing-cache.csv")
    monkeypatch.setattr(workforce_data, "_ACGME_CSV", tmp_path / "missing-bundled.csv")

    results = workforce_data.query_acgme_programs(
        institution="cleveland clinic",
        specialty="internal medicine",
        state="oh",
    )

    assert len(results) == 1
    assert results[0]["program_id"] == "1403521487"
    assert results[0]["filled_positions"] == 176
    assert set(results[0]["match_basis"]) == {"institution_contains", "specialty_contains", "state_exact"}


def test_query_acgme_programs_returns_actionable_error_when_missing(monkeypatch, tmp_path: Path):
    monkeypatch.delenv(workforce_data._ACGME_ENV_VAR, raising=False)
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_CSV", tmp_path / "missing-cache.csv")
    monkeypatch.setattr(workforce_data, "_ACGME_CSV", tmp_path / "missing-bundled.csv")

    results = workforce_data.query_acgme_programs(state="OH")

    assert len(results) == 1
    assert "error" in results[0]
    assert "scripts/import_acgme_programs.py" in results[0]["error"]
    assert "acgmecloud.org/analytics/explore-public-data/program-search" in results[0]["error"]


def test_get_acgme_source_status_missing_cache(monkeypatch, tmp_path: Path):
    monkeypatch.delenv(workforce_data._ACGME_ENV_VAR, raising=False)
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_CSV", tmp_path / "missing-cache.csv")
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_META", tmp_path / "missing-cache.meta.json")
    monkeypatch.setattr(workforce_data, "_ACGME_CSV", tmp_path / "missing-bundled.csv")

    status = workforce_data.get_acgme_source_status()

    assert status["status"] == "import_required"
    assert "scripts/import_acgme_programs.py" in status["next_step"]


@pytest.mark.asyncio
async def test_get_acgme_source_status_reads_valid_import_metadata(tmp_path: Path, monkeypatch):
    csv_path = tmp_path / "acgme_programs.csv"
    raw = pd.DataFrame(
        [
            {
                "Program ID": "0123456789",
                "Specialty Name": "Internal Medicine",
                "Sponsoring Institution": "Example Sponsor",
                "State": "PA",
            }
        ]
    )
    normalized = workforce_data.normalize_acgme_dataframe(raw)
    normalized.to_csv(csv_path, index=False)
    workforce_data.write_acgme_import_metadata(
        input_path=tmp_path / "source.csv",
        output_path=csv_path,
        raw_df=raw,
        normalized_df=normalized,
    )
    monkeypatch.setenv(workforce_data._ACGME_ENV_VAR, str(csv_path))
    monkeypatch.setattr(workforce_data, "_ACGME_CACHE_CSV", tmp_path / "missing-cache.csv")
    monkeypatch.setattr(workforce_data, "_ACGME_CSV", tmp_path / "missing-bundled.csv")

    status = workforce_data.get_acgme_source_status()
    result = workforce_data.get_acgme_program("0123456789")
    tool_result = await server.get_acgme_program("0123456789")

    assert status["status"] == "ready"
    assert status["row_count"] == 1
    assert result is not None
    assert result["match_basis"] == ["program_id_exact"]
    assert tool_result["status"] == "ready"
    assert tool_result["program"]["program_id"] == "0123456789"
    _assert_workforce_evidence(tool_result["evidence"], dataset_id="acgme_program_search_public_export")
    _assert_workforce_row_evidence(
        tool_result["program"]["evidence"],
        dataset_id="acgme_program_search_public_export",
        match_basis="acgme_program_id_exact_row",
    )


@pytest.mark.asyncio
async def test_productivity_profile_computes_public_ratios(monkeypatch):
    async def fake_pos_row(ccn: str):
        return None

    async def fake_ensure_hcris_cached():
        return True

    async def fake_ahrq_hospital_row(ccn: str):
        return {"hos_beds": "100", "hos_dsch": "5000"}

    async def fake_cost_report_row(ccn: str, year: int = 0):
        return pd.Series(
            {
                "total_inpatient_days": "25000",
                "adjusted_patient_days": "40000",
                "case_mix_index": "1.2",
            }
        )

    async def fake_hospital_linkage():
        return pd.DataFrame([{"ccn": "390001", "hosp_state": "PA"}])

    monkeypatch.setattr(server.workforce_data, "ensure_hcris_cached", fake_ensure_hcris_cached)
    monkeypatch.setattr(
        server.workforce_data,
        "query_hcris_staffing",
        lambda ccn, year=0: {
            "total_ftes": 250.0,
            "departments": [
                {
                    "dept_name": "Hospital Adults & Pediatrics",
                    "total_ftes": 100.5,
                    "rn_ftes": 60.25,
                    "lpn_ftes": 25.0,
                    "aide_ftes": 15.25,
                }
            ],
        },
    )
    monkeypatch.setattr(server.workforce_data, "query_hcris_gme", lambda ccn, year=0: {"total_resident_ftes": 25.0})
    monkeypatch.setattr(server, "_ahrq_hospital_row", fake_ahrq_hospital_row)
    monkeypatch.setattr(server, "_cost_report_row", fake_cost_report_row)
    monkeypatch.setattr(server, "_pos_row", fake_pos_row)
    monkeypatch.setattr(server.ahrq_data, "load_ahrq_hospital_linkage", fake_hospital_linkage)

    profile = await server._productivity_profile("390001", year=2025)
    hospital_staffing = await server.get_hospital_staffing_productivity("390001", year=2025)
    staffing_comparison = await server.compare_hospital_staffing_productivity("PA", year=2025, peer_group="state,teaching")
    teaching = await server.get_teaching_intensity("390001", year=2025)

    assert profile["fte_per_bed"] == 2.5
    assert profile["fte_per_discharge"] == 0.05
    assert profile["resident_to_bed_ratio"] == 0.25
    assert profile["case_mix_adjusted_discharges_per_fte"] == 24.0
    assert "case_mix_adjusted_discharges_per_fte" in profile
    assert profile["peer_group_metadata"]["attributes"]["bed_size"] == "100_299"
    assert profile["peer_group_metadata"]["attributes"]["teaching"] == "teaching"
    assert round(profile["fte_per_occupied_bed"], 4) == round(250 / (25000 / 365), 4)
    assert profile["identity"]["ccn"] == "390001"
    assert profile["identity"]["canonical_name"] == ""
    assert profile["identity"]["match_decisions"][0]["basis"] == "ccn_exact_public_cost_report_and_ahrq_linkage"
    _assert_workforce_row_evidence(
        profile["departments"][0]["evidence"],
        dataset_id="cms_hcris_workforce_productivity",
        match_basis="hcris_department_staffing_row",
    )
    _assert_workforce_identity_map(
        profile["identity_map"],
        expected_ccn="390001",
        expected_sources={"cms_hcris_workforce_productivity", "ahrq_hospital_linkage"},
    )
    claim = _workforce_source_claim(profile["identity_map"], "cms_hcris_workforce_productivity")
    assert "departments[].evidence" in claim["row_evidence_paths"]
    assert "bed_source.selected_candidate_evidence" in claim["row_evidence_paths"]
    _assert_workforce_evidence(profile["evidence"], dataset_id="cms_hcris_workforce_productivity")
    _assert_workforce_source_metadata(profile)
    _assert_workforce_row_evidence(
        profile["bed_source"]["selected_candidate_evidence"],
        dataset_id="hospital_bed_identity_resolution",
        match_basis="hospital_bed_source_selected_candidate_row",
    )
    _assert_workforce_row_evidence(
        profile["bed_source"]["candidates"][0]["evidence"],
        dataset_id="hospital_bed_identity_resolution",
        match_basis="hospital_bed_source_candidate_row",
    )
    _assert_workforce_evidence(hospital_staffing["evidence"], dataset_id="cms_hcris_workforce_productivity")
    _assert_workforce_source_metadata(hospital_staffing)
    _assert_workforce_evidence(staffing_comparison["evidence"], dataset_id="cms_hcris_workforce_productivity")
    _assert_workforce_source_metadata(staffing_comparison)
    _assert_workforce_evidence(
        staffing_comparison["profiles"][0]["evidence"],
        dataset_id="cms_hcris_workforce_productivity",
    )
    _assert_workforce_evidence(teaching["evidence"], dataset_id="cms_hcris_gme")
    _assert_workforce_source_metadata(teaching)


@pytest.mark.asyncio
async def test_throughput_profile_computes_public_operations_metrics(monkeypatch):
    async def fake_pos_row(ccn: str):
        return None

    async def fake_ahrq_hospital_row(ccn: str):
        return {"hospital_name": "Example Hospital", "hosp_state": "PA"}

    async def fake_cost_report_row(ccn: str, year: int = 0):
        return pd.Series(
            {
                "beds": "100",
                "total_discharges": "5000",
                "total_inpatient_days": "25000",
            }
        )

    monkeypatch.setattr(server, "_ahrq_hospital_row", fake_ahrq_hospital_row)
    monkeypatch.setattr(server, "_cost_report_row", fake_cost_report_row)
    monkeypatch.setattr(server, "_pos_row", fake_pos_row)
    monkeypatch.setattr(operations_data, "_load_state_health_data", lambda: None)

    profile = await server._throughput_profile(ccn="390001")

    assert profile["hospital_name"] == "Example Hospital"
    assert profile["occupancy_rate"] == round(25000 / 36500, 4)
    assert profile["average_length_of_stay"] == 5.0
    assert profile["bed_turnover_rate"] == 50.0
    assert profile["source_rankings"][0]["rank"] == 1
    assert profile["beds"] == 100
    assert profile["bed_source"]["selected_source_field"] == "beds"
    assert profile["metric_confidence"]["discharges"]["confidence"] == "high_for_reported_provider_year_field"
    assert set(profile["metric_evidence"]) == set(profile["metric_confidence"])
    _assert_workforce_metric_evidence(
        profile["metric_evidence"]["discharges"],
        dataset_id="public_hospital_throughput",
        match_basis="ccn_or_state_facility_id_public_source_lookup_metric_discharges",
        metric_name="discharges",
    )
    assert profile["metric_evidence"]["discharges"]["query"]["source_field"] == "total_discharges"
    _assert_workforce_metric_evidence(
        profile["metric_evidence"]["occupancy_rate"],
        dataset_id="public_hospital_throughput",
        match_basis="ccn_or_state_facility_id_public_source_lookup_metric_occupancy_rate",
        metric_name="occupancy_rate",
    )
    assert profile["metric_evidence"]["occupancy_rate"]["query"]["metric_value_present"] is True
    assert profile["pa_admissions_enhancement"] is None
    _assert_workforce_evidence(profile["evidence"], dataset_id="public_hospital_throughput")
    _assert_workforce_source_metadata(profile)
    assert profile["identity"]["ccn"] == "390001"
    assert profile["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    assert profile["identity"]["match_decisions"][0]["basis"] == "ccn_or_state_facility_id_public_source_lookup"
    _assert_workforce_identity_map(
        profile["identity_map"],
        expected_ccn="390001",
        expected_name="EXAMPLE HOSPITAL",
        expected_sources={"public_hospital_throughput"},
    )
    claim = _workforce_source_claim(profile["identity_map"], "public_hospital_throughput")
    assert "bed_source.selected_candidate_evidence" in claim["row_evidence_paths"]
    assert claim["metric_evidence_paths"] == ["metric_evidence.*"]


@pytest.mark.asyncio
async def test_workforce_public_tools_include_canonical_evidence(monkeypatch):
    async def fake_bls(_occupation: str, _area_code: str = "", _state: str = ""):
        return {
            "occupation_title": "Registered Nurses",
            "soc_code": "29-1141",
            "area_name": "Pennsylvania",
            "employment": 150000,
            "mean_wage": 92000,
            "median_wage": 87000,
            "pct_10_wage": 65000,
            "pct_90_wage": 120000,
            "data_year": "2025",
        }

    async def fake_ensure_hpsa_cached():
        return True

    monkeypatch.setattr(server.bls_client, "get_oes_data", fake_bls)
    monkeypatch.setattr(server.workforce_data, "ensure_hpsa_cached", fake_ensure_hpsa_cached)
    monkeypatch.setattr(
        server.workforce_data,
        "query_hpsas",
        lambda state, discipline, county_fips: [
            {
                "designation_id": "HPSA-1",
                "name": "Example County",
                "state": state,
                "county": "Example",
                "discipline": discipline or "Primary Care",
                "score": 18,
                "status": "Designated",
            }
        ],
    )
    monkeypatch.setattr(
        server.workforce_data,
        "get_acgme_source_status",
        lambda: {
            "status": "ready",
            "source_period": "2026 public export",
            "source_caveat": "Imported fixture.",
            "row_count": 1,
        },
    )
    monkeypatch.setattr(
        server.workforce_data,
        "query_acgme_programs",
        lambda institution, specialty, state: [
            {
                "program_id": "1403521487",
                "specialty": specialty or "Internal Medicine",
                "institution": institution or "Example Sponsor",
                "city": "Philadelphia",
                "state": state or "PA",
                "total_positions": 30,
                "filled_positions": 28,
                "accreditation_status": "Continued Accreditation",
            }
        ],
    )

    bls = await server.get_bls_employment("Registered Nurses", state="PA")
    hrsa = await server.get_hrsa_workforce("PA", discipline="Primary Care")
    acgme_status = await server.get_acgme_source_status()
    acgme_search = await server.search_acgme_programs("Example Sponsor", "Internal Medicine", "PA")

    assert bls["occupation_title"] == "Registered Nurses"
    _assert_workforce_evidence(bls["evidence"], dataset_id="bls_oes_employment")
    _assert_workforce_source_metadata(bls)
    _assert_workforce_identity_map(
        bls["identity_map"],
        expected_state="PA",
        expected_field_values={"occupation": "REGISTERED NURSES"},
        expected_sources={"bls_oes_employment"},
    )
    assert hrsa["total_hpsas"] == 1
    _assert_workforce_evidence(hrsa["evidence"], dataset_id="hrsa_hpsa_workforce")
    _assert_workforce_source_metadata(hrsa)
    _assert_workforce_identity_map(
        hrsa["identity_map"],
        expected_state="PA",
        expected_field_values={"discipline": "PRIMARY CARE"},
        expected_sources={"hrsa_hpsa_workforce"},
    )
    claim = _workforce_source_claim(hrsa["identity_map"], "hrsa_hpsa_workforce")
    assert claim["row_evidence_paths"] == ["hpsas[].evidence"]
    _assert_workforce_row_evidence(
        hrsa["hpsas"][0]["evidence"],
        dataset_id="hrsa_hpsa_workforce",
        match_basis="hrsa_hpsa_source_row",
    )
    assert acgme_status["status"] == "ready"
    _assert_workforce_evidence(acgme_status["evidence"], dataset_id="acgme_program_search_public_export")
    _assert_workforce_source_metadata(acgme_status)
    assert acgme_search["programs"][0]["program_id"] == "1403521487"
    _assert_workforce_row_evidence(
        acgme_search["programs"][0]["evidence"],
        dataset_id="acgme_program_search_public_export",
        match_basis="acgme_program_search_result_row",
    )


@pytest.mark.asyncio
async def test_union_activity_returns_candidate_employer_identity_map(monkeypatch):
    async def fake_ensure_nlrb_cached():
        return True

    async def fake_ensure_stoppages_cached():
        return True

    monkeypatch.setattr(server.labor_data, "ensure_nlrb_cached", fake_ensure_nlrb_cached)
    monkeypatch.setattr(server.labor_data, "ensure_stoppages_cached", fake_ensure_stoppages_cached)
    monkeypatch.setattr(
        server.labor_data,
        "search_nlrb_elections",
        lambda employer_name, state, year_start, year_end: [
            {
                "case_number": "01-RC-000001",
                "employer": "Example Health",
                "union": "Nurses United",
                "date": "2025-05-01",
                "result": "certified",
                "unit_size": 100,
                "city": "Pittsburgh",
                "state": state,
            }
        ],
    )
    monkeypatch.setattr(
        server.labor_data,
        "query_work_stoppages",
        lambda year_start, year_end: [
            {
                "employer": "Example Health",
                "union": "Nurses United",
                "start_date": "2025-06-01",
                "end_date": "2025-06-03",
                "workers_involved": 50,
                "duration_days": 3,
            }
        ],
    )

    result = await server.search_union_activity("Example Health", state="PA", year_start=2025, year_end=2025)

    assert result["total_elections"] == 1
    _assert_workforce_evidence(result["evidence"], dataset_id="nlrb_bls_labor_activity")
    _assert_workforce_source_metadata(result)
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_workforce_identity_map(
        result["identity_map"],
        expected_state="PA",
        expected_name="EXAMPLE HEALTH",
        expected_field_values={"employer_name": "EXAMPLE HEALTH"},
        expected_sources={"nlrb_bls_labor_activity"},
    )
    claim = _workforce_source_claim(result["identity_map"], "nlrb_bls_labor_activity")
    assert claim["row_evidence_paths"] == ["elections[].evidence", "work_stoppages[].evidence"]
    _assert_workforce_row_evidence(
        result["elections"][0]["evidence"],
        dataset_id="nlrb_bls_labor_activity",
        match_basis="nlrb_election_source_row",
    )
    _assert_workforce_row_evidence(
        result["work_stoppages"][0]["evidence"],
        dataset_id="nlrb_bls_labor_activity",
        match_basis="bls_work_stoppage_source_row",
    )


@pytest.mark.asyncio
async def test_workforce_public_no_data_paths_include_evidence(monkeypatch):
    async def fake_bls_empty(_occupation: str, _area_code: str = "", _state: str = ""):
        return {}

    async def fake_ensure_hcris_cached():
        return True

    monkeypatch.setattr(server.bls_client, "get_oes_data", fake_bls_empty)
    monkeypatch.setattr(server.workforce_data, "ensure_hcris_cached", fake_ensure_hcris_cached)
    monkeypatch.setattr(server.workforce_data, "query_hcris_gme", lambda ccn: {})

    bls = await server.get_bls_employment("Registered Nurses", state="PA")
    gme = await server.get_gme_profile(ccn="390999")

    _assert_workforce_no_data(
        bls,
        dataset_id="bls_oes_employment",
        match_basis="occupation_area_state_public_api_lookup_no_match",
    )
    assert bls["evidence"]["query"] == {"occupation": "Registered Nurses", "area_code": "", "state": "PA"}
    _assert_workforce_no_data(
        gme,
        dataset_id="cms_hcris_gme",
        match_basis="ccn_exact_hcris_gme_no_match",
        ccn="390999",
    )


@pytest.mark.asyncio
async def test_workforce_operations_tools_include_canonical_evidence(monkeypatch):
    async def fake_pos_row(ccn: str):
        return None

    async def fake_ahrq_hospital_row(ccn: str):
        return {"hospital_name": "Example Hospital", "hosp_state": "PA"}

    async def fake_cost_report_row(ccn: str, year: int = 0):
        return pd.Series({"beds": "100", "total_discharges": "5000", "total_inpatient_days": "25000"})

    monkeypatch.setattr(server, "_ahrq_hospital_row", fake_ahrq_hospital_row)
    monkeypatch.setattr(server, "_cost_report_row", fake_cost_report_row)
    monkeypatch.setattr(server, "_pos_row", fake_pos_row)
    monkeypatch.setattr(operations_data, "_load_state_health_data", lambda: None)

    beds = await server.resolve_hospital_beds(ccn="390001", state="PA", year=2025)
    throughput = await server.get_public_throughput_profile(ccn="390001", state="PA", year=2025)
    ed = await server.get_ed_volume_profile(ccn="390001", state="PA", year=2025)
    procedures = await server.get_or_procedure_volume_profile(ccn="390001", state="PA", year=2025)

    assert beds["selected_bed_count"] == 100.0
    _assert_workforce_evidence(beds["evidence"], dataset_id="hospital_bed_identity_resolution")
    _assert_workforce_source_metadata(beds)
    _assert_workforce_row_evidence(
        beds["selected_candidate_evidence"],
        dataset_id="hospital_bed_identity_resolution",
        match_basis="hospital_bed_source_selected_candidate_row",
    )
    _assert_workforce_row_evidence(
        beds["candidates"][0]["evidence"],
        dataset_id="hospital_bed_identity_resolution",
        match_basis="hospital_bed_source_candidate_row",
    )
    assert beds["identity"]["ccn"] == "390001"
    assert beds["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    _assert_workforce_identity_map(
        beds["identity_map"],
        expected_ccn="390001",
        expected_state="PA",
        expected_name="EXAMPLE HOSPITAL",
        expected_sources={"hospital_bed_resolution"},
    )
    bed_claim = _workforce_source_claim(beds["identity_map"], "hospital_bed_resolution")
    assert "selected_candidate_evidence" in bed_claim["row_evidence_paths"]
    assert "candidates[].evidence" in bed_claim["row_evidence_paths"]
    assert throughput["occupancy_rate"] == round(25000 / 36500, 4)
    _assert_workforce_evidence(throughput["evidence"], dataset_id="public_hospital_throughput")
    _assert_workforce_source_metadata(throughput)
    _assert_workforce_row_evidence(
        throughput["bed_source"]["selected_candidate_evidence"],
        dataset_id="hospital_bed_identity_resolution",
        match_basis="hospital_bed_source_selected_candidate_row",
    )
    assert throughput["identity"]["ccn"] == "390001"
    assert throughput["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    _assert_workforce_identity_map(
        throughput["identity_map"],
        expected_ccn="390001",
        expected_state="PA",
        expected_name="EXAMPLE HOSPITAL",
        expected_sources={"public_hospital_throughput"},
    )
    throughput_claim = _workforce_source_claim(throughput["identity_map"], "public_hospital_throughput")
    assert "bed_source.selected_candidate_evidence" in throughput_claim["row_evidence_paths"]
    assert throughput_claim["metric_evidence_paths"] == ["metric_evidence.*"]
    _assert_workforce_evidence(ed["evidence"], dataset_id="public_hospital_throughput")
    _assert_workforce_source_metadata(ed)
    _assert_workforce_evidence(ed["source_profile_evidence"], dataset_id="public_hospital_throughput")
    assert set(ed["metric_confidence"]) == {"ed_visits", "inpatient_admissions_from_ed"}
    assert set(ed["metric_evidence"]) == set(ed["metric_confidence"])
    _assert_workforce_metric_evidence(
        ed["metric_evidence"]["ed_visits"],
        dataset_id="public_hospital_throughput",
        match_basis="ccn_or_state_facility_id_public_source_lookup_metric_ed_visits",
        metric_name="ed_visits",
    )
    assert ed["identity"]["match_decisions"][0]["basis"] == "ccn_or_state_public_ed_volume_lookup"
    _assert_workforce_identity_map(ed["identity_map"], expected_ccn="390001", expected_state="PA")
    _assert_workforce_evidence(procedures["evidence"], dataset_id="public_hospital_throughput")
    _assert_workforce_source_metadata(procedures)
    _assert_workforce_evidence(procedures["source_profile_evidence"], dataset_id="public_hospital_throughput")
    assert set(procedures["metric_confidence"]) == {
        "or_procedure_volumes",
        "ct_scans",
        "mri_scans",
        "cardiac_catheterizations",
        "open_heart_procedures",
    }
    assert set(procedures["metric_evidence"]) == set(procedures["metric_confidence"])
    _assert_workforce_metric_evidence(
        procedures["metric_evidence"]["or_procedure_volumes"],
        dataset_id="public_hospital_throughput",
        match_basis="ccn_or_state_facility_id_public_source_lookup_metric_or_procedure_volumes",
        metric_name="or_procedure_volumes",
    )
    assert procedures["identity"]["match_decisions"][0]["basis"] == "ccn_or_state_public_or_procedure_lookup"
    _assert_workforce_identity_map(procedures["identity_map"], expected_ccn="390001", expected_state="PA")


@pytest.mark.asyncio
async def test_compare_public_throughput_profiles_keep_profile_receipts(monkeypatch):
    async def fake_pos_row(ccn: str):
        return None

    async def fake_ahrq_hospital_row(ccn: str):
        return {"hospital_name": "Example Hospital", "hosp_state": "PA"}

    async def fake_cost_report_row(ccn: str, year: int = 0):
        return pd.Series({"beds": "100", "total_discharges": "5000", "total_inpatient_days": "25000"})

    async def fake_hospital_linkage():
        return pd.DataFrame([{"ccn": "390001", "hosp_state": "PA"}])

    monkeypatch.setattr(server.ahrq_data, "load_ahrq_hospital_linkage", fake_hospital_linkage)
    monkeypatch.setattr(server, "_ahrq_hospital_row", fake_ahrq_hospital_row)
    monkeypatch.setattr(server, "_cost_report_row", fake_cost_report_row)
    monkeypatch.setattr(server, "_pos_row", fake_pos_row)
    monkeypatch.setattr(operations_data, "_load_state_health_data", lambda: None)

    result = await server.compare_public_throughput(state="PA", year=2025)

    assert result["total_results"] == 1
    _assert_workforce_evidence(result["evidence"], dataset_id="public_hospital_throughput")
    _assert_workforce_source_metadata(result)
    _assert_workforce_evidence(result["profiles"][0]["evidence"], dataset_id="public_hospital_throughput")
    _assert_workforce_row_evidence(
        result["profiles"][0]["bed_source"]["selected_candidate_evidence"],
        dataset_id="hospital_bed_identity_resolution",
        match_basis="hospital_bed_source_selected_candidate_row",
    )
    _assert_workforce_identity_map(
        result["profiles"][0]["identity_map"],
        expected_ccn="390001",
        expected_state="PA",
        expected_name="EXAMPLE HOSPITAL",
        expected_sources={"public_hospital_throughput"},
    )


@pytest.mark.asyncio
async def test_workforce_staffing_no_data_paths_include_evidence(monkeypatch):
    async def fake_ensure_hcris_cached():
        return True

    async def fake_pbj_staffing(**_kwargs):
        return []

    monkeypatch.setattr(server.workforce_data, "ensure_hcris_cached", fake_ensure_hcris_cached)
    monkeypatch.setattr(server.workforce_data, "query_pbj_staffing", fake_pbj_staffing)
    monkeypatch.setattr(server.workforce_data, "query_hcris_staffing", lambda ccn, year=0: {})

    pbj = await server.get_staffing_benchmarks(ccn="390999", facility_type="nursing_home")
    hcris_benchmark = await server.get_staffing_benchmarks(ccn="390999", facility_type="hospital")
    cost_report = await server.get_cost_report_staffing(ccn="390999", year=2025)

    _assert_workforce_no_data(
        pbj,
        dataset_id="cms_pbj_nursing_staffing",
        match_basis="ccn_or_state_pbj_lookup_no_match",
        ccn="390999",
    )
    _assert_workforce_no_data(
        hcris_benchmark,
        dataset_id="cms_hcris_workforce_staffing",
        match_basis="ccn_exact_hcris_staffing_no_match",
        ccn="390999",
    )
    _assert_workforce_no_data(
        cost_report,
        dataset_id="cms_hcris_workforce_staffing",
        match_basis="ccn_exact_hcris_staffing_no_match",
        ccn="390999",
    )
    assert cost_report["evidence"]["query"] == {"ccn": "390999", "year": 2025}


@pytest.mark.asyncio
async def test_workforce_staffing_rows_include_row_evidence(monkeypatch):
    async def fake_ensure_hcris_cached():
        return True

    async def fake_pbj_staffing(**_kwargs):
        return [
            {
                "ccn": "390001",
                "facility_name": "Example SNF",
                "rn_hprd": 1.2,
                "lpn_hprd": 0.8,
                "cna_hprd": 2.1,
                "total_nurse_hprd": 4.1,
                "date": "2025Q4",
            }
        ]

    monkeypatch.setattr(server.workforce_data, "ensure_hcris_cached", fake_ensure_hcris_cached)
    monkeypatch.setattr(server.workforce_data, "query_pbj_staffing", fake_pbj_staffing)
    monkeypatch.setattr(
        server.workforce_data,
        "query_hcris_staffing",
        lambda ccn, year=0: {
            "total_ftes": 110.5,
            "departments": [
                {
                    "dept_name": "Intensive Care Unit",
                    "total_ftes": 10.0,
                    "rn_ftes": 8.0,
                    "lpn_ftes": 1.0,
                    "aide_ftes": 1.0,
                }
            ],
        },
    )

    pbj = await server.get_snf_nursing_hprd(ccn="390001", quarter="2025Q4")
    cost_report = await server.get_cost_report_staffing(ccn="390001", year=2025)

    _assert_workforce_row_evidence(
        pbj["records"][0]["evidence"],
        dataset_id="cms_pbj_nursing_staffing",
        match_basis="pbj_daily_staffing_source_row",
    )
    _assert_workforce_row_evidence(
        cost_report["departments"][0]["evidence"],
        dataset_id="cms_hcris_workforce_staffing",
        match_basis="hcris_department_staffing_row",
    )
    pbj_claim = _workforce_source_claim(pbj["identity_map"], "cms_pbj_nursing_staffing")
    assert pbj_claim["row_evidence_paths"] == ["records[].evidence"]
    staffing_claim = _workforce_source_claim(cost_report["identity_map"], "cms_hcris_staffing")
    assert staffing_claim["row_evidence_paths"] == ["departments[].evidence"]


@pytest.mark.asyncio
async def test_throughput_profile_rejects_impossible_direct_beds_and_uses_bed_days(monkeypatch):
    async def fake_ahrq_hospital_row(ccn: str):
        return {"hospital_name": "Example Hospital", "hosp_state": "PA"}

    async def fake_cost_report_row(ccn: str, year: int = 0):
        return pd.Series(
            {
                "beds": "36500",
                "total_bed_days_available": "36500",
                "total_discharges": "5000",
                "total_inpatient_days": "25000",
            }
        )

    profile = await operations_data.throughput_profile(
        ccn="390001",
        state="PA",
        year=2025,
        hospital_row_loader=fake_ahrq_hospital_row,
        cost_report_row_loader=fake_cost_report_row,
    )

    assert profile["beds"] == 100.0
    assert profile["bed_turnover_rate"] == 50.0
    rejected = {row["source_field"]: row["rejection_reason"] for row in profile["bed_source"]["rejected_candidates"]}
    assert rejected["beds"] == "bed_value_above_ccn_ceiling_5000"


def test_query_hcris_staffing_maps_s3_total_rn_lpn_aide_by_provider_year(tmp_path: Path, monkeypatch):
    hcris_cache = tmp_path / "hcris_staffing.parquet"
    _write_parquet(pd.DataFrame(
        [
            {
                "prvdr_num": "390001",
                "wksht_cd": "S300001",
                "line_num": "00100",
                "clmn_num": "00100",
                "itm_val_num": "100.5",
                "fy_end_dt": "2025-12-31",
            },
            {
                "prvdr_num": "390001",
                "wksht_cd": "S300001",
                "line_num": "00100",
                "clmn_num": "00200",
                "itm_val_num": "60.25",
                "fy_end_dt": "2025-12-31",
            },
            {
                "prvdr_num": "390001",
                "wksht_cd": "S300001",
                "line_num": "00100",
                "clmn_num": "00300",
                "itm_val_num": "25",
                "fy_end_dt": "2025-12-31",
            },
            {
                "prvdr_num": "390001",
                "wksht_cd": "S300001",
                "line_num": "00100",
                "clmn_num": "00400",
                "itm_val_num": "15.25",
                "fy_end_dt": "2025-12-31",
            },
            {
                "prvdr_num": "390001",
                "wksht_cd": "S300001",
                "line_num": "00200",
                "clmn_num": "00100",
                "itm_val_num": "10",
                "fy_end_dt": "2025-12-31",
            },
            {
                "prvdr_num": "390001",
                "wksht_cd": "S300001",
                "line_num": "00100",
                "clmn_num": "00100",
                "itm_val_num": "999",
                "fy_end_dt": "2024-12-31",
            },
        ]
    ), hcris_cache)
    monkeypatch.setattr(workforce_data, "_HCRIS_CACHE", hcris_cache)

    result = workforce_data.query_hcris_staffing("390001", year=2025)

    assert result is not None
    assert result["total_ftes"] == 110.5
    departments = {department["dept_name"]: department for department in result["departments"]}
    adults_peds = departments["Hospital Adults & Pediatrics"]
    assert adults_peds["total_ftes"] == 100.5
    assert adults_peds["rn_ftes"] == 60.25
    assert adults_peds["lpn_ftes"] == 25.0
    assert adults_peds["aide_ftes"] == 15.25
    assert departments["Intensive Care Unit"]["total_ftes"] == 10.0


def test_query_hcris_gme_filters_by_provider_year(tmp_path: Path, monkeypatch):
    hcris_cache = tmp_path / "hcris_staffing.parquet"
    _write_parquet(pd.DataFrame(
        [
            {
                "prvdr_num": "390001",
                "wksht_cd": "S200001",
                "line_num": "06600",
                "clmn_num": "00100",
                "itm_val_num": "12.5",
                "fy_end_dt": "2025-12-31",
            },
            {
                "prvdr_num": "390001",
                "wksht_cd": "S200001",
                "line_num": "06600",
                "clmn_num": "00100",
                "itm_val_num": "0",
                "fy_end_dt": "2024-12-31",
            },
        ]
    ), hcris_cache)
    monkeypatch.setattr(workforce_data, "_HCRIS_CACHE", hcris_cache)

    result = workforce_data.query_hcris_gme("390001", year=2025)

    assert result == {"ccn": "390001", "teaching_status": "Teaching", "total_resident_ftes": 12.5}


@pytest.mark.asyncio
async def test_pa_admissions_hook_requires_existing_normalized_state_tables(monkeypatch):
    async def fake_ahrq_hospital_row(ccn: str):
        return {"hospital_name": "Example Hospital", "hosp_state": "PA"}

    async def fake_cost_report_row(ccn: str, year: int = 0):
        return pd.Series({"beds": "100", "total_discharges": "5000", "total_inpatient_days": "25000"})

    monkeypatch.setattr(operations_data, "_load_state_health_data", lambda: None)

    profile = await operations_data.throughput_profile(
        ccn="390001",
        state="PA",
        year=2025,
        hospital_row_loader=fake_ahrq_hospital_row,
        cost_report_row_loader=fake_cost_report_row,
    )

    assert profile["state"] == "PA"
    assert profile["pa_admissions_enhancement"] is None
