"""Tests for workforce-analytics ACGME import and query behavior."""

from pathlib import Path

import pandas as pd
import pytest
import duckdb

from servers.workforce_analytics import operations_data, server, workforce_data


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    con = duckdb.connect(":memory:")
    try:
        con.register("df", df)
        con.execute("COPY df TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(path)])
    finally:
        con.close()


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


def test_get_acgme_source_status_reads_valid_import_metadata(tmp_path: Path, monkeypatch):
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

    assert status["status"] == "ready"
    assert status["row_count"] == 1
    assert result is not None
    assert result["match_basis"] == ["program_id_exact"]


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

    monkeypatch.setattr(server.workforce_data, "ensure_hcris_cached", fake_ensure_hcris_cached)
    monkeypatch.setattr(
        server.workforce_data,
        "query_hcris_staffing",
        lambda ccn, year=0: {"total_ftes": 250.0, "departments": []},
    )
    monkeypatch.setattr(server.workforce_data, "query_hcris_gme", lambda ccn, year=0: {"total_resident_ftes": 25.0})
    monkeypatch.setattr(server, "_ahrq_hospital_row", fake_ahrq_hospital_row)
    monkeypatch.setattr(server, "_cost_report_row", fake_cost_report_row)
    monkeypatch.setattr(server, "_pos_row", fake_pos_row)

    profile = await server._productivity_profile("390001", year=2025)

    assert profile["fte_per_bed"] == 2.5
    assert profile["fte_per_discharge"] == 0.05
    assert profile["resident_to_bed_ratio"] == 0.25
    assert profile["case_mix_adjusted_discharges_per_fte"] == 24.0
    assert "case_mix_adjusted_discharges_per_fte" in profile
    assert profile["peer_group_metadata"]["attributes"]["bed_size"] == "100_299"
    assert profile["peer_group_metadata"]["attributes"]["teaching"] == "teaching"
    assert round(profile["fte_per_occupied_bed"], 4) == round(250 / (25000 / 365), 4)


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
    assert profile["pa_admissions_enhancement"] is None


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
