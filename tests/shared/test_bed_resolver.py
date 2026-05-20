from __future__ import annotations

import pandas as pd

from shared.utils.bed_resolver import resolve_hospital_bed_source


def test_resolver_selects_pos_bed_count_with_provenance() -> None:
    result = resolve_hospital_bed_source(
        ccn="390001",
        pos_row=pd.Series({"BED_CNT": "250", "CRTFD_BED_CNT": "240"}),
    )

    assert result["selected_bed_count"] == 250
    assert result["selected_source"] == "CMS Provider of Services"
    assert result["selected_source_field"] == "BED_CNT"
    assert result["row_scope"] == "ccn"
    assert result["rejected_candidates"] == []


def test_resolver_prefers_hcris_direct_beds_over_pos_when_available() -> None:
    result = resolve_hospital_bed_source(
        ccn="390001",
        pos_row=pd.Series({"BED_CNT": "300"}),
        hcris_row=pd.Series({"beds": "225", "fy_end_dt": "2025-06-30"}),
        year=2025,
    )

    assert result["selected_bed_count"] == 225
    assert result["selected_source_field"] == "beds"
    assert result["fiscal_year_end"] == "2025-06-30"
    assert result["warnings"]


def test_resolver_derives_beds_from_explicit_bed_days() -> None:
    result = resolve_hospital_bed_source(
        ccn="390001",
        hcris_row=pd.Series({"total_bed_days_available": "36500", "fy_end_dt": "2025-12-31"}),
    )

    assert result["selected_bed_count"] == 100.0
    assert result["selected_source_field"] == "total_bed_days_available"
    assert result["confidence"] == "medium_derived_from_bed_days_available"


def test_resolver_rejects_impossible_direct_bed_value() -> None:
    result = resolve_hospital_bed_source(
        ccn="390001",
        hcris_row=pd.Series({"beds": "36500", "total_bed_days_available": "36500"}),
    )

    assert result["selected_bed_count"] == 100.0
    rejected = {row["source_field"]: row["rejection_reason"] for row in result["rejected_candidates"]}
    assert rejected["beds"] == "bed_value_above_ccn_ceiling_5000"


def test_resolver_rejects_fractional_direct_bed_count() -> None:
    result = resolve_hospital_bed_source(ccn="390001", hcris_row=pd.Series({"beds": "12.5"}))

    assert result["selected_bed_count"] is None
    assert result["rejected_candidates"][0]["rejection_reason"] == "fractional_direct_bed_count"


def test_resolver_keeps_license_scope_out_of_ccn_selection() -> None:
    pa_row = {
        "state": "PA",
        "facility_name": "Example Hospital",
        "state_facility_id": "LIC123",
        "row_scope": "license",
        "metric_name": "beds",
        "metric_value": "120",
        "raw_column": "Licensed Beds",
        "report_year": "2024",
    }

    ccn_result = resolve_hospital_bed_source(ccn="390001", state="PA", pa_rows=[pa_row], target_scope="ccn")
    license_result = resolve_hospital_bed_source(
        state_facility_id="LIC123",
        state="PA",
        pa_rows=[pa_row],
        target_scope="license",
    )

    assert ccn_result["selected_bed_count"] is None
    assert ccn_result["rejected_candidates"][0]["rejection_reason"] == "row_scope_license_not_compatible_with_target_ccn"
    assert license_result["selected_bed_count"] == 120
    assert license_result["row_scope"] == "license"
