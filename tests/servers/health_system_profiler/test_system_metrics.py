"""Source-disciplined health-system metrics tests."""

from __future__ import annotations

import base64
from pathlib import Path
import json

import numpy as np
import pandas as pd
import pytest

from servers.health_system_profiler.data_loaders import parse_ahrq_hospital_linkage, parse_ahrq_system_file
from servers.health_system_profiler.system_metrics import (
    build_snapshot_id,
    get_health_system_metric,
    is_missing_scalar,
    json_safe,
    list_health_system_metric_rows,
    _frame_by_ccn,
    _int_or_none,
)


@pytest.fixture
def systems_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "health_sys_id": "HSI00000001",
                "health_sys_name": "Example Health",
                "health_sys_city": "Dothan",
                "health_sys_state": "AL",
                "total_mds": 10,
                "prim_care_mds": 4,
                "total_nps": 5,
                "total_pas": 3,
                "grp_cnt": 2,
                "hosp_cnt": 2,
                "acutehosp_cnt": 1,
                "sys_beds": 100,
                "sys_dsch": 1000,
            },
            {
                "health_sys_id": "HSI00000002",
                "health_sys_name": "Beta Health",
                "health_sys_city": "Tampa",
                "health_sys_state": "FL",
                "total_mds": 20,
                "prim_care_mds": 7,
                "total_nps": 8,
                "total_pas": 4,
                "grp_cnt": 3,
                "hosp_cnt": 1,
                "acutehosp_cnt": 1,
                "sys_beds": 200,
                "sys_dsch": 2000,
            },
        ]
    )


@pytest.fixture
def hospitals_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "compendium_hospital_id": "CHSP00000001",
                "ccn": "010001",
                "hospital_name": "Example Main",
                "hospital_street": "1 Main St",
                "hospital_city": "Dothan",
                "hospital_state": "AL",
                "hospital_zip": "00301",
                "acutehosp_flag": 1,
                "health_sys_id": "HSI00000001",
                "health_sys_name": "Example Health",
                "hos_beds": 90,
                "hos_dsch": 500,
            },
            {
                "compendium_hospital_id": "CHSP00000002",
                "ccn": "",
                "hospital_name": "Example Missing CCN",
                "hospital_street": "2 Main St",
                "hospital_city": "Dothan",
                "hospital_state": "AL",
                "hospital_zip": "00302",
                "acutehosp_flag": 0,
                "health_sys_id": "HSI00000001",
                "health_sys_name": "Example Health",
                "hos_beds": 30,
                "hos_dsch": 300,
            },
            {
                "compendium_hospital_id": "CHSP00000003",
                "ccn": "100001",
                "hospital_name": "Beta Main",
                "hospital_street": "3 Main St",
                "hospital_city": "Tampa",
                "hospital_state": "FL",
                "hospital_zip": "33601",
                "acutehosp_flag": 1,
                "health_sys_id": "HSI00000002",
                "health_sys_name": "Beta Health",
                "hos_beds": 200,
                "hos_dsch": 700,
            },
        ]
    )


def test_list_health_system_metrics_returns_universe_metadata_and_cursor(systems_df: pd.DataFrame, hospitals_df: pd.DataFrame) -> None:
    first = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        page_size=1,
        include_facilities=False,
    )

    assert first["universe"] == "ahrq_compendium_2023"
    assert first["data_mode"] == "compendium_snapshot"
    assert first["snapshot_year"] == 2023
    assert first["pagination"]["next_cursor"]
    assert first["coverage"]["total_systems_in_universe"] == 2
    assert first["systems"][0]["counts"]["physician_count"]["value"] == 10

    second = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        cursor=first["pagination"]["next_cursor"],
        page_size=1,
    )
    assert second["systems"][0]["system_id"] == "HSI00000002"
    assert second["pagination"]["next_cursor"] is None


def test_cursor_rejects_filter_mismatch_but_allows_page_size_change(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    first = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        page_size=1,
        sort="health_sys_id",
    )

    changed_page_size = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        cursor=first["pagination"]["next_cursor"],
        page_size=100,
        sort="health_sys_id",
    )
    assert changed_page_size["systems"][0]["system_id"] == "HSI00000002"

    changed_sort = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        cursor=first["pagination"]["next_cursor"],
        page_size=1,
        sort="bed_count",
    )
    assert changed_sort["error"]["code"] == "cursor_filter_mismatch"
    assert changed_sort["error"]["data"]["mismatches"]["sort"]["cursor"] == "health_sys_id"
    assert changed_sort["error"]["data"]["mismatches"]["sort"]["request"] == "bed_count"

    state_first = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        page_size=1,
        state_scope="facility_presence",
    )
    changed_state_scope = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        cursor=state_first["pagination"]["next_cursor"],
        page_size=1,
        state_scope="headquarters",
    )
    assert changed_state_scope["error"]["code"] == "cursor_filter_mismatch"
    assert "state_scope" in changed_state_scope["error"]["data"]["mismatches"]


def test_cursor_rejects_decoded_non_object_or_bad_offset(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    scalar_cursor = base64.urlsafe_b64encode(b"1").decode("ascii").rstrip("=")
    bad_offset_cursor = base64.urlsafe_b64encode(
        json.dumps(
            {
                "snapshot_id": build_snapshot_id(systems_df, hospitals_df),
                "offset": "1",
                "sort": "health_sys_id",
                "state": "",
                "state_scope": "headquarters",
                "as_of_mode": "compendium_snapshot",
            }
        ).encode("utf-8")
    ).decode("ascii").rstrip("=")

    scalar = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        cursor=scalar_cursor,
    )
    bad_offset = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        cursor=bad_offset_cursor,
    )

    assert scalar["error"]["code"] == "invalid_cursor"
    assert bad_offset["error"]["code"] == "invalid_cursor"


def test_frame_by_ccn_can_filter_to_requested_ccns() -> None:
    frame = pd.DataFrame(
        [
            {"facility_id": "010001", "address": "Included"},
            {"facility_id": "010002", "address": "Skipped"},
            {"facility_id": "100001", "address": "Also Included"},
        ]
    )

    index = _frame_by_ccn(frame, include_ccns={"010001", "100001"})

    assert set(index) == {"010001", "100001"}
    assert index["010001"]["address"] == "Included"
    assert "010002" not in index


def test_snapshot_id_uses_stable_content_hash(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    original = build_snapshot_id(systems_df, hospitals_df)
    reordered = build_snapshot_id(
        systems_df.iloc[::-1].reset_index(drop=True),
        hospitals_df.iloc[::-1].reset_index(drop=True),
    )
    changed_systems = systems_df.copy()
    changed_systems.loc[0, "sys_beds"] = 101
    changed_hospitals = hospitals_df.copy()
    changed_hospitals.loc[0, "ccn"] = "010099"

    assert reordered == original
    assert build_snapshot_id(changed_systems, hospitals_df) != original
    assert build_snapshot_id(systems_df, changed_hospitals) != original


def test_scalar_normalization_handles_pandas_and_numpy_missing_values() -> None:
    assert is_missing_scalar(pd.NA)
    assert is_missing_scalar(np.nan)
    assert is_missing_scalar("")
    assert is_missing_scalar(None)
    assert _int_or_none(pd.NA) is None
    assert _int_or_none(np.nan) is None
    assert _int_or_none("") is None
    assert _int_or_none(None) is None
    assert _int_or_none("298") == 298
    assert _int_or_none(298) == 298


def test_metric_response_is_json_safe_with_nullable_values(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    nullable_systems = systems_df.copy()
    nullable_systems["total_mds"] = pd.Series([10, pd.NA], dtype="Int64")
    nullable_hospitals = hospitals_df.copy()
    nullable_hospitals["hos_beds"] = pd.Series([90, pd.NA, 200], dtype="Int64")

    result = list_health_system_metric_rows(
        systems_df=nullable_systems,
        hospitals_df=nullable_hospitals,
        page_size=2,
        include_facilities=True,
    )

    json.dumps(result)
    assert json_safe(pd.NA) is None
    assert result["systems"][1]["counts"]["physician_count"]["value"] is None
    assert result["systems"][0]["hospitals"][1]["hospital_bed_count"]["primary"] is None


def test_invalid_arguments_return_explicit_errors(systems_df: pd.DataFrame, hospitals_df: pd.DataFrame) -> None:
    result = list_health_system_metric_rows(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        as_of_mode="current",
    )
    assert result["error"]["code"] == "INVALID_ARGUMENT"
    assert result["error"]["data"]["field"] == "as_of_mode"
    assert result["error"]["data"]["allowed_values"] == ["compendium_snapshot", "latest_public_overlay"]

    get_result = get_health_system_metric(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        system_id=None,
        system_name=None,
    )
    assert get_result["error"]["code"] == "INVALID_ARGUMENT"


def test_include_facilities_false_omits_hospital_payload(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    result = get_health_system_metric(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        system_id="HSI00000001",
        include_facilities=False,
    )

    assert "hospitals" not in result["system"]
    assert result["system"]["counts"]["linked_hospital_rows_count"]["value"] == 2


def test_compendium_snapshot_uses_ahrq_address_type_and_beds_even_with_overlay(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    hgi_df = pd.DataFrame(
        [
            {
                "facility_id": "010001",
                "facility_name": "Example Main",
                "address": "999 Current Ave",
                "city/town": "Dothan",
                "state": "AL",
                "zip_code": "36301",
                "hospital_type": "Critical Access Hospitals",
            }
        ]
    )
    pos_df = pd.DataFrame(
        [
            {
                "PRVDR_NUM": "010001",
                "ST_ADR": "888 POS Rd",
                "CITY_NAME": "DOTHAN",
                "STATE_CD": "AL",
                "ZIP_CD": "36301",
                "BED_CNT": "111",
                "CRTFD_BED_CNT": "110",
                "PRVDR_CTGRY_CD": "01",
            }
        ]
    )

    result = get_health_system_metric(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        system_id="HSI00000001",
        as_of_mode="compendium_snapshot",
        include_facilities=True,
        hgi_df=hgi_df,
        pos_df=pos_df,
    )

    hospital = result["system"]["hospitals"][0]
    assert hospital["hospital_bed_count"]["primary"] == 90
    assert hospital["hospital_address"]["primary"]["line1"] == "1 Main St"
    assert hospital["hospital_address"]["primary_basis"] == "compendium_snapshot"
    assert hospital["hospital_type"]["primary_basis"] == "compendium_snapshot"
    assert any(candidate["source"] == "CMS Hospital General Information" for candidate in hospital["hospital_address"]["candidates"])
    assert result["system"]["warnings"][0]["code"] == "sys_beds_acute_hos_beds_rollup_difference"


def test_latest_public_overlay_marks_cms_address_type_as_overlay_candidate(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    hgi_df = pd.DataFrame(
        [
            {
                "facility_id": "010001",
                "address": "999 Current Ave",
                "city/town": "Dothan",
                "state": "AL",
                "zip_code": "36301",
                "hospital_type": "Critical Access Hospitals",
            }
        ]
    )

    result = get_health_system_metric(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        system_id="HSI00000001",
        as_of_mode="latest_public_overlay",
        include_facilities=True,
        hgi_df=hgi_df,
    )

    hospital = result["system"]["hospitals"][0]
    assert result["data_mode"] == "latest_public_overlay"
    assert hospital["hospital_address"]["primary"]["line1"] == "999 Current Ave"
    assert hospital["hospital_address"]["primary_basis"] == "latest_public_overlay"
    assert hospital["hospital_type"]["cms_hgi_hospital_type_raw"] == "Critical Access Hospitals"


def test_get_health_system_metrics_low_confidence_name_returns_candidates(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    result = get_health_system_metric(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        system_name="Completely Different",
    )

    assert result["error"]["code"] == "ambiguous_system_name"
    assert result["candidates"]
    assert "system_id" in result["candidates"][0]


def test_get_health_system_metrics_duplicate_exact_name_returns_candidates(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    duplicate_systems = pd.DataFrame(
        [
            {
                **systems_df.iloc[0].to_dict(),
                "health_sys_id": "HSI00001106",
                "health_sys_name": "Trinity Health",
                "health_sys_city": "Livonia",
                "health_sys_state": "MI",
            },
            {
                **systems_df.iloc[1].to_dict(),
                "health_sys_id": "HSI00001107",
                "health_sys_name": "Trinity Health",
                "health_sys_city": "Minot",
                "health_sys_state": "ND",
            },
            {
                **systems_df.iloc[1].to_dict(),
                "health_sys_id": "HSI00000008",
                "health_sys_name": "Adena Health System",
            },
        ]
    )

    ambiguous = get_health_system_metric(
        systems_df=duplicate_systems,
        hospitals_df=hospitals_df,
        system_name="Trinity Health",
    )
    exact = get_health_system_metric(
        systems_df=duplicate_systems,
        hospitals_df=hospitals_df,
        system_id="HSI00001107",
    )
    unique = get_health_system_metric(
        systems_df=duplicate_systems,
        hospitals_df=hospitals_df,
        system_name="Adena Health System",
    )

    assert ambiguous["error"]["code"] == "ambiguous_system_name"
    assert {candidate["system_id"] for candidate in ambiguous["candidates"]} == {"HSI00001106", "HSI00001107"}
    assert exact["system"]["system_id"] == "HSI00001107"
    assert unique["system"]["system_id"] == "HSI00000008"


def test_medicare_public_clinician_roster_estimate_dedupes_by_npi(
    systems_df: pd.DataFrame,
    hospitals_df: pd.DataFrame,
) -> None:
    clinicians = pd.DataFrame(
        [
            {"npi": "1111111111", "cred": "MD", "pri_spec": "Internal Medicine", "facility_name": "Example Health Medical Group", "state": "AL"},
            {"npi": "1111111111", "cred": "MD", "pri_spec": "Internal Medicine", "facility_name": "Example Health Medical Group", "state": "AL"},
            {"npi": "2222222222", "cred": "DO", "pri_spec": "Cardiology", "facility_name": "Example Main", "state": "AL"},
            {"npi": "3333333333", "cred": "PA", "pri_spec": "Physician Assistant", "facility_name": "Example Health Medical Group", "state": "AL"},
            {"npi": "4444444444", "cred": "MD", "pri_spec": "Internal Medicine", "facility_name": "Other Group", "state": "AL"},
        ]
    )

    result = get_health_system_metric(
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        system_id="HSI00000001",
        include_facilities=True,
        include_medicare_public_clinician_roster_estimate=True,
        clinicians_df=clinicians,
    )

    estimate = result["system"]["medicare_public_clinician_roster_estimate"]
    assert estimate["status"] == "experimental_candidate"
    assert estimate["dedupe_key"] == "npi"
    assert estimate["value"] == 2


def test_canonical_cache_golden_counts_when_available() -> None:
    cache_root = Path.home() / ".healthcare-data-mcp" / "cache"
    system_path = cache_root / "ahrq_system_2023.csv"
    linkage_path = cache_root / "ahrq_hospital_linkage_2023.csv"
    if not system_path.exists() or not linkage_path.exists():
        pytest.skip("Canonical AHRQ cache files are not present")

    systems = parse_ahrq_system_file(system_path)
    hospitals = parse_ahrq_hospital_linkage(linkage_path)
    linked = hospitals[hospitals["health_sys_id"].astype(str).str.strip() != ""]
    acute = linked[pd.to_numeric(linked["acutehosp_flag"], errors="coerce").fillna(0).astype(int) == 1]

    assert len(systems) == 639
    assert len(hospitals) == 6800
    assert len(linked) == 4193
    assert len(acute) == 3602
