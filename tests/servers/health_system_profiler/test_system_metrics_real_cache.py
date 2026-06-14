"""Real-cache-shaped regression tests for health-system metrics."""

from __future__ import annotations

import json

import pandas as pd

from servers.health_system_profiler.system_metrics import get_health_system_metric, list_health_system_metric_rows


def test_real_cache_shaped_missing_values_and_leading_zeroes_are_safe() -> None:
    systems = pd.DataFrame(
        [
            {
                "health_sys_id": "HSI00000001",
                "health_sys_name": "Zero Lead Health",
                "health_sys_city": "Boston",
                "health_sys_state": "MA",
                "hosp_cnt": 2,
                "acutehosp_cnt": 1,
                "sys_beds": pd.NA,
                "total_mds": pd.NA,
                "prim_care_mds": pd.NA,
                "total_nps": pd.NA,
                "total_pas": pd.NA,
                "grp_cnt": pd.NA,
            }
        ]
    )
    for column in ("hosp_cnt", "acutehosp_cnt", "sys_beds", "total_mds", "prim_care_mds", "total_nps", "total_pas", "grp_cnt"):
        systems[column] = pd.to_numeric(systems[column], errors="coerce").astype("Int64")

    hospitals = pd.DataFrame(
        [
            {
                "compendium_hospital_id": "CHSP00000001",
                "ccn": "001234",
                "hospital_name": "Zero Lead Main",
                "hospital_street": "1 Main St",
                "hospital_city": "Boston",
                "hospital_state": "MA",
                "hospital_zip": "02108",
                "acutehosp_flag": 1,
                "health_sys_id": "HSI00000001",
                "health_sys_name": "Zero Lead Health",
                "hos_beds": pd.NA,
                "hos_dsch": pd.NA,
            },
            {
                "compendium_hospital_id": "CHSP00000002",
                "ccn": "",
                "hospital_name": "Missing CCN Campus",
                "hospital_street": "2 Main St",
                "hospital_city": "Boston",
                "hospital_state": "MA",
                "hospital_zip": "02109",
                "acutehosp_flag": 0,
                "health_sys_id": "HSI00000001",
                "health_sys_name": "Zero Lead Health",
                "hos_beds": pd.NA,
                "hos_dsch": pd.NA,
            },
        ]
    )
    for column in ("acutehosp_flag", "hos_beds", "hos_dsch"):
        hospitals[column] = pd.to_numeric(hospitals[column], errors="coerce").astype("Int64")

    listed = list_health_system_metric_rows(
        systems_df=systems,
        hospitals_df=hospitals,
        page_size=1,
        include_facilities=True,
    )
    exact = get_health_system_metric(
        systems_df=systems,
        hospitals_df=hospitals,
        system_id="HSI00000001",
        include_facilities=True,
    )

    json.dumps(listed)
    json.dumps(exact)
    hospital = exact["system"]["hospitals"][0]
    missing_ccn = exact["system"]["hospitals"][1]
    assert hospital["ccn"] == "001234"
    assert hospital["hospital_address"]["primary"]["zip_code"] == "02108"
    assert hospital["hospital_bed_count"]["primary"] is None
    assert exact["system"]["counts"]["physician_count"]["value"] is None
    assert exact["coverage"]["hospitals_missing_hcris_bed_data"]["status"] == "unavailable"
    assert missing_ccn["warnings"][0]["code"] == "missing_ccn"
