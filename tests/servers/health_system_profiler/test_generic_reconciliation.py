"""Coverage for generic AHRQ/CMS system facility reconciliation."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.health_system_profiler import server
from servers.health_system_profiler.generic_reconciliation import reconcile_generic_system_facilities
from servers.health_system_profiler.jefferson_resolver import JEFFERSON_SLUG


@pytest.fixture
def generic_systems() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "health_sys_id": "SYS_999",
                "health_sys_name": "Example Regional Health",
                "health_sys_city": "Lancaster",
                "health_sys_state": "PA",
                "hosp_count": 2,
            }
        ]
    )


@pytest.fixture
def generic_hospitals() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "health_sys_id": "SYS_999",
                "ccn": "390901",
                "hospital_name": "Example Regional Medical Center",
                "hosp_addr": "100 Main St",
                "hosp_city": "Lancaster",
                "hosp_state": "PA",
                "hosp_zip": "17601",
                "hos_beds": 120,
                "ownership": "Voluntary nonprofit",
                "teaching": "Yes",
            },
            {
                "health_sys_id": "SYS_999",
                "ccn": "390902",
                "hospital_name": "Example Valley Hospital",
                "hosp_addr": "200 Valley Rd",
                "hosp_city": "York",
                "hosp_state": "PA",
                "hosp_zip": "17401",
                "hos_beds": 80,
                "ownership": "Voluntary nonprofit",
                "teaching": "No",
            },
        ]
    )


@pytest.fixture
def generic_pos() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "PRVDR_NUM": "390901",
                "FAC_NAME": "Example Regional Medical Center",
                "ST_ADR": "100 CMS Main St",
                "CITY_NAME": "Lancaster",
                "STATE_CD": "PA",
                "ZIP_CD": "17602",
                "COUNTY_NAME": "Lancaster",
                "PHNE_NUM": "7175550100",
                "BED_CNT": "130",
                "CRTFD_BED_CNT": "125",
                "CRDC_CTHRTZTN_LAB_SRVC_CD": "1",
                "DCTD_ER_SRVC_CD": "1",
            }
        ]
    )


@pytest.fixture
def generic_provider_enrollment() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ccn": "390902",
                "facility_name": "Example Valley Hospital",
                "provider_name": "Example Valley Hospital",
            }
        ]
    )


def test_generic_reconciliation_resolves_system_id_and_enriches_ccns(
    generic_systems,
    generic_hospitals,
    generic_pos,
    generic_provider_enrollment,
):
    ledger = reconcile_generic_system_facilities(
        "SYS_999",
        as_of_date="2026-04-28",
        systems_df=generic_systems,
        ahrq_hospitals=generic_hospitals,
        cms_hgi=generic_pos,
        provider_enrollment=generic_provider_enrollment,
    )

    assert ledger["system_slug"] == "example-regional-health"
    assert ledger["system_id"] == "SYS_999"
    assert ledger["facility_count"] == 2
    assert ledger["alias_ledger"] == []
    assert ledger["merger_evidence"] == []
    assert ledger["discrepancy_closure"] is None
    assert "not a curated merger ledger" in ledger["method_note"]
    assert "not a curated merger ledger" in ledger["source_evidence"]["note"]

    by_ccn = {facility["ccn"]: facility for facility in ledger["facilities"]}
    assert set(by_ccn) == {"390901", "390902"}
    assert by_ccn["390901"]["beds"]["total"] == 130
    assert by_ccn["390901"]["city"] == "Lancaster"
    assert by_ccn["390901"]["source_refs"] == [
        "ahrq_compendium_2023",
        "ahrq_hospital_linkage_row",
        "cms_pos_row",
    ]
    assert "provider_enrollment_row" in by_ccn["390902"]["source_refs"]
    assert by_ccn["390902"]["confidence"] == 0.9
    assert "no_ccn_reason" not in by_ccn["390901"]


def test_generic_reconciliation_resolves_normalized_name_slug(
    generic_systems,
    generic_hospitals,
    generic_pos,
):
    ledger = reconcile_generic_system_facilities(
        "example-regional-health",
        as_of_date="2026-04-28",
        systems_df=generic_systems,
        ahrq_hospitals=generic_hospitals,
        cms_hgi=generic_pos,
    )

    assert ledger["system_id"] == "SYS_999"
    assert ledger["facility_count"] == 2


def test_generic_reconciliation_explains_missing_ccn(generic_systems, generic_hospitals):
    hospitals = pd.concat(
        [
            generic_hospitals,
            pd.DataFrame(
                [
                    {
                        "health_sys_id": "SYS_999",
                        "ccn": "",
                        "hospital_name": "Example Pending Hospital",
                        "hosp_city": "Reading",
                        "hosp_state": "PA",
                        "hos_beds": 25,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )

    ledger = reconcile_generic_system_facilities(
        "Example Regional Health",
        as_of_date="2026-04-28",
        systems_df=generic_systems,
        ahrq_hospitals=hospitals,
    )

    pending = next(facility for facility in ledger["facilities"] if facility["name"] == "Example Pending Hospital")
    assert pending["ccn"] == ""
    assert pending["no_ccn_reason"] == "AHRQ hospital linkage row did not include a CCN"
    assert pending["active_status"] == "active"


@pytest.mark.asyncio
async def test_reconcile_system_facilities_tool_uses_generic_path_without_stale_error(
    generic_systems,
    generic_hospitals,
    generic_pos,
    generic_provider_enrollment,
):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=generic_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=generic_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=generic_pos),
        patch.object(server, "_load_provider_enrollment", return_value=generic_provider_enrollment),
    ):
        result = await server.reconcile_system_facilities(system_slug="SYS_999", as_of_date="2026-04-28")

    assert "error" not in result
    assert result["system_id"] == "SYS_999"
    assert result["facility_count"] == 2
    assert {facility["ccn"] for facility in result["facilities"]} == {"390901", "390902"}
    assert result["alias_ledger"] == []
    assert result["merger_evidence"] == []
    assert result["discrepancy_closure"] is None
    assert "not a curated merger ledger" in result["source_evidence"]["note"]


@pytest.mark.asyncio
async def test_get_system_profile_includes_generic_facility_reconciliation(
    generic_systems,
    generic_hospitals,
    generic_pos,
    generic_provider_enrollment,
):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=generic_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=generic_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=generic_pos),
        patch.object(server, "_load_provider_enrollment", return_value=generic_provider_enrollment),
        patch.object(server, "_search_nppes", new_callable=AsyncMock, return_value=[]),
    ):
        result = await server.get_system_profile(
            system_name="Example Regional Health",
            edition_date="2026-04-28",
            include_outpatient=False,
        )

    assert result["system"]["system_id"] == "SYS_999"
    assert result["system"]["name"] == "Example Regional Health"
    assert len(result["inpatient_facilities"]) == 2

    ledger = result["facility_reconciliation"]
    assert ledger["system_id"] == "SYS_999"
    assert ledger["system_slug"] == "example-regional-health"
    assert ledger["as_of_date"] == "2026-04-28"
    assert ledger["facility_count"] == 2
    assert ledger["alias_ledger"] == []
    assert ledger["merger_evidence"] == []
    assert ledger["discrepancy_closure"] is None
    assert "not a curated merger ledger" in ledger["method_note"]
    assert {facility["ccn"] for facility in ledger["facilities"]} == {"390901", "390902"}


@pytest.mark.asyncio
async def test_reconcile_system_facilities_tool_keeps_jefferson_special_case_unchanged():
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock) as load_systems,
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock) as load_hospitals,
        patch.object(server, "_load_pos", new_callable=AsyncMock) as load_pos,
    ):
        result = await server.reconcile_system_facilities(system_slug="Jefferson Health", as_of_date="2026-04-28")

    load_systems.assert_not_awaited()
    load_hospitals.assert_not_awaited()
    load_pos.assert_not_awaited()
    assert result["system_slug"] == JEFFERSON_SLUG
    assert result["facility_count"] == 32
    assert len(result["merger_evidence"]) >= 4
