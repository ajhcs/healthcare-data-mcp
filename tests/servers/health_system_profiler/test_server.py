"""Tests for the health-system-profiler MCP server tools.

Uses monkeypatching to avoid real data downloads.
"""

import json
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from servers.health_system_profiler import server


@pytest.fixture
def mock_ahrq_systems():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "health_sys_name": "Jefferson Health",
         "health_sys_city": "Philadelphia", "health_sys_state": "PA", "hosp_count": 2, "phys_grp_count": 10},
    ])


@pytest.fixture
def mock_ahrq_hospitals():
    return pd.DataFrame([
        {"health_sys_id": "SYS_001", "ccn": "390001", "hospital_name": "Jefferson Main",
         "hosp_city": "Philadelphia", "hosp_state": "PA", "hosp_zip": "19107", "hos_beds": 900, "hos_dsch": 40000},
        {"health_sys_id": "SYS_001", "ccn": "390149", "hospital_name": "Jefferson Einstein",
         "hosp_city": "Philadelphia", "hosp_state": "PA", "hosp_zip": "19141", "hos_beds": 500, "hos_dsch": 20000},
    ])


@pytest.fixture
def mock_pos():
    return pd.DataFrame([
        {"PRVDR_NUM": "390001", "FAC_NAME": "Jefferson Main", "ST_ADR": "111 S 11th St",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "ZIP_CD": "19107", "COUNTY_NAME": "Philadelphia",
         "PHNE_NUM": "2155551234", "BED_CNT": "900", "CRTFD_BED_CNT": "880",
         "PSYCH_UNIT_BED_CNT": "50", "REHAB_UNIT_BED_CNT": "30", "HOSPC_BED_CNT": "0",
         "VNTLTR_BED_CNT": "10", "AIDS_BED_CNT": "0", "ALZHMR_BED_CNT": "0", "DLYS_BED_CNT": "0",
         "CRDAC_CTHRTZTN_LAB_SW": "Y", "OPN_HRT_SRGRY_SW": "Y", "MRI_SRVC_SW": "Y",
         "CT_SCNR_SW": "Y", "PET_SCNR_SW": "N", "NUCLR_MED_SRVC_SW": "Y",
         "TRMA_CTR_SW": "Y", "TRMA_CTR_LVL_CD": "1", "BRNCTR_SW": "N", "NNTL_ICU_SW": "Y",
         "OBSTTRCL_SRVC_SW": "Y", "ORNG_TRNSP_SW": "N", "EMER_DEPT_SW": "Y",
         "RN_CNT": "2000", "LPN_CNT": "150", "MDCL_STAFF_PHYSCN_CNT": "500",
         "PHRMCST_CNT": "50", "THRPST_CNT": "100", "TOT_STFNG": "4500",
         "OPRTN_RM_CNT": "30", "ENDSCPY_RM_CNT": "8", "CRDAC_CTHRTZTN_LAB_RM_CNT": "4",
         "TOT_OFSITE_EMER_DEPT_CNT": "2", "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "5",
         "TOT_OFSITE_PSYCH_UNIT_CNT": "1", "TOT_OFSITE_REHAB_HOSP_CNT": "1",
         "RLTD_PRVDR_NMBR": "", "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01",
         "GNRL_CNTL_TYPE_CD": "04"},
        {"PRVDR_NUM": "390149", "FAC_NAME": "Jefferson Einstein", "ST_ADR": "5501 Old York Rd",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "ZIP_CD": "19141", "COUNTY_NAME": "Philadelphia",
         "PHNE_NUM": "2155555678", "BED_CNT": "500", "CRTFD_BED_CNT": "490",
         "PSYCH_UNIT_BED_CNT": "20", "REHAB_UNIT_BED_CNT": "10", "HOSPC_BED_CNT": "0",
         "VNTLTR_BED_CNT": "5", "AIDS_BED_CNT": "0", "ALZHMR_BED_CNT": "0", "DLYS_BED_CNT": "0",
         "CRDAC_CTHRTZTN_LAB_SW": "Y", "OPN_HRT_SRGRY_SW": "N", "MRI_SRVC_SW": "Y",
         "CT_SCNR_SW": "Y", "PET_SCNR_SW": "N", "NUCLR_MED_SRVC_SW": "N",
         "TRMA_CTR_SW": "N", "TRMA_CTR_LVL_CD": "", "BRNCTR_SW": "N", "NNTL_ICU_SW": "N",
         "OBSTTRCL_SRVC_SW": "Y", "ORNG_TRNSP_SW": "N", "EMER_DEPT_SW": "Y",
         "RN_CNT": "1000", "LPN_CNT": "80", "MDCL_STAFF_PHYSCN_CNT": "200",
         "PHRMCST_CNT": "25", "THRPST_CNT": "50", "TOT_STFNG": "2200",
         "OPRTN_RM_CNT": "15", "ENDSCPY_RM_CNT": "4", "CRDAC_CTHRTZTN_LAB_RM_CNT": "2",
         "TOT_OFSITE_EMER_DEPT_CNT": "1", "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "3",
         "TOT_OFSITE_PSYCH_UNIT_CNT": "0", "TOT_OFSITE_REHAB_HOSP_CNT": "0",
         "RLTD_PRVDR_NMBR": "", "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01",
         "GNRL_CNTL_TYPE_CD": "04"},
    ])


@pytest.mark.asyncio
async def test_search_health_systems(mock_ahrq_systems):
    with patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems):
        result = json.loads(await server.search_health_systems("Jefferson"))
    assert "results" in result
    assert len(result["results"]) >= 1
    assert result["results"][0]["name"] == "Jefferson Health"


@pytest.mark.asyncio
async def test_get_system_profile(mock_ahrq_systems, mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
        patch.object(server, "_search_nppes", new_callable=AsyncMock, return_value=[]),
    ):
        result = json.loads(await server.get_system_profile(system_name="Jefferson Health"))
    assert result["system"]["name"] == "Jefferson Health"
    assert result["system"]["hospital_count"] == 2
    assert len(result["inpatient_facilities"]) == 2
    # Check enrichment worked
    main = next(f for f in result["inpatient_facilities"] if f["ccn"] == "390001")
    assert main["beds"]["total"] == 900
    assert main["services"]["cardiac_catheterization"] is True


@pytest.mark.asyncio
async def test_get_system_profile_not_found(mock_ahrq_systems, mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_systems", new_callable=AsyncMock, return_value=mock_ahrq_systems),
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
    ):
        result = json.loads(await server.get_system_profile(system_name="Mayo Clinic"))
    assert "error" in result


@pytest.mark.asyncio
async def test_get_system_facilities(mock_ahrq_hospitals, mock_pos):
    with (
        patch.object(server, "_load_ahrq_hospitals", new_callable=AsyncMock, return_value=mock_ahrq_hospitals),
        patch.object(server, "_load_pos", new_callable=AsyncMock, return_value=mock_pos),
    ):
        result = json.loads(await server.get_system_facilities(system_id="SYS_001"))
    assert result["system_id"] == "SYS_001"
    assert len(result["inpatient_facilities"]) == 2
