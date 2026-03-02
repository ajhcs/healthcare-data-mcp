"""Tests for POS-based facility enrichment."""

import pandas as pd
import pytest

from servers.health_system_profiler.facility_enrichment import (
    enrich_facility,
    aggregate_off_site,
)


@pytest.fixture
def sample_pos_df():
    """Minimal POS-like DataFrame with key columns."""
    return pd.DataFrame([
        {
            "PRVDR_NUM": "390001",
            "FAC_NAME": "Thomas Jefferson University Hospital",
            "ST_ADR": "111 S 11th St",
            "CITY_NAME": "Philadelphia",
            "STATE_CD": "PA",
            "ZIP_CD": "19107",
            "COUNTY_NAME": "Philadelphia",
            "PHNE_NUM": "2155556789",
            "BED_CNT": "900",
            "CRTFD_BED_CNT": "880",
            "PSYCH_UNIT_BED_CNT": "50",
            "REHAB_UNIT_BED_CNT": "30",
            "HOSPC_BED_CNT": "0",
            "VNTLTR_BED_CNT": "10",
            "AIDS_BED_CNT": "0",
            "ALZHMR_BED_CNT": "0",
            "DLYS_BED_CNT": "0",
            "CRDAC_CTHRTZTN_LAB_SW": "Y",
            "OPN_HRT_SRGRY_SW": "Y",
            "MRI_SRVC_SW": "Y",
            "CT_SCNR_SW": "Y",
            "PET_SCNR_SW": "N",
            "NUCLR_MED_SRVC_SW": "Y",
            "TRMA_CTR_SW": "Y",
            "TRMA_CTR_LVL_CD": "1",
            "BRNCTR_SW": "N",
            "NNTL_ICU_SW": "Y",
            "OBSTTRCL_SRVC_SW": "Y",
            "ORNG_TRNSP_SW": "N",
            "EMER_DEPT_SW": "Y",
            "RN_CNT": "2000",
            "LPN_CNT": "150",
            "MDCL_STAFF_PHYSCN_CNT": "500",
            "PHRMCST_CNT": "50",
            "THRPST_CNT": "100",
            "TOT_STFNG": "4500.5",
            "OPRTN_RM_CNT": "30",
            "ENDSCPY_RM_CNT": "8",
            "CRDAC_CTHRTZTN_LAB_RM_CNT": "4",
            "TOT_OFSITE_EMER_DEPT_CNT": "2",
            "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "5",
            "TOT_OFSITE_PSYCH_UNIT_CNT": "1",
            "TOT_OFSITE_REHAB_HOSP_CNT": "1",
            "RLTD_PRVDR_NMBR": "",
            "PRVDR_CTGRY_CD": "01",
            "PRVDR_CTGRY_SBTYP_CD": "01",
            "GNRL_CNTL_TYPE_CD": "04",
        },
    ])


def test_enrich_facility(sample_pos_df):
    facility = enrich_facility("390001", sample_pos_df)
    assert facility is not None
    assert facility.ccn == "390001"
    assert facility.name == "Thomas Jefferson University Hospital"
    assert facility.city == "Philadelphia"
    assert facility.state == "PA"
    # Beds
    assert facility.beds.total == 900
    assert facility.beds.certified == 880
    assert facility.beds.psychiatric == 50
    assert facility.beds.rehabilitation == 30
    # Services
    assert facility.services.cardiac_catheterization is True
    assert facility.services.open_heart_surgery is True
    assert facility.services.pet_scanner is False
    assert facility.services.trauma_center is True
    assert facility.services.trauma_level == "1"
    assert facility.services.emergency_department is True
    assert facility.services.operating_rooms == 30
    # Staffing
    assert facility.staffing.rn == 2000
    assert facility.staffing.physicians == 500
    assert facility.staffing.total_fte == 4500.5


def test_enrich_facility_not_found(sample_pos_df):
    facility = enrich_facility("999999", sample_pos_df)
    assert facility is None


def test_aggregate_off_site(sample_pos_df):
    summary = aggregate_off_site(["390001"], sample_pos_df)
    assert summary.emergency_departments == 2
    assert summary.urgent_care_centers == 5
    assert summary.psychiatric_units == 1
    assert summary.rehabilitation_hospitals == 1
    assert summary.total_off_site == 9
