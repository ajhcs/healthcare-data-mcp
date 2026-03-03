"""Tests for POS-based facility enrichment."""

import pandas as pd
import pytest

from servers.health_system_profiler.facility_enrichment import (
    enrich_facility,
    aggregate_off_site,
)


@pytest.fixture
def sample_pos_df():
    """Minimal POS-like DataFrame with actual Q4 2025 column names."""
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
            # Service codes: 0=no, 1=in-facility, 2=via agreement, 3=other
            "CRDC_CTHRTZTN_LAB_SRVC_CD": "1",
            "OPEN_HRT_SRGRY_SRVC_CD": "1",
            "MGNTC_RSNC_IMG_SRVC_CD": "1",
            "CT_SCAN_SRVC_CD": "1",
            "PET_SCAN_SRVC_CD": "0",
            "NUCLR_MDCN_SRVC_CD": "1",
            "SHCK_TRMA_SRVC_CD": "1",
            "BURN_CARE_UNIT_SRVC_CD": "0",
            "NEONTL_ICU_SRVC_CD": "1",
            "OB_SRVC_CD": "1",
            "ORGN_TRNSPLNT_SRVC_CD": "0",
            "DCTD_ER_SRVC_CD": "1",
            "RN_CNT": "2000",
            "LPN_CNT": "150",
            "PHYSN_CNT": "500",
            "REG_PHRMCST_CNT": "50",
            "OCPTNL_THRPST_CNT": "30",
            "PHYS_THRPST_CNT": "40",
            "INHLTN_THRPST_CNT": "30",
            "EMPLEE_CNT": "4500.5",
            "OPRTG_ROOM_CNT": "30",
            "ENDSCPY_PRCDR_ROOMS_CNT": "8",
            "CRDC_CTHRTZTN_PRCDR_ROOMS_CNT": "4",
            "TOT_OFSITE_EMER_DEPT_CNT": "2",
            "TOT_OFSITE_URGNT_CARE_CNTR_CNT": "5",
            "TOT_OFSITE_PSYCH_UNIT_CNT": "1",
            "TOT_OFSITE_REHAB_HOSP_CNT": "1",
            "RELATED_PROVIDER_NUMBER": "",
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
    assert facility.services.burn_care is False
    assert facility.services.emergency_department is True
    assert facility.services.operating_rooms == 30
    assert facility.services.endoscopy_rooms == 8
    assert facility.services.cardiac_cath_rooms == 4
    # Staffing
    assert facility.staffing.rn == 2000
    assert facility.staffing.physicians == 500
    assert facility.staffing.pharmacists == 50
    assert facility.staffing.therapists == 100  # 30 OT + 40 PT + 30 RT
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
