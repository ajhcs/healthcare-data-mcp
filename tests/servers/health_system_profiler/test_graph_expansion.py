"""Tests for RELATED_PROVIDER_NUMBER graph expansion."""

import pandas as pd
import pytest

from servers.health_system_profiler.graph_expansion import expand_related_providers


@pytest.fixture
def pos_with_related():
    """POS data with related provider linkages."""
    return pd.DataFrame([
        # Parent hospital
        {"PRVDR_NUM": "390001", "FAC_NAME": "Jefferson Main", "RELATED_PROVIDER_NUMBER": "",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "BED_CNT": "900",
         "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01"},
        # Related dialysis center
        {"PRVDR_NUM": "392001", "FAC_NAME": "Jefferson Dialysis", "RELATED_PROVIDER_NUMBER": "390001",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "BED_CNT": "0",
         "PRVDR_CTGRY_CD": "11", "PRVDR_CTGRY_SBTYP_CD": ""},
        # Related rehab facility
        {"PRVDR_NUM": "393001", "FAC_NAME": "Jefferson Rehab", "RELATED_PROVIDER_NUMBER": "390001",
         "CITY_NAME": "Philadelphia", "STATE_CD": "PA", "BED_CNT": "40",
         "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "02"},
        # Unrelated facility
        {"PRVDR_NUM": "390500", "FAC_NAME": "Some Other Hospital", "RELATED_PROVIDER_NUMBER": "",
         "CITY_NAME": "Allentown", "STATE_CD": "PA", "BED_CNT": "200",
         "PRVDR_CTGRY_CD": "01", "PRVDR_CTGRY_SBTYP_CD": "01"},
    ])


def test_expand_related_providers(pos_with_related):
    subs = expand_related_providers(["390001"], pos_with_related)
    assert len(subs) == 2
    ccns = {s.ccn for s in subs}
    assert "392001" in ccns
    assert "393001" in ccns
    assert "390500" not in ccns


def test_expand_no_related(pos_with_related):
    subs = expand_related_providers(["390500"], pos_with_related)
    assert len(subs) == 0


def test_sub_entity_fields(pos_with_related):
    subs = expand_related_providers(["390001"], pos_with_related)
    dialysis = next(s for s in subs if s.ccn == "392001")
    assert dialysis.name == "Jefferson Dialysis"
    assert dialysis.parent_ccn == "390001"
    assert dialysis.city == "Philadelphia"
    assert dialysis.facility_type == "Renal Dialysis Facility"
