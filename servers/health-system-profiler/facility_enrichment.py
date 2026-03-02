"""POS-based facility enrichment — beds, services, staffing, off-site counts."""

import logging

import pandas as pd

from .models import (
    BedBreakdown,
    FacilitySummary,
    OffSiteSummary,
    ServiceCapabilities,
    StaffingCounts,
)

logger = logging.getLogger(__name__)

# POS column name candidates (in priority order)
_CCN_COLS = ["PRVDR_NUM", "PROVIDER_NUMBER", "CCN"]
_NAME_COLS = ["FAC_NAME", "FACILITY_NAME", "PRVDR_NAME"]
_ADDR_COLS = ["ST_ADR", "STREET_ADDRESS", "ADDRESS"]
_CITY_COLS = ["CITY_NAME", "CITY"]
_STATE_COLS = ["STATE_CD", "STATE"]
_ZIP_COLS = ["ZIP_CD", "ZIP_CODE", "ZIP"]
_COUNTY_COLS = ["COUNTY_NAME", "COUNTY"]
_PHONE_COLS = ["PHNE_NUM", "PHONE_NUMBER", "PHONE"]


def _find(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _safe_int(row, col: str, default: int = 0) -> int:
    if col not in row.index:
        return default
    try:
        return int(float(str(row[col]).strip() or "0"))
    except (ValueError, TypeError):
        return default


def _safe_float(row, col: str, default: float = 0.0) -> float:
    if col not in row.index:
        return default
    try:
        return float(str(row[col]).strip() or "0")
    except (ValueError, TypeError):
        return default


def _is_yes(row, col: str) -> bool:
    if col not in row.index:
        return False
    return str(row[col]).strip().upper() in ("Y", "YES", "1", "TRUE")


def enrich_facility(ccn: str, pos_df: pd.DataFrame) -> FacilitySummary | None:
    """Look up a CCN in the POS DataFrame and return an enriched FacilitySummary.

    Returns None if CCN not found.
    """
    ccn_col = _find(pos_df, _CCN_COLS)
    if not ccn_col:
        logger.warning("Cannot find CCN column in POS data")
        return None

    matches = pos_df[pos_df[ccn_col].astype(str).str.strip().str.zfill(6) == ccn.strip().zfill(6)]
    if matches.empty:
        return None

    row = matches.iloc[0]

    name_col = _find(pos_df, _NAME_COLS) or ""
    addr_col = _find(pos_df, _ADDR_COLS) or ""
    city_col = _find(pos_df, _CITY_COLS) or ""
    state_col = _find(pos_df, _STATE_COLS) or ""
    zip_col = _find(pos_df, _ZIP_COLS) or ""
    county_col = _find(pos_df, _COUNTY_COLS) or ""
    phone_col = _find(pos_df, _PHONE_COLS) or ""

    beds = BedBreakdown(
        total=_safe_int(row, "BED_CNT"),
        certified=_safe_int(row, "CRTFD_BED_CNT"),
        psychiatric=_safe_int(row, "PSYCH_UNIT_BED_CNT"),
        rehabilitation=_safe_int(row, "REHAB_UNIT_BED_CNT"),
        hospice=_safe_int(row, "HOSPC_BED_CNT"),
        ventilator=_safe_int(row, "VNTLTR_BED_CNT"),
        aids=_safe_int(row, "AIDS_BED_CNT"),
        alzheimer=_safe_int(row, "ALZHMR_BED_CNT"),
        dialysis=_safe_int(row, "DLYS_BED_CNT"),
    )

    services = ServiceCapabilities(
        cardiac_catheterization=_is_yes(row, "CRDAC_CTHRTZTN_LAB_SW"),
        open_heart_surgery=_is_yes(row, "OPN_HRT_SRGRY_SW"),
        mri=_is_yes(row, "MRI_SRVC_SW"),
        ct_scanner=_is_yes(row, "CT_SCNR_SW"),
        pet_scanner=_is_yes(row, "PET_SCNR_SW"),
        nuclear_medicine=_is_yes(row, "NUCLR_MED_SRVC_SW"),
        trauma_center=_is_yes(row, "TRMA_CTR_SW"),
        trauma_level=str(row.get("TRMA_CTR_LVL_CD", "") or "").strip(),
        burn_care=_is_yes(row, "BRNCTR_SW"),
        neonatal_icu=_is_yes(row, "NNTL_ICU_SW"),
        obstetrics=_is_yes(row, "OBSTTRCL_SRVC_SW"),
        transplant=_is_yes(row, "ORNG_TRNSP_SW"),
        emergency_department=_is_yes(row, "EMER_DEPT_SW"),
        operating_rooms=_safe_int(row, "OPRTN_RM_CNT"),
        endoscopy_rooms=_safe_int(row, "ENDSCPY_RM_CNT"),
        cardiac_cath_rooms=_safe_int(row, "CRDAC_CTHRTZTN_LAB_RM_CNT"),
    )

    staffing = StaffingCounts(
        rn=_safe_int(row, "RN_CNT"),
        lpn=_safe_int(row, "LPN_CNT"),
        physicians=_safe_int(row, "MDCL_STAFF_PHYSCN_CNT"),
        pharmacists=_safe_int(row, "PHRMCST_CNT"),
        therapists=_safe_int(row, "THRPST_CNT"),
        total_fte=_safe_float(row, "TOT_STFNG"),
    )

    return FacilitySummary(
        ccn=ccn,
        name=str(row.get(name_col, "") or "").strip() if name_col else "",
        address=str(row.get(addr_col, "") or "").strip() if addr_col else "",
        city=str(row.get(city_col, "") or "").strip() if city_col else "",
        state=str(row.get(state_col, "") or "").strip() if state_col else "",
        zip_code=str(row.get(zip_col, "") or "").strip() if zip_col else "",
        county=str(row.get(county_col, "") or "").strip() if county_col else "",
        phone=str(row.get(phone_col, "") or "").strip() if phone_col else "",
        beds=beds,
        services=services,
        staffing=staffing,
    )


def aggregate_off_site(ccns: list[str], pos_df: pd.DataFrame) -> OffSiteSummary:
    """Aggregate off-site location counts across all system CCNs."""
    ccn_col = _find(pos_df, _CCN_COLS)
    if not ccn_col:
        return OffSiteSummary()

    ccn_set = {c.strip().zfill(6) for c in ccns}
    matches = pos_df[pos_df[ccn_col].astype(str).str.strip().str.zfill(6).isin(ccn_set)]

    ed = 0
    uc = 0
    psych = 0
    rehab = 0

    for _, row in matches.iterrows():
        ed += _safe_int(row, "TOT_OFSITE_EMER_DEPT_CNT")
        uc += _safe_int(row, "TOT_OFSITE_URGNT_CARE_CNTR_CNT")
        psych += _safe_int(row, "TOT_OFSITE_PSYCH_UNIT_CNT")
        rehab += _safe_int(row, "TOT_OFSITE_REHAB_HOSP_CNT")

    return OffSiteSummary(
        emergency_departments=ed,
        urgent_care_centers=uc,
        psychiatric_units=psych,
        rehabilitation_hospitals=rehab,
        total_off_site=ed + uc + psych + rehab,
    )
