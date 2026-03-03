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


def _service_available(row, col: str) -> bool:
    """Check a POS _SRVC_CD column — any non-zero/non-empty value means available.

    POS service codes: 0 = not available, 1 = provided in-facility,
    2 = provided via agreement, 3 = available through others.
    """
    if col not in row.index:
        return False
    val = str(row[col]).strip()
    if not val or val.upper() in ("0", "NAN", "NONE", ""):
        return False
    return True


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
        cardiac_catheterization=_service_available(row, "CRDC_CTHRTZTN_LAB_SRVC_CD"),
        open_heart_surgery=_service_available(row, "OPEN_HRT_SRGRY_SRVC_CD"),
        mri=_service_available(row, "MGNTC_RSNC_IMG_SRVC_CD"),
        ct_scanner=_service_available(row, "CT_SCAN_SRVC_CD"),
        pet_scanner=_service_available(row, "PET_SCAN_SRVC_CD"),
        nuclear_medicine=_service_available(row, "NUCLR_MDCN_SRVC_CD"),
        trauma_center=_service_available(row, "SHCK_TRMA_SRVC_CD"),
        trauma_level=str(row.get("SHCK_TRMA_SRVC_CD", "") or "").strip(),
        burn_care=_service_available(row, "BURN_CARE_UNIT_SRVC_CD"),
        neonatal_icu=_service_available(row, "NEONTL_ICU_SRVC_CD"),
        obstetrics=_service_available(row, "OB_SRVC_CD"),
        transplant=_service_available(row, "ORGN_TRNSPLNT_SRVC_CD"),
        emergency_department=_service_available(row, "DCTD_ER_SRVC_CD"),
        operating_rooms=_safe_int(row, "OPRTG_ROOM_CNT"),
        endoscopy_rooms=_safe_int(row, "ENDSCPY_PRCDR_ROOMS_CNT"),
        cardiac_cath_rooms=_safe_int(row, "CRDC_CTHRTZTN_PRCDR_ROOMS_CNT"),
    )

    therapists = (
        _safe_int(row, "OCPTNL_THRPST_CNT")
        + _safe_int(row, "PHYS_THRPST_CNT")
        + _safe_int(row, "INHLTN_THRPST_CNT")
    )

    staffing = StaffingCounts(
        rn=_safe_int(row, "RN_CNT"),
        lpn=_safe_int(row, "LPN_CNT"),
        physicians=_safe_int(row, "PHYSN_CNT"),
        pharmacists=_safe_int(row, "REG_PHRMCST_CNT"),
        therapists=therapists,
        total_fte=_safe_float(row, "EMPLEE_CNT"),
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
