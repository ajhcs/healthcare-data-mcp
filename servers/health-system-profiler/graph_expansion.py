"""Graph expansion via RELATED_PROVIDER_NUMBER in POS file.

Discovers sub-entities (dialysis, rehab, psychiatric, etc.) linked to
parent hospital CCNs.
"""

import logging

import pandas as pd

from .models import SubEntity

logger = logging.getLogger(__name__)

# POS provider category codes
CATEGORY_LABELS = {
    "01": "Hospital",
    "02": "Skilled Nursing Facility",
    "03": "Home Health Agency",
    "04": "Religious Nonmedical Health Care Institution",
    "05": "Federally Qualified Health Center",
    "06": "End-Stage Renal Disease Facility",
    "07": "Rural Health Clinic",
    "08": "Ambulatory Surgical Center",
    "09": "Hospice",
    "10": "Organ Procurement Organization",
    "11": "Renal Dialysis Facility",
    "12": "Outpatient Physical Therapy",
    "13": "Community Mental Health Center",
    "14": "Portable X-Ray Supplier",
    "15": "Comprehensive Outpatient Rehabilitation Facility",
}

_RLTD_COLS = ["RLTD_PRVDR_NMBR", "RELATED_PROVIDER_NUMBER", "RLTD_PRVDR_NUM"]
_CCN_COLS = ["PRVDR_NUM", "PROVIDER_NUMBER", "CCN"]


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


def expand_related_providers(
    parent_ccns: list[str],
    pos_df: pd.DataFrame,
) -> list[SubEntity]:
    """Find all POS rows whose RELATED_PROVIDER_NUMBER points to one of parent_ccns.

    Walks one level of the graph (direct children only).

    Args:
        parent_ccns: List of parent hospital CCNs.
        pos_df: Full POS DataFrame.

    Returns:
        List of SubEntity models.
    """
    rltd_col = _find(pos_df, _RLTD_COLS)
    ccn_col = _find(pos_df, _CCN_COLS)

    if not rltd_col or not ccn_col:
        return []

    parent_set = {c.strip().zfill(6) for c in parent_ccns}

    # Find rows whose related provider is one of our parents
    related_values = pos_df[rltd_col].astype(str).str.strip().str.zfill(6)
    mask = related_values.isin(parent_set)

    # Exclude rows that ARE parent CCNs (self-references)
    own_ccns = pos_df[ccn_col].astype(str).str.strip().str.zfill(6)
    mask = mask & ~own_ccns.isin(parent_set)

    children = pos_df[mask]
    results = []

    for _, row in children.iterrows():
        ccn = str(row.get(ccn_col, "")).strip().zfill(6)
        parent_ccn = str(row.get(rltd_col, "")).strip().zfill(6)
        category_code = str(row.get("PRVDR_CTGRY_CD", "")).strip()
        facility_type = CATEGORY_LABELS.get(category_code, f"Category {category_code}")

        results.append(SubEntity(
            ccn=ccn,
            name=str(row.get("FAC_NAME", row.get("FACILITY_NAME", ""))).strip(),
            parent_ccn=parent_ccn,
            facility_type=facility_type,
            city=str(row.get("CITY_NAME", row.get("CITY", ""))).strip(),
            state=str(row.get("STATE_CD", row.get("STATE", ""))).strip(),
            beds=_safe_int(row, "BED_CNT"),
        ))

    return results
