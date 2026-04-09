"""Shared Cost Report PUF loading and metric extraction utilities.

Both the hospital-quality and cms-facility servers draw from the same CMS
Hospital Cost Report Public Use File (PUF).  This module centralises the
common steps so each server only needs to supply the column names that matter
for its own output model.

Typical usage::

    from shared.utils.cost_report import load_cost_report_row

    row, error = await load_cost_report_row(data_loaders, ccn)
    if error:
        return json.dumps({"error": error})

    cmi = cr_safe_float(row, "case_mix_index", "cmi", "casemix_index")
    beds = cr_safe_int(row, "total_bed_days_available", "beds", "total_beds")
"""

from __future__ import annotations

__all__ = [
    "cr_col",
    "cr_safe_float",
    "cr_safe_int",
    "load_cost_report_row",
]

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------

def cr_col(df: pd.DataFrame, *candidates: str, default: str = "") -> str:
    """Return the first candidate column name that exists in *df*."""
    for c in candidates:
        if c in df.columns:
            return c
    return default


# ---------------------------------------------------------------------------
# Safe scalar parsers
# ---------------------------------------------------------------------------

_MISSING = {"", "not available", "n/a", "too few"}


def cr_safe_float(row: Any, *candidates: str) -> float | None:
    """Return the first parseable float from *row* for any of *candidates*."""
    for c in candidates:
        raw = row.get(c) if hasattr(row, "get") else getattr(row, c, None)
        if raw is None:
            continue
        text = str(raw).strip()
        if text.lower() in _MISSING or text == "":
            continue
        try:
            return float(text.replace(",", ""))
        except (ValueError, TypeError):
            continue
    return None


def cr_safe_int(row: Any, *candidates: str) -> int | None:
    """Return the first parseable int from *row* for any of *candidates*."""
    v = cr_safe_float(row, *candidates)
    return int(v) if v is not None else None


# ---------------------------------------------------------------------------
# Primary entry point
# ---------------------------------------------------------------------------

# Standard column name candidates for the CCN / provider number column.
_CCN_CANDIDATES = (
    "facility_id",
    "ccn",
    "provider_ccn",
    "provider_id",
    "provider_number",
    "cms_certification_number",
    "prvdr_num",
)

# Standard column name candidates for the fiscal-year-end date column.
_FY_END_CANDIDATES = (
    "fiscal_year_end",
    "fy_end",
    "fiscal_year_end_date",
    "fy_end_dt",
    "fiscal_year_end_dt",
)


async def load_cost_report_row(
    data_loaders: Any,
    ccn: str,
) -> tuple[Any, str]:
    """Load the most-recent Cost Report PUF row for *ccn*.

    Parameters
    ----------
    data_loaders:
        The ``data_loaders`` module (or object) that exposes a
        ``load_cost_report()`` coroutine returning a :class:`pandas.DataFrame`.
    ccn:
        The 6-character CMS Certification Number to look up.

    Returns
    -------
    (row, error)
        *row* is a pandas ``Series`` (the most recent row for *ccn*) when
        successful, or ``None`` on failure.  *error* is an empty string on
        success, or a human-readable error message on failure.
    """
    df: pd.DataFrame = await data_loaders.load_cost_report()

    if df.empty:
        return None, "Cost report data not available"

    ccn_col = cr_col(df, *_CCN_CANDIDATES)
    if not ccn_col:
        return None, "Cannot identify CCN column in cost report dataset"

    matches = df[df[ccn_col].str.strip() == ccn.strip()]
    if matches.empty:
        return None, f"No cost report data found for CCN: {ccn}"

    # Prefer the most recent fiscal year when multiple rows exist.
    fy_col = cr_col(matches, *_FY_END_CANDIDATES)
    if fy_col:
        matches = matches.sort_values(fy_col, ascending=False)

    row = matches.iloc[0]
    return row, ""


def get_fiscal_year_end(row: Any) -> str:
    """Extract the fiscal year end date string from a cost report row."""
    for c in _FY_END_CANDIDATES:
        raw = row.get(c) if hasattr(row, "get") else None
        if raw is None:
            try:
                raw = row[c]
            except (KeyError, TypeError):
                continue
        if raw is not None:
            return str(raw).strip()
    return ""
