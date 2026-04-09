"""Safe value extraction helpers for dict rows.

Consolidates the ``_s`` and ``_i`` micro-utilities from public_records.
"""

from __future__ import annotations

__all__ = ["safe_str", "safe_int"]


def safe_str(row: dict, col: str | None) -> str:
    """Extract a trimmed string from *row[col]*, or ``""`` on any miss."""
    if not col or col not in row:
        return ""
    v = row.get(col)
    return str(v).strip() if v is not None else ""


def safe_int(row: dict, col: str | None) -> int:
    """Extract an integer from *row[col]*, or ``0`` on any miss/parse error."""
    v = safe_str(row, col)
    try:
        return int(float(v)) if v else 0
    except (ValueError, TypeError):
        return 0
