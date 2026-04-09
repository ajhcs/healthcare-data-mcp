"""Shared column-name detection for pandas DataFrames.

Consolidates the ``_find_column`` helpers from health-system-profiler
and service-area that need case-insensitive matching against DataFrame
columns (as opposed to the DuckDB-level helpers in duckdb_helpers.py).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["find_df_column"]


def find_df_column(df: "pd.DataFrame", candidates: list[str]) -> str | None:
    """Find the first matching column in *df* from *candidates*.

    Tries an exact match first, then falls back to a case-insensitive
    comparison with spaces normalised to underscores.
    """
    lower_map: dict[str, str] = {
        col.lower().strip().replace(" ", "_"): col for col in df.columns
    }
    for c in candidates:
        if c in df.columns:
            return c
        normalised = c.lower().strip().replace(" ", "_")
        if normalised in lower_map:
            return lower_map[normalised]
    return None
