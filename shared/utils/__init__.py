"""Shared utility modules.

Re-exports the most commonly used helpers so callers can do::

    from shared.utils import is_cache_valid, get_connection, safe_str
"""

from shared.utils.cache import is_cache_valid
from shared.utils.column_detection import find_df_column
from shared.utils.cost_report import (
    cr_col,
    cr_safe_float,
    cr_safe_int,
    get_fiscal_year_end,
    load_cost_report_row,
)
from shared.utils.duckdb_helpers import (
    detect_columns,
    find_column,
    get_connection,
    get_connection_with_view,
)
from shared.utils.extraction import safe_int, safe_str

__all__ = [
    "is_cache_valid",
    "find_df_column",
    "cr_col",
    "cr_safe_float",
    "cr_safe_int",
    "get_fiscal_year_end",
    "load_cost_report_row",
    "detect_columns",
    "find_column",
    "get_connection",
    "get_connection_with_view",
    "safe_int",
    "safe_str",
]
