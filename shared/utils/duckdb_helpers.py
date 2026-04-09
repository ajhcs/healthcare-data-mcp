"""Shared DuckDB connection helpers.

Consolidates the ``_get_con`` and ``_get_con_with_view`` patterns that
were duplicated across public_records, claims-analytics, and
web_intelligence servers.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from shared.utils.duckdb_safe import safe_parquet_sql

__all__ = [
    "get_connection",
    "get_connection_with_view",
    "detect_columns",
    "find_column",
]

logger = logging.getLogger(__name__)


def get_connection(
    parquet_path: Path,
    view_name: str = "data",
) -> duckdb.DuckDBPyConnection | None:
    """Create an in-memory DuckDB connection with a view over *parquet_path*.

    If the Parquet file is missing, returns ``None``.
    If the file is corrupt, deletes it and returns ``None`` so the caller
    can trigger a re-download on the next request.
    """
    if not parquet_path.exists():
        return None
    con = duckdb.connect(":memory:")
    try:
        con.execute(
            f"CREATE VIEW {view_name} AS "
            f"SELECT * FROM {safe_parquet_sql(parquet_path)}"
        )
        return con
    except Exception:
        logger.warning("Corrupt Parquet cache, deleting: %s", parquet_path)
        con.close()
        parquet_path.unlink(missing_ok=True)
        return None


def get_connection_with_view(
    parquet_path: Path,
    view_name: str = "data",
) -> duckdb.DuckDBPyConnection | None:
    """Alias for :func:`get_connection` (legacy compat)."""
    return get_connection(parquet_path, view_name=view_name)


# ------------------------------------------------------------------
# Column introspection
# ------------------------------------------------------------------


def detect_columns(
    con: duckdb.DuckDBPyConnection,
    view_name: str = "data",
) -> list[str]:
    """Return the column names present in *view_name*."""
    return [
        r[0]
        for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name='{view_name}'"
        ).fetchall()
    ]


def find_column(cols: list[str], candidates: list[str]) -> str | None:
    """Return the first element of *candidates* that appears in *cols*.

    Performs a case-sensitive set lookup for speed.
    """
    col_set = set(cols)
    for c in candidates:
        if c in col_set:
            return c
    return None
