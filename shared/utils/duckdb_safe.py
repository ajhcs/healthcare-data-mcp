"""Safe DuckDB SQL helpers.

Prevents SQL injection via file paths interpolated into DuckDB queries.
Path objects from pathlib are safe (no user input), but a single quote
in a directory name (e.g., /tmp/O'Brien/data.parquet) breaks the SQL
and could be exploited.
"""

import os
from pathlib import Path


def safe_parquet_sql(path: str | Path) -> str:
    """Return a DuckDB SQL expression for read_parquet with a safely escaped path.

    Escapes single quotes by doubling them, which is the standard SQL
    string literal escape. Uses os.fspath to normalize Path objects.

    Usage:
        con.execute(f"CREATE VIEW v AS SELECT * FROM {safe_parquet_sql(path)}")
    """
    safe_path = os.fspath(path).replace("'", "''")
    return f"read_parquet('{safe_path}')"
