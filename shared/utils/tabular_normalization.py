"""Shared helpers for source tabular files."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

import pandas as pd


def normalize_tabular_key(value: object) -> str:
    """Normalize a source column/key to the repo's snake_case tabular form."""

    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def normalize_tabular_columns(columns: list[object]) -> list[str]:
    """Normalize source column names without dropping positional columns."""

    return [normalize_tabular_key(column) for column in columns]


def read_csv_strings(
    path: str | Path,
    *,
    normalize_columns: bool = False,
    **kwargs: Any,
) -> pd.DataFrame:
    """Read a source CSV as strings, preserving blanks and leading zeroes."""

    read_kwargs = {
        "dtype": str,
        "keep_default_na": False,
        "encoding_errors": "replace",
        **kwargs,
    }
    frame = pd.read_csv(path, **read_kwargs)
    if normalize_columns:
        frame.columns = normalize_tabular_columns(list(frame.columns))
    return frame


__all__ = [
    "normalize_tabular_columns",
    "normalize_tabular_key",
    "read_csv_strings",
]
