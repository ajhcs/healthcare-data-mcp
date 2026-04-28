"""
Validation utilities for parquet data packs.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import json

import polars as pl

from .duckdb_store import (
    CHARGES_SCHEMA,
    HOSPITALS_SCHEMA,
    DESCRIPTIONS_SCHEMA,
    PAYERS_SCHEMA,
    PLANS_SCHEMA,
    ALGORITHMS_SCHEMA,
    METHODOLOGIES_SCHEMA,
)


REQUIRED_TABLES = [
    "charges",
    "hospitals",
    "descriptions",
    "payers",
    "plans",
    "algorithms",
    "methodologies",
]

EXPECTED_SCHEMAS = {
    "charges": CHARGES_SCHEMA,
    "hospitals": HOSPITALS_SCHEMA,
    "descriptions": DESCRIPTIONS_SCHEMA,
    "payers": PAYERS_SCHEMA,
    "plans": PLANS_SCHEMA,
    "algorithms": ALGORITHMS_SCHEMA,
    "methodologies": METHODOLOGIES_SCHEMA,
}


@dataclass
class PackValidationResult:
    """Structured validation result for one data pack."""

    pack_path: Path
    is_valid: bool
    metadata: dict[str, Any] = field(default_factory=dict)
    metadata_errors: list[str] = field(default_factory=list)
    missing_tables: list[str] = field(default_factory=list)
    schema_errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    table_row_counts: dict[str, int] = field(default_factory=dict)


def _read_metadata(pack_path: Path) -> tuple[dict[str, Any], list[str], list[str]]:
    """Load metadata.json and return (metadata, errors, warnings)."""
    metadata_path = pack_path / "metadata.json"
    errors: list[str] = []
    warnings: list[str] = []

    if not metadata_path.exists():
        errors.append("Missing metadata.json")
        return {}, errors, warnings

    try:
        with open(metadata_path, "r", encoding="utf-8") as handle:
            metadata = json.load(handle)
    except Exception as exc:
        errors.append(f"Invalid metadata.json: {exc}")
        return {}, errors, warnings

    for required_key in ["name", "version", "created"]:
        if required_key not in metadata:
            warnings.append(f"metadata.json missing key: {required_key}")

    return metadata, errors, warnings


def _validate_table_schema(table_name: str, parquet_path: Path) -> tuple[list[str], list[str]]:
    """Validate required columns and basic type compatibility for one table."""
    errors: list[str] = []
    warnings: list[str] = []

    expected = EXPECTED_SCHEMAS.get(table_name)
    if expected is None:
        return errors, warnings

    try:
        schema = pl.scan_parquet(parquet_path).collect_schema()
    except Exception as exc:
        errors.append(f"{table_name}.parquet unreadable: {exc}")
        return errors, warnings

    actual_names = set(schema.names())
    expected_names = set(expected.keys())

    missing_cols = sorted(expected_names - actual_names)
    if missing_cols:
        errors.append(f"{table_name}.parquet missing columns: {', '.join(missing_cols)}")

    common_cols = sorted(expected_names & actual_names)
    for col_name in common_cols:
        actual_dtype = schema[col_name]
        expected_dtype = expected[col_name]
        if actual_dtype != expected_dtype:
            warnings.append(
                f"{table_name}.{col_name} type differs: "
                f"expected {expected_dtype}, got {actual_dtype}"
            )

    return errors, warnings


def _table_row_count(parquet_path: Path) -> int:
    """Return row count for parquet file, or 0 if unreadable."""
    try:
        return int(
            pl.scan_parquet(parquet_path)
            .select(pl.len().alias("n"))
            .collect()
            .item(0, 0)
        )
    except Exception:
        return 0


def validate_data_pack(pack_path: str | Path) -> PackValidationResult:
    """
    Validate data pack structure and schema compatibility.

    A pack is considered valid when metadata exists and all required tables
    with required columns are present.
    """
    pack_path = Path(pack_path)
    metadata_errors: list[str] = []
    warnings: list[str] = []
    missing_tables: list[str] = []
    schema_errors: list[str] = []
    table_row_counts: dict[str, int] = {}

    if not pack_path.exists() or not pack_path.is_dir():
        return PackValidationResult(
            pack_path=pack_path,
            is_valid=False,
            metadata_errors=[f"Pack path does not exist: {pack_path}"],
        )

    metadata, md_errors, md_warnings = _read_metadata(pack_path)
    metadata_errors.extend(md_errors)
    warnings.extend(md_warnings)

    for table_name in REQUIRED_TABLES:
        parquet_path = pack_path / f"{table_name}.parquet"
        if not parquet_path.exists():
            missing_tables.append(table_name)
            continue

        table_row_counts[table_name] = _table_row_count(parquet_path)
        table_errors, table_warnings = _validate_table_schema(table_name, parquet_path)
        schema_errors.extend(table_errors)
        warnings.extend(table_warnings)

    is_valid = (
        len(metadata_errors) == 0
        and len(missing_tables) == 0
        and len(schema_errors) == 0
    )

    return PackValidationResult(
        pack_path=pack_path,
        is_valid=is_valid,
        metadata=metadata,
        metadata_errors=metadata_errors,
        missing_tables=missing_tables,
        schema_errors=schema_errors,
        warnings=warnings,
        table_row_counts=table_row_counts,
    )

