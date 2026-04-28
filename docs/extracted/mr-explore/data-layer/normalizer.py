"""
Data normalization for CSV import → Parquet data pack conversion.

Takes a raw Polars DataFrame from ChargeFileImporter (with text columns like
description, payer_name, plan_name) and produces normalized DataFrames with
integer ID lookup tables matching the parquet pack schema.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

import polars as pl

from .duckdb_store import (
    CHARGES_SCHEMA,
    HOSPITALS_SCHEMA,
    DESCRIPTIONS_SCHEMA,
    PAYERS_SCHEMA,
    PLANS_SCHEMA,
    ALGORITHMS_SCHEMA,
    METHODOLOGIES_SCHEMA,
    create_data_pack_structure,
)
from .importer import HospitalInfo

# Maps column names from ChargeFileImporter output to CHARGES_SCHEMA names
_IMPORTER_TO_SCHEMA = {
    "code_1": "code1",
    "code_1_type": "code1_type",
    "code_2": "code2",
    "code_2_type": "code2_type",
    "drug_unit_of_measurement": "drug_unit",
    "drug_type_of_measurement": "drug_type",
    "additional_notes": "notes",
}

# Text columns that need normalization into lookup tables
# (source_col, lookup_table_name, lookup_value_col, id_col)
_LOOKUP_COLUMNS = [
    ("description", "descriptions", "text", "description_id"),
    ("payer_name", "payers", "name", "payer_id"),
    ("plan_name", "plans", "name", "plan_id"),
    ("methodology", "methodologies", "name", "methodology_id"),
    ("negotiated_algorithm", "algorithms", "text", "algorithm_id"),
]


def _missing_rate(df: pl.DataFrame, column: str) -> float:
    """Return null rate for a column in the range [0, 1]."""
    if len(df) == 0:
        return 0.0
    if column not in df.columns:
        return 1.0

    missing = df.select(pl.col(column).is_null().sum()).item()
    return float(missing) / float(len(df))


def normalize_dataframe(
    df: pl.DataFrame,
    hospital_info: HospitalInfo,
    hospital_id: int = 1,
) -> dict[str, pl.DataFrame]:
    """
    Convert a raw imported DataFrame into normalized parquet-ready DataFrames.

    Args:
        df: Raw DataFrame from ChargeFileImporter (text columns)
        hospital_info: Hospital metadata from file header
        hospital_id: ID to assign to this hospital

    Returns:
        Dict with keys: "charges", "hospitals", "descriptions",
        "payers", "plans", "algorithms", "methodologies"
    """
    result = {}

    # Rename importer column names to match CHARGES_SCHEMA
    rename_map = {k: v for k, v in _IMPORTER_TO_SCHEMA.items() if k in df.columns}
    if rename_map:
        df = df.rename(rename_map)

    # Build lookup tables and replace text columns with IDs
    for source_col, table_name, value_col, id_col in _LOOKUP_COLUMNS:
        if source_col in df.columns:
            # Extract unique non-null values
            unique_vals = (
                df.select(source_col)
                .unique()
                .drop_nulls()
                .sort(source_col)
            )

            if len(unique_vals) > 0:
                lookup_df = unique_vals.with_row_index("id", offset=1).rename(
                    {source_col: value_col, "id": "id"}
                )
                lookup_df = lookup_df.cast({"id": pl.Int32})
                result[table_name] = lookup_df

                # Join to get IDs, then drop text column
                join_df = lookup_df.rename({value_col: source_col, "id": id_col})
                df = df.join(join_df, on=source_col, how="left").drop(source_col)
            else:
                # All nulls - create empty lookup and null ID column
                result[table_name] = pl.DataFrame(
                    {"id": pl.Series([], dtype=pl.Int32), value_col: pl.Series([], dtype=pl.Utf8)}
                )
                df = df.with_columns(pl.lit(None).cast(pl.Int32).alias(id_col)).drop(source_col)
        else:
            # Column doesn't exist - create empty lookup and null ID column
            result[table_name] = pl.DataFrame(
                {"id": pl.Series([], dtype=pl.Int32), value_col: pl.Series([], dtype=pl.Utf8)}
            )
            df = df.with_columns(pl.lit(None).cast(pl.Int32).alias(id_col))

    # Add hospital_id column
    df = df.with_columns(pl.lit(hospital_id).cast(pl.Int32).alias("hospital_id"))

    # Add auto-increment ID
    df = df.with_row_index("id", offset=1)

    # Ensure all CHARGES_SCHEMA columns exist
    for col_name, col_type in CHARGES_SCHEMA.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    # Select only schema columns in schema order
    schema_cols = list(CHARGES_SCHEMA.keys())
    existing_cols = [c for c in schema_cols if c in df.columns]
    df = df.select(existing_cols)

    result["charges"] = df

    # Build hospitals table
    result["hospitals"] = pl.DataFrame({
        "id": pl.Series([hospital_id], dtype=pl.Int32),
        "name": [hospital_info.name],
        "location": [hospital_info.location],
        "address": [hospital_info.address],
    })

    return result


def write_data_pack(
    normalized: dict[str, pl.DataFrame],
    pack_name: str,
    base_dir: Path,
    hospital_info: HospitalInfo,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> Path:
    """
    Write normalized DataFrames as a parquet data pack.

    Args:
        normalized: Dict of table_name → DataFrame from normalize_dataframe()
        pack_name: Name for the data pack directory
        base_dir: Base directory for packs (e.g., data/packs)
        hospital_info: Hospital metadata for metadata.json
        progress_callback: Optional (percent, message) callback

    Returns:
        Path to the created pack directory
    """
    def _report(pct: int, msg: str):
        if progress_callback:
            progress_callback(pct, msg)

    # Create directory structure
    pack_path = create_data_pack_structure(pack_name, base_dir)
    _report(85, "Writing parquet files...")

    table_schemas = {
        "charges": CHARGES_SCHEMA,
        "hospitals": HOSPITALS_SCHEMA,
        "descriptions": DESCRIPTIONS_SCHEMA,
        "payers": PAYERS_SCHEMA,
        "plans": PLANS_SCHEMA,
        "algorithms": ALGORITHMS_SCHEMA,
        "methodologies": METHODOLOGIES_SCHEMA,
    }

    # Always write all parquet tables. Query code expects every lookup table
    # to exist, even when it is empty for a particular import.
    for table_name, schema in table_schemas.items():
        if table_name in normalized:
            table_df = normalized[table_name]
        else:
            table_df = pl.DataFrame(schema=schema)
        for col_name, col_type in schema.items():
            if col_name not in table_df.columns:
                table_df = table_df.with_columns(pl.lit(None).cast(col_type).alias(col_name))
        table_df = table_df.select(list(schema.keys())).cast(schema, strict=False)

        output_file = pack_path / f"{table_name}.parquet"
        table_df.write_parquet(
            output_file,
            compression="zstd",
            compression_level=3,
            statistics=True,
            use_pyarrow=False,
        )

    _report(95, "Writing metadata...")

    # Write metadata
    charges_count = len(normalized.get("charges", pl.DataFrame()))
    hospitals_count = len(normalized.get("hospitals", pl.DataFrame()))

    metadata = {
        "name": pack_name,
        "version": "1.0",
        "schema_version": "1.0",
        "created": datetime.now().isoformat(),
        "description": f"Imported from {hospital_info.name}",
        "source": "csv_import",
        "charges_count": charges_count,
        "hospitals_count": hospitals_count,
        "quality_report": {
            "missing_payer_rate": _missing_rate(
                normalized.get("charges", pl.DataFrame()), "payer_id"
            ),
            "missing_plan_rate": _missing_rate(
                normalized.get("charges", pl.DataFrame()), "plan_id"
            ),
            "missing_negotiated_dollar_rate": _missing_rate(
                normalized.get("charges", pl.DataFrame()), "negotiated_dollar"
            ),
            "validation_errors": [],
        },
        "ai_readiness": {
            "distinct_counts": {
                "payers": len(normalized.get("payers", pl.DataFrame())),
                "plans": len(normalized.get("plans", pl.DataFrame())),
                "descriptions": len(normalized.get("descriptions", pl.DataFrame())),
                "methodologies": len(normalized.get("methodologies", pl.DataFrame())),
                "algorithms": len(normalized.get("algorithms", pl.DataFrame())),
            },
            "missing_rates": {
                "description_id": _missing_rate(
                    normalized.get("charges", pl.DataFrame()), "description_id"
                ),
                "payer_id": _missing_rate(
                    normalized.get("charges", pl.DataFrame()), "payer_id"
                ),
                "plan_id": _missing_rate(
                    normalized.get("charges", pl.DataFrame()), "plan_id"
                ),
                "negotiated_dollar": _missing_rate(
                    normalized.get("charges", pl.DataFrame()), "negotiated_dollar"
                ),
            },
            "import_diagnostics": {
                "source_hospital_name": hospital_info.name,
                "source_version": hospital_info.version,
                "source_last_updated": hospital_info.last_updated,
                "license_number_present": bool(hospital_info.license_number),
                "tables_written": list(table_schemas.keys()),
            },
        },
    }

    metadata_path = pack_path / "metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    return pack_path
