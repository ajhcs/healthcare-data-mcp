"""
Build a Parquet data pack from SQLite database.

This script converts the existing SQLite database to a DuckDB-compatible
Parquet data pack with optimal compression and indexing.
"""

import sys
from pathlib import Path
import sqlite3
import polars as pl
from datetime import datetime
import json

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.duckdb_store import (
    create_charges_parquet,
    create_entities_parquet,
    create_data_pack_structure,
)


def build_pack_from_sqlite(sqlite_path: str, pack_name: str,
                          output_dir: str = "data/packs"):
    """
    Build a Parquet data pack from a SQLite database.

    Args:
        sqlite_path: Path to SQLite database file
        pack_name: Name for the new data pack
        output_dir: Directory where packs are stored
    """
    sqlite_path = Path(sqlite_path)
    if not sqlite_path.exists():
        print(f"Error: SQLite database not found: {sqlite_path}")
        return

    print(f"Building data pack '{pack_name}' from {sqlite_path}")

    # Create pack directory
    pack_path = create_data_pack_structure(pack_name, output_dir)
    print(f"Created pack directory: {pack_path}")

    # Connect to SQLite
    conn = sqlite3.connect(sqlite_path)

    try:
        # Export charges table
        print("Exporting charges table...")
        charges_df = pl.read_database(
            "SELECT * FROM charges",
            connection=conn
        )
        print(f"  Found {len(charges_df):,} charge records")

        charges_output = pack_path / "charges.parquet"
        create_charges_parquet(charges_df, charges_output)
        print(f"  Wrote charges.parquet ({charges_output.stat().st_size // 1024:,} KB)")

        # Export hospitals table
        print("Exporting hospitals table...")
        hospitals_df = pl.read_database(
            "SELECT * FROM hospitals",
            connection=conn
        )
        hospitals_output = pack_path / "hospitals.parquet"
        hospitals_df.write_parquet(hospitals_output, compression="zstd")
        print(f"  Wrote hospitals.parquet ({len(hospitals_df)} hospitals)")

        # Export lookup tables
        for table_name in ["descriptions", "payers", "plans", "algorithms", "methodologies"]:
            try:
                print(f"Exporting {table_name} table...")
                df = pl.read_database(f"SELECT * FROM {table_name}", connection=conn)
                output_file = pack_path / f"{table_name}.parquet"
                df.write_parquet(output_file, compression="zstd")
                print(f"  Wrote {table_name}.parquet ({len(df)} rows)")
            except Exception as e:
                print(f"  Warning: Could not export {table_name}: {e}")

        # Update metadata
        metadata_path = pack_path / "metadata.json"
        with open(metadata_path) as f:
            metadata = json.load(f)

        metadata.update({
            "created": datetime.now().isoformat(),
            "source": str(sqlite_path),
            "charges_count": len(charges_df),
            "hospitals_count": len(hospitals_df),
        })

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"\nData pack '{pack_name}' built successfully!")
        print(f"Location: {pack_path}")

        # Calculate total size
        total_size = sum(f.stat().st_size for f in pack_path.glob("*.parquet"))
        sqlite_size = sqlite_path.stat().st_size

        print(f"\nCompression stats:")
        print(f"  SQLite database: {sqlite_size // (1024*1024):,} MB")
        print(f"  Parquet pack: {total_size // (1024*1024):,} MB")
        print(f"  Savings: {((sqlite_size - total_size) / sqlite_size * 100):.1f}%")

    finally:
        conn.close()


def build_sample_pack():
    """Build a sample data pack with synthetic data for testing."""
    print("Building sample data pack...")

    pack_path = create_data_pack_structure("sample", "data/packs")

    # Create sample charges
    charges_df = pl.DataFrame({
        "id": range(1, 1001),
        "hospital_id": [i % 3 + 1 for i in range(1000)],
        "description_id": [i % 50 + 1 for i in range(1000)],
        "code1": [f"{10000 + i % 100}" for i in range(1000)],
        "code1_type": ["CPT"] * 1000,
        "code2": [None] * 1000,
        "code2_type": [None] * 1000,
        "modifiers": [None] * 1000,
        "setting": ["outpatient" if i % 2 else "inpatient" for i in range(1000)],
        "drug_unit": [None] * 1000,
        "drug_type": [None] * 1000,
        "gross_charge": [100.0 + i * 0.5 for i in range(1000)],
        "discounted_cash": [80.0 + i * 0.4 for i in range(1000)],
        "payer_id": [i % 5 + 1 for i in range(1000)],
        "plan_id": [i % 10 + 1 for i in range(1000)],
        "negotiated_dollar": [75.0 + i * 0.35 for i in range(1000)],
        "negotiated_percentage": [None] * 1000,
        "algorithm_id": [None] * 1000,
        "estimated_amount": [None] * 1000,
        "methodology_id": [1] * 1000,
        "min_charge": [50.0 + i * 0.3 for i in range(1000)],
        "max_charge": [150.0 + i * 0.6 for i in range(1000)],
        "notes": [None] * 1000,
        "billing_class": ["professional"] * 1000,
    })

    create_charges_parquet(charges_df, pack_path / "charges.parquet")
    print(f"Created charges.parquet with {len(charges_df)} records")

    # Create sample hospitals
    hospitals_df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Sample Hospital A", "Sample Hospital B", "Sample Hospital C"],
        "location": ["City A, State", "City B, State", "City C, State"],
        "address": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
    })
    hospitals_df.write_parquet(pack_path / "hospitals.parquet")
    print(f"Created hospitals.parquet with {len(hospitals_df)} records")

    # Create sample payers
    payers_df = pl.DataFrame({
        "id": range(1, 6),
        "name": ["Blue Cross", "Aetna", "United Healthcare", "Cigna", "Humana"],
    })
    payers_df.write_parquet(pack_path / "payers.parquet")

    # Create sample descriptions
    descriptions_df = pl.DataFrame({
        "id": range(1, 51),
        "text": [f"Sample medical procedure {i}" for i in range(1, 51)],
    })
    descriptions_df.write_parquet(pack_path / "descriptions.parquet")

    # Update metadata
    metadata_path = pack_path / "metadata.json"
    with open(metadata_path) as f:
        metadata = json.load(f)

    metadata.update({
        "created": datetime.now().isoformat(),
        "source": "synthetic data",
        "charges_count": len(charges_df),
        "hospitals_count": len(hospitals_df),
    })

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nSample pack created at: {pack_path}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--sample":
            build_sample_pack()
        else:
            # Build from SQLite
            sqlite_path = sys.argv[1]
            pack_name = sys.argv[2] if len(sys.argv) > 2 else "default"
            build_pack_from_sqlite(sqlite_path, pack_name)
    else:
        print("Usage:")
        print("  python build_parquet_pack.py <sqlite_path> [pack_name]")
        print("  python build_parquet_pack.py --sample")
        print("\nExamples:")
        print("  python build_parquet_pack.py dist/hospital_data.db main")
        print("  python build_parquet_pack.py --sample")
