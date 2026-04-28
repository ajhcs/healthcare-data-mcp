"""
Validate Parquet Migration
---------------------------
Quick validation script to verify the Parquet files are correct and queryable.

Usage:
    python scripts/validate_parquet.py
"""

import json
from pathlib import Path

import polars as pl


PACKS_DIR = Path(__file__).parent.parent / "data" / "packs"
LOOKUPS_DIR = PACKS_DIR / "lookups"


def validate_files_exist():
    """Check that all expected files exist."""
    print("=" * 60)
    print("File Existence Check")
    print("=" * 60)

    expected_files = [
        PACKS_DIR / "manifest.json",
        PACKS_DIR / "entities.parquet",
        PACKS_DIR / "mrf_charges.parquet",
        LOOKUPS_DIR / "payers.parquet",
        LOOKUPS_DIR / "plans.parquet",
        LOOKUPS_DIR / "descriptions.parquet",
        LOOKUPS_DIR / "algorithms.parquet",
        LOOKUPS_DIR / "methodologies.parquet",
    ]

    all_exist = True
    for file_path in expected_files:
        exists = file_path.exists()
        status = "✓" if exists else "✗"
        print(f"  {status} {file_path.name}")
        if not exists:
            all_exist = False

    return all_exist


def validate_manifest():
    """Validate the manifest file."""
    print("\n" + "=" * 60)
    print("Manifest Validation")
    print("=" * 60)

    manifest_path = PACKS_DIR / "manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)

    print(f"\nVersion: {manifest['version']}")
    print(f"Created: {manifest['created_at']}")
    print(f"Source: {manifest['source']}")
    print(f"Format: {manifest['format']}")
    print(f"Compression: {manifest['compression']}")

    stats = manifest['statistics']
    print(f"\nTotal size: {stats['total_size_mb']:.2f} MB")
    print(f"Entities: {stats['entities']['rows']:,} rows")
    print(f"Charges: {stats['charges']['rows']:,} rows")

    return manifest


def validate_schemas():
    """Validate Parquet file schemas."""
    print("\n" + "=" * 60)
    print("Schema Validation")
    print("=" * 60)

    # Check entities
    print("\nEntities schema:")
    df = pl.read_parquet(PACKS_DIR / "entities.parquet")
    print(f"  Rows: {len(df):,}")
    print(f"  Columns: {', '.join(df.columns)}")

    # Check charges (sample first few rows for speed)
    print("\nCharges schema:")
    df = pl.read_parquet(PACKS_DIR / "mrf_charges.parquet", n_rows=10)
    print(f"  Columns: {len(df.columns)}")
    print(f"  Sample columns: {', '.join(df.columns[:10])}...")

    # Check for denormalized columns
    required_denorm = ["hospital_name", "description", "payer_name", "plan_name"]
    missing = [col for col in required_denorm if col not in df.columns]
    if missing:
        print(f"  WARNING: Missing denormalized columns: {missing}")
        return False
    else:
        print(f"  ✓ All denormalized columns present")

    # Check lookups
    print("\nLookup tables:")
    for lookup in ["payers", "plans", "descriptions", "algorithms", "methodologies"]:
        df = pl.read_parquet(LOOKUPS_DIR / f"{lookup}.parquet")
        print(f"  {lookup}: {len(df):,} rows")

    return True


def validate_data_quality():
    """Check data quality in the Parquet files."""
    print("\n" + "=" * 60)
    print("Data Quality Checks")
    print("=" * 60)

    # Load charges (lazy to avoid loading all into memory)
    lf = pl.scan_parquet(PACKS_DIR / "mrf_charges.parquet")

    # Count nulls in key columns
    print("\nNull counts in key columns:")
    null_counts = lf.select([
        pl.col("hospital_id").is_null().sum().alias("hospital_id_nulls"),
        pl.col("description").is_null().sum().alias("description_nulls"),
        pl.col("code1").is_null().sum().alias("code1_nulls"),
    ]).collect()

    for col in null_counts.columns:
        count = null_counts[col][0]
        print(f"  {col}: {count:,}")

    # Check for some data
    print("\nSample records:")
    sample = lf.head(3).collect()
    for i, row in enumerate(sample.iter_rows(named=True)):
        print(f"\n  Record {i+1}:")
        print(f"    Hospital: {row.get('hospital_name', 'N/A')}")
        print(f"    Description: {row.get('description', 'N/A')[:60]}...")
        print(f"    Code: {row.get('code1', 'N/A')} ({row.get('code1_type', 'N/A')})")
        if row.get('negotiated_dollar'):
            print(f"    Price: ${row['negotiated_dollar']:.2f}")

    return True


def run_sample_queries():
    """Run sample queries to ensure data is queryable."""
    print("\n" + "=" * 60)
    print("Sample Queries")
    print("=" * 60)

    lf = pl.scan_parquet(PACKS_DIR / "mrf_charges.parquet")

    # Query 1: Count by hospital
    print("\nQuery 1: Charges per hospital")
    result = (
        lf.group_by("hospital_name")
        .agg(pl.count().alias("charge_count"))
        .sort("charge_count", descending=True)
        .collect()
    )
    print(result.head(5))

    # Query 2: Find specific CPT code
    print("\nQuery 2: CPT code 99213 (Office visit)")
    result = (
        lf.filter(pl.col("code1") == "99213")
        .select(["hospital_name", "description", "payer_name", "negotiated_dollar"])
        .sort("negotiated_dollar", descending=True)
        .head(5)
        .collect()
    )
    print(result)

    # Query 3: Price statistics
    print("\nQuery 3: Overall price statistics")
    result = (
        lf.filter(pl.col("negotiated_dollar").is_not_null())
        .select([
            pl.col("negotiated_dollar").min().alias("min_price"),
            pl.col("negotiated_dollar").max().alias("max_price"),
            pl.col("negotiated_dollar").mean().alias("avg_price"),
            pl.count().alias("total_with_price"),
        ])
        .collect()
    )
    print(result)

    return True


def main():
    print("=" * 60)
    print("Parquet Migration Validation")
    print("=" * 60)

    if not PACKS_DIR.exists():
        print(f"\nERROR: Packs directory not found: {PACKS_DIR}")
        print("Please run the migration script first:")
        print("  python scripts/migrate_to_parquet.py")
        return 1

    try:
        # Run validation steps
        if not validate_files_exist():
            print("\n✗ FAILED: Not all files exist")
            return 1

        validate_manifest()

        if not validate_schemas():
            print("\n✗ FAILED: Schema validation failed")
            return 1

        if not validate_data_quality():
            print("\n✗ FAILED: Data quality checks failed")
            return 1

        if not run_sample_queries():
            print("\n✗ FAILED: Sample queries failed")
            return 1

        # All checks passed
        print("\n" + "=" * 60)
        print("✓ All validation checks passed!")
        print("=" * 60)
        print("\nThe Parquet files are ready to use with DuckDB.")
        print(f"Location: {PACKS_DIR}")

        return 0

    except Exception as e:
        print(f"\n✗ ERROR during validation: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
