"""
Build 990 Data Pack - Parse IRS 990 files and export to Parquet format.

This script uses the IRS990Connector to parse Form 990 XML/PDF files
and export them to the Parquet data pack format for integration with
the Health System Explorer.

Usage:
    python scripts/build_990_pack.py <source_dir> [--output <output_dir>]

Example:
    python scripts/build_990_pack.py data_source/990_files --output data/packs/990
"""

import argparse
import sys
from pathlib import Path
from typing import List
import polars as pl

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.connectors.irs990_connector import IRS990Connector
from src.data.connectors.protocol import ConnectorResult
from src.data.entities import EntityStore


def find_990_files(source_dir: Path) -> List[Path]:
    """
    Find all 990 files in the source directory.

    Args:
        source_dir: Directory to search

    Returns:
        List of paths to 990 files (XML and PDF)
    """
    files = []

    # Find XML files
    files.extend(source_dir.glob("**/*.xml"))
    # Find PDF files
    files.extend(source_dir.glob("**/*.pdf"))

    return sorted(files)


def parse_all_files(
    files: List[Path],
    connector: IRS990Connector,
) -> List[ConnectorResult]:
    """
    Parse all 990 files using the connector.

    Args:
        files: List of file paths to parse
        connector: IRS990Connector instance

    Returns:
        List of ConnectorResult objects
    """
    results = []
    total = len(files)

    for i, file_path in enumerate(files, 1):
        print(f"\n[{i}/{total}] Processing: {file_path.name}")

        # Progress callback
        def progress_callback(percent: int, message: str):
            print(f"  {percent}%: {message}")

        result = connector.parse(file_path, progress_callback)

        if result.success:
            print(f"  [OK] Success - EIN: {result.metadata.get('ein', 'Unknown')}")
            results.append(result)
        else:
            print(f"  [FAIL] Failed: {result.error_message}")

    return results


def merge_results(results: List[ConnectorResult]) -> dict:
    """
    Merge multiple ConnectorResults into combined DataFrames.

    Args:
        results: List of ConnectorResult objects

    Returns:
        Dictionary of table_name -> merged DataFrame
    """
    merged_tables = {}

    # Get all table names from results
    table_names = set()
    for result in results:
        table_names.update(result.tables.keys())

    # Merge each table
    for table_name in table_names:
        table_dfs = []

        for result in results:
            if table_name in result.tables:
                df = result.tables[table_name]
                if len(df) > 0:  # Only include non-empty tables
                    table_dfs.append(df)

        if table_dfs:
            # Concatenate all DataFrames for this table
            merged_tables[table_name] = pl.concat(table_dfs)
            print(f"  {table_name}: {len(merged_tables[table_name])} rows")
        else:
            print(f"  {table_name}: 0 rows (skipped)")

    return merged_tables


def link_entities(
    entities_df: pl.DataFrame,
    entity_store: EntityStore,
) -> None:
    """
    Link 990 entities to the entity store using EIN matching.

    Args:
        entities_df: DataFrame with entity information
        entity_store: EntityStore instance for linking
    """
    print("\nLinking entities to entity store...")

    linked_count = 0
    created_count = 0

    for row in entities_df.iter_rows(named=True):
        ein = row["ein"]
        name = row["name"]

        # Try to find matching entity
        match = entity_store.find_matching_entity(name=name, ein=ein)

        if match:
            entity, confidence = match
            print(f"  [MATCH] '{name}' -> '{entity.name}' (confidence: {confidence:.2f})")

            # Create link
            entity_store.link_source(
                entity_id=entity.id,
                source_type="irs990",
                source_id=ein,
                confidence=confidence,
                linked_by="auto",
            )
            linked_count += 1
        else:
            # Create new entity
            entity = entity_store.create_entity(
                name=name,
                ein=ein,
            )
            print(f"  [NEW] Created entity '{name}' (EIN: {ein})")

            # Link to self
            entity_store.link_source(
                entity_id=entity.id,
                source_type="irs990",
                source_id=ein,
                confidence=1.0,
                linked_by="auto",
            )
            created_count += 1

    print(f"\nLinked {linked_count} existing entities, created {created_count} new entities")


def export_to_parquet(
    tables: dict,
    output_dir: Path,
    compression: str = "zstd",
) -> None:
    """
    Export tables to Parquet files.

    Args:
        tables: Dictionary of table_name -> DataFrame
        output_dir: Output directory
        compression: Compression algorithm (default: zstd)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nExporting to {output_dir}...")

    for table_name, df in tables.items():
        output_path = output_dir / f"{table_name}.parquet"
        df.write_parquet(output_path, compression=compression)
        print(f"  [OK] {output_path.name}: {len(df)} rows")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse IRS 990 files and export to Parquet data pack format"
    )
    parser.add_argument(
        "source_dir",
        type=Path,
        help="Directory containing 990 files (XML or PDF)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/packs/990"),
        help="Output directory for Parquet files (default: data/packs/990)",
    )
    parser.add_argument(
        "--link-entities",
        action="store_true",
        help="Link entities to entity store (default: False)",
    )
    parser.add_argument(
        "--entity-store-dir",
        type=Path,
        default=Path("data/packs"),
        help="Directory for entity store (default: data/packs)",
    )
    parser.add_argument(
        "--compression",
        choices=["zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed"],
        default="zstd",
        help="Parquet compression algorithm (default: zstd)",
    )

    args = parser.parse_args()

    # Validate source directory
    if not args.source_dir.exists():
        print(f"Error: Source directory does not exist: {args.source_dir}")
        sys.exit(1)

    if not args.source_dir.is_dir():
        print(f"Error: Source path is not a directory: {args.source_dir}")
        sys.exit(1)

    # Find 990 files
    print(f"Searching for 990 files in {args.source_dir}...")
    files = find_990_files(args.source_dir)

    if not files:
        print("No 990 files found (looking for *.xml and *.pdf)")
        sys.exit(1)

    print(f"Found {len(files)} files")

    # Parse all files
    connector = IRS990Connector()
    results = parse_all_files(files, connector)

    if not results:
        print("\nNo files were successfully parsed")
        sys.exit(1)

    print(f"\n{len(results)}/{len(files)} files parsed successfully")

    # Merge results
    print("\nMerging results...")
    merged_tables = merge_results(results)

    # Link entities if requested
    if args.link_entities and "entities" in merged_tables:
        entity_store = EntityStore(args.entity_store_dir)
        link_entities(merged_tables["entities"], entity_store)

    # Export to Parquet
    export_to_parquet(merged_tables, args.output, args.compression)

    print("\n[SUCCESS] Data pack build complete!")
    print(f"  Output: {args.output}")
    print(f"  Tables: {', '.join(merged_tables.keys())}")


if __name__ == "__main__":
    main()
