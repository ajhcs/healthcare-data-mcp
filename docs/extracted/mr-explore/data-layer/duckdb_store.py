"""
DuckDB + Parquet Storage Layer
-------------------------------
High-performance analytics storage using DuckDB with Parquet files.
Provides significantly better compression and query performance compared to SQLite.

Data Pack Structure:
    data/packs/<pack_name>/
        charges.parquet          # MRF charges data
        entities.parquet         # Health systems/entities
        entity_links.parquet     # Links between entities and charges
        metadata.json            # Pack metadata
"""

import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass
import polars as pl

from ..logging import get_logger

logger = get_logger(__name__)


# ==================== Parquet Schemas ====================

# MRF Charges Schema (matches SQLite schema)
CHARGES_SCHEMA = {
    "id": pl.Int64,
    "hospital_id": pl.Int32,
    "description_id": pl.Int32,
    "code1": pl.Utf8,
    "code1_type": pl.Utf8,
    "code2": pl.Utf8,
    "code2_type": pl.Utf8,
    "modifiers": pl.Utf8,
    "setting": pl.Utf8,
    "drug_unit": pl.Utf8,
    "drug_type": pl.Utf8,
    "gross_charge": pl.Float64,
    "discounted_cash": pl.Float64,
    "payer_id": pl.Int32,
    "plan_id": pl.Int32,
    "negotiated_dollar": pl.Float64,
    "negotiated_percentage": pl.Float64,
    "algorithm_id": pl.Int32,
    "estimated_amount": pl.Float64,
    "methodology_id": pl.Int32,
    "min_charge": pl.Float64,
    "max_charge": pl.Float64,
    "notes": pl.Utf8,
    "billing_class": pl.Utf8,
}

# Lookup Tables Schemas
HOSPITALS_SCHEMA = {
    "id": pl.Int32,
    "name": pl.Utf8,
    "location": pl.Utf8,
    "address": pl.Utf8,
}

DESCRIPTIONS_SCHEMA = {
    "id": pl.Int32,
    "text": pl.Utf8,
}

PAYERS_SCHEMA = {
    "id": pl.Int32,
    "name": pl.Utf8,
}

PLANS_SCHEMA = {
    "id": pl.Int32,
    "name": pl.Utf8,
}

ALGORITHMS_SCHEMA = {
    "id": pl.Int32,
    "text": pl.Utf8,
}

METHODOLOGIES_SCHEMA = {
    "id": pl.Int32,
    "name": pl.Utf8,
}

# Entities Schema (health systems)
ENTITIES_SCHEMA = {
    "id": pl.Int32,
    "name": pl.Utf8,
    "ein": pl.Utf8,  # Employer Identification Number
    "npi": pl.Utf8,  # National Provider Identifier
    "location": pl.Utf8,  # City, State
    "aliases": pl.List(pl.Utf8),  # Alternative names
}

# Entity Links Schema (entity -> source mappings)
ENTITY_LINKS_SCHEMA = {
    "entity_id": pl.Int32,
    "source_type": pl.Utf8,  # "hospital", "facility", "system"
    "source_id": pl.Int32,  # ID in the source table
    "confidence": pl.Float64,  # Link confidence score (0.0-1.0)
}


@dataclass
class QueryResult:
    """Result of a DuckDB query."""

    rows: List[Dict[str, Any]]
    row_count: int
    columns: List[str]


@dataclass
class PackInfo:
    """Metadata about a data pack."""

    name: str
    path: Path
    has_charges: bool
    has_entities: bool
    has_links: bool
    charges_count: Optional[int] = None
    entities_count: Optional[int] = None


class DuckDBStore:
    """
    DuckDB storage layer for health system analytics.

    Manages DuckDB connections and provides methods to query Parquet files
    directly without loading them into memory. Uses the data pack directory
    structure for organizing different datasets.
    """

    def __init__(self, db_path: Optional[Union[str, Path]] = None):
        """
        Initialize DuckDB store.

        Args:
            db_path: Path to DuckDB database file. If None, uses in-memory database.
                     Note: DuckDB can query Parquet files without loading into the DB.
        """
        self.db_path = str(db_path) if db_path else ":memory:"
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.registered_tables: Dict[str, str] = {}  # table_name -> parquet_path

    def connect(self):
        """Open DuckDB connection."""
        self.conn = duckdb.connect(self.db_path)

        # Configure DuckDB for optimal performance using config
        from .config import get_config

        config = get_config()
        duckdb_config = config.database.duckdb

        self.conn.execute(f"SET memory_limit='{duckdb_config.memory_limit}'")
        self.conn.execute(f"SET threads TO {duckdb_config.threads}")
        self.conn.execute(
            f"SET enable_object_cache={'true' if duckdb_config.enable_object_cache else 'false'}"
        )

        if duckdb_config.max_memory:
            self.conn.execute(f"SET max_memory='{duckdb_config.max_memory}'")

        if duckdb_config.temp_directory:
            self.conn.execute(f"SET temp_directory='{duckdb_config.temp_directory}'")

        if duckdb_config.default_order:
            self.conn.execute(f"SET default_order='{duckdb_config.default_order}'")

    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
        self.registered_tables.clear()

    # ==================== Table Registration ====================

    def register_parquet(
        self, table_name: str, parquet_path: Union[str, Path], view: bool = True
    ) -> bool:
        """
        Register a Parquet file as a table/view in DuckDB.

        Args:
            table_name: Name to use for the table
            parquet_path: Path to Parquet file
            view: If True, create a view (default). If False, load into a table.

        Returns:
            True if successful, False otherwise
        """
        path = Path(parquet_path)
        if not path.exists():
            return False

        try:
            if view:
                # Create a view that queries the Parquet file directly
                # This is extremely efficient and doesn't load data into memory
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{path}')
                """)
            else:
                # Load into an actual table (faster for repeated queries)
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{path}')
                """)

            self.registered_tables[table_name] = str(path)
            return True
        except Exception as e:
            logger.error(f"Error registering {table_name}: {e}")
            return False

    def register_parquet_glob(
        self, table_name: str, pattern: str, view: bool = True
    ) -> bool:
        """
        Register multiple Parquet files matching a glob pattern.

        Args:
            table_name: Name to use for the table
            pattern: Glob pattern (e.g., "data/packs/*/charges.parquet")
            view: If True, create a view. If False, load into a table.

        Returns:
            True if successful
        """
        try:
            if view:
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{pattern}')
                """)
            else:
                self.conn.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{pattern}')
                """)

            self.registered_tables[table_name] = pattern
            return True
        except Exception as e:
            logger.error(f"Error registering glob pattern {pattern}: {e}")
            return False

    def register_data_pack(
        self, pack_path: Union[str, Path], prefix: Optional[str] = None
    ) -> PackInfo:
        """
        Register all Parquet files in a data pack directory.

        Args:
            pack_path: Path to data pack directory
            prefix: Optional table name prefix (e.g., "pack1_charges")

        Returns:
            PackInfo with details about registered tables
        """
        pack_path = Path(pack_path)
        pack_name = pack_path.name
        prefix = prefix or pack_name

        info = PackInfo(
            name=pack_name,
            path=pack_path,
            has_charges=False,
            has_entities=False,
            has_links=False,
        )

        # Register charges
        charges_file = pack_path / "charges.parquet"
        if charges_file.exists():
            if self.register_parquet(f"{prefix}_charges", charges_file):
                info.has_charges = True
                # Get row count
                result = self.conn.execute(
                    f"SELECT COUNT(*) FROM {prefix}_charges"
                ).fetchone()
                info.charges_count = result[0] if result else None

        # Register lookup tables
        for table_name in [
            "hospitals",
            "descriptions",
            "payers",
            "plans",
            "algorithms",
            "methodologies",
        ]:
            table_file = pack_path / f"{table_name}.parquet"
            if table_file.exists():
                self.register_parquet(f"{prefix}_{table_name}", table_file)

        # Register entities
        entities_file = pack_path / "entities.parquet"
        if entities_file.exists():
            if self.register_parquet(f"{prefix}_entities", entities_file):
                info.has_entities = True
                result = self.conn.execute(
                    f"SELECT COUNT(*) FROM {prefix}_entities"
                ).fetchone()
                info.entities_count = result[0] if result else None

        # Register entity links
        links_file = pack_path / "entity_links.parquet"
        if links_file.exists():
            if self.register_parquet(f"{prefix}_entity_links", links_file):
                info.has_links = True

        return info

    # ==================== Query Execution ====================

    def execute(self, query: str, params: Optional[List[Any]] = None) -> QueryResult:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            QueryResult with rows, count, and column names
        """
        try:
            if params:
                result = self.conn.execute(query, params)
            else:
                result = self.conn.execute(query)

            rows = result.fetchall()
            columns = (
                [desc[0] for desc in result.description] if result.description else []
            )

            # Convert to list of dicts
            row_dicts = [{col: val for col, val in zip(columns, row)} for row in rows]

            return QueryResult(
                rows=row_dicts, row_count=len(row_dicts), columns=columns
            )
        except Exception as e:
            logger.error(f"Query error: {e}")
            return QueryResult(rows=[], row_count=0, columns=[])

    def execute_to_polars(
        self, query: str, params: Optional[List[Any]] = None
    ) -> pl.DataFrame:
        """
        Execute a query and return results as a Polars DataFrame.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            Polars DataFrame with query results
        """
        try:
            if params:
                result = self.conn.execute(query, params)
            else:
                result = self.conn.execute(query)

            # Convert to Polars DataFrame
            return result.pl()
        except Exception as e:
            logger.error(f"Query error: {e}")
            return pl.DataFrame()

    def execute_to_arrow(self, query: str, params: Optional[List[Any]] = None):
        """
        Execute a query and return results as an Arrow table.

        Args:
            query: SQL query string
            params: Optional query parameters

        Returns:
            PyArrow Table with query results
        """
        try:
            if params:
                result = self.conn.execute(query, params)
            else:
                result = self.conn.execute(query)

            return result.arrow()
        except Exception as e:
            logger.error(f"Query error: {e}")
            return None

    # ==================== Utility Methods ====================

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a registered table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table info (row_count, columns, etc.)
        """
        if table_name not in self.registered_tables:
            return None

        try:
            # Get row count
            count_result = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()
            row_count = count_result[0] if count_result else 0

            # Get column info
            desc_result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            columns = [{"name": row[0], "type": row[1]} for row in desc_result]

            return {
                "table_name": table_name,
                "parquet_path": self.registered_tables[table_name],
                "row_count": row_count,
                "columns": columns,
            }
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return None

    def list_tables(self) -> List[str]:
        """Get list of all registered tables."""
        return list(self.registered_tables.keys())

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Error getting row count for table {table_name}: {e}")
            return 0

    # ==================== High-Level Query Methods ====================

    def search_charges(
        self,
        table_name: str = "charges",
        description: Optional[str] = None,
        code: Optional[str] = None,
        payer: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        limit: int = 100,
    ) -> pl.DataFrame:
        """
        Search charges with filters.

        Args:
            table_name: Name of charges table
            description: Filter by description (uses LIKE)
            code: Filter by code1 (uses LIKE)
            payer: Filter by payer name
            min_price: Minimum negotiated_dollar
            max_price: Maximum negotiated_dollar
            limit: Maximum rows to return

        Returns:
            Polars DataFrame with filtered charges
        """
        conditions = []
        params = []

        if description:
            conditions.append(
                "description_id IN (SELECT id FROM descriptions WHERE text ILIKE ?)"
            )
            params.append(f"%{description}%")

        if code:
            conditions.append("code1 ILIKE ?")
            params.append(f"%{code}%")

        if payer:
            conditions.append("payer_id IN (SELECT id FROM payers WHERE name ILIKE ?)")
            params.append(f"%{payer}%")

        if min_price is not None:
            conditions.append("negotiated_dollar >= ?")
            params.append(min_price)

        if max_price is not None:
            conditions.append("negotiated_dollar <= ?")
            params.append(max_price)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT * FROM {table_name}
            WHERE {where_clause}
            LIMIT {limit}
        """

        return self.execute_to_polars(query, params if params else None)

    def compare_payers_for_code(
        self, code: str, charges_table: str = "charges", payers_table: str = "payers"
    ) -> pl.DataFrame:
        """
        Compare different payer rates for a specific code.

        Args:
            code: Medical code to compare
            charges_table: Name of charges table
            payers_table: Name of payers table

        Returns:
            Polars DataFrame with payer comparison
        """
        query = f"""
            SELECT
                p.name as payer_name,
                c.negotiated_dollar,
                c.gross_charge,
                c.discounted_cash,
                c.min_charge,
                c.max_charge
            FROM {charges_table} c
            LEFT JOIN {payers_table} p ON c.payer_id = p.id
            WHERE c.code1 = ?
            ORDER BY c.negotiated_dollar DESC
        """

        return self.execute_to_polars(query, [code])


# ==================== Parquet File Creation ====================


def create_charges_parquet(df: pl.DataFrame, output_path: Union[str, Path]):
    """
    Create a charges.parquet file with proper schema.

    Args:
        df: Polars DataFrame with charges data
        output_path: Path where to save the Parquet file
    """
    output_path = Path(output_path)

    # Ensure columns match schema
    for col_name, col_type in CHARGES_SCHEMA.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    # Write with optimal compression
    df.write_parquet(
        output_path,
        compression="zstd",
        compression_level=3,
        statistics=True,
        use_pyarrow=False,
    )


def create_entities_parquet(df: pl.DataFrame, output_path: Union[str, Path]):
    """
    Create an entities.parquet file with proper schema.

    Args:
        df: Polars DataFrame with entity data
        output_path: Path where to save the Parquet file
    """
    output_path = Path(output_path)

    # Ensure columns match schema
    for col_name, col_type in ENTITIES_SCHEMA.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    df.write_parquet(
        output_path,
        compression="zstd",
        compression_level=3,
        statistics=True,
        use_pyarrow=False,
    )


def create_entity_links_parquet(df: pl.DataFrame, output_path: Union[str, Path]):
    """
    Create an entity_links.parquet file with proper schema.

    Args:
        df: Polars DataFrame with entity link data
        output_path: Path where to save the Parquet file
    """
    output_path = Path(output_path)

    # Ensure columns match schema
    for col_name, col_type in ENTITY_LINKS_SCHEMA.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_type).alias(col_name))

    df.write_parquet(
        output_path,
        compression="zstd",
        compression_level=3,
        statistics=True,
        use_pyarrow=False,
    )


def create_data_pack_structure(
    pack_name: str, base_dir: Union[str, Path] = "data/packs"
) -> Path:
    """
    Create the directory structure for a new data pack.

    Args:
        pack_name: Name of the data pack
        base_dir: Base directory for all data packs

    Returns:
        Path to the created pack directory
    """
    pack_path = Path(base_dir) / pack_name
    pack_path.mkdir(parents=True, exist_ok=True)

    # Create metadata file
    import json

    metadata = {
        "name": pack_name,
        "version": "1.0",
        "created": None,  # Will be set when pack is built
        "description": f"Data pack: {pack_name}",
    }

    metadata_path = pack_path / "metadata.json"
    if not metadata_path.exists():
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)

    return pack_path
