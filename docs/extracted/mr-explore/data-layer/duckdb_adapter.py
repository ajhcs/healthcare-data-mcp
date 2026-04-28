"""
DuckDB Database Adapter
-----------------------
Drop-in replacement for EmbeddedDatabase using DuckDB + Parquet.
Provides the same interface for querying hospital price transparency data.

Performance Benefits:
- Faster analytics queries (column-oriented storage)
- Better compression (50-70% smaller than SQLite)
- Parallel query execution
- Zero-copy Parquet queries

Data Pack Structure:
    data/packs/<pack_name>/
        charges.parquet
        hospitals.parquet
        descriptions.parquet
        payers.parquet
        plans.parquet
        algorithms.parquet
        methodologies.parquet
"""

import duckdb
import sys

from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# Import protocol for type hints
from .database_protocol import DatabaseProtocol
from ..logging import get_logger

logger = get_logger(__name__)


@dataclass
class SearchResult:
    """Result of a database search/filter operation."""

    rows: List[Dict[str, Any]]
    total_count: int
    page: int
    page_size: int


@dataclass
class HospitalRecord:
    """Metadata about a loaded hospital."""

    id: int
    name: str
    filename: str
    address: str
    row_count: int
    last_updated: str


def get_parquet_pack_path() -> Path:
    """Get the path to the Parquet data pack directory."""
    # Check if running as PyInstaller bundle
    if getattr(sys, "frozen", False):
        # Running as compiled executable
        if hasattr(sys, "_MEIPASS"):
            base_path = Path(sys._MEIPASS)
        else:
            base_path = Path(sys.executable).parent

        # Check possible locations
        candidates = [
            base_path / "data" / "packs",
            base_path / "_internal" / "data" / "packs",
            base_path / "packs",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        # Fallback
        return base_path / "data" / "packs"
    else:
        # Running in development
        return Path(__file__).parent.parent.parent / "data" / "packs"


class DuckDBDatabase(DatabaseProtocol):
    """
    DuckDB adapter for hospital price transparency data.

    Drop-in replacement for EmbeddedDatabase that queries Parquet files
    using DuckDB instead of SQLite. Provides the same interface while
    offering better performance for analytics queries.

    Implements DatabaseProtocol.

    Uses data pack structure:
        data/packs/<pack_name>/
            charges.parquet
            hospitals.parquet
            descriptions.parquet
            payers.parquet
            plans.parquet
            algorithms.parquet
            methodologies.parquet
    """

    def __init__(self, pack_path: Optional[str | Path] = None, pack_name: str = "main"):
        """
        Initialize with the Parquet data pack path.

        Args:
            pack_path: Path to data packs directory or specific pack (default: auto-detect)
            pack_name: Name of the pack to use (default: "main")
        """
        if pack_path:
            pack_path_obj = Path(pack_path)
            # If pack_path already points to a valid pack directory, use it directly
            if (pack_path_obj / "metadata.json").exists():
                self.pack_path = pack_path_obj
                self.pack_base_path = pack_path_obj.parent
                self.pack_name = pack_path_obj.name
            else:
                # pack_path is the base directory, append pack_name
                self.pack_base_path = pack_path_obj
                self.pack_name = pack_name
                self.pack_path = self.pack_base_path / pack_name
        else:
            self.pack_base_path = get_parquet_pack_path()
            self.pack_name = pack_name
            self.pack_path = self.pack_base_path / pack_name

        self.conn: Optional[duckdb.DuckDBPyConnection] = None

        # Cache for frequently accessed data
        self._payers_cache: Optional[List[str]] = None

    def connect(self):
        """Open DuckDB connection and register Parquet files."""
        if not self.pack_path.exists():
            raise FileNotFoundError(
                f"Data pack not found: {self.pack_path}\n"
                f"Please build a data pack using: python scripts/build_parquet_pack.py"
            )

        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(":memory:")

        # Configure for optimal performance using config
        from .config import get_config

        config = get_config()
        duckdb_config = config.database.duckdb

        self.conn.execute(f"SET memory_limit='{duckdb_config.memory_limit}'")
        self.conn.execute(f"SET threads={duckdb_config.threads}")
        self.conn.execute(
            f"SET enable_object_cache={'true' if duckdb_config.enable_object_cache else 'false'}"
        )

        if duckdb_config.max_memory:
            self.conn.execute(f"SET max_memory='{duckdb_config.max_memory}'")

        if duckdb_config.temp_directory:
            self.conn.execute(f"SET temp_directory='{duckdb_config.temp_directory}'")

        if duckdb_config.default_order:
            self.conn.execute(f"SET default_order='{duckdb_config.default_order}'")

        # Register Parquet files as views (zero-copy)
        self._register_parquet_files()

    def _register_parquet_files(self):
        """Register all Parquet files in the pack as views."""
        # Note: payers is excluded - it's created as a materialized table below
        view_tables = [
            "charges",
            "hospitals",
            "descriptions",
            "plans",
            "algorithms",
            "methodologies",
        ]

        for table_name in view_tables:
            parquet_file = self.pack_path / f"{table_name}.parquet"
            if parquet_file.exists():
                # Create view that queries Parquet directly (zero-copy, very efficient)
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table_name} AS
                    SELECT * FROM read_parquet('{parquet_file}')
                """)

        # Create materialized table for payers (for performance optimization)
        # The get_unique_payers query is frequently used and this makes it much faster
        payers_file = self.pack_path / "payers.parquet"
        if payers_file.exists():
            self.conn.execute(f"""
                CREATE TABLE payers AS
                SELECT * FROM read_parquet('{payers_file}')
            """)
            # Create index on payers.name for faster filtering
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_payers_name ON payers(name)"
            )

    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
        self._payers_cache = None

    def invalidate_payers_cache(self) -> None:
        """Invalidate the payers cache so it's rebuilt on next access."""
        self._payers_cache = None

    # ==================== Hospital Management ====================

    def get_all_hospitals(self) -> List[HospitalRecord]:
        """Get all hospitals in the database."""
        try:
            result = self.conn.execute("""
                SELECT
                    h.id,
                    h.name,
                    h.name as filename,
                    COALESCE(h.location, '') || ' ' || COALESCE(h.address, '') as address,
                    (SELECT COUNT(*) FROM charges WHERE hospital_id = h.id) as row_count,
                    '' as last_updated
                FROM hospitals h
                ORDER BY h.name
            """).fetchall()

            return [
                HospitalRecord(
                    id=row[0],
                    name=row[1],
                    filename=row[2],
                    address=row[3].strip(),
                    row_count=row[4],
                    last_updated=row[5],
                )
                for row in result
            ]
        except Exception as e:
            # Log error but return empty list to keep UI functional

            sys.stderr.write(f"Error getting unique settings: {e}\n")
            return []

    def get_hospital_by_id(self, hospital_id: int) -> Optional[HospitalRecord]:
        """Get a hospital by ID."""
        try:
            result = self.conn.execute(
                """
                SELECT
                    h.id,
                    h.name,
                    h.name as filename,
                    COALESCE(h.location, '') || ' ' || COALESCE(h.address, '') as address,
                    (SELECT COUNT(*) FROM charges WHERE hospital_id = h.id) as row_count,
                    '' as last_updated
                FROM hospitals h
                WHERE h.id = ?
            """,
                [hospital_id],
            ).fetchone()

            if result:
                return HospitalRecord(
                    id=result[0],
                    name=result[1],
                    filename=result[2],
                    address=result[3].strip(),
                    row_count=result[4],
                    last_updated=result[5],
                )
            return None
        except Exception as e:
            # Log error but return None to indicate failure

            sys.stderr.write(f"Error getting hospital by ID {hospital_id}: {e}\n")
            return None

    # ==================== Querying ====================

    def search(
        self,
        query: str = "",
        hospital_ids: Optional[List[int]] = None,
        payer: Optional[str] = None,
        setting: Optional[str] = None,
        code_type: Optional[str] = None,
        code: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> SearchResult:
        """
        Search charges with filters.

        This denormalizes the data on the fly using JOINs.
        """
        conditions = []
        params = []

        # Handle full-text search using DuckDB's ILIKE (case-insensitive LIKE)
        if query:
            # Use ILIKE for case-insensitive search on description text
            conditions.append("""
                c.description_id IN (
                    SELECT id FROM descriptions WHERE text ILIKE ?
                )
            """)
            # Support multiple search terms
            search_pattern = f"%{query}%"
            params.append(search_pattern)

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"c.hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        if payer:
            conditions.append("p.name = ?")
            params.append(payer)

        if setting:
            conditions.append("c.setting = ?")
            params.append(setting)

        if code_type:
            conditions.append("c.code1_type = ?")
            params.append(code_type)

        if code:
            conditions.append("(c.code1 LIKE ? OR c.code2 LIKE ?)")
            params.extend([f"%{code}%", f"%{code}%"])

        if min_price is not None:
            conditions.append("c.negotiated_dollar >= ?")
            params.append(min_price)

        if max_price is not None:
            conditions.append("c.negotiated_dollar <= ?")
            params.append(max_price)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Build the base query with JOINs to denormalize
        base_from = """
            FROM charges c
            JOIN hospitals h ON c.hospital_id = h.id
            LEFT JOIN descriptions d ON c.description_id = d.id
            LEFT JOIN payers p ON c.payer_id = p.id
            LEFT JOIN plans pl ON c.plan_id = pl.id
            LEFT JOIN algorithms a ON c.algorithm_id = a.id
            LEFT JOIN methodologies m ON c.methodology_id = m.id
        """

        # Count query
        count_sql = f"SELECT COUNT(*) {base_from} WHERE {where_clause}"
        try:
            total_count = self.conn.execute(count_sql, params).fetchone()[0]
        except Exception as e:
            # Log error and return empty result

            sys.stderr.write(f"Error executing search query: {e}\n")
            return SearchResult(rows=[], total_count=0, page=page, page_size=page_size)

        # Select query with denormalized columns
        offset = (page - 1) * page_size
        select_sql = f"""
            SELECT
                c.id,
                c.hospital_id,
                h.name as hospital_name,
                d.text as description,
                c.code1 as code_1,
                c.code1_type as code_1_type,
                c.code2 as code_2,
                c.code2_type as code_2_type,
                c.modifiers,
                c.setting,
                c.drug_unit as drug_unit_of_measurement,
                c.drug_type as drug_type_of_measurement,
                c.gross_charge,
                c.discounted_cash,
                p.name as payer_name,
                pl.name as plan_name,
                c.negotiated_dollar,
                c.negotiated_percentage,
                a.text as negotiated_algorithm,
                c.estimated_amount,
                m.name as methodology,
                c.min_charge,
                c.max_charge,
                c.notes as additional_notes,
                c.billing_class
            {base_from}
            WHERE {where_clause}
            ORDER BY d.text, c.id
            LIMIT ? OFFSET ?
        """
        params.extend([page_size, offset])

        try:
            result = self.conn.execute(select_sql, params)
            columns = (
                [col[0] for col in result.description] if result.description else []
            )
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
        except Exception as e:
            # Log error and return empty result

            sys.stderr.write(f"Error fetching search results: {e}\n")
            return SearchResult(rows=[], total_count=0, page=page, page_size=page_size)

        return SearchResult(
            rows=rows, total_count=total_count, page=page, page_size=page_size
        )

    def get_unique_code_types(self) -> List[str]:
        """Get unique code types."""
        try:
            result = self.conn.execute("""
                SELECT DISTINCT code1_type FROM charges
                WHERE code1_type IS NOT NULL
                ORDER BY code1_type
            """).fetchall()
            return [row[0] for row in result if row[0]]
        except Exception as e:
            # Log error but return empty list to keep UI functional

            logger.error(f"Error getting unique code types: {e}")
            return []

    def get_unique_settings(self) -> List[str]:
        """Get unique settings."""
        try:
            result = self.conn.execute("""
                SELECT DISTINCT setting FROM charges
                WHERE setting IS NOT NULL
                ORDER BY setting
            """).fetchall()
            return [row[0] for row in result if row[0]]
        except Exception as e:
            # Log error but return empty list to keep UI functional

            logger.error(f"Error getting unique settings: {e}")
            return []

    def get_unique_payers(self, hospital_ids: Optional[List[int]] = None) -> List[str]:
        """Get unique payer names."""
        # Use cache if available and no hospital filter
        if hospital_ids is None and self._payers_cache is not None:
            return self._payers_cache

        try:
            if hospital_ids and len(hospital_ids) > 0:
                placeholders = ",".join("?" * len(hospital_ids))
                result = self.conn.execute(
                    f"""
                    SELECT DISTINCT p.name
                    FROM payers p
                    JOIN charges c ON c.payer_id = p.id
                    WHERE c.hospital_id IN ({placeholders})
                    ORDER BY p.name
                """,
                    hospital_ids,
                ).fetchall()
            else:
                result = self.conn.execute("""
                    SELECT DISTINCT name FROM payers
                    ORDER BY name
                """).fetchall()

            payers = [row[0] for row in result if row[0]]

            # Cache the result for future calls (only when no hospital filter)
            if hospital_ids is None:
                self._payers_cache = payers

            return payers
        except Exception as e:
            # Log error but return empty list to keep UI functional
            logger.error(f"Error getting unique payers: {e}")
            return []

    # ==================== Comparison Queries ====================

    def compare_payers_for_code(
        self, code: str, hospital_ids: Optional[List[int]] = None
    ) -> List[Dict]:
        """Get all payer rates for a specific code."""
        conditions = ["c.code1 = ?"]
        params = [code]

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"c.hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        where_clause = " AND ".join(conditions)

        try:
            result = self.conn.execute(
                f"""
                SELECT
                    h.name as hospital_name,
                    d.text as description,
                    c.code1 as code_1,
                    c.code1_type as code_1_type,
                    p.name as payer_name,
                    pl.name as plan_name,
                    c.negotiated_dollar,
                    c.gross_charge,
                    c.min_charge,
                    c.max_charge,
                    c.setting
                FROM charges c
                JOIN hospitals h ON c.hospital_id = h.id
                LEFT JOIN descriptions d ON c.description_id = d.id
                LEFT JOIN payers p ON c.payer_id = p.id
                LEFT JOIN plans pl ON c.plan_id = pl.id
                WHERE {where_clause}
                ORDER BY h.name, p.name
            """,
                params,
            ).fetchall()

            columns = [
                "hospital_name",
                "description",
                "code_1",
                "code_1_type",
                "payer_name",
                "plan_name",
                "negotiated_dollar",
                "gross_charge",
                "min_charge",
                "max_charge",
                "setting",
            ]

            return [{col: val for col, val in zip(columns, row)} for row in result]
        except Exception as e:
            # Log error and return empty result

            logger.error(f"Error comparing payers for code: {e}")
            return []

    def compare_hospitals_for_code(
        self, code: str, payer: Optional[str] = None
    ) -> List[Dict]:
        """Compare the same code across hospitals, optionally filtered by payer."""
        conditions = ["c.code1 = ?"]
        params = [code]

        if payer:
            conditions.append("p.name = ?")
            params.append(payer)

        where_clause = " AND ".join(conditions)

        try:
            result = self.conn.execute(
                f"""
                SELECT
                    h.name as hospital_name,
                    d.text as description,
                    c.code1 as code_1,
                    p.name as payer_name,
                    ANY_VALUE(pl.name) as plan_name,
                    AVG(c.negotiated_dollar) as avg_negotiated,
                    MIN(c.negotiated_dollar) as min_negotiated,
                    MAX(c.negotiated_dollar) as max_negotiated,
                    AVG(c.gross_charge) as avg_gross,
                    COUNT(*) as plan_count
                FROM charges c
                JOIN hospitals h ON c.hospital_id = h.id
                LEFT JOIN descriptions d ON c.description_id = d.id
                LEFT JOIN payers p ON c.payer_id = p.id
                LEFT JOIN plans pl ON c.plan_id = pl.id
                WHERE {where_clause}
                GROUP BY h.id, h.name, d.text, c.code1, p.name
                ORDER BY h.name, p.name
            """,
                params,
            ).fetchall()

            columns = [
                "hospital_name",
                "description",
                "code_1",
                "payer_name",
                "plan_name",
                "avg_negotiated",
                "min_negotiated",
                "max_negotiated",
                "avg_gross",
                "plan_count",
            ]

            return [{col: val for col, val in zip(columns, row)} for row in result]
        except Exception as e:
            # Log error and return empty result

            logger.error(f"Error comparing hospitals for code: {e}")
            return []

    def get_payer_variance(
        self,
        hospital_ids: Optional[List[int]] = None,
        code: Optional[str] = None,
        top_n: int = 10,
    ) -> List[Dict]:
        """Find services with highest payer variance."""
        conditions = ["c.negotiated_dollar IS NOT NULL"]
        params = []

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"c.hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        if code:
            conditions.append("c.code1 LIKE ?")
            params.append(f"%{code}%")

        where_clause = " AND ".join(conditions)

        try:
            result = self.conn.execute(
                f"""
                SELECT
                    c.code1 as code_1,
                    c.code1_type as code_1_type,
                    d.text as description,
                    COUNT(DISTINCT c.payer_id) as payer_count,
                    MIN(c.negotiated_dollar) as min_price,
                    MAX(c.negotiated_dollar) as max_price,
                    AVG(c.negotiated_dollar) as avg_price,
                    (MAX(c.negotiated_dollar) - MIN(c.negotiated_dollar)) as variance
                FROM charges c
                LEFT JOIN descriptions d ON c.description_id = d.id
                WHERE {where_clause}
                GROUP BY c.code1, c.code1_type, d.text
                HAVING payer_count > 1
                ORDER BY variance DESC
                LIMIT ?
            """,
                params + [top_n],
            ).fetchall()

            columns = [
                "code_1",
                "code_1_type",
                "description",
                "payer_count",
                "min_price",
                "max_price",
                "avg_price",
                "variance",
            ]

            return [{col: val for col, val in zip(columns, row)} for row in result]
        except Exception as e:
            # Log error and return empty result

            logger.error(f"Error getting payer variance: {e}")
            return []

    def get_total_row_count(self) -> int:
        """Get total rows in the database."""
        try:
            result = self.conn.execute("SELECT COUNT(*) FROM charges").fetchone()
            return result[0] if result else 0
        except Exception as e:
            # Log error and return 0 as default

            sys.stderr.write(f"Error getting total row count: {e}\n")
            return 0
