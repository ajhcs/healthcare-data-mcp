"""
SQLite database layer with multi-hospital support.
Provides unified search and comparison across hospital files.
"""

import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
import hashlib

# Try to import Rust extension, fall back to SQL-based search if not available
try:
    import mr_search

    RUST_SEARCH_AVAILABLE = True
except ImportError:
    mr_search = None
    RUST_SEARCH_AVAILABLE = False

# Import protocol for type hints
from .database_protocol import (
    DatabaseProtocol,
    HospitalManagementProtocol,
    DataImportProtocol,
)


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


class MultiHospitalDatabase(
    DatabaseProtocol, HospitalManagementProtocol, DataImportProtocol
):
    """
    SQLite database supporting multiple hospital files.
    Enables cross-hospital comparison and unified search.

    Implements DatabaseProtocol, HospitalManagementProtocol, and DataImportProtocol.
    """

    CHARGE_COLUMNS = [
        ("id", "INTEGER PRIMARY KEY AUTOINCREMENT"),
        ("hospital_id", "INTEGER NOT NULL"),
        ("description", "TEXT"),
        ("code_1", "TEXT"),
        ("code_1_type", "TEXT"),
        ("code_2", "TEXT"),
        ("code_2_type", "TEXT"),
        ("modifiers", "TEXT"),
        ("setting", "TEXT"),
        ("billing_class", "TEXT"),
        ("drug_unit_of_measurement", "TEXT"),
        ("drug_type_of_measurement", "TEXT"),
        ("gross_charge", "REAL"),
        ("discounted_cash", "REAL"),
        ("payer_name", "TEXT"),
        ("plan_name", "TEXT"),
        ("negotiated_dollar", "REAL"),
        ("negotiated_percentage", "REAL"),
        ("negotiated_algorithm", "TEXT"),
        ("estimated_amount", "REAL"),
        ("methodology", "TEXT"),
        ("min_charge", "REAL"),
        ("max_charge", "REAL"),
        ("additional_notes", "TEXT"),
    ]

    def __init__(self, db_path: Optional[str | Path] = None):
        """Initialize with optional persistent database path."""
        self.db_path = str(db_path) if db_path else ":memory:"
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        """Open database connection and create tables."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _create_tables(self):
        """Create database schema."""
        # Hospitals table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS hospitals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                filename TEXT UNIQUE NOT NULL,
                address TEXT,
                last_updated TEXT,
                version TEXT,
                row_count INTEGER DEFAULT 0,
                is_loaded INTEGER DEFAULT 1
            )
        """)

        # Charges table with hospital foreign key
        columns_sql = ", ".join(
            f"{name} {dtype}" for name, dtype in self.CHARGE_COLUMNS
        )
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS charges (
                {columns_sql},
                FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE
            )
        """)

        self.conn.commit()

        # Create indexes
        self._create_indexes()

    def _create_indexes(self):
        """Create database indexes."""
        # Indexes for fast queries
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hospital_id ON charges(hospital_id)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_description ON charges(description)"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_code_1 ON charges(code_1)")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_code_1_type ON charges(code_1_type)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_payer_name ON charges(payer_name)"
        )
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_setting ON charges(setting)")

        # Compound indexes for common queries
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hospital_payer ON charges(hospital_id, payer_name)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_code_hospital ON charges(code_1, hospital_id)"
        )

        # Full-text search
        # Rust-based Search Index table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS search_indexes (
                hospital_id INTEGER PRIMARY KEY,
                index_data BLOB,
                last_updated TEXT,
                FOREIGN KEY (hospital_id) REFERENCES hospitals(id) ON DELETE CASCADE
            )
        """)
        self.conn.commit()

    def _drop_indexes(self):
        """Drop non-primary key indexes for bulk import performance."""
        indexes = [
            "idx_hospital_id",
            "idx_description",
            "idx_code_1",
            "idx_code_1_type",
            "idx_payer_name",
            "idx_setting",
            "idx_hospital_payer",
            "idx_code_hospital",
        ]

        for idx in indexes:
            self.conn.execute(f"DROP INDEX IF EXISTS {idx}")

        # We also don't touch the search_indexes table here as it's separate

        self.conn.commit()

    # ==================== Hospital Management ====================

    def add_hospital(
        self,
        name: str,
        filename: str,
        address: str = "",
        last_updated: str = "",
        version: str = "",
    ) -> int:
        """Add a hospital record. Returns hospital_id."""
        cursor = self.conn.execute(
            """
            INSERT INTO hospitals (name, filename, address, last_updated, version)
            VALUES (?, ?, ?, ?, ?)
        """,
            (name, filename, address, last_updated, version),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_hospital_by_filename(self, filename: str) -> Optional[HospitalRecord]:
        """Get hospital by filename."""
        cursor = self.conn.execute(
            "SELECT * FROM hospitals WHERE filename = ?", (filename,)
        )
        row = cursor.fetchone()
        if row:
            return HospitalRecord(
                id=row["id"],
                name=row["name"],
                filename=row["filename"],
                address=row["address"] or "",
                row_count=row["row_count"],
                last_updated=row["last_updated"] or "",
            )
        return None

    def get_all_hospitals(self) -> List[HospitalRecord]:
        """Get all loaded hospitals."""
        cursor = self.conn.execute("SELECT * FROM hospitals ORDER BY name")
        return [
            HospitalRecord(
                id=row["id"],
                name=row["name"],
                filename=row["filename"],
                address=row["address"] or "",
                row_count=row["row_count"],
                last_updated=row["last_updated"] or "",
            )
            for row in cursor.fetchall()
        ]

    def update_hospital_row_count(self, hospital_id: int, count: int):
        """Update the row count for a hospital."""
        self.conn.execute(
            "UPDATE hospitals SET row_count = ? WHERE id = ?", (count, hospital_id)
        )
        self.conn.commit()

    def remove_hospital(self, hospital_id: int):
        """Remove a hospital and all its charges."""
        self.conn.execute("DELETE FROM charges WHERE hospital_id = ?", (hospital_id,))
        self.conn.execute("DELETE FROM hospitals WHERE id = ?", (hospital_id,))
        self.conn.commit()
        # Rebuild FTS
        self.conn.execute("INSERT INTO charges_fts(charges_fts) VALUES('rebuild')")
        self.conn.commit()

    # ==================== Data Import ====================

    def import_charges(self, hospital_id: int, data, progress_callback=None) -> int:
        """
        Import charges from a Polars DataFrame.

        Args:
            hospital_id: Target hospital ID
            data: Polars DataFrame with charge data
            progress_callback: Optional (percent, message) callback

        Returns:
            Number of rows imported
        """
        import polars as pl

        total_rows = len(data)

        # Clear existing charges for this hospital
        self.conn.execute("DELETE FROM charges WHERE hospital_id = ?", (hospital_id,))

        if progress_callback:
            progress_callback(55, "Optimizing database for bulk insert...")

        # Drop indexes for performance
        self._drop_indexes()

        # Get column mappings
        data_columns = set(data.columns)
        our_columns = [
            name for name, _ in self.CHARGE_COLUMNS if name not in ("id", "hospital_id")
        ]
        insert_columns_names = [c for c in our_columns if c in data_columns]

        # Add hospital_id column to DataFrame
        # This is much faster than doing it in Python loop
        if progress_callback:
            progress_callback(60, "Preparing data vectors...")

        # Select only the columns we need, in the right order
        df_sorted = data.select(insert_columns_names)

        # Add hospital_id as the first column
        df_sorted = df_sorted.with_columns(pl.lit(hospital_id).alias("hospital_id"))

        # Reorder to put hospital_id first: hospital_id, col1, col2...
        final_cols = ["hospital_id"] + insert_columns_names
        df_sorted = df_sorted.select(final_cols)

        # Prepare SQL
        placeholders = ", ".join(["?" for _ in final_cols])
        columns_str = ", ".join(final_cols)
        insert_sql = f"INSERT INTO charges ({columns_str}) VALUES ({placeholders})"

        # Bulk insert using iterator (named=False yields tuples, which is much faster)
        if progress_callback:
            progress_callback(65, f"Inserting {total_rows:,} rows...")

        # We can increase batch size significantly since we're using tuples
        batch_size = 50000
        rows_inserted = 0

        # Use a transaction for the whole insert
        try:
            for i in range(0, total_rows, batch_size):
                chunk = df_sorted.slice(i, batch_size)
                # named=False returns tuples, preventing dict overhead
                # This is the critical optimization
                self.conn.executemany(insert_sql, chunk.iter_rows(named=False))

                rows_inserted += len(chunk)

                if progress_callback:
                    # Progress from 65 to 90
                    pct = 65 + int(25 * rows_inserted / total_rows)
                    progress_callback(
                        pct, f"Imported {rows_inserted:,} / {total_rows:,}"
                    )

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            # Restore indexes if we fail
            self._create_indexes()
            raise e

        # Update hospital row count
        self.update_hospital_row_count(hospital_id, rows_inserted)

        # Rebuild Indexes
        if progress_callback:
            progress_callback(90, "Rebuilding search indexes...")

        self._create_indexes()

        # Rebuild Rust Search Index (if available)
        if RUST_SEARCH_AVAILABLE:
            if progress_callback:
                progress_callback(95, "Building high-performance search index...")

            # Fetch data for indexing (description + code + payer)
            cursor = self.conn.execute(
                "SELECT id, description, code_1, payer_name FROM charges WHERE hospital_id = ?",
                (hospital_id,),
            )

            # Prepare content for tokenization
            # We combine fields into a single text for simple broad search
            ids = []
            texts = []
            for row in cursor:
                ids.append(row[0])
                # Handle None values
                desc = row[1] or ""
                code = row[2] or ""
                payer = row[3] or ""
                texts.append(f"{desc} {code} {payer}")

            if ids:
                # Build index using Rust extension -- FAST!
                index_bytes = bytes(mr_search.build_index(ids, texts))

                # Store in database
                self.conn.execute(
                    "INSERT OR REPLACE INTO search_indexes (hospital_id, index_data, last_updated) VALUES (?, ?, datetime('now'))",
                    (hospital_id, index_bytes),
                )
                self.conn.commit()

        return rows_inserted

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

        Args:
            query: Full-text search query
            hospital_ids: Filter to specific hospitals (None = all)
            payer: Filter by payer name
            setting: Filter by setting
            code_type: Filter by code type
            code: Filter by code
            min_price: Minimum negotiated price
            max_price: Maximum negotiated price
            page: Page number (1-indexed)
            page_size: Results per page
        """
        conditions = []
        params = []

        row_ids_from_search = None

        if query:
            if RUST_SEARCH_AVAILABLE:
                # Use Rust extension for fast BM25 search
                # 1. First find matching hospitals if filters applied
                target_hospitals = (
                    hospital_ids
                    if hospital_ids
                    else [
                        h[0]
                        for h in self.conn.execute(
                            "SELECT id FROM hospitals"
                        ).fetchall()
                    ]
                )

                row_ids_from_search = set()

                # 2. Search each hospital's index
                for hid in target_hospitals:
                    cursor = self.conn.execute(
                        "SELECT index_data FROM search_indexes WHERE hospital_id = ?",
                        (hid,),
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        # Search using Rust extension
                        # limit=1000 per hospital to keep it fast
                        results = mr_search.search(row[0], query, 1000)
                        row_ids_from_search.update(id for id, _ in results)

                if not row_ids_from_search:
                    # No results found across any hospital
                    return SearchResult(
                        rows=[], total_count=0, page=page, page_size=page_size
                    )

                # Restrict main query to these IDs
                placeholders = ",".join("?" for _ in row_ids_from_search)
                conditions.append(f"charges.id IN ({placeholders})")
                params.extend(row_ids_from_search)
            else:
                # Fallback: SQL LIKE search on description, code, and payer
                search_term = f"%{query}%"
                conditions.append(
                    "(description LIKE ? OR code_1 LIKE ? OR payer_name LIKE ?)"
                )
                params.extend([search_term, search_term, search_term])

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        if payer:
            conditions.append("payer_name = ?")
            params.append(payer)

        if setting:
            conditions.append("setting = ?")
            params.append(setting)

        if code_type:
            conditions.append("code_1_type = ?")
            params.append(code_type)

        if code:
            conditions.append("(code_1 LIKE ? OR code_2 LIKE ?)")
            params.extend([f"%{code}%", f"%{code}%"])

        if min_price is not None:
            conditions.append("negotiated_dollar >= ?")
            params.append(min_price)

        if max_price is not None:
            conditions.append("negotiated_dollar <= ?")
            params.append(max_price)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count
        count_sql = f"""
            SELECT COUNT(*) FROM charges
            JOIN hospitals ON charges.hospital_id = hospitals.id
            WHERE {where_clause}
        """
        total_count = self.conn.execute(count_sql, params).fetchone()[0]

        # Select with hospital name
        offset = (page - 1) * page_size
        select_sql = f"""
            SELECT charges.*, hospitals.name as hospital_name
            FROM charges
            JOIN hospitals ON charges.hospital_id = hospitals.id
            WHERE {where_clause}
            ORDER BY description
            LIMIT ? OFFSET ?
        """
        params.extend([page_size, offset])

        cursor = self.conn.execute(select_sql, params)
        rows = [dict(row) for row in cursor.fetchall()]

        return SearchResult(
            rows=rows, total_count=total_count, page=page, page_size=page_size
        )

    def get_unique_payers(self, hospital_ids: Optional[List[int]] = None) -> List[str]:
        """Get unique payer names, optionally filtered by hospitals."""
        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            cursor = self.conn.execute(
                f"""
                SELECT DISTINCT payer_name FROM charges
                WHERE hospital_id IN ({placeholders}) AND payer_name IS NOT NULL
                ORDER BY payer_name
            """,
                hospital_ids,
            )
        else:
            cursor = self.conn.execute("""
                SELECT DISTINCT payer_name FROM charges
                WHERE payer_name IS NOT NULL ORDER BY payer_name
            """)
        return [row[0] for row in cursor.fetchall() if row[0]]

    def get_unique_code_types(self) -> List[str]:
        """Get unique code types."""
        cursor = self.conn.execute("""
            SELECT DISTINCT code_1_type FROM charges
            WHERE code_1_type IS NOT NULL ORDER BY code_1_type
        """)
        return [row[0] for row in cursor.fetchall() if row[0]]

    def get_unique_settings(self) -> List[str]:
        """Get unique settings."""
        cursor = self.conn.execute("""
            SELECT DISTINCT setting FROM charges
            WHERE setting IS NOT NULL ORDER BY setting
        """)
        return [row[0] for row in cursor.fetchall() if row[0]]

    # ==================== Comparison Queries ====================

    def compare_payers_for_code(
        self, code: str, hospital_ids: Optional[List[int]] = None
    ) -> List[Dict]:
        """
        Get all payer rates for a specific code.
        Returns rows suitable for a comparison matrix.
        """
        conditions = ["code_1 = ?"]
        params = [code]

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        where_clause = " AND ".join(conditions)

        cursor = self.conn.execute(
            f"""
            SELECT 
                hospitals.name as hospital_name,
                charges.description,
                charges.code_1,
                charges.code_1_type,
                charges.payer_name,
                charges.plan_name,
                charges.negotiated_dollar,
                charges.gross_charge,
                charges.min_charge,
                charges.max_charge,
                charges.setting
            FROM charges
            JOIN hospitals ON charges.hospital_id = hospitals.id
            WHERE {where_clause}
            ORDER BY hospital_name, payer_name
        """,
            params,
        )

        return [dict(row) for row in cursor.fetchall()]

    def compare_hospitals_for_code(
        self, code: str, payer: Optional[str] = None
    ) -> List[Dict]:
        """
        Compare the same code across hospitals, optionally filtered by payer.
        """
        conditions = ["code_1 = ?"]
        params = [code]

        if payer:
            conditions.append("payer_name = ?")
            params.append(payer)

        where_clause = " AND ".join(conditions)

        cursor = self.conn.execute(
            f"""
            SELECT 
                hospitals.name as hospital_name,
                charges.description,
                charges.code_1,
                charges.payer_name,
                charges.plan_name,
                AVG(charges.negotiated_dollar) as avg_negotiated,
                MIN(charges.negotiated_dollar) as min_negotiated,
                MAX(charges.negotiated_dollar) as max_negotiated,
                AVG(charges.gross_charge) as avg_gross,
                COUNT(*) as plan_count
            FROM charges
            JOIN hospitals ON charges.hospital_id = hospitals.id
            WHERE {where_clause}
            GROUP BY hospitals.id, charges.payer_name
            ORDER BY hospital_name, payer_name
        """,
            params,
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_payer_variance(
        self,
        hospital_ids: Optional[List[int]] = None,
        code: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """
        Find services with highest payer variance.
        Returns services sorted by price variance across payers.
        """
        conditions = ["negotiated_dollar IS NOT NULL"]
        params = []

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        if code:
            conditions.append("code_1 LIKE ?")
            params.append(f"%{code}%")

        where_clause = " AND ".join(conditions)

        cursor = self.conn.execute(
            f"""
            SELECT 
                code_1,
                code_1_type,
                description,
                COUNT(DISTINCT payer_name) as payer_count,
                MIN(negotiated_dollar) as min_price,
                MAX(negotiated_dollar) as max_price,
                AVG(negotiated_dollar) as avg_price,
                (MAX(negotiated_dollar) - MIN(negotiated_dollar)) as variance
            FROM charges
            WHERE {where_clause}
            GROUP BY code_1
            HAVING payer_count > 1
            ORDER BY variance DESC
            LIMIT ?
        """,
            params + [limit],
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_total_row_count(self) -> int:
        """Get total rows across all hospitals."""
        cursor = self.conn.execute("SELECT SUM(row_count) FROM hospitals")
        result = cursor.fetchone()[0]
        return result or 0
