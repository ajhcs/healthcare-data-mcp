"""
Embedded Database Adapter
--------------------------
Reads from the pre-built, normalized hospital_data.db shipped with the app.
Provides the same interface as MultiHospitalDatabase but uses JOINs
to denormalize data on the fly for display.
"""

import sqlite3
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# Import protocol for type hints
from .database_protocol import DatabaseProtocol


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


def get_embedded_db_path() -> Path:
    """Get the path to the embedded hospital_data.db file."""
    # Check if running as PyInstaller bundle
    if getattr(sys, "frozen", False):
        # Running as compiled executable
        if hasattr(sys, "_MEIPASS"):
            base_path = Path(sys._MEIPASS)
        else:
            base_path = Path(sys.executable).parent

        # Check possible locations
        candidates = [
            base_path / "dist" / "hospital_data.db",
            base_path / "_internal" / "dist" / "hospital_data.db",
            base_path / "_internal" / "hospital_data.db",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

        # Fallback
        return base_path / "dist" / "hospital_data.db"
    else:
        # Running in development
        return Path(__file__).parent.parent.parent / "dist" / "hospital_data.db"


class EmbeddedDatabase(DatabaseProtocol):
    """
    Adapter for the pre-built, normalized hospital database.

    This class provides a read-only interface to the embedded database,
    using JOINs to denormalize data for display while keeping the
    storage compact.

    Implements DatabaseProtocol.
    """

    def __init__(self, db_path: Optional[str | Path] = None):
        """Initialize with the embedded database path."""
        if db_path:
            self.db_path = Path(db_path)
        else:
            self.db_path = get_embedded_db_path()

        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        """Open read-only database connection."""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Embedded database not found: {self.db_path}")

        # Open in read-only mode for the embedded database
        self.conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro", uri=True, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    # ==================== Hospital Management ====================

    def get_all_hospitals(self) -> List[HospitalRecord]:
        """Get all hospitals in the embedded database."""
        cursor = self.conn.execute("""
            SELECT 
                h.id,
                h.name,
                h.name as filename,  -- We don't have filename in new schema
                COALESCE(h.location, '') || ' ' || COALESCE(h.address, '') as address,
                (SELECT COUNT(*) FROM charges WHERE hospital_id = h.id) as row_count,
                '' as last_updated
            FROM hospitals h
            ORDER BY h.name
        """)
        return [
            HospitalRecord(
                id=row["id"],
                name=row["name"],
                filename=row["filename"],
                address=row["address"].strip(),
                row_count=row["row_count"],
                last_updated=row["last_updated"],
            )
            for row in cursor.fetchall()
        ]

    def get_hospital_by_id(self, hospital_id: int) -> Optional[HospitalRecord]:
        """Get a hospital by ID."""
        cursor = self.conn.execute(
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
            (hospital_id,),
        )
        row = cursor.fetchone()
        if row:
            return HospitalRecord(
                id=row["id"],
                name=row["name"],
                filename=row["filename"],
                address=row["address"].strip(),
                row_count=row["row_count"],
                last_updated=row["last_updated"],
            )
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

        # Handle FTS query
        if query:
            # Use FTS5 external content table
            conditions.append("""
                c.description_id IN (
                    SELECT rowid FROM descriptions_fts WHERE descriptions_fts MATCH ?
                )
            """)
            # FTS5 requires special query syntax
            fts_query = " ".join(f'"{word}"*' for word in query.split())
            params.append(fts_query)

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
        except sqlite3.OperationalError as e:
            # FTS query might fail if no matches
            if "fts5" in str(e).lower():
                return SearchResult(
                    rows=[], total_count=0, page=page, page_size=page_size
                )
            raise

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
            cursor = self.conn.execute(select_sql, params)
            rows = [dict(row) for row in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            if "fts5" in str(e).lower():
                return SearchResult(
                    rows=[], total_count=0, page=page, page_size=page_size
                )
            raise

        return SearchResult(
            rows=rows, total_count=total_count, page=page, page_size=page_size
        )

    def get_unique_payers(self, hospital_ids: Optional[List[int]] = None) -> List[str]:
        """Get unique payer names."""
        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            cursor = self.conn.execute(
                f"""
                SELECT DISTINCT p.name 
                FROM payers p
                JOIN charges c ON c.payer_id = p.id
                WHERE c.hospital_id IN ({placeholders})
                ORDER BY p.name
            """,
                hospital_ids,
            )
        else:
            cursor = self.conn.execute("""
                SELECT name FROM payers ORDER BY name
            """)
        return [row[0] for row in cursor.fetchall() if row[0]]

    def get_unique_code_types(self) -> List[str]:
        """Get unique code types."""
        cursor = self.conn.execute("""
            SELECT DISTINCT code1_type FROM charges
            WHERE code1_type IS NOT NULL ORDER BY code1_type
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
        """Get all payer rates for a specific code."""
        conditions = ["c.code1 = ?"]
        params = [code]

        if hospital_ids and len(hospital_ids) > 0:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"c.hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        where_clause = " AND ".join(conditions)

        cursor = self.conn.execute(
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
        )

        return [dict(row) for row in cursor.fetchall()]

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

        cursor = self.conn.execute(
            f"""
            SELECT 
                h.name as hospital_name,
                d.text as description,
                c.code1 as code_1,
                p.name as payer_name,
                pl.name as plan_name,
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
            GROUP BY h.id, p.name
            ORDER BY h.name, p.name
        """,
            params,
        )

        return [dict(row) for row in cursor.fetchall()]

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

        cursor = self.conn.execute(
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
            GROUP BY c.code1
            HAVING payer_count > 1
            ORDER BY variance DESC
            LIMIT ?
        """,
            params + [top_n],
        )

        return [dict(row) for row in cursor.fetchall()]

    def get_total_row_count(self) -> int:
        """Get total rows in the database."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM charges")
        return cursor.fetchone()[0] or 0
