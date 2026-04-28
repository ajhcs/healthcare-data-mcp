"""
Database protocol for consistent database interfaces.

This protocol defines the common interface that all database implementations
(MultiHospitalDatabase, DuckDBDatabase, EmbeddedDatabase) must follow.
"""

from typing import Protocol, Optional, List, Dict, TYPE_CHECKING
from pathlib import Path

from .models import (
    ChargeRecord,
    HospitalInfo,
    Setting,
    BillingClass,
    CodeType,
    Methodology,
    AssetCategory,
)

# Type hints for classes defined in database.py to avoid circular imports
if TYPE_CHECKING:
    from .database import HospitalRecord, SearchResult


class DatabaseProtocol(Protocol):
    """
    Protocol defining the common interface for all database implementations.

    All database classes must implement these methods with matching signatures
    to ensure consistent behavior across different database backends.
    """

    def __init__(self, db_path: Optional[str | Path] = None):
        """
        Initialize the database with optional path.

        Args:
            db_path: Path to database file. If None, uses default location.
        """
        ...

    def connect(self) -> None:
        """Establish connection to the database."""
        ...

    def close(self) -> None:
        """Close database connection."""
        ...

    def get_all_hospitals(self) -> List["HospitalRecord"]:
        """Retrieve all hospitals in the database.

        Returns:
            List of HospitalRecord objects.
        """
        ...

    def get_hospital_by_id(self, hospital_id: int) -> Optional["HospitalRecord"]:
        """Get a hospital by its ID.

        Args:
            hospital_id: The hospital ID to look up.

        Returns:
            HospitalRecord if found, None otherwise.
        """
        ...

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
    ) -> "SearchResult":
        """Search charges with optional filters.

        Args:
            query: Full-text search query.
            hospital_ids: List of hospital IDs to filter by.
            payer: Payer name filter.
            setting: Care setting filter (inpatient/outpatient).
            code_type: Code type filter (CPT/HCPCS/DRG/etc).
            code: Specific code to match.
            min_price: Minimum price filter.
            max_price: Maximum price filter.
            page: Page number for pagination.
            page_size: Number of results per page.

        Returns:
            SearchResult containing matching rows and metadata.
        """
        ...


class HospitalManagementProtocol(Protocol):
    """Protocol for hospital metadata management operations."""

    def add_hospital(
        self,
        name: str,
        filename: str,
        address: str,
        last_updated: str,
        version: str,
    ) -> int:
        """Add a hospital to the database.

        Args:
            name: Hospital name.
            filename: Source filename.
            address: Hospital address.
            last_updated: Last update timestamp.
            version: File version.

        Returns:
            ID of the newly created hospital record.
        """
        ...

    def get_hospital_by_filename(self, filename: str) -> Optional["HospitalRecord"]:
        """Get hospital by source filename.

        Args:
            filename: Source CSV filename.

        Returns:
            HospitalRecord if found, None otherwise.
        """
        ...

    def get_total_row_count(self) -> int:
        """Get total number of charge records across all hospitals.

        Returns:
            Total row count.
        """
        ...


class DataImportProtocol(Protocol):
    """Protocol for data import operations."""

    def get_unique_payers(self, hospital_ids: Optional[List[int]] = None) -> List[str]:
        """Get list of unique payer names.

        Args:
            hospital_ids: Optional list of hospital IDs to filter by.

        Returns:
            List of unique payer names.
        """
        ...

    def get_unique_code_types(self) -> List[str]:
        """Get list of unique code types.

        Returns:
            List of code type strings (CPT, HCPCS, etc.).
        """
        ...

    def get_unique_settings(self) -> List[str]:
        """Get list of unique care settings.

        Returns:
            List of setting strings (inpatient, outpatient).
        """
        ...

    def import_charge_data(self, charges: List[ChargeRecord], hospital_id: int) -> None:
        """Import charge records for a hospital.

        Args:
            charges: List of ChargeRecord objects to import.
            hospital_id: ID of the hospital to associate with.
        """
        ...
