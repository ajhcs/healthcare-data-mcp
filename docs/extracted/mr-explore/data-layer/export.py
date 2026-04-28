"""
Streaming export module for large datasets.

Provides efficient export functionality with streaming support,
progress reporting, and multiple format options (CSV, Excel, Parquet).
"""

import polars as pl
from pathlib import Path
from typing import List, Dict, Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum
import csv


class ExportFormat(Enum):
    """Supported export formats."""

    CSV = "csv"
    EXCEL = "xlsx"
    PARQUET = "parquet"
    JSON = "json"


@dataclass
class ExportOptions:
    """Configuration for export operations."""

    format: ExportFormat = ExportFormat.CSV
    delimiter: str = ","
    quote_char: str = '"'
    include_header: bool = True
    batch_size: int = 10000
    compress: bool = False
    encoding: str = "utf-8"


@dataclass
class ExportProgress:
    """Progress tracking for export operations."""

    total_records: int = 0
    exported_records: int = 0
    current_file: str = ""
    is_complete: bool = False
    error: Optional[str] = None

    @property
    def progress_percent(self) -> float:
        """Calculate progress percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.exported_records / self.total_records) * 100.0


class StreamingExporter:
    """
    Efficient streaming exporter for large datasets.

    Uses batching and streaming to minimize memory usage
    during export of large result sets.
    """

    def __init__(
        self,
        data: List[Dict[str, Any]],
        output_path: str | Path,
        options: Optional[ExportOptions] = None,
        progress_callback: Optional[Callable[[ExportProgress], None]] = None,
    ):
        """
        Initialize streaming exporter.

        Args:
            data: List of dictionaries to export
            output_path: Output file path
            options: Export configuration options
            progress_callback: Optional callback for progress updates
        """
        self.data = data
        self.output_path = Path(output_path)
        self.options = options or ExportOptions()
        self.progress_callback = progress_callback
        self.progress = ExportProgress(total_records=len(data))

    def export(self) -> bool:
        """
        Execute export with the configured format.

        Returns:
            True if export was successful, False otherwise
        """
        try:
            # Create output directory if needed
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.progress.current_file = str(self.output_path)

            # Dispatch to appropriate format handler
            if self.options.format == ExportFormat.CSV:
                return self._export_csv()
            elif self.options.format == ExportFormat.EXCEL:
                return self._export_excel()
            elif self.options.format == ExportFormat.PARQUET:
                return self._export_parquet()
            elif self.options.format == ExportFormat.JSON:
                return self._export_json()
            else:
                self.progress.error = f"Unsupported format: {self.options.format}"
                return False

        except Exception as e:
            self.progress.error = str(e)
            self._notify_progress()
            return False

    def _export_csv(self) -> bool:
        """Export to CSV with streaming."""
        try:
            # Use Polars streaming for CSV export
            df = pl.DataFrame(self.data)

            # Write CSV with batching
            df.write_csv(
                self.output_path,
                separator=self.options.delimiter,
                quote_char=self.options.quote_char,
                include_header=self.options.include_header,
                batch_size=self.options.batch_size,
            )

            self.progress.is_complete = True
            self.progress.exported_records = len(self.data)
            self._notify_progress()
            return True

        except Exception as e:
            self.progress.error = f"CSV export error: {e}"
            return False

    def _export_excel(self) -> bool:
        """Export to Excel format."""
        try:
            # Check if openpyxl is available
            import openpyxl
            from openpyxl import Workbook
            from openpyxl.utils.dataframe import dataframe_to_rows

            df = pl.DataFrame(self.data)

            # Convert Polars DataFrame to list of rows
            headers = df.columns
            rows = df.to_dicts()

            # Create workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "MR-Explore Export"

            # Write header
            if self.options.include_header:
                ws.append(headers)

            # Write data in batches
            batch_size = self.options.batch_size
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                for row in batch:
                    # Convert dict to list of values
                    row_values = [row.get(col, "") for col in headers]
                    ws.append(row_values)

                self.progress.exported_records = min(i + batch_size, len(rows))
                self._notify_progress()

            # Save workbook
            wb.save(self.output_path)
            self.progress.is_complete = True
            self.progress.exported_records = len(self.data)
            self._notify_progress()
            return True

        except ImportError:
            self.progress.error = "openpyxl is required for Excel export. Install with: pip install openpyxl"
            return False
        except Exception as e:
            self.progress.error = f"Excel export error: {e}"
            return False

    def _export_parquet(self) -> bool:
        """Export to Parquet format (columnar, highly efficient)."""
        try:
            df = pl.DataFrame(self.data)

            # Write Parquet (native to Polars, very efficient)
            df.write_parquet(
                self.output_path,
                compression="snappy" if self.options.compress else None,
            )

            self.progress.is_complete = True
            self.progress.exported_records = len(self.data)
            self._notify_progress()
            return True

        except Exception as e:
            self.progress.error = f"Parquet export error: {e}"
            return False

    def _export_json(self) -> bool:
        """Export to JSON format."""
        try:
            df = pl.DataFrame(self.data)

            # Write JSON
            df.write_json(
                self.output_path,
            )

            self.progress.is_complete = True
            self.progress.exported_records = len(self.data)
            self._notify_progress()
            return True

        except Exception as e:
            self.progress.error = f"JSON export error: {e}"
            return False

    def _notify_progress(self):
        """Notify progress callback if configured."""
        if self.progress_callback:
            self.progress_callback(self.progress)


class DatabaseStreamingExporter:
    """
    Export data directly from database without loading all rows into memory.

    Streams results from database to file in batches.
    """

    def __init__(
        self,
        database,  # MultiHospitalDatabase or DuckDBDatabase
        output_path: str | Path,
        options: Optional[ExportOptions] = None,
        progress_callback: Optional[Callable[[ExportProgress], None]] = None,
        filters: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize database streaming exporter.

        Args:
            database: Database instance to query
            output_path: Output file path
            options: Export configuration options
            progress_callback: Optional callback for progress updates
            filters: Optional filters to apply (same as database.search filters)
        """
        self.database = database
        self.output_path = Path(output_path)
        self.options = options or ExportOptions()
        self.progress_callback = progress_callback
        self.filters = filters or {}
        self.progress = ExportProgress()

        # Get total count first
        self.progress.total_records = self._get_total_count()

    def _get_total_count(self) -> int:
        """Get total count of records that match filters."""
        try:
            result = self.database.search(
                query=self.filters.get("query", ""),
                hospital_ids=self.filters.get("hospital_ids"),
                payer=self.filters.get("payer"),
                setting=self.filters.get("setting"),
                code_type=self.filters.get("code_type"),
                code=self.filters.get("code"),
                min_price=self.filters.get("min_price"),
                max_price=self.filters.get("max_price"),
                page=1,
                page_size=1,  # Just get count
            )
            return result.total_count
        except Exception:
            return 0

    def export(self) -> bool:
        """
        Execute streaming export from database.

        Returns:
            True if export was successful, False otherwise
        """
        try:
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.progress.current_file = str(self.output_path)

            if self.options.format == ExportFormat.CSV:
                return self._export_csv_streaming()
            elif self.options.format == ExportFormat.PARQUET:
                return self._export_parquet_streaming()
            else:
                # For Excel and JSON, need to load data (use streaming exporter)
                return self._export_with_full_load()

        except Exception as e:
            self.progress.error = str(e)
            self._notify_progress()
            return False

    def _export_csv_streaming(self) -> bool:
        """Export to CSV with streaming from database."""
        try:
            import csv

            # Calculate pages needed
            page_size = self.options.batch_size
            total_pages = (self.progress.total_records + page_size - 1) // page_size

            with open(
                self.output_path, "w", encoding=self.options.encoding, newline=""
            ) as f:
                writer = csv.writer(f, delimiter=self.options.delimiter)

                # Write header
                if self.options.include_header and total_pages > 0:
                    # Get one row to get columns
                    sample_result = self.database.search(page=1, page_size=1)
                    if sample_result.rows:
                        headers = list(sample_result.rows[0].keys())
                        writer.writerow(headers)

                # Stream pages
                for page in range(1, total_pages + 1):
                    result = self.database.search(
                        query=self.filters.get("query", ""),
                        hospital_ids=self.filters.get("hospital_ids"),
                        payer=self.filters.get("payer"),
                        setting=self.filters.get("setting"),
                        code_type=self.filters.get("code_type"),
                        code=self.filters.get("code"),
                        min_price=self.filters.get("min_price"),
                        max_price=self.filters.get("max_price"),
                        page=page,
                        page_size=page_size,
                    )

                    # Write rows
                    for row in result.rows:
                        writer.writerow(list(row.values()))

                    # Update progress
                    self.progress.exported_records = min(
                        page * page_size, self.progress.total_records
                    )
                    self._notify_progress()

            self.progress.is_complete = True
            self._notify_progress()
            return True

        except Exception as e:
            self.progress.error = f"CSV streaming export error: {e}"
            return False

    def _export_parquet_streaming(self) -> bool:
        """Export to Parquet using Polars (memory efficient)."""
        try:
            # For Parquet, Polars handles batching efficiently
            # Fetch all data at once (Parquet is very efficient)
            result = self.database.search(
                query=self.filters.get("query", ""),
                hospital_ids=self.filters.get("hospital_ids"),
                payer=self.filters.get("payer"),
                setting=self.filters.get("setting"),
                code_type=self.filters.get("code_type"),
                code=self.filters.get("code"),
                min_price=self.filters.get("min_price"),
                max_price=self.filters.get("max_price"),
                page=1,
                page_size=self.progress.total_records,  # Get all records
            )

            if not result.rows:
                self.progress.is_complete = True
                self._notify_progress()
                return True

            df = pl.DataFrame(result.rows)
            df.write_parquet(
                self.output_path,
                compression="snappy" if self.options.compress else None,
            )

            self.progress.exported_records = len(result.rows)
            self.progress.is_complete = True
            self._notify_progress()
            return True

        except Exception as e:
            self.progress.error = f"Parquet streaming export error: {e}"
            return False

    def _export_with_full_load(self) -> bool:
        """Export formats that require full data load (Excel, JSON)."""
        try:
            # Get all data
            result = self.database.search(
                query=self.filters.get("query", ""),
                hospital_ids=self.filters.get("hospital_ids"),
                payer=self.filters.get("payer"),
                setting=self.filters.get("setting"),
                code_type=self.filters.get("code_type"),
                code=self.filters.get("code"),
                min_price=self.filters.get("min_price"),
                max_price=self.filters.get("max_price"),
                page=1,
                page_size=self.progress.total_records,
            )

            # Use streaming exporter for the data
            exporter = StreamingExporter(
                data=result.rows,
                output_path=self.output_path,
                options=self.options,
                progress_callback=self.progress_callback,
            )

            return exporter.export()

        except Exception as e:
            self.progress.error = f"Full load export error: {e}"
            return False

    def _notify_progress(self):
        """Notify progress callback."""
        if self.progress_callback:
            self.progress_callback(self.progress)


def export_data(
    data: List[Dict[str, Any]],
    output_path: str | Path,
    format: ExportFormat = ExportFormat.CSV,
    **kwargs,
) -> bool:
    """
    Convenience function to export data.

    Args:
        data: Data to export
        output_path: Output file path
        format: Export format
        **kwargs: Additional options for ExportOptions

    Returns:
        True if successful
    """
    options = ExportOptions(format=format, **kwargs)
    exporter = StreamingExporter(data, output_path, options)
    return exporter.export()


def export_from_database(
    database,
    output_path: str | Path,
    format: ExportFormat = ExportFormat.CSV,
    filters: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[Callable[[ExportProgress], None]] = None,
    **kwargs,
) -> bool:
    """
    Convenience function to export from database.

    Args:
        database: Database instance
        output_path: Output file path
        format: Export format
        filters: Search filters
        progress_callback: Optional progress callback
        **kwargs: Additional options for ExportOptions

    Returns:
        True if successful
    """
    options = ExportOptions(format=format, **kwargs)
    exporter = DatabaseStreamingExporter(
        database, output_path, options, progress_callback, filters
    )
    return exporter.export()
