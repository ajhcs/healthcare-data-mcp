"""
High-performance CSV importer using Polars.
Handles large hospital charge files with progress tracking.
"""

import csv
import polars as pl
from pathlib import Path
from typing import Optional, Callable
from dataclasses import dataclass

from .models import CMS_COLUMN_MAPPING


@dataclass
class HospitalInfo:
    """Hospital metadata from file header."""

    name: str
    location: str
    address: str
    last_updated: str
    version: str
    license_number: Optional[str] = None


@dataclass
class ImportResult:
    """Result of a file import operation."""

    success: bool
    row_count: int
    hospital_info: Optional[HospitalInfo] = None
    error_message: Optional[str] = None
    data: Optional[pl.DataFrame] = None
    filename: str = ""
    pack_path: Optional[Path] = None


class ChargeFileImporter:
    """
    Importer for CMS hospital price transparency files.
    Uses Polars for fast CSV parsing.
    """

    def __init__(self, progress_callback: Optional[Callable[[int, str], None]] = None):
        self.progress_callback = progress_callback

    def _report_progress(self, percent: int, message: str):
        if self.progress_callback:
            self.progress_callback(percent, message)

    def _fallback_hospital_name(self, file_path: Path) -> str:
        """Generate a default hospital name from filename."""
        return (
            file_path.stem.replace("_standardcharges", "")
            .replace("_", " ")
            .title()
        )

    def _looks_like_data_header(self, values: list[str]) -> bool:
        """Detect whether a row is the tabular CSV header row."""
        normalized = {v.strip().lower() for v in values if v is not None}
        if not normalized:
            return False

        has_description = "description" in normalized
        has_code = any(k in normalized for k in ("code", "code_1", "code|1"))
        has_charge = any(
            k in normalized
            for k in ("standard_charge", "gross_charge", "standard_charge|gross")
        )
        has_billing = "billing_class" in normalized
        has_payer = "payer_name" in normalized

        return (has_description and has_code) or (has_charge and has_code) or (
            has_billing and has_payer
        )

    def _find_data_header_row(self, file_path: Path, max_scan_rows: int = 50) -> int:
        """Find the row index where column headers begin."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                for idx, line in enumerate(f):
                    if idx >= max_scan_rows:
                        break
                    stripped = line.strip()
                    if not stripped:
                        continue
                    values = self._parse_csv_line(stripped)
                    if self._looks_like_data_header(values):
                        return idx
        except Exception:
            return 2

        return 2

    def _extract_hospital_info(self, file_path: Path) -> Optional[HospitalInfo]:
        """Extract hospital metadata from file header."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                raw_lines = []
                for _ in range(6):
                    line = f.readline()
                    if not line:
                        break
                    raw_lines.append(line.strip())

            parsed = [self._parse_csv_line(line) for line in raw_lines if line]
            if not parsed:
                return HospitalInfo(
                    name=self._fallback_hospital_name(file_path),
                    location="",
                    address="",
                    last_updated="",
                    version="",
                )

            first = [v.strip() for v in parsed[0]]
            second = [v.strip() for v in parsed[1]] if len(parsed) > 1 else []
            third = [v.strip() for v in parsed[2]] if len(parsed) > 2 else []

            # Format A:
            #   Hospital Name
            #   <hospital name>
            #   <address>,<city>,<state_zip>,<last_updated>,<version>,<license?>
            if first and first[0].lower() == "hospital name" and len(first) == 1:
                name = second[0] if second else self._fallback_hospital_name(file_path)
                meta = third if len(third) >= 5 else []
                location = ", ".join([v for v in meta[:3] if v]) if meta else ""
                return HospitalInfo(
                    name=name,
                    location=location,
                    address=meta[0] if meta else "",
                    last_updated=meta[3] if len(meta) > 3 else "",
                    version=meta[4] if len(meta) > 4 else "",
                    license_number=meta[5] if len(meta) > 5 else None,
                )

            # Format B:
            #   Hospital Name,<name>,<location>,<address>,<last_updated>,<version>,...
            if first and first[0].lower() == "hospital name" and len(first) >= 2:
                return HospitalInfo(
                    name=first[1] or self._fallback_hospital_name(file_path),
                    location=first[2] if len(first) > 2 else "",
                    address=first[3] if len(first) > 3 else "",
                    last_updated=first[4] if len(first) > 4 else "",
                    version=first[5] if len(first) > 5 else "",
                    license_number=first[6] if len(first) > 6 else None,
                )

            # Format C (legacy):
            #   <name>,<last_updated>,<version>,<location>,<address>,<license?>
            if len(first) >= 5 and not self._looks_like_data_header(first):
                return HospitalInfo(
                    name=first[0] or self._fallback_hospital_name(file_path),
                    last_updated=first[1] if len(first) > 1 else "",
                    version=first[2] if len(first) > 2 else "",
                    location=first[3] if len(first) > 3 else "",
                    address=first[4] if len(first) > 4 else "",
                    license_number=first[5] if len(first) > 5 else None,
                )

            return HospitalInfo(
                name=self._fallback_hospital_name(file_path),
                location="",
                address="",
                last_updated="",
                version="",
            )
        except Exception as e:
            # Log error but return default HospitalInfo to keep import flowing
            print(f"Error extracting hospital info from {file_path}: {e}")
            return HospitalInfo(
                name=self._fallback_hospital_name(file_path),
                location="",
                address="",
                last_updated="",
                version="",
            )

    def _parse_csv_line(self, line: str) -> list[str]:
        """Parse a CSV line respecting quoted fields."""
        try:
            return next(csv.reader([line]))
        except Exception:
            # Preserve legacy behavior as fallback.
            return [part.strip().strip('"') for part in line.split(",")]

    def import_file(self, file_path: str | Path) -> ImportResult:
        """
        Import a CMS hospital charge file.
        Returns ImportResult with Polars DataFrame.
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return ImportResult(
                success=False,
                row_count=0,
                error_message=f"File not found: {file_path}",
                filename=file_path.name,
            )

        self._report_progress(5, f"Reading {file_path.name}...")
        hospital_info = self._extract_hospital_info(file_path)

        self._report_progress(10, "Loading with Polars...")

        try:
            header_row = self._find_data_header_row(file_path)

            # Read CSV with Polars, skip header rows
            df = pl.read_csv(
                file_path,
                skip_rows=header_row,
                infer_schema_length=10000,
                ignore_errors=True,
                truncate_ragged_lines=True,
                null_values=["", "N/A", "NA", "null"],
            )

            self._report_progress(60, "Processing columns...")

            # Rename columns to standard names
            rename_mapping = {}
            for orig_col in df.columns:
                if orig_col in CMS_COLUMN_MAPPING:
                    rename_mapping[orig_col] = CMS_COLUMN_MAPPING[orig_col]

            # If fewer than 3 columns matched via exact mapping, try fuzzy matching
            if len(rename_mapping) < 3:
                from .recognition import fuzzy_match_columns

                fuzzy_mapping = fuzzy_match_columns(df.columns, CMS_COLUMN_MAPPING)
                # Use fuzzy matches for columns not already mapped.
                # Skip identity mappings (column already has the target name)
                # and skip targets that already exist as raw column names to
                # avoid Polars duplicate-column errors.
                for orig, target in fuzzy_mapping.items():
                    if orig == target:
                        continue
                    if orig not in rename_mapping and target not in rename_mapping.values():
                        if target not in df.columns:
                            rename_mapping[orig] = target

            if rename_mapping:
                df = df.rename(rename_mapping)

            self._report_progress(80, "Optimizing types...")

            # Cast numeric columns
            numeric_columns = [
                "gross_charge",
                "discounted_cash",
                "negotiated_dollar",
                "negotiated_percentage",
                "estimated_amount",
                "min_charge",
                "max_charge",
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

            row_count = len(df)
            self._report_progress(100, f"Loaded {row_count:,} records")

            return ImportResult(
                success=True,
                row_count=row_count,
                hospital_info=hospital_info,
                data=df,
                filename=file_path.name,
            )

        except Exception as e:
            return ImportResult(
                success=False,
                row_count=0,
                error_message=f"Import error: {str(e)}",
                filename=file_path.name,
            )
