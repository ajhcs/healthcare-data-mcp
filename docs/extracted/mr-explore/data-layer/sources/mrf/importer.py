"""
Template-aware MRF importer.
Uses templates to import files from various vendors.
"""

import polars as pl
from pathlib import Path
from typing import Optional, Callable
from dataclasses import dataclass

from .templates import MRFTemplate, TemplateManager
from .json_parser import CMSJSONParser
from ...importer import HospitalInfo, ImportResult


class TemplateAwareImporter:
    """
    Importer that uses MRF templates to handle various file formats.

    Automatically detects or uses specified templates to parse files
    from different EHR vendors.
    """

    def __init__(self, progress_callback: Optional[Callable[[int, str], None]] = None):
        """
        Initialize the template-aware importer.

        Args:
            progress_callback: Optional callback for progress updates (percent, message)
        """
        self.progress_callback = progress_callback
        self.template_manager = TemplateManager()

    def _report_progress(self, percent: int, message: str):
        """Report progress via callback if provided."""
        if self.progress_callback:
            self.progress_callback(percent, message)

    def import_file(
        self,
        file_path: str | Path,
        template: Optional[MRFTemplate] = None
    ) -> ImportResult:
        """
        Import an MRF file using a template.

        Args:
            file_path: Path to the MRF file
            template: Optional template to use. If None, will auto-detect.

        Returns:
            ImportResult with parsed data
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return ImportResult(
                success=False,
                row_count=0,
                error_message=f"File not found: {file_path}",
                filename=file_path.name
            )

        self._report_progress(5, f"Reading {file_path.name}...")

        # Auto-detect template if not provided
        if template is None:
            self._report_progress(10, "Detecting file format...")
            template = self.template_manager.detect_template(file_path)

            if template is None:
                return ImportResult(
                    success=False,
                    row_count=0,
                    error_message="Could not detect file format. Please specify a template.",
                    filename=file_path.name
                )

            self._report_progress(15, f"Detected format: {template.name}")
        else:
            self._report_progress(10, f"Using template: {template.name}")

        # Route to appropriate importer
        if template.file_format == "json":
            return self._import_json(file_path, template)
        else:
            return self._import_csv(file_path, template)

    def _import_json(self, file_path: Path, template: MRFTemplate) -> ImportResult:
        """Import a JSON MRF file."""
        self._report_progress(20, "Parsing JSON...")

        parser = CMSJSONParser()
        result = parser.parse_file(file_path)

        if not result.success:
            return ImportResult(
                success=False,
                row_count=0,
                error_message=result.error_message,
                filename=file_path.name
            )

        self._report_progress(80, "Processing data...")

        hospital_info = None
        if result.hospital_name:
            hospital_info = HospitalInfo(
                name=result.hospital_name,
                location="",
                address="",
                last_updated=result.last_updated or "",
                version=""
            )

        self._report_progress(100, f"Loaded {result.record_count:,} records")

        return ImportResult(
            success=True,
            row_count=result.record_count,
            hospital_info=hospital_info,
            data=result.data,
            filename=file_path.name
        )

    def _import_csv(self, file_path: Path, template: MRFTemplate) -> ImportResult:
        """Import a CSV MRF file using a template."""
        self._report_progress(20, "Parsing CSV...")

        try:
            # Apply preprocessing steps
            skip_rows = template.header_rows
            encoding = template.encoding

            # Read CSV with Polars
            df = pl.read_csv(
                file_path,
                skip_rows=skip_rows,
                separator=template.delimiter,
                quote_char=template.quote_char,
                encoding=encoding,
                infer_schema_length=10000,
                ignore_errors=True,
                truncate_ragged_lines=True,
                null_values=template.null_values,
            )

            self._report_progress(50, "Mapping columns...")

            # Create reverse mapping (vendor column -> standard field)
            reverse_mapping = {v: k for k, v in template.column_mappings.items()}

            # Rename columns to standard names
            rename_mapping = {}
            for col in df.columns:
                if col in reverse_mapping:
                    rename_mapping[col] = reverse_mapping[col]

            if rename_mapping:
                df = df.rename(rename_mapping)

            self._report_progress(70, "Processing data types...")

            # Cast numeric columns
            numeric_columns = [
                'gross_charge', 'discounted_cash', 'negotiated_dollar',
                'negotiated_percentage', 'estimated_amount', 'min_charge', 'max_charge'
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))

            # Apply preprocessing steps
            preprocessing_steps = template.get_preprocessing_steps()
            for step, param in preprocessing_steps:
                df = self._apply_preprocessing(df, step, param)

            # Remove footer rows if specified
            if template.footer_rows > 0:
                df = df.head(len(df) - template.footer_rows)

            self._report_progress(80, "Extracting metadata...")

            # Extract hospital info
            hospital_info = self._extract_hospital_info(file_path, template)

            row_count = len(df)
            self._report_progress(100, f"Loaded {row_count:,} records")

            return ImportResult(
                success=True,
                row_count=row_count,
                hospital_info=hospital_info,
                data=df,
                filename=file_path.name
            )

        except Exception as e:
            return ImportResult(
                success=False,
                row_count=0,
                error_message=f"Import error: {str(e)}",
                filename=file_path.name
            )

    def _apply_preprocessing(self, df: pl.DataFrame, step, param: Optional[str]) -> pl.DataFrame:
        """Apply a preprocessing step to the DataFrame."""
        from .templates import PreprocessingStep

        if step == PreprocessingStep.TRIM_WHITESPACE:
            # Trim whitespace from string columns
            for col in df.columns:
                if df[col].dtype == pl.Utf8:
                    df = df.with_columns(pl.col(col).str.strip_chars())

        elif step == PreprocessingStep.REMOVE_EMPTY_ROWS:
            # Remove rows where all values are null
            df = df.filter(~pl.all_horizontal(pl.all().is_null()))

        elif step == PreprocessingStep.NORMALIZE_NULLS:
            # Already handled by null_values parameter in read_csv
            pass

        return df

    def _extract_hospital_info(self, file_path: Path, template: MRFTemplate) -> Optional[HospitalInfo]:
        """
        Extract hospital metadata from file.

        For CSV files, reads the header rows based on template configuration.
        """
        try:
            if template.header_rows > 0:
                with open(file_path, 'r', encoding=template.encoding, errors='replace') as f:
                    # Read header rows
                    header_lines = [f.readline().strip() for _ in range(template.header_rows)]

                # Try to extract info from second row (common pattern)
                if len(header_lines) >= 2:
                    info_line = header_lines[1]
                    info_values = self._parse_csv_line(info_line, template.delimiter)

                    if len(info_values) >= 5:
                        return HospitalInfo(
                            name=info_values[0] if len(info_values) > 0 else "Unknown",
                            last_updated=info_values[1] if len(info_values) > 1 else "",
                            version=info_values[2] if len(info_values) > 2 else "",
                            location=info_values[3] if len(info_values) > 3 else "",
                            address=info_values[4] if len(info_values) > 4 else "",
                            license_number=info_values[5] if len(info_values) > 5 else None,
                        )

            # Fallback: extract name from filename
            name = file_path.stem.replace("_standardcharges", "").replace("_", " ").title()
            return HospitalInfo(
                name=name,
                location="",
                address="",
                last_updated="",
                version=""
            )

        except Exception:
            name = file_path.stem.replace("_standardcharges", "").replace("_", " ").title()
            return HospitalInfo(name=name, location="", address="", last_updated="", version="")

    def _parse_csv_line(self, line: str, delimiter: str = ',') -> list[str]:
        """Parse a CSV line respecting quoted fields."""
        result = []
        current = ""
        in_quotes = False

        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == delimiter and not in_quotes:
                result.append(current.strip('"'))
                current = ""
            else:
                current += char

        result.append(current.strip('"'))
        return result

    def get_available_templates(self) -> list[MRFTemplate]:
        """Get list of all available templates."""
        return self.template_manager.get_templates()

    def detect_template(self, file_path: Path) -> Optional[MRFTemplate]:
        """Detect the best matching template for a file."""
        return self.template_manager.detect_template(file_path)
