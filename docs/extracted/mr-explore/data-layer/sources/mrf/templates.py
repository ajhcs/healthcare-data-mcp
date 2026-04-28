"""
MRF Template System for handling various hospital price transparency file formats.
Supports multiple EHR vendors (Epic, Cerner, Meditech) and CMS JSON format.
"""

import json
import polars as pl
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum

from ....logging import get_logger

logger = get_logger(__name__)


class PreprocessingStep(Enum):
    """Preprocessing operations that can be applied to MRF files."""

    SKIP_HEADER_ROWS = "skip_header_rows"
    REMOVE_FOOTER_ROWS = "remove_footer_rows"
    TRIM_WHITESPACE = "trim_whitespace"
    NORMALIZE_NULLS = "normalize_nulls"
    FLATTEN_JSON = "flatten_json"
    EXTRACT_METADATA = "extract_metadata"
    REMOVE_EMPTY_ROWS = "remove_empty_rows"
    CONVERT_ENCODING = "convert_encoding"


@dataclass
class MRFTemplate:
    """
    Template for parsing MRF files from different vendors.

    Attributes:
        name: Template identifier (e.g., "CMS Standard", "Epic MyChart")
        vendor: EHR vendor name (Epic, Cerner, Meditech, etc.)
        version: Template version string
        column_mappings: Maps standard field names to vendor-specific column names
        preprocessing: List of preprocessing steps to apply (format: "step:param")
        date_format: strftime format string for date parsing
        encoding: File encoding (utf-8, utf-16, etc.)
        file_format: File format (csv, json, xlsx)
        json_schema: Optional JSON schema for nested JSON files
        header_rows: Number of header rows to skip
        footer_rows: Number of footer rows to remove
        delimiter: CSV delimiter character
        quote_char: CSV quote character
        null_values: List of strings to treat as null
        description: Human-readable description
        custom_parsers: Custom parsing logic per field
    """

    name: str
    vendor: str
    version: str
    column_mappings: Dict[str, str]
    preprocessing: List[str] = field(default_factory=list)
    date_format: str = "%Y-%m-%d"
    encoding: str = "utf-8"
    file_format: str = "csv"
    json_schema: Optional[Dict[str, Any]] = None
    header_rows: int = 2
    footer_rows: int = 0
    delimiter: str = ","
    quote_char: str = '"'
    null_values: List[str] = field(
        default_factory=lambda: ["", "N/A", "NA", "null", "NULL"]
    )
    description: str = ""
    custom_parsers: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert template to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MRFTemplate":
        """Create template from dictionary."""
        return cls(**data)

    def get_preprocessing_steps(self) -> List[Tuple[PreprocessingStep, Optional[str]]]:
        """Parse preprocessing steps into (step, parameter) tuples."""
        steps = []
        for step_str in self.preprocessing:
            if ":" in step_str:
                step_name, param = step_str.split(":", 1)
                try:
                    step = PreprocessingStep(step_name)
                    steps.append((step, param))
                except ValueError:
                    pass
            else:
                try:
                    step = PreprocessingStep(step_str)
                    steps.append((step, None))
                except ValueError:
                    pass
        return steps


class TemplateManager:
    """
    Manages MRF templates: built-in and user-defined.
    Handles template detection, saving, loading, and import/export.
    """

    def __init__(self, templates_dir: Optional[Path] = None):
        """
        Initialize template manager.

        Args:
            templates_dir: Directory for storing custom templates.
                          Defaults to data/templates/mrf/
        """
        if templates_dir is None:
            templates_dir = (
                Path(__file__).parent.parent.parent.parent.parent
                / "data"
                / "templates"
                / "mrf"
            )

        self.templates_dir = Path(templates_dir)
        self.templates_dir.mkdir(parents=True, exist_ok=True)

        self._builtin_templates = self._create_builtin_templates()
        self._custom_templates: Dict[str, MRFTemplate] = {}
        self._load_custom_templates()

    def _create_builtin_templates(self) -> Dict[str, MRFTemplate]:
        """Create built-in templates for common vendors."""
        templates = {}

        # CMS Standard Template
        templates["CMS Standard"] = MRFTemplate(
            name="CMS Standard",
            vendor="CMS",
            version="1.0",
            description="Standard CMS hospital price transparency format",
            column_mappings={
                "description": "Description",
                "code_1": "Code|1",
                "code_1_type": "Code|1|Type",
                "code_2": "Code|2",
                "code_2_type": "Code|2|Type",
                "modifiers": "Modifiers",
                "setting": "Setting",
                "drug_unit_of_measurement": "Drug_Unit_Of_Measurement",
                "drug_type_of_measurement": "Drug_Type_Of_Measurement",
                "gross_charge": "Standard_Charge|Gross",
                "discounted_cash": "Standard_Charge|Discounted_Cash",
                "payer_name": "Payer_Name",
                "plan_name": "Plan_Name",
                "negotiated_dollar": "Standard_Charge|Negotiated_Dollar",
                "negotiated_percentage": "Standard_Charge|Negotiated_Percentage",
                "negotiated_algorithm": "Standard_Charge|Negotiated_Algorithm",
                "estimated_amount": "Estimated_Amount",
                "methodology": "Standard_Charge|Methodology",
                "min_charge": "Standard_Charge|Min",
                "max_charge": "Standard_Charge|Max",
                "additional_notes": "Additional_Generic_Notes",
                "billing_class": "Billing_Class",
            },
            preprocessing=["skip_header_rows:2", "normalize_nulls", "trim_whitespace"],
            header_rows=2,
            encoding="utf-8",
            file_format="csv",
        )

        # Epic MyChart Export Template
        templates["Epic MyChart"] = MRFTemplate(
            name="Epic MyChart",
            vendor="Epic",
            version="2024.1",
            description="Epic MyChart hospital price export format",
            column_mappings={
                "description": "Procedure Description",
                "code_1": "CPT/HCPCS Code",
                "code_1_type": "Code Type",
                "code_2": "Secondary Code",
                "code_2_type": "Secondary Code Type",
                "modifiers": "CPT Modifiers",
                "setting": "Patient Type",
                "gross_charge": "Gross Charge",
                "discounted_cash": "Cash Price",
                "payer_name": "Insurance Carrier",
                "plan_name": "Insurance Plan",
                "negotiated_dollar": "Contracted Rate",
                "min_charge": "Minimum Negotiated Charge",
                "max_charge": "Maximum Negotiated Charge",
                "additional_notes": "Notes",
                "billing_class": "Revenue Code",
            },
            preprocessing=["skip_header_rows:1", "normalize_nulls", "trim_whitespace"],
            header_rows=1,
            encoding="utf-8",
            file_format="csv",
            delimiter=",",
            date_format="%m/%d/%Y",
        )

        # Cerner Standard Template
        templates["Cerner Standard"] = MRFTemplate(
            name="Cerner Standard",
            vendor="Cerner",
            version="2024",
            description="Cerner Millennium price transparency export",
            column_mappings={
                "description": "SERVICE_DESCRIPTION",
                "code_1": "PROCEDURE_CODE",
                "code_1_type": "PROCEDURE_CODE_TYPE",
                "code_2": "DRG_CODE",
                "code_2_type": "DRG_TYPE",
                "modifiers": "MODIFIER",
                "setting": "SERVICE_SETTING",
                "gross_charge": "GROSS_CHARGE_AMT",
                "discounted_cash": "SELF_PAY_DISCOUNT_AMT",
                "payer_name": "PAYER_NAME",
                "plan_name": "PLAN_NAME",
                "negotiated_dollar": "NEGOTIATED_RATE",
                "negotiated_percentage": "NEGOTIATED_PERCENT",
                "methodology": "RATE_METHODOLOGY",
                "min_charge": "MIN_NEGOTIATED_RATE",
                "max_charge": "MAX_NEGOTIATED_RATE",
                "additional_notes": "COMMENTS",
            },
            preprocessing=["skip_header_rows:1", "normalize_nulls", "trim_whitespace"],
            header_rows=1,
            encoding="utf-8",
            file_format="csv",
            delimiter="|",
            quote_char='"',
        )

        # Meditech Template
        templates["Meditech"] = MRFTemplate(
            name="Meditech",
            vendor="Meditech",
            version="6.1",
            description="Meditech Expanse price transparency format",
            column_mappings={
                "description": "CHARGE_DESCRIPTION",
                "code_1": "CDM_CODE",
                "code_1_type": "CODE_TYPE_1",
                "code_2": "ALT_CODE",
                "code_2_type": "CODE_TYPE_2",
                "setting": "PATIENT_CLASS",
                "gross_charge": "STANDARD_CHARGE",
                "discounted_cash": "CASH_DISCOUNT_PRICE",
                "payer_name": "FINANCIAL_CLASS",
                "plan_name": "PLAN_DESCRIPTION",
                "negotiated_dollar": "CONTRACTED_AMOUNT",
                "min_charge": "MIN_CHARGE",
                "max_charge": "MAX_CHARGE",
                "billing_class": "REVENUE_CODE",
            },
            preprocessing=[
                "skip_header_rows:3",
                "remove_footer_rows:1",
                "normalize_nulls",
                "trim_whitespace",
            ],
            header_rows=3,
            footer_rows=1,
            encoding="utf-8",
            file_format="csv",
        )

        # CMS JSON Template
        templates["CMS JSON"] = MRFTemplate(
            name="CMS JSON",
            vendor="CMS",
            version="1.0",
            description="CMS JSON machine-readable format (in-network and allowed amounts)",
            column_mappings={
                "description": "billing_code_description",
                "code_1": "billing_code",
                "code_1_type": "billing_code_type",
                "negotiated_dollar": "negotiated_rate",
                "payer_name": "negotiation_arrangement",
                "plan_name": "plan_name",
                "billing_class": "billing_class",
            },
            preprocessing=["flatten_json", "normalize_nulls"],
            file_format="json",
            encoding="utf-8",
            json_schema={
                "root": "in_network",
                "items": "negotiated_rates",
                "rate_path": "negotiated_prices.negotiated_rate",
                "plan_path": "provider_references",
            },
        )

        # Custom Template (placeholder for user-defined)
        templates["Custom"] = MRFTemplate(
            name="Custom",
            vendor="Custom",
            version="1.0",
            description="User-defined custom template",
            column_mappings={},
            preprocessing=["normalize_nulls"],
            header_rows=1,
            encoding="utf-8",
            file_format="csv",
        )

        return templates

    def _load_custom_templates(self):
        """Load custom templates from the templates directory."""
        if not self.templates_dir.exists():
            return

        for template_file in self.templates_dir.glob("*.json"):
            try:
                with open(template_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    template = MRFTemplate.from_dict(data)
                    self._custom_templates[template.name] = template
            except Exception as e:
                logger.error(f"Failed to load template {template_file}: {e}")

    def get_templates(self) -> List[MRFTemplate]:
        """Get all available templates (built-in and custom)."""
        all_templates = list(self._builtin_templates.values())
        all_templates.extend(self._custom_templates.values())
        return all_templates

    def get_template(self, name: str) -> Optional[MRFTemplate]:
        """Get a template by name."""
        if name in self._builtin_templates:
            return self._builtin_templates[name]
        return self._custom_templates.get(name)

    def detect_template(self, file_path: Path) -> Optional[MRFTemplate]:
        """
        Auto-detect the best matching template for a file.

        Analyzes file structure and column headers to match against known templates.
        Returns the template with the highest confidence score.

        Args:
            file_path: Path to the MRF file

        Returns:
            Best matching template or None if no good match found
        """
        file_path = Path(file_path)

        if not file_path.exists():
            return None

        # Determine file format
        file_format = self._detect_file_format(file_path)

        if file_format == "json":
            return self._detect_json_template(file_path)
        else:
            return self._detect_csv_template(file_path)

    def _detect_file_format(self, file_path: Path) -> str:
        """Detect if file is CSV, JSON, or other format."""
        ext = file_path.suffix.lower()

        if ext == ".json":
            return "json"
        elif ext in [".csv", ".txt"]:
            return "csv"
        elif ext in [".xlsx", ".xls"]:
            return "xlsx"
        else:
            # Try to detect from content
            try:
                with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                    first_char = f.read(1)
                    if first_char in ["{", "["]:
                        return "json"
            except (IOError, OSError, UnicodeDecodeError):
                pass
            return "csv"

    def _detect_json_template(self, file_path: Path) -> Optional[MRFTemplate]:
        """Detect template for JSON files."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Check for CMS JSON schema markers
            if "in_network" in data or "allowed_amounts" in data:
                return self.get_template("CMS JSON")

            return None
        except (IOError, OSError, json.JSONDecodeError):
            return None

    def _detect_csv_template(self, file_path: Path) -> Optional[MRFTemplate]:
        """
        Detect template for CSV files by analyzing column headers.

        Scores each template based on column name matches and returns best fit.
        """
        try:
            # Read first few lines with different encodings
            encodings = ["utf-8", "utf-16", "latin-1", "cp1252"]
            lines = None

            for encoding in encodings:
                try:
                    with open(file_path, "r", encoding=encoding, errors="replace") as f:
                        lines = [f.readline() for _ in range(10)]
                    break
                except (IOError, OSError, UnicodeDecodeError):
                    continue

            if not lines:
                return None

            # Try to find the header row
            header_candidates = []
            for i, line in enumerate(lines):
                if "," in line and not line.startswith("#"):
                    header_candidates.append((i, line))

            if not header_candidates:
                return None

            # Score each template against each header candidate
            best_template = None
            best_score = 0.0

            templates_to_check = [
                t
                for t in self.get_templates()
                if t.file_format == "csv" and t.name != "Custom"
            ]

            for row_num, header_line in header_candidates:
                # Parse header line
                delimiter = self._detect_delimiter(header_line)
                columns = self._parse_csv_line(header_line, delimiter)

                for template in templates_to_check:
                    score = self._score_template(template, columns, row_num)
                    if score > best_score:
                        best_score = score
                        best_template = template

            # Return template if confidence is above threshold
            if best_score > 0.3:
                return best_template

            return None

        except Exception as e:
            logger.error(f"Template detection error: {e}")
            return None

    def _detect_delimiter(self, line: str) -> str:
        """Detect CSV delimiter from header line."""
        delimiters = [",", "|", "\t", ";"]
        counts = {d: line.count(d) for d in delimiters}
        return max(counts, key=counts.get)

    def _parse_csv_line(self, line: str, delimiter: str = ",") -> List[str]:
        """Parse a CSV line respecting quotes."""
        result = []
        current = ""
        in_quotes = False

        for char in line:
            if char == '"':
                in_quotes = not in_quotes
            elif char == delimiter and not in_quotes:
                result.append(current.strip().strip('"'))
                current = ""
            else:
                current += char

        result.append(current.strip().strip('"'))
        return result

    def _score_template(
        self, template: MRFTemplate, columns: List[str], header_row: int
    ) -> float:
        """
        Score a template against detected columns.

        Returns a confidence score from 0.0 to 1.0.
        """
        if not columns or not template.column_mappings:
            return 0.0

        # Normalize column names for comparison
        normalized_columns = {col.lower().strip() for col in columns if col}

        matches = 0
        total_template_columns = len(template.column_mappings)

        # Check how many template columns are found
        for standard_field, vendor_column in template.column_mappings.items():
            if vendor_column.lower() in normalized_columns:
                matches += 1

        # Calculate base score
        if total_template_columns == 0:
            return 0.0

        base_score = matches / total_template_columns

        # Bonus for correct header row position
        if header_row == template.header_rows:
            base_score *= 1.2

        # Bonus for delimiter match
        if template.delimiter in [",", "|", "\t", ";"]:
            # This would need the actual line to check
            pass

        return min(base_score, 1.0)

    def save_template(self, template: MRFTemplate) -> bool:
        """
        Save a custom template to disk.

        Args:
            template: Template to save

        Returns:
            True if successful, False otherwise
        """
        try:
            # Don't save built-in templates
            if template.name in self._builtin_templates:
                return False

            template_path = self.templates_dir / f"{template.name}.json"

            with open(template_path, "w", encoding="utf-8") as f:
                json.dump(template.to_dict(), f, indent=2)

            self._custom_templates[template.name] = template
            return True

        except Exception as e:
            logger.error(f"Failed to save template: {e}")
            return False

    def delete_template(self, name: str) -> bool:
        """
        Delete a custom template.

        Args:
            name: Template name

        Returns:
            True if successful, False otherwise
        """
        # Don't delete built-in templates
        if name in self._builtin_templates:
            return False

        if name not in self._custom_templates:
            return False

        try:
            template_path = self.templates_dir / f"{name}.json"
            if template_path.exists():
                template_path.unlink()

            del self._custom_templates[name]
            return True

        except Exception as e:
            logger.error(f"Failed to delete template: {e}")
            return False

    def export_template(self, name: str, path: Path) -> bool:
        """
        Export a template to a JSON file.

        Args:
            name: Template name
            path: Destination file path

        Returns:
            True if successful, False otherwise
        """
        template = self.get_template(name)
        if not template:
            return False

        try:
            path = Path(path)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(template.to_dict(), f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to export template: {e}")
            return False

    def import_template(self, path: Path) -> Optional[MRFTemplate]:
        """
        Import a template from a JSON file.

        Args:
            path: Source file path

        Returns:
            Imported template or None if failed
        """
        try:
            path = Path(path)
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            template = MRFTemplate.from_dict(data)

            # Save to custom templates
            if self.save_template(template):
                return template

            return None

        except Exception as e:
            logger.error(f"Failed to import template: {e}")
            return None
