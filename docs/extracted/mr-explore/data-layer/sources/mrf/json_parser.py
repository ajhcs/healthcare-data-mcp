"""
CMS JSON MRF Parser.
Handles the CMS JSON machine-readable file format for hospital price transparency.
"""

import json
import polars as pl
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class JSONParseResult:
    """Result of parsing a JSON MRF file."""
    success: bool
    data: Optional[pl.DataFrame] = None
    error_message: Optional[str] = None
    record_count: int = 0
    hospital_name: Optional[str] = None
    last_updated: Optional[str] = None


class CMSJSONParser:
    """
    Parser for CMS JSON machine-readable files.

    Handles two main sections:
    1. in_network: Negotiated rates with payers
    2. allowed_amounts: Allowed amounts for out-of-network
    """

    def __init__(self):
        self.hospital_name = None
        self.last_updated = None

    def parse_file(self, file_path: Path) -> JSONParseResult:
        """
        Parse a CMS JSON MRF file into a Polars DataFrame.

        Args:
            file_path: Path to JSON file

        Returns:
            JSONParseResult with parsed data
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract metadata
            self._extract_metadata(data)

            # Parse in-network rates
            in_network_records = []
            if "in_network" in data:
                in_network_records = self._parse_in_network(data["in_network"])

            # Parse allowed amounts
            allowed_amount_records = []
            if "allowed_amounts" in data:
                allowed_amount_records = self._parse_allowed_amounts(data["allowed_amounts"])

            # Combine records
            all_records = in_network_records + allowed_amount_records

            if not all_records:
                return JSONParseResult(
                    success=False,
                    error_message="No records found in JSON file"
                )

            # Convert to DataFrame
            df = pl.DataFrame(all_records)

            return JSONParseResult(
                success=True,
                data=df,
                record_count=len(df),
                hospital_name=self.hospital_name,
                last_updated=self.last_updated
            )

        except json.JSONDecodeError as e:
            return JSONParseResult(
                success=False,
                error_message=f"Invalid JSON: {str(e)}"
            )
        except Exception as e:
            return JSONParseResult(
                success=False,
                error_message=f"Parse error: {str(e)}"
            )

    def _extract_metadata(self, data: Dict[str, Any]):
        """Extract hospital metadata from JSON root."""
        self.hospital_name = data.get("hospital_name", "Unknown")
        self.last_updated = data.get("last_updated_on", "")

    def _parse_in_network(self, in_network_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse in-network negotiated rates section.

        Structure:
        {
            "negotiation_arrangement": "ffs",
            "name": "Service Name",
            "billing_code_type": "CPT",
            "billing_code": "12345",
            "description": "Procedure description",
            "negotiated_rates": [
                {
                    "provider_references": [1, 2, 3],
                    "negotiated_prices": [
                        {
                            "negotiated_type": "negotiated",
                            "negotiated_rate": 123.45,
                            "expiration_date": "2024-12-31",
                            "billing_class": "institutional"
                        }
                    ]
                }
            ]
        }
        """
        records = []

        for item in in_network_data:
            billing_code = item.get("billing_code", "")
            billing_code_type = item.get("billing_code_type", "")
            description = item.get("description", "")
            negotiation_arrangement = item.get("negotiation_arrangement", "")

            # Parse negotiated rates
            negotiated_rates = item.get("negotiated_rates", [])

            for rate_item in negotiated_rates:
                provider_refs = rate_item.get("provider_references", [])
                negotiated_prices = rate_item.get("negotiated_prices", [])

                for price in negotiated_prices:
                    record = {
                        "description": description,
                        "code_1": billing_code,
                        "code_1_type": billing_code_type,
                        "code_2": None,
                        "code_2_type": None,
                        "modifiers": None,
                        "setting": None,
                        "gross_charge": None,
                        "discounted_cash": None,
                        "payer_name": negotiation_arrangement,
                        "plan_name": self._format_provider_refs(provider_refs),
                        "negotiated_dollar": price.get("negotiated_rate"),
                        "negotiated_percentage": None,
                        "negotiated_algorithm": price.get("negotiated_type"),
                        "estimated_amount": None,
                        "methodology": price.get("service_code", [None])[0] if isinstance(price.get("service_code"), list) else None,
                        "min_charge": None,
                        "max_charge": None,
                        "additional_notes": None,
                        "billing_class": price.get("billing_class"),
                        "expiration_date": price.get("expiration_date"),
                    }
                    records.append(record)

        return records

    def _parse_allowed_amounts(self, allowed_amounts_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse allowed amounts for out-of-network section.

        Structure:
        {
            "billing_code_type": "CPT",
            "billing_code": "12345",
            "description": "Procedure description",
            "allowed_amounts": [
                {
                    "tin": {"type": "ein", "value": "12-3456789"},
                    "service_code": ["01"],
                    "billing_class": "professional",
                    "payments": [
                        {
                            "allowed_amount": 123.45,
                            "providers": [1, 2, 3]
                        }
                    ]
                }
            ]
        }
        """
        records = []

        for item in allowed_amounts_data:
            billing_code = item.get("billing_code", "")
            billing_code_type = item.get("billing_code_type", "")
            description = item.get("description", "")

            allowed_amounts = item.get("allowed_amounts", [])

            for allowed_item in allowed_amounts:
                tin = allowed_item.get("tin", {})
                service_codes = allowed_item.get("service_code", [])
                billing_class = allowed_item.get("billing_class")
                payments = allowed_item.get("payments", [])

                for payment in payments:
                    providers = payment.get("providers", [])

                    record = {
                        "description": description,
                        "code_1": billing_code,
                        "code_1_type": billing_code_type,
                        "code_2": None,
                        "code_2_type": None,
                        "modifiers": None,
                        "setting": None,
                        "gross_charge": None,
                        "discounted_cash": None,
                        "payer_name": "Out-of-Network",
                        "plan_name": self._format_tin(tin),
                        "negotiated_dollar": payment.get("allowed_amount"),
                        "negotiated_percentage": None,
                        "negotiated_algorithm": "allowed_amount",
                        "estimated_amount": None,
                        "methodology": ", ".join(service_codes) if service_codes else None,
                        "min_charge": None,
                        "max_charge": None,
                        "additional_notes": f"Providers: {self._format_provider_refs(providers)}",
                        "billing_class": billing_class,
                        "expiration_date": None,
                    }
                    records.append(record)

        return records

    def _format_provider_refs(self, refs: List[int]) -> str:
        """Format provider references into a readable string."""
        if not refs:
            return ""
        if len(refs) <= 3:
            return ", ".join(str(r) for r in refs)
        return f"{refs[0]}, {refs[1]}, ... (+{len(refs)-2} more)"

    def _format_tin(self, tin: Dict[str, str]) -> str:
        """Format TIN (Tax Identification Number) data."""
        if not tin:
            return ""
        tin_type = tin.get("type", "")
        tin_value = tin.get("value", "")
        return f"{tin_type.upper()}: {tin_value}" if tin_type and tin_value else tin_value


def flatten_json_to_tabular(data: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten nested JSON structure to tabular format.

    Args:
        data: Nested dictionary
        parent_key: Parent key for recursion
        sep: Separator for nested keys

    Returns:
        Flattened dictionary
    """
    items = []

    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            items.extend(flatten_json_to_tabular(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # Convert list to comma-separated string
            if value and isinstance(value[0], (str, int, float)):
                items.append((new_key, ", ".join(str(v) for v in value)))
            else:
                items.append((new_key, str(value)))
        else:
            items.append((new_key, value))

    return dict(items)
