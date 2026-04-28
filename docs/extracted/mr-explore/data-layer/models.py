"""
Data models for Medical Charges Explorer.
Defines schema and type annotations for hospital charge data.
"""

from dataclasses import dataclass
from typing import Optional
from enum import Enum


class Setting(Enum):
    """Care setting for charge."""

    INPATIENT = "inpatient"
    OUTPATIENT = "outpatient"
    BOTH = "both"


class BillingClass(Enum):
    """Billing classification."""

    FACILITY = "facility"
    PROFESSIONAL = "professional"
    BOTH = "both"


class CodeType(Enum):
    """Type of medical code."""

    CPT = "CPT"
    CDM = "CDM"
    HCPCS = "HCPCS"
    DRG = "DRG"
    MS_DRG = "MS-DRG"
    APC = "APC"
    NDC = "NDC"
    ICD = "ICD"
    UNKNOWN = "UNKNOWN"


class Methodology(Enum):
    """Pricing methodology."""

    FEE_SCHEDULE = "fee schedule"
    PERCENT_OF_BILLED = "percent of total billed charges"
    PER_DIEM = "per diem"
    CASE_RATE = "case rate"
    OTHER = "other"


class AssetCategory(Enum):
    """Asset categories derived from methodology analysis."""

    # Inpatient categories
    INPATIENT_ROOM_BOARD = "Inpatient Room & Board"
    INPATIENT_DRG_BUNDLE = "Inpatient DRG Bundle"
    INPATIENT_SURGICAL = "Inpatient Surgical"
    INPATIENT_MEDICAL = "Inpatient Medical"
    INPATIENT_PHARMACY = "Inpatient Pharmacy"
    INPATIENT_THERAPY = "Inpatient Therapy"

    # Outpatient categories
    OUTPATIENT_PROFESSIONAL = "Outpatient Professional"
    OUTPATIENT_DIAGNOSTIC = "Outpatient Diagnostic"
    OUTPATIENT_THERAPEUTIC = "Outpatient Therapeutic"
    OUTPATIENT_SURGICAL = "Outpatient Surgical"
    OUTPATIENT_EMERGENCY = "Outpatient Emergency"
    OUTPATIENT_ADMINISTRATIVE = "Outpatient Administrative"

    # Pharmacy/Medication
    PHARMACY_DRUG = "Pharmacy - Drug"
    PHARMACY_SUPPLY = "Pharmacy - Supply"

    # Ancillary services
    ANCILLARY_LAB = "Ancillary - Laboratory"
    ANCILLARY_RADIOLOGY = "Ancillary - Radiology"
    ANCILLARY_THERAPY = "Ancillary - Therapy"
    ANCILLARY_PATHOLOGY = "Ancillary - Pathology"

    # Equipment/Supplies
    EQUIPMENT_DURABLE = "Equipment - Durable"
    EQUIPMENT_IMPLANT = "Equipment - Implant"
    SUPPLY_GENERAL = "Supply - General"
    SUPPLY_OPERATING_ROOM = "Supply - Operating Room"

    # Uncategorized
    UNCLASSIFIED = "Unclassified"


@dataclass
class ChargeRecord:
    """
    Represents a single charge record from hospital price transparency file.
    """

    # Core identifiers
    id: int
    description: str

    # Codes
    code_1: Optional[str] = None
    code_1_type: Optional[str] = None
    code_2: Optional[str] = None
    code_2_type: Optional[str] = None
    modifiers: Optional[str] = None

    # Setting and classification
    setting: Optional[str] = None
    billing_class: Optional[str] = None

    # Drug information
    drug_unit_of_measurement: Optional[str] = None
    drug_type_of_measurement: Optional[str] = None

    # Standard charges
    gross_charge: Optional[float] = None
    discounted_cash: Optional[float] = None

    # Payer/Plan information
    payer_name: Optional[str] = None
    plan_name: Optional[str] = None

    # Negotiated rates
    negotiated_dollar: Optional[float] = None
    negotiated_percentage: Optional[float] = None
    negotiated_algorithm: Optional[str] = None
    estimated_amount: Optional[float] = None
    methodology: Optional[str] = None

    # Min/Max
    min_charge: Optional[float] = None
    max_charge: Optional[float] = None

    # Additional info
    additional_notes: Optional[str] = None


@dataclass
class HospitalInfo:
    """
    Hospital metadata from file header.
    """

    name: str
    location: str
    address: str
    last_updated: str
    version: str
    license_number: Optional[str] = None


# Column name mappings from CMS format
CMS_COLUMN_MAPPING = {
    "Description": "description",
    "Code|1": "code_1",
    "Code|1|Type": "code_1_type",
    "Code|2": "code_2",
    "Code|2|Type": "code_2_type",
    "Modifiers": "modifiers",
    "Setting": "setting",
    "Drug_Unit_Of_Measurement": "drug_unit_of_measurement",
    "Drug_Type_Of_Measurement": "drug_type_of_measurement",
    "Standard_Charge|Gross": "gross_charge",
    "Standard_Charge|Discounted_Cash": "discounted_cash",
    "Payer_Name": "payer_name",
    "Plan_Name": "plan_name",
    "Standard_Charge|Negotiated_Dollar": "negotiated_dollar",
    "Standard_Charge|Negotiated_Percentage": "negotiated_percentage",
    "Standard_Charge|Negotiated_Algorithm": "negotiated_algorithm",
    "Estimated_Amount": "estimated_amount",
    "Standard_Charge|Methodology": "methodology",
    "Standard_Charge|Min": "min_charge",
    "Standard_Charge|Max": "max_charge",
    "Additional_Generic_Notes": "additional_notes",
    "Billing_Class": "billing_class",
}
