"""Pydantic models for the Service Area MCP server."""

from pydantic import BaseModel, Field


class ZipDischargeRecord(BaseModel):
    """A single ZIP code's discharge volume for a facility."""

    zip_code: str
    discharges: int
    cumulative_pct: float = 0.0


class ServiceAreaResult(BaseModel):
    """Result of a PSA/SSA derivation for a facility."""

    facility_ccn: str
    facility_name: str = ""
    total_discharges: int
    psa_zips: list[str]
    psa_discharge_count: int
    psa_pct: float
    ssa_zips: list[str]
    ssa_discharge_count: int
    ssa_pct: float
    remaining_zips_count: int


class MarketShareEntry(BaseModel):
    """A hospital's share of discharges from a specific ZIP."""

    ccn: str
    facility_name: str
    discharges: int
    market_share_pct: float


class MarketShareResult(BaseModel):
    """Market share analysis for a ZIP code."""

    zip_code: str
    total_discharges: int
    hospitals: list[MarketShareEntry]


class HsaHrrMapping(BaseModel):
    """Dartmouth Atlas HSA/HRR assignment for a ZIP code."""

    zip_code: str
    hsa_number: int = 0
    hsa_city: str = ""
    hsa_state: str = ""
    hrr_number: int = 0
    hrr_city: str = ""
    hrr_state: str = ""


class DartmouthOverlap(BaseModel):
    """Overlap between a computed PSA and the Dartmouth HSA."""

    facility_ccn: str
    facility_name: str = ""
    facility_zip: str = ""
    hsa_number: int = 0
    hsa_city: str = ""
    hsa_state: str = ""
    psa_zip_count: int
    zips_in_hsa: int
    zips_outside_hsa: int
    overlap_pct: float = Field(description="Percentage of PSA ZIPs that fall within the same HSA")
    zips_only_in_psa: list[str] = Field(default_factory=list, description="PSA ZIPs not in the HSA")
    zips_only_in_hsa: list[str] = Field(default_factory=list, description="HSA ZIPs not in the PSA")
