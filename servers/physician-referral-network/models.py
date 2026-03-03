"""Pydantic models for physician & referral network server."""

from pydantic import BaseModel, Field


class PhysicianSummary(BaseModel):
    """A physician from search results."""

    npi: str = ""
    first_name: str = ""
    last_name: str = ""
    credential: str = ""
    specialty: str = ""
    city: str = ""
    state: str = ""
    org_name: str = ""
    gender: str = ""
    enumeration_date: str = ""


class PhysicianSearchResponse(BaseModel):
    """Response from search_physicians."""

    total_results: int = 0
    physicians: list[PhysicianSummary] = Field(default_factory=list)


class UtilizationSummary(BaseModel):
    """Medicare utilization summary for a physician."""

    total_services: int = 0
    total_beneficiaries: int = 0
    total_medicare_payment: float | None = None
    avg_allowed_amount: float | None = None
    avg_submitted_charge: float | None = None
    top_hcpcs: list[dict] = Field(default_factory=list, description="Top HCPCS codes by service volume")


class QualityInfo(BaseModel):
    """Quality and affiliation data from Physician Compare."""

    group_practice_pac_id: str = ""
    group_practice_name: str = ""
    hospital_affiliations: list[str] = Field(default_factory=list)
    graduation_year: str = ""
    medical_school: str = ""


class PhysicianProfile(BaseModel):
    """Full physician profile from get_physician_profile."""

    npi: str = ""
    first_name: str = ""
    last_name: str = ""
    credential: str = ""
    specialties: list[str] = Field(default_factory=list)
    practice_locations: list[dict] = Field(default_factory=list)
    org_affiliations: list[str] = Field(default_factory=list)
    gender: str = ""
    enumeration_date: str = ""
    utilization: UtilizationSummary | None = None
    quality: QualityInfo | None = None


class ReferralNode(BaseModel):
    """A node in a referral network graph."""

    npi: str = ""
    name: str = ""
    specialty: str = ""
    city: str = ""
    state: str = ""


class ReferralEdge(BaseModel):
    """An edge in a referral network graph."""

    npi_from: str = ""
    npi_to: str = ""
    shared_count: int = 0
    transaction_count: int = 0
    same_day_count: int = 0


class ReferralNetworkResponse(BaseModel):
    """Response from map_referral_network."""

    center_npi: str = ""
    center_name: str = ""
    nodes: list[ReferralNode] = Field(default_factory=list)
    edges: list[ReferralEdge] = Field(default_factory=list)
    total_connections: int = 0
    data_vintage: str = Field(default="2014-2020", description="DocGraph data years")


class LeakageDestination(BaseModel):
    """A single out-of-network referral destination."""

    npi: str = ""
    name: str = ""
    specialty: str = ""
    shared_count: int = 0
    city: str = ""
    state: str = ""
    classification: str = Field(default="", description="out_of_network_in_area or out_of_area")


class SpecialtyLeakage(BaseModel):
    """Leakage breakdown for one specialty."""

    specialty: str = ""
    total_referrals: int = 0
    in_network: int = 0
    out_of_network: int = 0
    leakage_pct: float = 0.0


class LeakageResponse(BaseModel):
    """Response from detect_leakage."""

    system_name: str = ""
    total_referrals: int = 0
    in_network_pct: float = 0.0
    out_of_network_in_area_pct: float = 0.0
    out_of_area_pct: float = 0.0
    top_leakage_destinations: list[LeakageDestination] = Field(default_factory=list)
    specialty_breakdown: list[SpecialtyLeakage] = Field(default_factory=list)
    data_vintage: str = "2014-2020"


class PhysicianClassification(BaseModel):
    """Employment classification for one physician."""

    npi: str = ""
    name: str = ""
    specialty: str = ""
    status: str = Field(default="", description="employed, affiliated, or independent")
    confidence: float = 0.0
    evidence: list[str] = Field(default_factory=list)


class PhysicianMixResponse(BaseModel):
    """Response from analyze_physician_mix."""

    system_name: str = ""
    total_physicians: int = 0
    employed: int = 0
    affiliated: int = 0
    independent: int = 0
    employed_pct: float = 0.0
    affiliated_pct: float = 0.0
    independent_pct: float = 0.0
    by_specialty: list[dict] = Field(default_factory=list)
    sample_physicians: list[PhysicianClassification] = Field(
        default_factory=list, description="Sample of classified physicians for verification"
    )
