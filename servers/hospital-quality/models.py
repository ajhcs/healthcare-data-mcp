"""Pydantic models for hospital quality, safety, readmission, and experience data."""

from pydantic import BaseModel, Field


class QualityScores(BaseModel):
    """Hospital overall quality ratings from CMS Hospital General Info."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    overall_rating: str = Field(default="", description="Overall hospital rating (1-5 stars)")
    mortality_national_comparison: str = ""
    safety_national_comparison: str = ""
    readmission_national_comparison: str = ""
    patient_experience_national_comparison: str = ""
    timeliness_national_comparison: str = ""


class ConditionReadmission(BaseModel):
    """Readmission data for a single condition."""

    measure: str = Field(description="Condition code (AMI, HF, PN, COPD, HIP_KNEE, CABG)")
    excess_readmission_ratio: float | None = None
    predicted_readmission_rate: float | None = None
    expected_readmission_rate: float | None = None
    number_of_discharges: int | None = None
    number_of_readmissions: int | None = None


class ReadmissionData(BaseModel):
    """HRRP readmission data for a hospital."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    conditions: list[ConditionReadmission] = Field(default_factory=list)
    payment_reduction_percentage: float | None = Field(
        default=None,
        description="Payment adjustment factor as reduction percentage (e.g. 0.5 means 0.5% reduction)",
    )


class DomainScores(BaseModel):
    """Individual HAC domain scores."""

    psi90: float | None = None
    clabsi: float | None = None
    cauti: float | None = None
    ssi_colon: float | None = None
    ssi_hyst: float | None = None
    mrsa: float | None = None
    cdi: float | None = None


class SafetyScores(BaseModel):
    """HAC Reduction Program safety scores for a hospital."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    total_hac_score: float | None = None
    payment_reduction: str = Field(default="", description="Yes or No")
    domain_scores: DomainScores = Field(default_factory=DomainScores)


class ExperienceDomain(BaseModel):
    """A single HCAHPS patient experience domain."""

    domain: str = ""
    star_rating: str = ""
    top_box_percent: str = Field(default="", description="Percent of patients giving most positive response")
    middle_box_percent: str = ""
    bottom_box_percent: str = ""


class PatientExperience(BaseModel):
    """HCAHPS patient experience survey results for a hospital."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    survey_response_rate: str = ""
    num_completed_surveys: str = ""
    domains: list[ExperienceDomain] = Field(default_factory=list)


class ComplicationRecord(BaseModel):
    """A single complication or death measure for a hospital."""

    measure_id: str = Field(default="", description="CMS measure identifier (e.g. PSI_90)")
    measure_name: str = ""
    compared_to_national: str = Field(
        default="", description="National comparison label (e.g. 'Better than the National Rate')"
    )
    denominator: int | None = Field(default=None, description="Number of cases/patients")
    score: float | None = Field(default=None, description="Observed rate or score")
    lower_estimate: float | None = None
    higher_estimate: float | None = None
    footnote: str = ""


class ComplicationsData(BaseModel):
    """Complications and Deaths data for a hospital (CMS dataset ynj2-r877)."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    measures: list[ComplicationRecord] = Field(default_factory=list)


class FinancialProfile(BaseModel):
    """Financial profile derived from CMS Cost Report or IPPS Impact File."""

    ccn: str = Field(description="CMS Certification Number")
    facility_name: str = ""
    case_mix_index: float | None = None
    total_discharges: int | None = None
    total_beds: int | None = None
    teaching_status: str = Field(default="", description="Resident-to-bed ratio classification")
    resident_to_bed_ratio: float | None = None
    dsh_pct: float | None = None
    wage_index: float | None = None
    geographic_location: str = Field(default="", description="Urban or Rural")
