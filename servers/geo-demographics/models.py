"""Pydantic models for the Geographic Demographics MCP server."""

from pydantic import BaseModel, Field


class AgeDistribution(BaseModel):
    """Age breakdown for a ZCTA."""

    under_18: int = 0
    age_18_to_64: int = 0
    age_65_plus: int = 0


class InsuranceCoverage(BaseModel):
    """Health insurance coverage estimates for a ZCTA."""

    private: int = 0
    public_medicare: int = 0
    public_medicaid: int = 0
    uninsured: int = 0
    uninsured_pct: float = 0.0


class RaceEthnicity(BaseModel):
    """ACS race and Hispanic/Latino origin estimates for a ZCTA."""

    white_alone: int = 0
    black_alone: int = 0
    american_indian_alaska_native_alone: int = 0
    asian_alone: int = 0
    native_hawaiian_pacific_islander_alone: int = 0
    some_other_race_alone: int = 0
    two_or_more_races: int = 0
    hispanic_latino: int = 0
    not_hispanic_latino: int = 0


class LandArea(BaseModel):
    """ZCTA land area input used for density calculations."""

    land_area_square_meters: float | None = None
    land_area_square_miles: float | None = None
    source_dataset_id: str = ""
    source_period: str = ""


class PopulationDensity(BaseModel):
    """Population density calculation inputs for a ZCTA."""

    people_per_square_mile: float | None = None
    population_input: int = 0
    land_area_input_square_miles: float | None = None
    source_dataset_id: str = ""


class ZctaDemographics(BaseModel):
    """Census ACS demographic profile for a single ZCTA."""

    zcta: str
    year: int
    total_population: int = 0
    median_age: float | None = None
    male_population: int = 0
    female_population: int = 0
    age_distribution: AgeDistribution = Field(default_factory=AgeDistribution)
    race_ethnicity: RaceEthnicity = Field(default_factory=RaceEthnicity)
    land_area: LandArea = Field(default_factory=LandArea)
    population_density: PopulationDensity = Field(default_factory=PopulationDensity)
    median_household_income: int | None = None
    insurance: InsuranceCoverage = Field(default_factory=InsuranceCoverage)


class ZctaAdjacency(BaseModel):
    """Adjacency result for a ZCTA."""

    zcta: str
    adjacent_zctas: list[str] = Field(default_factory=list)
    count: int = 0


class MedicareEnrollment(BaseModel):
    """Medicare enrollment summary for a geography."""

    geography_type: str = ""
    geography_code: str = ""
    geography_name: str = ""
    total_beneficiaries: int | None = None
    ma_penetration_pct: float | None = None
    avg_age: float | None = None
    pct_female: float | None = None
    pct_dual_eligible: float | None = None
    pct_a_b_coverage: float | None = None
    per_capita_spending: float | None = None


class GeographicVariation(BaseModel):
    """CMS Geographic Variation PUF data for a county or HRR."""

    geography_type: str = ""
    geography_code: str = ""
    geography_name: str = ""
    total_beneficiaries: int | None = None
    avg_age: float | None = None
    pct_female: float | None = None
    pct_dual_eligible: float | None = None
    per_capita_spending: float | None = None
    ip_spending_per_capita: float | None = None
    op_spending_per_capita: float | None = None
    physician_spending_per_capita: float | None = None
    snf_spending_per_capita: float | None = None
    discharges_per_1000: float | None = None
    er_visits_per_1000: float | None = None
    readmission_rate: float | None = None


class CrosswalkResult(BaseModel):
    """A single crosswalk mapping from ZIP to another geography."""

    zip_code: str
    target_type: str = ""
    target_code: str = ""
    residential_ratio: float | None = None
    business_ratio: float | None = None
    other_ratio: float | None = None
    total_ratio: float | None = None


class CrosswalkResponse(BaseModel):
    """Full crosswalk response for a ZIP code."""

    zip_code: str
    target_type: str = ""
    results: list[CrosswalkResult] = Field(default_factory=list)
