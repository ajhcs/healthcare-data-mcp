"""Pydantic models for price transparency / MRF engine."""

from pydantic import BaseModel, Field


class MRFLocation(BaseModel):
    """A single MRF file location for a hospital."""

    url: str = Field(description="URL to the MRF file (CSV or JSON)")
    format: str = Field(default="csv", description="File format: 'csv' or 'json'")
    last_verified: str = ""


class MRFIndexResult(BaseModel):
    """Result from search_mrf_index — hospital info + MRF URLs."""

    hospital_name: str = ""
    ccn: str = Field(default="", description="CMS Certification Number")
    ein: str = Field(default="", description="Employer Identification Number")
    city: str = ""
    state: str = ""
    mrf_urls: list[MRFLocation] = Field(default_factory=list)
    cached: bool = Field(default=False, description="Whether Parquet index exists for this hospital")
    cache_date: str = Field(default="", description="Date the Parquet index was built")
    row_count: int | None = Field(default=None, description="Number of charge records in cache")


class NegotiatedRate(BaseModel):
    """A single negotiated rate for a CPT code from a payer/plan."""

    cpt_code: str = ""
    description: str = ""
    payer_name: str = ""
    plan_name: str = ""
    negotiated_dollar: float | None = None
    negotiated_percentage: float | None = None
    methodology: str = ""
    setting: str = Field(default="", description="inpatient, outpatient, or both")
    billing_class: str = ""
    gross_charge: float | None = None
    min_charge: float | None = None
    max_charge: float | None = None


class NegotiatedRatesResponse(BaseModel):
    """Response from get_negotiated_rates."""

    hospital_name: str = ""
    hospital_id: str = ""
    cpt_codes_requested: list[str] = Field(default_factory=list)
    rates: list[NegotiatedRate] = Field(default_factory=list)
    total_rates: int = 0
    source: str = Field(default="", description="'parquet_cache' or 'live_download'")


class RateDispersion(BaseModel):
    """Rate dispersion statistics for a single CPT code across payers."""

    cpt_code: str = ""
    description: str = ""
    payer_count: int = 0
    min_rate: float | None = None
    max_rate: float | None = None
    median_rate: float | None = None
    mean_rate: float | None = None
    iqr: float | None = Field(default=None, description="Interquartile range (Q3 - Q1)")
    q25: float | None = None
    q75: float | None = None
    cv: float | None = Field(default=None, description="Coefficient of variation (std/mean)")
    std_dev: float | None = None


class HospitalRateComparison(BaseModel):
    """Rates for one hospital within a system comparison."""

    hospital_name: str = ""
    hospital_id: str = ""
    rates: list[NegotiatedRate] = Field(default_factory=list)


class SystemComparisonResponse(BaseModel):
    """Response from compare_rates_system."""

    system_name: str = ""
    cpt_codes: list[str] = Field(default_factory=list)
    hospitals: list[HospitalRateComparison] = Field(default_factory=list)


class BenchmarkComparison(BaseModel):
    """Benchmark data for a single CPT code."""

    cpt_code: str = ""
    description: str = ""
    hospital_median_rate: float | None = None
    medicare_allowed_amount: float | None = Field(default=None, description="PFS-calculated Medicare allowed amount")
    pct_of_medicare: float | None = Field(default=None, description="Hospital rate as % of Medicare")
    medicare_actual_avg_payment: float | None = Field(default=None, description="From CMS utilization data")
    peer_percentile: float | None = Field(default=None, description="Where this rate falls among cached peers (0-100)")
    peer_25th: float | None = None
    peer_50th: float | None = None
    peer_75th: float | None = None
    peer_90th: float | None = None
    peer_hospital_count: int = Field(default=0, description="Number of peer hospitals with data for this code")


class BenchmarkResponse(BaseModel):
    """Response from benchmark_rates."""

    hospital_name: str = ""
    hospital_id: str = ""
    locality: str = Field(default="", description="Medicare GPCI locality used")
    benchmarks: list[BenchmarkComparison] = Field(default_factory=list)
