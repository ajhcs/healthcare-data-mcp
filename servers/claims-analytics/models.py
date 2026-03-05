"""Pydantic models for claims & service line analytics server."""

from pydantic import BaseModel, Field


# --- Tool 1: get_inpatient_volumes ---

class DRGDetail(BaseModel):
    """Detail for a single DRG at a provider."""

    drg_code: str = ""
    drg_description: str = ""
    service_line: str = ""
    discharges: int = 0
    avg_charges: float = 0.0
    avg_total_payment: float = 0.0
    avg_medicare_payment: float = 0.0


class ServiceLineSummary(BaseModel):
    """Aggregated summary for one service line."""

    service_line: str = ""
    discharges: int = 0
    pct_of_total: float = 0.0
    avg_charges: float = 0.0
    avg_medicare_payment: float = 0.0


class InpatientVolumesResponse(BaseModel):
    """Response from get_inpatient_volumes."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    year: str = ""
    total_discharges: int = 0
    total_drgs: int = 0
    service_line_summary: list[ServiceLineSummary] = Field(default_factory=list)
    drg_details: list[DRGDetail] = Field(default_factory=list)


# --- Tool 2: get_outpatient_volumes ---

class APCDetail(BaseModel):
    """Detail for a single APC at a provider."""

    apc_code: str = ""
    apc_description: str = ""
    services: int = 0
    avg_charges: float = 0.0
    avg_total_payment: float = 0.0
    avg_medicare_payment: float = 0.0


class OutpatientVolumesResponse(BaseModel):
    """Response from get_outpatient_volumes."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    year: str = ""
    total_services: int = 0
    total_apcs: int = 0
    apc_details: list[APCDetail] = Field(default_factory=list)


# --- Tool 3: trend_service_lines ---

class ServiceLineTrend(BaseModel):
    """Multi-year trend for one inpatient service line."""

    service_line: str = ""
    volumes_by_year: dict[str, int] = Field(default_factory=dict)
    yoy_change_pct: dict[str, float] = Field(default_factory=dict)
    cagr_pct: float = 0.0


class OutpatientTrend(BaseModel):
    """Multi-year trend for one outpatient APC."""

    apc_code: str = ""
    apc_description: str = ""
    volumes_by_year: dict[str, int] = Field(default_factory=dict)
    yoy_change_pct: dict[str, float] = Field(default_factory=dict)
    cagr_pct: float = 0.0


class ServiceLineTrendResponse(BaseModel):
    """Response from trend_service_lines."""

    ccn: str = ""
    provider_name: str = ""
    years: list[str] = Field(default_factory=list)
    inpatient_trends: list[ServiceLineTrend] = Field(default_factory=list)
    outpatient_trends: list[OutpatientTrend] | None = None


# --- Tool 4: compute_case_mix ---

class ServiceLineAcuity(BaseModel):
    """Acuity metrics for one service line."""

    service_line: str = ""
    discharges: int = 0
    avg_drg_weight: float = 0.0
    pct_of_total_weight: float = 0.0


class DRGWeightContribution(BaseModel):
    """A DRG's contribution to total case mix weight."""

    drg_code: str = ""
    drg_description: str = ""
    service_line: str = ""
    discharges: int = 0
    drg_weight: float = 0.0
    total_weight_contribution: float = 0.0
    pct_of_total_weight: float = 0.0


class CaseMixResponse(BaseModel):
    """Response from compute_case_mix."""

    ccn: str = ""
    provider_name: str = ""
    year: str = ""
    case_mix_index: float = 0.0
    total_discharges: int = 0
    service_line_acuity: list[ServiceLineAcuity] = Field(default_factory=list)
    top_drgs_by_weight: list[DRGWeightContribution] = Field(default_factory=list)


# --- Tool 5: analyze_market_volumes ---

class ServiceLineShare(BaseModel):
    """Service line breakdown for a provider in market context."""

    service_line: str = ""
    discharges: int = 0
    market_share_pct: float = 0.0


class ProviderMarketShare(BaseModel):
    """One provider's market share within the defined geography."""

    ccn: str = ""
    provider_name: str = ""
    state: str = ""
    total_discharges: int = 0
    market_share_pct: float = 0.0
    service_line_breakdown: list[ServiceLineShare] = Field(default_factory=list)


class ServiceLineMarketTotal(BaseModel):
    """Total market volume for one service line."""

    service_line: str = ""
    total_discharges: int = 0
    pct_of_market: float = 0.0
    top_provider_ccn: str = ""
    top_provider_name: str = ""


class MarketVolumesResponse(BaseModel):
    """Response from analyze_market_volumes."""

    year: str = ""
    total_market_discharges: int = 0
    total_providers: int = 0
    provider_shares: list[ProviderMarketShare] = Field(default_factory=list)
    service_line_totals: list[ServiceLineMarketTotal] = Field(default_factory=list)
