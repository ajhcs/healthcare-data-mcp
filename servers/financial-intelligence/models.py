"""Pydantic models for financial intelligence data — IRS 990, SEC EDGAR, municipal bonds."""

from pydantic import BaseModel, Field


class Form990Summary(BaseModel):
    """Summary of a nonprofit organization from ProPublica search."""

    ein: str = Field(description="Employer Identification Number")
    name: str = ""
    city: str = ""
    state: str = ""
    ntee_code: str = Field(default="", description="National Taxonomy of Exempt Entities code")
    total_revenue: float | None = None
    total_expenses: float | None = None
    net_assets: float | None = None
    tax_period: str = Field(default="", description="Tax period end date (YYYYMM)")


class Officer(BaseModel):
    """Officer/director compensation entry from Form 990."""

    name: str = ""
    title: str = ""
    compensation: float | None = None


class Form990Details(BaseModel):
    """Detailed Form 990 data parsed from IRS e-file XML."""

    ein: str = Field(description="Employer Identification Number")
    name: str = ""
    tax_period: str = ""
    contributions: float | None = None
    program_service_revenue: float | None = None
    investment_income: float | None = None
    other_revenue: float | None = None
    total_revenue: float | None = None
    total_expenses: float | None = None
    program_expenses: float | None = None
    management_expenses: float | None = None
    fundraising_expenses: float | None = None
    community_benefit_total: float | None = Field(default=None, description="Total community benefit expense (Schedule H)")
    community_benefit_pct: float | None = Field(default=None, description="Community benefit as % of total expenses")
    officers: list[Officer] = Field(default_factory=list)
    program_descriptions: list[str] = Field(default_factory=list)
    source: str = Field(default="", description="'xml' if parsed from IRS e-file, 'propublica' if summary only")


class SecFiling(BaseModel):
    """SEC filing summary from EDGAR full-text search."""

    accession_number: str = Field(description="EDGAR accession number (e.g. 0000320193-24-000058)")
    company_name: str = ""
    cik: str = ""
    form_type: str = ""
    filing_date: str = ""
    filing_url: str = ""


class SecFilingDetail(BaseModel):
    """Detailed SEC filing data from XBRL and HTML parsing."""

    accession_number: str = ""
    company_name: str = ""
    cik: str = ""
    form_type: str = ""
    filing_date: str = ""
    financials: dict | None = Field(default=None, description="XBRL financials: revenue, net_income, total_assets, equity")
    debt_summary: dict | None = Field(default=None, description="Debt metrics: long_term_debt, short_term_debt, debt_to_equity")
    mda_text: str | None = Field(default=None, description="MD&A narrative text (truncated to ~2000 chars)")
    risk_factors_text: str | None = Field(default=None, description="Risk factors text (truncated to ~2000 chars)")


class MuniBond(BaseModel):
    """Municipal bond offering from EDGAR Official Statement search."""

    accession_number: str = ""
    issuer_name: str = ""
    state: str = ""
    filing_date: str = ""
    filing_url: str = ""


class MuniBondDetails(BaseModel):
    """Municipal bond details from EDGAR filing index."""

    accession_number: str = ""
    issuer_name: str = ""
    filing_date: str = ""
    documents: list[dict] = Field(default_factory=list, description="[{name, url, type}]")
    description: str = ""
