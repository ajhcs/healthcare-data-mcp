# Financial Intelligence Server Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a FastMCP server exposing 6 tools for IRS Form 990 nonprofit financials, SEC EDGAR corporate filings, and municipal bond data.

**Architecture:** Single server (`financial-intelligence`, port 8008) with three data modules: `propublica_client.py` (ProPublica REST API), `irs990_parser.py` (IRS 990 XML download + parse), `edgar_client.py` (EDGAR EFTS search + XBRL companyfacts + HTML section extraction). All APIs are public/no-auth except EDGAR which requires a `User-Agent` header.

**Tech Stack:** FastMCP, httpx, pydantic, xml.etree.ElementTree, re (for HTML section extraction)

**Design doc:** `docs/plans/2026-03-03-financial-intelligence-design.md`

---

### Task 1: Scaffold directory and Pydantic models

**Files:**
- Create: `servers/financial-intelligence/__init__.py`
- Create: `servers/financial-intelligence/models.py`

**Step 1: Create directory and empty init**

```bash
mkdir -p "servers/financial-intelligence"
```

Write `servers/financial-intelligence/__init__.py`:

```python
```

(Empty file — just marks directory as a Python package.)

**Step 2: Write models.py with all Pydantic response models**

Write `servers/financial-intelligence/models.py`:

```python
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
    # Revenue breakdown
    contributions: float | None = None
    program_service_revenue: float | None = None
    investment_income: float | None = None
    other_revenue: float | None = None
    total_revenue: float | None = None
    # Expenses (Part IX functional)
    total_expenses: float | None = None
    program_expenses: float | None = None
    management_expenses: float | None = None
    fundraising_expenses: float | None = None
    # Schedule H (hospitals)
    community_benefit_total: float | None = Field(default=None, description="Total community benefit expense (Schedule H)")
    community_benefit_pct: float | None = Field(default=None, description="Community benefit as % of total expenses")
    # Compensation
    officers: list[Officer] = Field(default_factory=list)
    # Program descriptions
    program_descriptions: list[str] = Field(default_factory=list)
    # Source indicator
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
```

**Step 3: Create symlink for Python import compatibility**

```bash
cd servers && ln -sf financial-intelligence financial_intelligence && cd ..
```

**Step 4: Verify import works**

Run: `python -c "from servers.financial_intelligence.models import Form990Summary; print('OK')"`
Expected: `OK`

**Step 5: Commit**

```bash
git add servers/financial-intelligence/__init__.py servers/financial-intelligence/models.py servers/financial_intelligence
git commit -m "feat(financial-intelligence): scaffold directory and Pydantic models"
```

---

### Task 2: ProPublica client — search_form990

**Files:**
- Create: `servers/financial-intelligence/propublica_client.py`
- Test: Manual integration test via Python REPL

**Step 1: Write propublica_client.py**

Write `servers/financial-intelligence/propublica_client.py`:

```python
"""ProPublica Nonprofit Explorer API client."""

import logging

import httpx

logger = logging.getLogger(__name__)

PROPUBLICA_BASE = "https://projects.propublica.org/nonprofits/api/v2"


async def search_organizations(query: str, state: str = "", ntee_code: str = "", page: int = 0) -> dict:
    """Search nonprofits via ProPublica Nonprofit Explorer.

    Returns raw JSON response with 'organizations' list and 'total_results' count.
    """
    params: dict[str, str | int] = {"q": query, "page": page}
    if state:
        params["state[id]"] = state.upper()
    if ntee_code:
        params["ntee[id]"] = ntee_code

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{PROPUBLICA_BASE}/search.json", params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("ProPublica search failed: %s", e)
        return {"organizations": [], "total_results": 0}


async def get_organization(ein: str) -> dict:
    """Get organization details and filing list from ProPublica.

    Returns raw JSON with 'organization' dict and 'filings_with_data' list.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{PROPUBLICA_BASE}/organizations/{ein}.json")
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("ProPublica org lookup failed for EIN %s: %s", ein, e)
        return {}
```

**Step 2: Verify ProPublica API responds**

Run: `python -c "
import asyncio, json
from servers.financial_intelligence.propublica_client import search_organizations
result = asyncio.run(search_organizations('cleveland clinic'))
print(json.dumps({'total': result.get('total_results', 0), 'first': result.get('organizations', [{}])[0].get('name', 'N/A')}, indent=2))
"`

Expected: JSON showing total results > 0, first org name containing "Cleveland Clinic".

**Step 3: Commit**

```bash
git add servers/financial-intelligence/propublica_client.py
git commit -m "feat(financial-intelligence): add ProPublica Nonprofit Explorer client"
```

---

### Task 3: IRS 990 XML parser — get_form990_details

**Files:**
- Create: `servers/financial-intelligence/irs990_parser.py`

**Step 1: Write irs990_parser.py**

Write `servers/financial-intelligence/irs990_parser.py`:

```python
"""IRS Form 990 e-file XML parser.

Downloads 990 XML from IRS e-file URLs (provided by ProPublica) and extracts:
- Revenue breakdown (Part VIII)
- Functional expenses (Part IX)
- Schedule H community benefit (hospitals)
- Officer/director compensation (Part VII)
- Program service descriptions (Part III)
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx

from shared.utils.cms_client import DATA_DIR, get_cache_path

logger = logging.getLogger(__name__)

# IRS 990 XML namespaces vary by tax year; we search without namespace prefix
# by using a local-name() XPath pattern or by stripping namespaces after parse.


def _strip_ns(tree: ET.Element) -> ET.Element:
    """Remove XML namespace prefixes from all tags for easier searching."""
    for el in tree.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]
    return tree


def _find_text(root: ET.Element, *tags: str) -> str:
    """Find the first matching tag's text content."""
    for tag in tags:
        el = root.find(f".//{tag}")
        if el is not None and el.text:
            return el.text.strip()
    return ""


def _find_float(root: ET.Element, *tags: str) -> float | None:
    """Find the first matching tag's text and parse as float."""
    text = _find_text(root, *tags)
    if not text:
        return None
    try:
        return float(text.replace(",", ""))
    except (ValueError, TypeError):
        return None


async def download_990_xml(xml_url: str, ein: str, tax_period: str) -> Path | None:
    """Download a 990 XML file, caching locally.

    Returns local file path, or None if download fails.
    """
    cache_key = f"irs990_{ein}_{tax_period}"
    cached = get_cache_path(cache_key, suffix=".xml")
    if cached.exists():
        return cached

    try:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(xml_url)
            resp.raise_for_status()
            cached.write_bytes(resp.content)
            logger.info("Cached 990 XML for EIN %s period %s", ein, tax_period)
            return cached
    except Exception as e:
        logger.warning("Failed to download 990 XML from %s: %s", xml_url, e)
        return None


def parse_990_xml(xml_path: Path) -> dict:
    """Parse a 990 XML file and extract structured financial data.

    Returns a dict with keys matching Form990Details model fields.
    """
    tree = ET.parse(xml_path)
    root = _strip_ns(tree.getroot())

    # Find the Return/ReturnData element (990 schema root)
    return_data = root.find(".//ReturnData")
    if return_data is None:
        return_data = root

    # Find the main 990 form element
    form = return_data.find(".//IRS990")
    if form is None:
        form = return_data

    result: dict = {}

    # --- Revenue (Part VIII) ---
    result["contributions"] = _find_float(
        form, "CYContributionsGrantsAmt", "ContributionsGrantsCurrentYear",
        "CYContributionsGrantsAmt", "TotalContributionsAmt",
    )
    result["program_service_revenue"] = _find_float(
        form, "CYProgramServiceRevenueAmt", "ProgramServiceRevCurrentYear",
        "ProgramServiceRevenueAmt",
    )
    result["investment_income"] = _find_float(
        form, "CYInvestmentIncomeAmt", "InvestmentIncomeCurrentYear",
        "InvestmentIncomeAmt",
    )
    result["other_revenue"] = _find_float(
        form, "CYOtherRevenueAmt", "OtherRevenueCurrentYear",
        "OtherRevenueAmt",
    )
    result["total_revenue"] = _find_float(
        form, "CYTotalRevenueAmt", "TotalRevenueCurrentYear",
        "TotalRevenueAmt",
    )

    # --- Expenses (Part IX functional) ---
    result["total_expenses"] = _find_float(
        form, "CYTotalExpensesAmt", "TotalFunctionalExpensesAmt",
        "TotalExpensesCurrentYear",
    )
    result["program_expenses"] = _find_float(
        form, "TotalProgramServiceExpensesAmt", "ProgramServicesAmt",
    )
    result["management_expenses"] = _find_float(
        form, "ManagementAndGeneralAmt", "ManagementAndGeneral",
    )
    result["fundraising_expenses"] = _find_float(
        form, "FundraisingAmt", "Fundraising", "FundraisingExpensesAmt",
    )

    # --- Schedule H (hospitals) ---
    sched_h = return_data.find(".//IRS990ScheduleH")
    if sched_h is not None:
        result["community_benefit_total"] = _find_float(
            sched_h, "TotalCommunityBenefitExpnsAmt", "TotalCommunityBenefitsAmt",
            "CommunityBenefitTotalAmt",
        )
        total_exp = result.get("total_expenses")
        cb = result.get("community_benefit_total")
        if cb is not None and total_exp and total_exp > 0:
            result["community_benefit_pct"] = round(cb / total_exp * 100, 2)

    # --- Officer compensation (Part VII) ---
    officers = []
    for comp_el in form.findall(".//Form990PartVIISectionAListGrp"):
        name = _find_text(comp_el, "PersonNm", "BusinessName", "NamePerson")
        title = _find_text(comp_el, "TitleTxt", "Title")
        comp = _find_float(comp_el, "ReportableCompFromOrgAmt", "Compensation", "TotalCompensationAmt")
        if name:
            officers.append({"name": name, "title": title, "compensation": comp})
    # Fallback: older schema uses different grouping
    if not officers:
        for comp_el in form.findall(".//CompensationOfHghstPdEmplGrp"):
            name = _find_text(comp_el, "PersonNm", "BusinessName")
            title = _find_text(comp_el, "TitleTxt", "Title")
            comp = _find_float(comp_el, "CompensationAmt", "Compensation")
            if name:
                officers.append({"name": name, "title": title, "compensation": comp})

    result["officers"] = officers

    # --- Program descriptions (Part III) ---
    descriptions = []
    for prog_el in form.findall(".//ProgSrvcAccomActy2Grp"):
        desc = _find_text(prog_el, "Desc", "DescriptionProgramSrvcAccomTxt", "ActivityOrMissionDesc")
        if desc:
            descriptions.append(desc[:500])  # Truncate long descriptions
    if not descriptions:
        for prog_el in form.findall(".//ProgSrvcAccomActyOtherGrp"):
            desc = _find_text(prog_el, "Desc", "DescriptionProgramSrvcAccomTxt")
            if desc:
                descriptions.append(desc[:500])
    # Try top-level mission/activity description
    if not descriptions:
        mission = _find_text(form, "ActivityOrMissionDesc", "MissionDesc", "Description")
        if mission:
            descriptions.append(mission[:500])

    result["program_descriptions"] = descriptions

    return result
```

**Step 2: Verify XML parser with a test download**

Run: `python -c "
import asyncio, json
from servers.financial_intelligence.propublica_client import get_organization
from servers.financial_intelligence.irs990_parser import download_990_xml, parse_990_xml

async def test():
    org = await get_organization('341323166')  # Cleveland Clinic EIN
    filings = org.get('filings_with_data', [])
    if not filings:
        print('No filings found')
        return
    f = filings[0]
    xml_url = f.get('xml_url', '')
    if not xml_url:
        print('No XML URL in filing')
        return
    path = await download_990_xml(xml_url, '341323166', str(f.get('tax_prd', '')))
    if not path:
        print('Download failed')
        return
    result = parse_990_xml(path)
    print(json.dumps({k: v for k, v in result.items() if k != 'officers'}, indent=2, default=str))
    print(f'Officers found: {len(result.get(\"officers\", []))}')

asyncio.run(test())
"`

Expected: JSON with revenue/expense fields populated, officers count > 0.

**Step 3: Commit**

```bash
git add servers/financial-intelligence/irs990_parser.py
git commit -m "feat(financial-intelligence): add IRS 990 XML parser"
```

---

### Task 4: EDGAR client — search + XBRL + HTML parsing

**Files:**
- Create: `servers/financial-intelligence/edgar_client.py`

**Step 1: Write edgar_client.py**

Write `servers/financial-intelligence/edgar_client.py`:

```python
"""SEC EDGAR API client — EFTS search, XBRL companyfacts, and HTML section extraction."""

import json
import logging
import os
import re
import time
from pathlib import Path

import httpx

from shared.utils.cms_client import DATA_DIR, get_cache_path

logger = logging.getLogger(__name__)

SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "healthcare-data-mcp support@example.com")
EFTS_BASE = "https://efts.sec.gov/LATEST/search-index"
XBRL_BASE = "https://data.sec.gov/api/xbrl/companyfacts"
SUBMISSIONS_BASE = "https://data.sec.gov/submissions"
ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

# Rate limit: 10 req/sec for SEC APIs
_last_request_time: float = 0.0


def _headers() -> dict[str, str]:
    return {"User-Agent": SEC_USER_AGENT, "Accept": "application/json"}


async def _rate_limited_get(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
    """GET with rate limiting for SEC fair access policy."""
    global _last_request_time
    elapsed = time.monotonic() - _last_request_time
    if elapsed < 0.1:  # 10 req/sec max
        import asyncio
        await asyncio.sleep(0.1 - elapsed)
    _last_request_time = time.monotonic()
    return await client.get(url, headers=_headers(), **kwargs)


# ---------------------------------------------------------------------------
# EFTS Full-Text Search
# ---------------------------------------------------------------------------

async def search_filings(
    query: str,
    forms: str = "10-K",
    date_from: str = "",
    date_to: str = "",
) -> dict:
    """Search EDGAR filings via EFTS full-text search.

    Returns dict with 'hits' containing filing results.
    """
    params: dict[str, str] = {"q": query, "forms": forms}
    if date_from or date_to:
        params["dateRange"] = "custom"
        if date_from:
            params["startdt"] = date_from
        if date_to:
            params["enddt"] = date_to

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _rate_limited_get(client, EFTS_BASE, params=params)
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.warning("EDGAR EFTS search failed: %s", e)
        return {"hits": {"hits": [], "total": {"value": 0}}}


# ---------------------------------------------------------------------------
# Company Submissions (for CIK lookup from accession number)
# ---------------------------------------------------------------------------

async def get_company_submissions(cik: str) -> dict:
    """Get company submission history from EDGAR.

    CIK is zero-padded to 10 digits.
    """
    padded_cik = cik.zfill(10)
    cache_key = f"edgar_submissions_{padded_cik}"
    cached = get_cache_path(cache_key, suffix=".json")

    # Cache for 24 hours
    if cached.exists():
        age_hours = (time.time() - cached.stat().st_mtime) / 3600
        if age_hours < 24:
            return json.loads(cached.read_text())

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await _rate_limited_get(client, f"{SUBMISSIONS_BASE}/CIK{padded_cik}.json")
            resp.raise_for_status()
            data = resp.json()
            cached.write_text(json.dumps(data))
            return data
    except Exception as e:
        logger.warning("EDGAR submissions lookup failed for CIK %s: %s", cik, e)
        return {}


# ---------------------------------------------------------------------------
# XBRL Company Facts
# ---------------------------------------------------------------------------

async def get_company_facts(cik: str) -> dict:
    """Get XBRL company facts (structured financial data).

    Returns dict with facts organized by taxonomy (us-gaap, dei).
    """
    padded_cik = cik.zfill(10)
    cache_key = f"edgar_companyfacts_{padded_cik}"
    cached = get_cache_path(cache_key, suffix=".json")

    # Cache for 24 hours
    if cached.exists():
        age_hours = (time.time() - cached.stat().st_mtime) / 3600
        if age_hours < 24:
            return json.loads(cached.read_text())

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await _rate_limited_get(client, f"{XBRL_BASE}/CIK{padded_cik}.json")
            resp.raise_for_status()
            data = resp.json()
            cached.write_text(json.dumps(data))
            return data
    except Exception as e:
        logger.warning("EDGAR company facts failed for CIK %s: %s", cik, e)
        return {}


def extract_latest_xbrl_value(facts: dict, taxonomy: str, concept: str) -> float | None:
    """Extract the most recent value for a given XBRL concept.

    Prefers 10-K annual filings, takes the most recent by period end date.
    """
    try:
        concept_data = facts.get("facts", {}).get(taxonomy, {}).get(concept, {})
        units = concept_data.get("units", {})
        # Financial values are typically in USD
        values = units.get("USD", [])
        if not values:
            return None

        # Filter to 10-K filings, prefer annual
        annual = [v for v in values if v.get("form") == "10-K" and v.get("fp") == "FY"]
        if not annual:
            annual = [v for v in values if v.get("form") == "10-K"]
        if not annual:
            annual = values

        # Sort by end date descending, take most recent
        annual.sort(key=lambda v: v.get("end", ""), reverse=True)
        return float(annual[0]["val"])
    except (KeyError, IndexError, ValueError, TypeError):
        return None


def extract_financials(facts: dict) -> dict:
    """Extract key financial metrics from XBRL company facts."""
    gaap = "us-gaap"
    return {
        "revenue": extract_latest_xbrl_value(facts, gaap, "Revenues")
        or extract_latest_xbrl_value(facts, gaap, "RevenueFromContractWithCustomerExcludingAssessedTax")
        or extract_latest_xbrl_value(facts, gaap, "SalesRevenueNet"),
        "net_income": extract_latest_xbrl_value(facts, gaap, "NetIncomeLoss")
        or extract_latest_xbrl_value(facts, gaap, "ProfitLoss"),
        "total_assets": extract_latest_xbrl_value(facts, gaap, "Assets"),
        "stockholders_equity": extract_latest_xbrl_value(facts, gaap, "StockholdersEquity")
        or extract_latest_xbrl_value(facts, gaap, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
    }


def extract_debt_summary(facts: dict) -> dict:
    """Extract debt-related metrics from XBRL company facts."""
    gaap = "us-gaap"
    long_term = (
        extract_latest_xbrl_value(facts, gaap, "LongTermDebt")
        or extract_latest_xbrl_value(facts, gaap, "LongTermDebtNoncurrent")
    )
    short_term = (
        extract_latest_xbrl_value(facts, gaap, "ShortTermBorrowings")
        or extract_latest_xbrl_value(facts, gaap, "DebtCurrent")
    )
    equity = (
        extract_latest_xbrl_value(facts, gaap, "StockholdersEquity")
        or extract_latest_xbrl_value(facts, gaap, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    )
    debt_to_equity = None
    if long_term is not None and equity and equity != 0:
        debt_to_equity = round(long_term / equity, 4)

    return {
        "long_term_debt": long_term,
        "short_term_debt": short_term,
        "debt_to_equity": debt_to_equity,
    }


# ---------------------------------------------------------------------------
# HTML Filing Section Extraction
# ---------------------------------------------------------------------------

async def download_filing_html(cik: str, accession_number: str) -> str | None:
    """Download the primary HTML filing document from EDGAR.

    Returns HTML content as string, or None on failure.
    """
    # accession number: 0000320193-24-000058 -> path segment 000032019324000058
    acc_no_hyphens = accession_number.replace("-", "")
    cache_key = f"edgar_filing_{acc_no_hyphens}"
    cached = get_cache_path(cache_key, suffix=".html")

    if cached.exists():
        return cached.read_text(errors="replace")

    # First, get the filing index to find the primary document
    padded_cik = cik.lstrip("0") or "0"  # EDGAR paths use unpadded CIK
    index_url = f"{ARCHIVES_BASE}/{padded_cik}/{acc_no_hyphens}/{accession_number}-index.htm"

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await _rate_limited_get(client, index_url)
            resp.raise_for_status()
            index_html = resp.text

            # Find the primary document link (usually the first .htm file)
            doc_match = re.search(r'href="([^"]+\.htm)"', index_html)
            if not doc_match:
                logger.warning("No primary HTML doc found in filing index for %s", accession_number)
                return None

            doc_path = doc_match.group(1)
            if not doc_path.startswith("http"):
                doc_url = f"{ARCHIVES_BASE}/{padded_cik}/{acc_no_hyphens}/{doc_path}"
            else:
                doc_url = doc_path

            resp = await _rate_limited_get(client, doc_url)
            resp.raise_for_status()
            html = resp.text
            cached.write_text(html)
            return html
    except Exception as e:
        logger.warning("Failed to download filing HTML for %s: %s", accession_number, e)
        return None


def extract_section(html: str, section: str, max_chars: int = 2000) -> str | None:
    """Extract a named section from a 10-K HTML filing.

    Searches for section headings (Item 1A Risk Factors, Item 7 MD&A) and
    extracts text until the next section heading.
    """
    if not html:
        return None

    # Strip HTML tags for text extraction
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)

    section_patterns = {
        "mda": r"(?:Item\s*7\.?\s*[\.\-—]?\s*Management.s?\s*Discussion\s*and\s*Analysis)",
        "risk_factors": r"(?:Item\s*1A\.?\s*[\.\-—]?\s*Risk\s*Factors)",
    }

    pattern = section_patterns.get(section)
    if not pattern:
        return None

    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None

    start = match.end()
    # Find the next "Item N" heading to delimit the section
    next_item = re.search(r"Item\s*\d+[A-Za-z]?\.?\s*[\.\-—]", text[start:], re.IGNORECASE)
    end = start + next_item.start() if next_item else start + max_chars

    extracted = text[start:end].strip()
    if len(extracted) > max_chars:
        extracted = extracted[:max_chars] + "..."
    return extracted if extracted else None


# ---------------------------------------------------------------------------
# CIK Lookup from Accession Number
# ---------------------------------------------------------------------------

async def get_cik_from_accession(accession_number: str) -> str | None:
    """Extract CIK from an accession number by fetching the filing index.

    Accession numbers start with the CIK (first 10 digits of the filer).
    """
    # Accession format: XXXXXXXXXX-YY-ZZZZZZ where X is the filer's CIK
    parts = accession_number.split("-")
    if len(parts) == 3:
        return parts[0].lstrip("0") or "0"
    return None


# ---------------------------------------------------------------------------
# Filing Index (for muni bond details)
# ---------------------------------------------------------------------------

async def get_filing_index(cik: str, accession_number: str) -> dict:
    """Get the filing index page and extract document listing.

    Returns dict with issuer info and list of documents.
    """
    acc_no_hyphens = accession_number.replace("-", "")
    padded_cik = cik.lstrip("0") or "0"
    index_url = f"{ARCHIVES_BASE}/{padded_cik}/{acc_no_hyphens}/{accession_number}-index.htm"

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await _rate_limited_get(client, index_url)
            resp.raise_for_status()
            html = resp.text

            # Extract document links and descriptions
            documents = []
            for match in re.finditer(
                r'<tr[^>]*>.*?href="([^"]+)"[^>]*>([^<]*)</a>.*?<td[^>]*>([^<]*)</td>',
                html,
                re.DOTALL,
            ):
                href, name, doc_type = match.groups()
                if not href.startswith("http"):
                    href = f"{ARCHIVES_BASE}/{padded_cik}/{acc_no_hyphens}/{href}"
                documents.append({
                    "name": name.strip(),
                    "url": href,
                    "type": doc_type.strip(),
                })

            # Extract filing description from the page
            desc_match = re.search(r"<div[^>]*>.*?Filing Type.*?</div>\s*<div[^>]*>(.*?)</div>", html, re.DOTALL)
            description = ""
            if desc_match:
                description = re.sub(r"<[^>]+>", "", desc_match.group(1)).strip()

            return {"documents": documents, "description": description}
    except Exception as e:
        logger.warning("Failed to get filing index for %s: %s", accession_number, e)
        return {"documents": [], "description": ""}
```

**Step 2: Verify EDGAR EFTS search works**

Run: `python -c "
import asyncio, json
from servers.financial_intelligence.edgar_client import search_filings
result = asyncio.run(search_filings('HCA Healthcare', forms='10-K'))
hits = result.get('hits', result.get('filings', []))
print(json.dumps({'keys': list(result.keys()), 'type': type(hits).__name__}, indent=2))
"`

Expected: Shows top-level response keys. Adjust field names in code if the actual API response structure differs from expected.

**Step 3: Verify XBRL company facts works**

Run: `python -c "
import asyncio, json
from servers.financial_intelligence.edgar_client import get_company_facts, extract_financials
facts = asyncio.run(get_company_facts('320193'))  # Apple CIK
financials = extract_financials(facts)
print(json.dumps(financials, indent=2, default=str))
"`

Expected: JSON with revenue, net_income, total_assets, stockholders_equity values for Apple.

**Step 4: Commit**

```bash
git add servers/financial-intelligence/edgar_client.py
git commit -m "feat(financial-intelligence): add EDGAR client (EFTS, XBRL, HTML parsing)"
```

---

### Task 5: Server.py — wire up all 6 tools

**Files:**
- Create: `servers/financial-intelligence/server.py`

**Step 1: Write server.py with all 6 tools**

Write `servers/financial-intelligence/server.py`:

```python
"""Financial Intelligence MCP Server.

Provides tools for IRS Form 990 nonprofit financials, SEC EDGAR corporate
filings, and municipal bond data from public APIs.
"""

import json
import logging
import os as _os

from mcp.server.fastmcp import FastMCP

from . import edgar_client, propublica_client
from .irs990_parser import download_990_xml, parse_990_xml
from .models import (
    Form990Details,
    Form990Summary,
    MuniBond,
    MuniBondDetails,
    Officer,
    SecFiling,
    SecFilingDetail,
)

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "financial-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8008"))
mcp = FastMCP(**_mcp_kwargs)


def _safe_float(val) -> float | None:
    """Parse a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Tool 1: search_form990
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_form990(query: str, state: str = "", ntee_code: str = "") -> str:
    """Search IRS Form 990 filings by organization name or EIN.

    Returns nonprofit organizations with revenue, expenses, and net assets
    from the most recent filing.

    Args:
        query: Organization name or EIN to search for.
        state: Two-letter state code filter (e.g. "OH").
        ntee_code: NTEE category code filter (1-10).
    """
    try:
        data = await propublica_client.search_organizations(query, state=state, ntee_code=ntee_code)
        orgs = data.get("organizations", [])

        results = []
        for org in orgs[:25]:  # Limit to 25 results
            results.append(Form990Summary(
                ein=str(org.get("ein", "")),
                name=org.get("name", ""),
                city=org.get("city", ""),
                state=org.get("state", ""),
                ntee_code=org.get("ntee_code", ""),
                total_revenue=_safe_float(org.get("income_amount")),
                total_expenses=_safe_float(org.get("revenue_amount")),  # ProPublica field naming
                net_assets=_safe_float(org.get("asset_amount")),
                tax_period=str(org.get("tax_period", "")),
            ).model_dump())

        return json.dumps({"total_results": data.get("total_results", 0), "organizations": results})
    except Exception as e:
        logger.exception("search_form990 failed")
        return json.dumps({"error": f"search_form990 failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 2: get_form990_details
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_form990_details(ein: str) -> str:
    """Get detailed Form 990 data for a nonprofit by EIN.

    Returns revenue breakdown, functional expenses (Part IX), Schedule H
    community benefit (hospitals), officer compensation, and program descriptions.
    Parses the full IRS e-file XML when available; falls back to ProPublica summary.

    Args:
        ein: Employer Identification Number (e.g. "341323166").
    """
    try:
        org_data = await propublica_client.get_organization(ein)
        if not org_data:
            return json.dumps({"error": f"Organization not found for EIN: {ein}"})

        org = org_data.get("organization", {})
        filings = org_data.get("filings_with_data", [])

        if not filings:
            return json.dumps({"error": f"No filings with data found for EIN: {ein}"})

        latest = filings[0]
        xml_url = latest.get("xml_url", "")
        tax_period = str(latest.get("tax_prd", latest.get("tax_prd_yr", "")))

        # Try to download and parse the full XML
        if xml_url:
            xml_path = await download_990_xml(xml_url, ein, tax_period)
            if xml_path:
                parsed = parse_990_xml(xml_path)
                result = Form990Details(
                    ein=ein,
                    name=org.get("name", ""),
                    tax_period=tax_period,
                    source="xml",
                    contributions=parsed.get("contributions"),
                    program_service_revenue=parsed.get("program_service_revenue"),
                    investment_income=parsed.get("investment_income"),
                    other_revenue=parsed.get("other_revenue"),
                    total_revenue=parsed.get("total_revenue"),
                    total_expenses=parsed.get("total_expenses"),
                    program_expenses=parsed.get("program_expenses"),
                    management_expenses=parsed.get("management_expenses"),
                    fundraising_expenses=parsed.get("fundraising_expenses"),
                    community_benefit_total=parsed.get("community_benefit_total"),
                    community_benefit_pct=parsed.get("community_benefit_pct"),
                    officers=[Officer(**o) for o in parsed.get("officers", [])],
                    program_descriptions=parsed.get("program_descriptions", []),
                )
                return json.dumps(result.model_dump())

        # Fallback: ProPublica summary data
        result = Form990Details(
            ein=ein,
            name=org.get("name", ""),
            tax_period=tax_period,
            source="propublica",
            total_revenue=_safe_float(latest.get("totrevenue")),
            total_expenses=_safe_float(latest.get("totfuncexpns")),
            net_assets=None,
            officers=[],
            program_descriptions=[],
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_form990_details failed")
        return json.dumps({"error": f"get_form990_details failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 3: search_sec_filings
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_sec_filings(query: str, filing_type: str = "10-K", date_from: str = "", date_to: str = "") -> str:
    """Search SEC EDGAR filings by company name, CIK, or keyword.

    Returns a list of filings with accession numbers, filing dates, and links.

    Args:
        query: Company name, CIK number, or keyword to search.
        filing_type: SEC form type filter (e.g. "10-K", "10-Q", "8-K"). Default "10-K".
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).
    """
    try:
        data = await edgar_client.search_filings(query, forms=filing_type, date_from=date_from, date_to=date_to)

        # EFTS returns hits in varying structures; normalize
        hits_container = data.get("hits", data)
        if isinstance(hits_container, dict):
            hits = hits_container.get("hits", [])
            total = hits_container.get("total", {})
            if isinstance(total, dict):
                total_count = total.get("value", 0)
            else:
                total_count = total
        else:
            hits = hits_container if isinstance(hits_container, list) else []
            total_count = len(hits)

        results = []
        for hit in hits[:25]:
            source = hit.get("_source", hit)
            cik = str(source.get("entity_id", source.get("cik", "")))
            acc = source.get("file_num", source.get("accession_no", source.get("accession_number", "")))
            results.append(SecFiling(
                accession_number=acc,
                company_name=source.get("entity_name", source.get("company_name", source.get("display_names", [""])[0] if isinstance(source.get("display_names"), list) else "")),
                cik=cik,
                form_type=source.get("form_type", source.get("file_type", filing_type)),
                filing_date=source.get("file_date", source.get("filing_date", "")),
                filing_url=source.get("file_url", ""),
            ).model_dump())

        return json.dumps({"total_results": total_count, "filings": results})
    except Exception as e:
        logger.exception("search_sec_filings failed")
        return json.dumps({"error": f"search_sec_filings failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 4: get_sec_filing
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_sec_filing(accession_number: str, sections: list[str] | None = None) -> str:
    """Get detailed data from a specific SEC filing.

    Retrieves structured XBRL financial data and/or narrative sections (MD&A,
    Risk Factors) from 10-K/10-Q filings.

    Args:
        accession_number: EDGAR accession number (e.g. "0000320193-24-000058").
        sections: Which sections to retrieve. Options: "financials", "debt", "mda", "risk_factors". Default ["financials"].
    """
    if sections is None:
        sections = ["financials"]

    try:
        # Extract CIK from accession number
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return json.dumps({"error": f"Could not determine CIK from accession number: {accession_number}"})

        # Get company info from submissions
        submissions = await edgar_client.get_company_submissions(cik)
        company_name = submissions.get("name", "")
        form_type = ""
        filing_date = ""

        # Find this specific filing in submission history
        recent = submissions.get("filings", {}).get("recent", {})
        accession_numbers = recent.get("accessionNumber", [])
        for i, acc in enumerate(accession_numbers):
            if acc == accession_number:
                form_type = recent.get("form", [])[i] if i < len(recent.get("form", [])) else ""
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        result = SecFilingDetail(
            accession_number=accession_number,
            company_name=company_name,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
        )

        # XBRL sections
        if "financials" in sections or "debt" in sections:
            facts = await edgar_client.get_company_facts(cik)
            if "financials" in sections:
                result.financials = edgar_client.extract_financials(facts)
            if "debt" in sections:
                result.debt_summary = edgar_client.extract_debt_summary(facts)

        # HTML narrative sections
        if "mda" in sections or "risk_factors" in sections:
            html = await edgar_client.download_filing_html(cik, accession_number)
            if html:
                if "mda" in sections:
                    result.mda_text = edgar_client.extract_section(html, "mda")
                if "risk_factors" in sections:
                    result.risk_factors_text = edgar_client.extract_section(html, "risk_factors")

        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_sec_filing failed")
        return json.dumps({"error": f"get_sec_filing failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 5: search_muni_bonds
# ---------------------------------------------------------------------------
@mcp.tool()
async def search_muni_bonds(query: str, state: str = "", date_from: str = "", date_to: str = "") -> str:
    """Search municipal bond offerings via SEC EDGAR Official Statements.

    Returns municipal bond filings with issuer name, filing date, and accession number.

    Args:
        query: Issuer name or keyword to search.
        state: Two-letter state code filter (e.g. "CA").
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).
    """
    try:
        search_query = query
        if state:
            search_query = f"{query} {state}"

        data = await edgar_client.search_filings(search_query, forms="OS", date_from=date_from, date_to=date_to)

        hits_container = data.get("hits", data)
        if isinstance(hits_container, dict):
            hits = hits_container.get("hits", [])
            total = hits_container.get("total", {})
            total_count = total.get("value", 0) if isinstance(total, dict) else total
        else:
            hits = hits_container if isinstance(hits_container, list) else []
            total_count = len(hits)

        results = []
        for hit in hits[:25]:
            source = hit.get("_source", hit)
            results.append(MuniBond(
                accession_number=source.get("file_num", source.get("accession_no", "")),
                issuer_name=source.get("entity_name", source.get("display_names", [""])[0] if isinstance(source.get("display_names"), list) else ""),
                state=state,
                filing_date=source.get("file_date", source.get("filing_date", "")),
                filing_url=source.get("file_url", ""),
            ).model_dump())

        return json.dumps({"total_results": total_count, "bonds": results})
    except Exception as e:
        logger.exception("search_muni_bonds failed")
        return json.dumps({"error": f"search_muni_bonds failed: {e}"})


# ---------------------------------------------------------------------------
# Tool 6: get_muni_bond_details
# ---------------------------------------------------------------------------
@mcp.tool()
async def get_muni_bond_details(accession_number: str) -> str:
    """Get details for a specific municipal bond filing from EDGAR.

    Returns the issuer information, filing documents list, and links to
    the Official Statement PDF.

    Args:
        accession_number: EDGAR accession number for the Official Statement.
    """
    try:
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return json.dumps({"error": f"Could not determine CIK from accession number: {accession_number}"})

        submissions = await edgar_client.get_company_submissions(cik)
        issuer_name = submissions.get("name", "")
        filing_date = ""

        recent = submissions.get("filings", {}).get("recent", {})
        for i, acc in enumerate(recent.get("accessionNumber", [])):
            if acc == accession_number:
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        index_data = await edgar_client.get_filing_index(cik, accession_number)

        result = MuniBondDetails(
            accession_number=accession_number,
            issuer_name=issuer_name,
            filing_date=filing_date,
            documents=index_data.get("documents", []),
            description=index_data.get("description", ""),
        )
        return json.dumps(result.model_dump())
    except Exception as e:
        logger.exception("get_muni_bond_details failed")
        return json.dumps({"error": f"get_muni_bond_details failed: {e}"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
```

**Step 2: Verify server imports cleanly**

Run: `python -c "from servers.financial_intelligence.server import mcp; print(f'Server: {mcp.name}, Tools: {len(mcp._tools) if hasattr(mcp, \"_tools\") else \"unknown\"}')"`

Expected: `Server: financial-intelligence, Tools: 6`

**Step 3: Commit**

```bash
git add servers/financial-intelligence/server.py
git commit -m "feat(financial-intelligence): wire up all 6 tools in server.py"
```

---

### Task 6: Integration — Docker, MCP registration, env vars

**Files:**
- Modify: `docker-compose.yml` (add service)
- Modify: `.mcp.json` (add server entry)
- Modify: `.env.example` (add SEC_USER_AGENT)

**Step 1: Add docker-compose service**

Append to `docker-compose.yml` before the `volumes:` block:

```yaml
  financial-intelligence:
    build: .
    command: python -m servers.financial_intelligence.server
    ports:
      - "8008:8008"
    environment:
      - MCP_TRANSPORT=streamable-http
      - MCP_PORT=8008
      - SEC_USER_AGENT=${SEC_USER_AGENT:-healthcare-data-mcp support@example.com}
    volumes:
      - healthcare-cache:/root/.healthcare-data-mcp/cache
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "import socket; s=socket.create_connection(('localhost',8008),5); s.close()"]
      interval: 60s
      timeout: 10s
      retries: 3
      start_period: 30s
```

**Step 2: Add .mcp.json entry**

Add to the `mcpServers` object in `.mcp.json`:

```json
    "financial-intelligence": {
      "type": "http",
      "url": "http://localhost:8008/mcp"
    }
```

**Step 3: Add SEC_USER_AGENT to .env.example**

Append to `.env.example`:

```
# SEC EDGAR User-Agent header (required, format: "AppName email@domain.com")
# No API key needed — just identifies your application for fair access policy
SEC_USER_AGENT=healthcare-data-mcp support@example.com
```

**Step 4: Commit**

```bash
git add docker-compose.yml .mcp.json .env.example
git commit -m "feat(financial-intelligence): add Docker, MCP registration, and env config"
```

---

### Task 7: Smoke test

**Files:**
- Modify: `smoke_test.py` (add test_financial_intelligence function)

**Step 1: Add smoke test function**

Add to `smoke_test.py` a new test function following the existing pattern:

```python
def test_financial_intelligence():
    """Test financial intelligence server: ProPublica search, EDGAR search."""
    import asyncio
    import time
    from servers.financial_intelligence.propublica_client import search_organizations, get_organization
    from servers.financial_intelligence.edgar_client import search_filings, get_company_facts, extract_financials

    results = {}

    # Test ProPublica search
    t0 = time.time()
    orgs = asyncio.run(search_organizations("Cleveland Clinic"))
    results["propublica_search_time"] = round(time.time() - t0, 2)
    results["propublica_results"] = orgs.get("total_results", 0)
    assert results["propublica_results"] > 0, "ProPublica search returned no results"

    # Test ProPublica org detail
    t0 = time.time()
    org = asyncio.run(get_organization("341323166"))  # Cleveland Clinic
    results["propublica_org_time"] = round(time.time() - t0, 2)
    results["propublica_has_filings"] = len(org.get("filings_with_data", []))
    assert results["propublica_has_filings"] > 0, "Cleveland Clinic should have filings"

    # Test EDGAR EFTS search
    t0 = time.time()
    filings = asyncio.run(search_filings("HCA Healthcare", forms="10-K"))
    results["edgar_search_time"] = round(time.time() - t0, 2)
    results["edgar_search_keys"] = list(filings.keys())

    # Test EDGAR XBRL company facts (Apple as known-good CIK)
    t0 = time.time()
    facts = asyncio.run(get_company_facts("320193"))
    results["xbrl_time"] = round(time.time() - t0, 2)
    financials = extract_financials(facts)
    results["xbrl_revenue"] = financials.get("revenue")
    assert results["xbrl_revenue"] is not None, "Apple XBRL should have revenue"

    return results
```

Also add `test_financial_intelligence` to the main execution block alongside existing tests.

**Step 2: Run the smoke test**

Run: `python smoke_test.py`

Expected: `test_financial_intelligence` passes with ProPublica results > 0, XBRL revenue populated.

**Step 3: Commit**

```bash
git add smoke_test.py
git commit -m "test(financial-intelligence): add smoke test for ProPublica and EDGAR APIs"
```

---

### Task 8: API response calibration

After running the smoke tests, the EDGAR EFTS response structure may differ from what we assumed. This task handles adjusting field mappings.

**Files:**
- Modify: `servers/financial-intelligence/edgar_client.py` (if EFTS response fields differ)
- Modify: `servers/financial-intelligence/server.py` (if field mapping needs updating)

**Step 1: Run EFTS search and inspect raw response**

Run: `python -c "
import asyncio, json
from servers.financial_intelligence.edgar_client import search_filings
result = asyncio.run(search_filings('Apple', forms='10-K'))
# Print the full response structure (truncated)
print(json.dumps(result, indent=2, default=str)[:3000])
"`

**Step 2: Compare actual field names vs what server.py expects**

Inspect the `_source` (or top-level) keys in the EFTS response. If field names differ from `entity_name`, `file_num`, `file_date`, `form_type`, update the field mappings in `server.py`'s `search_sec_filings` and `search_muni_bonds` tools.

**Step 3: Fix any mismatches and re-run smoke test**

Update the `source.get(...)` calls in `server.py` to match the actual EFTS response fields. Re-run:

Run: `python smoke_test.py`

Expected: All tests pass.

**Step 4: Commit**

```bash
git add servers/financial-intelligence/edgar_client.py servers/financial-intelligence/server.py
git commit -m "fix(financial-intelligence): calibrate EDGAR EFTS response field mappings"
```

---

### Task 9: IRS 990 XML parsing validation

**Files:**
- Modify: `servers/financial-intelligence/irs990_parser.py` (if XML element names need adjusting)

**Step 1: Test 990 XML parse with a known hospital**

Run: `python -c "
import asyncio, json
from servers.financial_intelligence.propublica_client import get_organization
from servers.financial_intelligence.irs990_parser import download_990_xml, parse_990_xml

async def test():
    org = await get_organization('341323166')  # Cleveland Clinic
    filings = org.get('filings_with_data', [])
    latest = filings[0]
    xml_url = latest.get('xml_url', '')
    tax_period = str(latest.get('tax_prd', ''))
    if not xml_url:
        print('No XML URL — check ProPublica response')
        print(json.dumps(latest, indent=2)[:1000])
        return
    path = await download_990_xml(xml_url, '341323166', tax_period)
    if not path:
        print('Download failed')
        return
    parsed = parse_990_xml(path)
    for k, v in parsed.items():
        if k == 'officers':
            print(f'officers: {len(v)} entries')
            for o in v[:3]:
                print(f'  {o}')
        elif k == 'program_descriptions':
            print(f'program_descriptions: {len(v)} entries')
        else:
            print(f'{k}: {v}')

asyncio.run(test())
"`

**Step 2: Inspect which fields parsed successfully vs None**

If key fields (total_revenue, total_expenses, officers) are all None, inspect the raw XML to identify the actual element names:

Run: `python -c "
from pathlib import Path
from shared.utils.cms_client import get_cache_path
# Find the cached XML
import glob
xmls = list(Path.home().glob('.healthcare-data-mcp/cache/*.xml'))
if xmls:
    print(f'Found {len(xmls)} cached XML files')
    # Print first 100 lines of the most recent one
    with open(xmls[-1]) as f:
        for i, line in enumerate(f):
            if i > 100: break
            print(line.rstrip())
"`

**Step 3: Adjust element name candidates in _find_float/_find_text calls if needed**

Update the tag name candidates in `parse_990_xml()` to match the actual XML schema.

**Step 4: Re-run and verify**

Re-run the Step 1 command. Expected: total_revenue, total_expenses populated with non-None values, officers count > 0 for Cleveland Clinic.

**Step 5: Commit**

```bash
git add servers/financial-intelligence/irs990_parser.py
git commit -m "fix(financial-intelligence): calibrate IRS 990 XML element name mappings"
```

---

### Task 10: End-to-end MCP server test

**Step 1: Start the server in stdio mode and verify it responds**

Run: `python -m servers.financial_intelligence.server &`

Then test via MCP inspect or direct tool call. Alternatively, run the full server check:

Run: `python -c "
import asyncio
from servers.financial_intelligence.server import search_form990, get_form990_details, search_sec_filings

async def e2e():
    # 990 search
    r1 = await search_form990('Mayo Clinic')
    print('search_form990:', r1[:200])

    # 990 details
    r2 = await get_form990_details('410693889')  # Mayo Clinic EIN
    print('get_form990_details:', r2[:200])

    # SEC search
    r3 = await search_sec_filings('UnitedHealth Group', filing_type='10-K')
    print('search_sec_filings:', r3[:200])

asyncio.run(e2e())
"`

Expected: All three tools return JSON with real data (not error objects).

**Step 2: Final commit if any adjustments were made**

```bash
git add -u
git commit -m "fix(financial-intelligence): end-to-end adjustments after MCP server test"
```

---

## Summary of Commits

| Task | Commit Message |
|------|---------------|
| 1 | `feat(financial-intelligence): scaffold directory and Pydantic models` |
| 2 | `feat(financial-intelligence): add ProPublica Nonprofit Explorer client` |
| 3 | `feat(financial-intelligence): add IRS 990 XML parser` |
| 4 | `feat(financial-intelligence): add EDGAR client (EFTS, XBRL, HTML parsing)` |
| 5 | `feat(financial-intelligence): wire up all 6 tools in server.py` |
| 6 | `feat(financial-intelligence): add Docker, MCP registration, and env config` |
| 7 | `test(financial-intelligence): add smoke test for ProPublica and EDGAR APIs` |
| 8 | `fix(financial-intelligence): calibrate EDGAR EFTS response field mappings` |
| 9 | `fix(financial-intelligence): calibrate IRS 990 XML element name mappings` |
| 10 | `fix(financial-intelligence): end-to-end adjustments after MCP server test` |
