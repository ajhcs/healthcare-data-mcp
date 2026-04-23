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


from shared.utils.http_client import resilient_request
import pandas as pd

from shared.utils.cms_client import get_cache_path

logger = logging.getLogger(__name__)

IRS_EFILE_INDEX_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# In-memory cache for loaded indexes
_index_cache: dict[str, pd.DataFrame] = {}


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
        resp = await resilient_request("GET", xml_url, timeout=120.0)
        cached.write_bytes(resp.content)
        logger.info("Cached 990 XML for EIN %s period %s", ein, tax_period)
        return cached
    except Exception as e:
        logger.warning("Failed to download 990 XML from %s: %s", xml_url, e)
        return None


async def load_efile_index(year: str) -> pd.DataFrame:
    """Download and cache the IRS 990 e-file index CSV for a given year.

    Index contains RETURN_ID, FILING_TYPE, EIN, TAX_PERIOD, SUB_DATE,
    TAXPAYER_NAME, RETURN_TYPE, DLN, OBJECT_ID.
    """
    if year in _index_cache:
        return _index_cache[year]

    cache_key = f"irs990_index_{year}"
    cached = get_cache_path(cache_key, suffix=".csv")

    if not cached.exists():
        url = f"{IRS_EFILE_INDEX_BASE}/{year}/index_{year}.csv"
        try:
            resp = await resilient_request("GET", url, timeout=300.0)
            cached.write_bytes(resp.content)
            logger.info("Cached IRS e-file index for year %s (%d bytes)", year, len(resp.content))
        except Exception as e:
            logger.warning("Failed to download IRS e-file index for %s: %s", year, e)
            return pd.DataFrame()

    try:
        df = pd.read_csv(cached, dtype=str, keep_default_na=False)
        df.columns = [c.strip().upper() for c in df.columns]
        _index_cache[year] = df
        return df
    except Exception as e:
        logger.warning("Failed to parse IRS e-file index for %s: %s", year, e)
        return pd.DataFrame()


async def lookup_xml_url(ein: str, tax_period: str) -> str | None:
    """Look up the IRS e-file XML URL for a given EIN and tax period.

    Args:
        ein: Employer Identification Number (digits only, no hyphens).
        tax_period: Tax period in YYYYMM or YYYY format.

    Returns:
        Full URL to the XML file, or None if not found.
    """
    # Extract year from tax_period (YYYYMM -> YYYY)
    year = tax_period[:4] if len(tax_period) >= 4 else ""
    if not year or not year.isdigit():
        return None

    # Try the filing year and the next year (filings may appear in index of following year)
    for try_year in (year, str(int(year) + 1)):
        df = await load_efile_index(try_year)
        if df.empty:
            continue

        ein_col = "EIN" if "EIN" in df.columns else None
        if not ein_col:
            continue

        # Filter by EIN (strip leading zeros for comparison)
        matches = df[df[ein_col].str.strip() == ein.strip().lstrip("0")]
        if matches.empty:
            # Try with original (some EINs have leading zeros in index)
            matches = df[df[ein_col].str.strip() == ein.strip()]

        if matches.empty:
            continue

        # If we have a specific tax_period (YYYYMM), filter further
        if len(tax_period) >= 6 and "TAX_PERIOD" in matches.columns:
            period_matches = matches[matches["TAX_PERIOD"].str.strip() == tax_period.strip()]
            if not period_matches.empty:
                matches = period_matches

        # Take the most recent by TAX_PERIOD
        if "TAX_PERIOD" in matches.columns:
            matches = matches.sort_values("TAX_PERIOD", ascending=False)

        # Get OBJECT_ID from the first (most recent) match
        obj_id_col = "OBJECT_ID" if "OBJECT_ID" in matches.columns else None
        if not obj_id_col:
            continue

        object_id = str(matches.iloc[0][obj_id_col]).strip()
        if not object_id:
            continue

        xml_url = f"{IRS_EFILE_INDEX_BASE}/{try_year}/{object_id}_public.xml"
        logger.info("Found IRS e-file XML for EIN %s: %s", ein, xml_url)
        return xml_url

    return None


def parse_990_xml(xml_path: Path) -> dict:
    """Parse a 990 XML file and extract structured financial data.

    Returns a dict with keys matching Form990Details model fields.
    """
    tree = ET.parse(xml_path)
    root = _strip_ns(tree.getroot())

    return_data = root.find(".//ReturnData")
    if return_data is None:
        return_data = root

    form = return_data.find(".//IRS990")
    if form is None:
        form = return_data

    result: dict = {}

    # --- Revenue (Part VIII) ---
    result["contributions"] = _find_float(
        form, "CYContributionsGrantsAmt", "ContributionsGrantsCurrentYear",
        "TotalContributionsAmt",
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
    # Current schema uses Form990PartVIISectionAGrp; older versions may use ListGrp
    for tag in ("Form990PartVIISectionAGrp", "Form990PartVIISectionAListGrp"):
        for comp_el in form.findall(f".//{tag}"):
            name = _find_text(comp_el, "PersonNm", "BusinessName", "NamePerson")
            title = _find_text(comp_el, "TitleTxt", "Title")
            comp = _find_float(comp_el, "ReportableCompFromOrgAmt", "Compensation", "TotalCompensationAmt")
            if name:
                officers.append({"name": name, "title": title, "compensation": comp})
        if officers:
            break
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
            descriptions.append(desc[:500])
    if not descriptions:
        for prog_el in form.findall(".//ProgSrvcAccomActyOtherGrp"):
            desc = _find_text(prog_el, "Desc", "DescriptionProgramSrvcAccomTxt")
            if desc:
                descriptions.append(desc[:500])
    if not descriptions:
        mission = _find_text(form, "ActivityOrMissionDesc", "MissionDesc", "Description")
        if mission:
            descriptions.append(mission[:500])

    result["program_descriptions"] = descriptions

    return result
