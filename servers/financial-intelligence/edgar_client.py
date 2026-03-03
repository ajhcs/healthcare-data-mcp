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

_last_request_time: float = 0.0


def _headers() -> dict[str, str]:
    return {"User-Agent": SEC_USER_AGENT, "Accept": "application/json"}


async def _rate_limited_get(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
    """GET with rate limiting for SEC fair access policy."""
    global _last_request_time
    elapsed = time.monotonic() - _last_request_time
    if elapsed < 0.1:
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
    """Search EDGAR filings via EFTS full-text search."""
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
# Company Submissions
# ---------------------------------------------------------------------------

async def get_company_submissions(cik: str) -> dict:
    """Get company submission history from EDGAR. CIK is zero-padded to 10 digits."""
    padded_cik = cik.zfill(10)
    cache_key = f"edgar_submissions_{padded_cik}"
    cached = get_cache_path(cache_key, suffix=".json")

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
    """Get XBRL company facts (structured financial data)."""
    padded_cik = cik.zfill(10)
    cache_key = f"edgar_companyfacts_{padded_cik}"
    cached = get_cache_path(cache_key, suffix=".json")

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
    """Extract the most recent value for a given XBRL concept."""
    try:
        concept_data = facts.get("facts", {}).get(taxonomy, {}).get(concept, {})
        units = concept_data.get("units", {})
        values = units.get("USD", [])
        if not values:
            return None

        annual = [v for v in values if v.get("form") == "10-K" and v.get("fp") == "FY"]
        if not annual:
            annual = [v for v in values if v.get("form") == "10-K"]
        if not annual:
            annual = values

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
    """Download the primary HTML filing document from EDGAR."""
    acc_no_hyphens = accession_number.replace("-", "")
    cache_key = f"edgar_filing_{acc_no_hyphens}"
    cached = get_cache_path(cache_key, suffix=".html")

    if cached.exists():
        return cached.read_text(errors="replace")

    padded_cik = cik.lstrip("0") or "0"
    index_url = f"{ARCHIVES_BASE}/{padded_cik}/{acc_no_hyphens}/{accession_number}-index.htm"

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await _rate_limited_get(client, index_url)
            resp.raise_for_status()
            index_html = resp.text

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
    """Extract a named section from a 10-K HTML filing."""
    if not html:
        return None

    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)

    section_patterns = {
        "mda": r"(?:Item\s*7\.?\s*[\.\-\u2014]?\s*Management.s?\s*Discussion\s*and\s*Analysis)",
        "risk_factors": r"(?:Item\s*1A\.?\s*[\.\-\u2014]?\s*Risk\s*Factors)",
    }

    pattern = section_patterns.get(section)
    if not pattern:
        return None

    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None

    start = match.end()
    next_item = re.search(r"Item\s*\d+[A-Za-z]?\.?\s*[\.\-\u2014]", text[start:], re.IGNORECASE)
    end = start + next_item.start() if next_item else start + max_chars

    extracted = text[start:end].strip()
    if len(extracted) > max_chars:
        extracted = extracted[:max_chars] + "..."
    return extracted if extracted else None


# ---------------------------------------------------------------------------
# CIK Lookup from Accession Number
# ---------------------------------------------------------------------------

async def get_cik_from_accession(accession_number: str) -> str | None:
    """Extract CIK from an accession number."""
    parts = accession_number.split("-")
    if len(parts) == 3:
        return parts[0].lstrip("0") or "0"
    return None


# ---------------------------------------------------------------------------
# Filing Index (for muni bond details)
# ---------------------------------------------------------------------------

async def get_filing_index(cik: str, accession_number: str) -> dict:
    """Get the filing index page and extract document listing."""
    acc_no_hyphens = accession_number.replace("-", "")
    padded_cik = cik.lstrip("0") or "0"
    index_url = f"{ARCHIVES_BASE}/{padded_cik}/{acc_no_hyphens}/{accession_number}-index.htm"

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await _rate_limited_get(client, index_url)
            resp.raise_for_status()
            html = resp.text

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

            desc_match = re.search(r"<div[^>]*>.*?Filing Type.*?</div>\s*<div[^>]*>(.*?)</div>", html, re.DOTALL)
            description = ""
            if desc_match:
                description = re.sub(r"<[^>]+>", "", desc_match.group(1)).strip()

            return {"documents": documents, "description": description}
    except Exception as e:
        logger.warning("Failed to get filing index for %s: %s", accession_number, e)
        return {"documents": [], "description": ""}
