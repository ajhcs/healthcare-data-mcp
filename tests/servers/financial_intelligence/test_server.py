"""Tests for the financial-intelligence MCP server tools.

Uses monkeypatching to avoid live ProPublica/EDGAR API calls.
"""

from tests.helpers import parse_tool_result
import os
from unittest.mock import AsyncMock, patch

import pytest

# SEC_USER_AGENT must be set before the edgar_client module is imported, because
# it raises RuntimeError at module level when the var is missing.
os.environ.setdefault("SEC_USER_AGENT", "CI ci@example.com")

from servers.financial_intelligence import audited_financial_pdf, server, propublica_client, edgar_client  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures — realistic ProPublica/EDGAR-shaped payloads
# ---------------------------------------------------------------------------

PROPUBLICA_SEARCH_RESPONSE = {
    "total_results": 2,
    "organizations": [
        {
            "ein": 231352166,
            "name": "Thomas Jefferson University",
            "city": "Philadelphia",
            "state": "PA",
            "ntee_code": "E21",
            "revenue_amount": 2_800_000_000,
            "expenses_amount": 2_750_000_000,
            "asset_amount": 4_200_000_000,
            "tax_period": 202312,
        },
        {
            "ein": 236002364,
            "name": "Temple University Health System",
            "city": "Philadelphia",
            "state": "PA",
            "ntee_code": "E21",
            "revenue_amount": 1_500_000_000,
            "expenses_amount": 1_480_000_000,
            "asset_amount": 2_100_000_000,
            "tax_period": 202312,
        },
    ],
}

PROPUBLICA_ORG_DETAIL = {
    "organization": {
        "ein": 231352166,
        "name": "Thomas Jefferson University",
        "city": "Philadelphia",
        "state": "PA",
        "ntee_code": "E21",
    },
    "filings_with_data": [
        {
            "tax_prd": 202312,
            "tax_prd_yr": 2023,
            "totrevenue": 2_800_000_000,
            "totfuncexpns": 2_750_000_000,
            "totnetassetend": 1_450_000_000,
            "xml_url": "",
        }
    ],
}

EDGAR_SEARCH_RESPONSE = {
    "hits": {
        "total": {"value": 2},
        "hits": [
            {
                "_source": {
                    "adsh": "0001234567-24-000001",
                    "display_names": ["HCA Healthcare Inc"],
                    "ciks": ["0000860730"],
                    "form": "10-K",
                    "file_date": "2024-02-15",
                }
            },
            {
                "_source": {
                    "adsh": "0001234567-24-000002",
                    "display_names": ["Tenet Healthcare Corp"],
                    "ciks": ["0000070858"],
                    "form": "10-K",
                    "file_date": "2024-02-20",
                }
            },
        ],
    }
}


# ---------------------------------------------------------------------------
# Tests: search_form990
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_form990_returns_results():
    with (
        patch.object(propublica_client, "search_organizations", new_callable=AsyncMock,
                     return_value=PROPUBLICA_SEARCH_RESPONSE),
        patch.object(propublica_client, "get_organization", new_callable=AsyncMock,
                     return_value=PROPUBLICA_ORG_DETAIL),
    ):
        result = parse_tool_result(await server.search_form990("Jefferson"))

    assert result["total_results"] == 2
    assert len(result["organizations"]) == 2
    org = result["organizations"][0]
    assert "ein" in org
    assert "name" in org
    assert "total_revenue" in org
    assert org["total_revenue"] > 0


@pytest.mark.asyncio
async def test_search_form990_empty_results():
    with (
        patch.object(propublica_client, "search_organizations", new_callable=AsyncMock,
                     return_value={"total_results": 0, "organizations": []}),
    ):
        result = parse_tool_result(await server.search_form990("zzznonexistent"))
    assert result["total_results"] == 0
    assert result["organizations"] == []


@pytest.mark.asyncio
async def test_search_form990_api_failure():
    with patch.object(propublica_client, "search_organizations", new_callable=AsyncMock,
                      side_effect=Exception("Connection refused")):
        result = parse_tool_result(await server.search_form990("Jefferson"))
    assert "error" in result


@pytest.mark.asyncio
async def test_search_form990_state_filter():
    """State filter is passed through to ProPublica; results are whatever the mock returns."""
    with (
        patch.object(propublica_client, "search_organizations", new_callable=AsyncMock,
                     return_value=PROPUBLICA_SEARCH_RESPONSE) as mock_search,
        patch.object(propublica_client, "get_organization", new_callable=AsyncMock,
                     return_value=PROPUBLICA_ORG_DETAIL),
    ):
        await server.search_form990("health system", state="PA")

    # Ensure state was forwarded to the client
    call_kwargs = mock_search.call_args
    assert call_kwargs.kwargs.get("state") == "PA" or (len(call_kwargs.args) > 1 and call_kwargs.args[1] == "PA")


# ---------------------------------------------------------------------------
# Tests: search_sec_filings
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_sec_filings_returns_results():
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock,
                      return_value=EDGAR_SEARCH_RESPONSE):
        result = parse_tool_result(await server.search_sec_filings("HCA Healthcare"))

    assert result["total_results"] == 2
    assert len(result["filings"]) == 2
    filing = result["filings"][0]
    assert filing["accession_number"] == "0001234567-24-000001"
    assert "HCA" in filing["company_name"]
    assert filing["form_type"] == "10-K"
    assert "filing_url" in filing
    assert filing["filing_url"].startswith("https://www.sec.gov/")


@pytest.mark.asyncio
async def test_search_sec_filings_deduplicates_accession_numbers():
    """Duplicate adsh entries in raw hits should be deduplicated."""
    duplicate_response = {
        "hits": {
            "total": {"value": 3},
            "hits": [
                {
                    "_source": {
                        "adsh": "0001234567-24-000001",
                        "display_names": ["HCA Healthcare Inc"],
                        "ciks": ["0000860730"],
                        "form": "10-K",
                        "file_date": "2024-02-15",
                    }
                },
                {
                    # Same accession number — should be skipped
                    "_source": {
                        "adsh": "0001234567-24-000001",
                        "display_names": ["HCA Healthcare Inc"],
                        "ciks": ["0000860730"],
                        "form": "10-K",
                        "file_date": "2024-02-15",
                    }
                },
            ],
        }
    }
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock,
                      return_value=duplicate_response):
        result = parse_tool_result(await server.search_sec_filings("HCA"))

    assert len(result["filings"]) == 1


@pytest.mark.asyncio
async def test_search_sec_filings_api_failure():
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock,
                      side_effect=Exception("EDGAR rate limit")):
        result = parse_tool_result(await server.search_sec_filings("SomeCo"))
    assert "error" in result


@pytest.mark.asyncio
async def test_search_sec_filings_empty_hits():
    empty_response = {"hits": {"total": {"value": 0}, "hits": []}}
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock,
                      return_value=empty_response):
        result = parse_tool_result(await server.search_sec_filings("NonexistentCorpXYZ"))
    assert result["total_results"] == 0
    assert result["filings"] == []


def test_parse_audited_financial_pdf_extracts_jefferson_fy2025_golden_values(monkeypatch):
    pages = [
        (
            7,
            """
            Assets 2025 2024
            Cash and cash equivalents $406,139 $562,202
            Total current assets 4,213,628 3,024,479
            Total assets $18,291,623 $11,833,679
            Total current liabilities 2,810,768 2,079,850
            Long-term obligations 5,182,444 3,111,618
            Total liabilities 9,811,297 6,391,477
            Total net assets without donor restriction 6,807,638 4,256,347
            Total net assets 8,480,326 5,442,202
            Thomas Jefferson University
            Consolidated Balance Sheets
            June 30, 2025 and 2024
            (In Thousands)
            """,
        ),
        (
            8,
            """
            2025 2024
            Net patient service revenue $11,054,287 $6,384,366
            Insurance premium revenue 2,225,203 2,163,755
            Total operating revenues, gains and other support 15,755,857 9,999,656
            Total operating expenses 15,964,163 9,998,314
            (Loss) Income from operations (208,306) 1,342
            Thomas Jefferson University
            Consolidated Statements of Operations and Changes in Net Assets without Donor Restrictions
            For the Years Ended June 30, 2025 and 2024
            (In Thousands)
            """,
        ),
    ]
    monkeypatch.setattr(
        audited_financial_pdf,
        "_extract_pdf_pages",
        lambda url_or_path: (pages, "https://example.org/tju-federal-ug-report-2025.pdf"),
    )

    result = audited_financial_pdf.parse_audited_financial_pdf(
        "https://example.org/tju-federal-ug-report-2025.pdf",
        "Thomas Jefferson University",
        2025,
    )

    assert result["scale"] == "thousands"
    assert result["metrics"]["total_assets"] == 18_291_623_000
    assert result["metrics"]["total_liabilities"] == 9_811_297_000
    assert result["metrics"]["total_net_assets"] == 8_480_326_000
    assert result["metrics"]["net_patient_service_revenue"] == 11_054_287_000
    assert result["metrics"]["total_operating_revenues"] == 15_755_857_000
    assert result["metrics"]["total_operating_expenses"] == 15_964_163_000
    assert result["metrics"]["operating_income_loss"] == -208_306_000
    assert result["citations"]["total_assets"]["page"] == 7
    assert result["citations"]["operating_income_loss"]["page"] == 8
    assert result["page_anchors"]["balance_sheet"]["page"] == 7
    assert result["page_anchors"]["operations"]["page"] == 8


@pytest.mark.asyncio
async def test_search_muni_bonds_includes_source_urls():
    muni_response = {
        "hits": {
            "total": {"value": 1},
            "hits": [
                {
                    "_source": {
                        "adsh": "0001193125-25-123456",
                        "display_names": ["Jefferson Health Obligated Group"],
                        "ciks": ["0001193125"],
                        "form": "OS",
                        "file_date": "2025-10-28",
                        "biz_states": ["PA"],
                    }
                }
            ],
        }
    }
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock, return_value=muni_response):
        result = parse_tool_result(await server.search_muni_bonds("Jefferson obligated group", state="PA"))

    bond = result["bonds"][0]
    assert bond["accession_number"] == "0001193125-25-123456"
    assert bond["source_url"] == bond["filing_url"]
    assert bond["source_url"].startswith("https://www.sec.gov/Archives/edgar/data/")


@pytest.mark.asyncio
async def test_get_muni_bond_details_bounds_disclosures_and_rejects_html_only():
    with (
        patch.object(edgar_client, "get_cik_from_accession", new_callable=AsyncMock, return_value="1193125"),
        patch.object(
            edgar_client,
            "get_company_submissions",
            new_callable=AsyncMock,
            return_value={
                "name": "Jefferson Health Obligated Group",
                "filings": {"recent": {"accessionNumber": ["0001193125-25-123456"], "filingDate": ["2025-10-28"]}},
            },
        ),
        patch.object(
            edgar_client,
            "get_filing_index",
            new_callable=AsyncMock,
            return_value={
                "source_url": "https://www.sec.gov/Archives/edgar/data/1193125/000119312525123456/0001193125-25-123456-index.htm",
                "description": "OS",
                "documents": [
                    {"name": "index.html", "url": "https://www.sec.gov/index.html", "type": "HTML"},
                ],
            },
        ),
    ):
        html_only = await server.get_muni_bond_details("0001193125-25-123456")

    assert html_only["ok"] is False
    assert html_only["error"]["code"] == "source_unparsed"

    docs = [
        {"name": f"doc-{idx}.pdf", "url": f"https://www.sec.gov/doc-{idx}.pdf", "type": "PDF"}
        for idx in range(30)
    ]
    docs[0]["description"] = "Official Statement"
    with (
        patch.object(edgar_client, "get_cik_from_accession", new_callable=AsyncMock, return_value="1193125"),
        patch.object(
            edgar_client,
            "get_company_submissions",
            new_callable=AsyncMock,
            return_value={
                "name": "Jefferson Health Obligated Group",
                "filings": {"recent": {"accessionNumber": ["0001193125-25-123456"], "filingDate": ["2025-10-28"]}},
            },
        ),
        patch.object(
            edgar_client,
            "get_filing_index",
            new_callable=AsyncMock,
            return_value={
                "source_url": "https://www.sec.gov/Archives/edgar/data/1193125/000119312525123456/0001193125-25-123456-index.htm",
                "description": "OS",
                "documents": docs,
            },
        ),
    ):
        result = parse_tool_result(await server.get_muni_bond_details("0001193125-25-123456"))

    assert result["disclosure_count"] == 25
    assert len(result["documents"]) == 25
    assert result["official_statement_url"] == "https://www.sec.gov/doc-0.pdf"
    assert all(doc["source_url"] for doc in result["documents"])
