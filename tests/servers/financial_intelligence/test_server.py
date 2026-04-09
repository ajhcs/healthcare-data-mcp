"""Tests for the financial-intelligence MCP server tools.

Uses monkeypatching to avoid live ProPublica/EDGAR API calls.
"""

import json
import os
from unittest.mock import AsyncMock, patch

import pytest

# SEC_USER_AGENT must be set before the edgar_client module is imported, because
# it raises RuntimeError at module level when the var is missing.
os.environ.setdefault("SEC_USER_AGENT", "CI ci@example.com")

from servers.financial_intelligence import server, propublica_client, edgar_client  # noqa: E402


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
        result = json.loads(await server.search_form990("Jefferson"))

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
        result = json.loads(await server.search_form990("zzznonexistent"))
    assert result["total_results"] == 0
    assert result["organizations"] == []


@pytest.mark.asyncio
async def test_search_form990_api_failure():
    with patch.object(propublica_client, "search_organizations", new_callable=AsyncMock,
                      side_effect=Exception("Connection refused")):
        result = json.loads(await server.search_form990("Jefferson"))
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
        result = json.loads(await server.search_sec_filings("HCA Healthcare"))

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
        result = json.loads(await server.search_sec_filings("HCA"))

    assert len(result["filings"]) == 1


@pytest.mark.asyncio
async def test_search_sec_filings_api_failure():
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock,
                      side_effect=Exception("EDGAR rate limit")):
        result = json.loads(await server.search_sec_filings("SomeCo"))
    assert "error" in result


@pytest.mark.asyncio
async def test_search_sec_filings_empty_hits():
    empty_response = {"hits": {"total": {"value": 0}, "hits": []}}
    with patch.object(edgar_client, "search_filings", new_callable=AsyncMock,
                      return_value=empty_response):
        result = json.loads(await server.search_sec_filings("NonexistentCorpXYZ"))
    assert result["total_results"] == 0
    assert result["filings"] == []
