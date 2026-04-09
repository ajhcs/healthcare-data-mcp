"""Regression tests for financial-intelligence Form 990 search mapping."""

import json
import os

import pytest

os.environ.setdefault("SEC_USER_AGENT", "healthcare-data-mcp-tests test@example.com")

from servers.financial_intelligence import server


@pytest.mark.asyncio
async def test_search_form990_uses_latest_filing_financials(monkeypatch):
    async def fake_search_organizations(query, state="", ntee_code="", page=0):
        return {
            "total_results": 1,
            "organizations": [
                {
                    "ein": "340714585",
                    "name": "Cleveland Clinic",
                    "city": "Cleveland",
                    "state": "OH",
                    "ntee_code": "E220",
                    "income_amount": "10783069848",
                    "revenue_amount": "9680031802",
                    "asset_amount": "15354669845",
                }
            ],
        }

    async def fake_get_organization(ein):
        assert ein == "340714585"
        return {
            "organization": {
                "ein": "340714585",
                "name": "Cleveland Clinic",
                "revenue_amount": "9680031802",
                "asset_amount": "15354669845",
            },
            "filings_with_data": [
                {
                    "tax_prd": "202306",
                    "totrevenue": "7583607049",
                    "totfuncexpns": "7671629275",
                    "totnetassetend": "7683048561",
                    "totassetsend": "15354669845",
                }
            ],
        }

    monkeypatch.setattr(server.propublica_client, "search_organizations", fake_search_organizations)
    monkeypatch.setattr(server.propublica_client, "get_organization", fake_get_organization)

    payload = json.loads(await server.search_form990("Cleveland Clinic", state="OH"))
    org = payload["organizations"][0]

    assert payload["total_results"] == 1
    assert org["ein"] == "340714585"
    assert org["total_revenue"] == 7583607049.0
    assert org["total_expenses"] == 7671629275.0
    assert org["net_assets"] == 7683048561.0
    assert org["tax_period"] == "202306"


@pytest.mark.asyncio
async def test_search_form990_falls_back_to_org_level_when_filing_data_missing(monkeypatch):
    async def fake_search_organizations(query, state="", ntee_code="", page=0):
        return {
            "total_results": 1,
            "organizations": [
                {
                    "ein": "123456789",
                    "name": "Fallback Org",
                    "city": "Boston",
                    "state": "MA",
                    "ntee_code": "B200",
                }
            ],
        }

    async def fake_get_organization(ein):
        assert ein == "123456789"
        return {
            "organization": {
                "ein": "123456789",
                "name": "Fallback Org",
                "revenue_amount": "2500000",
                "asset_amount": "900000",
                "tax_period": "202212",
            },
            "filings_with_data": [],
        }

    monkeypatch.setattr(server.propublica_client, "search_organizations", fake_search_organizations)
    monkeypatch.setattr(server.propublica_client, "get_organization", fake_get_organization)

    payload = json.loads(await server.search_form990("Fallback Org"))
    org = payload["organizations"][0]

    assert org["total_revenue"] == 2500000.0
    assert org["total_expenses"] is None
    assert org["net_assets"] == 900000.0
    assert org["tax_period"] == "202212"


@pytest.mark.asyncio
async def test_search_form990_uses_empty_tax_period_when_missing(monkeypatch):
    async def fake_search_organizations(query, state="", ntee_code="", page=0):
        return {
            "total_results": 1,
            "organizations": [
                {
                    "ein": "555555555",
                    "name": "No Tax Period Org",
                    "city": "Seattle",
                    "state": "WA",
                    "ntee_code": "C300",
                }
            ],
        }

    async def fake_get_organization(ein):
        assert ein == "555555555"
        return {"organization": {"ein": "555555555", "name": "No Tax Period Org"}}

    monkeypatch.setattr(server.propublica_client, "search_organizations", fake_search_organizations)
    monkeypatch.setattr(server.propublica_client, "get_organization", fake_get_organization)

    payload = json.loads(await server.search_form990("No Tax Period Org"))
    org = payload["organizations"][0]

    assert org["tax_period"] == ""
