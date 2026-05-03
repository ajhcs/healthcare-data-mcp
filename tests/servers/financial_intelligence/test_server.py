"""Tests for the financial-intelligence MCP server tools.

Uses monkeypatching to avoid live ProPublica/EDGAR API calls.
"""

from tests.helpers import parse_tool_result
import os
from unittest.mock import AsyncMock, patch
from pathlib import Path
import zipfile

import pytest

# SEC_USER_AGENT must be set before the edgar_client module is imported, because
# it raises RuntimeError at module level when the var is missing.
os.environ.setdefault("SEC_USER_AGENT", "CI ci@example.com")

from servers.financial_intelligence import audited_financial_pdf, server, propublica_client, edgar_client  # noqa: E402
from servers.financial_intelligence.financial_health import load_ahrq_hfmd_profile  # noqa: E402
from servers.financial_intelligence.irs990_parser import parse_990_xml  # noqa: E402


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

PROHIBITED_MAP_KPI_FIELDS = {
    "clean_claim_rate",
    "denial_rate",
    "net_days_in_ar",
    "cost_to_collect",
    "dnfb",
    "aged_ar",
}


def _payload_has_any_key(payload, prohibited: set[str]) -> bool:
    if isinstance(payload, dict):
        return any(key in prohibited or _payload_has_any_key(value, prohibited) for key, value in payload.items())
    if isinstance(payload, list):
        return any(_payload_has_any_key(item, prohibited) for item in payload)
    return False


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


def test_parse_990_xml_extracts_schedule_h_public_financial_fields(tmp_path: Path):
    xml = tmp_path / "return.xml"
    xml.write_text(
        """
        <Return>
          <ReturnData>
            <IRS990>
              <CYTotalExpensesAmt>1000000</CYTotalExpensesAmt>
            </IRS990>
            <IRS990ScheduleH>
              <TotalCommunityBenefitExpnsAmt>120000</TotalCommunityBenefitExpnsAmt>
              <CharityCareAtCostAmt>50000</CharityCareAtCostAmt>
              <BadDebtExpenseAmt>25000</BadDebtExpenseAmt>
              <MedicareShortfallAmt>30000</MedicareShortfallAmt>
              <MedicaidShortfallAmt>40000</MedicaidShortfallAmt>
            </IRS990ScheduleH>
          </ReturnData>
        </Return>
        """,
        encoding="utf-8",
    )

    parsed = parse_990_xml(xml)

    assert parsed["community_benefit_total"] == 120000
    assert parsed["charity_care_cost"] == 50000
    assert parsed["bad_debt_expense"] == 25000
    assert parsed["medicare_shortfall"] == 30000
    assert parsed["medicaid_shortfall"] == 40000
    assert parsed["community_benefit_pct"] == 12.0


@pytest.mark.asyncio
async def test_hcris_s10_alias_mapping_uses_realistic_fixture_fields(monkeypatch):
    async def fake_load_cost_report_row(_loaders, ccn: str):
        return (
            {
                "Provider CCN": ccn,
                "FY End Date": "2024-12-31",
                "Hospital Beds": "250",
                "Total Discharges": "12,345",
                "Total Inpatient Days": "67,890",
                "Net Patient Service Revenue": "987,654,321",
                "Operating Margin %": "4.5",
                "Total Margin %": "5.6",
                "Worksheet S-10 Total Uncompensated Care Cost": "$12,000,000",
                "S-10 Charity Care Cost": "$7,500,000",
                "S-10 Bad Debt Expense": "$2,250,000",
                "Medicaid Shortfall": "(1500000)",
                "Medicare Shortfall": "3000000",
            },
            "",
        )

    monkeypatch.setattr(server, "load_cost_report_row", fake_load_cost_report_row)

    result = await server._cost_report_public_metrics("390001")

    assert result["ccn"] == "390001"
    assert result["fiscal_year_end"] == "2024-12-31"
    assert result["beds"] == 250
    assert result["discharges"] == 12345
    assert result["net_patient_revenue"] == 987_654_321
    assert result["uncompensated_care_cost"] == 12_000_000
    assert result["charity_care_cost"] == 7_500_000
    assert result["bad_debt_expense"] == 2_250_000
    assert result["medicaid_shortfall"] == -1_500_000
    assert result["metrics"]["uncompensated_care_cost"]["confidence"] == "high_reported_hcris_field"
    assert result["metrics"]["uncompensated_care_cost"]["source_field"] == "Worksheet S-10 Total Uncompensated Care Cost"
    assert result["metric_confidence"]["bad_debt_expense"] == "high_reported_hcris_field"
    assert not _payload_has_any_key(result, PROHIBITED_MAP_KPI_FIELDS)


def test_ahrq_hfmd_zip_cache_parser_normalizes_and_filters_map_kpis(tmp_path: Path):
    cache = tmp_path / "state-health-data" / "ahrq-hfmd"
    cache.mkdir(parents=True)
    zip_path = cache / "hfmd.zip"
    csv_text = (
        "Provider ID,Hospital Name,State,Fiscal Year,Operating Margin %,Total Margin %,"
        "Net Patient Service Revenue,Days in Accounts Receivable,Net Days in AR,Denial Rate,"
        "DNFB,Clean Claim Rate,Cost to Collect,Aged AR\n"
        "390001,Jefferson Health,PA,2024,4.5,5.6,987654321,42,41,3.2,1000000,98.1,2.1,15\n"
    )
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("HFMD_2024.csv", csv_text)

    result = load_ahrq_hfmd_profile(ccn="390001", state="PA", cache_root=tmp_path)

    assert result["source_status"] == "ready"
    assert result["matched_count"] == 1
    assert result["matched_on"] == "ccn"
    assert result["join_keys"]["hfmd_provider_id"] == "390001"
    assert result["metrics"]["operating_margin"]["value"] == 4.5
    assert result["metrics"]["operating_margin"]["confidence"] == "high_reported_hfmd_ccn_match"
    assert result["metrics"]["net_patient_revenue"]["value"] == 987_654_321
    assert not _payload_has_any_key(result, PROHIBITED_MAP_KPI_FIELDS)


def test_ahrq_hfmd_csv_cache_parser_supports_state_fallback(tmp_path: Path):
    cache = tmp_path / "state-health-data" / "ahrq-hfmd"
    cache.mkdir(parents=True)
    (cache / "hfmd.csv").write_text(
        (
            "Medicare Provider Number,Hospital Name,State,Fiscal Year,Current Ratio,Days Cash on Hand\n"
            "390002,Temple Health,PA,2024,1.8,143\n"
        ),
        encoding="utf-8",
    )

    result = load_ahrq_hfmd_profile(state="PA", cache_root=tmp_path)

    assert result["source_status"] == "ready"
    assert result["matched_count"] == 1
    assert result["matched_on"] == "state"
    assert result["metrics"]["current_ratio"]["value"] == 1.8
    assert result["metrics"]["days_cash_on_hand"]["confidence"] == "medium_reported_hfmd_field"


@pytest.mark.asyncio
async def test_public_financial_health_profile_joins_sources_and_omits_map_kpis(monkeypatch):
    async def fake_hcris(_ccn: str):
        return {
            "ccn": "390001",
            "source_status": "ready",
            "net_patient_revenue": 987_654_321,
            "metrics": {
                "net_patient_revenue": {
                    "value": 987_654_321,
                    "confidence": "high_reported_hcris_field",
                    "source_field": "Net Patient Service Revenue",
                }
            },
            "metric_confidence": {"net_patient_revenue": "high_reported_hcris_field"},
        }

    async def fake_form990(_ein: str):
        return {
            "ein": "231352166",
            "source_status": "ready",
            "community_benefit_total": 120_000_000,
            "metrics": {
                "community_benefit_total": {
                    "value": 120_000_000,
                    "confidence": "high_reported_irs_schedule_h_xml",
                    "source_field": "TotalCommunityBenefitExpnsAmt",
                }
            },
            "metric_confidence": {"community_benefit_total": "high_reported_irs_schedule_h_xml"},
        }

    def fake_hfmd(**_kwargs):
        return {
            "source_status": "ready",
            "matched_on": "ccn",
            "join_keys": {"hfmd_provider_id": "390001"},
            "metrics": {
                "operating_margin": {
                    "value": 4.5,
                    "confidence": "high_reported_hfmd_ccn_match",
                    "source_field": "Operating Margin %",
                }
            },
            "metric_confidence": {"operating_margin": "high_reported_hfmd_ccn_match"},
        }

    monkeypatch.setattr(server, "_cost_report_public_metrics", fake_hcris)
    monkeypatch.setattr(server, "_latest_990_schedule_h", fake_form990)
    monkeypatch.setattr(server, "load_ahrq_hfmd_profile", fake_hfmd)

    result = parse_tool_result(
        await server.get_public_financial_health_profile(ccn="390001", ein="231352166", state="PA")
    )

    assert result["join_summary"]["hcris_hfmd_joined"] is True
    assert result["join_summary"]["joined_on"] == "ccn"
    assert result["metric_confidence"]["hcris"]["net_patient_revenue"] == "high_reported_hcris_field"
    assert result["metric_confidence"]["form990_schedule_h"]["community_benefit_total"] == "high_reported_irs_schedule_h_xml"
    assert result["metric_confidence"]["ahrq_hfmd"]["operating_margin"] == "high_reported_hfmd_ccn_match"
    assert "source_policy" in result
    assert not _payload_has_any_key(result, PROHIBITED_MAP_KPI_FIELDS)


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
