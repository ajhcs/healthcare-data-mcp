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
from shared.utils.mcp_response import validate_evidence_receipt  # noqa: E402
from shared.utils.source_backed_result import validate_source_claim_paths  # noqa: E402


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
    _assert_financial_evidence(org["evidence"], dataset_id="irs_form_990_search")
    assert org["evidence"]["match_basis"] == "form990_organization_search_row"
    assert org["evidence"]["source_url"].endswith("/231352166")
    _assert_financial_evidence(result["evidence"], dataset_id="irs_form_990_search")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "organization_name_or_ein_search"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_ein="231352166",
        expected_name="THOMAS JEFFERSON UNIVERSITY",
        expected_state="PA",
        expected_sources={"irs_form_990_search"},
    )
    claim = _financial_source_claim(result["identity_map"], "irs_form_990_search")
    assert claim["row_evidence_paths"] == ["organizations[].evidence"]


@pytest.mark.asyncio
async def test_search_form990_empty_results():
    with (
        patch.object(propublica_client, "search_organizations", new_callable=AsyncMock,
                     return_value={"total_results": 0, "organizations": []}),
    ):
        result = parse_tool_result(await server.search_form990("zzznonexistent"))
    assert result["total_results"] == 0
    assert result["organizations"] == []
    _assert_financial_evidence(result["evidence"], dataset_id="irs_form_990_search")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "organization_name_or_ein_search_no_match"
    assert result["evidence"]["confidence"] == "no_matching_public_form990_records_returned"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_name="ZZZNONEXISTENT",
        expected_sources={"irs_form_990_search"},
    )


@pytest.mark.asyncio
async def test_get_form990_details_not_found_has_evidence():
    with patch.object(propublica_client, "get_organization", new_callable=AsyncMock, return_value=None):
        result = await server.get_form990_details("999999999")

    assert result["ok"] is False
    assert result["error"]["code"] == "not_found"
    _assert_financial_evidence(result["evidence"], dataset_id="irs_form_990_detail")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "ein_exact_no_form990_organization_match"
    assert result["identity"]["unresolved_identifiers"] == [{"type": "ein", "value": "999999999"}]
    _assert_financial_identity_map(result["identity_map"], expected_ein="999999999")


@pytest.mark.asyncio
async def test_get_form990_details_no_filings_has_evidence():
    with patch.object(
        propublica_client,
        "get_organization",
        new_callable=AsyncMock,
        return_value={"organization": {"ein": "231352166", "name": "Example Health"}, "filings_with_data": []},
    ):
        result = await server.get_form990_details("231352166")

    assert result["ok"] is False
    assert result["error"]["code"] == "not_found"
    _assert_financial_evidence(result["evidence"], dataset_id="irs_form_990_detail")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "ein_exact_no_form990_filings_with_data"
    assert result["identity"]["canonical_name"] == "EXAMPLE HEALTH"
    _assert_financial_identity_map(result["identity_map"], expected_ein="231352166", expected_name="EXAMPLE HEALTH")


@pytest.mark.asyncio
async def test_get_form990_details_summary_success_has_identity_map():
    with (
        patch.object(propublica_client, "get_organization", new_callable=AsyncMock, return_value=PROPUBLICA_ORG_DETAIL),
        patch.object(server, "lookup_xml_url", new_callable=AsyncMock, return_value=""),
    ):
        result = parse_tool_result(await server.get_form990_details("231352166"))

    assert result["source"] == "propublica"
    _assert_financial_evidence(result["evidence"], dataset_id="propublica_form_990_summary")
    _assert_financial_source_metadata(result)
    assert result["identity"]["canonical_name"] == "THOMAS JEFFERSON UNIVERSITY"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_ein="231352166",
        expected_name="THOMAS JEFFERSON UNIVERSITY",
        expected_sources={"propublica_form_990_summary"},
    )


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
    _assert_financial_evidence(filing["evidence"], dataset_id="sec_edgar_filings_search")
    assert filing["evidence"]["match_basis"] == "sec_filing_search_row"
    assert filing["evidence"]["source_url"] == filing["filing_url"]
    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_filings_search")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "edgar_full_text_search"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_cik="860730",
        expected_accession="0001234567-24-000001",
        expected_sources={"sec_edgar_filings_search"},
    )


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
    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_filings_search")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "edgar_full_text_search_no_match"
    assert result["evidence"]["confidence"] == "no_matching_sec_filings_returned"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_name="NONEXISTENTCORPXYZ",
        expected_sources={"sec_edgar_filings_search"},
    )


@pytest.mark.asyncio
async def test_get_sec_filing_no_cik_has_evidence():
    with patch.object(edgar_client, "get_cik_from_accession", new_callable=AsyncMock, return_value=""):
        result = await server.get_sec_filing("0000000000-24-000001")

    assert result["ok"] is False
    assert result["error"]["code"] == "not_found"
    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_filing_detail")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "accession_number_no_cik_match"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_accession="0000000000-24-000001",
        expected_sources={"sec_edgar_filing_detail"},
    )


@pytest.mark.asyncio
async def test_get_sec_filing_success_has_identity_map():
    accession = "0001234567-24-000001"
    with (
        patch.object(edgar_client, "get_cik_from_accession", new_callable=AsyncMock, return_value="0000860730"),
        patch.object(
            edgar_client,
            "get_company_submissions",
            new_callable=AsyncMock,
            return_value={
                "name": "HCA Healthcare Inc",
                "filings": {
                    "recent": {
                        "accessionNumber": [accession],
                        "form": ["10-K"],
                        "filingDate": ["2024-02-15"],
                    }
                },
            },
        ),
        patch.object(edgar_client, "get_company_facts", new_callable=AsyncMock, return_value={}),
        patch.object(edgar_client, "extract_financials", return_value={"revenue": 1_000_000}),
    ):
        result = parse_tool_result(await server.get_sec_filing(accession, sections=["financials"]))

    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_filing_detail")
    _assert_financial_source_metadata(result)
    assert result["company_name"] == "HCA Healthcare Inc"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_cik="860730",
        expected_accession=accession,
        expected_name="HCA HEALTHCARE",
        expected_source_url=result["evidence"]["source_url"],
        expected_sources={"sec_edgar_filing_detail"},
    )


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
async def test_parse_audited_financial_pdf_tool_adds_evidence_and_identity_map(monkeypatch):
    def fake_parse(url_or_path: str, entity_name: str, fiscal_year: int | str):
        return {
            "entity_name": entity_name,
            "fiscal_year": str(fiscal_year),
            "source_url": url_or_path,
            "metrics": {"total_assets": 18_291_623_000},
            "citations": {"total_assets": {"page": 7}},
        }

    monkeypatch.setattr(server, "_parse_audited_financial_pdf", fake_parse)

    result = parse_tool_result(
        await server.parse_audited_financial_pdf(
            "https://example.org/tju-audited-financials-2025.pdf",
            "Thomas Jefferson University",
            2025,
        )
    )

    _assert_financial_evidence(result["evidence"], dataset_id="audited_financial_statement_pdf")
    _assert_financial_source_metadata(result)
    assert result["identity"]["canonical_name"] == "THOMAS JEFFERSON UNIVERSITY"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_name="THOMAS JEFFERSON UNIVERSITY",
        expected_source_url="https://example.org/tju-audited-financials-2025.pdf",
        expected_sources={"audited_financial_statement_pdf"},
    )


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
async def test_uncompensated_care_profile_falls_back_to_schedule_h_fields(monkeypatch):
    async def fake_cost_report_public_metrics(ccn: str):
        return {"ccn": ccn, "source_status": "unavailable", "metric_confidence": {}}

    async def fake_latest_990_schedule_h(ein: str):
        return {
            "ein": ein,
            "source_status": "ready",
            "tax_period": "2023",
            "xml_url": "https://example.org/form990.xml",
            "charity_care": 50_000,
            "bad_debt_expense": 25_000,
            "medicare_shortfall": 30_000,
            "medicaid_shortfall": 40_000,
            "metrics": {
                "charity_care_cost": {
                    "value": 50_000,
                    "confidence": "high_reported_irs_schedule_h_xml",
                    "source_field": "CharityCareAtCostAmt",
                },
                "bad_debt_expense": {
                    "value": 25_000,
                    "confidence": "high_reported_irs_schedule_h_xml",
                    "source_field": "BadDebtExpenseAmt",
                },
            },
            "metric_confidence": {
                "charity_care": "high_reported_irs_schedule_h_xml",
                "bad_debt_expense": "high_reported_irs_schedule_h_xml",
                "medicare_shortfall": "high_reported_irs_schedule_h_xml",
                "medicaid_shortfall": "high_reported_irs_schedule_h_xml",
            },
        }

    monkeypatch.setattr(server, "_cost_report_public_metrics", fake_cost_report_public_metrics)
    monkeypatch.setattr(server, "_latest_990_schedule_h", fake_latest_990_schedule_h)

    result = await server.get_uncompensated_care_profile(ccn="390001", ein="231352651")

    assert result["charity_care_cost"] == 50_000
    assert result["bad_debt_expense"] == 25_000
    assert result["medicare_shortfall"] == 30_000
    assert result["medicaid_shortfall"] == 40_000
    assert result["metric_confidence"]["bad_debt_expense"] == "high_reported_irs_schedule_h_xml"
    _assert_financial_evidence(result["evidence"], dataset_id="public_financial_health_profile")
    _assert_financial_source_metadata(result)
    assert set(result["metric_evidence"]) == {"charity_care_cost", "bad_debt_expense"}
    _assert_financial_evidence(result["metric_evidence"]["charity_care_cost"], dataset_id="irs_form_990_schedule_h")
    assert result["metric_evidence"]["charity_care_cost"]["query"]["promoted_metric_name"] == "charity_care_cost"
    assert result["metric_evidence"]["charity_care_cost"]["query"]["selected_source_metric"] == "charity_care"
    _assert_financial_evidence(result["metric_evidence"]["bad_debt_expense"], dataset_id="irs_form_990_schedule_h")
    assert result["identity"]["ccn"] == "390001"
    assert {"type": "ein", "value": "231352651"} in result["identity"]["unresolved_identifiers"]
    _assert_financial_source_block(
        result["sources"]["hcris"],
        dataset_id="cms_hcris_public_cost_report",
        match_basis="ccn_hcris_public_cost_report_unavailable",
    )
    _assert_financial_source_block(
        result["sources"]["form990_schedule_h"],
        dataset_id="irs_form_990_schedule_h",
        match_basis="ein_exact_irs_schedule_h_xml",
    )
    _assert_financial_identity_map(result["identity_map"], expected_ccn="390001", expected_ein="231352651")
    selected_claim = _financial_source_claim(result["identity_map"], "selected_public_financial_metrics")
    assert selected_claim["metric_evidence_paths"] == ["metric_evidence.*"]


@pytest.mark.asyncio
async def test_charity_care_profile_exposes_nested_source_receipts(monkeypatch):
    async def fake_cost_report_public_metrics(ccn: str):
        return {
            "ccn": ccn,
            "source_status": "ready",
            "charity_care_cost": 75_000,
            "metric_confidence": {"charity_care_cost": "high_reported_hcris_field"},
            "metrics": {
                "charity_care_cost": {
                    "value": 75_000,
                    "confidence": "high_reported_hcris_field",
                    "source_field": "S-10 Charity Care Cost",
                }
            },
        }

    async def fake_latest_990_schedule_h(ein: str):
        return {
            "ein": ein,
            "source_status": "ready",
            "tax_period": "2023",
            "xml_url": "https://example.org/form990.xml",
            "community_benefit_pct": 12.5,
            "total_expenses": 1_000_000,
            "metric_confidence": {
                "community_benefit_pct": "medium_derived_from_schedule_h_total_expenses",
                "total_expenses": "high_reported_irs_xml_or_propublica_summary",
            },
            "metrics": {
                "community_benefit_pct": {
                    "value": 12.5,
                    "confidence": "medium_derived_from_schedule_h_total_expenses",
                    "source_field": "TotalCommunityBenefitExpnsAmt / CYTotalExpensesAmt",
                }
            },
        }

    monkeypatch.setattr(server, "_cost_report_public_metrics", fake_cost_report_public_metrics)
    monkeypatch.setattr(server, "_latest_990_schedule_h", fake_latest_990_schedule_h)

    result = await server.get_charity_care_profile(ccn="390001", ein="231352651")

    assert result["charity_care_cost"] == 75_000
    assert result["community_benefit_pct"] == 12.5
    _assert_financial_evidence(result["evidence"], dataset_id="public_financial_health_profile")
    _assert_financial_source_metadata(result)
    assert set(result["metric_evidence"]) == {"charity_care_cost", "community_benefit_pct"}
    _assert_financial_evidence(result["metric_evidence"]["charity_care_cost"], dataset_id="cms_hcris_public_cost_report")
    assert result["metric_evidence"]["charity_care_cost"]["query"]["promoted_metric_name"] == "charity_care_cost"
    assert result["metric_evidence"]["charity_care_cost"]["query"]["selected_source_metric"] == "charity_care_cost"
    _assert_financial_evidence(result["metric_evidence"]["community_benefit_pct"], dataset_id="irs_form_990_schedule_h")
    _assert_financial_source_block(
        result["sources"]["hcris"],
        dataset_id="cms_hcris_public_cost_report",
        match_basis="ccn_exact_hcris_public_cost_report",
    )
    _assert_financial_source_block(
        result["sources"]["form990_schedule_h"],
        dataset_id="irs_form_990_schedule_h",
        match_basis="ein_exact_irs_schedule_h_xml",
    )
    _assert_financial_identity_map(result["identity_map"], expected_ccn="390001", expected_ein="231352651")


@pytest.mark.asyncio
async def test_bad_debt_profile_falls_back_to_schedule_h(monkeypatch):
    async def fake_cost_report_public_metrics(ccn: str):
        return {"ccn": ccn, "source_status": "unavailable", "metric_confidence": {}}

    async def fake_latest_990_schedule_h(ein: str):
        return {
            "ein": ein,
            "source_status": "ready",
            "tax_period": "2023",
            "xml_url": "https://example.org/form990.xml",
            "bad_debt_expense": 25_000,
            "metrics": {
                "bad_debt_expense": {
                    "value": 25_000,
                    "confidence": "high_reported_irs_schedule_h_xml",
                    "source_field": "BadDebtExpenseAmt",
                }
            },
            "metric_confidence": {"bad_debt_expense": "high_reported_irs_schedule_h_xml"},
        }

    monkeypatch.setattr(server, "_cost_report_public_metrics", fake_cost_report_public_metrics)
    monkeypatch.setattr(server, "_latest_990_schedule_h", fake_latest_990_schedule_h)

    result = await server.get_bad_debt_profile(ccn="390001", ein="231352651")

    assert result["bad_debt_expense"] == 25_000
    assert result["metric_confidence"]["bad_debt_expense"] == "high_reported_irs_schedule_h_xml"
    _assert_financial_evidence(result["evidence"], dataset_id="public_financial_health_profile")
    _assert_financial_source_metadata(result)
    assert set(result["metric_evidence"]) == {"bad_debt_expense"}
    _assert_financial_evidence(result["metric_evidence"]["bad_debt_expense"], dataset_id="irs_form_990_schedule_h")
    assert result["metric_evidence"]["bad_debt_expense"]["query"]["promoted_metric_name"] == "bad_debt_expense"
    assert result["metric_evidence"]["bad_debt_expense"]["query"]["selected_source_metric"] == "bad_debt_expense"
    assert result["identity"]["ccn"] == "390001"
    assert result["identity"]["match_decisions"][0]["basis"] == "ccn_or_ein_public_source_lookup"
    _assert_financial_source_block(
        result["sources"]["hcris"],
        dataset_id="cms_hcris_public_cost_report",
        match_basis="ccn_hcris_public_cost_report_unavailable",
    )
    _assert_financial_source_block(
        result["sources"]["form990_schedule_h"],
        dataset_id="irs_form_990_schedule_h",
        match_basis="ein_exact_irs_schedule_h_xml",
    )
    _assert_financial_identity_map(result["identity_map"], expected_ccn="390001", expected_ein="231352651")


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
            "facility_name": "Example Hospital LLC",
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
    _assert_financial_evidence(result["evidence"], dataset_id="public_financial_health_profile")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "ccn_ein_public_source_join"
    _assert_financial_evidence(result["hcris"]["evidence"], dataset_id="cms_hcris_public_cost_report")
    assert result["hcris"]["evidence"]["match_basis"] == "ccn_exact_hcris_public_cost_report"
    assert result["hcris"]["source_metadata"]["dataset_id"] == "cms_hcris_public_cost_report"
    _assert_financial_metric_evidence(
        result["hcris"],
        metric_name="net_patient_revenue",
        dataset_id="cms_hcris_public_cost_report",
        match_basis="ccn_exact_hcris_public_cost_report_metric_net_patient_revenue",
    )
    _assert_financial_evidence(result["form990_schedule_h"]["evidence"], dataset_id="irs_form_990_schedule_h")
    assert result["form990_schedule_h"]["evidence"]["match_basis"] == "ein_exact_irs_schedule_h_xml"
    _assert_financial_metric_evidence(
        result["form990_schedule_h"],
        metric_name="community_benefit_total",
        dataset_id="irs_form_990_schedule_h",
        match_basis="ein_exact_irs_schedule_h_xml_metric_community_benefit_total",
    )
    _assert_financial_evidence(result["ahrq_hfmd"]["evidence"], dataset_id="ahrq_hfmd")
    assert result["ahrq_hfmd"]["evidence"]["match_basis"] == "ccn_exact_ahrq_hfmd_profile"
    _assert_financial_metric_evidence(
        result["ahrq_hfmd"],
        metric_name="operating_margin",
        dataset_id="ahrq_hfmd",
        match_basis="ccn_exact_ahrq_hfmd_profile_metric_operating_margin",
    )
    assert result["identity"]["ccn"] == "390001"
    assert result["identity"]["canonical_name"] == "EXAMPLE HOSPITAL"
    assert result["identity"]["match_decisions"][0]["basis"] == "ccn_ein_public_source_join"
    assert {"type": "ein", "value": "231352166"} in result["identity"]["unresolved_identifiers"]
    _assert_financial_identity_map(
        result["identity_map"],
        expected_ccn="390001",
        expected_ein="231352166",
        expected_name="EXAMPLE HOSPITAL",
        expected_sources={"hcris", "form990_schedule_h", "ahrq_hfmd"},
    )
    source_claims = {claim["collection"]: claim for claim in result["identity_map"]["source_claims"]}
    assert "hcris.metric_evidence.*" in source_claims["hcris"]["metric_evidence_paths"]
    assert "form990_schedule_h.metric_evidence.*" in source_claims["form990_schedule_h"]["metric_evidence_paths"]
    assert "ahrq_hfmd.metric_evidence.*" in source_claims["ahrq_hfmd"]["metric_evidence_paths"]
    assert not _payload_has_any_key(result, PROHIBITED_MAP_KPI_FIELDS)


@pytest.mark.asyncio
async def test_public_financial_health_profile_nested_no_match_sources_have_evidence(monkeypatch):
    async def fake_hcris(_ccn: str):
        return {"ccn": "390999", "source_status": "unavailable", "detail": "No cost-report row found"}

    async def fake_form990(_ein: str):
        return {"ein": "999999999", "source_status": "no_990_filing_found"}

    def fake_hfmd(**_kwargs):
        return {
            "source_id": "ahrq_hfmd",
            "source_name": "AHRQ Hospital Financial Measures Database",
            "source_url": "https://www.ahrq.gov/data/innovations/hfmd.html",
            "source_status": "no_match",
            "cache_path": "/tmp/hfmd",
            "record_count": 10,
            "matched_count": 0,
            "metrics": {},
            "metric_confidence": {},
            "records": [],
        }

    monkeypatch.setattr(server, "_cost_report_public_metrics", fake_hcris)
    monkeypatch.setattr(server, "_latest_990_schedule_h", fake_form990)
    monkeypatch.setattr(server, "load_ahrq_hfmd_profile", fake_hfmd)

    result = parse_tool_result(
        await server.get_public_financial_health_profile(ccn="390999", ein="999999999", state="PA")
    )

    assert result["hcris"]["source_status"] == "unavailable"
    assert result["hcris"]["evidence"]["match_basis"] == "ccn_hcris_public_cost_report_unavailable"
    assert result["hcris"]["evidence"]["confidence"] == "not_evaluated_or_no_reported_hcris_fields"
    _assert_financial_evidence(result["hcris"]["evidence"], dataset_id="cms_hcris_public_cost_report")
    assert result["form990_schedule_h"]["evidence"]["match_basis"] == "ein_no_form990_filing_found"
    _assert_financial_evidence(result["form990_schedule_h"]["evidence"], dataset_id="irs_form_990_schedule_h")
    assert result["ahrq_hfmd"]["evidence"]["match_basis"] == "ahrq_hfmd_no_match"
    _assert_financial_evidence(result["ahrq_hfmd"]["evidence"], dataset_id="ahrq_hfmd")


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
    _assert_financial_evidence(bond["evidence"], dataset_id="sec_edgar_municipal_bond_search")
    assert bond["evidence"]["match_basis"] == "municipal_bond_search_row"
    assert bond["evidence"]["source_url"] == bond["source_url"]
    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_municipal_bond_search")
    _assert_financial_source_metadata(result)
    _assert_financial_identity_map(
        result["identity_map"],
        expected_cik="1193125",
        expected_accession="0001193125-25-123456",
        expected_source_url=bond["source_url"],
        expected_state="PA",
        expected_sources={"sec_edgar_municipal_bond_search"},
    )


@pytest.mark.asyncio
async def test_search_muni_bonds_empty_results_have_no_match_evidence():
    with patch.object(
        edgar_client,
        "search_filings",
        new_callable=AsyncMock,
        return_value={"hits": {"total": {"value": 0}, "hits": []}},
    ):
        result = await server.search_muni_bonds("No Such Obligated Group")

    assert result["total_results"] == 0
    assert result["bonds"] == []
    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_municipal_bond_search")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["match_basis"] == "edgar_official_statement_search_no_match"
    assert result["evidence"]["confidence"] == "no_matching_municipal_bond_filings_returned"
    _assert_financial_identity_map(
        result["identity_map"],
        expected_name="NO SUCH OBLIGATED GROUP",
        expected_sources={"sec_edgar_municipal_bond_search"},
    )


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
    _assert_financial_evidence(html_only["evidence"], dataset_id="sec_edgar_municipal_bond_detail")
    assert html_only["evidence"]["match_basis"] == "accession_number_exact_no_parseable_disclosure_documents"
    _assert_financial_identity_map(
        html_only["identity_map"],
        expected_accession="0001193125-25-123456",
        expected_sources={"sec_edgar_municipal_bond_detail"},
    )

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
    _assert_financial_evidence(result["documents"][0]["evidence"], dataset_id="sec_edgar_municipal_bond_detail")
    assert result["documents"][0]["evidence"]["match_basis"] == "municipal_disclosure_document_row"
    assert result["documents"][0]["evidence"]["source_url"] == "https://www.sec.gov/doc-0.pdf"
    _assert_financial_evidence(result["evidence"], dataset_id="sec_edgar_municipal_bond_detail")
    _assert_financial_source_metadata(result)
    assert result["evidence"]["source_url"].endswith("-index.htm")
    _assert_financial_identity_map(
        result["identity_map"],
        expected_accession="0001193125-25-123456",
        expected_source_url=result["source_url"],
        expected_sources={"sec_edgar_municipal_bond_detail"},
    )
    claim = _financial_source_claim(result["identity_map"], "sec_edgar_municipal_bond_detail")
    assert claim["row_evidence_paths"] == ["documents[].evidence"]


def _assert_financial_evidence(evidence: dict, *, dataset_id: str) -> None:
    validate_evidence_receipt(evidence, require_content=True)
    assert evidence["dataset_id"] == dataset_id
    assert evidence["source_name"]
    assert evidence["source_url"]
    assert evidence["source_period"]
    assert evidence["retrieved_at"]
    assert evidence["cache_status"]
    assert evidence["cache_freshness"]
    assert evidence["entity_scope"] == "facility_or_nonprofit_finance"
    assert evidence["match_basis"]
    assert evidence["confidence"]
    assert evidence["caveat"]
    assert evidence["next_step"]


def _assert_financial_source_metadata(result: dict) -> None:
    metadata = result["source_metadata"]
    evidence = result["evidence"]

    assert metadata["source_name"] == evidence["source_name"]
    assert metadata["source_url"] == evidence["source_url"]
    assert metadata["dataset_id"] == evidence["dataset_id"]
    assert metadata["source_period"] == evidence["source_period"]
    assert metadata["landing_page"] == evidence["landing_page"]
    assert metadata["retrieved_at"] == evidence["retrieved_at"]
    assert metadata["source_modified"] == evidence["source_modified"]
    assert metadata["cache_status"] == evidence["cache_status"]
    assert metadata["cache_freshness"] == evidence["cache_freshness"]
    assert metadata["entity_scope"] == evidence["entity_scope"]
    assert metadata["query"] == evidence["query"]
    assert metadata["cache_key"] == evidence["cache_key"]
    assert metadata["source_type"] == "public_financial_source"
    assert validate_source_claim_paths(result, require_boundary_traceability=True)["valid"] is True


def _assert_financial_source_block(source: dict, *, dataset_id: str, match_basis: str) -> None:
    assert source["source_metadata"]["dataset_id"] == dataset_id
    assert source["source_metadata"]["source_name"]
    assert source["source_metadata"]["source_status"]
    _assert_financial_evidence(source["evidence"], dataset_id=dataset_id)
    assert source["evidence"]["match_basis"] == match_basis
    assert source["evidence"]["query"]["source_status"] == source["source_metadata"]["source_status"]
    assert "metric_evidence" in source
    metrics = source.get("metrics") if isinstance(source.get("metrics"), dict) else {}
    assert set(source["metric_evidence"]) == set(metrics)
    for metric_name in metrics:
        _assert_financial_metric_evidence(
            source,
            metric_name=metric_name,
            dataset_id=dataset_id,
            match_basis=f"{match_basis}_metric_{metric_name}",
        )


def _assert_financial_metric_evidence(
    source: dict,
    *,
    metric_name: str,
    dataset_id: str,
    match_basis: str,
) -> None:
    evidence = source["metric_evidence"][metric_name]
    _assert_financial_evidence(evidence, dataset_id=dataset_id)
    assert evidence["match_basis"] == match_basis
    assert evidence["query"]["metric_name"] == metric_name
    assert evidence["query"]["metric_source"]
    assert "metric_value_present" in evidence["query"]
    assert evidence["query"]["metric_confidence"]
    assert "source_field" in evidence["query"]
    assert "missing fields are not zero values" in evidence["caveat"]


def _assert_financial_identity_map(
    identity_map: dict,
    *,
    expected_ccn: str = "",
    expected_ein: str = "",
    expected_name: str = "",
    expected_state: str = "",
    expected_cik: str = "",
    expected_accession: str = "",
    expected_source_url: str = "",
    expected_sources: set[str] | None = None,
) -> None:
    by_field = {entry["field"]: entry for entry in identity_map["join_keys"]}

    assert identity_map["entity_scope"] == "facility_or_nonprofit_finance"
    assert identity_map["source_claims"]
    assert identity_map["conflict_policy"]
    assert identity_map["missing_data_policy"].startswith("No-match or missing financial-source responses")
    if expected_ccn:
        assert expected_ccn in by_field["ccn"]["values"]
        assert by_field["ccn"]["status"] == "provided"
    if expected_ein:
        assert expected_ein in by_field["ein"]["values"]
        assert by_field["ein"]["status"] == "provided"
    if expected_name:
        assert expected_name in by_field["canonical_name"]["values"]
    if expected_state:
        assert expected_state in by_field["state"]["values"]
    if expected_cik:
        assert expected_cik in by_field["cik"]["values"]
        assert by_field["cik"]["status"] == "provided"
    if expected_accession:
        assert expected_accession in by_field["accession_number"]["values"]
        assert by_field["accession_number"]["status"] == "provided"
    if expected_source_url:
        assert expected_source_url in by_field["source_url"]["values"]
        assert by_field["source_url"]["status"] == "provided"
    if expected_sources:
        assert {claim["collection"] for claim in identity_map["source_claims"]} >= expected_sources


def _financial_source_claim(identity_map: dict, collection: str) -> dict:
    claims = {claim["collection"]: claim for claim in identity_map["source_claims"]}
    return claims[collection]
