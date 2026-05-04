from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import pandas as pd

from shared import state_health_data
from shared.utils.cost_report import load_cost_report_row


@pytest.mark.asyncio
async def test_search_phc4_reports_uses_cached_index(tmp_path) -> None:
    cache = tmp_path / "state-health-data" / "phc4"
    cache.mkdir(parents=True)
    (cache / "report_index.json").write_text(
        json.dumps(
            [
                {
                    "title": "Hospital Performance Report 2024",
                    "url": "https://www.phc4.org/example.pdf",
                    "year": 2024,
                    "report_type": "hospital_performance",
                },
                {
                    "title": "Financial Analysis Fiscal Year 2023",
                    "url": "https://www.phc4.org/financial.pdf",
                    "year": 2023,
                    "report_type": "financial_analysis",
                },
            ]
        ),
        encoding="utf-8",
    )

    result = await state_health_data.search_phc4_reports(
        "Hospital Performance",
        year="2024",
        report_type="hospital_performance",
        cache_root=tmp_path,
    )

    assert result["total_results"] == 1
    assert result["reports"][0]["title"] == "Hospital Performance Report 2024"


def test_extract_structured_tables_from_csv_records_provenance(tmp_path) -> None:
    artifact = tmp_path / "common-procedures.csv"
    artifact.write_text("hospital,procedure,volume\nExample Hospital,Knee Replacement,42\n", encoding="utf-8")

    refs = state_health_data._extract_structured_tables(artifact, tmp_path / "cache")

    assert refs
    assert refs[0]["table_index"] == 1
    assert refs[0]["provenance"]["source_artifact"] == str(artifact)
    extracted = refs[0]["extracted_path"]
    assert "common-procedures-table-1.json" in extracted


def test_state_artifact_index_writes_normalized_json_and_metadata_table(tmp_path) -> None:
    artifact = state_health_data._normalized_artifact_record(
        source_id="pa_hospital_reports",
        source_name="Pennsylvania DOH Hospital Reports",
        source_url=state_health_data.PA_HOSPITAL_REPORTS_URL,
        title="2024 Hospital Questionnaire Directory PDF",
        artifact_url="https://www.pa.gov/content/dam/example/hospital-2024.pdf",
    )

    index_path = state_health_data._write_artifact_indexes(
        tmp_path,
        source_id="pa_hospital_reports",
        source_name="Pennsylvania DOH Hospital Reports",
        source_url=state_health_data.PA_HOSPITAL_REPORTS_URL,
        artifacts=[artifact],
    )

    payload = json.loads(index_path.read_text(encoding="utf-8"))
    row = payload["artifacts"][0]
    assert row["state"] == "PA"
    assert row["source"] == "Pennsylvania DOH Hospital Reports"
    assert row["artifact_url"] == "https://www.pa.gov/content/dam/example/hospital-2024.pdf"
    assert row["landing_page_url"] == state_health_data.PA_HOSPITAL_REPORTS_URL
    assert row["publication_year"] == 2024
    assert row["publication_date"] == "2024"
    assert row["artifact_type"] == "pdf"

    metadata = pd.read_csv(tmp_path / "artifact_metadata.csv")
    assert metadata.loc[0, "state"] == "PA"
    assert metadata.loc[0, "artifact_url"] == row["artifact_url"]


@pytest.mark.asyncio
async def test_nj_public_data_combines_financial_and_charity_artifacts_without_network(tmp_path, monkeypatch) -> None:
    async def fake_request(method: str, url: str, **kwargs):  # noqa: ARG001
        if url == state_health_data.NJ_HOSPITAL_FINANCIAL_URL:
            html = '<a href="/health/hcf/documents/financial-reports/hospital_financial_2024.xlsx">Hospital Financial 2024</a>'
            return SimpleNamespace(text=html, content=html.encode())
        if url == state_health_data.NJ_CHARITY_CARE_URL:
            html = '<a href="/health/charitycare/documents/charity-care-2023.pdf">Charity Care 2023</a>'
            return SimpleNamespace(text=html, content=html.encode())
        return SimpleNamespace(text="artifact", content=b"artifact")

    monkeypatch.setattr(state_health_data, "resilient_request", fake_request)

    status = await state_health_data.acquire_nj_hospital_public_data(tmp_path)

    assert status.status == "ready"
    assert status.artifact_count == 2
    payload = json.loads((tmp_path / "state-health-data" / "nj-hospital-public-data" / "artifact_index.json").read_text())
    assert [row["state"] for row in payload["artifacts"]] == ["NJ", "NJ"]
    assert {row["source_id"] for row in payload["artifacts"]} == {"nj_hospital_financial", "nj_charity_care"}
    metadata = pd.read_csv(tmp_path / "state-health-data" / "nj-hospital-public-data" / "artifact_metadata.csv")
    assert set(metadata["artifact_type"]) == {"xlsx", "pdf"}


def test_normalize_phc4_csv_facility_year_rows(tmp_path) -> None:
    artifact = tmp_path / "financial-analysis.csv"
    artifact.write_text(
        "Facility Name,Fiscal Year,Measure,Value\nExample Hospital,2024,Operating Margin,3.2%\n",
        encoding="utf-8",
    )
    refs = state_health_data._extract_structured_tables(artifact, tmp_path / "cache")
    report = {
        "title": "Financial Analysis Fiscal Year 2024",
        "artifact_url": "https://www.phc4.org/financial-analysis.csv",
        "landing_page_url": state_health_data.PHC4_REPORT_LIBRARY_URL,
        "year": 2024,
        "publication_year": 2024,
        "report_type": "financial_analysis",
        "state": "PA",
        "source": "PHC4 Public Reports Library",
        "table_references": refs,
    }

    rows = state_health_data._normalized_phc4_rows_for_report(report)

    assert rows == [
        {
            "report_title": "Financial Analysis Fiscal Year 2024",
            "report_type": "financial_analysis",
            "report_year": 2024,
            "page": None,
            "table_index": 1,
            "source_artifact": "https://www.phc4.org/financial-analysis.csv",
            "source_artifact_path": str(artifact),
            "landing_page_url": state_health_data.PHC4_REPORT_LIBRARY_URL,
            "publication_date": "",
            "publication_year": 2024,
            "state": "PA",
            "source": "PHC4 Public Reports Library",
            "hospital_name": "Example Hospital",
            "facility_name": "Example Hospital",
            "fiscal_year": "2024",
            "procedure": "",
            "measure_name": "Operating Margin",
            "measure_value": "3.2%",
            "raw_row": {"Facility Name": "Example Hospital", "Fiscal Year": "2024", "Measure": "Operating Margin", "Value": "3.2%"},
            "confidence": "high_structured_table",
        }
    ]


def test_normalize_phc4_html_table_fixture(tmp_path) -> None:
    artifact = tmp_path / "hospital-performance.html"
    artifact.write_text(
        """
        <table>
          <tr><th>Hospital</th><th>Year</th><th>Procedure</th><th>Volume</th></tr>
          <tr><td>Example Hospital</td><td>2024</td><td>Knee Replacement</td><td>42</td></tr>
        </table>
        """,
        encoding="utf-8",
    )
    refs = state_health_data._extract_structured_tables(artifact, tmp_path / "cache")
    report = {"title": "Hospital Performance 2024", "artifact_url": "https://www.phc4.org/hpr.html", "year": 2024, "report_type": "hospital_performance", "table_references": refs}

    rows = state_health_data._normalized_phc4_rows_for_report(report)

    assert rows[0]["hospital_name"] == "Example Hospital"
    assert rows[0]["fiscal_year"] == "2024"
    assert rows[0]["procedure"] == "Knee Replacement"
    assert rows[0]["measure_name"] == "Volume"
    assert rows[0]["measure_value"] == "42"


def test_normalize_phc4_xlsx_table_fixture_without_engine_dependency(tmp_path, monkeypatch) -> None:
    artifact = tmp_path / "common-procedures.xlsx"
    artifact.write_bytes(b"not a real workbook; pandas is monkeypatched")

    def fake_read_excel(path, sheet_name=None, dtype=None):  # noqa: ARG001
        return {
            "Procedures": pd.DataFrame(
                [{"Facility": "Example Hospital", "FY": "2024", "Service": "Hip Replacement", "Count": "12"}]
            )
        }

    monkeypatch.setattr(state_health_data.pd, "read_excel", fake_read_excel)
    refs = state_health_data._extract_structured_tables(artifact, tmp_path / "cache")
    report = {"title": "Common Procedures 2024", "artifact_url": "https://www.phc4.org/common-procedures.xlsx", "year": 2024, "report_type": "common_procedure", "table_references": refs}

    rows = state_health_data._normalized_phc4_rows_for_report(report)

    assert rows[0]["hospital_name"] == "Example Hospital"
    assert rows[0]["procedure"] == "Hip Replacement"
    assert rows[0]["measure_name"] == "Count"
    assert rows[0]["measure_value"] == "12"


def test_normalize_phc4_pdf_like_text_when_facility_year_rows_are_parseable() -> None:
    report = {"title": "PHC4 PDF 2024", "artifact_url": "https://www.phc4.org/report.pdf", "year": 2024, "report_type": "hospital_performance"}
    table_ref = {"artifact_path": "/tmp/report.pdf", "page": 3, "table_index": 1, "extraction_status": "text_table_extracted"}
    payload = {
        "lines": [
            "Hospital  Fiscal Year  Measure  Value",
            "Example Hospital  2024  Mortality Rate  1.1%",
        ]
    }

    rows = state_health_data._normalize_phc4_pdf_lines(report, table_ref, payload)

    assert rows[0]["hospital_name"] == "Example Hospital"
    assert rows[0]["fiscal_year"] == "2024"
    assert rows[0]["measure_name"] == "Mortality Rate"
    assert rows[0]["measure_value"] == "1.1%"


@pytest.mark.asyncio
async def test_phc4_profile_reports_not_structured_enough_for_unparseable_table_text(tmp_path) -> None:
    cache = tmp_path / "state-health-data" / "phc4"
    table_dir = cache / "tables"
    table_dir.mkdir(parents=True)
    extracted = table_dir / "report-page-1-table-text.json"
    extracted.write_text(json.dumps({"lines": ["Narrative paragraph", "Another line"]}), encoding="utf-8")
    (cache / "report_index.json").write_text(
        json.dumps(
            [
                {
                    "title": "Hospital Performance Report 2024",
                    "artifact_url": "https://www.phc4.org/report.pdf",
                    "url": "https://www.phc4.org/report.pdf",
                    "year": 2024,
                    "report_type": "hospital_performance",
                    "table_references": [
                        {
                            "artifact_path": "/tmp/report.pdf",
                            "page": 1,
                            "table_index": 1,
                            "extracted_path": str(extracted),
                            "extraction_status": "not_structured_enough",
                        }
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )

    result = await state_health_data.phc4_report_profile(
        hospital_name="Example Hospital",
        year=2024,
        report_type="hospital_performance",
        cache_root=tmp_path,
    )

    assert result["confidence"] == "not_structured_enough"
    assert result["table_rows"] == []


@pytest.mark.asyncio
async def test_phc4_profile_returns_normalized_extracted_rows(tmp_path) -> None:
    cache = tmp_path / "state-health-data" / "phc4"
    cache.mkdir(parents=True)
    artifact = tmp_path / "financial-analysis.csv"
    artifact.write_text("hospital,measure,value\nExample Hospital,Operating Margin,3.2%\n", encoding="utf-8")
    refs = state_health_data._extract_structured_tables(artifact, cache)
    (cache / "report_index.json").write_text(
        json.dumps(
            [
                {
                    "title": "Financial Analysis Fiscal Year 2024",
                    "artifact_url": "https://www.phc4.org/financial-analysis.csv",
                    "url": "https://www.phc4.org/financial-analysis.csv",
                    "year": 2024,
                    "report_type": "financial_analysis",
                    "table_references": refs,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = await state_health_data.phc4_report_profile(
        hospital_name="Example Hospital",
        fiscal_year=2024,
        report_type="financial_analysis",
        cache_root=tmp_path,
    )

    assert result["confidence"] == "high_extracted_table_row"
    assert result["table_rows"][0]["hospital_name"] == "Example Hospital"
    assert result["table_rows"][0]["measure_name"] == "Operating Margin"
    assert result["table_rows"][0]["measure_value"] == "3.2%"


@pytest.mark.asyncio
async def test_phc4_common_procedure_profile_filters_extracted_rows_not_report_title(tmp_path) -> None:
    cache = tmp_path / "state-health-data" / "phc4"
    cache.mkdir(parents=True)
    artifact = tmp_path / "common-procedures.csv"
    artifact.write_text("hospital,procedure,volume\nExample Hospital,Knee Replacement,42\n", encoding="utf-8")
    refs = state_health_data._extract_structured_tables(artifact, cache)
    (cache / "report_index.json").write_text(
        json.dumps(
            [
                {
                    "title": "Common Procedures Fiscal Year 2024",
                    "artifact_url": "https://www.phc4.org/common-procedures.csv",
                    "url": "https://www.phc4.org/common-procedures.csv",
                    "year": 2024,
                    "report_type": "common_procedure",
                    "table_references": refs,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = await state_health_data.phc4_report_profile(
        hospital_name="Example Hospital",
        procedure="Knee Replacement",
        year=2024,
        report_type="common_procedure",
        cache_root=tmp_path,
    )

    assert result["confidence"] == "high_extracted_table_row"
    assert result["table_rows"][0]["procedure"] == "Knee Replacement"
    assert result["table_rows"][0]["measure_name"] == "volume"
    assert result["table_rows"][0]["measure_value"] == "42"


@pytest.mark.asyncio
async def test_load_cost_report_row_selects_requested_year() -> None:
    class Loaders:
        @staticmethod
        async def load_cost_report():
            return pd.DataFrame(
                [
                    {"ccn": "390001", "fiscal_year_end": "2023-06-30", "total_discharges": "100"},
                    {"ccn": "390001", "fiscal_year_end": "2024-06-30", "total_discharges": "200"},
                ]
            )

    row, error = await load_cost_report_row(Loaders, "390001", year=2023)

    assert error == ""
    assert row["total_discharges"] == "100"
