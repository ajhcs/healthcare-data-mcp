from __future__ import annotations

from pathlib import Path

from servers.form_990_facts.fact_store import Form990FactStore


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"


def test_operator_can_ingest_and_retrieve_exact_filing_facts(tmp_path: Path) -> None:
    store = Form990FactStore(tmp_path / "facts.sqlite3")
    source_url = "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_11C.zip"

    receipt = store.ingest_xml(
        (FIXTURES / "health_system_2024.xml").read_bytes(),
        source_url=source_url,
        source_member="202523169349306307_public.xml",
        object_id="202523169349306307",
        filing_year=2025,
    )
    result = store.get_facts(ein="81-1244422", tax_year=2024)

    assert receipt["status"] == "ingested"
    assert result["status"] == "ready"
    assert result["filing"] == {
        "legal_filer": "PROVIDENCE ST JOSEPH HEALTH",
        "ein": "811244422",
        "tax_period_end": "2024-12-31",
        "tax_year": 2024,
        "form_type": "990",
    }
    assert result["facts"]["total_revenue"]["value"] == 32_100_123_456
    assert result["facts"]["total_expenses"]["value"] == 30_987_654_321
    assert result["facts"]["net_assets"]["value"] == 9_876_543_210
    assert {
        key: result["facts"]["top_reported_executive"][key]
        for key in ("name", "title", "total_reported_compensation", "units", "definition")
    } == {
        "name": "TAYLOR EXECUTIVE",
        "title": "REGIONAL CEO",
        "total_reported_compensation": 1_900_000,
        "units": "USD",
        "definition": "Form 990 Part VII columns D + E + F",
    }

    revenue_provenance = result["facts"]["total_revenue"]["provenance"]
    assert revenue_provenance["source_url"] == source_url
    assert revenue_provenance["source_member"] == "202523169349306307_public.xml"
    assert revenue_provenance["object_id"] == "202523169349306307"
    assert revenue_provenance["xml_sha256"] == receipt["xml_sha256"]
    assert revenue_provenance["reported_label"] == "Total revenue"
    assert revenue_provenance["metric_key"] == "total_revenue"
    assert revenue_provenance["units"] == "USD"
    assert revenue_provenance["xml_field_path"] == "/Return/ReturnData/IRS990/CYTotalRevenueAmt"
