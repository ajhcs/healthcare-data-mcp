from __future__ import annotations

from pathlib import Path

from servers.form_990_facts.fact_store import Form990FactStore


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"


def test_missing_reported_fields_are_no_data_findings_not_zeroes(tmp_path: Path) -> None:
    store = Form990FactStore(tmp_path / "facts.sqlite3")
    store.ingest_xml(
        (FIXTURES / "health_system_missing_fields_2023.xml").read_bytes(),
        source_url="https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_05A.zip",
        source_member="202411119349300001_public.xml",
        object_id="202411119349300001",
        filing_year=2024,
    )

    result = store.get_facts(ein="251423657", tax_year=2023)

    assert result["facts"]["net_assets"]["value"] is None
    assert result["facts"]["net_assets"]["status"] == "not_reported"
    assert result["facts"]["net_assets"]["provenance"]["xml_field_path"] == ""
    assert result["facts"]["top_reported_executive"]["total_reported_compensation"] is None
    assert result["fact_status"]["top_reported_executive"] == "not_reported"


def test_unknown_exact_ein_and_year_returns_not_found(tmp_path: Path) -> None:
    result = Form990FactStore(tmp_path / "facts.sqlite3").get_facts(
        ein="811244422",
        tax_year=2024,
    )

    assert result == {
        "status": "not_found",
        "query": {"ein": "811244422", "tax_year": 2024},
        "facts": {},
        "caveat": "No ingested official Form 990 filing matches this exact EIN and tax year.",
    }
