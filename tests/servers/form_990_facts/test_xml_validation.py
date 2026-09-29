from __future__ import annotations

from pathlib import Path

import pytest

from servers.form_990_facts.fact_store import Form990FactStore, extract_filing


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"


def test_extractor_rejects_non_irs_xml_namespace() -> None:
    xml_bytes = (
        (FIXTURES / "health_system_2024.xml")
        .read_bytes()
        .replace(
            b"http://www.irs.gov/efile",
            b"https://example.com/not-irs",
        )
    )

    with pytest.raises(ValueError, match="official IRS e-file"):
        extract_filing(xml_bytes)


def test_ingestion_rejects_non_irs_source_url(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="official HTTPS IRS URL"):
        Form990FactStore(tmp_path / "facts.sqlite3").ingest_xml(
            (FIXTURES / "health_system_2024.xml").read_bytes(),
            source_url="https://example.com/filing.xml",
            source_member="202523169349306307_public.xml",
            object_id="202523169349306307",
            filing_year=2025,
        )
