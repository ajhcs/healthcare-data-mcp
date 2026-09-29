from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

import pytest

from servers.form_990_facts.fact_store import Form990FactStore
from servers.form_990_facts.irs_source import (
    IrsEfileIndexEntry,
    ingest_batch_member,
    select_index_entry,
)


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"

INDEX_CSV = b"""RETURN_ID,FILING_TYPE,EIN,TAX_PERIOD,SUB_DATE,TAXPAYER_NAME,RETURN_TYPE,DLN,OBJECT_ID,XML_BATCH_ID
23809922,EFILE,470617373,202406,2025,COMMONSPIRIT HEALTH,990,93493134054885,202531349349305488,2025_TEOS_XML_05A
,EFILE,811244422,202412,2025,PROVIDENCE ST JOSEPH HEALTH,990,93493316063075,202523169349306307,2025_TEOS_XML_11C
23798947,EFILE,811244422,202412,2025,PROVIDENCE ST JOSEPH HEALTH,990T,93393311003125,202523119339300312,2025_TEOS_XML_11C
"""


def test_official_index_selects_exact_ein_year_and_form() -> None:
    entry = select_index_entry(INDEX_CSV, ein="81-1244422", tax_year=2024, filing_year=2025)

    assert entry == IrsEfileIndexEntry(
        return_id="",
        filing_year=2025,
        ein="811244422",
        tax_period="202412",
        legal_filer="PROVIDENCE ST JOSEPH HEALTH",
        form_type="990",
        dln="93493316063075",
        object_id="202523169349306307",
        xml_batch_id="2025_TEOS_XML_11C",
    )
    assert entry.index_url == "https://apps.irs.gov/pub/epostcard/990/xml/2025/index_2025.csv"
    assert entry.batch_url == "https://apps.irs.gov/pub/epostcard/990/xml/2025/2025_TEOS_XML_11C.zip"
    assert entry.source_member == "202523169349306307_public.xml"


def test_index_requires_an_object_id_when_exact_scope_is_ambiguous() -> None:
    duplicate = INDEX_CSV + (
        b"999,EFILE,811244422,202412,2025,PROVIDENCE ST JOSEPH HEALTH,990,"
        b"93493316063076,202523169349306308,2025_TEOS_XML_11C\n"
    )

    with pytest.raises(ValueError, match="multiple Form 990 filings"):
        select_index_entry(duplicate, ein="811244422", tax_year=2024, filing_year=2025)


def test_operator_ingests_the_exact_member_from_an_official_batch(tmp_path: Path) -> None:
    entry = select_index_entry(INDEX_CSV, ein="811244422", tax_year=2024, filing_year=2025)
    archive_path = tmp_path / "2025_TEOS_XML_11C.zip"
    with ZipFile(archive_path, "w") as archive:
        archive.writestr(entry.source_member, (FIXTURES / "health_system_2024.xml").read_bytes())

    store = Form990FactStore(tmp_path / "facts.sqlite3")
    receipt = ingest_batch_member(store, entry=entry, archive_path=archive_path)

    assert receipt["status"] == "ingested"
    assert store.get_facts(ein=entry.ein, tax_year=2024)["status"] == "ready"
