from __future__ import annotations

from pathlib import Path

from servers.form_990_facts.fact_store import Form990FactStore
from servers.form_990_facts.irs_source import IrsEfileIndexEntry, ingest_batch_member


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"


def test_operator_ingests_an_official_style_deflate64_member(tmp_path: Path) -> None:
    entry = IrsEfileIndexEntry(
        return_id="23826779",
        filing_year=2025,
        ein="811244422",
        tax_period="202412",
        legal_filer="PROVIDENCE ST JOSEPH HEALTH",
        form_type="990",
        dln="93493134087295",
        object_id="202541349349308729",
        xml_batch_id="2025_TEOS_XML_05B",
    )

    store = Form990FactStore(tmp_path / "facts.sqlite3")
    receipt = ingest_batch_member(
        store,
        entry=entry,
        archive_path=FIXTURES / "deflate64_member.zip",
    )

    assert receipt["status"] == "ingested"
    assert receipt["xml_sha256"] == ("107dfdc7a67f8e8feb72e5deee63e1ad442785488e03a521d7329cb49a61ea5e")
