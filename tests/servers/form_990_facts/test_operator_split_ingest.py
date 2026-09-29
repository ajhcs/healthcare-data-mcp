from __future__ import annotations

from pathlib import Path
from zipfile import ZipFile

from servers.form_990_facts.fact_store import Form990FactStore
from servers.form_990_facts.operator_ingest import IngestRequest, ingest_official_requests


FIXTURES = Path(__file__).parents[2] / "fixtures" / "irs990"

INDEX_CSV = """RETURN_ID,FILING_TYPE,EIN,TAX_PERIOD,SUB_DATE,TAXPAYER_NAME,RETURN_TYPE,DLN,OBJECT_ID,XML_BATCH_ID
23826779,EFILE,811244422,202412,2025,PROVIDENCE ST JOSEPH HEALTH,990,93493134087295,202541349349308729,2025_TEOS_XML_05A
"""


def test_operator_finds_an_indexed_member_in_official_split_part_b(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "index_2025.csv").write_text(INDEX_CSV, encoding="utf-8")
    with ZipFile(cache / "2025_TEOS_XML_05A.zip", "w") as archive:
        archive.writestr("unrelated_public.xml", b"<unrelated/>")
    with ZipFile(cache / "2025_TEOS_XML_05B.zip", "w") as archive:
        archive.writestr(
            "202541349349308729_public.xml",
            (FIXTURES / "health_system_2024.xml").read_bytes(),
        )

    store = Form990FactStore(tmp_path / "facts.sqlite3")
    receipts = ingest_official_requests(
        store,
        [
            IngestRequest(
                ein="811244422",
                tax_year=2024,
                filing_year=2025,
                object_id="202541349349308729",
            )
        ],
        cache_dir=cache,
        keep_archives=True,
    )

    assert receipts[0]["status"] == "ingested"
    result = store.get_facts(ein="811244422", tax_year=2024)
    assert result["fact_provenance"]["total_revenue"]["source_url"].endswith("/2025_TEOS_XML_05B.zip")
    assert result["fact_provenance"]["total_revenue"]["xml_batch_id"] == "2025_TEOS_XML_05B"
