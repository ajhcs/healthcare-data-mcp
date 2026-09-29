from servers.form_990_facts.irs_source import IrsEfileIndexEntry


def test_official_batch_url_normalizes_legacy_index_suffix_case() -> None:
    entry = IrsEfileIndexEntry(
        return_id="22588066",
        filing_year=2024,
        ein="232829095",
        tax_period="202306",
        legal_filer="THOMAS JEFFERSON UNIVERSITY HOSPITALS INC",
        form_type="990",
        dln="93493136095714",
        object_id="202411369349309571",
        xml_batch_id="2024_TEOS_XML_05a",
    )

    assert entry.xml_batch_id == "2024_TEOS_XML_05a"
    assert entry.batch_url == ("https://apps.irs.gov/pub/epostcard/990/xml/2024/2024_TEOS_XML_05A.zip")
