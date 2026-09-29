from servers.form_990_facts.irs_source import IrsEfileIndexEntry


def test_indexed_part_a_can_resolve_an_official_split_part_b() -> None:
    entry = IrsEfileIndexEntry(
        return_id="23826779",
        filing_year=2025,
        ein="232829095",
        tax_period="202406",
        legal_filer="THOMAS JEFFERSON UNIVERSITY HOSPITALS INC",
        form_type="990",
        dln="93493134087295",
        object_id="202541349349308729",
        xml_batch_id="2025_TEOS_XML_05A",
    )

    assert entry.candidate_xml_batch_ids == (
        "2025_TEOS_XML_05A",
        "2025_TEOS_XML_05B",
    )
