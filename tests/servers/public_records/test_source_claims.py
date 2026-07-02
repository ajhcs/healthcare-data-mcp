from servers.public_records.source_claims import public_source_claim


def test_public_source_claim_uses_standard_public_record_paths_by_default() -> None:
    assert public_source_claim(
        collection="cms_provider_directory",
        match_policy="ccn_exact_required_for_facility_identity_claim",
    ) == {
        "collection": "cms_provider_directory",
        "identity_paths": ["evidence.query"],
        "evidence_path": "evidence",
        "source_metadata_path": "source_metadata",
        "match_policy": "ccn_exact_required_for_facility_identity_claim",
    }


def test_public_source_claim_includes_optional_dataset_and_row_evidence_paths() -> None:
    assert public_source_claim(
        collection="sam_gov_exclusions_metadata",
        dataset_id="sam_gov_exclusions",
        match_policy="source_metadata_lookup_no_entity_match_claim",
        identity_paths=["source_name", "source_url", "evidence.query"],
        row_evidence_paths=["records[].evidence"],
    ) == {
        "collection": "sam_gov_exclusions_metadata",
        "identity_paths": ["source_name", "source_url", "evidence.query"],
        "evidence_path": "evidence",
        "source_metadata_path": "source_metadata",
        "match_policy": "source_metadata_lookup_no_entity_match_claim",
        "dataset_id": "sam_gov_exclusions",
        "row_evidence_paths": ["records[].evidence"],
    }
