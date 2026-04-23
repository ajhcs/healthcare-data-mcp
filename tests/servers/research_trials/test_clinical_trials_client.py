from __future__ import annotations

from servers.research_trials import clinical_trials_client


def test_build_studies_params_preserves_filters_and_page_token() -> None:
    params = clinical_trials_client.build_studies_params(
        query="cancer",
        sponsor="M.D. Anderson",
        condition="Neoplasms",
        intervention="questionnaire",
        location="Houston Texas",
        status="RECRUITING",
        phase="NA",
        fields=["NCTId", "BriefTitle"],
        page_size=250,
        page_token="NEXT",
    )

    assert params["query.term"] == "cancer"
    assert params["query.spons"] == "M.D. Anderson"
    assert params["query.cond"] == "Neoplasms"
    assert params["query.intr"] == "questionnaire"
    assert params["query.locn"] == "Houston Texas"
    assert params["filter.overallStatus"] == "RECRUITING"
    assert params["filter.phase"] == "NA"
    assert params["fields"] == "NCTId,BriefTitle"
    assert params["pageSize"] == 100
    assert params["pageToken"] == "NEXT"


def test_build_studies_params_bounds_invalid_page_size() -> None:
    params = clinical_trials_client.build_studies_params(page_size="bad")  # type: ignore[arg-type]

    assert params["pageSize"] == 25


def test_normalize_study_maps_v2_protocol_sections() -> None:
    raw = {
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT06367959",
                "organization": {"fullName": "M.D. Anderson Cancer Center"},
                "briefTitle": "Writing Therapy",
                "officialTitle": "Health Benefits of Writing Therapy",
            },
            "statusModule": {
                "overallStatus": "RECRUITING",
                "startDateStruct": {"date": "2023-07-27"},
                "primaryCompletionDateStruct": {"date": "2026-08-31"},
                "completionDateStruct": {"date": "2026-08-31"},
                "lastUpdatePostDateStruct": {"date": "2025-12-30"},
            },
            "sponsorCollaboratorsModule": {
                "leadSponsor": {"name": "M.D. Anderson Cancer Center", "class": "OTHER"},
                "collaborators": [{"name": "NCI", "class": "NIH"}],
            },
            "conditionsModule": {"conditions": ["Cancer"]},
            "designModule": {
                "studyType": "INTERVENTIONAL",
                "phases": ["NA"],
                "enrollmentInfo": {"count": 192},
            },
            "armsInterventionsModule": {
                "interventions": [{"type": "BEHAVIORAL", "name": "Questionnaires"}],
            },
            "contactsLocationsModule": {
                "overallOfficials": [
                    {"name": "Qian Lu", "affiliation": "M.D. Anderson Cancer Center", "role": "PRINCIPAL_INVESTIGATOR"}
                ],
                "locations": [
                    {
                        "facility": "MD Anderson Cancer Center",
                        "status": "RECRUITING",
                        "city": "Houston",
                        "state": "Texas",
                        "country": "United States",
                        "zip": "77030",
                    }
                ],
            },
        },
        "derivedSection": {"miscInfoModule": {"versionHolder": "2026-04-23"}},
    }

    trial = clinical_trials_client.normalize_study(raw)

    assert trial.nct_id == "NCT06367959"
    assert trial.lead_sponsor.name == "M.D. Anderson Cancer Center"
    assert trial.collaborators[0].name == "NCI"
    assert trial.locations[0].city == "Houston"
    assert trial.interventions == ["BEHAVIORAL: Questionnaires"]
    assert trial.version_holder == "2026-04-23"
    assert trial.url.endswith("/NCT06367959")


def test_metadata_from_clinical_trials_response_includes_version_and_page_token() -> None:
    metadata = clinical_trials_client.metadata_from_response(
        {
            "nextPageToken": "TOKEN",
            "_version": {"apiVersion": "2.0.5", "dataTimestamp": "2026-04-23T09:00:05"},
        },
        page_size=25,
    )

    assert metadata.api_version == "2.0.5"
    assert metadata.data_timestamp == "2026-04-23T09:00:05"
    assert metadata.next_page_token == "TOKEN"
    assert metadata.page_size == 25
