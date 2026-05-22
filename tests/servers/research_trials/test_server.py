from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from servers.research_trials import server
from shared.utils.mcp_response import validate_evidence_receipt


@pytest.mark.asyncio
async def test_research_trials_server_imports_with_output_schemas() -> None:
    tools = await server.mcp.list_tools()

    assert tools
    assert {tool.name for tool in tools} >= {
        "search_nih_projects",
        "get_nih_project",
        "profile_research_funding",
        "search_clinical_trials",
        "get_clinical_trial",
        "inventory_clinical_trial_sponsors",
        "inventory_clinical_trial_sites",
        "profile_research_activity",
    }
    assert all(tool.outputSchema for tool in tools)


@pytest.mark.asyncio
async def test_search_nih_projects_validates_query() -> None:
    result = await server.search_nih_projects(org_uei="FTMTDMBR29C7")

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_search_nih_projects_reports_uei_filtered_count() -> None:
    raw = {
        "meta": {"total": 2, "limit": 25, "offset": 0},
        "results": [
            {
                "appl_id": 1,
                "project_num": "1R01AA000001-01",
                "project_title": "Kept",
                "organization": {"org_name": "Example University", "primary_uei": "FTMTDMBR29C7"},
            },
            {
                "appl_id": 2,
                "project_num": "1R01AA000002-01",
                "project_title": "Dropped",
                "organization": {"org_name": "Example University", "primary_uei": "OTHERUEI12345"},
            },
        ],
    }

    with patch.object(server.reporter_client, "search_projects", new_callable=AsyncMock, return_value=raw):
        result = await server.search_nih_projects(org_name="Example University", org_uei="FTMTDMBR29C7")

    assert result["total_results"] == 1
    assert len(result["projects"]) == 1
    assert result["projects"][0]["organization"]["uei"] == "FTMTDMBR29C7"
    assert result["evidence"]["dataset_id"] == "nih_reporter_projects"
    assert result["evidence"]["match_basis"] == "nih_reporter_search_filters"
    assert result["identity"]["unresolved_identifiers"][0] == {"type": "uei", "value": "FTMTDMBR29C7"}
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["projects"][0]["evidence"], require_content=True)
    assert result["projects"][0]["evidence"]["dataset_id"] == "nih_reporter_projects"
    assert result["projects"][0]["evidence"]["match_basis"] == "nih_reporter_project_row"
    assert result["projects"][0]["evidence"]["query"]["appl_id"] == "1"


@pytest.mark.asyncio
async def test_search_clinical_trials_returns_normalized_results() -> None:
    raw = {
        "nextPageToken": "NEXT",
        "_version": {"apiVersion": "2.0.5", "dataTimestamp": "2026-04-23T09:00:05"},
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {
                        "nctId": "NCT00000001",
                        "briefTitle": "Example Trial",
                        "organization": {"fullName": "Example Sponsor"},
                    },
                    "statusModule": {"overallStatus": "RECRUITING"},
                    "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Example Sponsor", "class": "OTHER"}},
                    "conditionsModule": {"conditions": ["Cancer"]},
                    "designModule": {"studyType": "INTERVENTIONAL"},
                }
            }
        ],
    }

    with patch.object(server.clinical_trials_client, "search_studies", new_callable=AsyncMock, return_value=raw):
        result = await server.search_clinical_trials(query="cancer", page_size=10, page_token="TOKEN")

    assert result["total_results"] == 1
    assert result["trials"][0]["nct_id"] == "NCT00000001"
    assert result["metadata"]["next_page_token"] == "NEXT"
    assert result["metadata"]["api_version"] == "2.0.5"
    assert result["evidence"]["dataset_id"] == "clinicaltrials_gov"
    assert result["evidence"]["source_period"] == "2026-04-23T09:00:05"
    assert result["evidence"]["match_basis"] == "clinicaltrials_search_filters"
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["trials"][0]["evidence"], require_content=True)
    assert result["trials"][0]["evidence"]["dataset_id"] == "clinicaltrials_gov"
    assert result["trials"][0]["evidence"]["match_basis"] == "clinicaltrials_study_row"
    assert result["trials"][0]["evidence"]["query"]["nct_id"] == "NCT00000001"


@pytest.mark.asyncio
async def test_get_clinical_trial_validates_nct_id() -> None:
    result = await server.get_clinical_trial("bad-id")

    assert result["ok"] is False
    assert result["error"]["code"] == "invalid_params"


@pytest.mark.asyncio
async def test_get_nih_project_returns_evidence_receipts() -> None:
    raw = {
        "meta": {"total": 1, "limit": 1, "offset": 0},
        "results": [
            {
                "appl_id": 1,
                "project_num": "1R01AA000001-01",
                "project_title": "Example Project",
                "organization": {"org_name": "Example University", "primary_uei": "FTMTDMBR29C7"},
            }
        ],
    }

    with (
        patch.object(server.reporter_client, "get_project", new_callable=AsyncMock, return_value=raw),
        patch.object(
            server.reporter_client,
            "search_publications_by_appl_id",
            new_callable=AsyncMock,
            return_value={"results": []},
        ),
    ):
        result = await server.get_nih_project(appl_id="1")

    assert result["project"]["project_num"] == "1R01AA000001-01"
    assert result["evidence"]["dataset_id"] == "nih_reporter_projects"
    assert result["evidence"]["match_basis"] == "appl_id_or_project_num_lookup"
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["project"]["evidence"], require_content=True)
    assert result["project"]["evidence"]["match_basis"] == "nih_reporter_project_row"


@pytest.mark.asyncio
async def test_profile_research_funding_returns_evidence_receipts() -> None:
    expected = {
        "organization_name": "Example University",
        "total_projects": 1,
        "total_award_amount": 100000,
        "metadata": {
            "source_name": "NIH RePORTER",
            "source_url": "https://api.reporter.nih.gov/v2/projects/search",
            "api_version": "v2",
            "queried_at": "2026-04-23T00:00:00+00:00",
            "query": {"org_name": "Example University"},
            "total_records": 1,
            "returned_records": 1,
        },
        "projects": [
            {
                "appl_id": "1",
                "project_num": "1R01AA000001-01",
                "project_title": "Example Project",
                "organization": {"name": "Example University", "uei": "FTMTDMBR29C7"},
                "fiscal_year": 2025,
            }
        ],
        "by_fiscal_year": [{"fiscal_year": 2025, "project_count": 1, "award_amount": 100000}],
        "by_institute": [],
        "by_pi": [],
        "by_activity_code": [],
        "top_terms": [],
    }

    with patch.object(server.profiles, "build_funding_profile", new_callable=AsyncMock, return_value=expected):
        result = await server.profile_research_funding(org_name="Example University", years=[2025])

    assert result["organization_name"] == "Example University"
    assert result["evidence"]["dataset_id"] == "nih_reporter_projects"
    assert result["evidence"]["match_basis"] == "organization_name_search_with_optional_uei_filter"
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["projects"][0]["evidence"], require_content=True)
    validate_evidence_receipt(result["by_fiscal_year"][0]["evidence"], require_content=True)
    assert result["projects"][0]["evidence"]["match_basis"] == "nih_reporter_project_row"
    assert result["by_fiscal_year"][0]["evidence"]["match_basis"] == "nih_reporter_fiscal_year_aggregate"


@pytest.mark.asyncio
async def test_get_clinical_trial_returns_evidence_receipts() -> None:
    raw = {
        "_version": {"apiVersion": "2.0.5", "dataTimestamp": "2026-04-23T09:00:05"},
        "protocolSection": {
            "identificationModule": {
                "nctId": "NCT00000001",
                "briefTitle": "Example Trial",
                "organization": {"fullName": "Example Sponsor"},
            },
            "statusModule": {"overallStatus": "RECRUITING"},
            "sponsorCollaboratorsModule": {"leadSponsor": {"name": "Example Sponsor", "class": "OTHER"}},
            "conditionsModule": {"conditions": ["Cancer"]},
            "designModule": {"studyType": "INTERVENTIONAL"},
        },
    }

    with patch.object(server.clinical_trials_client, "get_study", new_callable=AsyncMock, return_value=raw):
        result = await server.get_clinical_trial("NCT00000001")

    assert result["trial"]["nct_id"] == "NCT00000001"
    assert result["evidence"]["dataset_id"] == "clinicaltrials_gov"
    assert result["evidence"]["match_basis"] == "nct_id_exact"
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["trial"]["evidence"], require_content=True)
    assert result["trial"]["evidence"]["match_basis"] == "clinicaltrials_study_row"


@pytest.mark.asyncio
async def test_inventory_clinical_trial_sponsors_separates_roles_and_counts() -> None:
    raw = {
        "_version": {"apiVersion": "2.0.5"},
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {
                        "nctId": "NCT00000001",
                        "organization": {"fullName": "Example Health"},
                    },
                    "statusModule": {
                        "overallStatus": "RECRUITING",
                        "startDateStruct": {"date": "2025-01-01"},
                    },
                    "sponsorCollaboratorsModule": {
                        "leadSponsor": {"name": "Example Health", "class": "OTHER"},
                        "collaborators": [{"name": "Example University", "class": "OTHER"}],
                    },
                }
            },
            {
                "protocolSection": {
                    "identificationModule": {"nctId": "NCT00000002"},
                    "statusModule": {
                        "overallStatus": "COMPLETED",
                        "startDateStruct": {"date": "2024-01-01"},
                    },
                    "sponsorCollaboratorsModule": {
                        "leadSponsor": {"name": "Example University", "class": "OTHER"},
                        "collaborators": [{"name": "Example Health", "class": "OTHER"}],
                    },
                }
            },
        ],
    }

    with patch.object(server.clinical_trials_client, "search_studies", new_callable=AsyncMock, return_value=raw):
        result = await server.inventory_clinical_trial_sponsors("Example", scan_limit=10)

    records = {record["normalized_sponsor_name"]: record for record in result["records"]}
    assert records["EXAMPLE HEALTH"]["lead_sponsor_count"] == 1
    assert records["EXAMPLE HEALTH"]["collaborator_count"] == 1
    assert records["EXAMPLE HEALTH"]["org_full_name_count"] == 1
    assert records["EXAMPLE HEALTH"]["active_recruiting_count"] == 1
    assert records["EXAMPLE UNIVERSITY"]["lead_sponsor_count"] == 1
    assert result["evidence"]["dataset_id"] == "clinicaltrials_gov"
    assert result["evidence"]["match_basis"] == "clinicaltrials_sponsor_inventory_grouping"
    assert result["identity_map"]["entities"]
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(records["EXAMPLE HEALTH"]["evidence"], require_content=True)
    assert records["EXAMPLE HEALTH"]["evidence"]["match_basis"] == "clinicaltrials_sponsor_inventory_row"
    assert records["EXAMPLE HEALTH"]["evidence"]["query"]["nct_ids"] == ["NCT00000001", "NCT00000002"]


@pytest.mark.asyncio
async def test_inventory_clinical_trial_sites_keys_facility_by_geography_and_unresolved_count() -> None:
    raw = {
        "_version": {"apiVersion": "2.0.5"},
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {"nctId": "NCT00000001"},
                    "statusModule": {
                        "overallStatus": "RECRUITING",
                        "lastUpdatePostDateStruct": {"date": "2026-01-01"},
                    },
                    "contactsLocationsModule": {
                        "locations": [
                            {"facility": "Example Hospital", "city": "Philadelphia", "state": "PA", "country": "United States", "zip": "19107", "status": "RECRUITING"},
                            {"facility": "Example Hospital", "city": "Camden", "state": "NJ", "country": "United States", "zip": "08103", "status": "RECRUITING"},
                            {"city": "Philadelphia", "state": "PA", "country": "United States"},
                        ]
                    },
                }
            }
        ],
    }

    with patch.object(server.clinical_trials_client, "search_studies", new_callable=AsyncMock, return_value=raw):
        result = await server.inventory_clinical_trial_sites("Example Hospital", scan_limit=10)

    assert result["unique_site_count"] == 2
    assert result["unresolved_location_count"] == 1
    assert {record["state"] for record in result["records"]} == {"PA", "NJ"}
    assert result["evidence"]["match_basis"] == "clinicaltrials_site_inventory_grouping"
    assert {entity["state"] for entity in result["identity_map"]["entities"]} == {"PA", "NJ"}
    validate_evidence_receipt(result["evidence"], require_content=True)
    validate_evidence_receipt(result["records"][0]["evidence"], require_content=True)
    assert result["records"][0]["evidence"]["match_basis"] == "clinicaltrials_site_inventory_row"
    assert result["records"][0]["evidence"]["query"]["nct_ids"] == ["NCT00000001"]


@pytest.mark.asyncio
async def test_profile_research_activity_delegates_profile_builder() -> None:
    expected = {
        "organization_name": "Example",
        "warnings": [],
        "combined_summary": {"nih_project_count": 0, "clinical_trial_count": 0},
    }

    with patch.object(server.profiles, "build_research_activity_profile", new_callable=AsyncMock, return_value=expected):
        result = await server.profile_research_activity(organization_name="Example", years=[2025])

    assert result["organization_name"] == "Example"
    assert result["combined_summary"]["clinical_trial_count"] == 0
    assert result["evidence"]["dataset_id"] == "research_activity_profile"
    assert result["identity"]["canonical_name"] == "EXAMPLE"
    validate_evidence_receipt(result["evidence"], require_content=True)
