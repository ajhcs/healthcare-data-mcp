from __future__ import annotations

from servers.research_trials import reporter_client


def test_build_project_search_payload_uses_documented_criteria_only() -> None:
    payload = reporter_client.build_project_search_payload(
        org_name="Johns Hopkins",
        org_uei="FTMTDMBR29C7",
        pi_name="Karakousis",
        text="tuberculosis",
        fiscal_years=[2025, "2024"],  # type: ignore[list-item]
        activity_codes=["R01"],
        agencies=["NIAID"],
        limit=999,
        offset=-4,
    )

    assert payload["limit"] == 500
    assert payload["offset"] == 0
    criteria = payload["criteria"]
    assert criteria["org_names"] == ["JOHNS HOPKINS"]
    assert "org_ueis" not in criteria
    assert criteria["pi_names"] == [{"any_name": "Karakousis"}]
    assert criteria["advanced_text_search"]["search_text"] == "tuberculosis"
    assert criteria["fiscal_years"] == [2025, 2024]
    assert criteria["activity_codes"] == ["R01"]
    assert criteria["agencies"] == ["NIAID"]


def test_build_project_search_payload_bounds_invalid_pagination() -> None:
    payload = reporter_client.build_project_search_payload(limit="bad", offset="bad")  # type: ignore[arg-type]

    assert payload["limit"] == 25
    assert payload["offset"] == 0


def test_normalize_project_maps_nih_reporter_shapes() -> None:
    raw = {
        "appl_id": 11133648,
        "project_num": "1R01AI186308-01A1",
        "core_project_num": "R01AI186308",
        "project_title": "Host-Directed Therapy for TB",
        "fiscal_year": 2025,
        "award_amount": 2922220,
        "activity_code": "R01",
        "agency_code": "NIAID",
        "funding_mechanism": "Research Project Grants",
        "project_detail_url": "https://reporter.nih.gov/project-details/11133648",
        "organization": {
            "org_name": "JOHNS HOPKINS UNIVERSITY",
            "dept_type": "INTERNAL MEDICINE/MEDICINE",
            "org_city": "BALTIMORE",
            "org_state": "MD",
            "org_country": "UNITED STATES",
            "org_zipcode": "212182680",
            "org_ueis": ["FTMTDMBR29C7"],
            "primary_uei": "FTMTDMBR29C7",
            "org_duns": ["001910777"],
            "primary_duns": "001910777",
            "org_ipf_code": "4134401",
        },
        "principal_investigators": [
            {
                "profile_id": 7296323,
                "first_name": "Petros",
                "middle_name": "C",
                "last_name": "Karakousis",
                "full_name": "Petros C Karakousis",
                "title": "PROFESSOR",
                "is_contact_pi": True,
            }
        ],
        "agency_ic_fundings": [
            {
                "fy": 2025,
                "code": "AI",
                "name": "National Institute of Allergy and Infectious Diseases",
                "abbreviation": "NIAID",
                "total_cost": 2922220.0,
                "direct_cost_ic": 1880514.0,
                "indirect_cost_ic": 1041706.0,
            }
        ],
        "pref_terms": "Tuberculosis;Clinical Trials;Human",
    }

    project = reporter_client.normalize_project(raw)

    assert project.appl_id == "11133648"
    assert project.project_num == "1R01AI186308-01A1"
    assert project.organization.name == "JOHNS HOPKINS UNIVERSITY"
    assert project.organization.uei == "FTMTDMBR29C7"
    assert project.principal_investigators[0].is_contact_pi is True
    assert project.institute_fundings[0].abbreviation == "NIAID"
    assert project.terms == ["Tuberculosis", "Clinical Trials", "Human"]


def test_metadata_from_reporter_response_preserves_search_id_and_url() -> None:
    metadata = reporter_client.metadata_from_response(
        {
            "meta": {
                "search_id": "abc123",
                "properties": {"URL": "https://reporter.nih.gov/search/abc123/projects"},
            }
        }
    )

    assert metadata.source_name == "NIH RePORTER"
    assert metadata.search_id == "abc123"
    assert metadata.source_detail_url.endswith("/abc123/projects")
