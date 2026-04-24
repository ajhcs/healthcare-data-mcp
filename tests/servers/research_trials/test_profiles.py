from __future__ import annotations

from servers.research_trials import profiles, reporter_client
from servers.research_trials.models import SourceMetadata
from servers.research_trials.org_matching import decide_organization_match, normalize_org_name


def _project(raw_updates: dict) -> object:
    raw = {
        "appl_id": 1,
        "project_num": "1R01AA000001-01",
        "project_title": "Example",
        "fiscal_year": 2025,
        "award_amount": 100.0,
        "activity_code": "R01",
        "organization": {"org_name": "JOHNS HOPKINS UNIVERSITY", "primary_uei": "FTMTDMBR29C7"},
        "principal_investigators": [{"full_name": "Jane Doe"}],
        "agency_ic_fundings": [{"abbreviation": "NIAID", "total_cost": 100.0}],
        "pref_terms": "Cancer;Clinical Trials",
    }
    raw.update(raw_updates)
    return reporter_client.normalize_project(raw)


def test_aggregate_funding_profile_summarizes_projects() -> None:
    project_one = _project({})
    project_two = _project(
        {
            "appl_id": 2,
            "fiscal_year": 2024,
            "award_amount": 50.0,
            "activity_code": "U01",
            "principal_investigators": [{"full_name": "John Smith"}],
            "pref_terms": "Cancer;Informatics",
        }
    )

    profile = profiles.aggregate_funding_profile(
        organization_search="Johns Hopkins",
        org_uei="FTMTDMBR29C7",
        years=[2025, 2024],
        projects=[project_one, project_two],  # type: ignore[list-item]
        metadata=SourceMetadata(source_name="NIH RePORTER"),
    )

    assert profile.total_projects == 2
    assert profile.total_award_amount == 150.0
    assert profile.by_fiscal_year[0]["fiscal_year"] == 2025
    assert profile.by_activity_code[0]["activity_code"] == "R01"
    assert profile.top_terms[0] == {"term": "Cancer", "count": 2}


def test_filter_projects_by_uei_is_exact() -> None:
    kept = _project({})
    dropped = _project({"organization": {"org_name": "OTHER", "primary_uei": "OTHERUEI"}})

    assert profiles.filter_projects_by_uei([kept, dropped], "FTMTDMBR29C7") == [kept]


def test_org_matching_marks_multiple_brand_entities_ambiguous() -> None:
    decision = decide_organization_match(
        query_name="Johns Hopkins",
        nih_candidates=[("JOHNS HOPKINS UNIVERSITY", "FTMTDMBR29C7")],
        trial_candidates=["Johns Hopkins Health System"],
    )

    assert decision.status == "ambiguous"
    assert "JOHNS HOPKINS UNIVERSITY" in decision.ambiguous_candidates
    assert "Johns Hopkins Health System" in decision.ambiguous_candidates


def test_org_matching_accepts_exact_uei() -> None:
    decision = decide_organization_match(
        query_name="Johns Hopkins",
        query_uei="FTMTDMBR29C7",
        nih_candidates=[("JOHNS HOPKINS UNIVERSITY", "FTMTDMBR29C7")],
        trial_candidates=["Johns Hopkins Health System"],
    )

    assert decision.status == "matched"
    assert decision.confidence == "identifier"
    assert normalize_org_name("The Johns Hopkins, Inc.") == "JOHNS HOPKINS"

