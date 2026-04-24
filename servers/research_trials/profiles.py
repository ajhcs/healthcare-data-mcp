"""Aggregation helpers for research funding and trial activity profiles."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

from . import clinical_trials_client, reporter_client
from .models import (
    ClinicalTrialSearchResponse,
    FundingProfileResponse,
    NIHProject,
    ResearchActivityProfileResponse,
)
from .org_matching import decide_organization_match


def _year_list(years: list[int] | None) -> list[int]:
    return sorted({int(year) for year in years or []}, reverse=True)


def filter_projects_by_uei(projects: list[NIHProject], org_uei: str = "") -> list[NIHProject]:
    """Apply a conservative UEI post-filter to normalized NIH projects."""
    normalized = org_uei.strip().upper()
    if not normalized:
        return projects
    return [project for project in projects if project.organization.uei.upper() == normalized]


def aggregate_funding_profile(
    *,
    organization_search: str,
    org_uei: str = "",
    years: list[int] | None = None,
    projects: list[NIHProject],
    metadata: Any,
) -> FundingProfileResponse:
    """Aggregate normalized NIH projects into profile dimensions."""
    year_counts: dict[int, dict[str, Any]] = defaultdict(lambda: {"fiscal_year": 0, "project_count": 0, "award_amount": 0.0})
    institute_counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"institute": "", "project_count": 0, "award_amount": 0.0})
    pi_counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"pi_name": "", "project_count": 0, "award_amount": 0.0})
    activity_counts: dict[str, dict[str, Any]] = defaultdict(lambda: {"activity_code": "", "project_count": 0, "award_amount": 0.0})
    terms: Counter[str] = Counter()

    total_amount = 0.0
    for project in projects:
        amount = float(project.award_amount or 0)
        total_amount += amount

        if project.fiscal_year is not None:
            row = year_counts[project.fiscal_year]
            row["fiscal_year"] = project.fiscal_year
            row["project_count"] += 1
            row["award_amount"] += amount

        if project.activity_code:
            row = activity_counts[project.activity_code]
            row["activity_code"] = project.activity_code
            row["project_count"] += 1
            row["award_amount"] += amount

        if project.institute_fundings:
            for funding in project.institute_fundings:
                key = funding.abbreviation or funding.code or funding.name
                row = institute_counts[key]
                row["institute"] = key
                row["project_count"] += 1
                row["award_amount"] += funding.total_cost or amount
        elif project.agency_code:
            row = institute_counts[project.agency_code]
            row["institute"] = project.agency_code
            row["project_count"] += 1
            row["award_amount"] += amount

        for pi in project.principal_investigators:
            if not pi.full_name:
                continue
            row = pi_counts[pi.full_name]
            row["pi_name"] = pi.full_name
            row["project_count"] += 1
            row["award_amount"] += amount

        terms.update(term for term in project.terms if len(term) > 2)

    return FundingProfileResponse(
        organization_search=organization_search,
        org_uei=org_uei.strip().upper(),
        years=_year_list(years),
        total_projects=len(projects),
        total_award_amount=total_amount,
        by_fiscal_year=sorted(year_counts.values(), key=lambda row: row["fiscal_year"], reverse=True),
        by_institute=sorted(institute_counts.values(), key=lambda row: row["award_amount"], reverse=True),
        by_pi=sorted(pi_counts.values(), key=lambda row: row["award_amount"], reverse=True),
        by_activity_code=sorted(activity_counts.values(), key=lambda row: row["award_amount"], reverse=True),
        top_terms=[{"term": term, "count": count} for term, count in terms.most_common(15)],
        projects=projects,
        metadata=metadata,
    )


async def build_funding_profile(
    *,
    org_name: str = "",
    org_uei: str = "",
    years: list[int] | None = None,
    limit: int = 100,
) -> FundingProfileResponse | dict[str, str]:
    """Fetch and aggregate NIH funding activity for an organization."""
    if org_uei and not org_name:
        return {
            "error": (
                "NIH RePORTER v2 exposes UEI in responses but does not document UEI as a project-search criterion; "
                "provide org_name with org_uei to search and then post-filter."
            )
        }

    raw = await reporter_client.search_projects(org_name=org_name, fiscal_years=years, limit=limit, offset=0)
    if "error" in raw:
        return raw

    projects = [reporter_client.normalize_project(item) for item in raw.get("results") or []]
    projects = filter_projects_by_uei(projects, org_uei)
    return aggregate_funding_profile(
        organization_search=org_name,
        org_uei=org_uei,
        years=years,
        projects=projects,
        metadata=reporter_client.metadata_from_response(raw),
    )


async def build_research_activity_profile(
    *,
    organization_name: str,
    uei: str = "",
    facility_name: str = "",
    state: str = "",
    years: list[int] | None = None,
) -> ResearchActivityProfileResponse | dict[str, str]:
    """Build a conservative combined NIH funding and ClinicalTrials.gov profile."""
    funding = await build_funding_profile(org_name=organization_name, org_uei=uei, years=years)
    if isinstance(funding, dict):
        return funding

    trial_location = " ".join(part for part in [facility_name, state] if part)
    raw_trials = await clinical_trials_client.search_studies(
        sponsor=organization_name,
        location=trial_location,
        page_size=50,
    )
    if "error" in raw_trials:
        return raw_trials

    trials = [clinical_trials_client.normalize_study(item) for item in raw_trials.get("studies") or []]
    trial_response = ClinicalTrialSearchResponse(
        total_results=len(trials),
        trials=trials,
        metadata=clinical_trials_client.metadata_from_response(raw_trials, page_size=50),
    )

    nih_candidates = [(project.organization.name, project.organization.uei) for project in funding.projects]
    trial_candidates = []
    for trial in trials:
        if trial.lead_sponsor.name:
            trial_candidates.append(trial.lead_sponsor.name)
        if trial.organization:
            trial_candidates.append(trial.organization)

    match = decide_organization_match(
        query_name=organization_name,
        query_uei=uei,
        nih_candidates=nih_candidates,
        trial_candidates=trial_candidates,
    )

    warnings: list[str] = []
    combined_summary: dict[str, Any] = {
        "nih_project_count": funding.total_projects,
        "nih_total_award_amount": funding.total_award_amount,
        "clinical_trial_count": len(trials),
        "active_or_recruiting_trial_count": sum(
            1 for trial in trials if trial.overall_status in {"RECRUITING", "ACTIVE_NOT_RECRUITING", "ENROLLING_BY_INVITATION"}
        ),
    }

    if match.status == "ambiguous":
        warnings.append("Organization matching is ambiguous; NIH funding and trial activity are reported side by side without asserting a single merged entity.")
    elif match.status == "unmatched":
        warnings.append("No conservative cross-source organization match was found; combined totals should be treated as query activity, not entity-verified activity.")

    return ResearchActivityProfileResponse(
        organization_name=organization_name,
        uei=uei.strip().upper(),
        facility_name=facility_name,
        state=state.upper(),
        years=_year_list(years),
        match_decision=match,
        funding=funding,
        trials=trial_response,
        combined_summary=combined_summary,
        warnings=warnings,
    )

