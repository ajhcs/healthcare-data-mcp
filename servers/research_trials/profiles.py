"""Aggregation helpers for research funding and trial activity profiles."""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
from pathlib import Path
from typing import Any

from . import clinical_trials_client, reporter_client
from .models import (
    ClinicalTrialSearchResponse,
    FundingProfileResponse,
    NIHProject,
    ResearchActivityProfileResponse,
)
from .org_matching import decide_organization_match
from .org_matching import normalize_org_name

_ALIAS_CSV = Path(__file__).parent / "org_aliases.csv"
_ACTIVE_STATUSES = {"RECRUITING", "ACTIVE_NOT_RECRUITING", "ENROLLING_BY_INVITATION", "AVAILABLE"}


def _alias_map(entity_type: str = "", state: str = "") -> dict[str, str]:
    aliases: dict[str, str] = {}
    if not _ALIAS_CSV.exists():
        return aliases
    with open(_ALIAS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if entity_type and str(row.get("entity_type", "")).strip().lower() not in {"", entity_type.lower()}:
                continue
            if state and str(row.get("state", "")).strip().upper() not in {"", state.upper()}:
                continue
            alias = normalize_org_name(str(row.get("alias", "")))
            canonical = normalize_org_name(str(row.get("canonical_name", "")))
            if alias and canonical:
                aliases[alias] = canonical
    return aliases


def _min_date(values: list[str]) -> str:
    return min([value for value in values if value], default="")


def _max_date(values: list[str]) -> str:
    return max([value for value in values if value], default="")


def _match_basis_for_bucket(key: str, display_names: set[str], aliases: dict[str, str], ambiguous: bool) -> tuple[str, str]:
    if ambiguous:
        return "ambiguous", "low"
    if any(normalize_org_name(name) in aliases for name in display_names):
        return "known_alias", "high"
    if all(normalize_org_name(name) == key for name in display_names):
        return "exact_normalized_name", "high"
    return "high_similarity_singleton", "medium"


async def fetch_clinical_trial_scan(
    *,
    sponsor: str = "",
    location: str = "",
    status: str = "",
    page_size: int = 100,
    scan_limit: int = 500,
    hard_max: int = 5000,
) -> tuple[list[Any], Any, bool, str]:
    """Fetch paginated ClinicalTrials.gov results up to a conservative scan limit."""
    bounded_limit = max(1, min(int(scan_limit or 500), int(hard_max or 5000)))
    studies = []
    page_token = ""
    metadata = None
    truncated = False
    while len(studies) < bounded_limit:
        raw = await clinical_trials_client.search_studies(
            sponsor=sponsor,
            location=location,
            status=status,
            page_size=min(page_size, bounded_limit - len(studies), 100),
            page_token=page_token,
        )
        if "error" in raw:
            raise RuntimeError(str(raw["error"]))
        page = [clinical_trials_client.normalize_study(item) for item in raw.get("studies") or []]
        studies.extend(page)
        metadata = clinical_trials_client.metadata_from_response(raw, page_size=page_size)
        page_token = str(raw.get("nextPageToken") or "")
        if not page_token or not page:
            break
    if page_token:
        truncated = True
    return studies, metadata, truncated, page_token


async def inventory_clinical_trial_sponsors(
    *,
    sponsor: str,
    status: str = "",
    scan_limit: int = 500,
    hard_max: int = 5000,
) -> dict[str, Any]:
    aliases = _alias_map("sponsor")
    trials, metadata, truncated, next_page_token = await fetch_clinical_trial_scan(
        sponsor=sponsor,
        status=status,
        scan_limit=scan_limit,
        hard_max=hard_max,
    )
    buckets: dict[str, dict[str, Any]] = {}
    for trial in trials:
        sponsor_rows = []
        if trial.lead_sponsor.name:
            sponsor_rows.append((trial.lead_sponsor.name, trial.lead_sponsor.sponsor_class, "lead_sponsor"))
        sponsor_rows.extend((item.name, item.sponsor_class, "collaborator") for item in trial.collaborators if item.name)
        if trial.organization:
            sponsor_rows.append((trial.organization, "", "org_full_name"))
        for name, sponsor_class, role in sponsor_rows:
            norm = normalize_org_name(name)
            key = aliases.get(norm, norm)
            bucket = buckets.setdefault(
                key,
                {
                    "normalized_sponsor_name": key,
                    "display_names": set(),
                    "role_counts": Counter(),
                    "nct_ids": set(),
                    "classes": set(),
                    "study_dates": [],
                    "active_recruiting_nct_ids": set(),
                },
            )
            bucket["display_names"].add(name)
            bucket["role_counts"][role] += 1
            bucket["nct_ids"].add(trial.nct_id)
            if sponsor_class:
                bucket["classes"].add(sponsor_class)
            bucket["study_dates"].append(trial.start_date)
            if trial.overall_status in _ACTIVE_STATUSES:
                bucket["active_recruiting_nct_ids"].add(trial.nct_id)

    records = []
    normalized_counts = Counter(normalize_org_name(name) for bucket in buckets.values() for name in bucket["display_names"])
    for key, bucket in buckets.items():
        ambiguous = normalized_counts[key] > 1 and key not in aliases.values()
        match_basis, confidence = _match_basis_for_bucket(key, bucket["display_names"], aliases, ambiguous)
        role_counts = dict(bucket["role_counts"])
        records.append(
            {
                "normalized_sponsor_name": key,
                "display_names": sorted(bucket["display_names"]),
                "role_counts": role_counts,
                "lead_sponsor_count": role_counts.get("lead_sponsor", 0),
                "collaborator_count": role_counts.get("collaborator", 0),
                "org_full_name_count": role_counts.get("org_full_name", 0),
                "nct_ids": sorted(bucket["nct_ids"]),
                "classes": sorted(bucket["classes"]),
                "first_study_date": _min_date(bucket["study_dates"]),
                "last_study_date": _max_date(bucket["study_dates"]),
                "active_recruiting_count": len(bucket["active_recruiting_nct_ids"]),
                "match_basis": match_basis,
                "match_confidence": confidence,
            }
        )
    warnings = []
    if any(record["match_basis"] == "ambiguous" for record in records):
        warnings.append("ambiguous_entity_inventory: similar sponsor names were not collapsed without alias/identifier support.")
    if truncated:
        warnings.append("Inventory scan truncated at configured scan_limit; use next_page_token to continue.")
    return {
        "query": sponsor,
        "filters": {"status": status, "scan_limit": scan_limit},
        "metadata": metadata,
        "total_studies_scanned": len(trials),
        "unique_sponsor_count": len(records),
        "unique_site_count": 0,
        "records": records,
        "truncated": truncated,
        "next_page_token": next_page_token,
        "warnings": warnings,
    }


async def inventory_clinical_trial_sites(
    *,
    location: str,
    status: str = "",
    scan_limit: int = 500,
    hard_max: int = 5000,
) -> dict[str, Any]:
    trials, metadata, truncated, next_page_token = await fetch_clinical_trial_scan(
        location=location,
        status=status,
        scan_limit=scan_limit,
        hard_max=hard_max,
    )
    buckets: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    unresolved = 0
    for trial in trials:
        for site in trial.locations:
            if not site.facility:
                unresolved += 1
                continue
            key = (
                normalize_org_name(site.facility),
                site.city.strip().upper(),
                site.state.strip().upper(),
                site.country.strip().upper(),
                site.zip_code.strip(),
            )
            bucket = buckets.setdefault(
                key,
                {
                    "normalized_facility_name": key[0],
                    "display_names": set(),
                    "city": site.city,
                    "state": site.state,
                    "country": site.country,
                    "zip_code": site.zip_code,
                    "location_status_counts": Counter(),
                    "nct_ids": set(),
                    "update_dates": [],
                },
            )
            bucket["display_names"].add(site.facility)
            bucket["location_status_counts"][site.status or "UNKNOWN"] += 1
            bucket["nct_ids"].add(trial.nct_id)
            bucket["update_dates"].append(trial.last_update_posted)
    records = []
    for bucket in buckets.values():
        records.append(
            {
                "normalized_facility_name": bucket["normalized_facility_name"],
                "display_names": sorted(bucket["display_names"]),
                "city": bucket["city"],
                "state": bucket["state"],
                "country": bucket["country"],
                "zip_code": bucket["zip_code"],
                "location_status_counts": dict(bucket["location_status_counts"]),
                "nct_ids": sorted(bucket["nct_ids"]),
                "first_update_date": _min_date(bucket["update_dates"]),
                "last_update_date": _max_date(bucket["update_dates"]),
                "match_basis": "facility_city_state_country_zip",
                "match_confidence": "high",
            }
        )
    warnings = []
    if unresolved:
        warnings.append("Blank facility locations were excluded from exact site inventory.")
    if truncated:
        warnings.append("Inventory scan truncated at configured scan_limit; use next_page_token to continue.")
    return {
        "query": location,
        "filters": {"status": status, "scan_limit": scan_limit},
        "metadata": metadata,
        "total_studies_scanned": len(trials),
        "unique_sponsor_count": 0,
        "unique_site_count": len(records),
        "records": records,
        "unresolved_location_count": unresolved,
        "truncated": truncated,
        "next_page_token": next_page_token,
        "warnings": warnings,
    }


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
