"""ClinicalTrials.gov API v2 client and response normalization."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from shared.utils.http_client import resilient_request

from .models import ClinicalTrial, ClinicalTrialLocation, ClinicalTrialSponsor, ClinicalTrialsMetadata

logger = logging.getLogger(__name__)

BASE_URL = "https://clinicaltrials.gov/api/v2"
STUDIES_URL = f"{BASE_URL}/studies"
VERSION_URL = f"{BASE_URL}/version"
TIMEOUT = 30.0


def _str(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _date_struct_date(value: dict[str, Any] | None) -> str:
    if not isinstance(value, dict):
        return ""
    return _str(value.get("date"))


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_studies_params(
    *,
    query: str = "",
    sponsor: str = "",
    condition: str = "",
    intervention: str = "",
    location: str = "",
    status: str = "",
    phase: str = "",
    fields: list[str] | None = None,
    page_size: int = 25,
    page_token: str = "",
) -> dict[str, Any]:
    """Build ClinicalTrials.gov v2 study search query parameters."""
    params: dict[str, Any] = {
        "format": "json",
        "pageSize": _bounded_int(page_size, default=25, minimum=1, maximum=100),
    }
    if query:
        params["query.term"] = query
    if sponsor:
        params["query.spons"] = sponsor
    if condition:
        params["query.cond"] = condition
    if intervention:
        params["query.intr"] = intervention
    if location:
        params["query.locn"] = location
    if status:
        params["filter.overallStatus"] = status
    if phase:
        params["filter.phase"] = phase
    if fields:
        params["fields"] = ",".join(field for field in fields if field)
    if page_token:
        params["pageToken"] = page_token
    return params


async def get_version() -> dict[str, Any]:
    """Fetch ClinicalTrials.gov API version/data timestamp metadata."""
    try:
        resp = await resilient_request("GET", VERSION_URL, timeout=TIMEOUT)
        return resp.json()
    except Exception as exc:
        logger.warning("ClinicalTrials.gov version lookup failed: %s", exc)
        return {"error": str(exc)}


async def search_studies(
    *,
    query: str = "",
    sponsor: str = "",
    condition: str = "",
    intervention: str = "",
    location: str = "",
    status: str = "",
    phase: str = "",
    fields: list[str] | None = None,
    page_size: int = 25,
    page_token: str = "",
) -> dict[str, Any]:
    """Search ClinicalTrials.gov studies and return raw API JSON plus version metadata."""
    params = build_studies_params(
        query=query,
        sponsor=sponsor,
        condition=condition,
        intervention=intervention,
        location=location,
        status=status,
        phase=phase,
        fields=fields,
        page_size=page_size,
        page_token=page_token,
    )
    try:
        version = await get_version()
        resp = await resilient_request("GET", STUDIES_URL, params=params, timeout=TIMEOUT)
        data = resp.json()
        data["_version"] = version
        data["_request_params"] = params
        return data
    except Exception as exc:
        logger.warning("ClinicalTrials.gov study search failed: %s", exc)
        return {"error": str(exc), "request": params}


async def get_study(nct_id: str) -> dict[str, Any]:
    """Fetch one ClinicalTrials.gov v2 study by NCT ID."""
    normalized = nct_id.strip().upper()
    try:
        version = await get_version()
        resp = await resilient_request("GET", f"{STUDIES_URL}/{normalized}", params={"format": "json"}, timeout=TIMEOUT)
        data = resp.json()
        data["_version"] = version
        return data
    except Exception as exc:
        logger.warning("ClinicalTrials.gov study detail failed for %s: %s", normalized, exc)
        return {"error": str(exc), "nct_id": normalized}


def normalize_study(raw: dict[str, Any]) -> ClinicalTrial:
    """Normalize a ClinicalTrials.gov v2 study object."""
    protocol = raw.get("protocolSection") or {}
    derived = raw.get("derivedSection") or {}
    ident = protocol.get("identificationModule") or {}
    status = protocol.get("statusModule") or {}
    sponsors = protocol.get("sponsorCollaboratorsModule") or {}
    conditions = protocol.get("conditionsModule") or {}
    design = protocol.get("designModule") or {}
    interventions_mod = protocol.get("armsInterventionsModule") or {}
    contacts = protocol.get("contactsLocationsModule") or {}

    lead = sponsors.get("leadSponsor") or {}
    collaborators = [
        ClinicalTrialSponsor(name=_str(item.get("name")), sponsor_class=_str(item.get("class")), role="collaborator")
        for item in sponsors.get("collaborators") or []
    ]

    interventions = [
        ": ".join(part for part in [_str(item.get("type")), _str(item.get("name"))] if part)
        for item in interventions_mod.get("interventions") or []
    ]

    locations = [
        ClinicalTrialLocation(
            facility=_str(item.get("facility")),
            status=_str(item.get("status")),
            city=_str(item.get("city")),
            state=_str(item.get("state")),
            country=_str(item.get("country")),
            zip_code=_str(item.get("zip")),
        )
        for item in contacts.get("locations") or []
    ]

    officials = []
    for item in contacts.get("overallOfficials") or []:
        officials.append(
            {
                "name": _str(item.get("name")),
                "affiliation": _str(item.get("affiliation")),
                "role": _str(item.get("role")),
            }
        )

    nct_id = _str(ident.get("nctId"))
    misc = derived.get("miscInfoModule") or {}
    enrollment = design.get("enrollmentInfo") or {}
    org = ident.get("organization") or {}

    return ClinicalTrial(
        nct_id=nct_id,
        brief_title=_str(ident.get("briefTitle")),
        official_title=_str(ident.get("officialTitle")),
        organization=_str(org.get("fullName")),
        overall_status=_str(status.get("overallStatus")),
        study_type=_str(design.get("studyType")),
        phases=[_str(phase) for phase in design.get("phases") or []],
        conditions=[_str(condition) for condition in conditions.get("conditions") or []],
        interventions=[value for value in interventions if value],
        lead_sponsor=ClinicalTrialSponsor(
            name=_str(lead.get("name")),
            sponsor_class=_str(lead.get("class")),
            role="lead_sponsor",
        ),
        collaborators=collaborators,
        locations=locations,
        overall_officials=officials,
        start_date=_date_struct_date(status.get("startDateStruct")),
        primary_completion_date=_date_struct_date(status.get("primaryCompletionDateStruct")),
        completion_date=_date_struct_date(status.get("completionDateStruct")),
        last_update_posted=_date_struct_date(status.get("lastUpdatePostDateStruct")),
        enrollment=_int_or_none(enrollment.get("count")),
        version_holder=_str(misc.get("versionHolder")),
        url=f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else "",
    )


def metadata_from_response(raw: dict[str, Any], page_size: int = 0) -> ClinicalTrialsMetadata:
    """Build ClinicalTrials.gov source metadata from a search/detail response."""
    version = raw.get("_version") or {}
    return ClinicalTrialsMetadata(
        source_name="ClinicalTrials.gov",
        source_url=STUDIES_URL,
        api_version=_str(version.get("apiVersion")),
        data_timestamp=_str(version.get("dataTimestamp")),
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        next_page_token=_str(raw.get("nextPageToken")),
        page_size=page_size,
    )


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))
