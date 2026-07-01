"""Public Records & Regulatory MCP Server.

Provides tools for federal spending, HIPAA breaches,
accreditation, and interoperability data. Port 8013.
"""

from typing import Any
import csv
from datetime import UTC, datetime
import json
import logging
import os as _os
from pathlib import Path
import re


from shared.utils.http_client import resilient_request
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared import state_health_data

from . import data_loaders, usaspending_client, sam_client, sam_exclusions_client  # pyright: ignore[reportAttributeAccessIssue]
from .models import (
    OIG_LEIE_CAVEAT,
    SAM_EXCLUSIONS_CAVEAT,
    USAspendingAward,
    USAspendingResponse,
    SAMOpportunity,
    SAMResponse,
    CISAKevContext,
    CyberIncidentRecord,
    CyberSourceStatus,
    BreachRecord,
    BreachHistoryResponse,
    AccreditationRecord,
    AccreditationResponse,
    InteropRecord,
    InteropResponse,
    LEIEBatchResponse,
    LEIEBatchResult,
    LEIEBatchCandidate,
    LEIEExclusionRecord,
    LEIESearchResponse,
    LEIESourceMetadata,
    SAMExclusionBatchCandidate,
    SAMExclusionBatchResponse,
    SAMExclusionBatchResult,
    SAMExclusionRecord,
    SAMExclusionSearchResponse,
    SAMExclusionsSourceMetadata,
)
from shared.utils.healthcare_identity import MatchDecision, identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_name, normalize_npi, normalize_state

logger = logging.getLogger(__name__)

_SENSITIVE_IDENTIFIER_KEYS = {
    "ssn",
    "social_security_number",
    "social_security_num",
    "social_security",
    "ein",
    "fein",
    "tin",
    "tax_id",
    "tax_identifier",
    "taxpayer_id",
    "taxpayer_identifier",
    "taxpayer_identification_number",
    "employer_identification_number",
    "federal_tax_id",
    "federal_tax_identifier",
}

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "public-records"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8013"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "public-records")

# Load accreditation code lookup once at module level
_ACCR_CODES: dict[str, str] = {}
_codes_csv = Path(__file__).parent / "data" / "accreditation_codes.csv"
if _codes_csv.exists():
    with open(_codes_csv, newline="") as _f:
        for _row in csv.DictReader(_f):
            _ACCR_CODES[_row["code"].strip()] = _row["organization"].strip()


# ---------------------------------------------------------------------------
# CHPL API helper (optional enrichment for interop tool)
# ---------------------------------------------------------------------------

async def _lookup_chpl(cehrt_id: str, api_key: str) -> dict:
    """Look up EHR certification details from ONC CHPL API.

    Returns dict with ehr_product_name and ehr_developer, or empty dict on failure.
    """
    url = f"https://chpl.healthit.gov/rest/certification_ids/{cehrt_id}"
    try:
        resp = await resilient_request("GET", url, headers={"API-key": api_key}, timeout=15.0)
        data = resp.json()
        products = data.get("products", [])
        if products:
            product = products[0]
            return {
                "ehr_product_name": product.get("name", ""),
                "ehr_developer": product.get("developer", ""),
            }
        return {}
    except Exception as e:
        logger.debug("CHPL lookup failed for %s: %s", cehrt_id, e)
        return {}


# ---------------------------------------------------------------------------
# LEIE response helpers
# ---------------------------------------------------------------------------

def _leie_source_metadata(data: dict) -> LEIESourceMetadata:
    return LEIESourceMetadata(
        **{k: v for k, v in data.items() if k in LEIESourceMetadata.model_fields}
    )


def _leie_records(rows: list[dict]) -> list[LEIEExclusionRecord]:
    return [
        LEIEExclusionRecord(**{k: v for k, v in row.items() if k in LEIEExclusionRecord.model_fields})
        for row in rows
    ]


def _leie_status(records: list[LEIEExclusionRecord]) -> str:
    if not records:
        return "no_current_leie_match_found"
    if any(record.verification_status == "strong_potential_match" for record in records):
        return "strong_potential_match"
    return "potential_match"


def _leie_evidence(
    metadata: dict[str, Any],
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=metadata,
        dataset_id="hhs_oig_leie",
        entity_scope="current_exclusion_screening",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=OIG_LEIE_CAVEAT,
        next_step="Use OIG's online searchable database and documented follow-up process for SSN/EIN-level verification.",
    )


def _identity_with_match_decision(
    *,
    name: Any = "",
    entity_type: str = "",
    npi: Any = "",
    address: Any = "",
    zip_code: Any = "",
    source_name: str = "",
    source_url: str = "",
    match_basis: str = "",
    confidence: str = "",
    notes: str = "",
    unresolved_identifiers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=name,
        entity_type=entity_type,
        npi=npi,
        address=address,
        zip_code=zip_code,
        source_name=source_name,
        source_url=source_url,
    )
    if match_basis or confidence:
        identity.match_decisions.append(
            MatchDecision(
                basis=match_basis,
                confidence=confidence,
                decided_at=datetime.now(UTC).isoformat(),
                notes=notes,
            )
        )
    for identifier_type, value in (unresolved_identifiers or {}).items():
        if value not in (None, ""):
            identity.unresolved_identifiers.append(
                {"type": identifier_type, "value": str(value).strip()}
            )
    return identity.to_dict()


def _leie_identity(
    *,
    records: list[LEIEExclusionRecord],
    query: dict[str, Any],
    metadata: dict[str, Any],
    candidate: dict[str, Any] | None = None,
    match_basis: str = "",
    confidence: str = "",
) -> dict[str, Any]:
    source_name = str(metadata.get("source_name") or "HHS OIG LEIE")
    source_url = str(metadata.get("source_url") or "")
    first_record = records[0] if records else None
    candidate = candidate or {}
    query_name = str(
        query.get("entity_name")
        or candidate.get("entity_name")
        or " ".join(
            part
            for part in (
                query.get("first_name") or candidate.get("first_name"),
                query.get("last_name") or candidate.get("last_name"),
            )
            if part
        )
    ).strip()
    name = (
        first_record.display_name
        if first_record
        else query_name
    )
    entity_type = (
        first_record.entity_type
        if first_record
        else str(candidate.get("entity_type") or ("individual" if query.get("last_name") or candidate.get("last_name") else ""))
    )
    return _identity_with_match_decision(
        name=name,
        entity_type=entity_type,
        npi=(first_record.npi if first_record else query.get("npi") or candidate.get("npi")),
        address=(first_record.address if first_record else ""),
        zip_code=(first_record.zip_code if first_record else ""),
        source_name=source_name,
        source_url=source_url,
        match_basis=match_basis,
        confidence=confidence,
        notes="LEIE identity is screening-only; SSN/EIN-level verification is not available in the downloadable public file.",
    )


def _normalized_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _contains_sensitive_identifier_keys(payload: dict[str, Any]) -> bool:
    return bool(_SENSITIVE_IDENTIFIER_KEYS & {_normalized_key(key) for key in payload})


# ---------------------------------------------------------------------------
# Cyber incident enrichment helpers
# ---------------------------------------------------------------------------

_STATE_BREACH_SOURCE_CSV = Path(__file__).parent / "data" / "state_breach_notice_sources.csv"
_STATE_BREACH_SOURCE_LOCATOR_URL = "https://www.naag.org/find-my-ag/"


def _state_breach_notice_source_statuses() -> dict[str, CyberSourceStatus]:
    statuses: dict[str, CyberSourceStatus] = {}
    if _STATE_BREACH_SOURCE_CSV.exists():
        with open(_STATE_BREACH_SOURCE_CSV, newline="", encoding="utf-8") as source_file:
            for row in csv.DictReader(source_file):
                state = str(row.get("state", "")).strip().upper()
                if not state:
                    continue
                statuses[state] = CyberSourceStatus(
                    source_name=str(row.get("source_name", "")),
                    source_type="state_ag_breach_notice",
                    status=str(row.get("status", "")),
                    reason=str(row.get("reason", "")),
                    source_url=str(row.get("source_url", "")),
                    next_step=str(row.get("next_step", "")),
                )
    return statuses


def _cyber_source_statuses(states: list[str] | None = None) -> dict[str, dict[str, Any]]:
    requested = [state.strip().upper() for state in states or [] if state.strip()]
    keys = requested or ["PA", "NJ", "DE"]
    statuses: dict[str, dict[str, Any]] = {}
    configured = _state_breach_notice_source_statuses()
    for state in keys:
        status = configured.get(
            state,
            CyberSourceStatus(
                source_name=f"{state} state AG breach notices",
                source_type="state_ag_breach_notice",
                status="not_searchable",
                reason="No state-specific public breach notice search status has been configured.",
                next_step="Add a reviewed source status before using this state for automated enrichment.",
            ),
        )
        statuses[state] = status.model_dump()
    return statuses


def _state_breach_notice_evidence_url(
    *,
    state: str = "",
    states: list[str] | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> str:
    """Return the best public locator URL for state breach notice evidence."""

    metadata_url = str((source_metadata or {}).get("source_url") or "").strip()
    if metadata_url:
        return metadata_url

    requested_states = [state.strip().upper()] if state.strip() else [
        item.strip().upper() for item in states or [] if item.strip()
    ]
    configured = _state_breach_notice_source_statuses()
    configured_urls = [
        str(configured[item].source_url or "").strip()
        for item in requested_states
        if item in configured and str(configured[item].source_url or "").strip()
    ]
    unique_urls = sorted(set(configured_urls))
    if len(unique_urls) == 1:
        return unique_urls[0]
    return _STATE_BREACH_SOURCE_LOCATOR_URL


def _breach_incident_type(breach: dict[str, Any]) -> str:
    haystack = json.dumps(breach).lower()
    if "ransomware" in haystack:
        return "ransomware"
    if "malware" in haystack:
        return "malware"
    if "phishing" in haystack:
        return "phishing"
    if "hacking" in haystack or "unauthorized access" in haystack:
        return "unauthorized_access"
    if "network" in haystack or "it incident" in haystack or "cyber" in haystack:
        return "cybersecurity_incident"
    return ""


def _breach_incident_type_confidence(breach: dict[str, Any]) -> str:
    incident_type = _breach_incident_type(breach)
    if incident_type in {"ransomware", "malware", "phishing", "unauthorized_access"}:
        return "high"
    if incident_type:
        return "medium"
    return "low"


def _public_record_evidence(
    *,
    source_name: str,
    source_url: str,
    dataset_id: str,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = source_metadata or {}
    return evidence_receipt(
        source_metadata=metadata,
        source_name=source_name,
        source_url=source_url or str(metadata.get("source_url") or ""),
        dataset_id=dataset_id,
        source_period=_public_source_period(dataset_id, query, metadata),
        cache_status=_public_cache_status(metadata),
        cache_freshness=_public_cache_freshness(metadata),
        entity_scope="public_records",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
    )


def _public_record_source_type(dataset_id: str) -> str:
    if dataset_id in {
        "hhs_ocr_breach_portal",
        "hhs_ocr_enforcement_actions",
        "sec_cyber_disclosures",
        "state_ag_breach_notices",
        "public_cyber_incident_profile",
        "unsupported_cybersecurity_attestation",
        "cisa_kev_context",
    }:
        return "public_cyber_breach_record"
    return "public_record_source"


def _public_record_source_metadata(evidence: dict[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a public-record evidence receipt."""

    dataset_id = str(evidence.get("dataset_id") or "")
    return {
        "source_name": evidence.get("source_name", ""),
        "source_url": evidence.get("source_url", ""),
        "dataset_id": dataset_id,
        "source_period": evidence.get("source_period", ""),
        "landing_page": evidence.get("landing_page", ""),
        "retrieved_at": evidence.get("retrieved_at", ""),
        "source_modified": evidence.get("source_modified", ""),
        "cache_status": evidence.get("cache_status", ""),
        "cache_freshness": evidence.get("cache_freshness", ""),
        "entity_scope": evidence.get("entity_scope", "public_records"),
        "query": evidence.get("query", {}),
        "cache_key": evidence.get("cache_key", ""),
        "source_type": _public_record_source_type(dataset_id),
    }


def _attach_public_record_source_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        payload["source_metadata"] = _public_record_source_metadata(evidence)
    return payload


def _public_record_row_evidence(
    *,
    row: dict[str, Any],
    parent_query: dict[str, Any],
    source_name: str,
    source_url: str,
    dataset_id: str,
    match_basis: str,
    confidence: str,
    caveat: str,
    next_step: str,
    source_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row_source_url = str(row.get("source_url") or row.get("evidence_url") or source_url or "").strip()
    row_query = {
        **parent_query,
        "row_entity_name": row.get("entity_name") or row.get("title") or "",
        "row_state": row.get("state") or "",
        "row_title": row.get("title") or "",
        "row_source_type": row.get("source_type") or "",
        "row_incident_type": row.get("incident_type") or row.get("breach_type") or "",
        "row_incident_type_confidence": row.get("incident_type_confidence") or "",
        "row_entity_match_confidence": row.get("entity_match_confidence") or "",
        "row_confidence": row.get("confidence") or "",
        "row_individuals_affected": row.get("individuals_affected") or row.get("affected_individuals") or "",
        "row_date": row.get("date") or row.get("disclosure_date") or row.get("breach_submission_date") or row.get("filing_date") or "",
        "row_accession": row.get("accession_number") or row.get("accession") or "",
        "row_award_id": row.get("award_id") or row.get("Award ID") or "",
        "row_notice_id": row.get("notice_id") or row.get("noticeId") or "",
        "row_solicitation_number": row.get("solicitation_number") or row.get("solicitationNumber") or "",
        "row_source_file": row.get("source_file") or "",
    }
    row_query = {key: value for key, value in row_query.items() if value not in ("", None, [], {})}
    return _public_record_evidence(
        source_name=source_name,
        source_url=row_source_url,
        dataset_id=dataset_id,
        query=row_query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=caveat,
        next_step=next_step,
        source_metadata=source_metadata,
    )


def _public_source_period(dataset_id: str, query: dict[str, Any], metadata: dict[str, Any]) -> str:
    if metadata.get("source_period"):
        return str(metadata["source_period"])
    if dataset_id == "sec_cyber_disclosures":
        start = query.get("start_date") or "unbounded"
        end = query.get("end_date") or "latest indexed filing"
        return f"{start} to {end}"
    if dataset_id == "state_ag_breach_notices":
        start = query.get("start_date") or "unbounded"
        end = query.get("end_date") or "latest reviewed import"
        return f"{start} to {end}"
    if dataset_id == "hhs_ocr_breach_portal":
        return "current imported HHS OCR large-breach report cache"
    if dataset_id == "public_cyber_incident_profile":
        return "current imported public cyber/breach source caches at query time"
    if dataset_id == "unsupported_cybersecurity_attestation":
        return "not applicable; no reviewed public attestation source is configured"
    return "latest available public-record source at query time"


def _public_cache_status(metadata: dict[str, Any]) -> str:
    status = str(metadata.get("cache_status") or metadata.get("status") or "").strip()
    if status:
        return status
    if metadata.get("cache_path"):
        return "ready"
    return "not_applicable"


def _public_cache_freshness(metadata: dict[str, Any]) -> str:
    parts: list[str] = []
    status = _public_cache_status(metadata)
    if status:
        parts.append(status)
    if metadata.get("record_count") not in ("", None):
        parts.append(f"record_count={metadata['record_count']}")
    if metadata.get("cache_path"):
        parts.append(f"cache_path={metadata['cache_path']}")
    if metadata.get("reason"):
        parts.append(str(metadata["reason"]))
    if metadata.get("next_step") and status != "ready":
        parts.append(f"next_step={metadata['next_step']}")
    return "; ".join(parts) or "source freshness is not cache-managed for this public-record lookup"


def _breach_source_metadata() -> dict[str, Any]:
    cache_path = data_loaders._BREACH_PARQUET
    csv_path = data_loaders._BREACH_CSV
    source_path = cache_path if cache_path.exists() else csv_path
    metadata: dict[str, Any] = {
        "source_name": "HHS OCR Breach Portal",
        "source_url": "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
        "dataset_id": "hhs_ocr_breach_portal",
        "source_period": "current imported HHS OCR large-breach report cache",
        "status": "ready" if source_path.exists() else "missing",
        "cache_path": str(cache_path),
        "next_step": "Refresh the manually downloaded OCR breach CSV when source recency matters.",
    }
    if source_path.exists():
        metadata["source_modified"] = datetime.fromtimestamp(source_path.stat().st_mtime, UTC).isoformat()
    return metadata


def _public_cache_file_metadata(
    *,
    source_name: str,
    source_url: str,
    dataset_id: str,
    cache_path: Path,
    loaded: bool,
    source_period: str,
    next_step: str,
) -> dict[str, Any]:
    """Return source metadata for public-record caches without downloading."""

    metadata: dict[str, Any] = {
        "source_name": source_name,
        "source_url": source_url,
        "dataset_id": dataset_id,
        "source_period": source_period,
        "status": "ready" if loaded and cache_path.exists() else "missing",
        "cache_status": "ready" if loaded and cache_path.exists() else "missing",
        "cache_path": str(cache_path),
        "next_step": next_step,
    }
    if cache_path.exists():
        metadata["source_modified"] = datetime.fromtimestamp(cache_path.stat().st_mtime, UTC).isoformat()
    return metadata


def _public_facility_identity(
    *,
    rows: list[dict[str, Any]],
    query: dict[str, Any],
    source_name: str,
    source_url: str,
    match_basis: str,
    confidence: str,
):
    first = rows[0] if rows else {}
    identity = identity_from_public_record(
        name=first.get("provider_name") or first.get("facility_name") or query.get("provider_name") or query.get("facility_name") or "",
        entity_type="facility",
        ccn=first.get("ccn") or query.get("ccn") or "",
        address=first.get("address") or "",
        zip_code=first.get("zip_code") or "",
        source_name=source_name,
        source_url=source_url,
    )
    if query.get("state") and not normalize_state(query.get("state")):
        identity.unresolved_identifiers.append({"type": "state", "value": str(query["state"])})
    identity.match_decisions.append(
        MatchDecision(
            basis=match_basis,
            confidence=confidence,
            notes="Public facility regulatory identity is anchored on CCN when present; names are candidate filters.",
        )
    )
    return identity


def _public_facility_identity_map(
    *,
    rows: list[dict[str, Any]],
    query: dict[str, Any],
    dataset_id: str,
    collection: str,
) -> dict[str, Any]:
    ccns = sorted({normalize_ccn(row.get("ccn")) or "" for row in rows if normalize_ccn(row.get("ccn"))})
    query_ccn = normalize_ccn(query.get("ccn"))
    if query_ccn:
        ccns = sorted(set([*ccns, query_ccn]))
    names = sorted(
        {
            normalized
            for value in (
                *(row.get("provider_name") or row.get("facility_name") or "" for row in rows),
                query.get("provider_name") or query.get("facility_name") or "",
            )
            if (normalized := normalize_name(value, remove_legal_suffixes=True))
        }
    )
    states = sorted(
        {
            normalized
            for value in (*(row.get("state") or "" for row in rows), query.get("state") or "")
            if (normalized := normalize_state(value))
        }
    )
    return {
        "entity_scope": "public_records_facility_regulatory",
        "join_keys": [
            {
                "field": "ccn",
                "values": ccns,
                "status": "provided" if ccns else "missing",
                "used_by": [collection] if ccns else [],
            },
            {
                "field": "canonical_name",
                "values": names,
                "status": "candidate" if names and not ccns else ("provided" if names else "missing"),
                "used_by": [collection] if names else [],
            },
            {
                "field": "state",
                "values": states,
                "status": "provided" if states else "missing",
                "used_by": [collection] if states else [],
            },
        ],
        "source_claims": [
            _public_source_claim(
                collection=collection,
                dataset_id=dataset_id,
                match_policy="ccn_exact_required_for_facility_identity_claim",
                row_evidence_paths=[f"{collection}[].evidence"] if rows else [],
            )
        ],
        "conflict_policy": [
            "Use CCN as the exact public facility join key when present.",
            "Treat facility/provider names and state filters as candidate matching context unless CCN also matches.",
            "Do not substitute accreditation, PI, or CHPL adjacent records for cybersecurity or certification claims outside their source fields.",
        ],
        "missing_data_policy": (
            "No-match public-record regulatory responses identify the searched CMS public-source scope; "
            "they are not proof of no accreditation, no PI participation, or no certified health IT."
        ),
}


def _public_source_claim(
    *,
    collection: str,
    dataset_id: str = "",
    match_policy: str,
    identity_paths: list[str] | None = None,
    row_evidence_paths: list[str] | None = None,
) -> dict[str, Any]:
    claim: dict[str, Any] = {
        "collection": collection,
        "identity_paths": identity_paths or ["evidence.query"],
        "evidence_path": "evidence",
        "source_metadata_path": "source_metadata",
        "match_policy": match_policy,
    }
    if dataset_id:
        claim["dataset_id"] = dataset_id
    if row_evidence_paths:
        claim["row_evidence_paths"] = row_evidence_paths
    return claim


def _public_no_match_basis(match_basis: str) -> str:
    return match_basis if match_basis.endswith("_no_match") else f"{match_basis}_no_match"


def _public_api_search_metadata(
    *,
    source_name: str,
    source_url: str,
    dataset_id: str,
    query: dict[str, Any],
    cache_prefix: str,
    cache_hit: bool,
    record_count: int,
    docs_url: str = "",
) -> dict[str, Any]:
    cache_path = data_loaders._api_cache_path(cache_prefix, query)
    metadata: dict[str, Any] = {
        "source_name": source_name,
        "source_url": source_url,
        "landing_page": docs_url,
        "dataset_id": dataset_id,
        "source_period": _public_source_period(dataset_id, query, {}),
        "status": "cache_hit" if cache_hit else "live_api",
        "cache_status": "hit" if cache_hit else "written",
        "cache_key": str(cache_path),
        "record_count": record_count,
        "query": query,
        "next_step": "Review returned public records and preserve source URLs/identifiers before citing award or opportunity facts.",
    }
    if cache_hit and cache_path.exists():
        try:
            cached_payload = json.loads(cache_path.read_text(encoding="utf-8"))
            metadata["retrieved_at"] = str(cached_payload.get("cached_at") or "")
        except Exception:
            metadata["retrieved_at"] = datetime.now(UTC).isoformat()
    else:
        metadata["retrieved_at"] = datetime.now(UTC).isoformat()
    return metadata


def _public_search_identity(
    *,
    name: str,
    source_name: str,
    source_url: str,
    match_basis: str,
    confidence: str,
):
    identity = identity_from_public_record(
        name=name,
        entity_type="organization",
        source_name=source_name,
        source_url=source_url,
    )
    identity.match_decisions.append(
        MatchDecision(
            basis=match_basis,
            confidence=confidence,
            notes="Federal spending/opportunity search terms are candidate organization aliases unless exact source identifiers support the join.",
        )
    )
    return identity


def _public_api_search_identity_map(
    *,
    query_name: str,
    observed_names: list[str],
    dataset_id: str,
    collection: str,
) -> dict[str, Any]:
    names = sorted(
        {
            normalized
            for value in [query_name, *observed_names]
            if (normalized := normalize_name(value, remove_legal_suffixes=True))
        }
    )
    identity_paths = (
        ["awards[].recipient_name", "awards[].award_id"]
        if collection == "awards"
        else ["keyword", "opportunities[].notice_id", "opportunities[].solicitation_number"]
    )
    return {
        "entity_scope": "public_records_federal_search",
        "join_keys": [
            {
                "field": "canonical_name",
                "values": names,
                "status": "candidate" if names else "missing",
                "used_by": [collection] if names else [],
            }
        ],
        "source_claims": [
            {
                "collection": collection,
                "dataset_id": dataset_id,
                "identity_paths": identity_paths,
                "evidence_path": "evidence",
                "source_metadata_path": "source_metadata",
                "row_evidence_paths": [f"{collection}[].evidence"],
                "match_policy": "candidate_public_records_search_not_identity_proof",
            }
        ],
        "conflict_policy": [
            "Treat recipient names, opportunity keywords, and award titles as candidate aliases.",
            "Do not merge federal awards or opportunities into a facility/system identity without exact source identifiers or reviewed source URLs.",
        ],
        "missing_data_policy": (
            "No-result federal spending or opportunity searches identify only the queried API/filter scope; "
            "they are not proof that no awards, grants, contracts, or solicitations exist."
        ),
    }


def _breach_entity_match_confidence(entity_name: str, breach: dict[str, Any]) -> str:
    query_name = entity_name.strip()
    breach_name = str(breach.get("entity_name", "")).strip()
    if not query_name:
        return "not_requested"
    if breach_name and (query_name.lower() in breach_name.lower() or breach_name.lower() in query_name.lower()):
        return "high"
    if query_name.lower() in json.dumps(breach).lower():
        return "medium"
    return "low"


def _cyber_incident_from_breach(entity_name: str, breach: dict[str, Any]) -> dict[str, Any]:
    incident_type_confidence = _breach_incident_type_confidence(breach)
    entity_match_confidence = _breach_entity_match_confidence(entity_name, breach)
    return CyberIncidentRecord(
        entity_name=str(breach.get("entity_name", "")),
        state=str(breach.get("state", "")),
        incident_type=_breach_incident_type(breach),
        incident_date="",
        disclosure_date=str(breach.get("breach_submission_date", "")),
        date=str(breach.get("breach_submission_date", "")),
        individuals_affected=int(breach.get("individuals_affected") or 0),
        title=str(breach.get("entity_name", "")),
        summary=str(breach.get("web_description", "")),
        source_type="hhs_ocr_breach_portal",
        entity_match_confidence=entity_match_confidence,
        incident_type_confidence=incident_type_confidence,
        timeline_disclosed=bool(breach.get("breach_submission_date")),
        timeline_inferred=False,
        confidence=(
            "high"
            if entity_match_confidence == "high" and incident_type_confidence == "high"
            else "medium"
            if incident_type_confidence in {"high", "medium"}
            else "low"
        ),
    ).model_dump()


_CYBER_NO_ASSURANCE_CAVEAT = (
    "Public cyber/breach records are incomplete, source-specific public records; "
    "a zero-result response is not proof that no incident, breach, enforcement "
    "action, disclosure, state notice, or cybersecurity issue exists."
)


_PHC4_CAVEAT = (
    "PHC4 public reports are indexed public documents and extracted table rows where available. "
    "They are not paid PHC4 discharge datasets and should not be used as substitutes for exact CMS quality facts."
)


def _phc4_source_url(payload: dict[str, Any]) -> str:
    for row in [*payload.get("reports", []), *payload.get("table_rows", [])]:
        if not isinstance(row, dict):
            continue
        for key in ("url", "artifact_url", "source_artifact", "landing_page_url"):
            value = str(row.get(key) or "").strip()
            if value:
                return value
    return state_health_data.PHC4_REPORT_LIBRARY_URL


def _phc4_source_metadata(payload: dict[str, Any], *, query: dict[str, Any]) -> dict[str, Any]:
    reports = payload.get("reports") if isinstance(payload.get("reports"), list) else []
    table_rows = payload.get("table_rows") if isinstance(payload.get("table_rows"), list) else []
    years = sorted(
        {
            str(value)
            for row in [*reports, *table_rows]
            if isinstance(row, dict)
            for value in (row.get("year"), row.get("report_year"), row.get("publication_year"))
            if value not in ("", None)
        }
    )
    requested_year = query.get("year") or query.get("fiscal_year")
    source_period = ", ".join(years) if years else (str(requested_year) if requested_year else "latest indexed PHC4 public reports")
    return {
        "source_name": "PHC4 Public Reports Library",
        "source_url": _phc4_source_url(payload),
        "dataset_id": "phc4_public_reports",
        "source_period": source_period,
        "landing_page_url": state_health_data.PHC4_REPORT_LIBRARY_URL,
        "queried_at": datetime.now(UTC).isoformat(),
        "cache_status": str(payload.get("source_status") or "public_report_index"),
        "cache_freshness": "current local PHC4 public report index at query time",
        "record_count": len(reports),
        "table_row_count": len(table_rows),
        "query": query,
    }


def _phc4_evidence(
    payload: dict[str, Any],
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
) -> dict[str, Any]:
    metadata = _phc4_source_metadata(payload, query=query)
    return evidence_receipt(
        source_metadata=metadata,
        source_name="PHC4 Public Reports Library",
        source_url=metadata["source_url"],
        dataset_id="phc4_public_reports",
        source_period=metadata["source_period"],
        cache_status=metadata["cache_status"],
        cache_freshness=metadata["cache_freshness"],
        entity_scope="phc4_public_report",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=_PHC4_CAVEAT,
        next_step=next_step,
    )


def _phc4_identity(payload: dict[str, Any], *, query: dict[str, Any]) -> dict[str, Any] | None:
    hospital_name = str(query.get("hospital_name") or payload.get("hospital_name") or query.get("query") or "").strip()
    if not hospital_name:
        return None
    identity = identity_from_public_record(
        name=hospital_name,
        entity_type="hospital",
        source_name="PHC4 Public Reports Library",
        source_url=_phc4_source_url(payload),
    )
    identity.match_decisions.append(
        MatchDecision(
            basis="phc4_public_report_name_or_query_match",
            confidence=str(payload.get("confidence") or "public_report_index_context"),
            decided_at=datetime.now(UTC).isoformat(),
            notes=(
                "PHC4 identity is source-scoped public-report context. "
                "Hospital names, report titles, and procedure labels are candidate joins unless a stable facility identifier is also present."
            ),
        )
    )
    for identifier_type, value in (
        ("year", query.get("year")),
        ("fiscal_year", query.get("fiscal_year")),
        ("report_type", query.get("report_type") or payload.get("report_type")),
        ("procedure", query.get("procedure") or payload.get("procedure")),
    ):
        if value not in ("", None, 0):
            identity.unresolved_identifiers.append({"type": identifier_type, "value": str(value)})
    return identity.to_dict()


def _phc4_identity_map(payload: dict[str, Any], *, query: dict[str, Any]) -> dict[str, Any]:
    reports = payload.get("reports") if isinstance(payload.get("reports"), list) else []
    table_rows = payload.get("table_rows") if isinstance(payload.get("table_rows"), list) else []
    source_urls = _phc4_identity_values(
        "source_url",
        state_health_data.PHC4_REPORT_LIBRARY_URL,
        *(row.get("url") or row.get("artifact_url") for row in reports if isinstance(row, dict)),
        *(row.get("source_artifact") or row.get("landing_page_url") for row in table_rows if isinstance(row, dict)),
    )
    return {
        "entity_scope": "phc4_public_report",
        "join_keys": [
            {
                "field": "canonical_name",
                "values": _phc4_identity_values(
                    "canonical_name",
                    query.get("hospital_name"),
                    query.get("query"),
                    payload.get("hospital_name"),
                    *(row.get("hospital_name") or row.get("hospital") for row in table_rows if isinstance(row, dict)),
                ),
                "status": "candidate_public_report_name",
                "used_by": ["phc4_public_reports"],
            },
            {
                "field": "report_type",
                "values": _phc4_identity_values(
                    "text",
                    query.get("report_type"),
                    payload.get("report_type"),
                    *(row.get("report_type") for row in reports if isinstance(row, dict)),
                ),
                "status": "source_filter",
                "used_by": ["phc4_public_reports"],
            },
            {
                "field": "year",
                "values": _phc4_identity_values(
                    "text",
                    query.get("year"),
                    query.get("fiscal_year"),
                    payload.get("year"),
                    *(row.get("year") or row.get("report_year") or row.get("publication_year") for row in reports if isinstance(row, dict)),
                    *(row.get("report_year") or row.get("publication_year") for row in table_rows if isinstance(row, dict)),
                ),
                "status": "source_filter",
                "used_by": ["phc4_public_reports"],
            },
            {
                "field": "procedure",
                "values": _phc4_identity_values(
                    "text",
                    query.get("procedure"),
                    payload.get("procedure"),
                    *(row.get("procedure") for row in table_rows if isinstance(row, dict)),
                ),
                "status": "candidate_public_report_row",
                "used_by": ["phc4_public_reports"],
            },
            {
                "field": "source_url",
                "values": source_urls,
                "status": "source_locator" if source_urls else "missing",
                "used_by": ["phc4_public_reports"],
            },
        ],
        "source_claims": [
            {
                "collection": "phc4_public_reports",
                "identity_paths": [
                    "query.query",
                    "query.hospital_name",
                    "query.year",
                    "query.fiscal_year",
                    "query.report_type",
                    "query.procedure",
                    "reports.title",
                    "reports.url",
                    "table_rows.hospital_name",
                    "table_rows.procedure",
                    "table_rows.source_artifact",
                ],
                "evidence_path": "evidence",
                "source_metadata_path": "source_metadata",
                "match_policy": "public_report_index_and_extracted_rows_are_source_scoped_candidate_context",
            }
        ],
        "conflict_policy": [
            "Treat PHC4 hospital names and procedure labels as source-specific aliases unless a stable facility identifier is independently resolved.",
            "Do not substitute PHC4 report rows for exact CMS quality, readmission, HAI, cost-report, or enrollment facts.",
            "Preserve report URL, report year, extraction status, and evidence caveat before citing PHC4-derived facts.",
        ],
        "missing_data_policy": (
            "No PHC4 report or table-row match means only that the indexed public PHC4 reports did not contain a matching public-report artifact; "
            "it is not proof that PHC4 paid datasets, CMS sources, or other state sources lack the fact."
        ),
    }


def _phc4_identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        if value in ("", None, 0):
            continue
        normalized = normalize_name(value, remove_legal_suffixes=True) if field == "canonical_name" else str(value).strip()
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _with_phc4_evidence(
    payload: dict[str, Any],
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
) -> dict[str, Any]:
    enriched = dict(payload)
    enriched["source_metadata"] = _phc4_source_metadata(enriched, query=query)
    enriched["evidence"] = _phc4_evidence(
        enriched,
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        next_step=next_step,
    )
    identity = _phc4_identity(enriched, query=query)
    if identity:
        enriched["identity"] = identity
    enriched["identity_map"] = _phc4_identity_map(enriched, query=query)
    return enriched


def _cyber_identity(
    *,
    entity_name: str = "",
    state: str = "",
    cik: str = "",
    source_name: str,
    source_url: str,
    match_basis: str,
    confidence: str,
) -> dict[str, Any]:
    unresolved = {}
    if state:
        unresolved["state"] = state.strip().upper()
    if cik:
        unresolved["cik"] = cik.strip()
    return _identity_with_match_decision(
        name=entity_name,
        entity_type="organization" if entity_name else "",
        source_name=source_name,
        source_url=source_url,
        match_basis=match_basis,
        confidence=confidence,
        notes=(
            "Public cyber/breach identity is query-seed and record-match context only; "
            "no-hit results must not be treated as entity clearance."
        ),
        unresolved_identifiers=unresolved,
    )


def _cyber_identity_map(
    *,
    query: dict[str, Any],
    payload: dict[str, Any] | None = None,
    dataset_id: str = "",
    entity_name: str = "",
    state: str = "",
    cik: str = "",
) -> dict[str, Any]:
    """Return cyber/breach source joins and no-assurance boundaries."""

    data = payload or {}
    evidence = data.get("evidence") if isinstance(data.get("evidence"), dict) else {}
    effective_dataset_id = dataset_id or str(evidence.get("dataset_id") or "")
    records = _cyber_payload_records(data)
    join_values = {
        "canonical_name": _cyber_identity_values(
            "canonical_name",
            entity_name,
            query.get("entity_name"),
            data.get("entity_name"),
            data.get("search_entity"),
            *(record.get("entity_name") for record in records),
        ),
        "state": _cyber_identity_values("state", state, query.get("state"), data.get("state"), *(record.get("state") for record in records)),
        "cik": _cyber_identity_values("cik", cik, query.get("cik"), data.get("cik"), *(record.get("cik") for record in records)),
        "accession_number": _cyber_identity_values(
            "accession_number",
            query.get("accession_number"),
            data.get("accession_number"),
            *(record.get("accession_number") or record.get("accession") for record in records),
        ),
        "source_url": _cyber_identity_values(
            "source_url",
            data.get("source_url"),
            *(record.get("source_url") for record in records),
        ),
        "source_status": _cyber_identity_values(
            "source_status",
            (data.get("source_status") or {}).get("status") if isinstance(data.get("source_status"), dict) else "",
            *((source or {}).get("status") if isinstance(source, dict) else source for source in (data.get("sources") or {}).values())
            if isinstance(data.get("sources"), dict)
            else (),
        ),
    }
    source_claims = _cyber_source_claims(dataset_id=effective_dataset_id, payload=data)
    return {
        "entity_scope": "public_cyber_breach_records",
        "join_keys": [
            {
                "field": field,
                "values": values,
                "status": "provided" if values else "missing",
                "used_by": _cyber_join_key_usage(field, source_claims),
            }
            for field, values in join_values.items()
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Use CIK and accession number only for SEC issuer disclosure records.",
            "Use OCR/state entity names and states as source-specific record context, not legal entity resolution.",
            "Keep OCR breach rows, OCR enforcement pages, SEC issuer filings, state notices, and CISA KEV context as separate source claims.",
            "Preserve source URL, source status, match confidence, and evidence caveat before citing cyber/breach facts.",
        ],
        "missing_data_policy": (
            "No-hit or not-evaluated cyber/breach responses describe only the searched public source scope; "
            "they are not proof of no breach, no incident, no enforcement action, no disclosure, no state notice, "
            "no cybersecurity issue, or any cybersecurity attestation."
        ),
    }


def _cyber_payload_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for key in ("records", "breaches", "incidents"):
        value = payload.get(key)
        if isinstance(value, list):
            records.extend(record for record in value if isinstance(record, dict))
    return records


def _cyber_identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        normalized = _normalize_cyber_identity_value(field, value)
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _normalize_cyber_identity_value(field: str, value: Any) -> str:
    if value in ("", None):
        return ""
    if field == "canonical_name":
        return normalize_name(value, remove_legal_suffixes=True)
    if field == "state":
        return str(value).strip().upper()
    if field == "cik":
        return "".join(character for character in str(value) if character.isdigit()).lstrip("0") or "0"
    if field == "source_status":
        return str(value).strip().lower()
    return str(value).strip()


def _cyber_source_claims(*, dataset_id: str, payload: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    data = payload or {}
    has_breaches = bool(data.get("breaches"))
    has_records = bool(data.get("records"))
    has_incidents = bool(data.get("incidents"))
    claims_by_dataset = {
        "hhs_ocr_breach_portal": [
            _public_source_claim(
                collection="hhs_ocr_breach_portal",
                match_policy="entity_name_state_filter_returns_public_large_breach_candidates",
                row_evidence_paths=["breaches[].evidence"] if has_breaches else [],
            )
        ],
        "hhs_ocr_enforcement_actions": [
            _public_source_claim(
                collection="hhs_ocr_enforcement_actions",
                match_policy="public_page_index_search_returns_candidate_enforcement_pages",
                row_evidence_paths=["records[].evidence"] if has_records else [],
            )
        ],
        "sec_cyber_disclosures": [
            _public_source_claim(
                collection="sec_cyber_disclosures",
                match_policy="cik_and_accession_anchor_sec_issuer_disclosure_facts",
                row_evidence_paths=["records[].evidence"] if has_records else [],
            )
        ],
        "state_ag_breach_notices": [
            _public_source_claim(
                collection="state_ag_breach_notices",
                match_policy="reviewed_state_import_search_returns_source_scoped_notice_candidates",
                row_evidence_paths=["records[].evidence"] if has_records else [],
            )
        ],
        "public_cyber_incident_profile": [
            _public_source_claim(
                collection="public_cyber_incident_profile",
                match_policy="aggregates_adjacent_public_record_sources_without_attestation_or_assurance_claims",
                row_evidence_paths=["incidents[].evidence"] if has_incidents else [],
            )
        ],
        "unsupported_cybersecurity_attestation": [
            _public_source_claim(
                collection="unsupported_cybersecurity_attestation",
                match_policy="source_status_only_no_reviewed_attestation_field_configured",
            )
        ],
        "cisa_kev_context": [
            _public_source_claim(
                collection="cisa_kev_context",
                match_policy="vulnerability_context_only_not_entity_attribution",
            )
        ],
    }
    return claims_by_dataset.get(
        dataset_id,
        [
            _public_source_claim(
                collection="public_cyber_source_query",
                match_policy="source_scoped_public_record_search_not_entity_assurance",
                row_evidence_paths=["records[].evidence"] if has_records else [],
            )
        ],
    )


def _cyber_join_key_usage(field: str, source_claims: list[dict[str, Any]]) -> list[str]:
    path_tokens = {
        "canonical_name": ("entity_name",),
        "state": ("state",),
        "cik": ("cik",),
        "accession_number": ("accession_number",),
        "source_url": ("source_url",),
        "source_status": ("status",),
    }[field]
    used_by = []
    for claim in source_claims:
        paths = " ".join(str(path) for path in claim.get("identity_paths", []))
        if any(token in paths for token in path_tokens):
            used_by.append(str(claim.get("collection") or ""))
    return sorted(item for item in used_by if item)


def _cyber_search_basis_and_confidence(
    *,
    records: list[Any],
    source_status: dict[str, Any],
    search_basis: str,
    no_match_basis: str,
    no_match_confidence: str,
) -> tuple[str, str]:
    status = str(source_status.get("status") or "").strip()
    if status and status != "ready":
        return f"{search_basis}_source_{status}", f"not_evaluated_source_{status}"
    if records:
        return search_basis, "record_level_confidence"
    return no_match_basis, no_match_confidence


# ---------------------------------------------------------------------------
# SAM.gov Exclusions response helpers
# ---------------------------------------------------------------------------

def _sam_source_metadata(data: dict[str, Any]) -> SAMExclusionsSourceMetadata:
    return SAMExclusionsSourceMetadata(
        **{k: v for k, v in data.items() if k in SAMExclusionsSourceMetadata.model_fields}
    )


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _first_sam_action(raw: dict[str, Any]) -> dict[str, Any]:
    actions = raw.get("exclusionActions", {}).get("listOfActions", [])
    if isinstance(actions, list) and actions:
        first = actions[0]
        if isinstance(first, dict):
            return first
    return {}


def _sam_references(raw: dict[str, Any]) -> list[dict[str, str]]:
    refs = (
        raw.get("exclusionOtherInformation", {})
        .get("references", {})
        .get("referencesList", [])
    )
    if not isinstance(refs, list):
        return []
    normalized: list[dict[str, str]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        exclusion_name = _text(ref.get("exclusionName") or ref.get("name"))
        ref_type = _text(ref.get("type"))
        if exclusion_name or ref_type:
            normalized.append({"exclusion_name": exclusion_name, "type": ref_type})
    return normalized


def _sam_display_name(identification: dict[str, Any]) -> str:
    entity_name = _text(identification.get("entityName") or identification.get("name")).strip()
    if entity_name:
        return entity_name
    name_parts = [
        _text(identification.get("prefix")).strip(),
        _text(identification.get("firstName")).strip(),
        _text(identification.get("middleName")).strip(),
        _text(identification.get("lastName")).strip(),
        _text(identification.get("suffix")).strip(),
    ]
    return " ".join(part for part in name_parts if part)


def _sam_match_basis(record: SAMExclusionRecord, query: dict[str, Any]) -> tuple[str, int, str]:
    if query.get("ueiSAM") and record.uei.upper() == _text(query.get("ueiSAM")).upper():
        return "uei_exact", 100, "strong_potential_match"
    if query.get("cageCode") and record.cage_code.upper() == _text(query.get("cageCode")).upper():
        return "cage_code_exact", 100, "strong_potential_match"
    query_npi = normalize_npi(query.get("npi"))
    record_npi = normalize_npi(record.npi)
    if query_npi and record_npi == query_npi:
        return "npi_exact", 100, "strong_potential_match"
    if query.get("exclusionName"):
        return "name_search", 70, "potential_match"
    return "filtered_search", 50, "potential_match"


def _sam_records(rows: list[dict[str, Any]], query: dict[str, Any] | None = None) -> list[SAMExclusionRecord]:
    records: list[SAMExclusionRecord] = []
    query = query or {}
    for row in rows:
        details = row.get("exclusionDetails", {})
        identification = row.get("exclusionIdentification", {})
        address = row.get("exclusionPrimaryAddress") or row.get("exclusionAddress") or {}
        action = _first_sam_action(row)
        other = row.get("exclusionOtherInformation", {})
        record = SAMExclusionRecord(
            classification=_text(details.get("classificationType")),
            exclusion_type=_text(details.get("exclusionType")),
            exclusion_program=_text(details.get("exclusionProgram")),
            excluding_agency_code=_text(details.get("excludingAgencyCode")),
            excluding_agency_name=_text(details.get("excludingAgencyName")),
            uei=_text(identification.get("ueiSAM")),
            cage_code=_text(identification.get("cageCode")),
            npi=_text(identification.get("npi")),
            prefix=_text(identification.get("prefix")),
            first_name=_text(identification.get("firstName")),
            middle_name=_text(identification.get("middleName")),
            last_name=_text(identification.get("lastName")),
            suffix=_text(identification.get("suffix")),
            entity_name=_text(identification.get("entityName") or identification.get("name")),
            display_name=_sam_display_name(identification),
            address_line_1=_text(address.get("addressLine1")),
            address_line_2=_text(address.get("addressLine2")),
            city=_text(address.get("city")),
            state=_text(address.get("stateOrProvinceCode")),
            zip_code=_text(address.get("zipCode")),
            zip_code_plus_4=_text(address.get("zipCodePlus4")),
            country=_text(address.get("countryCode")),
            create_date=_text(action.get("createDate")),
            update_date=_text(action.get("updateDate")),
            activation_date=_text(action.get("activateDate")),
            termination_date=_text(action.get("terminationDate")),
            termination_type=_text(action.get("terminationType")),
            record_status=_text(action.get("recordStatus")),
            ct_code=_text(other.get("ctCode")),
            fascsa_order=_text(other.get("isFASCSAOrder")),
            additional_comments=_text(other.get("additionalComments")),
            references=_sam_references(row),
        )
        match_basis, match_score, verification_status = _sam_match_basis(record, query)
        record.match_basis = match_basis
        record.match_score = match_score
        record.verification_status = verification_status
        records.append(record)
    return records


def _sam_status(records: list[SAMExclusionRecord]) -> str:
    if not records:
        return "no_current_sam_exclusion_found"
    if any(record.verification_status == "strong_potential_match" for record in records):
        return "strong_potential_match"
    return "potential_match"


def _sam_evidence(
    metadata: SAMExclusionsSourceMetadata,
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
) -> dict[str, Any]:
    return evidence_receipt(
        source_metadata=metadata.model_dump(),
        dataset_id="sam_gov_exclusions",
        cache_status="live_api",
        cache_freshness="live_api",
        entity_scope="active_federal_exclusion_screening",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat=SAM_EXCLUSIONS_CAVEAT,
        next_step="Open the full SAM.gov record and follow agency guidance before eligibility or contracting decisions.",
    )


def _sam_identity(
    *,
    records: list[SAMExclusionRecord],
    query: dict[str, Any],
    metadata: SAMExclusionsSourceMetadata,
    candidate: dict[str, Any] | None = None,
    match_basis: str = "",
    confidence: str = "",
) -> dict[str, Any]:
    first_record = records[0] if records else None
    candidate = candidate or {}
    query_name = str(
        query.get("entity_name")
        or candidate.get("entity_name")
        or " ".join(
            part
            for part in (
                query.get("first_name") or candidate.get("first_name"),
                query.get("last_name") or candidate.get("last_name"),
            )
            if part
        )
    ).strip()
    unresolved_identifiers = {
        "uei": first_record.uei if first_record else query.get("uei") or candidate.get("uei"),
        "cage_code": (
            first_record.cage_code
            if first_record
            else query.get("cage_code") or candidate.get("cage_code")
        ),
    }
    return _identity_with_match_decision(
        name=(first_record.display_name if first_record else query_name),
        entity_type=(first_record.classification if first_record else str(candidate.get("classification") or "")),
        npi=(first_record.npi if first_record else query.get("npi") or candidate.get("npi")),
        address=(first_record.address_line_1 if first_record else ""),
        zip_code=(first_record.zip_code if first_record else ""),
        source_name=metadata.source_name,
        source_url=metadata.source_url,
        match_basis=match_basis,
        confidence=confidence,
        notes="SAM.gov Exclusions identity is screening-only; verify against the full SAM.gov record before decisions.",
        unresolved_identifiers=unresolved_identifiers,
    )


def _sam_query_payload(**kwargs: str) -> dict[str, str]:
    return {key: value for key, value in kwargs.items() if value}


def _sam_error_response(raw: dict[str, Any], tool_name: str) -> dict[str, Any]:
    return error_response(
        raw.get("error", f"{tool_name} failed."),
        code=raw.get("code", "source_unavailable"),
        detail=raw.get("detail") or raw.get("instructions"),
        retryable=bool(raw.get("retryable", False)),
        source_metadata=raw.get("source_metadata"),
    )


# ---------------------------------------------------------------------------
# Tool 1: search_usaspending
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_usaspending(
    recipient_name: str, award_type: str = "", fiscal_year: str = "", limit: int = 25,
) -> dict[str, Any]:
    """Search federal spending awarded to a health system or hospital.

    Uses USAspending.gov (fully open, no auth needed).

    Args:
        recipient_name: Health system or hospital name to search.
        award_type: Filter: "contracts", "grants", "direct_payments", or "" for all.
        fiscal_year: e.g. "2024". Default: current fiscal year.
        limit: Max results (default 25, max 100).

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_usaspending","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        cache_params = {
            "recipient_name": recipient_name,
            "award_type": award_type,
            "fiscal_year": fiscal_year,
            "limit": limit,
        }
        cached = data_loaders.load_cached_api_response("usaspending", cache_params)
        cache_hit = cached is not None
        if cached is not None:
            raw = cached
        else:
            raw = await usaspending_client.search_awards(
                recipient_name, award_type, fiscal_year, limit,
            )
            if "error" in raw:
                source_metadata = _public_api_search_metadata(
                    source_name="USAspending.gov",
                    source_url="https://api.usaspending.gov/api/v2/search/spending_by_award/",
                    dataset_id="usaspending_awards",
                    query=cache_params,
                    cache_prefix="usaspending",
                    cache_hit=False,
                    record_count=0,
                    docs_url="https://api.usaspending.gov/",
                )
                return error_response(
                    str(raw.get("error") or "USAspending.gov search failed."),
                    code="source_unavailable",
                    detail=raw.get("instructions"),
                    source_metadata=source_metadata,
                    evidence=_public_record_evidence(
                        source_name="USAspending.gov",
                        source_url="https://api.usaspending.gov/api/v2/search/spending_by_award/",
                        dataset_id="usaspending_awards",
                        query=cache_params,
                        match_basis="recipient_name_search_not_evaluated",
                        confidence="source_unavailable",
                        caveat="USAspending award searches are public-record candidate matches by recipient text and filters.",
                        next_step="Retry the public API or review USAspending directly before citing no awards.",
                        source_metadata=source_metadata,
                    ),
                )
            data_loaders.cache_api_response("usaspending", cache_params, raw)

        results = raw.get("results", [])
        awards: list[USAspendingAward] = []
        for r in results:
            awards.append(USAspendingAward(
                award_id=str(r.get("Award ID", "")),
                recipient_name=str(r.get("Recipient Name", "")),
                awarding_agency=str(r.get("Awarding Agency", "")),
                awarding_sub_agency=str(r.get("Awarding Sub Agency", "")),
                award_type=str(r.get("Award Type", "")),
                total_obligation=float(r.get("Award Amount", 0) or 0),
                description=str(r.get("Description", "")),
                start_date=str(r.get("Start Date", "")),
                end_date=str(r.get("End Date", "")),
                naics_code=str(r.get("NAICS Code", "")),
                naics_description=str(r.get("NAICS Description", "")),
            ))

        total_obligation = sum(a.total_obligation for a in awards)
        total_awards = raw.get("page_metadata", {}).get("total", len(awards))

        from datetime import datetime as _dt
        fy = fiscal_year or str(_dt.now().year)
        response = USAspendingResponse(
            recipient_search=recipient_name,
            fiscal_year=fy,
            total_awards=total_awards,
            total_obligation=total_obligation,
            awards=awards,
        )
        payload = response.model_dump()
        source_metadata = _public_api_search_metadata(
            source_name="USAspending.gov",
            source_url="https://api.usaspending.gov/api/v2/search/spending_by_award/",
            dataset_id="usaspending_awards",
            query=cache_params,
            cache_prefix="usaspending",
            cache_hit=cache_hit,
            record_count=len(awards),
            docs_url="https://api.usaspending.gov/",
        )
        for award in payload["awards"]:
            award["evidence"] = _public_record_row_evidence(
                row=award,
                parent_query=cache_params,
                source_name="USAspending.gov",
                source_url="https://api.usaspending.gov/api/v2/search/spending_by_award/",
                dataset_id="usaspending_awards",
                match_basis="usaspending_award_row",
                confidence="candidate_award_row",
                caveat=(
                    "USAspending award rows are public federal spending records matched by recipient search text; "
                    "recipient names are candidate aliases unless source identifiers are reviewed."
                ),
                next_step="Review the award ID, recipient source name, agency, dates, and USAspending source record before citing this row.",
                source_metadata=source_metadata,
            )
        match_basis = "recipient_name_fiscal_year_search" if awards else "recipient_name_fiscal_year_search_no_match"
        confidence = "candidate_award_matches" if awards else "no_awards_returned_for_query_scope"
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _public_record_evidence(
            source_name="USAspending.gov",
            source_url="https://api.usaspending.gov/api/v2/search/spending_by_award/",
            dataset_id="usaspending_awards",
            query=cache_params,
            match_basis=match_basis,
            confidence=confidence,
            caveat=(
                "USAspending award rows are public federal spending records matched by recipient search text; "
                "recipient names are candidate aliases unless source identifiers are reviewed."
            ),
            next_step="Review award IDs, recipient source names, agencies, dates, and source URLs before report citation.",
            source_metadata=source_metadata,
        )
        payload["identity"] = _public_search_identity(
            name=awards[0].recipient_name if awards else recipient_name,
            source_name="USAspending.gov",
            source_url="https://api.usaspending.gov/",
            match_basis=match_basis,
            confidence=confidence,
        ).to_dict()
        payload["identity_map"] = _public_api_search_identity_map(
            query_name=recipient_name,
            observed_names=[award.recipient_name for award in awards],
            dataset_id="usaspending_awards",
            collection="awards",
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("search_usaspending failed")
        return error_response(f"search_usaspending failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: search_sam_gov
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_sam_gov(
    keyword: str, posted_from: str = "", posted_to: str = "", ptype: str = "", limit: int = 25,
) -> dict[str, Any]:
    """Search federal contract opportunities and solicitations.

    Uses SAM.gov Opportunities API. Requires SAM_GOV_API_KEY env var.

    Args:
        keyword: Search keyword (organization name, service type, NAICS code).
        posted_from: Start date (MM/DD/YYYY). Default: 1 year ago.
        posted_to: End date. Default: today.
        ptype: Procurement type: "o" (solicitation), "p" (presolicitation), "k" (combined), or "" for all.
        limit: Max results (default 25).

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_sam_gov","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        cache_params = {
            "keyword": keyword,
            "posted_from": posted_from,
            "posted_to": posted_to,
            "ptype": ptype,
            "limit": limit,
        }
        cached = data_loaders.load_cached_api_response("sam_gov", cache_params)
        cache_hit = cached is not None
        if cached is not None:
            raw = cached
        else:
            raw = await sam_client.search_opportunities(
                keyword, posted_from, posted_to, ptype, limit,
            )
            if "error" in raw:
                source_metadata = _public_api_search_metadata(
                    source_name="SAM.gov Contract Opportunities",
                    source_url="https://api.sam.gov/prod/opportunities/v2/search",
                    dataset_id="sam_gov_opportunities",
                    query=cache_params,
                    cache_prefix="sam_gov",
                    cache_hit=False,
                    record_count=0,
                    docs_url="https://open.gsa.gov/api/get-opportunities-public-api/",
                )
                return error_response(
                    str(raw.get("error") or "SAM.gov Opportunities search failed."),
                    code="missing_api_key" if "API_KEY" in str(raw.get("error") or "") else "source_unavailable",
                    detail=raw.get("instructions"),
                    source_metadata=source_metadata,
                    evidence=_public_record_evidence(
                        source_name="SAM.gov Contract Opportunities",
                        source_url="https://api.sam.gov/prod/opportunities/v2/search",
                        dataset_id="sam_gov_opportunities",
                        query=cache_params,
                        match_basis="keyword_search_not_evaluated",
                        confidence="missing_api_key" if "API_KEY" in str(raw.get("error") or "") else "source_unavailable",
                        caveat="SAM.gov opportunity searches are public procurement notices, not provider enrollment or exclusion records.",
                        next_step="Set SAM_GOV_API_KEY or review SAM.gov directly before citing no opportunities.",
                        source_metadata=source_metadata,
                    ),
                )
            data_loaders.cache_api_response("sam_gov", cache_params, raw)

        opp_data = raw.get("opportunitiesData", [])
        opportunities: list[SAMOpportunity] = []
        for r in opp_data:
            opportunities.append(SAMOpportunity(
                notice_id=str(r.get("noticeId", "")),
                title=str(r.get("title", "")),
                solicitation_number=str(r.get("solicitationNumber", "")),
                department=str(r.get("department", "")),
                sub_tier=str(r.get("subTier", "")),
                posted_date=str(r.get("postedDate", "")),
                response_deadline=str(r.get("responseDeadLine", "")),
                naics_code=str(r.get("naicsCode", "")),
                set_aside_type=str(r.get("typeOfSetAsideDescription", "")),
                description=str(r.get("description", "")),
                active=bool(r.get("active", True)),
            ))

        total_results = raw.get("totalRecords", len(opportunities))

        response = SAMResponse(
            keyword=keyword,
            total_results=total_results,
            opportunities=opportunities,
        )
        payload = response.model_dump()
        source_metadata = _public_api_search_metadata(
            source_name="SAM.gov Contract Opportunities",
            source_url="https://api.sam.gov/prod/opportunities/v2/search",
            dataset_id="sam_gov_opportunities",
            query=cache_params,
            cache_prefix="sam_gov",
            cache_hit=cache_hit,
            record_count=len(opportunities),
            docs_url="https://open.gsa.gov/api/get-opportunities-public-api/",
        )
        for opportunity in payload["opportunities"]:
            opportunity["evidence"] = _public_record_row_evidence(
                row=opportunity,
                parent_query=cache_params,
                source_name="SAM.gov Contract Opportunities",
                source_url="https://api.sam.gov/prod/opportunities/v2/search",
                dataset_id="sam_gov_opportunities",
                match_basis="sam_gov_opportunity_row",
                confidence="candidate_opportunity_row",
                caveat=(
                    "SAM.gov opportunity rows are public procurement notices matched by keyword and posted-date filters; "
                    "they are not SAM.gov Exclusions or entity registration determinations."
                ),
                next_step="Review the notice ID, solicitation number, agency, and SAM.gov source record before citing this row.",
                source_metadata=source_metadata,
            )
        match_basis = "keyword_posted_date_search" if opportunities else "keyword_posted_date_search_no_match"
        confidence = "candidate_opportunity_matches" if opportunities else "no_opportunities_returned_for_query_scope"
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _public_record_evidence(
            source_name="SAM.gov Contract Opportunities",
            source_url="https://api.sam.gov/prod/opportunities/v2/search",
            dataset_id="sam_gov_opportunities",
            query=cache_params,
            match_basis=match_basis,
            confidence=confidence,
            caveat=(
                "SAM.gov opportunity rows are public procurement notices matched by keyword and posted-date filters; "
                "they are not SAM.gov Exclusions or entity registration determinations."
            ),
            next_step="Review notice IDs, solicitation numbers, agencies, and SAM.gov source records before report citation.",
            source_metadata=source_metadata,
        )
        payload["identity"] = _public_search_identity(
            name=keyword,
            source_name="SAM.gov Contract Opportunities",
            source_url="https://open.gsa.gov/api/get-opportunities-public-api/",
            match_basis=match_basis,
            confidence=confidence,
        ).to_dict()
        payload["identity_map"] = _public_api_search_identity_map(
            query_name=keyword,
            observed_names=[],
            dataset_id="sam_gov_opportunities",
            collection="opportunities",
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("search_sam_gov failed")
        return error_response(f"search_sam_gov failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: get_breach_history
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_breach_history(
    entity_name: str, state: str = "", min_individuals: int = 0,
) -> dict[str, Any]:
    """Look up HIPAA breach reports for an organization.

    Requires manual download of breach data CSV from HHS OCR portal.

    Args:
        entity_name: Organization name to search.
        state: Filter by state abbreviation.
        min_individuals: Minimum individuals affected (default 0).

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_breach_history","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        query_payload = {"entity_name": entity_name, "state": state, "min_individuals": min_individuals}
        if not data_loaders.ensure_breach_loaded():
            source_metadata = _breach_source_metadata()
            match_basis = "hhs_ocr_breach_cache_missing"
            confidence = "not_evaluated_source_missing"
            evidence = _public_record_evidence(
                source_name="HHS OCR Breach Portal",
                source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
                dataset_id="hhs_ocr_breach_portal",
                query=query_payload,
                match_basis=match_basis,
                confidence=confidence,
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step=(
                    "Seed or refresh the OCR breach CSV from the live portal before "
                    "making a source-backed breach-history statement."
                ),
                source_metadata=source_metadata,
            )
            return error_response(
                "HIPAA breach data not available",
                code="source_unavailable",
                instructions=(
                    "Download the breach report CSV from "
                    "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf "
                    f"and place it at: {data_loaders._BREACH_CSV}"
                ),
                evidence=evidence,
                source_metadata=_public_record_source_metadata(evidence),
                identity=_cyber_identity(
                    entity_name=entity_name,
                    state=state,
                    source_name="HHS OCR Breach Portal",
                    source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
                    match_basis=match_basis,
                    confidence=confidence,
                ),
                identity_map=_cyber_identity_map(
                    query=query_payload,
                    dataset_id="hhs_ocr_breach_portal",
                    entity_name=entity_name,
                    state=state,
                ),
            )

        rows = data_loaders.query_breaches(
            entity_name=entity_name, state=state, min_individuals=min_individuals,
        )

        breaches: list[BreachRecord] = []
        for r in rows:
            individuals = r.get("individuals_affected", 0)
            try:
                individuals = int(float(individuals)) if individuals else 0
            except (ValueError, TypeError):
                individuals = 0

            breaches.append(BreachRecord(
                entity_name=r.get("entity_name", ""),
                state=r.get("state", ""),
                covered_entity_type=r.get("covered_entity_type", ""),
                individuals_affected=individuals,
                breach_submission_date=r.get("breach_submission_date", ""),
                breach_type=r.get("breach_type", ""),
                location_of_breached_info=r.get("location_of_breached_info", ""),
                business_associate_present=r.get("business_associate_present", ""),
                web_description=r.get("web_description", ""),
                entity_match_confidence=_breach_entity_match_confidence(entity_name, r),
                incident_type_confidence=_breach_incident_type_confidence(r),
                timeline_disclosed=bool(r.get("breach_submission_date")),
                timeline_inferred=False,
                source_type="hhs_ocr_breach_portal",
            ))

        total_individuals = sum(b.individuals_affected for b in breaches)

        response = BreachHistoryResponse(
            search_entity=entity_name,
            total_breaches=len(breaches),
            total_individuals_affected=total_individuals,
            breaches=breaches,
        )
        payload = response.model_dump()
        match_basis = "entity_name_state_filter" if breaches else "entity_name_state_filter_no_match"
        confidence = "medium_name_match" if breaches else "no_imported_hhs_ocr_large_breach_match"
        breach_source_metadata = _breach_source_metadata()
        for breach in payload["breaches"]:
            breach["evidence"] = _public_record_row_evidence(
                row=breach,
                parent_query=query_payload,
                source_name="HHS OCR Breach Portal",
                source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
                dataset_id="hhs_ocr_breach_portal",
                match_basis="hhs_ocr_breach_row",
                confidence=str(breach.get("entity_match_confidence") or "medium_name_match"),
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step="Verify this breach row against the live OCR portal before citing it as a report fact.",
                source_metadata=breach_source_metadata,
            )
        payload["evidence"] = _public_record_evidence(
            source_name="HHS OCR Breach Portal",
            source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
            dataset_id="hhs_ocr_breach_portal",
            query=query_payload,
            match_basis=match_basis,
            confidence=confidence,
            caveat=_CYBER_NO_ASSURANCE_CAVEAT,
            next_step="Verify against the live OCR portal and any relevant state notice sources for final reporting.",
            source_metadata=breach_source_metadata,
        )
        payload["identity"] = _cyber_identity(
            entity_name=entity_name,
            state=state,
            source_name="HHS OCR Breach Portal",
            source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
            match_basis=match_basis,
            confidence=confidence,
        )
        payload["identity_map"] = _cyber_identity_map(
            query=query_payload,
            payload=payload,
            dataset_id="hhs_ocr_breach_portal",
            entity_name=entity_name,
            state=state,
        )
        return to_structured(_attach_public_record_source_metadata(payload))
    except Exception as e:
        logger.exception("get_breach_history failed")
        return error_response(f"get_breach_history failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_phc4_public_reports(query: str, year: str = "", report_type: str = "") -> dict[str, Any]:
    """Search the indexed PHC4 public report library without using paid PHC4 datasets.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_phc4_public_reports","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        payload = await state_health_data.search_phc4_reports(query, year=year, report_type=report_type)
        match_basis = "phc4_public_report_index_search" if payload.get("total_results") else "phc4_public_report_index_search_no_match"
        confidence = "public_report_index_match" if payload.get("total_results") else "no_indexed_phc4_public_report_match"
        return to_structured(
            _with_phc4_evidence(
                payload,
                query={"query": query, "year": year, "report_type": report_type},
                match_basis=match_basis,
                confidence=confidence,
                next_step="Open the returned PHC4 report URLs and verify report year/context before citing public-report facts.",
            )
        )
    except Exception as e:
        logger.exception("search_phc4_public_reports failed")
        return error_response(f"search_phc4_public_reports failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_phc4_hospital_performance(hospital_name: str = "", year: int = 0) -> dict[str, Any]:
    """Return PHC4 public Hospital Performance report matches and provenance.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_phc4_hospital_performance","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        payload = await state_health_data.phc4_report_profile(hospital_name=hospital_name, year=year, report_type="hospital_performance")
        return to_structured(
            _with_phc4_evidence(
                payload,
                query={"hospital_name": hospital_name, "year": year, "report_type": "hospital_performance"},
                match_basis="phc4_public_hospital_performance_profile" if payload.get("reports") else "phc4_public_hospital_performance_profile_no_match",
                confidence=str(payload.get("confidence") or "public_report_index_context"),
                next_step="Verify any extracted PHC4 table row against the linked public report before citing hospital performance context.",
            )
        )
    except Exception as e:
        logger.exception("get_phc4_hospital_performance failed")
        return error_response(f"get_phc4_hospital_performance failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_phc4_financial_analysis(hospital_name: str = "", fiscal_year: int = 0) -> dict[str, Any]:
    """Return PHC4 public Financial Analysis report matches and provenance.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_phc4_financial_analysis","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        payload = await state_health_data.phc4_report_profile(hospital_name=hospital_name, fiscal_year=fiscal_year, report_type="financial_analysis")
        return to_structured(
            _with_phc4_evidence(
                payload,
                query={"hospital_name": hospital_name, "fiscal_year": fiscal_year, "report_type": "financial_analysis"},
                match_basis="phc4_public_financial_analysis_profile" if payload.get("reports") else "phc4_public_financial_analysis_profile_no_match",
                confidence=str(payload.get("confidence") or "public_report_index_context"),
                next_step="Verify any extracted PHC4 financial row against the linked public report and do not substitute it for CMS cost-report facts.",
            )
        )
    except Exception as e:
        logger.exception("get_phc4_financial_analysis failed")
        return error_response(f"get_phc4_financial_analysis failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_phc4_common_procedure_profile(
    hospital_name: str = "", procedure: str = "", year: int = 0,
) -> dict[str, Any]:
    """Return PHC4 public Common Procedures report matches and provenance.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_phc4_common_procedure_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        payload = await state_health_data.phc4_report_profile(
            hospital_name=hospital_name,
            procedure=procedure,
            year=year,
            report_type="common_procedure",
        )
        return to_structured(
            _with_phc4_evidence(
                payload,
                query={"hospital_name": hospital_name, "procedure": procedure, "year": year, "report_type": "common_procedure"},
                match_basis="phc4_public_common_procedure_profile" if payload.get("reports") else "phc4_public_common_procedure_profile_no_match",
                confidence=str(payload.get("confidence") or "public_report_index_context"),
                next_step="Verify procedure labels and volumes against the linked PHC4 public report before using them as public context.",
            )
        )
    except Exception as e:
        logger.exception("get_phc4_common_procedure_profile failed")
        return error_response(f"get_phc4_common_procedure_profile failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_ocr_enforcement_actions(
    query: str = "",
    entity_name: str = "",
    state: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search locally indexed HHS OCR enforcement action public pages.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_ocr_enforcement_actions","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        result = data_loaders.search_ocr_enforcement_actions(
            query=query,
            entity_name=entity_name,
            state=state,
            limit=limit,
        )
        payload = {
                "query": query,
                "entity_name": entity_name,
                "state": state.upper() if state else "",
                "total_results": len(result.get("records", [])),
                "source_status": result.get("source_status", {}),
                "records": result.get("records", []),
        }
        records = result.get("records", [])
        source_status = result.get("source_status", {})
        match_basis, confidence = _cyber_search_basis_and_confidence(
            records=records,
            source_status=source_status,
            search_basis="imported_public_record_search",
            no_match_basis="imported_public_record_search_no_match",
            no_match_confidence="no_indexed_ocr_enforcement_action_match",
        )
        for record in payload["records"]:
            record["evidence"] = _public_record_row_evidence(
                row=record,
                parent_query={"query": query, "entity_name": entity_name, "state": state, "limit": limit},
                source_name="HHS OCR enforcement actions",
                source_url="https://www.hhs.gov/hipaa/for-professionals/compliance-enforcement/agreements/index.html",
                dataset_id="hhs_ocr_enforcement_actions",
                match_basis="ocr_enforcement_action_row",
                confidence=str(record.get("entity_match_confidence") or record.get("confidence") or "public_record_match"),
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step="Open the enforcement action source URL and verify entity/date/context before citing this row.",
                source_metadata=source_status,
            )
        payload["evidence"] = _public_record_evidence(
            source_name="HHS OCR enforcement actions",
            source_url="https://www.hhs.gov/hipaa/for-professionals/compliance-enforcement/agreements/index.html",
            dataset_id="hhs_ocr_enforcement_actions",
            query={"query": query, "entity_name": entity_name, "state": state, "limit": limit},
            match_basis=match_basis,
            confidence=confidence,
            caveat=_CYBER_NO_ASSURANCE_CAVEAT,
            next_step=(
                "Review source URLs on matched records before citing enforcement facts; "
                "if there are no hits, verify source coverage before saying only that this index had no match."
            ),
            source_metadata=source_status,
        )
        payload["identity"] = _cyber_identity(
            entity_name=entity_name,
            state=state,
            source_name="HHS OCR enforcement actions",
            source_url="https://www.hhs.gov/hipaa/for-professionals/compliance-enforcement/agreements/index.html",
            match_basis=match_basis,
            confidence=confidence,
        )
        payload["identity_map"] = _cyber_identity_map(
            query={"query": query, "entity_name": entity_name, "state": state, "limit": limit},
            payload=payload,
            dataset_id="hhs_ocr_enforcement_actions",
            entity_name=entity_name,
            state=state,
        )
        return to_structured(_attach_public_record_source_metadata(payload))
    except Exception as e:
        logger.exception("search_ocr_enforcement_actions failed")
        return error_response(f"search_ocr_enforcement_actions failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_sec_cyber_disclosures(
    entity_name: str = "",
    cik: str = "",
    query: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search locally indexed SEC cyber disclosures.

    SEC_USER_AGENT is required for this search path so callers do not build SEC
    workflows that ignore EDGAR's contactable user-agent policy.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_sec_cyber_disclosures","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        sec_user_agent = _os.environ.get("SEC_USER_AGENT", "").strip()
        if not sec_user_agent:
            source_status = CyberSourceStatus(
                source_name="SEC EDGAR cyber disclosures",
                source_type="sec_cyber_disclosure",
                status="not_searchable",
                reason="SEC_USER_AGENT is not set; SEC search paths require a contactable user agent.",
                source_url="https://www.sec.gov/edgar/search/",
                next_step="Set SEC_USER_AGENT before searching SEC cyber disclosures.",
            ).model_dump()
            match_basis = "sec_user_agent_missing"
            confidence = "not_evaluated_source_not_searchable"
            evidence = _public_record_evidence(
                source_name="SEC EDGAR cyber disclosures",
                source_url="https://www.sec.gov/edgar/search/",
                dataset_id="sec_cyber_disclosures",
                query={
                    "entity_name": entity_name,
                    "cik": cik,
                    "query": query,
                    "start_date": start_date,
                    "end_date": end_date,
                },
                match_basis=match_basis,
                confidence=confidence,
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step="Set SEC_USER_AGENT before evaluating indexed SEC cyber disclosures.",
                source_metadata=source_status,
            )
            return error_response(
                "SEC_USER_AGENT is required for SEC cyber disclosure search.",
                code="invalid_config",
                instructions="Set SEC_USER_AGENT to a contactable application name and email before using SEC paths.",
                evidence=evidence,
                source_metadata=_public_record_source_metadata(evidence),
                identity=_cyber_identity(
                    entity_name=entity_name,
                    cik=cik,
                    source_name="SEC EDGAR cyber disclosures",
                    source_url="https://www.sec.gov/edgar/search/",
                    match_basis=match_basis,
                    confidence=confidence,
                ),
                identity_map=_cyber_identity_map(
                    query={
                        "entity_name": entity_name,
                        "cik": cik,
                        "query": query,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                    dataset_id="sec_cyber_disclosures",
                    entity_name=entity_name,
                    cik=cik,
                ),
            )

        result = data_loaders.search_sec_cyber_disclosures(
            entity_name=entity_name,
            cik=cik,
            query=query,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        records = result.get("records", [])
        source_status = result.get("source_status", {})
        payload = {
                "entity_name": entity_name,
                "cik": cik,
                "query": query,
                "start_date": start_date,
                "end_date": end_date,
                "total_results": len(records),
                "source_status": source_status,
                "records": records,
        }
        match_basis, confidence = _cyber_search_basis_and_confidence(
            records=records,
            source_status=source_status,
            search_basis="imported_sec_disclosure_search",
            no_match_basis="imported_sec_disclosure_search_no_match",
            no_match_confidence="no_indexed_sec_cyber_disclosure_match",
        )
        for record in payload["records"]:
            record["evidence"] = _public_record_row_evidence(
                row=record,
                parent_query={"entity_name": entity_name, "cik": cik, "query": query, "start_date": start_date, "end_date": end_date},
                source_name="SEC EDGAR cyber disclosures",
                source_url="https://www.sec.gov/edgar/search/",
                dataset_id="sec_cyber_disclosures",
                match_basis="sec_cyber_disclosure_row",
                confidence=str(record.get("confidence") or record.get("entity_match_confidence") or "public_record_match"),
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step="Open the EDGAR source URL and cite accession/document context before using this row.",
                source_metadata=source_status,
            )
        payload["evidence"] = _public_record_evidence(
            source_name="SEC EDGAR cyber disclosures",
            source_url="https://www.sec.gov/edgar/search/",
            dataset_id="sec_cyber_disclosures",
            query={"entity_name": entity_name, "cik": cik, "query": query, "start_date": start_date, "end_date": end_date},
            match_basis=match_basis,
            confidence=confidence,
            caveat=_CYBER_NO_ASSURANCE_CAVEAT,
            next_step=(
                "Open matched EDGAR filing URLs and cite accession/document sections; "
                "if there are no hits, do not infer incident absence."
            ),
            source_metadata=source_status,
        )
        payload["identity"] = _cyber_identity(
            entity_name=entity_name,
            cik=cik,
            source_name="SEC EDGAR cyber disclosures",
            source_url="https://www.sec.gov/edgar/search/",
            match_basis=match_basis,
            confidence=confidence,
        )
        payload["identity_map"] = _cyber_identity_map(
            query={"entity_name": entity_name, "cik": cik, "query": query, "start_date": start_date, "end_date": end_date},
            payload=payload,
            dataset_id="sec_cyber_disclosures",
            entity_name=entity_name,
            cik=cik,
        )
        return to_structured(_attach_public_record_source_metadata(payload))
    except Exception as e:
        logger.exception("search_sec_cyber_disclosures failed")
        return error_response(f"search_sec_cyber_disclosures failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_state_ag_breach_notice_sources(states: list[str] | None = None) -> dict[str, Any]:
    """Return PA/NJ/DE state AG breach notice source statuses and reasons.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_state_ag_breach_notice_sources","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    sources = _cyber_source_statuses(states)
    payload = {
        "sources": sources,
        "evidence": _public_record_evidence(
            source_name="State AG breach notice source status",
            source_url=_state_breach_notice_evidence_url(states=states),
            dataset_id="state_ag_breach_notices",
            query={"states": states or []},
            match_basis="state_breach_notice_source_status",
            confidence="source_status_only",
            caveat=_CYBER_NO_ASSURANCE_CAVEAT,
            next_step="Use searchable states only after reviewed imports are ready and source URLs are preserved.",
            source_metadata={"status": "source_status", "record_count": len(sources)},
        ),
    }
    payload["identity_map"] = _cyber_identity_map(
        query={"states": states or []},
        payload=payload,
        dataset_id="state_ag_breach_notices",
    )
    return to_structured(_attach_public_record_source_metadata(payload))


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_cyber_attestation_source_status() -> dict[str, Any]:
    """Return public source status for broad cybersecurity attestation claims.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_cyber_attestation_source_status","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    payload = {
        "status": "not_publicly_available",
        "source_name": "CMS cybersecurity attestation public dataset",
        "source_type": "cybersecurity_attestation",
        "source_url": "https://www.cms.gov/data-research/cms-data/data-available-everyone",
        "can_assert_attestation_status": False,
        "reason": (
            "No reviewed public CMS dataset/source field is configured that supports a broad "
            "organization-level cybersecurity attestation status."
        ),
        "supported_adjacent_sources": [
            {
                "source_type": "cms_promoting_interoperability",
                "supported_claim": "interoperability/EHR fields present in source rows",
                "unsupported_claim": "general cybersecurity attestation",
            },
            {"source_type": "hhs_ocr_breach_portal", "supported_claim": "large HIPAA breach reports"},
            {"source_type": "ocr_enforcement_action", "supported_claim": "OCR enforcement action public pages"},
            {"source_type": "sec_cyber_disclosure", "supported_claim": "SEC issuer cyber disclosures"},
            {"source_type": "state_ag_breach_notice", "supported_claim": "reviewed state notice rows only"},
            {"source_type": "cisa_kev_context", "supported_claim": "vulnerability context only"},
        ],
        "next_step": "Configure a reviewed public source with an explicit cybersecurity attestation field before asserting status.",
        "evidence": _public_record_evidence(
            source_name="CMS cybersecurity attestation public dataset",
            source_url="https://www.cms.gov/data-research/cms-data/data-available-everyone",
            dataset_id="unsupported_cybersecurity_attestation",
            query={},
            match_basis="no_reviewed_public_source_field",
            confidence="unsupported_assertion",
            caveat="Adjacent PI, breach, SEC, state notice, or CISA sources must not be promoted into a broad cybersecurity attestation.",
            next_step="Configure a reviewed public source with an explicit cybersecurity attestation field before asserting status.",
        ),
    }
    payload["identity_map"] = _cyber_identity_map(
        query={},
        payload=payload,
        dataset_id="unsupported_cybersecurity_attestation",
    )
    return to_structured(_attach_public_record_source_metadata(payload))


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_cisa_kev_context_status() -> dict[str, Any]:
    """Return CISA KEV context-only status; this source is not attribution evidence.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_cisa_kev_context_status","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    payload = CISAKevContext().model_dump()
    payload["evidence"] = _public_record_evidence(
        source_name="CISA Known Exploited Vulnerabilities Catalog",
        source_url="https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
        dataset_id="cisa_kev_context",
        query={},
        match_basis="source_status_context_only",
        confidence="not_entity_attribution",
        caveat="CISA KEV is vulnerability context only and must not be used to attribute incidents to an entity.",
        next_step="Use KEV only as vulnerability context alongside source-backed incident records.",
    )
    payload["identity_map"] = _cyber_identity_map(query={}, payload=payload, dataset_id="cisa_kev_context")
    return to_structured(_attach_public_record_source_metadata(payload))


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_state_breach_notices(
    entity_name: str = "",
    state: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 25,
) -> dict[str, Any]:
    """Search imported reviewed state AG breach notice rows.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_state_breach_notices","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        result = data_loaders.search_state_breach_notices(
            entity_name=entity_name,
            state=state,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        records = result.get("records", [])
        source_status = result.get("source_status", {})
        payload = {
                "entity_name": entity_name,
                "state": state.upper() if state else "",
                "total_results": len(records),
                "source_status": source_status,
                "records": records,
                "source_caveat": "State breach notice search covers only reviewed imported rows; it is not a national breach cache.",
        }
        match_basis, confidence = _cyber_search_basis_and_confidence(
            records=records,
            source_status=source_status,
            search_basis="reviewed_imported_state_notice_search",
            no_match_basis="reviewed_imported_state_notice_search_no_match",
            no_match_confidence="no_reviewed_state_breach_notice_match",
        )
        for record in payload["records"]:
            record["evidence"] = _public_record_row_evidence(
                row=record,
                parent_query={"entity_name": entity_name, "state": state, "start_date": start_date, "end_date": end_date},
                source_name="State AG breach notices",
                source_url=_state_breach_notice_evidence_url(state=state, source_metadata=source_status),
                dataset_id="state_ag_breach_notices",
                match_basis="state_breach_notice_row",
                confidence=str(record.get("entity_match_confidence") or record.get("confidence") or "reviewed_import_match"),
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step="Open the state notice source URL and verify entity/date/context before citing this row.",
                source_metadata=source_status,
            )
        payload["evidence"] = _public_record_evidence(
            source_name="State AG breach notices",
            source_url=_state_breach_notice_evidence_url(state=state, source_metadata=source_status),
            dataset_id="state_ag_breach_notices",
            query={"entity_name": entity_name, "state": state, "start_date": start_date, "end_date": end_date},
            match_basis=match_basis,
            confidence=confidence,
            caveat=_CYBER_NO_ASSURANCE_CAVEAT,
            next_step=(
                "Inspect matched source URLs and state source status before citing a state notice fact; "
                "a zero-result response only describes this reviewed import."
            ),
            source_metadata=source_status,
        )
        payload["identity"] = _cyber_identity(
            entity_name=entity_name,
            state=state,
            source_name="State AG breach notices",
            source_url=_state_breach_notice_evidence_url(state=state, source_metadata=source_status),
            match_basis=match_basis,
            confidence=confidence,
        )
        payload["identity_map"] = _cyber_identity_map(
            query={"entity_name": entity_name, "state": state, "start_date": start_date, "end_date": end_date},
            payload=payload,
            dataset_id="state_ag_breach_notices",
            entity_name=entity_name,
            state=state,
        )
        return to_structured(_attach_public_record_source_metadata(payload))
    except Exception as e:
        logger.exception("search_state_breach_notices failed")
        return error_response(f"search_state_breach_notices failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_cyber_incident_profile(entity_name: str, state: str = "") -> dict[str, Any]:
    """Enrich public cyber incident history from OCR breach data and public-source flags.

    The tool returns confidence flags and does not infer response timelines
    unless a source row explicitly contains timing fields.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_cyber_incident_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        breach_result = await get_breach_history(entity_name=entity_name, state=state)
        if breach_result.get("ok") is False:
            breaches: list[dict[str, Any]] = []
            ocr_status = breach_result.get("error", {}).get("code", "source_unavailable")
        else:
            breaches = breach_result.get("breaches", [])
            ocr_status = "ready"

        incidents = [_cyber_incident_from_breach(entity_name, breach) for breach in breaches]

        ocr_enforcement = data_loaders.search_ocr_enforcement_actions(entity_name=entity_name, state=state, limit=10)
        incidents.extend(ocr_enforcement.get("records", []))

        sec_status: dict[str, Any]
        if _os.environ.get("SEC_USER_AGENT", "").strip():
            sec_result = data_loaders.search_sec_cyber_disclosures(entity_name=entity_name, limit=10)
            sec_status = sec_result.get("source_status", {})
            incidents.extend(sec_result.get("records", []))
        else:
            sec_status = CyberSourceStatus(
                source_name="SEC EDGAR cyber disclosures",
                source_type="sec_cyber_disclosure",
                status="not_searchable",
                reason="SEC_USER_AGENT is not set; SEC search paths require a contactable user agent.",
                source_url="https://www.sec.gov/edgar/search/",
                next_step="Set SEC_USER_AGENT before searching SEC cyber disclosures.",
            ).model_dump()

        cisa_context = CISAKevContext().model_dump()
        profile_match_basis = (
            "multi_source_public_record_profile"
            if incidents
            else "multi_source_public_record_profile_no_match"
        )
        profile_confidence = (
            "record_level_confidence"
            if incidents
            else "no_configured_public_cyber_incident_source_match"
        )
        incident_metadata_by_source = {
            "hhs_ocr_breach_portal": _breach_source_metadata(),
            "ocr_enforcement_action": ocr_enforcement.get("source_status", {}),
            "sec_cyber_disclosure": sec_status,
            "state_ag_breach_notice": {"status": "not_queried_in_profile", "record_count": 0},
        }
        incident_source_names = {
            "hhs_ocr_breach_portal": "HHS OCR Breach Portal",
            "ocr_enforcement_action": "HHS OCR enforcement actions",
            "sec_cyber_disclosure": "SEC EDGAR cyber disclosures",
            "state_ag_breach_notice": "State AG breach notices",
        }
        incident_source_urls = {
            "hhs_ocr_breach_portal": "https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
            "ocr_enforcement_action": "https://www.hhs.gov/hipaa/for-professionals/compliance-enforcement/agreements/index.html",
            "sec_cyber_disclosure": "https://www.sec.gov/edgar/search/",
            "state_ag_breach_notice": "",
        }
        for incident in incidents:
            if not isinstance(incident, dict) or "evidence" in incident:
                continue
            source_type = str(incident.get("source_type") or "")
            dataset_id = {
                "ocr_enforcement_action": "hhs_ocr_enforcement_actions",
                "sec_cyber_disclosure": "sec_cyber_disclosures",
                "state_ag_breach_notice": "state_ag_breach_notices",
            }.get(source_type, source_type or "public_cyber_incident_profile")
            incident["evidence"] = _public_record_row_evidence(
                row=incident,
                parent_query={"entity_name": entity_name, "state": state},
                source_name=incident_source_names.get(source_type, "Public cyber incident profile sources"),
                source_url=incident_source_urls.get(source_type, ""),
                dataset_id=dataset_id,
                match_basis="public_cyber_incident_row",
                confidence=str(incident.get("entity_match_confidence") or incident.get("confidence") or "record_level_confidence"),
                caveat=_CYBER_NO_ASSURANCE_CAVEAT,
                next_step="Open the incident source URL and verify source-specific entity/date/context before citing this row.",
                source_metadata=incident_metadata_by_source.get(source_type, {"status": "aggregated"}),
            )
        payload = {
                "entity_name": entity_name,
                "state": state.upper() if state else "",
                "sources": {
                    "hhs_ocr_breach_portal": ocr_status,
                    "ocr_enforcement_actions": ocr_enforcement.get("source_status", {}),
                    "sec_cyber_disclosures": sec_status,
                    "state_ag_breach_notices": _cyber_source_statuses([state] if state else None),
                    "cisa_kev": cisa_context,
                },
                "incident_count": len(incidents),
                "incidents": incidents,
                "confidence_flags": {
                    "hhs_ocr_name_match": "medium" if breaches else "none",
                    "timeline_inferred": False,
                    "cisa_kev_used_for_attribution": cisa_context["attribution_used"],
                },
        }
        payload["evidence"] = _public_record_evidence(
            source_name="Public cyber incident profile sources",
            source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
            dataset_id="public_cyber_incident_profile",
            query={"entity_name": entity_name, "state": state},
            match_basis=profile_match_basis,
            confidence=profile_confidence,
            caveat=(
                "Cyber incident profile aggregates adjacent public sources and does not assert "
                "cybersecurity attestation status. Zero incidents here are not proof of no incident."
            ),
            next_step=(
                "Review each incident source URL and source status before report inclusion; "
                "when no incidents are returned, report only the configured-source no-match scope."
            ),
            source_metadata={"status": "aggregated", "record_count": len(incidents)},
        )
        payload["identity"] = _cyber_identity(
            entity_name=entity_name,
            state=state,
            source_name="Public cyber incident profile sources",
            source_url="https://ocrportal.hhs.gov/ocr/breach/breach_report.jsf",
            match_basis=profile_match_basis,
            confidence=profile_confidence,
        )
        payload["identity_map"] = _cyber_identity_map(
            query={"entity_name": entity_name, "state": state},
            payload=payload,
            dataset_id="public_cyber_incident_profile",
            entity_name=entity_name,
            state=state,
        )
        return to_structured(_attach_public_record_source_metadata(payload))
    except Exception as e:
        logger.exception("get_cyber_incident_profile failed")
        return error_response(f"get_cyber_incident_profile failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: get_accreditation
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_accreditation(
    ccn: str = "", provider_name: str = "", state: str = "",
) -> dict[str, Any]:
    """Look up hospital accreditation and certification status.

    Uses CMS Provider of Services (POS) file. Includes Joint Commission,
    DNV, CIHQ, and other CMS-approved accrediting organizations.

    Args:
        ccn: CMS Certification Number (6-digit, e.g. "390223").
        provider_name: Search by provider name (partial match).
        state: Filter by state abbreviation.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_accreditation","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not ccn and not provider_name:
            return error_response("At least one of ccn or provider_name is required.")
        normalized_ccn = normalize_ccn(ccn) if ccn else ""
        if ccn and not normalized_ccn:
            return error_response("ccn must normalize to a six-character CMS Certification Number.", code="invalid_params")

        pos_loaded = await data_loaders.ensure_pos_cached()
        source_metadata = _public_cache_file_metadata(
            source_name="CMS Provider of Services",
            source_url=data_loaders.POS_URL,
            dataset_id="cms_provider_of_services",
            cache_path=data_loaders._POS_PARQUET,
            loaded=pos_loaded,
            source_period="current CMS Provider of Services cache at query time",
            next_step="Refresh the CMS Provider of Services cache before citing no accreditation or certification rows.",
        )
        query_payload = {"ccn": normalized_ccn, "provider_name": provider_name, "state": state}

        rows = data_loaders.query_pos(
            ccn=normalized_ccn, provider_name=provider_name, state=state,
        )

        providers: list[AccreditationRecord] = []
        for r in rows:
            # Map accreditation_type_code to org name
            code = r.get("accreditation_type_code", "")
            accred_org = _ACCR_CODES.get(code.strip()) or code

            providers.append(AccreditationRecord(
                ccn=r.get("ccn", ""),
                provider_name=r.get("provider_name", ""),
                state=r.get("state", ""),
                city=r.get("city", ""),
                accreditation_org=accred_org,
                accreditation_type_code=code,
                accreditation_effective_date=r.get("accreditation_effective_date", ""),
                accreditation_expiration_date=r.get("accreditation_expiration_date", ""),
                certification_date=r.get("certification_date", ""),
                ownership_type=r.get("ownership_type", ""),
                bed_count=int(float(r.get("bed_count", 0) or 0)),
                medicare_medicaid=r.get("medicare_medicaid", ""),
                compliance_status=r.get("compliance_status", ""),
            ))

        search_term = ccn or provider_name
        response = AccreditationResponse(
            search_term=search_term,
            total_results=len(providers),
            providers=providers,
        )
        payload = response.model_dump()
        payload["source_metadata"] = source_metadata
        for provider in payload["providers"]:
            provider["evidence"] = _public_record_row_evidence(
                row=provider,
                parent_query=query_payload,
                source_name="CMS Provider of Services",
                source_url=data_loaders.POS_URL,
                dataset_id="cms_provider_of_services",
                match_basis="cms_provider_of_services_accreditation_row",
                confidence="source_row_with_ccn" if provider.get("ccn") else "candidate_public_record_row",
                caveat=(
                    "CMS Provider of Services accreditation rows are public administrative records; "
                    "cite source fields as source-scoped facility facts and preserve the CCN basis."
                ),
                next_step=(
                    "Use the CCN, provider name, accreditation dates, certification date, and POS cache metadata "
                    "when citing this accreditation row."
                ),
                source_metadata=source_metadata,
            )
        match_basis = "ccn_exact" if normalized_ccn else "provider_name_state_filter"
        confidence = "high_identifier_match" if normalized_ccn and rows else "candidate_facility_matches"
        if not rows:
            match_basis = _public_no_match_basis(match_basis)
            confidence = "no_matching_rows_in_loaded_cms_provider_of_services_cache"
        payload["evidence"] = _public_record_evidence(
            source_name="CMS Provider of Services",
            source_url=data_loaders.POS_URL,
            dataset_id="cms_provider_of_services",
            query=query_payload,
            match_basis=match_basis,
            confidence=confidence,
            caveat=(
                "CMS Provider of Services accreditation fields are source-scoped public records; "
                "names are candidate filters and should not replace exact CCN matching."
            ),
            next_step=(
                "Use CCN for reportable facility accreditation facts and preserve the POS source period/cache metadata."
            ),
            source_metadata=source_metadata,
        )
        payload["identity"] = _public_facility_identity(
            rows=rows,
            query=query_payload,
            source_name="CMS Provider of Services",
            source_url=data_loaders.POS_URL,
            match_basis=match_basis,
            confidence=confidence,
        ).to_dict()
        payload["identity_map"] = _public_facility_identity_map(
            rows=rows,
            query=query_payload,
            dataset_id="cms_provider_of_services",
            collection="providers",
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("get_accreditation failed")
        return error_response(f"get_accreditation failed: {e}")


# ---------------------------------------------------------------------------
# Tool 6: get_interop_status
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_interop_status(
    ccn: str = "", facility_name: str = "", state: str = "",
) -> dict[str, Any]:
    """Check Promoting Interoperability attestation and EHR certification.

    Uses CMS Promoting Interoperability dataset. Optionally enriches with
    ONC CHPL API for EHR product details (requires CHPL_API_KEY env var).

    Args:
        ccn: CMS Certification Number.
        facility_name: Search by facility name (partial match).
        state: Filter by state abbreviation.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_interop_status","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not ccn and not facility_name:
            return error_response("At least one of ccn or facility_name is required.")
        normalized_ccn = normalize_ccn(ccn) if ccn else ""
        if ccn and not normalized_ccn:
            return error_response("ccn must normalize to a six-character CMS Certification Number.", code="invalid_params")

        pi_loaded = await data_loaders.ensure_pi_cached()
        source_metadata = _public_cache_file_metadata(
            source_name="CMS Promoting Interoperability Hospital",
            source_url=data_loaders.PI_URL,
            dataset_id="cms_promoting_interoperability_hospital",
            cache_path=data_loaders._PI_PARQUET,
            loaded=pi_loaded,
            source_period="current CMS Promoting Interoperability hospital cache at query time",
            next_step="Refresh the CMS PI cache before citing no Promoting Interoperability or CEHRT rows.",
        )
        query_payload = {"ccn": normalized_ccn, "facility_name": facility_name, "state": state}

        rows = data_loaders.query_pi(
            ccn=normalized_ccn, facility_name=facility_name, state=state,
        )

        chpl_api_key = _os.environ.get("CHPL_API_KEY", "")

        # Deduplicate CHPL lookups: fetch each unique cehrt_id only once
        chpl_cache: dict[str, dict] = {}
        if chpl_api_key:
            unique_ids = {r.get("cehrt_id", "") for r in rows} - {""}
            for cid in list(unique_ids)[:10]:  # cap at 10 lookups
                chpl_data = await _lookup_chpl(cid, chpl_api_key)
                if chpl_data:
                    chpl_cache[cid] = chpl_data

        records: list[InteropRecord] = []
        for r in rows:
            ehr_product = r.get("ehr_product_name", "")
            ehr_developer = r.get("ehr_developer", "")
            cehrt_id = r.get("cehrt_id", "")

            # Enrich from pre-fetched CHPL data
            if cehrt_id in chpl_cache and not ehr_product:
                chpl_data = chpl_cache[cehrt_id]
                ehr_product = chpl_data.get("ehr_product_name", ehr_product)
                ehr_developer = chpl_data.get("ehr_developer", ehr_developer)

            records.append(InteropRecord(
                facility_name=r.get("facility_name", ""),
                ccn=r.get("ccn", ""),
                state=r.get("state", ""),
                city=r.get("city", ""),
                meets_pi_criteria=r.get("meets_pi_criteria", ""),
                cehrt_id=cehrt_id,
                reporting_period_start=r.get("reporting_period_start", ""),
                reporting_period_end=r.get("reporting_period_end", ""),
                ehr_product_name=ehr_product,
                ehr_developer=ehr_developer,
            ))

        search_term = ccn or facility_name
        response = InteropResponse(
            search_term=search_term,
            total_results=len(records),
            records=records,
        )
        payload = response.model_dump()
        payload["source_note"] = (
            "CMS Promoting Interoperability data supports interoperability/EHR fields present in the source. "
            "It does not establish a general cybersecurity attestation unless such a source field is present."
        )
        payload["source_type"] = "cms_promoting_interoperability"
        payload["can_assert_cybersecurity_attestation"] = False
        payload["source_metadata"] = source_metadata
        for record in payload["records"]:
            record["evidence"] = _public_record_row_evidence(
                row=record,
                parent_query=query_payload,
                source_name="CMS Promoting Interoperability Hospital",
                source_url=data_loaders.PI_URL,
                dataset_id="cms_promoting_interoperability_hospital",
                match_basis="cms_promoting_interoperability_hospital_row",
                confidence="source_row_with_ccn" if record.get("ccn") else "candidate_public_record_row",
                caveat=(
                    "CMS Promoting Interoperability rows are public interoperability/EHR records; "
                    "they do not establish broad cybersecurity attestation status."
                ),
                next_step=(
                    "Use the CCN, facility name, PI reporting period, CEHRT fields, and CMS PI cache metadata "
                    "when citing this interoperability row."
                ),
                source_metadata=source_metadata,
            )
        match_basis = "ccn_exact" if normalized_ccn else "facility_name_state_filter"
        confidence = "high_identifier_match" if normalized_ccn and rows else "candidate_facility_matches"
        if not rows:
            match_basis = _public_no_match_basis(match_basis)
            confidence = "no_matching_rows_in_loaded_cms_promoting_interoperability_cache"
        payload["evidence"] = _public_record_evidence(
            source_name="CMS Promoting Interoperability Hospital",
            source_url=data_loaders.PI_URL,
            dataset_id="cms_promoting_interoperability_hospital",
            query=query_payload,
            match_basis=match_basis,
            confidence=confidence,
            caveat=(
                "CMS PI and optional CHPL fields are interoperability/EHR public records; "
                "they do not establish broad cybersecurity attestation status."
            ),
            next_step=(
                "Use CCN for reportable PI/CEHRT facts and cite only fields present in CMS PI or CHPL source responses."
            ),
            source_metadata=source_metadata,
        )
        payload["identity"] = _public_facility_identity(
            rows=rows,
            query=query_payload,
            source_name="CMS Promoting Interoperability Hospital",
            source_url=data_loaders.PI_URL,
            match_basis=match_basis,
            confidence=confidence,
        ).to_dict()
        payload["identity_map"] = _public_facility_identity_map(
            rows=rows,
            query=query_payload,
            dataset_id="cms_promoting_interoperability_hospital",
            collection="records",
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("get_interop_status failed")
        return error_response(f"get_interop_status failed: {e}")


# ---------------------------------------------------------------------------
# Tool 7: HHS OIG LEIE screening
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def check_leie_npi(
    npi: str,
    limit: int = 25,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Check a provider NPI against the current HHS OIG LEIE exclusion file.

    Exact NPI matches are strong potential matches. The downloadable LEIE file
    does not include SSNs/EINs, so this tool does not provide final identity
    verification.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"check_leie_npi","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        normalized_npi = normalize_npi(npi)
        if not normalized_npi:
            return error_response(
                "A valid non-placeholder 10-digit NPI is required.",
                code="invalid_params",
            )

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        rows = data_loaders.query_leie_by_npi(normalized_npi)[:max(1, min(limit, 100))]
        records = _leie_records(rows)
        response = LEIESearchResponse(
            search_type="npi",
            query={"npi": normalized_npi},
            status=_leie_status(records),
            total_results=len(records),
            records=records,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        payload = response.model_dump()
        payload["evidence"] = _leie_evidence(
            metadata,
            query=payload["query"],
            match_basis="npi_exact" if records else "npi_exact_no_current_match",
            confidence="high_identifier_match" if records else "high_identifier_no_match_in_current_file",
        )
        payload["identity"] = _leie_identity(
            records=records,
            query=payload["query"],
            metadata=metadata,
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("check_leie_npi failed")
        return error_response(f"check_leie_npi failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_leie_individual(
    last_name: str,
    first_name: str = "",
    state: str = "",
    dob: str = "",
    limit: int = 25,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Search the current HHS OIG LEIE file for an excluded individual name.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_leie_individual","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not last_name.strip():
            return error_response("last_name is required.", code="invalid_params")

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        rows = data_loaders.query_leie_by_individual(
            last_name=last_name,
            first_name=first_name,
            state=state,
            dob=dob,
            limit=limit,
        )
        records = _leie_records(rows)
        response = LEIESearchResponse(
            search_type="individual",
            query={
                "last_name": last_name,
                "first_name": first_name,
                "state": state,
                "dob": dob,
            },
            status=_leie_status(records),
            total_results=len(records),
            records=records,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        payload = response.model_dump()
        payload["evidence"] = _leie_evidence(
            metadata,
            query=payload["query"],
            match_basis=records[0].match_basis if records else "name_state_dob_no_current_match",
            confidence="potential_name_match" if records else "no_current_match_for_supplied_terms",
        )
        payload["identity"] = _leie_identity(
            records=records,
            query=payload["query"],
            metadata=metadata,
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("search_leie_individual failed")
        return error_response(f"search_leie_individual failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_leie_entity(
    entity_name: str = "",
    state: str = "",
    npi: str = "",
    limit: int = 25,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Search the current HHS OIG LEIE file for an excluded business/entity.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_leie_entity","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not entity_name.strip() and not npi.strip():
            return error_response("entity_name or npi is required.", code="invalid_params")
        normalized_npi = normalize_npi(npi) if npi.strip() else None
        if npi.strip() and not normalized_npi:
            return error_response(
                "When provided, npi must be a valid non-placeholder 10-digit NPI.",
                code="invalid_params",
            )

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        rows = data_loaders.query_leie_by_entity(
            entity_name=entity_name,
            state=state,
            npi=normalized_npi or "",
            limit=limit,
        )
        records = _leie_records(rows)
        response = LEIESearchResponse(
            search_type="entity",
            query={"entity_name": entity_name, "state": state, "npi": normalized_npi or ""},
            status=_leie_status(records),
            total_results=len(records),
            records=records,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        payload = response.model_dump()
        payload["evidence"] = _leie_evidence(
            metadata,
            query=payload["query"],
            match_basis=records[0].match_basis if records else "entity_terms_no_current_match",
            confidence="potential_entity_match" if records else "no_current_match_for_supplied_terms",
        )
        payload["identity"] = _leie_identity(
            records=records,
            query=payload["query"],
            metadata=metadata,
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("search_leie_entity failed")
        return error_response(f"search_leie_entity failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def screen_leie_batch(
    candidates: list[dict[str, str]],
    limit_per_candidate: int = 5,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Screen up to 100 people/entities against the current HHS OIG LEIE file.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"screen_leie_batch","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if len(candidates) > 100:
            return error_response(
                "screen_leie_batch accepts at most 100 candidates per call.",
                code="invalid_params",
            )
        if any(_contains_sensitive_identifier_keys(dict(candidate)) for candidate in candidates):
            return error_response(
                "LEIE screening does not accept SSN, EIN, TIN, or tax identifier fields.",
                code="invalid_params",
            )

        metadata = await data_loaders.ensure_leie_cached(force_refresh=force_refresh)
        if metadata.get("cache_status") == "unavailable":
            return error_response(
                "LEIE cache is unavailable.",
                code="source_unavailable",
                detail=metadata.get("last_error", ""),
                retryable=True,
                source_metadata=metadata,
            )

        raw_results = data_loaders.screen_leie_candidates(
            [dict(candidate) for candidate in candidates],
            limit_per_candidate=limit_per_candidate,
        )
        results: list[LEIEBatchResult] = []
        for raw in raw_results:
            result_metadata = raw.get("source_metadata") or metadata
            results.append(LEIEBatchResult(
                candidate=LEIEBatchCandidate(
                    **{
                        k: v
                        for k, v in raw.get("candidate", {}).items()
                        if k in LEIEBatchCandidate.model_fields
                    }
                ),
                status=raw.get("status", "no_current_leie_match_found"),
                match_count=int(raw.get("match_count", 0) or 0),
                best_match_score=int(raw.get("best_match_score", 0) or 0),
                matches=_leie_records(raw.get("matches", [])),
                screened_at=str(raw.get("screened_at", "")),
                source_metadata=_leie_source_metadata(result_metadata),
                oig_verification_caveat=OIG_LEIE_CAVEAT,
            ))

        response = LEIEBatchResponse(
            total_candidates=len(candidates),
            results=results,
            source_metadata=_leie_source_metadata(metadata),
            oig_verification_caveat=OIG_LEIE_CAVEAT,
        )
        payload = response.model_dump()
        payload["evidence"] = _leie_evidence(
            metadata,
            query={"candidate_count": len(candidates), "limit_per_candidate": limit_per_candidate},
            match_basis="batch_candidate_screening",
            confidence="candidate_level_match_confidence",
        )
        for result_payload, result in zip(payload["results"], results, strict=True):
            result_payload["identity"] = _leie_identity(
                records=result.matches,
                query={},
                metadata=result.source_metadata.model_dump(),
                candidate=result.candidate.model_dump(),
                match_basis=result.matches[0].match_basis if result.matches else result.status,
                confidence=result.status,
            )
        payload["identity_map"] = [result["identity"] for result in payload["results"]]
        return to_structured(payload)
    except ValueError as e:
        return error_response(str(e), code="invalid_params")
    except Exception as e:
        logger.exception("screen_leie_batch failed")
        return error_response(f"screen_leie_batch failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_leie_metadata(force_refresh: bool = False) -> dict[str, Any]:
    """Return HHS OIG LEIE source/cache metadata without screening a person.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_leie_metadata","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        metadata = (
            await data_loaders.ensure_leie_cached(force_refresh=True)
            if force_refresh
            else data_loaders.get_leie_source_metadata()
        )
        source_metadata = _leie_source_metadata(metadata).model_dump()
        payload = dict(source_metadata)
        payload["source_metadata"] = source_metadata
        payload["evidence"] = _leie_evidence(
            source_metadata,
            query={"metadata_only": True, "force_refresh": force_refresh},
            match_basis="source_metadata_lookup",
            confidence="source_cache_metadata",
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("get_leie_metadata failed")
        return error_response(f"get_leie_metadata failed: {e}")


# ---------------------------------------------------------------------------
# Tool 8: SAM.gov Exclusions screening
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def search_sam_exclusions(
    entity_name: str = "",
    first_name: str = "",
    last_name: str = "",
    uei: str = "",
    cage_code: str = "",
    npi: str = "",
    state: str = "",
    country: str = "",
    classification: str = "",
    exclusion_type: str = "",
    excluding_agency: str = "",
    limit: int = 10,
) -> dict[str, Any]:
    """Search active SAM.gov Exclusions records through the v4 JSON API.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_sam_exclusions","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        query = _sam_query_payload(
            entity_name=entity_name,
            first_name=first_name,
            last_name=last_name,
            uei=uei,
            cage_code=cage_code,
            npi=npi,
            state=state,
            country=country,
            classification=classification,
            exclusion_type=exclusion_type,
            excluding_agency=excluding_agency,
        )
        if not query:
            return error_response(
                "At least one SAM.gov Exclusions search parameter is required.",
                code="invalid_params",
            )

        normalized_npi = normalize_npi(npi) if npi.strip() else ""
        if npi.strip() and not normalized_npi:
            return error_response(
                "When provided, npi must be a valid non-placeholder 10-digit NPI.",
                code="invalid_params",
            )

        raw = await sam_exclusions_client.search_exclusions(
            entity_name=entity_name,
            first_name=first_name,
            last_name=last_name,
            uei=uei,
            cage_code=cage_code,
            npi=normalized_npi or npi,
            state=state,
            country=country,
            classification=classification,
            exclusion_type=exclusion_type,
            excluding_agency=excluding_agency,
            limit=limit,
        )
        if "error" in raw:
            return _sam_error_response(raw, "search_sam_exclusions")

        metadata = _sam_source_metadata(raw.get("source_metadata", {}))
        records = _sam_records(raw.get("excludedEntity", []), metadata.query)
        response = SAMExclusionSearchResponse(
            search_type="search",
            query=query,
            status=_sam_status(records),
            total_results=int(raw.get("totalRecords", len(records)) or 0),
            records=records,
            source_metadata=metadata,
            sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
        )
        payload = response.model_dump()
        payload["evidence"] = _sam_evidence(
            metadata,
            query=payload["query"],
            match_basis=records[0].match_basis if records else "search_terms_no_current_match",
            confidence="potential_match" if records else "no_current_match_for_supplied_terms",
        )
        payload["identity"] = _sam_identity(
            records=records,
            query=payload["query"],
            metadata=metadata,
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("search_sam_exclusions failed")
        return error_response(f"search_sam_exclusions failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def check_sam_exclusion_identifier(
    uei: str = "",
    cage_code: str = "",
    npi: str = "",
    limit: int = 10,
) -> dict[str, Any]:
    """Check public identifiers against active SAM.gov Exclusions records.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"check_sam_exclusion_identifier","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if not uei.strip() and not cage_code.strip() and not npi.strip():
            return error_response(
                "At least one of uei, cage_code, or npi is required.",
                code="invalid_params",
            )
        normalized_npi = normalize_npi(npi) if npi.strip() else ""
        if npi.strip() and not normalized_npi:
            return error_response(
                "When provided, npi must be a valid non-placeholder 10-digit NPI.",
                code="invalid_params",
            )

        raw = await sam_exclusions_client.check_identifier(
            uei=uei,
            cage_code=cage_code,
            npi=normalized_npi or npi,
            limit=limit,
        )
        if "error" in raw:
            return _sam_error_response(raw, "check_sam_exclusion_identifier")

        metadata = _sam_source_metadata(raw.get("source_metadata", {}))
        records = _sam_records(raw.get("excludedEntity", []), metadata.query)
        response = SAMExclusionSearchResponse(
            search_type="identifier",
            query=_sam_query_payload(uei=uei, cage_code=cage_code, npi=normalized_npi or npi),
            status=_sam_status(records),
            total_results=int(raw.get("totalRecords", len(records)) or 0),
            records=records,
            source_metadata=metadata,
            sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
        )
        payload = response.model_dump()
        payload["evidence"] = _sam_evidence(
            metadata,
            query=payload["query"],
            match_basis=records[0].match_basis if records else "identifier_no_current_match",
            confidence="high_identifier_match" if records else "high_identifier_no_match_in_current_api_response",
        )
        payload["identity"] = _sam_identity(
            records=records,
            query=payload["query"],
            metadata=metadata,
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        return to_structured(payload)
    except Exception as e:
        logger.exception("check_sam_exclusion_identifier failed")
        return error_response(f"check_sam_exclusion_identifier failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def screen_sam_exclusions_batch(
    candidates: list[dict[str, str]],
    limit_per_candidate: int = 5,
) -> dict[str, Any]:
    """Screen up to 100 candidates against active SAM.gov Exclusions records.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"screen_sam_exclusions_batch","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        if len(candidates) > sam_exclusions_client.MAX_BATCH_SIZE:
            return error_response(
                f"screen_sam_exclusions_batch accepts at most {sam_exclusions_client.MAX_BATCH_SIZE} "
                "candidates per call.",
                code="invalid_params",
            )
        if any(_contains_sensitive_identifier_keys(dict(candidate)) for candidate in candidates):
            return error_response(
                "SAM.gov Exclusions screening does not accept SSN, EIN, TIN, or tax identifier fields.",
                code="invalid_params",
            )

        safe_limit = max(1, min(int(limit_per_candidate), 10))
        results: list[SAMExclusionBatchResult] = []
        response_metadata = sam_exclusions_client.source_metadata(limit=safe_limit)
        for candidate_payload in candidates:
            candidate_data = {
                key: value
                for key, value in dict(candidate_payload).items()
                if key in SAMExclusionBatchCandidate.model_fields
            }
            candidate = SAMExclusionBatchCandidate(**candidate_data)
            if candidate.npi.strip():
                normalized_npi = normalize_npi(candidate.npi)
                if not normalized_npi:
                    return error_response(
                        "When provided in a SAM.gov Exclusions batch candidate, "
                        "npi must be a valid non-placeholder 10-digit NPI.",
                        code="invalid_params",
                    )
                candidate.npi = normalized_npi
            raw = await sam_exclusions_client.search_exclusions(
                entity_name=candidate.entity_name,
                first_name=candidate.first_name,
                last_name=candidate.last_name,
                uei=candidate.uei,
                cage_code=candidate.cage_code,
                npi=candidate.npi,
                state=candidate.state,
                country=candidate.country,
                classification=candidate.classification,
                limit=safe_limit,
            )
            if "error" in raw:
                metadata = _sam_source_metadata(raw.get("source_metadata", {}))
                results.append(SAMExclusionBatchResult(
                    candidate=candidate,
                    status="source_error",
                    match_count=0,
                    matches=[],
                    match_basis="source_error",
                    best_match_score=0,
                    screened_at=datetime.now(UTC).isoformat(),
                    source_metadata=metadata,
                    sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
                ))
                response_metadata = metadata.model_dump()
                continue

            metadata = _sam_source_metadata(raw.get("source_metadata", {}))
            records = _sam_records(raw.get("excludedEntity", []), metadata.query)
            status = _sam_status(records)
            results.append(SAMExclusionBatchResult(
                candidate=candidate,
                status=status,
                match_count=len(records),
                matches=records,
                match_basis=records[0].match_basis if records else "",
                best_match_score=max((record.match_score for record in records), default=0),
                screened_at=datetime.now(UTC).isoformat(),
                source_metadata=metadata,
                sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
            ))
            response_metadata = metadata.model_dump()

        response = SAMExclusionBatchResponse(
            total_candidates=len(candidates),
            results=results,
            source_metadata=_sam_source_metadata(response_metadata),
            sam_verification_caveat=SAM_EXCLUSIONS_CAVEAT,
        )
        response_source_metadata = _sam_source_metadata(response_metadata)
        payload = response.model_dump()
        payload["evidence"] = _sam_evidence(
            response_source_metadata,
            query={"candidate_count": len(candidates), "limit_per_candidate": safe_limit},
            match_basis="batch_candidate_screening",
            confidence="candidate_level_match_confidence",
        )
        for result_payload, result in zip(payload["results"], results, strict=True):
            result_payload["identity"] = _sam_identity(
                records=result.matches,
                query={},
                metadata=result.source_metadata,
                candidate=result.candidate.model_dump(),
                match_basis=result.match_basis or result.status,
                confidence=result.status,
            )
        payload["identity_map"] = [result["identity"] for result in payload["results"]]
        return to_structured(payload)
    except Exception as e:
        logger.exception("screen_sam_exclusions_batch failed")
        return error_response(f"screen_sam_exclusions_batch failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("public-records")
async def get_sam_exclusions_metadata() -> dict[str, Any]:
    """Return SAM.gov Exclusions API metadata without running a search.

    Discovery
    ---------
    - Inspect this server's healthcare-data://server/.../capabilities resource for datasets, cache needs, and capability clusters.
    - Use discovery workflow plans when you need cross-server call order, source caveats, or identity handoffs.

    When to use
    -----------
    - Use this tool only for its named public healthcare data task.
    - Prefer exact identifiers when available; use search tools first when you only have names or partial context.
    - NOT for: patient-level data, PHI, legal clearance, or substituting adjacent public sources for exact source-backed facts.

    Parameters
    ----------
    See the function signature and parameter descriptions above. Preserve exact public identifiers such as CCN, NPI, ZCTA, state, dataset_id, workflow_id, or source-specific IDs.

    Returns
    -------
    dict
        Structured JSON-compatible payload. Preserve evidence, source_metadata, identity, and identity_map fields when present.

    Do / Don't
    ----------
    Do:
    - Preserve source evidence and identity fields with cited facts.
    - Follow returned next_step or next_actions hints before making source claims.

    Don't:
    - Treat candidate search rows as exact matches without exact identifiers.
    - Pass placeholders like <ccn> or YOUR_VALUE as real arguments.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_sam_exclusions_metadata","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        metadata = sam_exclusions_client.source_metadata()
        source_metadata = _sam_source_metadata(metadata)
        payload = source_metadata.model_dump()
        payload["source_metadata"] = payload.copy()
        payload["evidence"] = _sam_evidence(
            source_metadata,
            query={"metadata_only": True},
            match_basis="source_metadata_lookup",
            confidence="api_metadata",
        )
        payload["identity_map"] = {
            "entity_scope": "public_records_source_metadata",
            "join_keys": [
                {
                    "field": "dataset_id",
                    "values": ["sam_gov_exclusions"],
                    "status": "source_metadata",
                    "used_by": ["sam_gov_exclusions_metadata"],
                },
                {
                    "field": "source_name",
                    "values": [payload["source_name"]],
                    "status": "source_metadata",
                    "used_by": ["sam_gov_exclusions_metadata"],
                },
            ],
            "source_claims": [
                _public_source_claim(
                    collection="sam_gov_exclusions_metadata",
                    dataset_id="sam_gov_exclusions",
                    match_policy="source_metadata_lookup_no_entity_match_claim",
                    identity_paths=["source_name", "source_url", "evidence.query"],
                )
            ],
            "conflict_policy": [
                "Treat this metadata-only response as source status and query context, not an entity exclusion match.",
                "Use SAM.gov Exclusions search or identifier tools for candidate-level screening claims.",
            ],
            "missing_data_policy": (
                "Metadata availability describes SAM.gov Exclusions source configuration only; "
                "it is not evidence that an entity is or is not excluded."
            ),
        }
        return to_structured(payload)
    except Exception as e:
        logger.exception("get_sam_exclusions_metadata failed")
        return error_response(f"get_sam_exclusions_metadata failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
