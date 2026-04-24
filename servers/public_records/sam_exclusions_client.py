"""SAM.gov Exclusions API v4 JSON client.

Searches active public exclusion records through the synchronous JSON API.
Bulk extract/download modes are intentionally out of scope for this client.

API docs: https://open.gsa.gov/api/exclusions-api/
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging
import os
import re
from typing import Any

from shared.utils.http_client import resilient_request
from shared.utils.identity import normalize_npi

logger = logging.getLogger(__name__)

BASE_URL = "https://api.sam.gov/entity-information/v4/exclusions"
DOCS_URL = "https://open.gsa.gov/api/exclusions-api/"
SOURCE_NAME = "SAM.gov Exclusions"
API_VERSION = "v4"
TIMEOUT = 30.0
PAGE_SIZE = 10
MAX_SEARCH_LIMIT = 50
MAX_BATCH_SIZE = 100

SAM_EXCLUSIONS_CAVEAT = (
    "SAM.gov Exclusions is an official federal screening source for active "
    "exclusion records. Name matches are potential matches; verify against "
    "the full SAM.gov record and agency guidance before making eligibility "
    "or contracting decisions."
)

_DISALLOWED_PARAM_CHARS = re.compile(r"[&|{}^\\]+")
_API_KEY_QUERY_RE = re.compile(r"(?i)(api_key=)[^&\s]+")


def _get_api_key() -> str | None:
    return os.environ.get("SAM_GOV_API_KEY")


def source_metadata(
    *,
    query: dict[str, Any] | None = None,
    total_records: int = 0,
    returned_records: int = 0,
    limit: int = 0,
    page_count: int = 0,
    has_more: bool = False,
    last_error: str = "",
) -> dict[str, Any]:
    """Build stable source metadata for tool responses and errors."""
    return {
        "source_name": SOURCE_NAME,
        "source_url": BASE_URL,
        "docs_url": DOCS_URL,
        "api_version": API_VERSION,
        "queried_at": datetime.now(UTC).isoformat(),
        "query": query or {},
        "total_records": total_records,
        "returned_records": returned_records,
        "limit": limit,
        "page_count": page_count,
        "has_more": has_more,
        "api_key_configured": bool(_get_api_key()),
        "last_error": last_error,
    }


def _clean_text(value: str) -> str:
    """Remove characters SAM.gov disallows in query parameter values."""
    return " ".join(_DISALLOWED_PARAM_CHARS.sub(" ", value).split())


def _clean_filter(value: str) -> str:
    return _clean_text(value.strip())


def _redact_api_keys(value: Any) -> Any:
    if isinstance(value, str):
        return _API_KEY_QUERY_RE.sub(r"\1[REDACTED]", value)
    if isinstance(value, dict):
        return {key: _redact_api_keys(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_api_keys(item) for item in value]
    return value


def _excluded_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records = payload.get("excludedEntity") or []
    if isinstance(records, dict):
        return [records]
    if not isinstance(records, list):
        return []
    return [record for record in records if isinstance(record, dict)]


def _clamp_limit(limit: int, *, maximum: int = MAX_SEARCH_LIMIT) -> int:
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        parsed = PAGE_SIZE
    return max(1, min(parsed, maximum))


def _excluding_agency_params(excluding_agency: str) -> dict[str, str]:
    agency = _clean_filter(excluding_agency)
    if not agency:
        return {}
    compact = agency.replace("-", "")
    if len(agency) <= 8 and compact.isalnum() and agency.upper() == agency:
        return {"excludingAgencyCode": agency}
    return {"excludingAgencyName": agency}


def build_search_params(
    *,
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
    limit: int = PAGE_SIZE,
    page: int = 0,
) -> dict[str, Any]:
    """Map tool inputs onto SAM.gov Exclusions API v4 query parameters."""
    params: dict[str, Any] = {
        "recordStatus": "active",
        "page": max(0, int(page)),
        "size": min(_clamp_limit(limit), PAGE_SIZE),
    }

    name_parts = [_clean_text(part) for part in (entity_name, first_name, last_name) if part.strip()]
    if name_parts:
        params["exclusionName"] = " ".join(name_parts)

    if uei.strip():
        params["ueiSAM"] = _clean_filter(uei).upper()
    if cage_code.strip():
        params["cageCode"] = _clean_filter(cage_code).upper()
    if npi.strip():
        normalized_npi = normalize_npi(npi)
        params["npi"] = normalized_npi or _clean_filter(npi)
    if state.strip():
        params["stateProvince"] = _clean_filter(state).upper()
    if country.strip():
        params["country"] = _clean_filter(country).upper()
    if classification.strip():
        params["classification"] = _clean_filter(classification)
    if exclusion_type.strip():
        params["exclusionType"] = _clean_filter(exclusion_type)
    params.update(_excluding_agency_params(excluding_agency))
    return params


def _missing_api_key_error(query: dict[str, Any], limit: int) -> dict[str, Any]:
    message = "SAM_GOV_API_KEY is not set."
    return {
        "error": message,
        "code": "missing_api_key",
        "retryable": False,
        "instructions": (
            "Register for a SAM.gov public API key in the SAM.gov account "
            "details page, then set SAM_GOV_API_KEY in the server environment."
        ),
        "source_metadata": source_metadata(
            query=query,
            limit=limit,
            last_error=message,
        ),
    }


def _api_error_payload(exc: Exception, query: dict[str, Any], limit: int) -> dict[str, Any]:
    detail: dict[str, Any] = {}
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code is not None:
        detail["status_code"] = status_code
    if response is not None:
        try:
            detail["body"] = _redact_api_keys(response.json())
        except Exception:
            detail["body"] = _redact_api_keys(getattr(response, "text", ""))

    retryable = bool(status_code in {429, 500, 502, 503, 504})
    message = "SAM.gov Exclusions API request failed."
    return {
        "error": message,
        "code": "source_unavailable",
        "detail": detail or str(exc),
        "retryable": retryable,
        "source_metadata": source_metadata(
            query=query,
            limit=limit,
            last_error=str(_redact_api_keys(str(exc))),
        ),
    }


async def search_exclusions(
    *,
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
    limit: int = PAGE_SIZE,
) -> dict[str, Any]:
    """Search active SAM.gov Exclusions records and return raw JSON records."""
    safe_limit = _clamp_limit(limit)
    query = build_search_params(
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
        limit=safe_limit,
    )
    api_key = _get_api_key()
    if not api_key:
        return _missing_api_key_error(query, safe_limit)

    records: list[dict[str, Any]] = []
    total_records = 0
    page_count = 0

    try:
        while len(records) < safe_limit:
            params = dict(query)
            params["api_key"] = api_key
            params["page"] = page_count
            params["size"] = min(PAGE_SIZE, safe_limit - len(records))

            response = await resilient_request("GET", BASE_URL, params=params, timeout=TIMEOUT)
            payload = response.json()
            page_records = _excluded_records(payload)
            total_records = int(payload.get("totalRecords") or len(page_records) or total_records)
            records.extend(page_records)
            page_count += 1

            if not page_records or len(records) >= total_records:
                break

        has_more = total_records > len(records)
        return {
            "totalRecords": total_records,
            "excludedEntity": records[:safe_limit],
            "source_metadata": source_metadata(
                query=query,
                total_records=total_records,
                returned_records=min(len(records), safe_limit),
                limit=safe_limit,
                page_count=page_count,
                has_more=has_more,
            ),
        }
    except Exception as exc:
        logger.warning("SAM.gov Exclusions search failed: %s", _redact_api_keys(str(exc)))
        return _api_error_payload(exc, query, safe_limit)


async def check_identifier(
    *,
    uei: str = "",
    cage_code: str = "",
    npi: str = "",
    limit: int = PAGE_SIZE,
) -> dict[str, Any]:
    """Search SAM.gov Exclusions by one or more public identifiers."""
    return await search_exclusions(uei=uei, cage_code=cage_code, npi=npi, limit=limit)
