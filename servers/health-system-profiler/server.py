"""Health System Profiler MCP Server.

Returns complete health system profiles in 1-3 tool calls by combining
AHRQ Compendium, CMS Provider of Services, NPPES, and HSAF data.
"""

from typing import Any
import json
import logging
import os as _os
import sys
from pathlib import Path

import pandas as pd
from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.healthcare_identity import identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_name, normalize_npi
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils.source_backed_result import values_at_path

# Support running both as a package and as a standalone script
try:
    from .data_loaders import (
        AHRQ_HOSPITAL_LINKAGE_CACHE,
        AHRQ_SYSTEM_CACHE,
        load_ahrq_hospital_linkage,
        load_ahrq_systems,
        load_pos,
        NPPES_API_URL,
        search_nppes,
    )
    from .facility_enrichment import aggregate_off_site, enrich_facility
    from .graph_expansion import expand_related_providers
    from .generic_reconciliation import reconcile_generic_system_facilities
    from .jefferson_resolver import (
        JEFFERSON_SLUG,
        build_combined_system_profile,
        reconcile_system_facilities as reconcile_jefferson_facilities,
        resolve_combined_system_slug,
    )
    from .models import (
        BedBreakdown,
        FacilitySummary,
        HealthSystemSummary,
        SystemProfileResponse,
    )
    from .outpatient_discovery import build_search_patterns, parse_nppes_results
    from .profile_evidence_pack import (
        build_profile_evidence_pack as assemble_profile_evidence_pack,
        census_geocode_address,
        osm_geocode_address,
        reverse_geocode_coordinates,
    )
    from .system_discovery import fuzzy_search_systems, resolve_system_ccns
    from .system_metrics import (
        get_health_system_metric as assemble_health_system_metric,
        invalid_argument_payload,
        list_health_system_metric_rows,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from data_loaders import (
        AHRQ_HOSPITAL_LINKAGE_CACHE,
        AHRQ_SYSTEM_CACHE,
        load_ahrq_hospital_linkage,
        load_ahrq_systems,
        load_pos,
        NPPES_API_URL,
        search_nppes,
    )
    from facility_enrichment import aggregate_off_site, enrich_facility
    from graph_expansion import expand_related_providers
    from generic_reconciliation import reconcile_generic_system_facilities
    from jefferson_resolver import (
        JEFFERSON_SLUG,
        build_combined_system_profile,
        reconcile_system_facilities as reconcile_jefferson_facilities,
        resolve_combined_system_slug,
    )
    from models import (
        BedBreakdown,
        FacilitySummary,
        HealthSystemSummary,
        SystemProfileResponse,
    )
    from outpatient_discovery import build_search_patterns, parse_nppes_results
    from profile_evidence_pack import (
        build_profile_evidence_pack as assemble_profile_evidence_pack,
        census_geocode_address,
        osm_geocode_address,
        reverse_geocode_coordinates,
    )
    from system_discovery import fuzzy_search_systems, resolve_system_ccns
    from system_metrics import (
        get_health_system_metric as assemble_health_system_metric,
        invalid_argument_payload,
        list_health_system_metric_rows,
    )

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "health-system-profiler"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8007"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "health-system-profiler")


def _system_evidence(*, query: dict[str, Any], match_basis: str, confidence: str) -> dict[str, Any]:
    return evidence_receipt(
        source_name="AHRQ Compendium, CMS Provider of Services, and NPPES public registry",
        source_url="https://www.ahrq.gov/chsp/data-resources/compendium.html",
        dataset_id="ahrq_health_system_compendium",
        source_period="AHRQ Compendium 2023 with CMS POS/NPPES enrichment from configured local cache or live public lookup",
        landing_page="https://www.ahrq.gov/chsp/data-resources/compendium.html",
        cache_status="mixed_public_cache",
        cache_freshness="AHRQ/CMS source freshness depends on local cache files; NPPES outpatient discovery is live when enabled",
        entity_scope="health_system_facility_identity",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat="AHRQ Compendium linkage is public source context and may lag mergers or local operating names.",
        next_step="Use reconciliation output and source-specific facility aliases before asserting system membership.",
    )


def _system_source_metadata(evidence: dict[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a health-system evidence receipt."""

    return {
        "source_name": evidence.get("source_name", ""),
        "source_url": evidence.get("source_url", ""),
        "dataset_id": evidence.get("dataset_id", ""),
        "source_period": evidence.get("source_period", ""),
        "landing_page": evidence.get("landing_page", ""),
        "retrieved_at": evidence.get("retrieved_at", ""),
        "source_modified": evidence.get("source_modified", ""),
        "cache_status": evidence.get("cache_status", ""),
        "cache_freshness": evidence.get("cache_freshness", ""),
        "entity_scope": evidence.get("entity_scope", "health_system_facility_identity"),
        "query": evidence.get("query", {}),
        "cache_key": evidence.get("cache_key", ""),
        "source_type": "ahrq_cms_nppes_health_system_public_sources",
    }


def _attach_system_source_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        payload["source_metadata"] = _system_source_metadata(evidence)
    identity_map = payload.get("identity_map")
    if isinstance(identity_map, dict) and isinstance(identity_map.get("source_claims"), list):
        for claim in identity_map["source_claims"]:
            if not isinstance(claim, dict):
                continue
            row_paths = [path for path in claim.get("row_evidence_paths", []) if values_at_path(payload, str(path))]
            if row_paths:
                claim["row_evidence_paths"] = row_paths
            else:
                claim.pop("row_evidence_paths", None)
    return payload


def _system_row_evidence(
    row: dict[str, Any],
    *,
    query: dict[str, Any],
    row_kind: str,
    match_basis: str,
    confidence: str | None = None,
) -> dict[str, Any]:
    """Return a source receipt for a health-system row separated from its parent response."""

    source_url = str(row.get("url") or "https://www.ahrq.gov/chsp/data-resources/compendium.html")
    source_name = str(row.get("source") or "AHRQ Compendium, CMS Provider of Services, and NPPES public registry")
    dataset_id = "reviewed_jefferson_merger_evidence" if row_kind == "merger_evidence" else "ahrq_health_system_compendium"
    source_period = (
        "AHRQ Compendium 2023 with CMS POS/NPPES enrichment from configured local cache or reviewed merger evidence where present"
    )
    landing_page = "https://www.ahrq.gov/chsp/data-resources/compendium.html"
    cache_status = "mixed_public_cache"
    cache_freshness = "AHRQ/CMS source freshness depends on local cache files; reviewed merger rows use cited public URLs"
    if row_kind == "outpatient_site":
        npi = str(row.get("npi") or "").strip()
        source_name = "NPPES NPI Registry"
        source_url = f"{NPPES_API_URL}?number={npi}&version=2.1" if npi else NPPES_API_URL
        dataset_id = "nppes_npi_registry"
        source_period = "NPPES public registry lookup at request time"
        landing_page = "https://npiregistry.cms.hhs.gov/search"
        cache_status = "live_api"
        cache_freshness = "Live NPPES lookup; row freshness is controlled by the NPPES public registry response"
    row_system_name = row.get("system_name") or row.get("health_sys_name") or query.get("system_name") or ""
    if row_kind == "system_search_result" and not row_system_name:
        row_system_name = row.get("name") or ""
    row_query = {
        "row_kind": row_kind,
        "query": query.get("query") or "",
        "system_id": row.get("system_id") or row.get("health_sys_id") or query.get("system_id") or query.get("system_slug") or "",
        "system_name": row_system_name,
        "ccn": row.get("ccn") or "",
        "npi": row.get("npi") or "",
        "facility_name": row.get("facility_name") or (row.get("name") if row_kind != "system_search_result" else "") or "",
        "zip_code": row.get("zip_code") or row.get("zip") or "",
        "taxonomy_code": row.get("taxonomy_code") or "",
        "taxonomy_description": row.get("taxonomy_description") or "",
        "category": row.get("category") or "",
        "source_refs": row.get("source_refs") or "",
        "as_of_date": query.get("as_of_date") or query.get("edition_date") or "",
        "source_url": row.get("url") or "",
    }
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        landing_page=landing_page,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope="health_system_facility_identity",
        query={key: value for key, value in row_query.items() if value not in ("", [], None)},
        match_basis=match_basis,
        confidence=confidence or str(row.get("confidence") or "source_row"),
        caveat=(
            "NPPES outpatient sites are organization registry leads discovered by system-name search; exact NPI supports the site row, "
            "but system affiliation remains candidate context."
            if row_kind == "outpatient_site"
            else "Health-system rows are source-scoped public records or reviewed public evidence; names and aliases are candidate context unless exact system, CCN, or NPI identifiers support the join."
        ),
        next_step=(
            "Verify the NPI Registry row and preserve the parent identity_map before citing outpatient-site affiliation."
            if row_kind == "outpatient_site"
            else "Preserve this row receipt with the system ID, CCN/NPI, source refs, and parent identity_map before citing system membership or facility facts."
        ),
    )


def _attach_system_row_evidence(payload: dict[str, Any], *, query: dict[str, Any]) -> dict[str, Any]:
    """Attach row receipts to common health-system result collections in-place."""

    for row in payload.get("results") or []:
        if isinstance(row, dict):
            row["evidence"] = _system_row_evidence(
                row,
                query=query,
                row_kind="system_search_result",
                match_basis="ahrq_system_search_result",
                confidence="candidate_system_match",
            )

    row_kind_by_collection = {
        "inpatient_facilities": "inpatient_facility",
        "sub_entities": "sub_entity",
        "outpatient_sites": "outpatient_site",
    }
    for collection, row_kind in row_kind_by_collection.items():
        for row in payload.get(collection) or []:
            if isinstance(row, dict):
                row["evidence"] = _system_row_evidence(
                    row,
                    query=query,
                    row_kind=row_kind,
                    match_basis=f"{row_kind}_source_row",
                )

    reconciliation = payload.get("facility_reconciliation")
    if isinstance(reconciliation, dict):
        _attach_reconciliation_row_evidence(reconciliation, query=query)

    _attach_reconciliation_row_evidence(payload, query=query)
    return _attach_system_source_metadata(payload)


def _attach_reconciliation_row_evidence(payload: dict[str, Any], *, query: dict[str, Any]) -> None:
    for row in payload.get("facilities") or []:
        if isinstance(row, dict):
            row["evidence"] = _system_row_evidence(
                row,
                query=query,
                row_kind="facility_reconciliation",
                match_basis="health_system_facility_reconciliation_row",
            )
    for row in payload.get("merger_evidence") or []:
        if isinstance(row, dict):
            row["evidence"] = _system_row_evidence(
                row,
                query=query,
                row_kind="merger_evidence",
                match_basis="reviewed_merger_evidence_row",
                confidence="reviewed_public_source",
            )


def _system_error_response(
    message: str,
    *,
    code: str = "not_found",
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    next_step: str,
) -> dict[str, Any]:
    evidence = _system_evidence(query=query, match_basis=match_basis, confidence=confidence)
    evidence["next_step"] = next_step
    return error_response(
        message,
        code=code,
        evidence=evidence,
        source_metadata=_system_source_metadata(evidence),
        identity=identity_from_public_record(
            name=str(query.get("system_name") or query.get("system_slug") or ""),
            entity_type="health_system",
            ahrq_system_id=query.get("system_id") or "",
            source_name="workflow_or_tool_input",
        ).to_dict(),
        identity_map=_system_identity_map(
            query=query,
            system_id=str(query.get("system_id") or ""),
            system_name=str(query.get("system_name") or query.get("system_slug") or ""),
        ),
    )


def _system_identity_map(
    *,
    query: dict[str, Any],
    system_id: str = "",
    system_ids: list[Any] | None = None,
    system_name: str = "",
    system_names: list[Any] | None = None,
    facilities: list[dict[str, Any]] | None = None,
    result_collection: str = "",
) -> dict[str, Any]:
    """Return the health-system identity spine used by cross-server workflows."""

    facility_rows = facilities or []
    ahrq_ids = _identity_values("ahrq_system_id", system_id, query.get("system_id"), *(system_ids or []))
    ccns = _identity_values("ccn", *(row.get("ccn") for row in facility_rows), query.get("ccn"))
    npis = _identity_values("npi", *(row.get("npi") for row in facility_rows), query.get("npi"))
    names = _identity_values(
        "canonical_name",
        system_name,
        query.get("system_name"),
        query.get("system_slug"),
        query.get("query"),
        *(system_names or []),
        *(row.get("name") or row.get("facility_name") for row in facility_rows),
    )
    zip_codes = _identity_values("zip_code", *(row.get("zip_code") or row.get("zip") for row in facility_rows))
    source_claims = _system_source_claims(result_collection=result_collection, facilities=facility_rows)
    return {
        "entity_scope": "health_system_facility_identity",
        "join_keys": [
            {
                "field": "ahrq_system_id",
                "values": ahrq_ids,
                "status": "provided" if ahrq_ids else "missing",
                "used_by": _system_join_key_usage("ahrq_system_id", source_claims),
            },
            {
                "field": "ccn",
                "values": ccns,
                "status": "provided" if ccns else "missing",
                "used_by": _system_join_key_usage("ccn", source_claims),
            },
            {
                "field": "npi",
                "values": npis,
                "status": "provided" if npis else "missing",
                "used_by": _system_join_key_usage("npi", source_claims),
            },
            {
                "field": "canonical_name",
                "values": names,
                "status": "provided" if names else "missing",
                "used_by": _system_join_key_usage("canonical_name", source_claims),
            },
            {
                "field": "zip_code",
                "values": zip_codes,
                "status": "provided" if zip_codes else "missing",
                "used_by": _system_join_key_usage("zip_code", source_claims),
            },
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Use AHRQ system IDs for system-level joins and CCN/NPI for facility-level joins.",
            "Treat system names, facility names, and web or marketing aliases as candidate aliases unless exact identifiers agree.",
            "Keep AHRQ linkage, CMS POS/HGI enrichment, NPPES outpatient context, and reviewed merger ledgers source-scoped.",
            "Carry missing or conflicting CCNs forward instead of replacing them with name-only matches.",
        ],
        "missing_data_policy": (
            "No-match system-profiler responses identify the searched AHRQ/CMS/NPPES source scope; "
            "they are not proof that a system, facility, affiliation, or outpatient site does not exist."
        ),
    }


def _identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        normalized = _normalize_identity_value(field, value)
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _normalize_identity_value(field: str, value: Any) -> str:
    if value in ("", None):
        return ""
    if field == "ccn":
        return normalize_ccn(value) or ""
    if field == "npi":
        return normalize_npi(value) or ""
    if field == "canonical_name":
        return normalize_name(value, remove_legal_suffixes=True)
    if field == "zip_code":
        return str(value).strip()[:5]
    return str(value).strip()


def _system_source_claims(*, result_collection: str, facilities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    system_claim = {
        "collection": "system",
        "identity_paths": ["evidence.query"],
        "evidence_path": "evidence",
        "source_metadata_path": "source_metadata",
        "match_policy": "ahrq_system_id_required_for_system_merge",
    }
    if result_collection == "results":
        system_claim["row_evidence_paths"] = ["results[].evidence"]
    claims = [system_claim]
    if facilities:
        row_evidence_paths = _system_facility_row_evidence_paths(result_collection)
        claims.append(
            {
                "collection": "facilities",
                "identity_paths": ["evidence.query"],
                "evidence_path": "evidence",
                "source_metadata_path": "source_metadata",
                "row_evidence_paths": row_evidence_paths,
                "match_policy": "ccn_or_npi_required_for_facility_merge",
            }
        )
    return claims


def _system_facility_row_evidence_paths(result_collection: str) -> list[str]:
    if result_collection == "inpatient_facilities":
        return ["inpatient_facilities[].evidence", "sub_entities[].evidence"]
    if result_collection == "facilities":
        return ["facilities[].evidence", "merger_evidence[].evidence"]
    if result_collection == "facility_reconciliation":
        return [
            "inpatient_facilities[].evidence",
            "outpatient_sites[].evidence",
            "sub_entities[].evidence",
            "facility_reconciliation.facilities[].evidence",
            "facility_reconciliation.merger_evidence[].evidence",
        ]
    return ["facilities[].evidence"]


def _system_join_key_usage(field: str, source_claims: list[dict[str, Any]]) -> list[str]:
    collections_by_field = {
        "ahrq_system_id": {"system"},
        "ccn": {"facilities"},
        "npi": {"facilities"},
        "canonical_name": {"system", "facilities"},
        "zip_code": {"facilities"},
    }.get(field, set())
    path_tokens = {
        "ahrq_system_id": ("system_id", "ahrq_system_id"),
        "ccn": ("ccn",),
        "npi": ("npi",),
        "canonical_name": ("name",),
        "zip_code": ("zip_code",),
    }[field]
    used_by = []
    for claim in source_claims:
        collection = str(claim.get("collection") or "")
        if collection in collections_by_field:
            used_by.append(collection)
            continue
        paths = " ".join(str(path) for path in claim.get("identity_paths", []))
        if any(token in paths for token in path_tokens):
            used_by.append(collection)
    return sorted(item for item in used_by if item)


# ---- Internal loader wrappers (mockable for tests) ----

async def _load_ahrq_systems() -> pd.DataFrame:
    return await load_ahrq_systems()

async def _load_ahrq_hospitals() -> pd.DataFrame:
    return await load_ahrq_hospital_linkage()

async def _load_pos() -> pd.DataFrame:
    return await load_pos()


def _legacy_int_or_zero(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str) and value.strip() == "":
        return 0
    try:
        if pd.isna(value):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return 0


def _ahrq_cache_required_response(*, tool_name: str, query: dict[str, Any] | None = None) -> dict[str, Any] | None:
    required = [AHRQ_SYSTEM_CACHE, AHRQ_HOSPITAL_LINKAGE_CACHE]
    missing = [path for path in required if not path.exists()]
    if not missing:
        return None
    return to_structured(
        {
            "ok": False,
            "error_code": "AHRQ_CACHE_REQUIRED",
            "status": "blocked_missing_required_cache",
            "message": "AHRQ 2023 Compendium cache is required before national metrics can be served.",
            "tool": tool_name,
            "query": query or {},
            "required_files": [path.name for path in required],
            "missing_files": [path.name for path in missing],
            "required_paths": [str(path) for path in required],
            "recovery_steps": [
                "Run hc-mcp doctor --check",
                "Run python scripts/download_ahrq.py or the documented AHRQ cache setup command",
                "Use the Excel download if CSV handling drops leading zeros",
                "Retry list_health_system_metrics after cache setup",
            ],
            "source": "AHRQ Compendium 2023",
            "source_metadata": [
                {
                    "source_name": "AHRQ Compendium of U.S. Health Systems, 2023",
                    "dataset_id": "ahrq_health_system_compendium",
                    "source_period": "2023 Compendium; September 2025 revised system file when available",
                    "landing_page": "https://www.ahrq.gov/chsp/data-resources/compendium-2023.html",
                    "cache_status": "missing_required_cache",
                    "caveat": "National health-system metrics require local AHRQ system and hospital-linkage files.",
                }
            ],
            "next_actions": [
                "Populate the two required AHRQ cache files without changing CCN or ZIP strings.",
                "Retry the same metrics tool after cache setup.",
            ],
        }
    )


async def _load_required_ahrq_metric_frames(*, tool_name: str, query: dict[str, Any] | None = None) -> tuple[pd.DataFrame, pd.DataFrame] | dict[str, Any]:
    missing_response = _ahrq_cache_required_response(tool_name=tool_name, query=query)
    if missing_response is not None:
        return missing_response
    try:
        return await _load_ahrq_systems(), await _load_ahrq_hospitals()
    except Exception as exc:
        logger.warning("AHRQ metrics cache load failed for %s", tool_name, exc_info=True)
        response = _ahrq_cache_required_response(tool_name=tool_name, query=query)
        if response is not None:
            return response
        return to_structured(
            {
                "ok": False,
                "error_code": "AHRQ_CACHE_REQUIRED",
                "status": "blocked_required_cache_unavailable",
                "message": "AHRQ 2023 Compendium cache could not be loaded.",
                "tool": tool_name,
                "query": query or {},
                "required_files": [AHRQ_SYSTEM_CACHE.name, AHRQ_HOSPITAL_LINKAGE_CACHE.name],
                "required_paths": [str(AHRQ_SYSTEM_CACHE), str(AHRQ_HOSPITAL_LINKAGE_CACHE)],
                "recovery_steps": [
                    "Run hc-mcp doctor --check",
                    "Rebuild the AHRQ Compendium cache using the documented setup workflow",
                    "Verify CSV files preserve leading-zero CCNs and ZIP codes",
                    "Retry the metrics tool after cache setup",
                ],
                "source": "AHRQ Compendium 2023",
                "detail": {"exception_type": type(exc).__name__},
            }
        )


async def _load_hospital_general_info_overlay() -> pd.DataFrame:
    try:
        from shared.utils.cms_client import load_hospital_general_info

        return await load_hospital_general_info(normalize_columns=True)
    except Exception:
        logger.warning("CMS Hospital General Information overlay load failed", exc_info=True)
        return pd.DataFrame()


def _load_medicare_public_clinicians_overlay() -> pd.DataFrame:
    """Load optional Doctors and Clinicians cache without forcing a huge download."""

    candidates = [
        _os.environ.get("HC_MCP_DOCTORS_CLINICIANS_CSV", ""),
        str(Path.home() / ".healthcare-data-mcp" / "cache" / "doctors_clinicians_national_downloadable_file.csv"),
        str(Path.home() / ".healthcare-data-mcp" / "cache" / "DAC_NationalDownloadableFile.csv"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, encoding_errors="replace", low_memory=False)
        except Exception:
            logger.warning("CMS Doctors and Clinicians overlay cache read failed for %s", path, exc_info=True)
    return pd.DataFrame()

async def _search_nppes(**kwargs) -> list[dict]:
    return await search_nppes(**kwargs)

def _load_provider_enrollment() -> pd.DataFrame:
    try:
        from servers.provider_enrollment.data_loaders import ENROLLMENT_DATASET_KEYS, load_cached_frames
    except Exception:
        return pd.DataFrame()
    return load_cached_frames(ENROLLMENT_DATASET_KEYS)


def _load_profile_provider_rows(ccns: list[str]) -> list[dict[str, Any]]:
    """Return bounded provider-enrollment/ownership/CHOW rows for pack review."""

    try:
        from servers.provider_enrollment import data_loaders as provider_loaders
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for ccn in ccns[:50]:
        try:
            rows.extend(provider_loaders.query_ownership(ccn=ccn, limit=25))
            rows.extend(provider_loaders.query_chow(ccn=ccn, limit=25))
        except ImportError as exc:
            logger.warning(
                "Skipping optional provider enrollment profile rows; parquet support is unavailable: %s",
                exc,
            )
            break
        except Exception:
            logger.warning("Provider enrollment profile rows failed for CCN %s", ccn, exc_info=True)
    return rows[:250]


async def _load_hcris_bed_rows(state: str, ccns: list[str]) -> list[dict[str, Any]]:  # noqa: ARG001
    """Return CMS HCRIS/cost-report rows for exact CCNs from the configured cache."""

    try:
        from servers.hospital_quality import data_loaders as hospital_quality_loaders
        from shared.utils.cost_report import load_cost_report_row
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for ccn in ccns[:100]:
        try:
            row, error = await load_cost_report_row(hospital_quality_loaders, ccn)
        except Exception:
            logger.warning("HCRIS/cost-report bed row lookup failed for CCN %s", ccn, exc_info=True)
            continue
        if error or row is None:
            continue
        payload = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        payload.setdefault("ccn", ccn)
        payload.setdefault("source", "CMS Hospital Cost Report Public Use File")
        payload.setdefault("dataset_id", "cms_cost_report")
        payload.setdefault("cache_status", "configured_hcris_cache")
        rows.append(payload)
    return rows[:250]


def _load_state_bed_rows(state: str, ccns: list[str]) -> list[dict[str, Any]]:
    """Return public state bed rows from configured state-health-data caches."""

    if state.upper() != "PA":
        return []
    try:
        from shared import state_health_data
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    for ccn in ccns[:100]:
        try:
            candidates = state_health_data.load_pa_doh_bed_candidates(ccn=ccn)
        except Exception:
            logger.warning("PA DOH bed row lookup failed for CCN %s", ccn, exc_info=True)
            continue
        for row in candidates:
            payload = dict(row)
            payload.setdefault("ccn", ccn)
            payload.setdefault("state", "PA")
            payload.setdefault("source", "Pennsylvania Department of Health Hospital Reports")
            payload.setdefault("dataset_id", "pa_hospital_reports")
            payload.setdefault("cache_status", "configured_state_health_data_cache")
            rows.append(payload)
    return rows[:250]


def _load_official_profile_evidence(system_name: str, state: str) -> list[dict[str, Any]]:
    """Load reviewed official page/report evidence rows from an optional local cache."""

    root = Path(_os.environ.get("HC_MCP_CACHE_ROOT") or (Path.home() / ".healthcare-data-mcp" / "cache")).expanduser()
    evidence_dir = root / "profile-evidence"
    paths = (
        evidence_dir / "official_profile_evidence.json",
        evidence_dir / "official_profile_evidence.jsonl",
        evidence_dir / "official_profile_evidence.csv",
    )
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            rows.extend(_read_official_profile_evidence_path(path))
        except Exception:
            logger.warning("Official profile evidence cache read failed for %s", path, exc_info=True)
    wanted = normalize_name(system_name, remove_legal_suffixes=True)
    state_norm = state.upper()
    matched: list[dict[str, Any]] = []
    for row in rows:
        row_state = str(row.get("state") or row.get("system_state") or "").upper()
        if row_state and state_norm and row_state != state_norm:
            continue
        row_name = normalize_name(
            row.get("system_name") or row.get("health_system_name") or row.get("name") or "",
            remove_legal_suffixes=True,
        )
        if wanted and row_name and wanted not in row_name and row_name not in wanted:
            continue
        payload = dict(row)
        payload.setdefault("source_name", "Reviewed official health-system page/report")
        payload.setdefault("dataset_id", "official_system_page")
        payload.setdefault("cache_status", "reviewed_local_cache")
        payload.setdefault("cache_freshness", f"Reviewed cache file under {evidence_dir}")
        matched.append(payload)
    return matched[:250]


def _read_official_profile_evidence_path(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = payload.get("rows") or payload.get("evidence") or []
        return [dict(row) for row in payload if isinstance(row, dict)]
    if path.suffix == ".jsonl":
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return [dict(row) for row in rows if isinstance(row, dict)]
    if path.suffix == ".csv":
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        return frame.to_dict(orient="records")
    return []


async def _census_geocode_address(address: str) -> dict[str, Any] | None:
    return await census_geocode_address(address)


async def _osm_geocode_address(address: str) -> dict[str, Any] | None:
    return await osm_geocode_address(address)


async def _reverse_geocode_coordinates(latitude: float, longitude: float) -> dict[str, Any] | None:
    return await reverse_geocode_coordinates(latitude, longitude)


# ---- MCP Tools ----

@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def search_health_systems(query: str, limit: int = 10) -> dict[str, Any]:
    """Search for health systems by name using AHRQ Compendium.

    Performs fuzzy matching against ~700 US health system names.

    Args:
        query: System name to search for (e.g. "Jefferson Health", "LVHN", "Penn Medicine").
        limit: Maximum results to return (default 10).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_health_systems","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    systems_df = await _load_ahrq_systems()
    results = fuzzy_search_systems(query, systems_df, limit=limit)
    system_ids = [item.get("system_id") or item.get("health_sys_id") for item in results if isinstance(item, dict)]
    system_names = [item.get("name") or item.get("system_name") or item.get("health_sys_name") for item in results if isinstance(item, dict)]
    query_payload = {"query": query, "limit": limit}
    payload = {
        "count": len(results),
        "results": results,
        "evidence": _system_evidence(
            query=query_payload,
            match_basis="fuzzy_system_name_search",
            confidence="candidate_system_matches",
        ),
        "identity": identity_from_public_record(
            name=query,
            entity_type="health_system",
            source_name="workflow_or_tool_input",
        ).to_dict(),
        "identity_map": _system_identity_map(
            query=query_payload,
            system_ids=system_ids,
            system_names=system_names,
            result_collection="results",
        ),
    }
    return to_structured(
        _attach_system_row_evidence(payload, query=query_payload)
    )


@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def list_health_system_metrics(
    cursor: str | None = None,
    page_size: int = 50,
    sort: str = "health_sys_id",
    state: str | None = None,
    state_scope: str = "headquarters",
    as_of_mode: str = "compendium_snapshot",
    include_facilities: bool = False,
    include_medicare_public_clinician_roster_estimate: bool = False,
) -> dict[str, Any]:
    """List AHRQ Compendium 2023 health-system metrics with source-vintage discipline.

    Discovery
    ---------
    - Use this tool for cursor-paged national health-system metric inventory.
    - Use get_health_system_metrics for one exact AHRQ health_sys_id.
    - Default data_mode is compendium_snapshot, which does not silently prefer current CMS overlays.

    When to use
    -----------
    - Use when an agent needs hospital counts, system beds, physician counts, or facility rows across the AHRQ 2023 universe.
    - Use latest_public_overlay only when current CMS HGI/POS candidates are acceptable as later public overlays.
    - NOT for: an unqualified live list of every legal health-system entity in the United States.

    Parameters
    ----------
    cursor : str | None
        Cursor returned by the previous response. Encodes snapshot_id, filters, and offset.
    page_size : int
        Number of systems to return. Capped at 100.
    sort : str
        One of health_sys_id, health_sys_name, state, hospital_count, or bed_count.
    state : str | None
        Optional two-letter state filter.
    state_scope : str
        headquarters or facility_presence. Facility presence is based on AHRQ hospital linkage rows.
    as_of_mode : str
        compendium_snapshot or latest_public_overlay.
    include_facilities : bool
        Include hospital rows with address/type/bed candidates.
    include_medicare_public_clinician_roster_estimate : bool
        Include an experimental Doctors and Clinicians NPI-deduped estimate when a local cache is configured.

    Returns
    -------
    dict
        Paged systems, universe metadata, coverage, evidence, source_metadata, identity_map, and next_actions.

    Do / Don't
    ----------
    Do:
    - Preserve universe, snapshot_id, data_mode, evidence, and source_metadata with cited facts.
    - Keep compendium_snapshot values separate from latest_public_overlay candidates.

    Don't:
    - Treat state filters as all systems operating in a market; AHRQ aggregates at highest ownership level.
    - Treat CCN as campus-level identity; use compendium_hospital_id for AHRQ linkage rows.

    Examples
    --------
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"list_health_system_metrics","arguments":{"page_size":25,"state":"PA","include_facilities":true}}}

    Common mistakes
    ---------------
    - Calling this "every health system in America": say "AHRQ Compendium 2023 health-system universe."
    - Mixing current CMS address/type values into 2023 snapshot metrics without data_mode and candidate caveats.
    """

    query = {
        "cursor": cursor,
        "page_size": page_size,
        "sort": sort,
        "state": state,
        "state_scope": state_scope,
        "as_of_mode": as_of_mode,
        "include_facilities": include_facilities,
        "include_medicare_public_clinician_roster_estimate": include_medicare_public_clinician_roster_estimate,
    }
    frames = await _load_required_ahrq_metric_frames(tool_name="list_health_system_metrics", query=query)
    if isinstance(frames, dict):
        return frames
    systems_df, hospitals_df = frames
    hgi_df = await _load_hospital_general_info_overlay() if as_of_mode == "latest_public_overlay" else pd.DataFrame()
    pos_df = await _load_pos() if as_of_mode == "latest_public_overlay" else pd.DataFrame()
    clinicians_df = (
        _load_medicare_public_clinicians_overlay()
        if include_medicare_public_clinician_roster_estimate
        else pd.DataFrame()
    )
    return to_structured(
        list_health_system_metric_rows(
            systems_df=systems_df,
            hospitals_df=hospitals_df,
            cursor=cursor,
            page_size=page_size,
            sort=sort,
            state=state,
            state_scope=state_scope,
            as_of_mode=as_of_mode,
            include_facilities=include_facilities,
            include_medicare_public_clinician_roster_estimate=include_medicare_public_clinician_roster_estimate,
            hgi_df=hgi_df,
            pos_df=pos_df,
            clinicians_df=clinicians_df,
        )
    )


@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def get_health_system_metrics(
    system_id: str | None = None,
    system_name: str | None = None,
    as_of_mode: str = "compendium_snapshot",
    include_facilities: bool = True,
    include_medicare_public_clinician_roster_estimate: bool = False,
) -> dict[str, Any]:
    """Get source-disciplined metrics for one AHRQ Compendium 2023 health system.

    Discovery
    ---------
    - Prefer exact system_id from search_health_systems or list_health_system_metrics.
    - Use system_name only for discovery; low-confidence fuzzy matches return candidate rows.

    When to use
    -----------
    - Use for one source-backed health-system metric package with facility rows.
    - Use latest_public_overlay only for dated CMS HGI/POS candidates alongside AHRQ snapshot values.
    - NOT for: current legal ownership, PHI, or proprietary roster counts.

    Parameters
    ----------
    system_id : str | None
        Exact AHRQ health_sys_id. Preferred.
    system_name : str | None
        Name to resolve when system_id is unavailable.
    as_of_mode : str
        compendium_snapshot or latest_public_overlay.
    include_facilities : bool
        Include hospital-level address/type/bed candidates.
    include_medicare_public_clinician_roster_estimate : bool
        Include an experimental local-cache-only Doctors and Clinicians estimate.

    Returns
    -------
    dict
        System metrics, universe metadata, coverage, evidence, source_metadata, identity_map, and next_actions.

    Do / Don't
    ----------
    Do:
    - Cite AHRQ total_mds as "AHRQ Compendium physician count" with caveats.
    - Cite hospital_bed_count primary values as AHRQ hospital linkage hos_beds in compendium_snapshot mode.

    Don't:
    - Silently replace AHRQ 2023 metrics with current CMS overlays.
    - Use CCN as a campus-level unique hospital identity.

    Examples
    --------
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_health_system_metrics","arguments":{"system_id":"HSI00000008"}}}

    Common mistakes
    ---------------
    - Passing a vague system_name and assuming the top fuzzy match is exact.
    - Treating medicare_public_clinician_roster_estimate as a complete system physician roster.
    """

    if not system_id and not system_name:
        return to_structured(
            invalid_argument_payload(
                "system_id",
                system_id,
                message="Provide system_id or system_name.",
            )
        )

    query = {
        "system_id": system_id,
        "system_name": system_name,
        "as_of_mode": as_of_mode,
        "include_facilities": include_facilities,
        "include_medicare_public_clinician_roster_estimate": include_medicare_public_clinician_roster_estimate,
    }
    frames = await _load_required_ahrq_metric_frames(tool_name="get_health_system_metrics", query=query)
    if isinstance(frames, dict):
        return frames
    systems_df, hospitals_df = frames
    hgi_df = await _load_hospital_general_info_overlay() if as_of_mode == "latest_public_overlay" else pd.DataFrame()
    pos_df = await _load_pos() if as_of_mode == "latest_public_overlay" else pd.DataFrame()
    clinicians_df = (
        _load_medicare_public_clinicians_overlay()
        if include_medicare_public_clinician_roster_estimate
        else pd.DataFrame()
    )
    return to_structured(
        assemble_health_system_metric(
            systems_df=systems_df,
            hospitals_df=hospitals_df,
            system_id=system_id,
            system_name=system_name,
            as_of_mode=as_of_mode,
            include_facilities=include_facilities,
            include_medicare_public_clinician_roster_estimate=include_medicare_public_clinician_roster_estimate,
            hgi_df=hgi_df,
            pos_df=pos_df,
            clinicians_df=clinicians_df,
        )
    )


@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def get_system_profile(
    system_id: str | None = None,
    system_name: str | None = None,
    edition_date: str | None = None,
    include_outpatient: bool = True,
) -> dict[str, Any]:
    """Get a complete health system profile in one call.

    Combines AHRQ Compendium (system to hospitals), CMS POS (beds, services,
    staffing), NPPES (outpatient sites), and related provider graph expansion.

    Provide either system_id (from search_health_systems) or system_name
    (auto-resolved via fuzzy search, takes the top match).

    Args:
        system_id: AHRQ system ID (e.g. "SYS_001"). Preferred.
        system_name: System name for auto-resolution (e.g. "Jefferson Health").
        edition_date: Profile edition/as-of date. Jefferson Health uses this to apply the
            post-2024 LVHN combined-system resolver.
        include_outpatient: Include NPPES outpatient site discovery (default True).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_system_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if not system_id and system_name and resolve_combined_system_slug(system_name=system_name) == JEFFERSON_SLUG:
        profile = build_combined_system_profile(system_name, edition_date=edition_date)
        if profile is not None:
            payload = to_structured(profile)
            if isinstance(payload, dict):
                payload["evidence"] = _system_evidence(
                    query={"system_name": system_name, "edition_date": edition_date or ""},
                    match_basis="reviewed_jefferson_combined_system_rule",
                    confidence="reviewed_reconciliation_rule",
                )
                payload["identity"] = identity_from_public_record(
                    name=system_name,
                    entity_type="health_system",
                    ahrq_system_id=str(payload.get("system", {}).get("system_id") or ""),
                    source_name="health-system-profiler reconciliation",
                ).to_dict()
                payload["identity_map"] = _system_identity_map(
                    query={"system_name": system_name, "edition_date": edition_date or ""},
                    system_id=str(payload.get("system", {}).get("system_id") or payload.get("system_slug") or ""),
                    system_name=system_name,
                    facilities=list(payload.get("facility_reconciliation", {}).get("facilities", [])),
                    result_collection="facility_reconciliation",
                )
                _attach_system_row_evidence(
                    payload,
                    query={"system_name": system_name, "edition_date": edition_date or ""},
                )
            return to_structured(payload)

    systems_df = await _load_ahrq_systems()
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    # Resolve system_id if only name provided
    if not system_id and system_name:
        matches = fuzzy_search_systems(system_name, systems_df, limit=1)
        if not matches:
            return _system_error_response(
                f"No health system found matching '{system_name}'",
                query={"system_name": system_name, "edition_date": edition_date or ""},
                match_basis="system_name_search_no_ahrq_match",
                confidence="no_candidate_match_in_loaded_ahrq_compendium",
                next_step=(
                    "Review spelling, aliases, recent merger names, or use an exact AHRQ system_id when available. "
                    "Do not infer system affiliation from a failed name search."
                ),
            )
        system_id = matches[0]["system_id"]

    if not system_id:
        return _system_error_response(
            "Provide either system_id or system_name",
            code="invalid_params",
            query={"system_id": system_id or "", "system_name": system_name or ""},
            match_basis="missing_required_identifier",
            confidence="not_evaluated",
            next_step="Provide an exact AHRQ system_id or a health system name to search.",
        )

    # Get system info
    sys_row = systems_df[systems_df["health_sys_id"] == system_id]
    if sys_row.empty:
        return _system_error_response(
            f"System ID '{system_id}' not found in AHRQ Compendium",
            query={"system_id": system_id, "system_name": system_name or ""},
            match_basis="ahrq_system_id_no_match",
            confidence="no_match_in_loaded_ahrq_compendium",
            next_step="Verify the AHRQ system_id and source edition before using this identifier in a report.",
        )

    sys_info = sys_row.iloc[0]
    sys_name = str(sys_info.get("health_sys_name", ""))
    sys_city = str(sys_info.get("health_sys_city", ""))
    sys_state = str(sys_info.get("health_sys_state", ""))

    # Resolve CCNs
    ccns = resolve_system_ccns(system_id, hospitals_df)

    # Enrich each facility from POS
    facilities: list[FacilitySummary] = []
    total_beds = 0
    for ccn in ccns:
        facility = enrich_facility(ccn, pos_df)
        if facility:
            total_beds += facility.beds.total
            facilities.append(facility)
        else:
            # Fallback: use AHRQ data if POS has no match
            ahrq_row = hospitals_df[hospitals_df["ccn"] == ccn]
            if not ahrq_row.empty:
                r = ahrq_row.iloc[0]
                beds = _legacy_int_or_zero(r.get("hos_beds"))
                total_beds += beds
                facilities.append(FacilitySummary(
                    ccn=ccn,
                    name=str(r.get("hospital_name", "")),
                    city=str(r.get("hosp_city", "")),
                    state=str(r.get("hosp_state", "")),
                    zip_code=str(r.get("hosp_zip", "")),
                    beds=BedBreakdown(total=beds),
                ))

    # Graph expansion — find sub-entities
    sub_entities = expand_related_providers(ccns, pos_df)

    # Aggregate off-site counts
    off_site = aggregate_off_site(ccns, pos_df)

    # NPPES outpatient discovery
    outpatient_sites = []
    if include_outpatient and sys_state:
        patterns = build_search_patterns(sys_name, sys_state)
        for params in patterns:
            try:
                raw = await _search_nppes(**params)
                outpatient_sites.extend(parse_nppes_results(raw))
            except Exception as e:
                logger.warning("NPPES search failed for %s: %s", params, e)

        # Deduplicate by NPI
        seen_npis: set[str] = set()
        unique_sites = []
        for site in outpatient_sites:
            if site.npi not in seen_npis:
                seen_npis.add(site.npi)
                unique_sites.append(site)
        outpatient_sites = unique_sites

    # Compute total discharges from AHRQ linkage
    sys_hospitals = hospitals_df[hospitals_df["health_sys_id"] == system_id]
    total_dsch = sum(_legacy_int_or_zero(value) for value in sys_hospitals["hos_dsch"]) if "hos_dsch" in sys_hospitals.columns else 0

    # Build response
    profile = SystemProfileResponse(
        system=HealthSystemSummary(
            system_id=system_id,
            name=sys_name,
            hq_city=sys_city,
            hq_state=sys_state,
            hospital_count=len(ccns),
            total_beds=total_beds,
            total_discharges=total_dsch,
            physician_group_count=_legacy_int_or_zero(sys_info.get("phys_grp_count")),
        ),
        inpatient_facilities=[f.model_dump() for f in facilities],
        sub_entities=[s.model_dump() for s in sub_entities],
        outpatient_sites=[o.model_dump() for o in outpatient_sites],
        off_site_summary=off_site.model_dump(),
    )
    payload = profile.model_dump()
    payload["facility_reconciliation"] = reconcile_generic_system_facilities(
        system_id,
        as_of_date=edition_date,
        systems_df=systems_df,
        ahrq_hospitals=hospitals_df,
        cms_hgi=pos_df,
        resolved_system=sys_info.to_dict(),
    )
    payload["evidence"] = _system_evidence(
        query={"system_id": system_id, "system_name": system_name or "", "edition_date": edition_date or ""},
        match_basis="ahrq_system_id_exact" if system_id else "system_name_resolved_to_ahrq_id",
        confidence="high_for_ahrq_linkage_with_reconciliation",
    )
    payload["identity"] = identity_from_public_record(
        name=sys_name,
        entity_type="health_system",
        ahrq_system_id=system_id,
        source_name="AHRQ Compendium",
        source_url="https://www.ahrq.gov/chsp/data-resources/compendium.html",
    ).to_dict()
    payload["identity_map"] = _system_identity_map(
        query={"system_id": system_id, "system_name": system_name or "", "edition_date": edition_date or ""},
        system_id=system_id,
        system_name=sys_name,
        facilities=list(payload.get("facility_reconciliation", {}).get("facilities", []))
        or list(payload.get("inpatient_facilities", [])),
        result_collection="facility_reconciliation",
    )
    _attach_system_row_evidence(
        payload,
        query={"system_id": system_id, "system_name": system_name or "", "edition_date": edition_date or ""},
    )
    return to_structured(payload)


@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def reconcile_system_facilities(
    system_slug: str,
    as_of_date: str | None = None,
) -> dict[str, Any]:
    """Return a facility reconciliation ledger for a health system.

    Jefferson Health is reconciled as a combined post-merger system by merging the
    legacy Jefferson, Einstein, and LVHN rosters rather than relying on AHRQ 2023 alone.
    Other systems use generic AHRQ Compendium linkage with CMS POS enrichment.

    Args:
        system_slug: Jefferson alias, AHRQ system ID, normalized system name, or slug.
        as_of_date: Ledger as-of date. Jefferson/LVHN is valid on or after 2024-08-01.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"reconcile_system_facilities","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if resolve_combined_system_slug(system_name=system_slug, system_slug=system_slug) == JEFFERSON_SLUG:
        result = reconcile_jefferson_facilities(JEFFERSON_SLUG, as_of_date=as_of_date)
        if "error" in result:
            return _system_error_response(
                result["error"],
                query={"system_slug": system_slug, "as_of_date": as_of_date or ""},
                match_basis="reviewed_jefferson_reconciliation_not_applicable",
                confidence="not_reconciled",
                next_step="Review the requested as_of_date and Jefferson/LVHN merger effective-date caveat.",
            )
        result["evidence"] = _system_evidence(
            query={"system_slug": system_slug, "as_of_date": as_of_date or ""},
            match_basis="reviewed_jefferson_combined_system_rule",
            confidence="reviewed_reconciliation_rule",
        )
        result["identity"] = identity_from_public_record(
            name=result.get("system_name") or system_slug,
            entity_type="health_system",
            ahrq_system_id=result.get("system_id") or "",
            source_name="health-system-profiler reconciliation",
        ).to_dict()
        result["identity_map"] = _system_identity_map(
            query={"system_slug": system_slug, "as_of_date": as_of_date or ""},
            system_id=str(result.get("system_id") or result.get("system_slug") or ""),
            system_name=str(result.get("system_name") or system_slug),
            facilities=list(result.get("facilities", [])),
            result_collection="facilities",
        )
        _attach_system_row_evidence(result, query={"system_slug": system_slug, "as_of_date": as_of_date or ""})
        return to_structured(result)

    systems_df = await _load_ahrq_systems()
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()
    provider_enrollment_df = _load_provider_enrollment()
    result = reconcile_generic_system_facilities(
        system_slug,
        as_of_date=as_of_date,
        systems_df=systems_df,
        ahrq_hospitals=hospitals_df,
        cms_hgi=pos_df,
        provider_enrollment=provider_enrollment_df,
    )
    if "error" in result:
        return _system_error_response(
            result["error"],
            query={"system_slug": system_slug, "as_of_date": as_of_date or ""},
            match_basis="system_slug_reconciliation_no_match",
            confidence="not_reconciled",
            next_step="Resolve the health system with search_health_systems or provide an exact AHRQ system_id.",
        )
    result["evidence"] = _system_evidence(
        query={"system_slug": system_slug, "as_of_date": as_of_date or ""},
        match_basis="system_slug_reconciliation",
        confidence="reconciliation_rule_confidence",
    )
    result["identity"] = identity_from_public_record(
        name=result.get("system_name") or system_slug,
        entity_type="health_system",
        ahrq_system_id=result.get("system_id") or "",
        source_name="health-system-profiler reconciliation",
    ).to_dict()
    result["identity_map"] = _system_identity_map(
        query={"system_slug": system_slug, "as_of_date": as_of_date or ""},
        system_id=str(result.get("system_id") or result.get("system_slug") or ""),
        system_name=str(result.get("system_name") or system_slug),
        facilities=list(result.get("facilities", [])),
        result_collection="facilities",
    )
    _attach_system_row_evidence(result, query={"system_slug": system_slug, "as_of_date": as_of_date or ""})
    return to_structured(result)


@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def get_system_facilities(
    system_id: str,
    facility_type: str = "all",
) -> dict[str, Any]:
    """Get detailed facility data for a health system with full POS enrichment.

    Args:
        system_id: AHRQ system ID (from search_health_systems).
        facility_type: Filter: "inpatient", "outpatient", "rehab", "behavioral_health", "all" (default).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_system_facilities","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()

    ccns = resolve_system_ccns(system_id, hospitals_df)
    if not ccns:
        return _system_error_response(
            f"No hospitals found for system ID '{system_id}'",
            query={"system_id": system_id, "facility_type": facility_type},
            match_basis="ahrq_system_id_no_linked_hospitals",
            confidence="no_hospital_linkage_in_loaded_ahrq_compendium",
            next_step="Verify the AHRQ system_id and edition before reporting that a system has no hospitals.",
        )

    facilities = []
    for ccn in ccns:
        facility = enrich_facility(ccn, pos_df)
        if facility:
            facilities.append(facility)

    # Include sub-entities if not filtered to inpatient-only
    sub_entities = []
    if facility_type in ("all", "rehab", "behavioral_health"):
        sub_entities = expand_related_providers(ccns, pos_df)

    result = {
        "system_id": system_id,
        "facility_count": len(facilities) + len(sub_entities),
        "inpatient_facilities": [f.model_dump() for f in facilities],
        "evidence": _system_evidence(
            query={"system_id": system_id, "facility_type": facility_type},
            match_basis="ahrq_system_id_exact",
            confidence="high_for_ahrq_linkage_with_pos_enrichment",
        ),
        "identity": identity_from_public_record(
            entity_type="health_system",
            ahrq_system_id=system_id,
            source_name="AHRQ Compendium",
            source_url="https://www.ahrq.gov/chsp/data-resources/compendium.html",
        ).to_dict(),
        "identity_map": _system_identity_map(
            query={"system_id": system_id, "facility_type": facility_type},
            system_id=system_id,
            facilities=[f.model_dump() for f in facilities],
            result_collection="inpatient_facilities",
        ),
    }
    if sub_entities:
        result["sub_entities"] = [s.model_dump() for s in sub_entities]

    _attach_system_row_evidence(result, query={"system_id": system_id, "facility_type": facility_type})
    return to_structured(result)


@mcp.tool(structured_output=True)
@observe_tool("health-system-profiler")
async def build_profile_evidence_pack(
    state: str,
    system_slug: str | None = None,
    system_name: str | None = None,
    ccns: list[str] | None = None,
    required_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Build a read-only source-backed health-system profile evidence pack.

    The pack is designed for Healthcare Toolkit profile population. It returns
    structured public-data candidates, source conflicts, and unavailable-public
    findings for profile_sources, profile_metric_values, and
    profile_knowledge_objects. It does not write to Healthcare Toolkit.

    Discovery
    ---------
    - Use state plus one or more of system_slug, system_name, or exact ccns.
    - Run cache-manager.get_workflow_cache_readiness with workflow_id profile_evidence_pack before operational use.
    - Use search_health_systems when only a partial system name is available.

    When to use
    -----------
    - Use when Healthcare Toolkit needs source-backed evidence to populate a health-system profile.
    - Use when bed counts, facility rosters, official count claims, geography, or current-affiliation evidence must be reviewable.
    - NOT for: writing to Healthcare Toolkit, estimating missing values, PHI, or final legal ownership determinations.

    Parameters
    ----------
    state : str
        Two-letter state abbreviation such as "PA". Required.
    system_slug : str | None
        Optional Healthcare Toolkit or reviewed source slug, for example "jefferson-health".
    system_name : str | None
        Optional public system name, for example "Jefferson Health".
    ccns : list[str] | None
        Optional exact CMS Certification Numbers to anchor facility rows.
    required_fields : list[str] | None
        Optional fields that must be present or returned as unavailable_public/needs_review findings.

    Returns
    -------
    dict
        Evidence pack with source-backed candidates, conflicts, unavailable_public findings, cache_preflight,
        source_precedence, identity, identity_map, evidence, and source_metadata.

    Do / Don't
    ----------
    Do:
    - Persist only supported source-backed values with their evidence and source_metadata.
    - Route source_conflict, needs_review, and unavailable_public rows to manual review.
    - Preserve source precedence: CMS POS/HGI for facility identity, AHRQ as linkage spine, Census before OSM.

    Don't:
    - Treat AHRQ as final current-operator authority.
    - Sum bed rows when duplicate campus, incompatible row scope, or material source variance is reported.
    - Persist vague official count claims such as "more than 20 locations" as exact metric values.

    Examples
    --------
    Basic MCP call shape:
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"build_profile_evidence_pack","arguments":{"state":"PA","system_name":"Jefferson Health","required_fields":["county_geoid","system_bed_count"]}}}

    Common mistakes
    ---------------
    - Passing a system name without state: provide state to keep public-source matching bounded.
    - Treating unavailable_public as proof of absence: it only records searched public sources.
    - Using OSM fallback geography without review when Census Geocoder could not match.
    """

    state_norm = str(state or "").strip().upper()
    if len(state_norm) != 2 or not state_norm.isalpha():
        return _system_error_response(
            "state must be a two-letter abbreviation such as 'PA'.",
            code="invalid_params",
            query={
                "state": state,
                "system_slug": system_slug or "",
                "system_name": system_name or "",
                "ccns": ccns or [],
                "required_fields": required_fields or [],
            },
            match_basis="invalid_state_argument",
            confidence="not_evaluated",
            next_step="Retry with a two-letter state abbreviation and at least one system name, slug, or CCN when available.",
        )

    systems_df = await _load_ahrq_systems()
    hospitals_df = await _load_ahrq_hospitals()
    pos_df = await _load_pos()
    normalized_ccns = [normalize_ccn(ccn) for ccn in (ccns or []) if normalize_ccn(ccn)]

    return await assemble_profile_evidence_pack(
        state=state_norm,
        system_slug=system_slug or "",
        system_name=system_name or "",
        ccns=normalized_ccns,
        required_fields=required_fields or [],
        systems_df=systems_df,
        hospitals_df=hospitals_df,
        pos_df=pos_df,
        provider_enrollment_loader=_load_profile_provider_rows,
        hcris_loader=_load_hcris_bed_rows,
        state_bed_loader=_load_state_bed_rows,
        official_evidence_loader=_load_official_profile_evidence,
        census_geocoder=_census_geocode_address,
        osm_geocoder=_osm_geocode_address,
        reverse_geocoder=_reverse_geocode_coordinates,
    )


if __name__ == "__main__":
    mcp.run(transport=_transport)
