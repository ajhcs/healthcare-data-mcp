"""Financial Intelligence MCP Server.

Provides tools for IRS Form 990 nonprofit financials, SEC EDGAR corporate
filings, and municipal bond data from public APIs.
"""

from typing import Any, Mapping
import asyncio
from datetime import datetime, timezone
import logging
import os as _os

from mcp.server.fastmcp import FastMCP
from shared.utils.mcp_observability import observe_tool
from shared.utils.mcp_resources import register_standard_resources
from shared.utils.mcp_response import error_response, evidence_receipt, to_structured
from shared.utils.cost_report import load_cost_report_row
from shared.utils.healthcare_identity import MatchDecision, identity_from_public_record
from shared.utils.identity import normalize_ccn, normalize_name

from . import edgar_client, propublica_client
from .audited_financial_pdf import parse_audited_financial_pdf as _parse_audited_financial_pdf
from .financial_health import load_ahrq_hfmd_profile, normalize_hcris_public_metrics
from .irs990_parser import download_990_xml, lookup_xml_url, parse_990_xml
from .models import (
    Form990Details,
    Form990Summary,
    MuniBond,
    MuniBondDetails,
    Officer,
    SecFiling,
    SecFilingDetail,
)

from servers.hospital_quality import data_loaders as hospital_quality_data_loaders

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "financial-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = _os.environ.get("MCP_HOST", "127.0.0.1")
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8008"))
mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "financial-intelligence")


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _first_present(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _reported_metric(value: Any, confidence: str, source_field: str) -> dict[str, Any]:
    return {
        "value": value,
        "confidence": confidence if value is not None else "not_available",
        "source_field": source_field if value is not None else "",
    }


async def _latest_990_schedule_h(ein: str) -> dict[str, Any]:
    if not ein:
        return {}
    org_data = await propublica_client.get_organization(ein)
    filings = org_data.get("filings_with_data", []) if org_data else []
    if not filings:
        return {"ein": ein, "source_status": "no_990_filing_found"}
    latest = filings[0]
    tax_period = str(latest.get("tax_prd", latest.get("tax_prd_yr", "")))
    xml_url = latest.get("xml_url", "") or await lookup_xml_url(ein, tax_period) or ""
    parsed: dict[str, Any] = {}
    if xml_url:
        xml_path = await download_990_xml(xml_url, ein, tax_period)
        if xml_path:
            parsed = parse_990_xml(xml_path)
    metrics = {
        "charity_care_cost": _reported_metric(
            parsed.get("charity_care_cost") or parsed.get("community_benefit_total"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "CharityCareAtCostAmt" if parsed.get("charity_care_cost") else "TotalCommunityBenefitExpnsAmt",
        ),
        "bad_debt_expense": _reported_metric(
            parsed.get("bad_debt_expense"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "BadDebtExpenseAmt",
        ),
        "medicare_shortfall": _reported_metric(
            parsed.get("medicare_shortfall"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "MedicareShortfallAmt",
        ),
        "medicaid_shortfall": _reported_metric(
            parsed.get("medicaid_shortfall"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "MedicaidShortfallAmt",
        ),
        "community_benefit_total": _reported_metric(
            parsed.get("community_benefit_total"),
            "high_reported_irs_schedule_h_xml" if parsed else "not_available",
            "TotalCommunityBenefitExpnsAmt",
        ),
        "community_benefit_pct": _reported_metric(
            parsed.get("community_benefit_pct"),
            "medium_derived_from_schedule_h_total_expenses" if parsed.get("community_benefit_pct") is not None else "not_available",
            "TotalCommunityBenefitExpnsAmt / CYTotalExpensesAmt",
        ),
        "total_revenue": _reported_metric(
            parsed.get("total_revenue") or _safe_float(latest.get("totrevenue")),
            "high_reported_irs_xml_or_propublica_summary",
            "TotalRevenueAmt or totrevenue",
        ),
        "total_expenses": _reported_metric(
            parsed.get("total_expenses") or _safe_float(latest.get("totfuncexpns")),
            "high_reported_irs_xml_or_propublica_summary",
            "CYTotalExpensesAmt or totfuncexpns",
        ),
    }
    return {
        "ein": ein,
        "tax_period": tax_period,
        "source": "IRS Form 990 Schedule H XML" if parsed else "ProPublica Form 990 summary",
        "xml_url": xml_url,
        "charity_care": metrics["charity_care_cost"]["value"],
        "bad_debt_expense": parsed.get("bad_debt_expense"),
        "medicare_shortfall": parsed.get("medicare_shortfall"),
        "medicaid_shortfall": parsed.get("medicaid_shortfall"),
        "community_benefit_total": parsed.get("community_benefit_total"),
        "community_benefit_pct": parsed.get("community_benefit_pct"),
        "total_revenue": parsed.get("total_revenue") or _safe_float(latest.get("totrevenue")),
        "total_expenses": parsed.get("total_expenses") or _safe_float(latest.get("totfuncexpns")),
        "source_status": "ready" if parsed else "summary_only",
        "metrics": metrics,
        "metric_confidence": {name: metric["confidence"] for name, metric in metrics.items()},
    }


async def _cost_report_public_metrics(ccn: str) -> dict[str, Any]:
    if not ccn:
        return {}
    row, error = await load_cost_report_row(hospital_quality_data_loaders, ccn)
    if error:
        return {"ccn": ccn, "source_status": "unavailable", "detail": error}
    return normalize_hcris_public_metrics(row, requested_ccn=ccn)


def _metric_value(*sources: tuple[dict[str, Any], str]) -> Any:
    for source, key in sources:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def _metric_confidence(*sources: tuple[dict[str, Any], str]) -> str:
    for source, key in sources:
        value = source.get(key)
        if value in (None, ""):
            continue
        confidence = source.get("metric_confidence", {}).get(key)
        if confidence:
            return confidence
    return "not_available"


def _selected_metric_evidence(metric_name: str, *sources: tuple[dict[str, Any], str]) -> dict[str, Any]:
    """Return the evidence receipt for the source metric promoted to a profile field."""

    for source, source_metric_name in sources:
        value = source.get(source_metric_name)
        if value in (None, ""):
            continue
        evidence_key = _metric_evidence_key(source_metric_name)
        evidence = (source.get("metric_evidence") or {}).get(evidence_key)
        if not isinstance(evidence, dict):
            continue
        selected = dict(evidence)
        query = dict(selected.get("query") or {})
        query["promoted_metric_name"] = metric_name
        query["selected_source_metric"] = source_metric_name
        query["selected_evidence_metric"] = evidence_key
        selected["query"] = query
        selected["next_step"] = (
            "Preserve this promoted metric receipt, selected source metric, source_field, confidence, "
            "and parent identity_map before citing the profile field."
        )
        return selected
    return {}


def _metric_evidence_key(source_metric_name: str) -> str:
    return {
        "charity_care": "charity_care_cost",
    }.get(source_metric_name, source_metric_name)


def _selected_metric_evidence_map(
    selections: Mapping[str, tuple[tuple[dict[str, Any], str], ...]],
) -> dict[str, dict[str, Any]]:
    return {
        metric_name: evidence
        for metric_name, sources in selections.items()
        if (evidence := _selected_metric_evidence(metric_name, *sources))
    }


def _financial_evidence(
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    source_name: str = "CMS HCRIS, IRS Form 990 Schedule H, and AHRQ HFMD public sources",
    source_url: str = "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
    dataset_id: str = "public_financial_health_profile",
    source_period: str = "latest available public source period at request time",
    cache_status: str = "live_or_configured_public_source",
    cache_freshness: str = "source freshness depends on public API response or configured local cache",
) -> dict[str, Any]:
    return evidence_receipt(
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        retrieved_at=datetime.now(timezone.utc).isoformat(),
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        entity_scope="facility_or_nonprofit_finance",
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        caveat="Public financial fields vary by filing period and organization type; missing source fields are not zero values.",
        next_step="Preserve metric_confidence and source rows before citing financial facts.",
    )


def _financial_source_metadata(evidence: dict[str, Any]) -> dict[str, Any]:
    """Return source/cache metadata paired with a financial evidence receipt."""

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
        "entity_scope": evidence.get("entity_scope", "facility_or_nonprofit_finance"),
        "query": evidence.get("query", {}),
        "cache_key": evidence.get("cache_key", ""),
        "source_type": "public_financial_source",
    }


def _attach_financial_source_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        payload["source_metadata"] = _financial_source_metadata(evidence)
    return payload


def _financial_row_evidence(
    *,
    row: dict[str, Any],
    parent_query: dict[str, Any],
    source_name: str,
    source_url: str,
    dataset_id: str,
    source_period: str,
    match_basis: str,
    confidence: str,
    cache_status: str = "live_or_configured_public_source",
    cache_freshness: str = "source freshness depends on public API response or configured local cache",
    caveat: str = "Public financial rows are source-specific candidates unless exact EIN, CIK, accession number, or reviewed source URL supports the fact.",
    next_step: str = "Open the row source URL and preserve exact identifiers before citing this financial fact.",
) -> dict[str, Any]:
    row_source_url = str(
        row.get("url")
        or row.get("filing_url")
        or row.get("source_url")
        or source_url
        or ""
    ).strip()
    row_query = {
        **parent_query,
        "row_ein": row.get("ein") or "",
        "row_cik": row.get("cik") or "",
        "row_accession_number": row.get("accession_number") or "",
        "row_name": row.get("name") or row.get("company_name") or row.get("issuer_name") or row.get("description") or "",
        "row_source_url": row_source_url,
    }
    return _financial_evidence(
        query=row_query,
        source_name=source_name,
        source_url=row_source_url,
        dataset_id=dataset_id,
        source_period=source_period,
        cache_status=cache_status,
        cache_freshness=cache_freshness,
        match_basis=match_basis,
        confidence=confidence,
    ) | {
        "caveat": caveat,
        "next_step": next_step,
    }


def _financial_profile_source_with_evidence(
    source_key: str,
    source: dict[str, Any],
    *,
    query: dict[str, Any],
) -> dict[str, Any]:
    """Attach source-specific evidence to a financial profile source block."""

    if not source:
        return {}
    enriched = dict(source)
    evidence_kwargs = _financial_profile_source_evidence_kwargs(source_key, enriched, query=query)
    enriched["evidence"] = _financial_evidence(**evidence_kwargs)
    enriched["metric_evidence"] = _financial_profile_metric_evidence(
        source_key=source_key,
        source=enriched,
        evidence_kwargs=evidence_kwargs,
    )
    enriched["source_metadata"] = {
        "source_name": evidence_kwargs["source_name"],
        "source_url": evidence_kwargs["source_url"],
        "dataset_id": evidence_kwargs["dataset_id"],
        "source_period": evidence_kwargs["source_period"],
        "cache_status": evidence_kwargs["cache_status"],
        "cache_freshness": evidence_kwargs["cache_freshness"],
        "source_status": str(enriched.get("source_status") or ""),
    }
    return enriched


def _financial_profile_metric_evidence(
    *,
    source_key: str,
    source: dict[str, Any],
    evidence_kwargs: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    metrics = source.get("metrics") if isinstance(source.get("metrics"), dict) else {}
    metric_receipts: dict[str, dict[str, Any]] = {}
    for metric_name, metric in metrics.items():
        metric_payload = metric if isinstance(metric, dict) else {"value": metric}
        value = metric_payload.get("value")
        confidence = str(
            metric_payload.get("confidence")
            or source.get("metric_confidence", {}).get(metric_name, "")
            or ("not_available" if value in (None, "") else "source_metric_reported")
        )
        metric_query = {
            **evidence_kwargs["query"],
            "metric_source": source_key,
            "metric_name": str(metric_name),
            "metric_value_present": value not in (None, ""),
            "metric_confidence": confidence,
            "source_field": metric_payload.get("source_field") or "",
            "ccn": source.get("ccn") or evidence_kwargs["query"].get("ccn") or "",
            "ein": source.get("ein") or evidence_kwargs["query"].get("ein") or "",
            "tax_period": source.get("tax_period") or "",
            "fiscal_year_end": source.get("fiscal_year_end") or source.get("fiscal_year") or "",
            "matched_on": source.get("matched_on") or "",
        }
        metric_receipts[str(metric_name)] = _financial_evidence(
            query={key: item for key, item in metric_query.items() if item not in ("", None, [], {})},
            source_name=evidence_kwargs["source_name"],
            source_url=evidence_kwargs["source_url"],
            dataset_id=evidence_kwargs["dataset_id"],
            source_period=evidence_kwargs["source_period"],
            cache_status=evidence_kwargs["cache_status"],
            cache_freshness=evidence_kwargs["cache_freshness"],
            match_basis=f"{evidence_kwargs['match_basis']}_metric_{metric_name}",
            confidence=confidence,
        ) | {
            "caveat": (
                "This metric is source-field-specific public financial context; missing fields are not zero values "
                "and cross-source comparisons require matching filing periods and entity identifiers."
            ),
            "next_step": (
                "Preserve this metric receipt, source_field, confidence, and parent identity_map before citing the value."
            ),
        }
    return metric_receipts


def _financial_profile_source_evidence_kwargs(
    source_key: str,
    source: dict[str, Any],
    *,
    query: dict[str, Any],
) -> dict[str, Any]:
    source_status = str(source.get("source_status") or "not_requested")
    metrics = source.get("metrics") if isinstance(source.get("metrics"), dict) else {}
    source_query = {**query, "source": source_key, "source_status": source_status}
    if source_key == "hcris":
        fiscal_period = str(source.get("fiscal_year_end") or source.get("fiscal_year") or "")
        return {
            "query": source_query,
            "source_name": "CMS HCRIS / Hospital Cost Report public file",
            "source_url": "https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
            "dataset_id": "cms_hcris_public_cost_report",
            "source_period": fiscal_period or "latest available CMS public cost-report period at request time",
            "cache_status": source_status,
            "cache_freshness": "loaded from configured CMS cost-report cache at request time",
            "match_basis": "ccn_exact_hcris_public_cost_report" if source_status == "ready" else "ccn_hcris_public_cost_report_unavailable",
            "confidence": "metric_level_confidence" if metrics else "not_evaluated_or_no_reported_hcris_fields",
        }
    if source_key == "form990_schedule_h":
        ein = str(source.get("ein") or query.get("ein") or "")
        source_url = str(source.get("xml_url") or "")
        if not source_url and ein:
            source_url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"
        if not source_url:
            source_url = "https://projects.propublica.org/nonprofits/"
        match_basis_by_status = {
            "ready": "ein_exact_irs_schedule_h_xml",
            "summary_only": "ein_exact_propublica_form990_summary",
            "no_990_filing_found": "ein_no_form990_filing_found",
        }
        return {
            "query": source_query,
            "source_name": "IRS Form 990 Schedule H / ProPublica Nonprofit Explorer",
            "source_url": source_url,
            "dataset_id": "irs_form_990_schedule_h",
            "source_period": str(source.get("tax_period") or "latest available IRS public filing period at request time"),
            "cache_status": source_status,
            "cache_freshness": "live ProPublica/IRS lookup or cached XML parse at request time",
            "match_basis": match_basis_by_status.get(source_status, "ein_form990_schedule_h_not_evaluated"),
            "confidence": "metric_level_confidence" if metrics else "not_available_or_no_schedule_h_fields",
        }
    if source_key == "ahrq_hfmd":
        matched_on = str(source.get("matched_on") or "")
        match_basis = (
            "ccn_exact_ahrq_hfmd_profile"
            if matched_on == "ccn"
            else "state_filtered_ahrq_hfmd_profile"
            if matched_on == "state"
            else "ahrq_hfmd_no_match"
            if source_status == "no_match"
            else "ahrq_hfmd_cache_not_found"
            if source_status == "cache_not_found"
            else "ahrq_hfmd_source_status"
        )
        return {
            "query": source_query,
            "source_name": str(source.get("source_name") or "AHRQ Hospital Financial Measures Database"),
            "source_url": str(source.get("source_url") or "https://www.ahrq.gov/data/innovations/hfmd.html"),
            "dataset_id": "ahrq_hfmd",
            "source_period": "latest cached AHRQ HFMD artifact at request time",
            "cache_status": source_status,
            "cache_freshness": str(source.get("cache_path") or "configured AHRQ HFMD cache status at request time"),
            "match_basis": match_basis,
            "confidence": "metric_level_confidence" if metrics else "not_available_or_no_hfmd_fields",
        }
    return {
        "query": source_query,
        "source_name": "Public financial source",
        "source_url": "",
        "dataset_id": "public_financial_source",
        "source_period": "latest available public source period at request time",
        "cache_status": source_status,
        "cache_freshness": "source freshness depends on public API response or configured local cache",
        "match_basis": f"{source_key}_public_financial_source",
        "confidence": "source_specific_context",
    }


def _financial_identity(
    *,
    ccn: str = "",
    ein: str = "",
    facility_name: str = "",
    match_basis: str = "",
    confidence: str = "",
) -> dict[str, Any]:
    identity = identity_from_public_record(
        name=facility_name,
        entity_type="facility_or_nonprofit_finance",
        ccn=ccn,
        source_name="financial-intelligence public finance workflow",
        source_url="https://data.cms.gov/provider-compliance/cost-reports/hospital-provider-cost-report",
    )
    if match_basis or confidence:
        identity.match_decisions.append(
            MatchDecision(
                basis=match_basis,
                confidence=confidence,
                decided_at=datetime.now(timezone.utc).isoformat(),
                notes="Financial profile identity is anchored by public CCN when present; EIN is preserved as an unresolved nonprofit/tax identifier.",
            )
        )
    if ein:
        identity.unresolved_identifiers.append({"type": "ein", "value": str(ein)})
    return identity.to_dict()


def _financial_identity_map(
    *,
    query: dict[str, Any],
    ccn: str = "",
    ein: str = "",
    facility_name: str = "",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return finance-specific identity joins and source-claim boundaries."""

    data = payload or {}
    hcris = data.get("hcris") or data.get("sources", {}).get("hcris") or {}
    form990 = data.get("form990_schedule_h") or data.get("sources", {}).get("form990_schedule_h") or {}
    hfmd = data.get("ahrq_hfmd") or {}
    organizations = data.get("organizations") if isinstance(data.get("organizations"), list) else []
    filings = data.get("filings") if isinstance(data.get("filings"), list) else []
    bonds = data.get("bonds") if isinstance(data.get("bonds"), list) else []
    documents = data.get("documents") if isinstance(data.get("documents"), list) else []
    ccns = _financial_identity_values(
        "ccn",
        ccn,
        query.get("ccn"),
        data.get("ccn"),
        hcris.get("ccn"),
        hfmd.get("ccn"),
        hfmd.get("join_keys", {}).get("hfmd_provider_id") if isinstance(hfmd.get("join_keys"), dict) else "",
    )
    eins = _financial_identity_values(
        "ein",
        ein,
        query.get("ein"),
        data.get("ein"),
        form990.get("ein"),
        *(row.get("ein") for row in organizations if isinstance(row, dict)),
    )
    names = _financial_identity_values(
        "canonical_name",
        facility_name,
        query.get("entity_name"),
        query.get("query"),
        data.get("name"),
        data.get("company_name"),
        data.get("issuer_name"),
        hcris.get("facility_name"),
        hcris.get("hospital_name"),
        hfmd.get("facility_name"),
        form990.get("name"),
        *(row.get("name") for row in organizations if isinstance(row, dict)),
        *(row.get("company_name") for row in filings if isinstance(row, dict)),
        *(row.get("issuer_name") for row in bonds if isinstance(row, dict)),
    )
    states = _financial_identity_values(
        "state",
        query.get("state"),
        data.get("state"),
        hcris.get("state"),
        hfmd.get("state"),
        *(row.get("state") for row in organizations if isinstance(row, dict)),
        *(row.get("state") for row in bonds if isinstance(row, dict)),
    )
    ciks = _financial_identity_values(
        "cik",
        query.get("cik"),
        data.get("cik"),
        *(row.get("cik") for row in filings if isinstance(row, dict)),
        *(row.get("cik") for row in bonds if isinstance(row, dict)),
    )
    accession_numbers = _financial_identity_values(
        "accession_number",
        query.get("accession_number"),
        data.get("accession_number"),
        *(row.get("accession_number") for row in filings if isinstance(row, dict)),
        *(row.get("accession_number") for row in bonds if isinstance(row, dict)),
    )
    source_urls = _financial_identity_values(
        "source_url",
        data.get("source_url"),
        data.get("filing_url"),
        data.get("official_statement_url"),
        data.get("evidence", {}).get("source_url") if isinstance(data.get("evidence"), dict) else "",
        *(row.get("filing_url") or row.get("source_url") for row in filings if isinstance(row, dict)),
        *(row.get("filing_url") or row.get("source_url") for row in bonds if isinstance(row, dict)),
        *(row.get("url") or row.get("source_url") for row in documents if isinstance(row, dict)),
    )
    source_claims = _financial_source_claims(data)
    return {
        "entity_scope": "facility_or_nonprofit_finance",
        "join_keys": [
            {
                "field": "ccn",
                "values": ccns,
                "status": "provided" if ccns else "missing",
                "used_by": _financial_join_key_usage("ccn", source_claims),
            },
            {
                "field": "ein",
                "values": eins,
                "status": "provided" if eins else "missing",
                "used_by": _financial_join_key_usage("ein", source_claims),
            },
            {
                "field": "canonical_name",
                "values": names,
                "status": "provided" if names else "missing",
                "used_by": _financial_join_key_usage("canonical_name", source_claims),
            },
            {
                "field": "state",
                "values": states,
                "status": "provided" if states else "missing",
                "used_by": _financial_join_key_usage("state", source_claims),
            },
            {
                "field": "cik",
                "values": ciks,
                "status": "provided" if ciks else "missing",
                "used_by": _financial_join_key_usage("cik", source_claims),
            },
            {
                "field": "accession_number",
                "values": accession_numbers,
                "status": "provided" if accession_numbers else "missing",
                "used_by": _financial_join_key_usage("accession_number", source_claims),
            },
            {
                "field": "source_url",
                "values": source_urls,
                "status": "provided" if source_urls else "missing",
                "used_by": _financial_join_key_usage("source_url", source_claims),
            },
        ],
        "source_claims": source_claims,
        "conflict_policy": [
            "Use CCN for hospital HCRIS/HFMD joins and EIN for IRS Form 990 joins.",
            "Keep hospital-level cost-report rows and nonprofit/system-level filings separate unless exact identifiers support the join.",
            "Treat facility names, obligated-group names, and issuer names as aliases or candidate matches, not proof of common reporting entity.",
            "Preserve source period, accession/EIN/query basis, and metric-level confidence before citing public financial facts.",
        ],
        "missing_data_policy": (
            "No-match or missing financial-source responses identify the searched public-source scope; "
            "they are not proof of no filing, no public debt disclosure, no charity care, no uncompensated care, or current financial condition."
        ),
    }


def _financial_identity_values(field: str, *values: Any) -> list[str]:
    normalized_values: set[str] = set()
    for value in values:
        normalized = _normalize_financial_identity_value(field, value)
        if normalized:
            normalized_values.add(normalized)
    return sorted(normalized_values)


def _normalize_financial_identity_value(field: str, value: Any) -> str:
    if value in ("", None):
        return ""
    if field == "ccn":
        return normalize_ccn(value) or ""
    if field == "ein":
        return "".join(character for character in str(value) if character.isdigit())
    if field == "cik":
        return "".join(character for character in str(value) if character.isdigit()).lstrip("0") or "0"
    if field == "accession_number":
        return str(value).strip()
    if field == "canonical_name":
        return normalize_name(value, remove_legal_suffixes=True)
    if field == "state":
        return str(value).strip().upper()
    return str(value).strip()


def _financial_source_claims(payload: dict[str, Any]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    if "metric_evidence" in payload:
        claims.append(
            {
                "collection": "selected_public_financial_metrics",
                "identity_paths": ["ccn", "ein", "facility_name"],
                "evidence_path": "evidence",
                "metric_evidence_paths": ["metric_evidence.*"],
                "match_policy": "promoted_profile_metric_requires_selected_source_receipt",
            }
        )
    if "hcris" in payload or "hcris" in payload.get("sources", {}):
        claims.append(
            {
                "collection": "hcris",
                "identity_paths": ["hcris.ccn", "hcris.facility_name", "sources.hcris.ccn"],
                "evidence_path": "evidence",
                "metric_evidence_paths": ["hcris.metric_evidence.*", "sources.hcris.metric_evidence.*"],
                "match_policy": "ccn_required_for_hospital_cost_report_merge",
            }
        )
    if "form990_schedule_h" in payload or "form990_schedule_h" in payload.get("sources", {}):
        claims.append(
            {
                "collection": "form990_schedule_h",
                "identity_paths": ["form990_schedule_h.ein", "form990_schedule_h.name", "sources.form990_schedule_h.ein"],
                "evidence_path": "evidence",
                "metric_evidence_paths": [
                    "form990_schedule_h.metric_evidence.*",
                    "sources.form990_schedule_h.metric_evidence.*",
                ],
                "match_policy": "ein_required_for_form990_merge",
            }
        )
    if "ahrq_hfmd" in payload:
        claims.append(
            {
                "collection": "ahrq_hfmd",
                "identity_paths": ["ahrq_hfmd.join_keys.hfmd_provider_id", "ahrq_hfmd.facility_name", "ahrq_hfmd.state"],
                "evidence_path": "evidence",
                "metric_evidence_paths": ["ahrq_hfmd.metric_evidence.*"],
                "match_policy": "ccn_or_hfmd_provider_id_required_for_hfmd_merge",
            }
        )
    if "organizations" in payload:
        claims.append(
            {
                "collection": "irs_form_990_search",
                "identity_paths": ["query.query", "query.state", "organizations.ein", "organizations.name", "organizations.state"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["organizations[].evidence"],
                "match_policy": "ein_exact_for_form990_facts_name_search_returns_candidates",
            }
        )
    if "filings" in payload:
        claims.append(
            {
                "collection": "sec_edgar_filings_search",
                "identity_paths": ["query.query", "query.filing_type", "filings.cik", "filings.accession_number", "filings.company_name", "filings.filing_url"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["filings[].evidence"],
                "match_policy": "accession_number_and_cik_required_for_sec_filing_facts",
            }
        )
    if "bonds" in payload:
        claims.append(
            {
                "collection": "sec_edgar_municipal_bond_search",
                "identity_paths": ["query.query", "query.state", "bonds.accession_number", "bonds.issuer_name", "bonds.state", "bonds.source_url"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["bonds[].evidence"],
                "match_policy": "accession_number_required_for_municipal_bond_facts_issuer_names_are_candidates",
            }
        )
    dataset_id = str(payload.get("evidence", {}).get("dataset_id") or "")
    if dataset_id == "irs_form_990_xml" or dataset_id == "propublica_form_990_summary":
        claims.append(
            {
                "collection": dataset_id,
                "identity_paths": ["query.ein", "ein", "name", "tax_period", "evidence.source_url"],
                "evidence_path": "evidence",
                "match_policy": "ein_exact_latest_public_form990_filing",
            }
        )
    if dataset_id == "sec_edgar_filing_detail":
        claims.append(
            {
                "collection": "sec_edgar_filing_detail",
                "identity_paths": ["query.accession_number", "accession_number", "cik", "company_name", "evidence.source_url"],
                "evidence_path": "evidence",
                "match_policy": "accession_number_exact_sec_filing_detail",
            }
        )
    if dataset_id == "sec_edgar_municipal_bond_detail":
        claims.append(
            {
                "collection": "sec_edgar_municipal_bond_detail",
                "identity_paths": ["query.accession_number", "accession_number", "issuer_name", "source_url", "documents.url"],
                "evidence_path": "evidence",
                "row_evidence_paths": ["documents[].evidence"],
                "match_policy": "accession_number_exact_municipal_bond_detail",
            }
        )
    if dataset_id == "audited_financial_statement_pdf":
        claims.append(
            {
                "collection": "audited_financial_statement_pdf",
                "identity_paths": ["query.entity_name", "query.fiscal_year", "source_url", "metrics", "citations"],
                "evidence_path": "evidence",
                "match_policy": "user_supplied_document_identity_requires_source_review",
            }
        )
    if not claims:
        claims.append(
            {
                "collection": "financial_source_query",
                "identity_paths": ["query.ccn", "query.ein", "query.entity_name"],
                "evidence_path": "evidence",
                "match_policy": "exact_identifier_required_for_financial_fact",
            }
        )
    return claims


def _financial_join_key_usage(field: str, source_claims: list[dict[str, Any]]) -> list[str]:
    path_tokens = {
        "ccn": ("ccn", "hfmd_provider_id"),
        "ein": ("ein",),
        "canonical_name": ("name", "facility_name", "entity_name"),
        "state": ("state",),
        "cik": ("cik",),
        "accession_number": ("accession_number",),
        "source_url": ("source_url", "filing_url", "url"),
    }[field]
    used_by = []
    for claim in source_claims:
        paths = " ".join(str(path) for path in claim.get("identity_paths", []))
        if any(token in paths for token in path_tokens):
            used_by.append(str(claim.get("collection") or ""))
    return sorted(item for item in used_by if item)


def _financial_error_response(
    message: str,
    *,
    query: dict[str, Any],
    match_basis: str,
    confidence: str,
    code: str = "not_found",
    detail: Any | None = None,
    source_name: str = "Public financial source",
    source_url: str = "",
    dataset_id: str = "public_financial_source",
    source_period: str = "latest available public source period at request time",
    ccn: str = "",
    ein: str = "",
    facility_name: str = "",
) -> dict[str, Any]:
    evidence = _financial_evidence(
        query=query,
        match_basis=match_basis,
        confidence=confidence,
        source_name=source_name,
        source_url=source_url,
        dataset_id=dataset_id,
        source_period=source_period,
    )
    return error_response(
        message,
        code=code,
        detail=detail,
        evidence=evidence,
        source_metadata=_financial_source_metadata(evidence),
        identity=_financial_identity(
            ccn=ccn,
            ein=ein,
            facility_name=facility_name,
            match_basis=match_basis,
            confidence=confidence,
        ),
        identity_map=_financial_identity_map(
            query=query,
            ccn=ccn,
            ein=ein,
            facility_name=facility_name,
            payload={"evidence": evidence, "source_url": source_url},
        ),
    )


def _financial_facility_name(*sources: dict[str, Any]) -> str:
    for source in sources:
        for key in ("facility_name", "hospital_name", "provider_name", "name"):
            value = str(source.get(key, "") or "").strip()
            if value:
                return value
    return ""


def _build_form990_summary(search_org: dict, org_data: dict | None) -> dict:
    details = org_data or {}
    organization = details.get("organization", {})
    filings = details.get("filings_with_data", [])
    latest_filing = filings[0] if filings else {}

    return Form990Summary(
        ein=str(_first_present(search_org.get("ein"), organization.get("ein"), "")),
        name=_first_present(search_org.get("name"), organization.get("name"), "") or "",
        city=_first_present(search_org.get("city"), organization.get("city"), "") or "",
        state=_first_present(search_org.get("state"), organization.get("state"), "") or "",
        ntee_code=_first_present(search_org.get("ntee_code"), organization.get("ntee_code"), "") or "",
        total_revenue=_safe_float(
            _first_present(
                latest_filing.get("totrevenue"),
                organization.get("revenue_amount"),
                search_org.get("revenue_amount"),
            )
        ),
        total_expenses=_safe_float(
            _first_present(
                latest_filing.get("totfuncexpns"),
                organization.get("expenses_amount"),
                search_org.get("expenses_amount"),
            )
        ),
        net_assets=_safe_float(
            _first_present(
                latest_filing.get("totnetassetend"),
                latest_filing.get("totassetsend"),
                organization.get("asset_amount"),
                search_org.get("asset_amount"),
            )
        ),
        tax_period=str(
            _first_present(
                latest_filing.get("tax_prd"),
                latest_filing.get("tax_prd_yr"),
                search_org.get("tax_period"),
                organization.get("tax_period"),
            )
            or ""
        ),
    ).model_dump()


# ---------------------------------------------------------------------------
# Tool 1: search_form990
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def search_form990(query: str, state: str = "", ntee_code: str = "") -> dict[str, Any]:
    """Search IRS Form 990 filings by organization name or EIN.

    Returns nonprofit organizations with revenue, expenses, and net assets
    from the most recent filing.

    Args:
        query: Organization name or EIN to search for.
        state: Two-letter state code filter (e.g. "OH").
        ntee_code: NTEE category code filter (1-10).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_form990","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        data = await propublica_client.search_organizations(query, state=state, ntee_code=ntee_code)
        orgs = data.get("organizations", [])

        limited_orgs = orgs[:25]
        org_details = await asyncio.gather(
            *(propublica_client.get_organization(str(org.get("ein", ""))) for org in limited_orgs)
        )
        results = [
            _build_form990_summary(org, detail)
            for org, detail in zip(limited_orgs, org_details, strict=False)
        ]

        query_payload = {"query": query, "state": state, "ntee_code": ntee_code}
        for organization in results:
            ein = str(organization.get("ein") or "").strip()
            organization["source_url"] = f"https://projects.propublica.org/nonprofits/organizations/{ein}" if ein else "https://projects.propublica.org/nonprofits/"
            organization["evidence"] = _financial_row_evidence(
                row=organization,
                parent_query=query_payload,
                source_name="ProPublica Nonprofit Explorer organization row",
                source_url=organization["source_url"],
                dataset_id="irs_form_990_search",
                source_period=str(organization.get("tax_period") or "latest ProPublica/IRS public filing metadata available at query time"),
                match_basis="form990_organization_search_row",
                confidence="candidate_nonprofit_match",
                next_step="Open the ProPublica/IRS organization page and verify EIN/tax period before citing this row.",
            )
        payload = {
            "total_results": data.get("total_results", 0),
            "organizations": results,
            "evidence": _financial_evidence(
                query=query_payload,
                source_name="ProPublica Nonprofit Explorer and IRS Form 990 public filings",
                source_url="https://projects.propublica.org/nonprofits/",
                dataset_id="irs_form_990_search",
                source_period="latest ProPublica/IRS public filing metadata available at query time",
                match_basis="organization_name_or_ein_search_no_match" if not results else "organization_name_or_ein_search",
                confidence="no_matching_public_form990_records_returned" if not results else "candidate_nonprofit_matches",
            ),
        }
        payload["identity_map"] = _financial_identity_map(query=query_payload, payload=payload)
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("search_form990 failed")
        return error_response(f"search_form990 failed: {e}")


# ---------------------------------------------------------------------------
# Tool 2: get_form990_details
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_form990_details(ein: str) -> dict[str, Any]:
    """Get detailed Form 990 data for a nonprofit by EIN.

    Returns revenue breakdown, functional expenses (Part IX), Schedule H
    community benefit (hospitals), officer compensation, and program descriptions.
    Parses the full IRS e-file XML when available; falls back to ProPublica summary.

    Args:
        ein: Employer Identification Number (e.g. "341323166").

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_form990_details","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        org_data = await propublica_client.get_organization(ein)
        if not org_data:
            return _financial_error_response(
                f"Organization not found for EIN: {ein}",
                query={"ein": ein},
                match_basis="ein_exact_no_form990_organization_match",
                confidence="no_matching_public_form990_organization_returned",
                source_name="ProPublica Nonprofit Explorer and IRS Form 990 public filings",
                source_url="https://projects.propublica.org/nonprofits/",
                dataset_id="irs_form_990_detail",
                ein=ein,
            )

        org = org_data.get("organization", {})
        filings = org_data.get("filings_with_data", [])

        if not filings:
            return _financial_error_response(
                f"No filings with data found for EIN: {ein}",
                query={"ein": ein},
                match_basis="ein_exact_no_form990_filings_with_data",
                confidence="organization_found_no_public_filing_data",
                source_name="ProPublica Nonprofit Explorer and IRS Form 990 public filings",
                source_url="https://projects.propublica.org/nonprofits/",
                dataset_id="irs_form_990_detail",
                ein=ein,
                facility_name=str(org.get("name") or ""),
            )

        latest = filings[0]
        tax_period = str(latest.get("tax_prd", latest.get("tax_prd_yr", "")))

        # Try to get XML URL — ProPublica may or may not include it
        xml_url = latest.get("xml_url", "")

        # If ProPublica doesn't provide XML URL, try IRS e-file index
        if not xml_url:
            xml_url = await lookup_xml_url(ein, tax_period) or ""

        if xml_url:
            xml_path = await download_990_xml(xml_url, ein, tax_period)
            if xml_path:
                parsed = parse_990_xml(xml_path)
                result = Form990Details(
                    ein=ein,
                    name=org.get("name", ""),
                    tax_period=tax_period,
                    source="xml",
                    contributions=parsed.get("contributions"),
                    program_service_revenue=parsed.get("program_service_revenue"),
                    investment_income=parsed.get("investment_income"),
                    other_revenue=parsed.get("other_revenue"),
                    total_revenue=parsed.get("total_revenue"),
                    total_expenses=parsed.get("total_expenses"),
                    program_expenses=parsed.get("program_expenses"),
                    management_expenses=parsed.get("management_expenses"),
                    fundraising_expenses=parsed.get("fundraising_expenses"),
                    community_benefit_total=parsed.get("community_benefit_total"),
                    community_benefit_pct=parsed.get("community_benefit_pct"),
                    officers=[Officer(**o) for o in parsed.get("officers", [])],
                    program_descriptions=parsed.get("program_descriptions", []),
                )
                payload = result.model_dump()
                payload["evidence"] = _financial_evidence(
                    query={"ein": ein, "tax_period": tax_period},
                    source_name="IRS Form 990 e-file XML",
                    source_url=xml_url,
                    dataset_id="irs_form_990_xml",
                    source_period=tax_period,
                    match_basis="ein_exact_latest_filing_xml",
                    confidence="high_reported_irs_xml_fields",
                )
                payload["identity"] = _financial_identity(
                    ein=ein,
                    facility_name=str(org.get("name") or ""),
                    match_basis=payload["evidence"]["match_basis"],
                    confidence=payload["evidence"]["confidence"],
                )
                payload["identity_map"] = _financial_identity_map(
                    query={"ein": ein, "tax_period": tax_period},
                    ein=ein,
                    facility_name=str(org.get("name") or ""),
                    payload=payload,
                )
                return to_structured(_attach_financial_source_metadata(payload))

        # Fallback: ProPublica summary data only
        result = Form990Details(
            ein=ein,
            name=org.get("name", ""),
            tax_period=tax_period,
            source="propublica",
            total_revenue=_safe_float(latest.get("totrevenue")),
            total_expenses=_safe_float(latest.get("totfuncexpns")),
        )
        payload = result.model_dump()
        payload["evidence"] = _financial_evidence(
            query={"ein": ein, "tax_period": tax_period},
            source_name="ProPublica Nonprofit Explorer Form 990 summary",
            source_url="https://projects.propublica.org/nonprofits/",
            dataset_id="propublica_form_990_summary",
            source_period=tax_period,
            match_basis="ein_exact_latest_filing_summary",
            confidence="summary_level_public_filing_fields",
        )
        payload["identity"] = _financial_identity(
            ein=ein,
            facility_name=str(org.get("name") or ""),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _financial_identity_map(
            query={"ein": ein, "tax_period": tax_period},
            ein=ein,
            facility_name=str(org.get("name") or ""),
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_form990_details failed")
        return error_response(f"get_form990_details failed: {e}")


# ---------------------------------------------------------------------------
# Tool 3: search_sec_filings
# Uses ACTUAL EFTS response structure: hits.hits[]._source with fields:
# adsh, display_names[], ciks[], form, file_date
# Deduplicates by adsh (each file in a filing is a separate hit)
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def search_sec_filings(query: str, filing_type: str = "10-K", date_from: str = "", date_to: str = "") -> dict[str, Any]:
    """Search SEC EDGAR filings by company name, CIK, or keyword.

    Returns a list of filings with accession numbers, filing dates, and links.

    Args:
        query: Company name, CIK number, or keyword to search.
        filing_type: SEC form type filter (e.g. "10-K", "10-Q", "8-K"). Default "10-K".
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_sec_filings","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        data = await edgar_client.search_filings(query, forms=filing_type, date_from=date_from, date_to=date_to)

        hits_obj = data.get("hits", {})
        raw_hits = hits_obj.get("hits", [])
        total_obj = hits_obj.get("total", {})
        total_count = total_obj.get("value", 0) if isinstance(total_obj, dict) else 0

        # Deduplicate by accession number (adsh) — EFTS returns one hit per file, not per filing
        seen_adsh = set()
        results = []
        for hit in raw_hits:
            source = hit.get("_source", {})
            adsh = source.get("adsh", "")
            if not adsh or adsh in seen_adsh:
                continue
            seen_adsh.add(adsh)

            # Extract company name from display_names array
            display_names = source.get("display_names", [])
            company_name = display_names[0] if display_names else ""

            # Extract CIK from ciks array
            ciks = source.get("ciks", [])
            cik = ciks[0] if ciks else ""

            # Construct filing URL from accession number and CIK
            acc_no_hyphens = adsh.replace("-", "")
            unpadded_cik = cik.lstrip("0") or "0"
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{unpadded_cik}/{acc_no_hyphens}/{adsh}-index.htm"

            results.append(SecFiling(
                accession_number=adsh,
                company_name=company_name,
                cik=cik,
                form_type=source.get("form", filing_type),
                filing_date=source.get("file_date", ""),
                filing_url=filing_url,
            ).model_dump())
            results[-1]["evidence"] = _financial_row_evidence(
                row=results[-1],
                parent_query={"query": query, "filing_type": filing_type, "date_from": date_from, "date_to": date_to},
                source_name="SEC EDGAR Full-Text Search filing row",
                source_url=filing_url,
                dataset_id="sec_edgar_filings_search",
                source_period=str(source.get("file_date") or f"{date_from or 'unbounded'} to {date_to or 'latest'}"),
                match_basis="sec_filing_search_row",
                confidence="candidate_sec_filing_match",
                next_step="Open the SEC filing index and verify CIK/accession/form before citing this row.",
            )

            if len(results) >= 25:
                break

        query_payload = {"query": query, "filing_type": filing_type, "date_from": date_from, "date_to": date_to}
        payload = {
            "total_results": total_count,
            "filings": results,
            "evidence": _financial_evidence(
                query=query_payload,
                source_name="SEC EDGAR Full-Text Search",
                source_url="https://www.sec.gov/edgar/search/",
                dataset_id="sec_edgar_filings_search",
                source_period=f"{date_from or 'unbounded'} to {date_to or 'latest'}",
                match_basis="edgar_full_text_search_no_match" if not results else "edgar_full_text_search",
                confidence="no_matching_sec_filings_returned" if not results else "candidate_sec_filing_matches",
            ),
        }
        payload["identity_map"] = _financial_identity_map(query=query_payload, payload=payload)
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("search_sec_filings failed")
        return error_response(f"search_sec_filings failed: {e}")


# ---------------------------------------------------------------------------
# Tool 4: get_sec_filing
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_sec_filing(accession_number: str, sections: list[str] | None = None) -> dict[str, Any]:
    """Get detailed data from a specific SEC filing.

    Retrieves structured XBRL financial data and/or narrative sections (MD&A,
    Risk Factors) from 10-K/10-Q filings.

    Args:
        accession_number: EDGAR accession number (e.g. "0000320193-24-000058").
        sections: Which sections to retrieve. Options: "financials", "debt", "mda", "risk_factors". Default ["financials"].

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_sec_filing","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    if sections is None:
        sections = ["financials"]

    try:
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return _financial_error_response(
                f"Could not determine CIK from accession number: {accession_number}",
                query={"accession_number": accession_number, "sections": sections},
                match_basis="accession_number_no_cik_match",
                confidence="not_found_in_sec_accession_resolution",
                source_name="SEC EDGAR company submissions and filing archive",
                source_url="https://www.sec.gov/edgar/search/",
                dataset_id="sec_edgar_filing_detail",
            )

        submissions = await edgar_client.get_company_submissions(cik)
        company_name = submissions.get("name", "")
        form_type = ""
        filing_date = ""

        recent = submissions.get("filings", {}).get("recent", {})
        accession_numbers = recent.get("accessionNumber", [])
        for i, acc in enumerate(accession_numbers):
            if acc == accession_number:
                form_type = recent.get("form", [])[i] if i < len(recent.get("form", [])) else ""
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        result = SecFilingDetail(
            accession_number=accession_number,
            company_name=company_name,
            cik=cik,
            form_type=form_type,
            filing_date=filing_date,
        )

        if "financials" in sections or "debt" in sections:
            facts = await edgar_client.get_company_facts(cik)
            if "financials" in sections:
                result.financials = edgar_client.extract_financials(facts)
            if "debt" in sections:
                result.debt_summary = edgar_client.extract_debt_summary(facts)

        if "mda" in sections or "risk_factors" in sections:
            html = await edgar_client.download_filing_html(cik, accession_number)
            if html:
                if "mda" in sections:
                    result.mda_text = edgar_client.extract_section(html, "mda")
                if "risk_factors" in sections:
                    result.risk_factors_text = edgar_client.extract_section(html, "risk_factors")

        payload = result.model_dump()
        payload["evidence"] = _financial_evidence(
            query={"accession_number": accession_number, "sections": sections},
            source_name="SEC EDGAR company submissions and filing archive",
            source_url=f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number.replace('-', '')}/{accession_number}-index.htm",
            dataset_id="sec_edgar_filing_detail",
            source_period=filing_date,
            match_basis="accession_number_exact",
            confidence="high_for_sec_accession_metadata",
        )
        payload["identity_map"] = _financial_identity_map(
            query={"accession_number": accession_number, "sections": sections},
            facility_name=company_name,
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_sec_filing failed")
        return error_response(f"get_sec_filing failed: {e}")


# ---------------------------------------------------------------------------
# Tool 5: search_muni_bonds
# Same EFTS structure, but with forms="OS"
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def search_muni_bonds(query: str, state: str = "", date_from: str = "", date_to: str = "") -> dict[str, Any]:
    """Search municipal bond offerings via SEC EDGAR Official Statements.

    Returns municipal bond filings with issuer name, filing date, and accession number.

    Args:
        query: Issuer name or keyword to search.
        state: Two-letter state code filter (e.g. "CA").
        date_from: Start date filter (YYYY-MM-DD).
        date_to: End date filter (YYYY-MM-DD).

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"search_muni_bonds","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        search_query = query
        if state:
            search_query = f"{query} {state}"

        data = await edgar_client.search_filings(search_query, forms="OS", date_from=date_from, date_to=date_to)

        hits_obj = data.get("hits", {})
        raw_hits = hits_obj.get("hits", [])
        total_obj = hits_obj.get("total", {})
        total_count = total_obj.get("value", 0) if isinstance(total_obj, dict) else 0

        seen_adsh = set()
        results = []
        for hit in raw_hits:
            source = hit.get("_source", {})
            adsh = source.get("adsh", "")
            if not adsh or adsh in seen_adsh:
                continue
            seen_adsh.add(adsh)

            display_names = source.get("display_names", [])
            issuer_name = display_names[0] if display_names else ""

            ciks = source.get("ciks", [])
            cik = ciks[0] if ciks else ""
            acc_no_hyphens = adsh.replace("-", "")
            unpadded_cik = cik.lstrip("0") or "0"
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{unpadded_cik}/{acc_no_hyphens}/{adsh}-index.htm"

            # Try to extract state from biz_locations or biz_states
            biz_states = source.get("biz_states", [])
            hit_state = biz_states[0] if biz_states else state

            results.append(MuniBond(
                accession_number=adsh,
                issuer_name=issuer_name,
                cik=cik,
                state=hit_state,
                filing_date=source.get("file_date", ""),
                filing_url=filing_url,
                source_url=filing_url,
            ).model_dump())
            results[-1]["evidence"] = _financial_row_evidence(
                row=results[-1],
                parent_query={"query": query, "state": state, "date_from": date_from, "date_to": date_to},
                source_name="SEC EDGAR municipal official statement search row",
                source_url=filing_url,
                dataset_id="sec_edgar_municipal_bond_search",
                source_period=str(source.get("file_date") or f"{date_from or 'unbounded'} to {date_to or 'latest'}"),
                match_basis="municipal_bond_search_row",
                confidence="candidate_municipal_bond_match",
                next_step="Open the SEC filing index and verify issuer/accession/document before citing this row.",
            )

            if len(results) >= 25:
                break

        query_payload = {"query": query, "state": state, "date_from": date_from, "date_to": date_to}
        payload = {
            "total_results": total_count,
            "bonds": results,
            "evidence": _financial_evidence(
                query=query_payload,
                source_name="SEC EDGAR Official Statement search",
                source_url="https://www.sec.gov/edgar/search/",
                dataset_id="sec_edgar_municipal_bond_search",
                source_period=f"{date_from or 'unbounded'} to {date_to or 'latest'}",
                match_basis="edgar_official_statement_search_no_match" if not results else "edgar_official_statement_search",
                confidence="no_matching_municipal_bond_filings_returned" if not results else "candidate_municipal_bond_matches",
            ),
        }
        payload["identity_map"] = _financial_identity_map(query=query_payload, payload=payload)
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("search_muni_bonds failed")
        return error_response(f"search_muni_bonds failed: {e}")


# ---------------------------------------------------------------------------
# Tool 6: get_muni_bond_details
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_muni_bond_details(accession_number: str) -> dict[str, Any]:
    """Get details for a specific municipal bond filing from EDGAR.

    Returns the issuer information, filing documents list, and links to
    the Official Statement PDF.

    Args:
        accession_number: EDGAR accession number for the Official Statement.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_muni_bond_details","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        cik = await edgar_client.get_cik_from_accession(accession_number)
        if not cik:
            return _financial_error_response(
                f"Could not determine CIK from accession number: {accession_number}",
                query={"accession_number": accession_number},
                match_basis="accession_number_no_cik_match",
                confidence="not_found_in_sec_accession_resolution",
                source_name="SEC EDGAR municipal bond filing archive",
                source_url="https://www.sec.gov/edgar/search/",
                dataset_id="sec_edgar_municipal_bond_detail",
            )

        submissions = await edgar_client.get_company_submissions(cik)
        issuer_name = submissions.get("name", "")
        filing_date = ""

        recent = submissions.get("filings", {}).get("recent", {})
        for i, acc in enumerate(recent.get("accessionNumber", [])):
            if acc == accession_number:
                filing_date = recent.get("filingDate", [])[i] if i < len(recent.get("filingDate", [])) else ""
                break

        index_data = await edgar_client.get_filing_index(cik, accession_number)
        source_url = index_data.get("source_url", "")
        documents = _bounded_disclosure_documents(index_data.get("documents", []), source_url=source_url)
        if not documents:
            return _financial_error_response(
                "No parseable disclosure documents found for municipal bond filing.",
                code="source_unparsed",
                detail={"accession_number": accession_number, "source_url": source_url},
                query={"accession_number": accession_number},
                match_basis="accession_number_exact_no_parseable_disclosure_documents",
                confidence="sec_index_found_no_supported_document_format",
                source_name="SEC EDGAR municipal bond filing archive",
                source_url=source_url or "https://www.sec.gov/edgar/search/",
                dataset_id="sec_edgar_municipal_bond_detail",
                source_period=filing_date,
            )
        official_statement_url = _official_statement_url(documents)
        for document in documents:
            document["evidence"] = _financial_row_evidence(
                row=document,
                parent_query={"accession_number": accession_number},
                source_name="SEC EDGAR municipal disclosure document row",
                source_url=str(document.get("url") or source_url),
                dataset_id="sec_edgar_municipal_bond_detail",
                source_period=filing_date,
                match_basis="municipal_disclosure_document_row",
                confidence="sec_index_document_metadata",
                next_step="Open the disclosure document URL and verify document type/content before citing municipal bond facts.",
            )

        result = MuniBondDetails(
            accession_number=accession_number,
            issuer_name=issuer_name,
            cik=cik,
            filing_date=filing_date,
            documents=documents,
            source_url=source_url,
            official_statement_url=official_statement_url,
            disclosure_count=len(documents),
            description=index_data.get("description", ""),
        )
        payload = result.model_dump()
        payload["evidence"] = _financial_evidence(
            query={"accession_number": accession_number},
            source_name="SEC EDGAR municipal bond filing archive",
            source_url=source_url,
            dataset_id="sec_edgar_municipal_bond_detail",
            source_period=filing_date,
            match_basis="accession_number_exact_official_statement",
            confidence="high_for_sec_accession_metadata",
        )
        payload["identity_map"] = _financial_identity_map(
            query={"accession_number": accession_number},
            facility_name=issuer_name,
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_muni_bond_details failed")
        return error_response(f"get_muni_bond_details failed: {e}")


def _bounded_disclosure_documents(documents: list[dict], limit: int = 25, source_url: str = "") -> list[dict]:
    parseable_suffixes = (".pdf", ".txt", ".xml", ".xbrl")
    bounded: list[dict] = []
    for document in documents:
        url = str(document.get("url", ""))
        name = str(document.get("name", ""))
        if not url:
            continue
        lower_url = url.lower()
        lower_name = name.lower()
        if not (lower_url.endswith(parseable_suffixes) or lower_name.endswith(parseable_suffixes)):
            continue
        normalized = dict(document)
        normalized.setdefault("source_url", source_url)
        bounded.append(normalized)
        if len(bounded) >= limit:
            break
    return bounded


def _official_statement_url(documents: list[dict]) -> str:
    for document in documents:
        haystack = " ".join(
            str(document.get(key, "")) for key in ("name", "type", "description", "url")
        ).lower()
        if "official" in haystack and "statement" in haystack:
            return str(document.get("url", ""))
    for document in documents:
        if str(document.get("url", "")).lower().endswith(".pdf"):
            return str(document.get("url", ""))
    return str(documents[0].get("url", "")) if documents else ""


# ---------------------------------------------------------------------------
# Tool 7: parse_audited_financial_pdf
# ---------------------------------------------------------------------------
@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def parse_audited_financial_pdf(url_or_path: str, entity_name: str, fiscal_year: int | str) -> dict[str, Any]:
    """Parse headline financial metrics from an audited health-system PDF.

    Extracts common balance sheet, operations, and cash-flow metrics with page
    anchors and source citation locators. Values in PDFs labeled "In Thousands"
    are returned in whole dollars.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"parse_audited_financial_pdf","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        payload = _parse_audited_financial_pdf(url_or_path, entity_name, fiscal_year)
        if isinstance(payload, dict):
            payload["evidence"] = _financial_evidence(
                query={"url_or_path": url_or_path, "entity_name": entity_name, "fiscal_year": str(fiscal_year)},
                source_name="Audited financial statement PDF",
                source_url=str(payload.get("source_url") or url_or_path),
                dataset_id="audited_financial_statement_pdf",
                source_period=str(fiscal_year),
                cache_status="direct_pdf_parse",
                cache_freshness="parsed from supplied URL or local file at request time",
                match_basis="user_supplied_audited_financial_pdf",
                confidence="document_parse_with_page_citations",
            )
            payload["identity"] = _financial_identity(
                facility_name=entity_name,
                match_basis=payload["evidence"]["match_basis"],
                confidence=payload["evidence"]["confidence"],
            )
            payload["identity_map"] = _financial_identity_map(
                query={"url_or_path": url_or_path, "entity_name": entity_name, "fiscal_year": str(fiscal_year)},
                facility_name=entity_name,
                payload=payload,
            )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("parse_audited_financial_pdf failed")
        return error_response(f"parse_audited_financial_pdf failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_public_financial_health_profile(ccn: str = "", ein: str = "", state: str = "") -> dict[str, Any]:
    """Return high-confidence public financial health fields from HCRIS, 990 Schedule H, and HFMD.

    This intentionally excludes HFMA MAP KPIs and public accounts-receivable proxies.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_public_financial_health_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        query_payload = {"ccn": ccn, "ein": ein, "state": state}
        hcris = _financial_profile_source_with_evidence("hcris", await _cost_report_public_metrics(ccn), query=query_payload)
        form990 = _financial_profile_source_with_evidence("form990_schedule_h", await _latest_990_schedule_h(ein), query=query_payload)
        hfmd = _financial_profile_source_with_evidence(
            "ahrq_hfmd",
            load_ahrq_hfmd_profile(ccn=ccn, state=state),
            query=query_payload,
        )
        joined_on = "ccn" if ccn and hfmd.get("matched_on") == "ccn" else ""
        payload = {
                "ccn": ccn,
                "ein": ein,
                "state": state.upper() if state else "",
                "hcris": hcris,
                "form990_schedule_h": form990,
                "ahrq_hfmd": hfmd,
                "join_summary": {
                    "hcris_hfmd_joined": bool(joined_on),
                    "joined_on": joined_on,
                    "ccn": ccn,
                    "hfmd_provider_id": hfmd.get("join_keys", {}).get("hfmd_provider_id", ""),
                },
                "metric_confidence": {
                    "hcris": hcris.get("metric_confidence", {}),
                    "form990_schedule_h": form990.get("metric_confidence", {}),
                    "ahrq_hfmd": hfmd.get("metric_confidence", {}),
                },
                "source_policy": "reported_public_fields_only_no_revenue_cycle_map_kpi_derivations",
        }
        payload["evidence"] = _financial_evidence(
            query=query_payload,
            match_basis="ccn_ein_public_source_join",
            confidence="metric_level_confidence",
        )
        payload["identity"] = _financial_identity(
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, hfmd, form990),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _financial_identity_map(
            query=query_payload,
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, hfmd, form990),
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_public_financial_health_profile failed")
        return error_response(f"get_public_financial_health_profile failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_uncompensated_care_profile(ccn: str = "", ein: str = "") -> dict[str, Any]:
    """Return public uncompensated-care fields from CMS S-10/HCRIS and IRS Schedule H.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_uncompensated_care_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        query_payload = {"ccn": ccn, "ein": ein}
        hcris = _financial_profile_source_with_evidence("hcris", await _cost_report_public_metrics(ccn), query=query_payload)
        form990 = _financial_profile_source_with_evidence("form990_schedule_h", await _latest_990_schedule_h(ein), query=query_payload)
        payload = {
                "ccn": ccn,
                "ein": ein,
                "uncompensated_care_cost": hcris.get("uncompensated_care_cost"),
                "charity_care_cost": _metric_value((hcris, "charity_care_cost"), (form990, "charity_care")),
                "bad_debt_expense": _metric_value((hcris, "bad_debt_expense"), (form990, "bad_debt_expense")),
                "medicare_shortfall": _metric_value((hcris, "medicare_shortfall"), (form990, "medicare_shortfall")),
                "medicaid_shortfall": _metric_value((hcris, "medicaid_shortfall"), (form990, "medicaid_shortfall")),
                "sources": {"hcris": hcris, "form990_schedule_h": form990},
                "metric_confidence": {
                    "uncompensated_care_cost": hcris.get("metric_confidence", {}).get("uncompensated_care_cost", "not_available"),
                    "charity_care_cost": _metric_confidence((hcris, "charity_care_cost"), (form990, "charity_care")),
                    "bad_debt_expense": _metric_confidence((hcris, "bad_debt_expense"), (form990, "bad_debt_expense")),
                    "medicare_shortfall": _metric_confidence((hcris, "medicare_shortfall"), (form990, "medicare_shortfall")),
                    "medicaid_shortfall": _metric_confidence((hcris, "medicaid_shortfall"), (form990, "medicaid_shortfall")),
                },
                "metric_evidence": _selected_metric_evidence_map(
                    {
                        "uncompensated_care_cost": ((hcris, "uncompensated_care_cost"),),
                        "charity_care_cost": ((hcris, "charity_care_cost"), (form990, "charity_care")),
                        "bad_debt_expense": ((hcris, "bad_debt_expense"), (form990, "bad_debt_expense")),
                        "medicare_shortfall": ((hcris, "medicare_shortfall"), (form990, "medicare_shortfall")),
                        "medicaid_shortfall": ((hcris, "medicaid_shortfall"), (form990, "medicaid_shortfall")),
                    }
                ),
                "confidence": "high_when_source_field_present",
        }
        payload["evidence"] = _financial_evidence(
            query=query_payload,
            match_basis="ccn_or_ein_public_source_lookup",
            confidence="high_when_source_field_present",
        )
        payload["identity"] = _financial_identity(
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, form990),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _financial_identity_map(
            query=query_payload,
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, form990),
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_uncompensated_care_profile failed")
        return error_response(f"get_uncompensated_care_profile failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_charity_care_profile(ein: str = "", ccn: str = "") -> dict[str, Any]:
    """Return public charity-care fields without deriving revenue-cycle MAP KPIs.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_charity_care_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        query_payload = {"ccn": ccn, "ein": ein}
        hcris = _financial_profile_source_with_evidence("hcris", await _cost_report_public_metrics(ccn), query=query_payload)
        form990 = _financial_profile_source_with_evidence("form990_schedule_h", await _latest_990_schedule_h(ein), query=query_payload)
        payload = {
                "ein": ein,
                "ccn": ccn,
                "charity_care_cost": _metric_value((hcris, "charity_care_cost"), (form990, "charity_care")),
                "community_benefit_pct": form990.get("community_benefit_pct"),
                "total_expenses": form990.get("total_expenses"),
                "sources": {"hcris": hcris, "form990_schedule_h": form990},
                "metric_confidence": {
                    "charity_care_cost": _metric_confidence((hcris, "charity_care_cost"), (form990, "charity_care")),
                    "community_benefit_pct": form990.get("metric_confidence", {}).get("community_benefit_pct", "not_available"),
                    "total_expenses": form990.get("metric_confidence", {}).get("total_expenses", "not_available"),
                },
                "metric_evidence": _selected_metric_evidence_map(
                    {
                        "charity_care_cost": ((hcris, "charity_care_cost"), (form990, "charity_care")),
                        "community_benefit_pct": ((form990, "community_benefit_pct"),),
                        "total_expenses": ((form990, "total_expenses"),),
                    }
                ),
                "confidence": "high_when_schedule_h_or_s10_field_present",
        }
        payload["evidence"] = _financial_evidence(
            query=query_payload,
            match_basis="ein_schedule_h_or_ccn_s10_lookup",
            confidence="high_when_schedule_h_or_s10_field_present",
        )
        payload["identity"] = _financial_identity(
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, form990),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _financial_identity_map(
            query=query_payload,
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, form990),
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_charity_care_profile failed")
        return error_response(f"get_charity_care_profile failed: {e}")


@mcp.tool(structured_output=True)
@observe_tool("financial-intelligence")
async def get_bad_debt_profile(ccn: str = "", ein: str = "") -> dict[str, Any]:
    """Return public bad-debt disclosures from CMS S-10/HCRIS and 990 context.

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
    {"jsonrpc":"2.0","id":"1","method":"tools/call","params":{"name":"get_bad_debt_profile","arguments":{}}}

    Common mistakes
    ---------------
    - Name supplied to exact-ID lookup: search first, then retry with the returned identifier.
    - Missing API key or cache: run hc-mcp doctor or inspect the server datasets resource.
    - Source substitution: keep each claim scoped to the source that produced it.
    """
    try:
        query_payload = {"ccn": ccn, "ein": ein}
        hcris = _financial_profile_source_with_evidence("hcris", await _cost_report_public_metrics(ccn), query=query_payload)
        form990 = _financial_profile_source_with_evidence("form990_schedule_h", await _latest_990_schedule_h(ein), query=query_payload)
        payload = {
                "ccn": ccn,
                "ein": ein,
                "bad_debt_expense": _metric_value((hcris, "bad_debt_expense"), (form990, "bad_debt_expense")),
                "uncompensated_care_cost": hcris.get("uncompensated_care_cost"),
                "sources": {"hcris": hcris, "form990_schedule_h": form990},
                "metric_confidence": {
                    "bad_debt_expense": _metric_confidence((hcris, "bad_debt_expense"), (form990, "bad_debt_expense")),
                    "uncompensated_care_cost": hcris.get("metric_confidence", {}).get("uncompensated_care_cost", "not_available"),
                },
                "metric_evidence": _selected_metric_evidence_map(
                    {
                        "bad_debt_expense": ((hcris, "bad_debt_expense"), (form990, "bad_debt_expense")),
                        "uncompensated_care_cost": ((hcris, "uncompensated_care_cost"),),
                    }
                ),
                "confidence": "high_when_source_field_present",
        }
        payload["evidence"] = _financial_evidence(
            query=query_payload,
            match_basis="ccn_or_ein_public_source_lookup",
            confidence="high_when_source_field_present",
        )
        payload["identity"] = _financial_identity(
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, form990),
            match_basis=payload["evidence"]["match_basis"],
            confidence=payload["evidence"]["confidence"],
        )
        payload["identity_map"] = _financial_identity_map(
            query=query_payload,
            ccn=ccn,
            ein=ein,
            facility_name=_financial_facility_name(hcris, form990),
            payload=payload,
        )
        return to_structured(_attach_financial_source_metadata(payload))
    except Exception as e:
        logger.exception("get_bad_debt_profile failed")
        return error_response(f"get_bad_debt_profile failed: {e}")


if __name__ == "__main__":
    mcp.run(transport=_transport)
