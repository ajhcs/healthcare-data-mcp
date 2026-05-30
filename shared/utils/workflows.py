"""Executable task-first workflow plans for healthcare-data-mcp."""

from __future__ import annotations

import json
import os
import ast
import importlib.util
import shlex
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from shared.utils.healthcare_identity import (
    IDENTITY_CANDIDATE_FIELDS,
    IDENTITY_EXACT_FIELDS,
    identity_from_public_record,
)
from shared.utils.mcp_response import REPORT_SOURCE_METADATA_FIELDS, evidence_receipt, to_structured
from shared.utils.server_registry import SERVER_BY_ID, WORKFLOW_PRESETS


WORKFLOW_SOURCE_ALIASES: dict[str, dict[str, Any]] = {
    "cms_claims_reference": {
        "canonical_dataset_ids": ("cms_medicare_claims_pufs",),
        "source_type": "public_claims_reference",
        "caveat": "Public CMS claims reference files and service-line mappings are aggregate/reference data, not PHI.",
    },
    "census": {
        "canonical_dataset_ids": ("census_acs",),
        "source_type": "public_demographics_api",
        "caveat": "Census values are public geography estimates and should remain separate from facility facts.",
    },
    "census_acs5_zcta_demographics": {
        "canonical_dataset_ids": ("census_acs",),
        "source_type": "public_demographics_api",
        "caveat": "ACS5 ZCTA demographics are modeled public estimates for geography context.",
    },
    "census_tiger_zcta_adjacency": {
        "canonical_dataset_ids": ("census_acs",),
        "source_type": "public_geography_topology",
        "caveat": "ZCTA adjacency is topology context, not service-area membership.",
    },
    "docgraph-import": {
        "canonical_dataset_ids": ("docgraph_referrals",),
        "source_type": "licensed_import_optional",
        "caveat": "DocGraph/CareSet data is separately licensed and import-only; absence blocks leakage assertions.",
    },
    "nppes": {
        "canonical_dataset_ids": ("nppes_registry",),
        "source_type": "public_provider_identity",
        "caveat": "NPPES identifies provider records but does not prove current referral relationships.",
    },
    "public_financial_health": {
        "canonical_dataset_ids": ("ahrq_hfmd", "cms_cost_report", "nj_hospital_public_data", "state_health_data"),
        "source_type": "composite_public_financial_profile",
        "caveat": "Public financial-health profiles combine source-scoped filings and public hospital artifacts by reporting period.",
    },
    "public_web": {
        "canonical_dataset_ids": ("web_intelligence",),
        "source_type": "public_web_osint",
        "caveat": "Public web pages are candidate alias/context evidence and require source review before entity merges.",
    },
    "census_geocoder": {
        "canonical_dataset_ids": ("census_acs",),
        "source_type": "public_geocoding_api",
        "caveat": "Census Geocoder coordinates and county GEOIDs are request-time geography evidence, not facility identity proof.",
    },
    "osm_nominatim": {
        "canonical_dataset_ids": ("web_intelligence",),
        "source_type": "public_geocoding_fallback",
        "caveat": "OSM/Nominatim is fallback geography evidence and requires match-quality review before persistence.",
    },
    "official_system_pages_reports": {
        "canonical_dataset_ids": ("web_intelligence",),
        "source_type": "official_public_page_or_report_review",
        "caveat": "Official system pages and reports can support exact site/facility counts or current-operator claims only when the claim text is preserved.",
    },
    "routing": {
        "canonical_dataset_ids": ("cms_hospital_general_info",),
        "source_type": "public_or_configured_routing",
        "caveat": "Routing outputs depend on public or configured routing services; use coordinates/radius as access context.",
    },
    "state_hospital_reports": {
        "canonical_dataset_ids": (
            "de_hospital_discharge",
            "nj_hospital_public_data",
            "pa_hospital_reports",
            "state_health_data",
        ),
        "source_type": "state_public_hospital_artifacts",
        "caveat": "State hospital report availability varies by state, artifact type, and reporting year.",
    },
}


@dataclass(frozen=True, slots=True)
class WorkflowToolStep:
    """One ordered tool call in a task-first workflow."""

    server: str
    tool: str
    purpose: str
    required_inputs: tuple[str, ...] = ()
    optional_inputs: tuple[str, ...] = ()
    output_contract: tuple[str, ...] = ("evidence", "source_metadata", "match_basis", "confidence", "caveat")
    required_env: tuple[str, ...] = ()
    optional_env: tuple[str, ...] = ()
    required_sources: tuple[str, ...] = ()
    blocking: bool = True
    execution_notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    """User-facing workflow definition and execution plan metadata."""

    workflow_id: str
    title: str
    description: str
    required_identifiers: tuple[str, ...]
    recommended_servers: tuple[str, ...]
    required_sources: tuple[str, ...]
    steps: tuple[WorkflowToolStep, ...]
    caveats: tuple[str, ...]
    identity_join_keys: tuple[str, ...] = ()
    identity_strategy: tuple[str, ...] = ()
    report_fact_rows: tuple[dict[str, Any], ...] = field(default_factory=tuple)


WORKFLOW_DEFINITIONS: dict[str, WorkflowDefinition] = {
    "compliance_exclusion_screening": WorkflowDefinition(
        workflow_id="compliance_exclusion_screening",
        title="Compliance And Exclusion Screening",
        description="Screen a provider, owner, vendor, or organization against public exclusion sources.",
        required_identifiers=("npi or entity_name",),
        recommended_servers=WORKFLOW_PRESETS["compliance_exclusion_screening"],
        required_sources=("hhs_oig_leie", "sam_gov_exclusions", "cms_pecos_public_provider_enrollment"),
        steps=(
            WorkflowToolStep(
                "public-records",
                "get_leie_metadata",
                "Verify LEIE cache/source freshness before screening.",
                required_sources=("hhs_oig_leie",),
            ),
            WorkflowToolStep(
                "public-records",
                "check_leie_npi",
                "Run exact NPI screening when NPI is available.",
                required_inputs=("npi",),
                required_sources=("hhs_oig_leie",),
            ),
            WorkflowToolStep(
                "public-records",
                "search_leie_entity",
                "Run conservative entity-name screening when no exact NPI is available.",
                required_inputs=("entity_name",),
                optional_inputs=("state",),
                required_sources=("hhs_oig_leie",),
            ),
            WorkflowToolStep(
                "public-records",
                "search_sam_exclusions",
                "Run active SAM.gov Exclusions screening when SAM_GOV_API_KEY is configured.",
                optional_inputs=("entity_name", "npi", "uei", "cage_code", "state"),
                required_env=("SAM_GOV_API_KEY",),
                required_sources=("sam_gov_exclusions",),
                blocking=False,
                execution_notes=("Optional federal exclusions screen; unavailable without SAM_GOV_API_KEY.",),
            ),
            WorkflowToolStep(
                "provider-enrollment",
                "search_provider_enrollment",
                "Resolve public Medicare enrollment identifiers for follow-up joins.",
                optional_inputs=("npi", "provider_name", "state", "provider_type"),
                required_sources=("cms_pecos_public_provider_enrollment",),
            ),
        ),
        caveats=(
            "A zero-result LEIE or SAM.gov response is not legal clearance.",
            "Name matches are potential matches until verified through the official source and documented follow-up process.",
            "Do not submit SSN, EIN, TIN, or other sensitive tax identifiers to these tools.",
        ),
        identity_join_keys=("npi", "ccn", "entity_name", "state", "uei", "cage_code", "pecos_enrollment_id", "owner_id"),
        identity_strategy=(
            "Use NPI as the strongest screening join key when available.",
            "Use UEI and CAGE code as exact SAM.gov search identifiers when supplied, but keep them source-scoped until official records are reviewed.",
            "Use provider-enrollment outputs to resolve PECOS enrollment and owner identifiers before ownership follow-up.",
            "Keep name-only LEIE/SAM matches as candidate aliases until official-source verification is documented.",
        ),
        report_fact_rows=(
            {
                "label": "LEIE screening status",
                "value_path": "public_records.check_leie_npi.status",
                "required_evidence": "hhs_oig_leie receipt",
                "identity_fields": ("npi", "canonical_name"),
            },
            {
                "label": "LEIE entity-name potential matches",
                "value_path": "public_records.search_leie_entity.records",
                "required_evidence": "hhs_oig_leie receipt",
                "identity_fields": ("entity_name", "state", "npi"),
            },
            {
                "label": "SAM.gov exclusion status",
                "value_path": "public_records.search_sam_exclusions.records",
                "required_evidence": "sam_gov_exclusions receipt",
                "identity_fields": ("entity_name", "state", "npi", "uei", "cage_code"),
            },
            {
                "label": "PECOS enrollment join keys",
                "value_path": "provider_enrollment.search_provider_enrollment.enrollments",
                "required_evidence": "cms_pecos_public_provider_enrollment receipt",
                "identity_fields": ("npi", "pecos_enrollment_id", "ccn"),
            },
        ),
    ),
    "facility_profile": WorkflowDefinition(
        workflow_id="facility_profile",
        title="Facility Profile",
        description="Resolve one hospital facility across CMS master data, system affiliation, service-area, and public operating context.",
        required_identifiers=("ccn or facility_name",),
        recommended_servers=WORKFLOW_PRESETS["facility_profile"],
        required_sources=("cms_provider_of_services", "cms_hospital_general_info", "ahrq_health_system_compendium", "cms_hsaf"),
        steps=(
            WorkflowToolStep(
                "cms-facility",
                "get_facility",
                "Resolve the canonical CMS facility record and address identifiers.",
                required_inputs=("ccn",),
                required_sources=("cms_provider_of_services", "cms_hospital_general_info"),
            ),
            WorkflowToolStep(
                "health-system-profiler",
                "get_system_facilities",
                "Find AHRQ/system-linked facility context when an exact system identifier is known.",
                optional_inputs=("system_id", "facility_type"),
                required_sources=("ahrq_health_system_compendium", "cms_provider_of_services"),
                blocking=False,
                execution_notes=(
                    "System context is optional; provide an exact AHRQ system_id before calling this step.",
                    "Do not infer affiliation from facility name or CCN alone.",
                ),
            ),
            WorkflowToolStep(
                "service-area",
                "compute_service_area",
                "Build CMS HSAF ZIP/service-area context for the facility.",
                required_inputs=("ccn",),
                required_sources=("cms_hsaf",),
            ),
            WorkflowToolStep(
                "workforce-analytics",
                "resolve_hospital_beds",
                "Cross-check public bed/source context for operating profile denominators.",
                required_inputs=("ccn",),
                required_sources=("cms_cost_report",),
                blocking=False,
                execution_notes=("Use bed counts as public operating context, not current licensed-bed verification.",),
            ),
        ),
        caveats=(
            "CCN is the primary facility identity key; facility names and addresses are aliases or conflict checks.",
            "System affiliation and service-area context are source-scoped public records, not proof of current ownership or market share outside the cited period.",
            "Bed/source context can vary by source and reporting period; preserve each tool's evidence receipt.",
        ),
        identity_join_keys=("ccn", "npi", "ahrq_system_id", "facility_name", "address", "zip_code", "state"),
        identity_strategy=(
            "Resolve the CMS facility record first and carry CCN, normalized name, address, ZIP, and NPI forward.",
            "Use AHRQ system IDs only for system-affiliation joins; keep system names as aliases unless identifiers agree.",
            "Use service-area and bed records as context keyed by CCN, not as substitute identity proof.",
        ),
        report_fact_rows=(
            {
                "label": "Facility master identity",
                "value_path": "cms_facility.get_facility.identity",
                "required_evidence": "cms_provider_of_services receipt",
                "identity_fields": ("ccn", "npi", "canonical_name", "address", "zip_code"),
            },
            {
                "label": "System affiliation context",
                "value_path": "health_system_profiler.get_system_facilities.inpatient_facilities",
                "required_evidence": "ahrq_health_system_compendium receipt",
                "identity_fields": ("ccn", "ahrq_system_id", "canonical_name"),
            },
            {
                "label": "Service-area context",
                "value_path": "service_area.compute_service_area",
                "required_evidence": "cms_hsaf receipt",
                "identity_fields": ("ccn", "zip_code"),
            },
            {
                "label": "Public bed denominator",
                "value_path": "workforce_analytics.resolve_hospital_beds",
                "required_evidence": "cms_cost_report receipt",
                "identity_fields": ("ccn",),
            },
        ),
    ),
    "quality_profile": WorkflowDefinition(
        workflow_id="quality_profile",
        title="Quality Profile",
        description="Build a source-backed hospital quality profile with summary ratings and exact CMS measure rows.",
        required_identifiers=("ccn",),
        recommended_servers=WORKFLOW_PRESETS["quality_profile"],
        required_sources=("cms_hospital_general_info", "cms_hospital_quality"),
        steps=(
            WorkflowToolStep(
                "cms-facility",
                "get_facility",
                "Resolve the facility identity before citing quality facts.",
                required_inputs=("ccn",),
                required_sources=("cms_hospital_general_info",),
            ),
            WorkflowToolStep(
                "hospital-quality",
                "get_quality_scores",
                "Fetch CMS Hospital General Information quality summary ratings.",
                required_inputs=("ccn",),
                required_sources=("cms_hospital_quality", "cms_hospital_general_info"),
            ),
            WorkflowToolStep(
                "hospital-quality",
                "get_readmission_data",
                "Fetch HRRP readmission context as adjacent program data.",
                required_inputs=("ccn",),
                required_sources=("cms_hospital_quality",),
            ),
            WorkflowToolStep(
                "hospital-quality",
                "get_safety_scores",
                "Fetch HAC safety summary context.",
                required_inputs=("ccn",),
                required_sources=("cms_hospital_quality",),
            ),
            WorkflowToolStep(
                "hospital-quality",
                "get_patient_experience",
                "Fetch HCAHPS patient-experience context.",
                required_inputs=("ccn",),
                required_sources=("cms_hospital_quality",),
            ),
            WorkflowToolStep(
                "hospital-quality",
                "get_quality_measure_rows",
                "Fetch exact CMS row-level measures for any named report claims.",
                required_inputs=("ccn", "measure or measure_id"),
                required_sources=("cms_hospital_quality",),
            ),
        ),
        caveats=(
            "Use summary quality tools for context and get_quality_measure_rows for named CMS measure facts.",
            "HRRP, HAC, and HCAHPS program outputs are adjacent context unless their exact evidence receipt supports the report claim.",
            "Do not substitute PHC4, HRRP condition rows, or HAC totals for exact CMS mortality, hospital-wide readmission, or HAI measure rows.",
        ),
        identity_join_keys=("ccn", "measure_id", "facility_name", "address", "zip_code"),
        identity_strategy=(
            "Anchor all quality facts to the resolved facility CCN.",
            "Use measure_id as a second exact key for row-level quality claims.",
            "Carry source periods and cache freshness separately for each CMS quality program file.",
        ),
        report_fact_rows=(
            {
                "label": "Facility identity for quality profile",
                "value_path": "cms_facility.get_facility.identity",
                "required_evidence": "cms_hospital_general_info receipt",
                "identity_fields": ("ccn", "canonical_name", "address", "zip_code"),
            },
            {
                "label": "CMS summary quality rating",
                "value_path": "hospital_quality.get_quality_scores",
                "required_evidence": "cms_hospital_quality receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "CMS readmission context",
                "value_path": "hospital_quality.get_readmission_data.conditions",
                "required_evidence": "cms_hospital_quality receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "CMS HAC safety domain context",
                "value_path": "hospital_quality.get_safety_scores.domain_evidence",
                "required_evidence": "cms_hospital_quality receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "CMS HCAHPS patient-experience domain context",
                "value_path": "hospital_quality.get_patient_experience.domains",
                "required_evidence": "cms_hospital_quality receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "Exact CMS quality measure row",
                "value_path": "hospital_quality.get_quality_measure_rows.rows",
                "required_evidence": "cms_hospital_quality receipt",
                "identity_fields": ("ccn", "measure_id"),
            },
        ),
    ),
    "finance_profile": WorkflowDefinition(
        workflow_id="finance_profile",
        title="Finance Profile",
        description="Build a public financial profile for a hospital or health-system entity with source-period and identity caveats.",
        required_identifiers=("ccn or entity_name",),
        recommended_servers=WORKFLOW_PRESETS["finance_profile"],
        required_sources=("public_financial_health", "cms_cost_report"),
        steps=(
            WorkflowToolStep(
                "financial-intelligence",
                "get_public_financial_health_profile",
                "Fetch configured public financial-health profile fields.",
                required_inputs=("ccn",),
                optional_inputs=("ein", "state"),
                required_env=("SEC_USER_AGENT",),
                required_sources=("public_financial_health",),
            ),
            WorkflowToolStep(
                "financial-intelligence",
                "get_uncompensated_care_profile",
                "Fetch source-backed uncompensated-care, charity-care, bad-debt, and shortfall fields.",
                optional_inputs=("ccn", "ein"),
                required_env=("SEC_USER_AGENT",),
                required_sources=("public_financial_health",),
                blocking=False,
                execution_notes=(
                    "Use promoted metric_evidence receipts before citing top-level uncompensated-care profile fields.",
                ),
            ),
            WorkflowToolStep(
                "financial-intelligence",
                "get_charity_care_profile",
                "Fetch source-backed charity-care and community-benefit fields.",
                optional_inputs=("ccn", "ein"),
                required_env=("SEC_USER_AGENT",),
                required_sources=("public_financial_health",),
                blocking=False,
                execution_notes=("Use top-level metric_evidence receipts for promoted charity-care profile metrics.",),
            ),
            WorkflowToolStep(
                "financial-intelligence",
                "get_bad_debt_profile",
                "Fetch source-backed public bad-debt and uncompensated-care context.",
                optional_inputs=("ccn", "ein"),
                required_env=("SEC_USER_AGENT",),
                required_sources=("public_financial_health",),
                blocking=False,
                execution_notes=("Use top-level metric_evidence receipts for promoted bad-debt profile metrics.",),
            ),
            WorkflowToolStep(
                "hospital-quality",
                "get_financial_profile",
                "Fetch CMS cost-report-derived hospital operating and teaching context.",
                required_inputs=("ccn",),
                required_sources=("cms_cost_report",),
            ),
            WorkflowToolStep(
                "workforce-analytics",
                "get_public_throughput_profile",
                "Fetch public throughput context for financial denominator review.",
                required_inputs=("ccn",),
                required_sources=("state_hospital_reports", "cms_cost_report"),
                blocking=False,
                execution_notes=("State throughput availability varies; absence is a source-coverage note, not zero activity.",),
            ),
        ),
        caveats=(
            "Public financial records vary by filing entity, reporting period, and source coverage.",
            "Do not infer current operating performance from stale filings or adjacent system-level records.",
            "SEC/IRS/state documents should be cited with source URLs, accession/EIN/query basis, and cache/source freshness.",
        ),
        identity_join_keys=("ccn", "entity_name", "canonical_name", "npi", "address", "zip_code"),
        identity_strategy=(
            "Use CCN for hospital cost-report and throughput joins.",
            "Use entity_name/canonical_name only as a financial-filing search alias unless an exact identifier supports the join.",
            "Keep hospital-level and system-level financial records separate when public sources do not prove common reporting entity.",
        ),
        report_fact_rows=(
            {
                "label": "Public financial health profile",
                "value_path": "financial_intelligence.get_public_financial_health_profile",
                "required_evidence": "public_financial_health receipt",
                "identity_fields": ("ccn", "canonical_name"),
            },
            {
                "label": "Public financial source metric",
                "value_path": "financial_intelligence.get_public_financial_health_profile.hcris.metrics",
                "required_evidence": "public_financial_health metric receipt",
                "identity_fields": ("ccn", "canonical_name"),
                "evidence_path": "financial_intelligence.get_public_financial_health_profile.hcris.metric_evidence",
                "source_metadata_path": "financial_intelligence.get_public_financial_health_profile.hcris.source_metadata",
            },
            {
                "label": "Promoted uncompensated-care metric",
                "value_path": "financial_intelligence.get_uncompensated_care_profile.metric_confidence",
                "required_evidence": "public_financial_health promoted metric receipt",
                "identity_fields": ("ccn", "canonical_name"),
                "evidence_path": "financial_intelligence.get_uncompensated_care_profile.metric_evidence",
                "source_metadata_path": "financial_intelligence.get_uncompensated_care_profile.evidence",
            },
            {
                "label": "Promoted charity-care metric",
                "value_path": "financial_intelligence.get_charity_care_profile.metric_confidence",
                "required_evidence": "public_financial_health promoted metric receipt",
                "identity_fields": ("ccn", "canonical_name"),
                "evidence_path": "financial_intelligence.get_charity_care_profile.metric_evidence",
                "source_metadata_path": "financial_intelligence.get_charity_care_profile.evidence",
            },
            {
                "label": "Promoted bad-debt metric",
                "value_path": "financial_intelligence.get_bad_debt_profile.metric_confidence",
                "required_evidence": "public_financial_health promoted metric receipt",
                "identity_fields": ("ccn", "canonical_name"),
                "evidence_path": "financial_intelligence.get_bad_debt_profile.metric_evidence",
                "source_metadata_path": "financial_intelligence.get_bad_debt_profile.evidence",
            },
            {
                "label": "CMS cost-report operating context",
                "value_path": "hospital_quality.get_financial_profile",
                "required_evidence": "cms_cost_report receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "Public throughput denominator",
                "value_path": "workforce_analytics.get_public_throughput_profile",
                "required_evidence": "state_hospital_reports receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "Public throughput metric",
                "value_path": "workforce_analytics.get_public_throughput_profile.metric_confidence",
                "required_evidence": "public_hospital_throughput metric receipt",
                "identity_fields": ("ccn",),
                "evidence_path": "workforce_analytics.get_public_throughput_profile.metric_evidence",
                "source_metadata_path": "workforce_analytics.get_public_throughput_profile.evidence",
            },
        ),
    ),
    "hospital_competitive_profile": WorkflowDefinition(
        workflow_id="hospital_competitive_profile",
        title="Hospital Competitive Profile",
        description="Build a source-backed facility profile across identity, quality, finance, workforce, and claims context.",
        required_identifiers=("ccn",),
        recommended_servers=WORKFLOW_PRESETS["hospital_competitive_profile"],
        required_sources=("cms_hospital_general_info", "cms_hospital_quality", "cms_cost_report", "ahrq_health_system_compendium"),
        steps=(
            WorkflowToolStep("cms-facility", "get_facility", "Resolve the facility master record.", required_inputs=("ccn",), required_sources=("cms_provider_of_services",)),
            WorkflowToolStep(
                "health-system-profiler",
                "get_system_facilities",
                "Find system-linked facilities when an exact AHRQ system identifier is known.",
                optional_inputs=("system_id", "facility_type"),
                required_sources=("ahrq_health_system_compendium",),
                blocking=False,
                execution_notes=("System affiliation is optional; do not call this step without an exact AHRQ system_id.",),
            ),
            WorkflowToolStep("hospital-quality", "get_quality_scores", "Fetch CMS summary quality ratings.", required_inputs=("ccn",), required_sources=("cms_hospital_quality",)),
            WorkflowToolStep("hospital-quality", "get_quality_measure_rows", "Fetch exact CMS row-level measures for named measures.", required_inputs=("ccn", "measure"), required_sources=("cms_hospital_quality",)),
            WorkflowToolStep("financial-intelligence", "get_public_financial_health_profile", "Fetch public financial health fields.", required_inputs=("ccn",), required_env=("SEC_USER_AGENT",), required_sources=("public_financial_health",)),
            WorkflowToolStep("workforce-analytics", "get_hospital_staffing_productivity", "Fetch public staffing productivity metrics.", required_inputs=("ccn",), required_sources=("cms_cost_report",)),
        ),
        caveats=(
            "Exact CMS measure rows should be used for named quality measure assertions.",
            "Public financial/workforce fields vary by reporting year and source availability.",
        ),
        identity_join_keys=("ccn", "npi", "ahrq_system_id", "facility_name", "address", "zip_code"),
        identity_strategy=(
            "Use CCN as the primary cross-server facility key for facility, quality, finance, workforce, and claims tools.",
            "Use health-system-profiler/AHRQ system IDs only for system affiliation, not facility identity proof.",
            "Treat name/address joins as conflict checks or candidates when CCN is absent.",
        ),
        report_fact_rows=(
            {
                "label": "Facility identity",
                "value_path": "cms_facility.get_facility.identity",
                "required_evidence": "cms_provider_of_services receipt",
                "identity_fields": ("ccn", "npi", "canonical_name", "address", "zip_code"),
            },
            {
                "label": "System affiliation",
                "value_path": "health_system_profiler.get_system_facilities.inpatient_facilities",
                "required_evidence": "ahrq_health_system_compendium receipt",
                "identity_fields": ("ccn", "ahrq_system_id", "canonical_name"),
            },
            {
                "label": "CMS quality rating",
                "value_path": "hospital_quality.get_quality_scores.summary",
                "required_evidence": "cms_hospital_quality receipt",
                "evidence_path": "hospital_quality.get_quality_scores.evidence",
                "identity_fields": ("ccn",),
            },
            {
                "label": "Public financial profile",
                "value_path": "financial_intelligence.get_public_financial_health_profile",
                "required_evidence": "public_financial_health receipt",
                "identity_fields": ("ccn", "canonical_name"),
            },
            {
                "label": "Public financial source metric",
                "value_path": "financial_intelligence.get_public_financial_health_profile.hcris.metrics",
                "required_evidence": "public_financial_health metric receipt",
                "identity_fields": ("ccn", "canonical_name"),
                "evidence_path": "financial_intelligence.get_public_financial_health_profile.hcris.metric_evidence",
                "source_metadata_path": "financial_intelligence.get_public_financial_health_profile.hcris.source_metadata",
            },
            {
                "label": "Staffing productivity",
                "value_path": "workforce_analytics.get_hospital_staffing_productivity.departments",
                "required_evidence": "cms_cost_report receipt",
                "identity_fields": ("ccn",),
            },
        ),
    ),
    "ownership_chow_trace": WorkflowDefinition(
        workflow_id="ownership_chow_trace",
        title="Ownership And CHOW Trace",
        description="Trace CMS PECOS owner/managing-control rows and change-of-ownership history.",
        required_identifiers=("ccn or facility_name",),
        recommended_servers=WORKFLOW_PRESETS["ownership_chow_trace"],
        required_sources=("cms_pecos_hospital_owners", "cms_pecos_hospital_chow", "cms_provider_of_services"),
        steps=(
            WorkflowToolStep(
                "cms-facility",
                "get_facility",
                "Resolve the canonical CMS facility identity before joining ownership or CHOW rows.",
                required_inputs=("ccn",),
                required_sources=("cms_provider_of_services", "cms_hospital_general_info"),
            ),
            WorkflowToolStep(
                "health-system-profiler",
                "get_system_facilities",
                "Add AHRQ system-affiliation context when a system identifier is known.",
                optional_inputs=("system_id", "facility_type"),
                required_sources=("ahrq_health_system_compendium", "cms_provider_of_services"),
                blocking=False,
                execution_notes=("Use system context as affiliation context only; do not infer ownership from AHRQ linkage.",),
            ),
            WorkflowToolStep("provider-enrollment", "get_facility_ownership", "Fetch active owner/control rows.", optional_inputs=("ccn", "facility_name", "state"), required_sources=("cms_pecos_hospital_owners",)),
            WorkflowToolStep("provider-enrollment", "search_change_of_ownership", "Fetch CHOW history.", optional_inputs=("ccn", "facility_name", "state", "start_date", "end_date"), required_sources=("cms_pecos_hospital_chow",)),
            WorkflowToolStep(
                "provider-enrollment",
                "profile_provider_control",
                "Build a compact enrollment, ownership, CHOW, and owner-network control profile.",
                optional_inputs=("ccn", "npi"),
                required_sources=(
                    "cms_pecos_public_provider_enrollment",
                    "cms_pecos_hospital_owners",
                    "cms_pecos_hospital_chow",
                ),
                blocking=False,
                execution_notes=("Use the profile as a consolidated receipt set; keep atomic owner and CHOW rows for exact source-backed assertions.",),
            ),
            WorkflowToolStep("provider-enrollment", "trace_owner_network", "Trace bounded owner network.", optional_inputs=("owner_name", "owner_associate_id", "state"), required_sources=("cms_pecos_hospital_owners",), execution_notes=("Bound owner traversal and preserve owner IDs to avoid name-only merges.",)),
            WorkflowToolStep(
                "public-records",
                "search_sam_exclusions",
                "Optionally screen the owner/entity name against SAM.gov Exclusions when configured.",
                optional_inputs=("entity_name", "npi", "uei", "cage_code", "state"),
                required_env=("SAM_GOV_API_KEY",),
                required_sources=("sam_gov_exclusions",),
                blocking=False,
                execution_notes=("A no-result SAM.gov search is not legal clearance; preserve the exact query and caveat.",),
            ),
        ),
        caveats=(
            "Owner names are not unique; preserve owner identifiers and match basis in reports.",
            "System affiliation and public exclusion context are separate evidence domains, not ownership proof.",
        ),
        identity_join_keys=(
            "ccn",
            "npi",
            "ahrq_system_id",
            "facility_name",
            "address",
            "zip_code",
            "state",
            "pecos_enrollment_id",
            "owner_id",
            "owner_name",
            "entity_name",
        ),
        identity_strategy=(
            "Resolve the CMS facility record first and use CCN to anchor owner, CHOW, and optional public-record context.",
            "Use AHRQ system IDs only as system-affiliation context; do not treat system linkage as ownership proof.",
            "Use PECOS enrollment and owner associate IDs as relationship keys; names are aliases, not proof.",
            "Record owner-name conflicts separately instead of merging owners by name alone.",
        ),
        report_fact_rows=(
            {
                "label": "Facility identity",
                "value_path": "cms_facility.get_facility.identity",
                "required_evidence": "cms_provider_of_services receipt",
                "identity_fields": ("ccn", "npi", "canonical_name", "address", "zip_code"),
            },
            {
                "label": "System affiliation context",
                "value_path": "health_system_profiler.get_system_facilities.inpatient_facilities",
                "required_evidence": "ahrq_health_system_compendium receipt",
                "identity_fields": ("ccn", "ahrq_system_id", "canonical_name"),
            },
            {
                "label": "Active owner/control rows",
                "value_path": "provider_enrollment.get_facility_ownership.owners",
                "required_evidence": "cms_pecos_hospital_owners receipt",
                "identity_fields": ("ccn", "pecos_enrollment_id", "owner_id", "owner_name"),
            },
            {
                "label": "CHOW history",
                "value_path": "provider_enrollment.search_change_of_ownership.events",
                "required_evidence": "cms_pecos_hospital_chow receipt",
                "identity_fields": ("ccn", "pecos_enrollment_id", "owner_id"),
            },
            {
                "label": "Provider-control ownership profile",
                "value_path": "provider_enrollment.profile_provider_control.ownership",
                "required_evidence": "cms_pecos_hospital_owners receipt",
                "identity_fields": ("ccn", "npi", "pecos_enrollment_id", "owner_id", "owner_name"),
                "evidence_path": "provider_enrollment.profile_provider_control.ownership[].evidence",
                "identity_map_path": "provider_enrollment.profile_provider_control.identity_map",
            },
            {
                "label": "Provider-control owner network",
                "value_path": "provider_enrollment.profile_provider_control.owner_network.nodes",
                "required_evidence": "cms_pecos_hospital_owners receipt",
                "identity_fields": ("ccn", "npi", "owner_id", "owner_name"),
                "evidence_path": "provider_enrollment.profile_provider_control.owner_network.nodes[].evidence",
                "identity_map_path": "provider_enrollment.profile_provider_control.identity_map",
            },
            {
                "label": "Owner/entity public exclusion context",
                "value_path": "public_records.search_sam_exclusions.status",
                "required_evidence": "sam_gov_exclusions receipt",
                "identity_fields": ("entity_name", "npi", "state"),
            },
        ),
    ),
    "system_reconciliation": WorkflowDefinition(
        workflow_id="system_reconciliation",
        title="Health System Reconciliation",
        description=(
            "Resolve a health system across AHRQ Compendium, CMS facility records, "
            "PECOS enrollment identifiers, and public web aliases without merging on names alone."
        ),
        required_identifiers=("query or system_name or system_id or system_slug or ccn",),
        recommended_servers=WORKFLOW_PRESETS["system_reconciliation"],
        required_sources=(
            "ahrq_health_system_compendium",
            "cms_hospital_general_info",
            "cms_pecos_public_provider_enrollment",
        ),
        steps=(
            WorkflowToolStep(
                "health-system-profiler",
                "search_health_systems",
                "Find candidate AHRQ health-system records by public system name.",
                required_inputs=("query",),
                optional_inputs=("limit",),
                required_sources=("ahrq_health_system_compendium",),
            ),
            WorkflowToolStep(
                "health-system-profiler",
                "get_system_profile",
                "Resolve the AHRQ system profile and linked hospital CCNs.",
                optional_inputs=("system_id", "system_name", "edition_date"),
                required_sources=("ahrq_health_system_compendium", "cms_hospital_general_info"),
            ),
            WorkflowToolStep(
                "health-system-profiler",
                "reconcile_system_facilities",
                "Return the reconciliation ledger for reviewed merger rules or generic AHRQ/CMS joins.",
                required_inputs=("system_slug",),
                optional_inputs=("as_of_date",),
                required_sources=("ahrq_health_system_compendium", "cms_hospital_general_info"),
            ),
            WorkflowToolStep(
                "cms-facility",
                "search_facilities",
                "Check CMS facility candidates by resolved name/state when CCNs are incomplete.",
                optional_inputs=("name", "state", "limit"),
                required_sources=("cms_hospital_general_info",),
            ),
            WorkflowToolStep(
                "provider-enrollment",
                "search_provider_enrollment",
                "Cross-check PECOS enrollment identifiers for resolved facilities or organization names.",
                optional_inputs=("npi", "provider_name", "state", "provider_type"),
                required_sources=("cms_pecos_public_provider_enrollment",),
            ),
            WorkflowToolStep(
                "web-intelligence",
                "scrape_system_profile",
                "Collect public-web aliases and source URLs as candidate context only.",
                required_inputs=("system_name",),
                optional_inputs=("system_domain",),
                required_sources=("public_web",),
                blocking=False,
                execution_notes=("Use web output as alias/context evidence; do not assert affiliation from marketing pages alone.",),
            ),
        ),
        caveats=(
            "AHRQ system IDs, CCNs, NPIs, and PECOS enrollment IDs outrank names and web aliases for merges.",
            "Public web pages are candidate alias evidence only and may be stale or promotional.",
            "For mergers, preserve as-of dates and source edition dates before reporting a combined system roster.",
        ),
        identity_join_keys=(
            "ahrq_system_id",
            "system_id",
            "system_slug",
            "ccn",
            "npi",
            "pecos_enrollment_id",
            "canonical_name",
            "state",
            "address",
            "zip_code",
        ),
        identity_strategy=(
            "Resolve AHRQ system candidates first, then use the reconciliation ledger to produce candidate CCNs.",
            "Use CCN/NPI/PECOS identifiers to join facility and enrollment facts; keep system names and web domains as aliases.",
            "Treat missing CMS/PECOS rows as source-scoped no-match evidence, not proof that an affiliation or enrollment does not exist.",
            "Record conflicts when AHRQ, CMS, PECOS, and web names disagree rather than overwriting the identity map.",
        ),
        report_fact_rows=(
            {
                "label": "AHRQ system candidate",
                "value_path": "health_system_profiler.search_health_systems.results",
                "required_evidence": "ahrq_health_system_compendium receipt",
                "identity_fields": ("ahrq_system_id", "canonical_name", "state"),
            },
            {
                "label": "System facility reconciliation ledger",
                "value_path": "health_system_profiler.reconcile_system_facilities.facilities",
                "required_evidence": "ahrq_health_system_compendium receipt",
                "identity_fields": ("ahrq_system_id", "ccn", "canonical_name", "address", "zip_code"),
            },
            {
                "label": "CMS facility candidate",
                "value_path": "cms_facility.search_facilities.results",
                "required_evidence": "cms_hospital_general_info receipt",
                "identity_fields": ("ccn", "npi", "canonical_name", "address", "zip_code"),
            },
            {
                "label": "PECOS enrollment cross-check",
                "value_path": "provider_enrollment.search_provider_enrollment.enrollments",
                "required_evidence": "cms_pecos_public_provider_enrollment receipt",
                "identity_fields": ("ccn", "npi", "pecos_enrollment_id", "canonical_name"),
            },
            {
                "label": "Public web alias context",
                "value_path": "web_intelligence.scrape_system_profile.locations",
                "required_evidence": "public_web_system_profile receipt",
                "identity_fields": ("canonical_name", "system_slug"),
            },
        ),
    ),
    "profile_evidence_pack": WorkflowDefinition(
        workflow_id="profile_evidence_pack",
        title="Profile Evidence Pack",
        description=(
            "Return a read-only source-backed evidence pack for Healthcare Toolkit health-system profile population, "
            "including facility roster, identifiers, geography, beds, affiliation review, count evidence, conflicts, and unavailable-public findings."
        ),
        required_identifiers=("state",),
        recommended_servers=WORKFLOW_PRESETS["profile_evidence_pack"],
        required_sources=(
            "cms_provider_of_services",
            "cms_hospital_general_info",
            "ahrq_health_system_compendium",
            "cms_cost_report",
            "state_hospital_reports",
            "census_geocoder",
            "osm_nominatim",
            "official_system_pages_reports",
            "cms_pecos_public_provider_enrollment",
            "cms_pecos_hospital_chow",
        ),
        steps=(
            WorkflowToolStep(
                "cache-manager",
                "get_workflow_cache_readiness",
                "Preflight cache readiness and source blockers before treating missing public rows as unavailable.",
                optional_inputs=("workflow_id", "inputs"),
                required_sources=(
                    "cms_provider_of_services",
                    "cms_hospital_general_info",
                    "ahrq_health_system_compendium",
                    "cms_cost_report",
                    "state_hospital_reports",
                ),
            ),
            WorkflowToolStep(
                "health-system-profiler",
                "build_profile_evidence_pack",
                "Assemble the read-only structured profile evidence pack for Healthcare Toolkit ingestion/review using configured public caches and reviewed official rows when available.",
                required_inputs=("state",),
                optional_inputs=("system_slug", "system_name", "ccns", "required_fields"),
                required_sources=(
                    "cms_provider_of_services",
                    "cms_hospital_general_info",
                    "ahrq_health_system_compendium",
                    "cms_cost_report",
                    "state_hospital_reports",
                    "census_geocoder",
                    "osm_nominatim",
                    "official_system_pages_reports",
                    "cms_pecos_public_provider_enrollment",
                    "cms_pecos_hospital_chow",
                ),
            ),
            WorkflowToolStep(
                "provider-enrollment",
                "profile_provider_control",
                "Optional exact-CCN cross-check for current enrollment, ownership, and CHOW evidence.",
                optional_inputs=("ccn", "npi"),
                required_sources=("cms_pecos_public_provider_enrollment", "cms_pecos_hospital_chow"),
                blocking=False,
                execution_notes=("Use exact CCN/NPI only; do not infer current control from names.",),
            ),
            WorkflowToolStep(
                "web-intelligence",
                "scrape_system_profile",
                "Optional official/public page collection for exact count and current-operator claim review.",
                required_inputs=("system_name",),
                optional_inputs=("system_domain",),
                required_sources=("official_system_pages_reports", "public_web"),
                blocking=False,
                execution_notes=("Persist exact official counts only; vague count claims remain needs_review.",),
            ),
        ),
        caveats=(
            "This MCP workflow never writes to Healthcare Toolkit; it returns evidence candidates and review findings only.",
            "Do not estimate missing fields. Return unavailable_public, source_conflict, or needs_review with searched-source evidence.",
            "AHRQ is linkage/discovery context, not final current-operator authority.",
            "OSM/Nominatim is a fallback only when Census Geocoder does not produce an acceptable match.",
        ),
        identity_join_keys=(
            "state",
            "system_slug",
            "ahrq_system_id",
            "ccn",
            "npi",
            "pecos_enrollment_id",
            "canonical_name",
            "address",
            "zip_code",
            "county_fips",
            "lat",
            "lon",
        ),
        identity_strategy=(
            "Anchor facility facts on CCN/source-local identifiers and preserve names/addresses as aliases or conflict context.",
            "Use AHRQ system IDs as linkage spine only, then review PECOS/CHOW and official pages for current affiliation.",
            "Persist supported exact metric candidates with their row evidence; route needs_review/source_conflict/unavailable_public rows to manual review.",
            "Keep geography, bed, count, and affiliation facts source-scoped by source period and retrieval/access date.",
        ),
        report_fact_rows=(
            {
                "label": "Profile evidence pack",
                "value_path": "health_system_profiler.build_profile_evidence_pack",
                "required_evidence": "profile_evidence_pack receipt",
                "identity_fields": ("state", "system_slug", "ahrq_system_id", "ccn", "canonical_name"),
                "evidence_path": "health_system_profiler.build_profile_evidence_pack.evidence",
                "source_metadata_path": "health_system_profiler.build_profile_evidence_pack.source_metadata",
                "identity_map_path": "health_system_profiler.build_profile_evidence_pack.identity_map",
            },
            {
                "label": "Supported profile source candidates",
                "value_path": "health_system_profiler.build_profile_evidence_pack.current_hospital_roster",
                "required_evidence": "profile_evidence_pack row receipts",
                "identity_fields": ("ccn", "canonical_name", "address", "zip_code"),
                "evidence_path": "health_system_profiler.build_profile_evidence_pack.current_hospital_roster[].evidence",
                "source_metadata_path": "health_system_profiler.build_profile_evidence_pack.current_hospital_roster[].source_metadata",
                "identity_map_path": "health_system_profiler.build_profile_evidence_pack.identity_map",
            },
            {
                "label": "Profile metric candidates",
                "value_path": "health_system_profiler.build_profile_evidence_pack.hospital_bed_counts",
                "required_evidence": "profile_evidence_pack bed receipts",
                "identity_fields": ("ccn", "state"),
                "evidence_path": "health_system_profiler.build_profile_evidence_pack.hospital_bed_counts[].evidence",
                "source_metadata_path": "health_system_profiler.build_profile_evidence_pack.hospital_bed_counts[].source_metadata",
                "identity_map_path": "health_system_profiler.build_profile_evidence_pack.identity_map",
            },
            {
                "label": "Manual review findings",
                "value_path": "health_system_profiler.build_profile_evidence_pack.conflicts",
                "required_evidence": "profile_evidence_pack conflict receipts",
                "identity_fields": ("state", "ccn", "canonical_name"),
                "evidence_path": "health_system_profiler.build_profile_evidence_pack.conflicts[].evidence",
                "source_metadata_path": "health_system_profiler.build_profile_evidence_pack.conflicts[].source_metadata",
                "identity_map_path": "health_system_profiler.build_profile_evidence_pack.identity_map",
            },
        ),
    ),
    "market_community_health_scan": WorkflowDefinition(
        workflow_id="market_community_health_scan",
        title="Market And Community Health Scan",
        description="Combine geography, community-health estimates, service area, and access context.",
        required_identifiers=("market, state, ZIP/ZCTA, or facility CCN",),
        recommended_servers=WORKFLOW_PRESETS["market_community_health_scan"],
        required_sources=("cdc_places", "cms_hsaf", "census", "routing"),
        steps=(
            WorkflowToolStep("community-health", "get_market_community_profile", "Fetch CDC PLACES market profile.", optional_inputs=("county_fips", "zctas", "measure_ids", "data_value_types"), required_sources=("cdc_places",)),
            WorkflowToolStep("geo-demographics", "get_zcta_demographics", "Fetch Census/ZCTA context.", optional_inputs=("zcta",), optional_env=("CENSUS_API_KEY",), required_sources=("census_acs5_zcta_demographics",)),
            WorkflowToolStep("geo-demographics", "get_zcta_adjacency", "Add adjacent ZCTA topology context for market boundary review.", optional_inputs=("zcta",), required_sources=("census_tiger_zcta_adjacency",), blocking=False, execution_notes=("Adjacency is topology context only; do not treat neighboring ZCTAs as service-area membership without HSAF or other market evidence.",)),
            WorkflowToolStep("service-area", "compute_service_area", "Build hospital service-area context.", optional_inputs=("ccn",), required_sources=("cms_hsaf",)),
            WorkflowToolStep("drive-time", "compute_accessibility_score", "Estimate public routing access context.", optional_inputs=("demand_points", "supply_points", "catchment_minutes"), optional_env=("OSRM_BASE_URL",), required_sources=("routing",), execution_notes=("Use self-hosted OSRM for heavy or production access analysis.")),
        ),
        caveats=("CDC PLACES values are modeled community estimates, not facility-specific outcomes.",),
        identity_join_keys=("ccn", "state", "county_fips", "zcta", "zip_code", "demand_id", "catchment_minutes"),
        identity_strategy=(
            "Use facility CCN only for facility-anchored service-area joins.",
            "Use ZCTA/county/state keys for community estimates; do not merge modeled community values into facility outcomes.",
            "Preserve geography keys separately from facility identifiers.",
        ),
        report_fact_rows=(
            {
                "label": "Community health estimate",
                "value_path": "community_health.get_market_community_profile.market_profile.aggregated_measures",
                "required_evidence": "cdc_places receipt",
                "identity_fields": ("state", "county_fips", "zcta"),
            },
            {
                "label": "Adjacent ZCTA topology",
                "value_path": "geo_demographics.get_zcta_adjacency.adjacent_zcta_rows",
                "required_evidence": "census_tiger_zcta_adjacency receipt",
                "identity_fields": ("zcta", "zip_code"),
            },
            {
                "label": "Service area",
                "value_path": "service_area.compute_service_area",
                "required_evidence": "cms_hsaf receipt",
                "identity_fields": ("ccn",),
            },
            {
                "label": "Access score",
                "value_path": "drive_time.compute_accessibility_score.results",
                "required_evidence": "routing receipt",
                "identity_fields": ("zcta", "zip_code", "demand_id", "catchment_minutes"),
            },
        ),
    ),
    "quality_measure_lookup": WorkflowDefinition(
        workflow_id="quality_measure_lookup",
        title="Quality Measure Lookup",
        description="Return exact CMS quality measure rows and source shape when rows are unavailable.",
        required_identifiers=("ccn", "measure or measure_id"),
        recommended_servers=WORKFLOW_PRESETS["quality_measure_lookup"],
        required_sources=("cms_hospital_quality",),
        steps=(
            WorkflowToolStep("discovery", "get_dataset_source", "Inspect CMS quality source/cache metadata.", required_inputs=("dataset_id",), required_sources=("cms_hospital_quality",)),
            WorkflowToolStep("hospital-quality", "get_quality_measure_rows", "Fetch exact CMS row-level measure data.", required_inputs=("ccn", "measure or measure_id"), required_sources=("cms_hospital_quality",)),
        ),
        caveats=("Adjacent HRRP/HAC/PHC4 records must not be promoted as exact CMS measure facts.",),
        identity_join_keys=("ccn", "measure_id"),
        identity_strategy=(
            "Use CCN for facility identity and measure_id for exact CMS quality row identity.",
            "Do not substitute hospital name, HRRP, HAC, or PHC4 records for missing CMS measure rows.",
        ),
        report_fact_rows=(
            {
                "label": "Exact CMS quality measure row",
                "value_path": "hospital_quality.get_quality_measure_rows.rows",
                "required_evidence": "cms_hospital_quality receipt",
                "identity_fields": ("ccn", "measure_id"),
            },
        ),
    ),
    "research_trials_activity_profile": WorkflowDefinition(
        workflow_id="research_trials_activity_profile",
        title="Research And Trials Activity Profile",
        description="Profile NIH RePORTER funding and ClinicalTrials.gov sponsor/site activity.",
        required_identifiers=("organization or org_name or sponsor or location",),
        recommended_servers=WORKFLOW_PRESETS["research_trials_activity_profile"],
        required_sources=("nih_reporter_projects", "clinicaltrials_gov"),
        steps=(
            WorkflowToolStep("research-trials", "profile_research_funding", "Fetch NIH RePORTER public funding profile.", required_inputs=("org_name",), optional_inputs=("org_uei", "years"), required_sources=("nih_reporter_projects",)),
            WorkflowToolStep("research-trials", "inventory_clinical_trial_sponsors", "Build conservative ClinicalTrials sponsor inventory.", required_inputs=("sponsor",), optional_inputs=("status", "scan_limit"), required_sources=("clinicaltrials_gov",)),
            WorkflowToolStep("research-trials", "inventory_clinical_trial_sites", "Build conservative ClinicalTrials site inventory.", required_inputs=("location",), optional_inputs=("status", "scan_limit"), required_sources=("clinicaltrials_gov",)),
        ),
        caveats=("Sponsor/site inventories are conservative public-source entity resolution, not a complete internal research portfolio.",),
        identity_join_keys=("organization", "entity_name", "npi", "ccn"),
        identity_strategy=(
            "Use organization names as research aliases unless a facility NPI/CCN is independently resolved.",
            "Keep sponsor and site identities separate when public records do not prove common control.",
        ),
        report_fact_rows=(
            {
                "label": "NIH funding profile",
                "value_path": "research_trials.profile_research_funding.projects",
                "required_evidence": "nih_reporter_projects receipt",
                "identity_fields": ("canonical_name",),
            },
            {
                "label": "ClinicalTrials sponsor inventory",
                "value_path": "research_trials.inventory_clinical_trial_sponsors.records",
                "required_evidence": "clinicaltrials_gov receipt",
                "identity_fields": ("canonical_name",),
            },
            {
                "label": "ClinicalTrials site inventory",
                "value_path": "research_trials.inventory_clinical_trial_sites.records",
                "required_evidence": "clinicaltrials_gov receipt",
                "identity_fields": ("canonical_name",),
            },
        ),
    ),
    "referral_leakage_readiness": WorkflowDefinition(
        workflow_id="referral_leakage_readiness",
        title="Referral And Leakage Readiness",
        description="Assess whether public referral-network and service-line inputs are ready for leakage analysis.",
        required_identifiers=("facility CCN or market",),
        recommended_servers=WORKFLOW_PRESETS["referral_leakage_readiness"],
        required_sources=("nppes", "docgraph-import", "cms_claims_reference", "routing"),
        steps=(
            WorkflowToolStep("physician-referral-network", "load_docgraph_cache", "Confirm licensed DocGraph/CareSet cache when available.", optional_env=("DOCGRAPH_CSV_PATH",), required_sources=("docgraph-import",), blocking=False),
            WorkflowToolStep("physician-referral-network", "map_referral_network", "Map referral network from imported shared-patient data.", optional_inputs=("npi", "depth", "min_shared"), optional_env=("DOCGRAPH_CSV_PATH",), required_sources=("docgraph-import",), blocking=False),
            WorkflowToolStep("claims-analytics", "analyze_market_volumes", "Fetch public service-line reference volumes.", optional_inputs=("provider_ccns", "service_line", "year"), required_sources=("cms_claims_reference",)),
            WorkflowToolStep("drive-time", "find_competing_facilities", "Find access/competition context.", optional_inputs=("lat", "lon", "radius_minutes", "facility_type"), optional_env=("OSRM_BASE_URL",), required_sources=("routing",)),
        ),
        caveats=("DocGraph/CareSet data is separately licensed and import-only; absence means the workflow is not ready for leakage assertions.",),
        identity_join_keys=("ccn", "npi", "market", "service_line", "lat", "lon", "address", "zip_code", "radius_minutes"),
        identity_strategy=(
            "Use provider NPI and facility CCN separately; referral network identities are not interchangeable.",
            "Use market/service-line values as analytic scope, not entity identity.",
            "Use drive-time coordinates and radius as access-context scope, not proof of referral leakage.",
            "Do not assert leakage without licensed/imported referral data readiness.",
        ),
        report_fact_rows=(
            {
                "label": "Referral network readiness",
                "value_path": "physician_referral_network.load_docgraph_cache.status",
                "required_evidence": "docgraph-import receipt",
                "identity_fields": ("npi", "ccn"),
            },
            {
                "label": "Market service-line volume",
                "value_path": "claims_analytics.analyze_market_volumes",
                "required_evidence": "cms_claims_reference receipt",
                "identity_fields": ("market", "service_line"),
            },
            {
                "label": "Drive-time competition context",
                "value_path": "drive_time.find_competing_facilities.facilities",
                "required_evidence": "drive_time_competing_facilities receipt",
                "identity_fields": ("lat", "lon", "radius_minutes", "ccn", "address", "zip_code"),
            },
        ),
    ),
}


WORKFLOW_EXAMPLE_INPUTS: dict[str, dict[str, Any]] = {
    "compliance_exclusion_screening": {
        "npi": "1234567893",
        "entity_name": "Thomas Jefferson University Hospitals",
        "state": "PA",
    },
    "facility_profile": {
        "ccn": "390223",
        "facility_name": "Thomas Jefferson University Hospital",
    },
    "quality_profile": {
        "ccn": "390223",
        "measure_id": "HAI_1_SIR",
    },
    "finance_profile": {
        "ccn": "390223",
        "entity_name": "Thomas Jefferson University Hospitals",
    },
    "hospital_competitive_profile": {
        "ccn": "390223",
        "measure": "clabsi_sir",
    },
    "ownership_chow_trace": {
        "ccn": "390223",
        "facility_name": "Thomas Jefferson University Hospital",
        "owner_name": "Example Owner LLC",
        "entity_name": "Example Owner LLC",
        "state": "PA",
    },
    "market_community_health_scan": {
        "market": "Philadelphia",
        "zcta": "19107",
        "zip_code": "19107",
        "ccn": "390223",
    },
    "quality_measure_lookup": {
        "dataset_id": "cms_hospital_quality",
        "ccn": "390223",
        "measure": "clabsi_sir",
    },
    "research_trials_activity_profile": {
        "organization": "Jefferson Health",
        "org_name": "Jefferson Health",
        "sponsor": "Jefferson Health",
        "location": "Jefferson Health",
    },
    "referral_leakage_readiness": {
        "ccn": "390223",
        "npi": "1234567893",
        "market": "Philadelphia",
        "service_line": "cardiology",
        "lat": "39.95",
        "lon": "-75.16",
    },
    "system_reconciliation": {
        "query": "Jefferson Health",
        "system_name": "Jefferson Health",
        "system_slug": "jefferson-health",
        "ccn": "390223",
        "state": "PA",
    },
    "profile_evidence_pack": {
        "state": "PA",
        "system_name": "Jefferson Health",
        "system_slug": "jefferson-health",
        "ccns": ["390223"],
        "required_fields": ["county_geoid", "system_bed_count", "facility_site_count"],
    },
}


def list_workflow_plans() -> dict[str, Any]:
    """List available executable workflow plans."""

    tool_validation = validate_workflow_tool_references()
    contract_validation = validate_workflow_contracts()
    return {
        "workflow_count": len(WORKFLOW_DEFINITIONS),
        "workflows": [
            {
                "workflow_id": workflow.workflow_id,
                "title": workflow.title,
                "description": workflow.description,
                "required_identifiers": list(workflow.required_identifiers),
                "identity_join_keys": list(workflow.identity_join_keys),
                "identity_strategy": list(workflow.identity_strategy),
                "required_sources": list(workflow.required_sources),
                "source_resolution": _workflow_source_resolution(workflow),
                "recommended_servers": list(workflow.recommended_servers),
                "step_count": len(workflow.steps),
                "report_fact_row_count": len(workflow.report_fact_rows),
                "validation": _workflow_list_validation_summary(
                    workflow.workflow_id,
                    tool_validation=tool_validation,
                    contract_validation=contract_validation,
                ),
                "examples": _workflow_examples(workflow.workflow_id),
            }
            for workflow in sorted(WORKFLOW_DEFINITIONS.values(), key=lambda item: item.workflow_id)
        ],
    }


def build_workflow_plan(
    workflow_id: str,
    *,
    inputs: dict[str, Any] | None = None,
    cache_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return one executable workflow plan with readiness and report contracts."""

    key = workflow_id.strip().lower().replace("-", "_")
    workflow = WORKFLOW_DEFINITIONS.get(key)
    if workflow is None:
        return {
            "error": "workflow_not_found",
            "workflow_id": workflow_id,
            "available_workflows": sorted(WORKFLOW_DEFINITIONS),
        }

    input_payload = dict(inputs or {})
    readiness = _workflow_readiness(workflow, input_payload, cache_status or {})
    identity = _workflow_identity(input_payload)
    identity_map = _workflow_identity_map(workflow, identity.to_dict(), input_payload)
    tool_reference_validation = validate_workflow_tool_references(workflow.workflow_id)
    workflow_contract_validation = validate_workflow_contracts(workflow.workflow_id)
    workflow_payload = asdict(workflow)
    cache_entries = _workflow_cache_entries(cache_status)
    workflow_payload["steps"] = [
        _workflow_step_payload(
            step,
            input_payload,
            workflow.identity_join_keys,
            workflow.workflow_id,
            cache_entries,
            tool_reference_validation.get("steps", {}),
        )
        for step in workflow.steps
    ]
    return to_structured(
        {
            **workflow_payload,
            "readiness": readiness,
            "identity": identity.to_dict(),
            "identity_map": identity_map,
            "tool_reference_validation": tool_reference_validation,
            "workflow_contract_validation": workflow_contract_validation,
            "server_metadata": [
                {
                    "server_id": server_id,
                    "port": SERVER_BY_ID[server_id].port,
                    "module": SERVER_BY_ID[server_id].module,
                    "description": SERVER_BY_ID[server_id].description,
                    "safety_notes": list(SERVER_BY_ID[server_id].safety_notes),
                }
                for server_id in workflow.recommended_servers
                if server_id in SERVER_BY_ID
            ],
            "source_resolution": _workflow_source_resolution(workflow),
            "cache_readiness": _workflow_cache_readiness(workflow, cache_entries),
            "examples": _workflow_examples(workflow.workflow_id),
            "evidence": evidence_receipt(
                source_name="healthcare-data-mcp workflow registry",
                dataset_id=f"workflow:{workflow.workflow_id}",
                entity_scope="workflow_plan",
                query=input_payload,
                match_basis="workflow_id_exact",
                confidence="registry_defined_plan",
                caveat="Workflow plans describe source-backed steps; they do not execute tools or assert facts by themselves.",
                next_step="Run the listed tools in order and preserve each tool's evidence receipt in report fact rows.",
            ),
            "report_ingest_contract": _report_ingest_contract(workflow, input_payload),
        }
    )


def _workflow_list_validation_summary(
    workflow_id: str,
    *,
    tool_validation: dict[str, Any],
    contract_validation: dict[str, Any],
) -> dict[str, Any]:
    tool_issues = [
        issue
        for issue in tool_validation.get("issues", [])
        if issue.get("workflow_id") == workflow_id
    ]
    contract_status = (contract_validation.get("workflows") or {}).get(workflow_id, {})
    contract_issue_count = int(contract_status.get("issue_count", 0) or 0)
    return {
        "tool_references": {
            "status": "ok" if not tool_issues else "issues_found",
            "issue_count": len(tool_issues),
            "method": tool_validation.get("method", "registry_module_signature_ast"),
        },
        "report_contracts": {
            "status": contract_status.get("status", "not_checked"),
            "issue_count": contract_issue_count,
            "method": contract_validation.get("method", "workflow_report_contract_static"),
        },
    }


def _workflow_source_resolution(workflow: WorkflowDefinition) -> list[dict[str, Any]]:
    sources: list[str] = []
    for source in workflow.required_sources:
        if source not in sources:
            sources.append(source)
    for step in workflow.steps:
        for source in step.required_sources:
            if source not in sources:
                sources.append(source)
    return _source_resolution(tuple(sources))


def _source_resolution(sources: tuple[str, ...]) -> list[dict[str, Any]]:
    registry_dataset_ids = _registry_dataset_ids()
    resolved: list[dict[str, Any]] = []
    for source in sources:
        alias = WORKFLOW_SOURCE_ALIASES.get(source)
        if alias:
            resolved.append(
                {
                    "source_id": source,
                    "status": "alias",
                    "source_type": alias["source_type"],
                    "canonical_dataset_ids": list(alias["canonical_dataset_ids"]),
                    "caveat": alias["caveat"],
                }
            )
        else:
            resolved.append(
                {
                    "source_id": source,
                    "status": "registry_dataset" if source in registry_dataset_ids else "unknown",
                    "source_type": "registry_dataset",
                    "canonical_dataset_ids": [source] if source in registry_dataset_ids else [],
                    "caveat": "Canonical registry dataset ID." if source in registry_dataset_ids else "Source is not declared in registry or alias map.",
                }
            )
    return resolved


def format_workflow_plan(plan: dict[str, Any]) -> str:
    """Format a workflow plan for CLI users."""

    if "error" in plan:
        return (
            f"Unknown workflow: {plan['workflow_id']}\n"
            f"Available workflows: {', '.join(plan['available_workflows'])}\n"
        )
    lines = [
        f"{plan['title']} ({plan['workflow_id']})",
        plan["description"],
        "",
        "Readiness:",
    ]
    readiness = plan["readiness"]
    lines.append(f"  status: {readiness['status']}")
    for item in readiness.get("missing_inputs", []):
        lines.append(f"  missing input: {item}")
    for item in readiness.get("missing_caches", []):
        lines.append(f"  cache check: {item}")
    for item in readiness.get("missing_required_env", []):
        lines.append(f"  missing env: {item}")
    for item in readiness.get("optional_unavailable", []):
        lines.append(f"  optional step unavailable: {item['server']}.{item['tool']} missing {', '.join(item['missing_env'])}")
    lines.extend(["", "Workflow scope:"])
    required_identifiers = ", ".join(plan.get("required_identifiers", [])) or "none"
    required_sources = ", ".join(plan.get("required_sources", [])) or "none"
    recommended_servers = ", ".join(plan.get("recommended_servers", [])) or "none"
    lines.append(f"  required identifiers: {required_identifiers}")
    lines.append(f"  required sources: {required_sources}")
    lines.append(f"  recommended servers: {recommended_servers}")
    source_resolution = plan.get("source_resolution", [])
    if source_resolution:
        lines.append("  source resolution:")
        for source in source_resolution:
            canonical = ", ".join(source.get("canonical_dataset_ids", [])) or "none"
            lines.append(
                f"    {source['source_id']}: {source.get('status', 'unknown')} -> {canonical}"
            )
            if source.get("caveat"):
                lines.append(f"      source caveat: {source['caveat']}")
    tool_validation = plan.get("tool_reference_validation", {})
    contract_validation = plan.get("workflow_contract_validation", {})
    if tool_validation or contract_validation:
        lines.extend(["", "Planner validation:"])
        if tool_validation:
            lines.append(
                "  tool references: "
                f"{tool_validation.get('status', 'unknown')} "
                f"({tool_validation.get('issue_count', 0)} issues; {tool_validation.get('method', 'unknown')})"
            )
        if contract_validation:
            lines.append(
                "  report contracts: "
                f"{contract_validation.get('status', 'unknown')} "
                f"({contract_validation.get('issue_count', 0)} issues; {contract_validation.get('method', 'unknown')})"
            )
    examples = plan.get("examples", {})
    if examples:
        lines.extend(["", "Example:"])
        lines.append(f"  CLI: {examples['cli_command']}")
        lines.append(f"  JSON: {examples['json_command']}")
    lines.extend(["", "Tool sequence:"])
    for index, step in enumerate(plan["steps"], start=1):
        required = ", ".join(step["required_inputs"]) or "none"
        optional = ", ".join(step["optional_inputs"]) or "none"
        lines.append(f"  {index}. {step['server']}.{step['tool']} - {step['purpose']}")
        lines.append(f"     required: {required}; optional: {optional}")
        lines.append(f"     run server: {step['stdio_command']}")
        lines.append(f"     MCP call: {step['mcp_call']['tool']} args={json.dumps(step['mcp_call']['arguments_template'], sort_keys=True)}")
        identity_contract = step.get("identity_contract", {})
        consumes = ", ".join(identity_contract.get("consumes", [])) or "none"
        produces = ", ".join(identity_contract.get("produces", [])) or "none"
        lines.append(f"     identity: consumes {consumes}; preserves {produces}")
        execution = step.get("execution_readiness", {})
        if execution:
            lines.append(f"     execution readiness: {execution.get('status', 'unknown')}")
            if execution.get("missing_env"):
                lines.append(f"       missing env: {', '.join(execution['missing_env'])}")
            if execution.get("optional_missing_env"):
                lines.append(f"       optional env not configured: {', '.join(execution['optional_missing_env'])}")
            for check in execution.get("source_checks", []):
                lines.append(f"       source: {check['dataset_id']} ({check['status']})")
        for source in step.get("source_resolution", []):
            if source.get("caveat"):
                lines.append(f"       source caveat: {source['source_id']}: {source['caveat']}")
        tool_reference = step.get("tool_reference", {})
        if tool_reference:
            reference_status = tool_reference.get("status", "unknown")
            reference_module = tool_reference.get("module", "")
            suffix = f" ({reference_module})" if reference_module else ""
            lines.append(f"     tool reference: {reference_status}{suffix}")
    lines.extend(["", "Identity map:"])
    for join_key in plan.get("identity_map", {}).get("join_keys", []):
        used_by = ", ".join(join_key["used_by"]) or "none"
        value = join_key["value"] or "<missing>"
        lines.append(f"  {join_key['field']}: {value} ({join_key['status']}; used by {used_by})")
    resolution_steps = plan.get("identity_map", {}).get("resolution_plan", [])
    if resolution_steps:
        lines.extend(["", "Identity resolution:"])
        for resolution in resolution_steps:
            exact = ", ".join(resolution.get("exact_join_fields", [])) or "none"
            candidates = ", ".join(resolution.get("candidate_fields", [])) or "none"
            lines.append(
                f"  {resolution['order']}. {resolution['qualified_tool']}: "
                f"{resolution['merge_action']} (exact: {exact}; candidates: {candidates})"
            )
    lines.extend(["", "Report fact rows:"])
    fact_rows = plan.get("report_ingest_contract", {}).get("fact_rows") or plan.get("report_fact_rows", [])
    for row in fact_rows:
        identity_fields = ", ".join(row.get("identity_fields", [])) or "none"
        required_evidence = row.get("required_evidence") or row.get("source_name") or "tool evidence receipt"
        lines.append(f"  - {row['label']}: {row['value_path']}")
        lines.append(f"    evidence: {required_evidence}; identity: {identity_fields}")
        if row.get("evidence_path"):
            lines.append(f"    evidence path: {row['evidence_path']}")
        if row.get("source_metadata_path"):
            lines.append(f"    source metadata path: {row['source_metadata_path']}")
        if row.get("identity_path"):
            lines.append(f"    identity path: {row['identity_path']}")
        if row.get("identity_map_path"):
            lines.append(f"    identity map path: {row['identity_map_path']}")
    validation_modes = plan.get("report_ingest_contract", {}).get("validation_modes", {})
    if validation_modes:
        lines.extend(["", "Report validation:"])
        for mode_name in ("template", "final_report"):
            mode = validation_modes.get(mode_name)
            if mode:
                lines.append(f"  {mode_name}: {mode['python_call']}")
    lines.extend(["", "Caveats:"])
    for caveat in plan["caveats"]:
        lines.append(f"  - {caveat}")
    return "\n".join(lines) + "\n"


def print_workflow_plan(
    workflow_id: str | None,
    *,
    json_output: bool = False,
    inputs: dict[str, Any] | None = None,
    cache_status: dict[str, Any] | None = None,
) -> None:
    """Print workflow list or one plan."""

    if not workflow_id:
        payload = list_workflow_plans()
        print(json.dumps(payload, indent=2, sort_keys=True) if json_output else _format_workflow_list(payload), end="")
        return
    plan = build_workflow_plan(workflow_id, inputs=inputs, cache_status=cache_status)
    print(json.dumps(plan, indent=2, sort_keys=True) if json_output else format_workflow_plan(plan), end="")


def parse_workflow_inputs(
    *,
    input_items: list[str] | tuple[str, ...] | None = None,
    inputs_json: str | None = None,
) -> dict[str, Any]:
    """Parse CLI workflow inputs without executing any workflow tools.

    JSON values are loaded first; repeated KEY=VALUE items override the JSON
    object so operators can keep a reusable template and change one identifier.
    """

    parsed: dict[str, Any] = {}
    if inputs_json:
        try:
            payload = json.loads(inputs_json)
        except json.JSONDecodeError as exc:
            raise ValueError(f"--inputs-json must be valid JSON: {exc.msg}") from exc
        if not isinstance(payload, dict):
            raise ValueError("--inputs-json must be a JSON object")
        parsed.update(payload)

    for item in input_items or ():
        if "=" not in item:
            raise ValueError(f"--input must use KEY=VALUE syntax: {item!r}")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--input key cannot be empty: {item!r}")
        parsed[key] = value
    return parsed


def _format_workflow_list(payload: dict[str, Any]) -> str:
    lines = ["Available healthcare-data-mcp workflows:"]
    for workflow in payload["workflows"]:
        lines.append(f"  {workflow['workflow_id']:<34} {workflow['title']}")
        identifiers = ", ".join(workflow.get("required_identifiers", [])) or "none"
        sources = ", ".join(workflow.get("required_sources", [])) or "none"
        servers = ", ".join(workflow.get("recommended_servers", [])) or "none"
        lines.append(f"    identifiers: {identifiers}")
        identity_keys = ", ".join(workflow.get("identity_join_keys", [])) or "none"
        lines.append(f"    identity keys: {identity_keys}")
        lines.append(f"    sources: {sources}")
        aliases = [
            source
            for source in workflow.get("source_resolution", [])
            if source.get("status") == "alias"
        ]
        if aliases:
            alias_text = ", ".join(
                f"{source['source_id']}->{'+'.join(source.get('canonical_dataset_ids', []))}"
                for source in aliases[:3]
            )
            lines.append(f"    source aliases: {alias_text}")
        lines.append(
            "    servers: "
            f"{servers}; steps: {workflow.get('step_count', 0)}; "
            f"report rows: {workflow.get('report_fact_row_count', 0)}"
        )
        validation = workflow.get("validation", {})
        if validation:
            tool_validation = validation.get("tool_references", {})
            contract_validation = validation.get("report_contracts", {})
            lines.append(
                "    validation: "
                f"tools {tool_validation.get('status', 'unknown')} ({tool_validation.get('issue_count', 0)} issues); "
                f"reports {contract_validation.get('status', 'unknown')} ({contract_validation.get('issue_count', 0)} issues)"
            )
    lines.append("Run: hc-mcp workflow <workflow_id>")
    lines.append("Example: hc-mcp workflow quality_measure_lookup --input ccn=390223 --input measure=clabsi_sir")
    return "\n".join(lines) + "\n"


def _workflow_examples(workflow_id: str) -> dict[str, Any]:
    inputs = dict(WORKFLOW_EXAMPLE_INPUTS.get(workflow_id, {}))
    input_args = " ".join(
        f"--input {shlex.quote(f'{key}={value}')}"
        for key, value in inputs.items()
    )
    json_payload = json.dumps(inputs, sort_keys=True, separators=(",", ":"))
    return {
        "inputs": inputs,
        "cli_command": f"hc-mcp workflow {workflow_id} {input_args}".strip(),
        "json_command": f"hc-mcp workflow {workflow_id} --inputs-json {shlex.quote(json_payload)} --json",
        "mcp_tool_call": {
            "server": "discovery",
            "tool": "get_workflow_plan",
            "arguments": {
                "workflow_id": workflow_id,
                "inputs": inputs,
            },
        },
        "notes": [
            "Examples use public-data identifiers or valid-format placeholders for planner and tool-shape validation.",
            "Run hc-mcp doctor before executing workflow steps so missing keys and caches are visible.",
        ],
    }


def _workflow_step_payload(
    step: WorkflowToolStep,
    inputs: dict[str, Any],
    workflow_join_keys: tuple[str, ...],
    workflow_id: str = "",
    cache_entries: list[Any] | None = None,
    tool_reference_validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = asdict(step)
    arguments_template = _step_argument_template(step, inputs, workflow_id=workflow_id)
    payload["stdio_command"] = f"hc-mcp {step.server}"
    payload["mcp_call"] = {
        "server": step.server,
        "tool": step.tool,
        "qualified_tool": f"{step.server}.{step.tool}",
        "arguments_template": arguments_template,
        "resolved_arguments": _resolved_arguments(arguments_template),
    }
    payload["input_groups"] = [
        {"label": requirement, "alternatives": _input_alternatives(requirement), "required": True}
        for requirement in step.required_inputs
    ] + [
        {"label": optional, "alternatives": _input_alternatives(optional), "required": False}
        for optional in step.optional_inputs
    ]
    payload["identity_contract"] = _step_identity_contract(step, workflow_join_keys)
    payload["evidence_contract"] = _step_evidence_contract(step)
    payload["source_resolution"] = _source_resolution(step.required_sources)
    payload["execution_readiness"] = _step_execution_readiness(step, inputs, cache_entries or [])
    payload["tool_reference"] = (tool_reference_validation or {}).get(
        f"{step.server}.{step.tool}",
        {"status": "not_checked"},
    )
    return payload


def validate_workflow_tool_references(workflow_id: str | None = None) -> dict[str, Any]:
    """Validate workflow tool references against registry modules without importing servers.

    Some server modules intentionally validate runtime environment at import
    time. This checker parses source files from the canonical registry instead
    so CI can catch workflow drift without requiring optional API credentials.
    """

    workflow_ids = [workflow_id.strip().lower().replace("-", "_")] if workflow_id else sorted(WORKFLOW_DEFINITIONS)
    issues: list[dict[str, Any]] = []
    step_statuses: dict[str, dict[str, Any]] = {}
    module_functions: dict[str, dict[str, dict[str, Any]]] = {}

    for current_workflow_id in workflow_ids:
        workflow = WORKFLOW_DEFINITIONS.get(current_workflow_id)
        if workflow is None:
            issues.append(
                {
                    "workflow_id": current_workflow_id,
                    "status": "workflow_not_found",
                    "message": "Workflow id is not defined.",
                }
            )
            continue
        for step in workflow.steps:
            step_key = f"{step.server}.{step.tool}"
            server = SERVER_BY_ID.get(step.server)
            if server is None:
                issue = {
                    "workflow_id": workflow.workflow_id,
                    "step": step_key,
                    "status": "server_not_registered",
                    "message": f"{step.server} is not in the canonical server registry.",
                }
                issues.append(issue)
                step_statuses[step_key] = issue
                continue

            functions = module_functions.get(server.module)
            if functions is None:
                functions = _module_function_signatures(server.module)
                module_functions[server.module] = functions
            if step.tool not in functions:
                issue = {
                    "workflow_id": workflow.workflow_id,
                    "step": step_key,
                    "module": server.module,
                    "status": "tool_not_found",
                    "message": f"{step.tool} was not found as a top-level function in {server.module}.",
                }
                issues.append(issue)
                step_statuses[step_key] = issue
                continue

            signature = functions[step.tool]
            planned_arguments = sorted(_step_input_tokens(step))
            invalid_arguments = [
                argument
                for argument in planned_arguments
                if argument not in signature["parameters"] and not signature["accepts_var_kwargs"]
            ]
            if invalid_arguments:
                issue = {
                    "workflow_id": workflow.workflow_id,
                    "step": step_key,
                    "module": server.module,
                    "status": "tool_argument_not_found",
                    "invalid_arguments": invalid_arguments,
                    "allowed_arguments": sorted(signature["parameters"]),
                    "message": (
                        f"{step.tool} workflow template includes unsupported argument(s): "
                        f"{', '.join(invalid_arguments)}."
                    ),
                }
                issues.append(issue)
                step_statuses[step_key] = issue
                continue

            step_statuses[step_key] = {
                "workflow_id": workflow.workflow_id,
                "step": step_key,
                "module": server.module,
                "status": "ok",
                "arguments": planned_arguments,
            }

    return {
        "status": "ok" if not issues else "issues_found",
        "checked_workflows": workflow_ids,
        "issue_count": len(issues),
        "issues": issues,
        "steps": step_statuses,
        "method": "registry_module_signature_ast",
    }


def validate_workflow_contracts(workflow_id: str | None = None) -> dict[str, Any]:
    """Validate executable workflow report and identity contracts.

    Tool reference validation proves a step's function exists. This companion
    checker proves report fact-row templates and identity join declarations
    still line up with the workflow's own ordered tool sequence.
    """

    workflow_ids = [workflow_id.strip().lower().replace("-", "_")] if workflow_id else sorted(WORKFLOW_DEFINITIONS)
    issues: list[dict[str, Any]] = []
    workflow_statuses: dict[str, dict[str, Any]] = {}
    if workflow_id is None:
        for missing_workflow_id in sorted(set(WORKFLOW_PRESETS) - set(WORKFLOW_DEFINITIONS)):
            issues.append(
                {
                    "workflow_id": missing_workflow_id,
                    "status": "registry_preset_missing_workflow_definition",
                    "message": "WORKFLOW_PRESETS declares a workflow without an executable WorkflowDefinition.",
                }
            )
    literal_duplicate_issues = _workflow_definition_literal_duplicate_key_issues(workflow_ids)
    literal_duplicate_issues_by_workflow: dict[str, list[dict[str, Any]]] = {}
    for issue in literal_duplicate_issues:
        duplicate_workflow_id = str(issue.get("workflow_id") or "<registry>")
        if duplicate_workflow_id in workflow_ids:
            literal_duplicate_issues_by_workflow.setdefault(duplicate_workflow_id, []).append(issue)
        else:
            issues.append(issue)

    for current_workflow_id in workflow_ids:
        workflow = WORKFLOW_DEFINITIONS.get(current_workflow_id)
        if workflow is None:
            issues.append(
                {
                    "workflow_id": current_workflow_id,
                    "status": "workflow_not_found",
                    "message": "Workflow id is not defined.",
                }
            )
            continue

        step_by_key = {_step_value_path_key(step): step for step in workflow.steps}
        allowed_identity_fields = set(workflow.identity_join_keys) | {"canonical_name", "address", "zip_code"}
        workflow_issues: list[dict[str, Any]] = []
        workflow_issues.extend(_validate_workflow_registry_membership(workflow))
        workflow_issues.extend(literal_duplicate_issues_by_workflow.get(workflow.workflow_id, []))
        workflow_issues.extend(_validate_workflow_sources(workflow))

        for index, row in enumerate(workflow.report_fact_rows, start=1):
            row_issues = _validate_report_fact_row(
                workflow=workflow,
                row=row,
                row_index=index,
                step_by_key=step_by_key,
                allowed_identity_fields=allowed_identity_fields,
            )
            workflow_issues.extend(row_issues)

        if not workflow.report_fact_rows:
            workflow_issues.append(
                {
                    "workflow_id": workflow.workflow_id,
                    "status": "missing_report_fact_rows",
                    "message": "Workflow must define at least one report-ready fact-row template.",
                }
            )

        workflow_statuses[workflow.workflow_id] = {
            "status": "ok" if not workflow_issues else "issues_found",
            "issue_count": len(workflow_issues),
            "fact_row_count": len(workflow.report_fact_rows),
            "step_count": len(workflow.steps),
        }
        issues.extend(workflow_issues)

    return {
        "status": "ok" if not issues else "issues_found",
        "checked_workflows": workflow_ids,
        "issue_count": len(issues),
        "issues": issues,
        "workflows": workflow_statuses,
        "method": "workflow_report_contract_static",
    }


def _workflow_definition_literal_duplicate_key_issues(workflow_ids: list[str]) -> list[dict[str, Any]]:
    try:
        source = Path(__file__).read_text(encoding="utf-8")
    except OSError as exc:
        return [
            {
                "workflow_id": "<registry>",
                "status": "workflow_registry_source_unreadable",
                "message": f"Could not inspect workflow registry literals for duplicate keys: {exc}.",
            }
        ]
    return _duplicate_literal_key_issues_from_source(source, workflow_ids=workflow_ids)


def _duplicate_literal_key_issues_from_source(source: str, *, workflow_ids: list[str]) -> list[dict[str, Any]]:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [
            {
                "workflow_id": "<registry>",
                "status": "workflow_registry_source_unparseable",
                "message": f"Could not parse workflow registry literals for duplicate keys: {exc}.",
            }
        ]

    requested = set(workflow_ids)
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "WORKFLOW_DEFINITIONS" for target in node.targets):
            continue
        if not isinstance(node.value, ast.Dict):
            return []
        issues: list[dict[str, Any]] = []
        for workflow_key, workflow_node in zip(node.value.keys, node.value.values, strict=False):
            workflow_id = _literal_string(workflow_key) or "<unknown>"
            if workflow_id not in requested:
                continue
            for duplicate in _duplicate_key_dict_nodes(workflow_node):
                issues.append(
                    {
                        "workflow_id": workflow_id,
                        "status": "duplicate_workflow_literal_key",
                        "key": duplicate["key"],
                        "line": duplicate["line"],
                        "message": (
                            f"Workflow definition contains duplicate literal key {duplicate['key']!r} "
                            f"near line {duplicate['line']}; Python silently keeps the last value."
                        ),
                    }
                )
        return issues
    return []


def _duplicate_key_dict_nodes(node: ast.AST) -> list[dict[str, Any]]:
    duplicates: list[dict[str, Any]] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Dict):
            continue
        seen: dict[str, int] = {}
        for key in child.keys:
            literal = _literal_string(key)
            if literal is None:
                continue
            if literal in seen:
                duplicates.append({"key": literal, "line": getattr(key, "lineno", getattr(child, "lineno", 0))})
            else:
                seen[literal] = getattr(key, "lineno", getattr(child, "lineno", 0))
    return duplicates


def _literal_string(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _validate_workflow_registry_membership(workflow: WorkflowDefinition) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    preset_servers = WORKFLOW_PRESETS.get(workflow.workflow_id)
    if preset_servers is None:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "status": "workflow_missing_registry_preset",
                "message": "Workflow is executable but is not declared in WORKFLOW_PRESETS.",
            }
        )
    elif tuple(workflow.recommended_servers) != tuple(preset_servers):
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "status": "workflow_recommended_servers_drift",
                "recommended_servers": list(workflow.recommended_servers),
                "registry_servers": list(preset_servers),
                "message": "Workflow recommended_servers must match the canonical WORKFLOW_PRESETS entry.",
            }
        )

    unknown_recommended = sorted(server_id for server_id in workflow.recommended_servers if server_id not in SERVER_BY_ID)
    if unknown_recommended:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "status": "workflow_recommended_server_not_registered",
                "servers": unknown_recommended,
                "message": "Workflow recommended_servers include server ids missing from the canonical server registry.",
            }
        )

    recommended = set(workflow.recommended_servers)
    missing_step_servers = sorted({step.server for step in workflow.steps if step.server not in recommended})
    if missing_step_servers:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "status": "workflow_step_server_not_recommended",
                "servers": missing_step_servers,
                "message": "Every workflow step server must be included in recommended_servers.",
            }
        )

    return issues


def _validate_workflow_sources(workflow: WorkflowDefinition) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    registry_dataset_ids = _registry_dataset_ids()
    all_sources: list[tuple[str, str]] = [(source, "workflow.required_sources") for source in workflow.required_sources]
    for step in workflow.steps:
        all_sources.extend((source, f"{step.server}.{step.tool}.required_sources") for source in step.required_sources)

    for source, location in all_sources:
        alias = WORKFLOW_SOURCE_ALIASES.get(source)
        if source in registry_dataset_ids:
            continue
        if alias is None:
            issues.append(
                {
                    "workflow_id": workflow.workflow_id,
                    "status": "unknown_required_source",
                    "source": source,
                    "location": location,
                    "message": "Workflow required source is not a registry dataset ID or declared workflow source alias.",
                }
            )
            continue
        missing_targets = sorted(set(alias["canonical_dataset_ids"]) - registry_dataset_ids)
        if missing_targets:
            issues.append(
                {
                    "workflow_id": workflow.workflow_id,
                    "status": "workflow_source_alias_target_missing",
                    "source": source,
                    "location": location,
                    "missing_dataset_ids": missing_targets,
                    "message": "Workflow source alias points at dataset IDs missing from the canonical server registry.",
                }
            )
    return issues


def _registry_dataset_ids() -> set[str]:
    return {dataset_id for spec in SERVER_BY_ID.values() for dataset_id in spec.dataset_ids}


def _validate_report_fact_row(
    *,
    workflow: WorkflowDefinition,
    row: dict[str, Any],
    row_index: int,
    step_by_key: dict[str, WorkflowToolStep],
    allowed_identity_fields: set[str],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    label = str(row.get("label") or f"row_{row_index}")
    step_keys = set(step_by_key)
    for required_key in ("label", "value_path", "required_evidence", "identity_fields"):
        if not row.get(required_key):
            issues.append(
                {
                    "workflow_id": workflow.workflow_id,
                    "row": label,
                    "status": f"missing_{required_key}",
                    "message": f"Report fact row {row_index} is missing {required_key}.",
                }
            )

    value_path = str(row.get("value_path") or "")
    value_path_key = _value_path_step_key(value_path)
    if value_path_key and value_path_key not in step_keys:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "value_path_step_not_in_workflow",
                "message": f"{value_path!r} does not reference a tool step in this workflow.",
            }
        )
    elif not value_path_key:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "invalid_value_path",
                "message": "Report fact row value_path must start with '<server>.<tool>'.",
            }
        )

    identity_fields = row.get("identity_fields", ())
    if not isinstance(identity_fields, tuple | list):
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "invalid_identity_fields",
                "message": "identity_fields must be a tuple or list.",
            }
        )
        return issues

    undeclared_identity_fields = sorted(set(identity_fields) - allowed_identity_fields)
    if undeclared_identity_fields:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "undeclared_identity_fields",
                "message": "identity_fields are not declared workflow join keys: "
                + ", ".join(undeclared_identity_fields),
            }
        )

    if value_path_key in step_by_key:
        issues.extend(
            _validate_report_fact_paths(
                workflow=workflow,
                row=row,
                label=label,
                value_path_key=value_path_key,
                step=step_by_key[value_path_key],
            )
        )
    return issues


def _validate_report_fact_paths(
    *,
    workflow: WorkflowDefinition,
    row: dict[str, Any],
    label: str,
    value_path_key: str,
    step: WorkflowToolStep,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    path_fields = {
        "evidence_path": str(row.get("evidence_path") or _evidence_path_from_value_path(str(row.get("value_path", "")))),
        "identity_path": str(row.get("identity_path") or _identity_path_from_value_path(str(row.get("value_path", "")))),
        "source_metadata_path": str(
            row.get("source_metadata_path") or _source_metadata_path_from_value_path(str(row.get("value_path", "")))
        ),
    }
    identity_map_path = str(row.get("identity_map_path") or _identity_map_path_from_value_path(str(row.get("value_path", ""))))
    if identity_map_path:
        path_fields["identity_map_path"] = identity_map_path
    else:
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "missing_identity_map_path",
                "message": (
                    "Report fact rows must point at the owning tool identity_map so cross-server "
                    "joins preserve source-boundary and conflict policy."
                ),
            }
        )

    for path_field, path in path_fields.items():
        if _value_path_step_key(path) != value_path_key:
            issues.append(
                {
                    "workflow_id": workflow.workflow_id,
                    "row": label,
                    "status": f"{path_field}_step_mismatch",
                    "message": f"{path_field} {path!r} does not reference the same workflow step as value_path.",
                }
            )

    evidence_path = path_fields["evidence_path"]
    allowed_evidence_paths = _materialized_step_evidence_paths(value_path_key, step)
    if not _evidence_path_matches_allowed(evidence_path, allowed_evidence_paths):
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "evidence_path_not_in_step_contract",
                "message": (
                    f"evidence_path {evidence_path!r} is not advertised by "
                    f"{step.server}.{step.tool} evidence_contract."
                ),
            }
        )
    elif _uses_result_level_evidence_for_nested_fact(
        value_path=str(row.get("value_path") or ""),
        evidence_path=evidence_path,
        value_path_key=value_path_key,
        step=step,
    ):
        issues.append(
            {
                "workflow_id": workflow.workflow_id,
                "row": label,
                "status": "result_level_evidence_for_row_fact",
                "message": (
                    f"Nested fact {row.get('value_path')!r} should cite the most specific row "
                    f"receipt advertised by {step.server}.{step.tool}, not {evidence_path!r}."
                ),
            }
        )

    allowed_identity_paths = _materialized_step_identity_paths(value_path_key, step)
    for path_field in ("identity_path", "identity_map_path"):
        path = path_fields.get(path_field)
        if path and not _evidence_path_matches_allowed(path, allowed_identity_paths):
            issues.append(
                {
                    "workflow_id": workflow.workflow_id,
                    "row": label,
                    "status": f"{path_field}_not_in_step_contract",
                    "message": (
                        f"{path_field} {path!r} is not advertised by "
                        f"{step.server}.{step.tool} identity_contract."
                    ),
                }
            )
    return issues


def _uses_result_level_evidence_for_nested_fact(
    *,
    value_path: str,
    evidence_path: str,
    value_path_key: str,
    step: WorkflowToolStep,
) -> bool:
    if evidence_path != f"{value_path_key}.evidence":
        return False
    parts = [part for part in value_path.split(".") if part]
    if len(parts) <= 2 or parts[-1] in {"identity", "identity_map", "status"}:
        return False
    row_paths = [
        path
        for path in _materialized_step_evidence_paths(value_path_key, step)
        if path != f"{value_path_key}.evidence"
    ]
    return any(_row_evidence_path_targets_value_path(row_path=path, value_path=value_path) for path in row_paths)


def _row_evidence_path_targets_value_path(*, row_path: str, value_path: str) -> bool:
    return row_path in {
        f"{value_path}.evidence",
        f"{value_path}[].evidence",
        f"{value_path}.*",
    }


def _materialized_step_evidence_paths(step_key: str, step: WorkflowToolStep) -> set[str]:
    paths = {f"{step_key}.evidence"}
    for row_path in _step_row_evidence_paths(step):
        if row_path.startswith("result."):
            paths.add(f"{step_key}.{row_path[len('result.'):]}")
    return paths


def _materialized_step_identity_paths(step_key: str, step: WorkflowToolStep) -> set[str]:
    paths: set[str] = set()
    for output_path in _step_identity_output_paths(step):
        if output_path.startswith("result."):
            paths.add(f"{step_key}.{output_path[len('result.'):]}")
    return paths


def _evidence_path_matches_allowed(path: str, allowed_paths: set[str]) -> bool:
    if path in allowed_paths:
        return True
    for allowed in allowed_paths:
        if allowed.endswith(".*") and path.startswith(allowed[:-2]):
            return True
        if path == allowed.removesuffix(".*"):
            return True
    return False


def _step_value_path_key(step: WorkflowToolStep) -> str:
    return f"{step.server.replace('-', '_')}.{step.tool}"


def _value_path_step_key(value_path: str) -> str:
    parts = [part for part in value_path.split(".") if part]
    if len(parts) < 2:
        return ""
    return ".".join(parts[:2])


def _module_function_signatures(module_name: str) -> dict[str, dict[str, Any]]:
    spec = importlib.util.find_spec(module_name)
    if spec is None or not spec.origin:
        return {}
    module_path = Path(spec.origin)
    if not module_path.exists():
        return {}
    try:
        tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
    except (OSError, SyntaxError):
        return {}

    signatures: dict[str, dict[str, Any]] = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        parameters = {
            arg.arg
            for arg in (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            )
        }
        signatures[node.name] = {
            "parameters": parameters,
            "accepts_var_kwargs": node.args.kwarg is not None,
        }
    return signatures


def _step_argument_template(step: WorkflowToolStep, inputs: dict[str, Any], *, workflow_id: str = "") -> dict[str, Any]:
    template: dict[str, Any] = {}
    for label in (*step.required_inputs, *step.optional_inputs):
        for argument in _input_alternatives(label):
            template[argument] = _workflow_argument_value(argument, inputs)
    if step.server == "cache-manager" and step.tool == "get_workflow_cache_readiness":
        template["workflow_id"] = workflow_id or template.get("workflow_id") or _workflow_argument_value("workflow_id", inputs)
        template["inputs"] = dict(inputs)
    return template


def _workflow_argument_value(argument: str, inputs: dict[str, Any]) -> Any:
    if argument == "provider_ccns" and "ccn" in inputs and "provider_ccns" not in inputs:
        return [inputs["ccn"]]
    if argument == "zctas" and "zcta" in inputs and "zctas" not in inputs:
        return [inputs["zcta"]]
    if argument in inputs:
        value = inputs[argument]
        if argument in {"provider_ccns", "zctas"} and not isinstance(value, list):
            return [value]
        return value
    aliases = {
        "name": ("query", "facility_name", "system_name", "entity_name", "organization"),
        "org_name": ("organization", "entity_name", "query"),
        "provider_ccns": ("ccns",),
        "provider_name": ("entity_name", "facility_name", "organization", "query"),
        "sponsor": ("organization", "entity_name", "query"),
        "zctas": ("zip_code", "zip"),
        "location": ("facility_name", "organization", "entity_name", "query", "market"),
    }
    for alias in aliases.get(argument, ()):
        if alias in inputs:
            value = inputs[alias]
            if argument in {"provider_ccns", "zctas"} and not isinstance(value, list):
                return [value]
            return value
    return f"<{argument}>"


def _input_alternatives(label: str) -> list[str]:
    return [
        token.strip()
        for token in label.replace(" or ", ",").split(",")
        if token.strip() and token.strip().replace("_", "").replace("-", "").isalnum()
    ]


def _step_identity_contract(step: WorkflowToolStep, join_keys: tuple[str, ...]) -> dict[str, Any]:
    consumes = _step_identity_fields(step, join_keys)
    produces = _step_identity_outputs(step, join_keys)
    return {
        "consumes": consumes,
        "produces": produces,
        "output_paths": _step_identity_output_paths(step),
        "match_policy": _step_match_policy(step, join_keys),
        "evidence_required": "evidence" in step.output_contract,
        "preserve_with_fact_rows": sorted(set((*consumes, *produces))),
    }


def _step_evidence_contract(step: WorkflowToolStep) -> dict[str, Any]:
    return {
        "result_evidence_path": "result.evidence",
        "result_source_metadata_path": "result.source_metadata",
        "row_evidence_paths": _step_row_evidence_paths(step),
        "required_receipt_fields": list(REPORT_SOURCE_METADATA_FIELDS),
        "copy_rule": "Copy the most specific row evidence receipt available; use result.evidence only when the cited value is result-level.",
    }


def _step_row_evidence_paths(step: WorkflowToolStep) -> list[str]:
    if step.server == "provider-enrollment":
        if step.tool == "search_provider_enrollment":
            return ["result.enrollments[].evidence"]
        if step.tool == "get_provider_enrollment_detail":
            return ["result.enrollments[].evidence", "result.ownership[].evidence", "result.chow_history[].evidence"]
        if step.tool == "get_facility_ownership":
            return ["result.owners[].evidence"]
        if step.tool == "trace_owner_network":
            return ["result.nodes[].evidence", "result.edges[].evidence"]
        if step.tool == "search_change_of_ownership":
            return ["result.events[].evidence"]
        if step.tool == "profile_provider_control":
            return [
                "result.enrollment[].evidence",
                "result.ownership[].evidence",
                "result.chow_history[].evidence",
                "result.owner_network.nodes[].evidence",
                "result.owner_network.edges[].evidence",
            ]
    if step.server == "hospital-quality":
        if step.tool == "get_quality_measure_rows":
            return ["result.rows[].evidence"]
        if step.tool == "get_safety_scores":
            return ["result.domain_evidence[].evidence"]
        if step.tool == "get_readmission_data":
            return ["result.conditions[].evidence"]
        if step.tool == "get_patient_experience":
            return ["result.domains[].evidence"]
        return ["result.records[].evidence", "result.rows[].evidence"]
    if step.server == "financial-intelligence":
        return [
            "result.records[].evidence",
            "result.metric_evidence.*",
            "result.hcris.evidence",
            "result.hcris.metric_evidence.*",
            "result.form990_schedule_h.evidence",
            "result.form990_schedule_h.metric_evidence.*",
            "result.ahrq_hfmd.evidence",
            "result.ahrq_hfmd.metric_evidence.*",
        ]
    if step.server == "workforce-analytics":
        return [
            "result.records[].evidence",
            "result.metric_evidence.*",
            "result.departments[].evidence",
            "result.bed_source.selected_candidate_evidence",
            "result.bed_source.candidates[].evidence",
            "result.bed_source.rejected_candidates[].evidence",
        ]
    if step.server == "health-system-profiler":
        if step.tool == "build_profile_evidence_pack":
            return [
                "result.system_identity_aliases[].evidence",
                "result.current_hospital_roster[].evidence",
                "result.source_identifiers[].evidence",
                "result.addresses[].evidence",
                "result.geography_candidates[].evidence",
                "result.hospital_bed_counts[].evidence",
                "result.hospital_bed_counts[].value.resolution.candidates[].evidence",
                "result.hospital_bed_counts[].value.resolution.rejected_candidates[].evidence",
                "result.system_bed_count_candidates[].evidence",
                "result.bed_rollup_guidance[].evidence",
                "result.affiliation_evidence[].evidence",
                "result.facility_site_count_evidence[].evidence",
                "result.conflicts[].evidence",
                "result.unavailable_public_findings[].evidence",
            ]
        return [
            "result.records[].evidence",
            "result.results[].evidence",
            "result.facilities[].evidence",
            "result.inpatient_facilities[].evidence",
            "result.outpatient_sites[].evidence",
            "result.sub_entities[].evidence",
            "result.facility_reconciliation.facilities[].evidence",
            "result.facility_reconciliation.merger_evidence[].evidence",
            "result.systems[].evidence",
            "result.merger_evidence[].evidence",
        ]
    if step.server == "research-trials":
        if step.tool == "profile_research_funding":
            return [
                "result.projects[].evidence",
                "result.by_fiscal_year[].evidence",
                "result.by_institute[].evidence",
                "result.by_pi[].evidence",
                "result.by_activity_code[].evidence",
                "result.top_terms[].evidence",
            ]
        if step.tool.startswith("inventory_clinical_trial_"):
            return ["result.records[].evidence"]
        if step.tool == "search_clinical_trials":
            return ["result.trials[].evidence"]
        if step.tool == "get_clinical_trial":
            return ["result.trial.evidence"]
    if step.server == "service-area":
        if step.tool == "get_market_share":
            return ["result.hospitals[].evidence"]
        return ["result.records[].evidence", "result.results[].evidence"]
    if step.server == "web-intelligence":
        return [
            "result.results[].evidence",
            "result.locations[].evidence",
            "result.executives[].evidence",
            "result.items[].evidence",
            "result.matches[].evidence",
        ]
    if step.server == "public-records":
        return ["result.records[].evidence", "result.results[].evidence", "result.matches[].evidence", "result.incidents[].evidence", "result.breaches[].evidence"]
    if step.server == "community-health":
        if step.tool == "list_places_measures":
            return ["result.results[].evidence"]
        if step.tool == "search_places":
            return ["result.results[].evidence"]
        if step.tool == "get_places_profile":
            return ["result.profile.location.evidence", "result.profile.measures[].evidence"]
        if step.tool == "compare_places":
            return [
                "result.comparison.profiles.*.location.evidence",
                "result.comparison.profiles.*.measures[].evidence",
            ]
        if step.tool == "get_market_community_profile":
            return [
                "result.market_profile.locations[].evidence",
                "result.market_profile.aggregated_measures[].evidence",
            ]
        return ["result.records[].evidence", "result.results[].evidence"]
    if step.server == "geo-demographics":
        if step.tool == "get_zcta_demographics_batch":
            return ["result.results[].evidence"]
        if step.tool == "get_zcta_adjacency":
            return ["result.adjacent_zcta_rows[].evidence"]
        if step.tool == "crosswalk_zip":
            return ["result.results[].evidence"]
        return ["result.evidence"]
    if step.server == "cms-facility":
        return ["result.results[].evidence"]
    if step.server in {"service-area", "claims-analytics", "physician-referral-network", "drive-time"}:
        if step.server == "drive-time":
            if step.tool == "find_competing_facilities":
                return ["result.facilities[].evidence"]
            if step.tool == "compute_accessibility_score":
                return ["result.results[].evidence"]
            if step.tool == "compute_drive_time_matrix":
                return ["result.matrix[].evidence"]
        return ["result.records[].evidence", "result.results[].evidence"]
    return []


def _step_execution_readiness(step: WorkflowToolStep, inputs: dict[str, Any], cache_entries: list[Any]) -> dict[str, Any]:
    server = SERVER_BY_ID.get(step.server)
    server_required_env = tuple(key.name for key in server.required_env) if server else ()
    server_optional_env = tuple(key.name for key in server.optional_env) if server else ()
    required_env = tuple(dict.fromkeys((*server_required_env, *step.required_env)))
    optional_env = tuple(dict.fromkeys((*server_optional_env, *step.optional_env)))
    missing_inputs = _missing_inputs(step.required_inputs, inputs)
    missing_env = [name for name in required_env if not os.environ.get(name)]
    optional_missing = [name for name in optional_env if not os.environ.get(name)]
    source_checks = _source_checks(step.required_sources, cache_entries)
    missing_sources = [
        check["dataset_id"]
        for check in source_checks
        if check["status"] not in {"ready", "not_checked", "live_api", "not_applicable"}
    ]

    if missing_inputs:
        status = "needs_inputs" if step.blocking else "optional_unavailable"
    elif missing_env:
        status = "missing_configuration" if step.blocking else "optional_unavailable"
    elif missing_sources and step.blocking:
        status = "review_sources"
    else:
        status = "ready"

    return {
        "status": status,
        "blocking": step.blocking,
        "missing_inputs": missing_inputs,
        "required_env": list(required_env),
        "missing_env": missing_env,
        "optional_env": list(optional_env),
        "optional_missing_env": optional_missing,
        "source_checks": source_checks,
        "notes": list(step.execution_notes),
    }


def _resolved_arguments(arguments_template: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in arguments_template.items()
        if not _is_placeholder(value)
    }


def _is_placeholder(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("<") and value.endswith(">")


def _source_checks(required_sources: tuple[str, ...], cache_entries: list[Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for source in required_sources:
        canonical_sources = _canonical_dataset_ids_for_source(source)
        matching = [
            entry
            for entry in cache_entries
            if isinstance(entry, dict) and entry.get("dataset_id") in canonical_sources
        ]
        if not matching:
            checks.append(
                {
                    "source_id": source,
                    "dataset_id": source,
                    "canonical_dataset_ids": canonical_sources,
                    "status": "not_checked",
                    "cache_status": [],
                }
            )
            continue
        statuses = sorted({str(entry.get("readiness_status") or entry.get("status") or "unknown") for entry in matching})
        aggregate_status = _aggregate_cache_status(statuses)
        checks.append(
            {
                "source_id": source,
                "dataset_id": source,
                "canonical_dataset_ids": canonical_sources,
                "status": aggregate_status,
                "cache_status": statuses,
                "validation_status": sorted(
                    {str(entry.get("validation_status") or "") for entry in matching if entry.get("validation_status")}
                ),
                "source_period": sorted(
                    {str(entry.get("source_period") or "") for entry in matching if entry.get("source_period")}
                ),
                "report_eligible": all(bool(entry.get("report_eligible")) for entry in matching),
                "next_actions": [
                    str(entry.get("next_action"))
                    for entry in matching
                    if str(entry.get("next_action") or "").strip()
                ],
            }
        )
    return checks


def _step_identity_outputs(step: WorkflowToolStep, join_keys: tuple[str, ...]) -> list[str]:
    candidates: tuple[str, ...]
    if step.server == "cms-facility":
        candidates = ("ccn", "npi", "canonical_name", "address", "zip_code")
    elif step.server == "health-system-profiler":
        candidates = ("ccn", "ahrq_system_id", "canonical_name", "address", "zip_code")
    elif step.server == "hospital-quality":
        candidates = ("ccn", "measure_id", "canonical_name")
    elif step.server == "financial-intelligence":
        candidates = ("ccn", "canonical_name")
    elif step.server == "workforce-analytics":
        candidates = ("ccn", "canonical_name")
    elif step.server == "provider-enrollment":
        candidates = ("npi", "ccn", "pecos_enrollment_id", "owner_id", "owner_name", "canonical_name")
    elif step.server == "public-records":
        candidates = ("npi", "canonical_name", "entity_name")
    elif step.server in {"research-trials", "web-intelligence"}:
        candidates = ("canonical_name", "entity_name", "organization")
    elif step.server in {"community-health", "geo-demographics", "service-area", "drive-time"}:
        candidates = (
            "ccn",
            "state",
            "county_fips",
            "zcta",
            "zip_code",
            "address",
            "lat",
            "lon",
            "radius_minutes",
            "catchment_minutes",
            "demand_id",
        )
    elif step.server in {"claims-analytics", "physician-referral-network"}:
        candidates = ("ccn", "npi", "market", "service_line")
    else:
        candidates = ()

    allowed = set(join_keys) | {"canonical_name", "address", "zip_code"}
    return [field for field in candidates if field in allowed]


def _step_identity_output_paths(step: WorkflowToolStep) -> list[str]:
    if step.server == "public-records" and step.tool.startswith("screen_"):
        return ["result.identity_map", "result.results[].identity", "result.results[].matches[]"]
    if step.server == "public-records" and ("leie" in step.tool or "sam_exclusions" in step.tool):
        return ["result.identity", "result.identity_map", "result.records[]"]
    if step.server == "public-records":
        return ["result.identity", "result.identity_map", "result.records[]", "result.incidents[]", "result.breaches[]"]
    if step.server == "provider-enrollment":
        if step.tool == "profile_provider_control":
            return [
                "result.identity",
                "result.identity_map",
                "result.enrollment[].identity",
                "result.ownership[].identity",
                "result.chow_history[].identity",
                "result.owner_network.nodes[]",
                "result.owner_network.edges[]",
            ]
        if "ownership" in step.tool or "owner" in step.tool:
            return ["result.identity", "result.identity_map", "result.owners[].identity"]
        if "change_of_ownership" in step.tool:
            return ["result.identity", "result.identity_map", "result.events[].identity"]
        return ["result.identity", "result.identity_map", "result.records[].identity"]
    if step.server == "health-system-profiler":
        return ["result.identity", "result.identity_map", "result.records[].identity"]
    if step.server == "hospital-quality":
        return ["result.identity", "result.identity_map", "result.records[].identity"]
    if step.server == "cms-facility":
        return ["result.identity", "result.identity_map", "result.records[].identity", "result.results[].identity"]
    if step.server in {"community-health", "geo-demographics", "service-area", "drive-time"}:
        return ["result.identity", "result.identity_map", "result.records[].identity", "result.results[].identity"]
    if step.server in {"financial-intelligence", "workforce-analytics", "research-trials", "web-intelligence"}:
        return ["result.identity", "result.identity_map", "result.records[].identity"]
    if step.server in {"claims-analytics", "physician-referral-network"}:
        return ["result.identity", "result.identity_map", "result.records[].identity", "result.results[].identity"]
    return ["result.evidence.query", "result.source_metadata"]


def _report_ingest_contract(workflow: WorkflowDefinition, inputs: dict[str, Any]) -> dict[str, Any]:
    """Return fact-row templates that satisfy the shared report ingest contract.

    These rows are not executed facts. They show how an agent should copy each
    owning tool's evidence receipt into a final report row.
    """

    fact_rows = []
    for row in workflow.report_fact_rows:
        receipt = evidence_receipt(
            source_name=str(row.get("required_evidence") or "tool evidence receipt"),
            source_url="copy_from_tool_evidence.source_url",
            dataset_id=_dataset_id_from_required_evidence(str(row.get("required_evidence") or "")),
            source_period="copy_from_tool_evidence.source_period",
            landing_page="copy_from_tool_evidence.landing_page",
            retrieved_at="copy_from_tool_evidence.retrieved_at",
            source_modified="copy_from_tool_evidence.source_modified",
            cache_status="copy_from_tool_evidence.cache_status",
            cache_freshness="copy_from_tool_evidence.cache_freshness",
            entity_scope=f"workflow:{workflow.workflow_id}",
            query=inputs or {"workflow_id": workflow.workflow_id},
            cache_key="copy_from_tool_evidence.cache_key",
            match_basis="copy_from_tool_evidence.match_basis",
            confidence="copy_from_tool_evidence.confidence",
            caveat=(
                "Template row only; replace every copy_from_tool_evidence.* placeholder "
                "with the owning tool's evidence receipt before citing facts."
            ),
            next_step="Run the value_path tool and copy its evidence receipt into this report fact row.",
        )
        fact_rows.append(
            {
                "label": row.get("label", ""),
                "value_path": row.get("value_path", ""),
                "identity_path": row.get("identity_path")
                or _identity_path_from_value_path(str(row.get("value_path", ""))),
                "identity_map_path": row.get("identity_map_path")
                or _identity_map_path_from_value_path(str(row.get("value_path", ""))),
                "evidence_path": row.get("evidence_path")
                or _evidence_path_from_value_path(str(row.get("value_path", ""))),
                "source_metadata_path": row.get("source_metadata_path")
                or _source_metadata_path_from_value_path(str(row.get("value_path", ""))),
                "identity_fields": list(row.get("identity_fields", ())),
                "required_evidence_fields": list(REPORT_SOURCE_METADATA_FIELDS),
                "contract_status": "template_requires_tool_execution",
                **receipt,
            }
        )

    return {
        "workflow_id": workflow.workflow_id,
        "status": "template_requires_tool_execution",
        "validation_modes": {
            "template": {
                "function": "validate_report_ingest_payload",
                "arguments": {
                    "require_content": False,
                    "allow_placeholders": True,
                    "require_identity_context": False,
                },
                "python_call": "validate_report_ingest_payload(payload)",
                "purpose": "Validate planner templates before tool execution.",
            },
            "final_report": {
                "function": "validate_report_ingest_payload",
                "arguments": {
                    "require_content": True,
                    "allow_placeholders": False,
                    "require_identity_context": True,
                },
                "python_call": (
                    "validate_report_ingest_payload(payload, require_content=True, "
                    "allow_placeholders=False, require_identity_context=True)"
                ),
                "purpose": "Validate cited report rows after evidence and identity context are copied from tool results.",
            },
        },
        "fact_rows": fact_rows,
        "instructions": [
            "Use these rows as report-builder templates, not source facts.",
            "Before citing a fact, run the value_path tool and replace copy_from_tool_evidence.* placeholders.",
            "Keep identity_fields, identity_path, and identity_map_path next to every fact row so cross-server joins remain auditable.",
            "Run final_report validation after copying tool evidence and identity context into cited report rows.",
        ],
    }


def _dataset_id_from_required_evidence(required_evidence: str) -> str:
    value = required_evidence.strip()
    if value.endswith(" receipt"):
        value = value[: -len(" receipt")]
    return value.replace(" ", "_") or "tool_evidence"


def _identity_map_path_from_value_path(value_path: str) -> str:
    parts = [part for part in value_path.split(".") if part]
    if len(parts) < 2 or parts[0] not in _IDENTITY_MAP_VALUE_PATH_PREFIXES:
        return ""
    return ".".join((*parts[:2], "identity_map"))


def _identity_path_from_value_path(value_path: str) -> str:
    parts = [part for part in value_path.split(".") if part]
    if len(parts) < 2:
        return "copy_from_tool_result.identity"
    return ".".join((*parts[:2], "identity"))


def _evidence_path_from_value_path(value_path: str) -> str:
    parts = [part for part in value_path.split(".") if part]
    if len(parts) < 2:
        return "copy_from_tool_result.evidence"
    prefix = ".".join(parts[:2])
    if len(parts) == 2 or parts[-1] in {"identity", "identity_map", "status"}:
        return f"{prefix}.evidence"
    return f"{'.'.join(parts)}[].evidence"


def _source_metadata_path_from_value_path(value_path: str) -> str:
    parts = [part for part in value_path.split(".") if part]
    if len(parts) < 2:
        return "copy_from_tool_result.source_metadata"
    return ".".join((*parts[:2], "source_metadata"))


_IDENTITY_MAP_VALUE_PATH_PREFIXES = {
    "claims_analytics",
    "community_health",
    "cms_facility",
    "drive_time",
    "financial_intelligence",
    "geo_demographics",
    "health_system_profiler",
    "hospital_quality",
    "physician_referral_network",
    "price_transparency",
    "provider_enrollment",
    "public_records",
    "research_trials",
    "service_area",
    "web_intelligence",
    "workforce_analytics",
}


def _workflow_readiness(
    workflow: WorkflowDefinition,
    inputs: dict[str, Any],
    cache_status: dict[str, Any],
) -> dict[str, Any]:
    missing_inputs = _missing_inputs(workflow.required_identifiers, inputs)
    cache_entries = _workflow_cache_entries(cache_status)
    missing_caches = _missing_cache_notes(workflow.required_sources, cache_entries)
    missing_required_env = _workflow_missing_required_env(workflow)
    optional_unavailable = _workflow_optional_unavailable(workflow)
    if missing_inputs:
        status = "needs_inputs"
    elif missing_required_env:
        status = "needs_configuration"
    elif missing_caches:
        status = "review_caches"
    else:
        status = "ready"
    return {
        "status": status,
        "missing_inputs": missing_inputs,
        "missing_caches": missing_caches,
        "missing_required_env": missing_required_env,
        "optional_unavailable": optional_unavailable,
        "required_sources": list(workflow.required_sources),
    }


def _workflow_missing_required_env(workflow: WorkflowDefinition) -> list[str]:
    required: set[str] = set()
    for server_id in workflow.recommended_servers:
        spec = SERVER_BY_ID.get(server_id)
        if spec:
            required.update(key.name for key in spec.required_env)
    for step in workflow.steps:
        if step.blocking:
            required.update(step.required_env)
    return sorted(name for name in required if not os.environ.get(name))


def _workflow_optional_unavailable(workflow: WorkflowDefinition) -> list[dict[str, Any]]:
    unavailable: list[dict[str, Any]] = []
    for step in workflow.steps:
        missing = [name for name in step.required_env if not os.environ.get(name)]
        if missing and not step.blocking:
            unavailable.append(
                {
                    "server": step.server,
                    "tool": step.tool,
                    "missing_env": missing,
                    "status": "optional_unavailable",
                }
            )
    return unavailable


def _missing_inputs(required_identifiers: tuple[str, ...], inputs: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    normalized = {key.lower(): value for key, value in inputs.items() if value not in ("", None)}
    for requirement in required_identifiers:
        alternatives = [part.strip().lower() for part in requirement.replace(" or ", ",").split(",")]
        if not any(alt in normalized for alt in alternatives if alt):
            missing.append(requirement)
    return missing


def _missing_cache_notes(required_sources: tuple[str, ...], cache_entries: list[Any]) -> list[str]:
    if not cache_entries:
        return []
    notes: list[str] = []
    for source in required_sources:
        canonical_sources = _canonical_dataset_ids_for_source(source)
        matching = [
            entry
            for entry in cache_entries
            if isinstance(entry, dict) and entry.get("dataset_id") in canonical_sources
        ]
        if matching and not all((entry.get("readiness_status") or entry.get("status")) == "ready" for entry in matching):
            statuses = ", ".join(
                sorted({str(entry.get("readiness_status") or entry.get("status")) for entry in matching})
            )
            next_actions = [
                str(entry.get("next_action"))
                for entry in matching
                if str(entry.get("next_action") or "").strip()
            ]
            suffix = f"; next={next_actions[0]}" if next_actions else ""
            notes.append(f"{source}: {statuses}{suffix}")
        elif not matching:
            notes.append(f"{source}: not_checked")
    return notes


def _workflow_cache_readiness(workflow: WorkflowDefinition, cache_entries: list[Any]) -> dict[str, Any]:
    checks = _source_checks(workflow.required_sources, cache_entries)
    counts: dict[str, int] = {}
    blockers: list[dict[str, Any]] = []
    for check in checks:
        status = str(check.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
        if status not in {"ready", "live_api", "not_applicable"}:
            blockers.append(
                {
                    "source_id": check.get("source_id"),
                    "canonical_dataset_ids": check.get("canonical_dataset_ids", []),
                    "status": status,
                    "next_actions": check.get("next_actions", []),
                }
            )
    return {
        "status": "ready" if not blockers else "blocked",
        "readiness_model": "validated_source_readiness_not_file_existence",
        "required_sources": list(workflow.required_sources),
        "checks": checks,
        "status_counts": counts,
        "blockers": blockers,
        "missing_data_policy": "Missing cache data is an unknown, not a negative factual claim.",
    }


def _canonical_dataset_ids_for_source(source: str) -> list[str]:
    alias = WORKFLOW_SOURCE_ALIASES.get(source)
    if alias:
        return [str(item) for item in alias.get("canonical_dataset_ids", ())]
    return [source]


def _workflow_cache_entries(cache_status: dict[str, Any] | None) -> list[Any]:
    if not isinstance(cache_status, dict):
        return []
    datasets = cache_status.get("datasets")
    if isinstance(datasets, list) and datasets:
        return datasets
    entries = cache_status.get("entries")
    return entries if isinstance(entries, list) else []


_CACHE_STATUS_SEVERITY = {
    "corrupt": 90,
    "env_required": 80,
    "unsupported": 75,
    "licensed_import_required": 70,
    "manual_import_required": 70,
    "state_limited": 65,
    "partial": 60,
    "missing": 50,
    "stale": 40,
    "pattern": 20,
    "not_checked": 10,
    "live_api": 0,
    "not_applicable": 0,
    "ready": 0,
}


def _aggregate_cache_status(statuses: list[str]) -> str:
    normalized = {status or "unknown" for status in statuses}
    if not normalized:
        return "not_checked"
    if normalized == {"ready"}:
        return "ready"
    if normalized <= {"ready", "stale"} and "stale" in normalized:
        return "stale"
    if "ready" in normalized and "missing" in normalized:
        return "partial"
    if "ready" in normalized and any(status not in {"ready", "not_checked", "live_api", "not_applicable"} for status in normalized):
        return "partial"
    return max(normalized, key=lambda status: _CACHE_STATUS_SEVERITY.get(status, 55))


def _workflow_identity(inputs: dict[str, Any]):
    return identity_from_public_record(
        name=(
            inputs.get("entity_name")
            or inputs.get("facility_name")
            or inputs.get("system_name")
            or inputs.get("system_slug")
            or inputs.get("query")
            or inputs.get("organization")
            or inputs.get("provider_name")
            or inputs.get("owner_name")
            or ""
        ),
        entity_type=str(inputs.get("entity_type") or inputs.get("market") or ""),
        ccn=inputs.get("ccn") or "",
        npi=inputs.get("npi") or "",
        pecos_enrollment_id=inputs.get("enrollment_id") or "",
        ahrq_system_id=inputs.get("system_id") or inputs.get("ahrq_system_id") or "",
        owner_id=inputs.get("owner_associate_id") or inputs.get("owner_id") or "",
        address=inputs.get("address") or "",
        zip_code=inputs.get("zip_code") or inputs.get("zip") or "",
        source_name="workflow_input",
    )


def _workflow_identity_map(
    workflow: WorkflowDefinition,
    identity: dict[str, Any],
    inputs: dict[str, Any],
) -> dict[str, Any]:
    join_keys = []
    for join_field in workflow.identity_join_keys:
        value = _identity_value(join_field, identity, inputs)
        join_keys.append(
            {
                "field": join_field,
                "value": value,
                "status": "provided" if value else "missing",
                "used_by": _steps_using_identity_field(workflow.steps, join_field),
            }
        )

    return {
        "seed_identity": identity,
        "join_keys": join_keys,
        "source_claims": [
            {
                **_step_identity_contract(step, workflow.identity_join_keys),
                "server": step.server,
                "tool": step.tool,
                "extract_identity_fields": _step_identity_fields(step, workflow.identity_join_keys),
                "required_evidence": list(step.output_contract),
            }
            for step in workflow.steps
        ],
        "resolution_plan": _identity_resolution_plan(workflow),
        "merge_policy": _workflow_identity_merge_policy(workflow),
        "identity_strategy": list(workflow.identity_strategy),
        "conflict_policy": [
            "Prefer exact public identifiers over names for joins.",
            "Record source-specific names as aliases, not canonical proof, unless an exact identifier also matches.",
            "Carry conflicts forward in the identity map instead of overwriting identifiers.",
            "Do not substitute adjacent records for exact source-backed facts.",
        ],
        "unresolved_identifiers": identity.get("unresolved_identifiers", []),
    }


def _workflow_identity_merge_policy(workflow: WorkflowDefinition) -> dict[str, Any]:
    exact_fields = [field for field in IDENTITY_EXACT_FIELDS if field in workflow.identity_join_keys]
    candidate_fields = [
        field
        for field in (
            *IDENTITY_CANDIDATE_FIELDS,
            "entity_name",
            "facility_name",
            "owner_name",
            "organization",
            "state",
            "market",
            "service_line",
            "county_fips",
            "zcta",
            "lat",
            "lon",
            "radius_minutes",
            "catchment_minutes",
            "demand_id",
        )
        if field in workflow.identity_join_keys or field in {"canonical_name", "address", "zip_code"}
    ]
    return {
        "helper": "shared.utils.healthcare_identity.merge_healthcare_identities",
        "exact_identifier_fields": exact_fields,
        "candidate_alias_fields": candidate_fields,
        "merge_rule": "merge_exact_identifiers_only_when_non_conflicting",
        "candidate_rule": "record_name_address_zip_and_state_as_alias_or_conflict_context",
        "conflict_rule": "preserve_conflicts_in_identity_map_do_not_overwrite_source_identifiers",
        "reporting_rule": "report fact rows must keep the owning tool evidence receipt and identity_fields together",
    }


_EXACT_IDENTITY_FIELDS = {
    "ccn",
    "npi",
    "pecos_enrollment_id",
    "owner_id",
    "ahrq_system_id",
    "system_id",
    "measure_id",
}


def _identity_resolution_plan(workflow: WorkflowDefinition) -> list[dict[str, Any]]:
    """Return ordered merge guidance for workflow-level identity maps."""

    plan: list[dict[str, Any]] = []
    for index, step in enumerate(workflow.steps, start=1):
        consumes = _step_identity_fields(step, workflow.identity_join_keys)
        produces = _step_identity_outputs(step, workflow.identity_join_keys)
        exact_fields = sorted(
            field
            for field in set((*consumes, *produces))
            if field in _EXACT_IDENTITY_FIELDS
        )
        candidate_fields = sorted(
            field
            for field in set((*consumes, *produces))
            if field not in _EXACT_IDENTITY_FIELDS
        )
        if exact_fields:
            merge_action = "merge_on_exact_identifier"
        elif candidate_fields:
            merge_action = "record_candidate_alias_requires_source_review"
        else:
            merge_action = "context_only_no_entity_merge"
        plan.append(
            {
                "order": index,
                "server": step.server,
                "tool": step.tool,
                "qualified_tool": f"{step.server}.{step.tool}",
                "merge_action": merge_action,
                "exact_join_fields": exact_fields,
                "candidate_fields": candidate_fields,
                "identity_output_paths": _step_identity_output_paths(step),
                "evidence_path": "result.evidence",
                "conflict_checks": _identity_conflict_checks(exact_fields, candidate_fields),
            }
        )
    return plan


def _identity_conflict_checks(exact_fields: list[str], candidate_fields: list[str]) -> list[str]:
    checks: list[str] = []
    if exact_fields:
        checks.append("Reject or flag source rows when exact identifiers disagree.")
    if any(field in candidate_fields for field in ("canonical_name", "entity_name", "owner_name")):
        checks.append("Record source-specific names as aliases unless an exact identifier also matches.")
    if any(field in candidate_fields for field in ("address", "zip_code", "state")):
        checks.append("Use address, ZIP, and state as disambiguation fields, not primary merge keys.")
    if not checks:
        checks.append("Preserve evidence as context; do not merge identity from this step alone.")
    return checks


def _identity_value(field: str, identity: dict[str, Any], inputs: dict[str, Any]) -> Any:
    aliases = {
        "entity_name": "canonical_name",
        "facility_name": "canonical_name",
        "organization": "canonical_name",
        "owner_name": "canonical_name",
        "pecos_enrollment_id": "pecos_enrollment_id",
        "measure_id": "measure_id",
        "system_id": "ahrq_system_id",
        "system_slug": "canonical_name",
        "zip": "zip_code",
    }
    identity_key = aliases.get(field, field)
    return inputs.get(field) or inputs.get(identity_key) or identity.get(identity_key) or ""


def _steps_using_identity_field(steps: tuple[WorkflowToolStep, ...], field: str) -> list[str]:
    used_by: list[str] = []
    for step in steps:
        input_tokens = _step_input_tokens(step)
        if _identity_field_matches_inputs(field, input_tokens):
            used_by.append(f"{step.server}.{step.tool}")
    return used_by


def _step_identity_fields(step: WorkflowToolStep, join_keys: tuple[str, ...]) -> list[str]:
    input_tokens = _step_input_tokens(step)
    fields = [
        field
        for field in join_keys
        if _identity_field_matches_inputs(field, input_tokens)
    ]
    if not fields and step.server in {"cms-facility", "hospital-quality", "financial-intelligence", "workforce-analytics"}:
        fields.append("ccn")
    if not fields and step.server == "provider-enrollment":
        fields.extend([field for field in ("npi", "ccn", "pecos_enrollment_id", "owner_id") if field in join_keys])
    return fields


def _step_input_tokens(step: WorkflowToolStep) -> set[str]:
    tokens: set[str] = set()
    for label in (*step.required_inputs, *step.optional_inputs):
        tokens.update(_input_alternatives(label))
    return tokens


def _identity_field_matches_inputs(field: str, input_tokens: set[str]) -> bool:
    aliases = {
        "ahrq_system_id": {"ahrq_system_id", "system_id"},
        "system_id": {"system_id"},
        "system_slug": {"system_slug"},
        "canonical_name": {
            "entity_name",
            "facility_name",
            "system_name",
            "organization",
            "owner_name",
            "provider_name",
            "query",
        },
        "entity_name": {"entity_name", "provider_name", "query"},
        "facility_name": {"facility_name", "provider_name", "query"},
        "organization": {"organization", "query"},
        "owner_name": {"owner_name"},
        "pecos_enrollment_id": {"pecos_enrollment_id", "enrollment_id"},
        "owner_id": {"owner_id", "owner_associate_id", "owner_pac_id"},
        "zip_code": {"zip_code", "zip"},
        "measure_id": {"measure_id", "measure"},
        "ccn": {"ccn", "provider_ccn"},
        "npi": {"npi"},
        "state": {"state"},
        "uei": {"uei"},
        "cage_code": {"cage_code"},
        "country": {"country"},
        "address": {"address"},
        "county_fips": {"county_fips"},
        "zcta": {"zcta"},
        "market": {"market"},
        "service_line": {"service_line"},
        "lat": {"lat", "latitude"},
        "lon": {"lon", "longitude"},
        "radius_minutes": {"radius_minutes", "catchment_minutes"},
        "catchment_minutes": {"catchment_minutes"},
        "demand_id": {"demand_id", "demand_points"},
    }
    return bool((aliases.get(field, {field}) | {field}) & input_tokens)


def _step_match_policy(step: WorkflowToolStep, join_keys: tuple[str, ...]) -> str:
    fields = _step_identity_fields(step, join_keys)
    if any(field in {"ccn", "npi", "pecos_enrollment_id", "owner_id", "measure_id"} for field in fields):
        return "exact_identifier_required_for_report_fact"
    if fields:
        return "candidate_match_requires_source_review"
    return "context_step_no_entity_merge"


__all__ = [
    "WORKFLOW_DEFINITIONS",
    "WorkflowDefinition",
    "WorkflowToolStep",
    "build_workflow_plan",
    "format_workflow_plan",
    "list_workflow_plans",
    "parse_workflow_inputs",
    "print_workflow_plan",
    "validate_workflow_contracts",
    "validate_workflow_tool_references",
]
