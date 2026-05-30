"""Capability gates for broad MCP server tool surfaces."""

from __future__ import annotations

TOOL_CLUSTERS: dict[str, dict[str, tuple[str, ...]]] = {
    "public-records": {
        "leie-exclusions": (
            "check_leie_npi",
            "search_leie_individual",
            "search_leie_entity",
            "screen_leie_batch",
            "get_leie_metadata",
        ),
        "sam-exclusions": (
            "search_sam_exclusions",
            "check_sam_exclusion_identifier",
            "screen_sam_exclusions_batch",
            "get_sam_exclusions_metadata",
        ),
        "state-public-records": (
            "search_phc4_public_reports",
            "get_phc4_hospital_performance",
            "get_phc4_financial_analysis",
            "get_phc4_common_procedure_profile",
        ),
        "breach-records": (
            "get_breach_history",
            "search_ocr_enforcement_actions",
            "search_sec_cyber_disclosures",
            "get_state_ag_breach_notice_sources",
            "search_state_breach_notices",
            "get_cyber_incident_profile",
        ),
        "cyber-source-status": (
            "get_cyber_attestation_source_status",
            "get_cisa_kev_context_status",
        ),
        "accreditation-interop": (
            "get_accreditation",
            "get_interop_status",
            "search_usaspending",
            "search_sam_gov",
        ),
    },
    "workforce-analytics": {
        "workforce-supply": (
            "get_bls_employment",
            "get_hrsa_workforce",
            "get_staffing_benchmarks",
            "get_cost_report_staffing",
            "resolve_hospital_beds",
            "get_hospital_staffing_productivity",
            "compare_hospital_staffing_productivity",
        ),
        "training-programs": (
            "get_gme_profile",
            "get_acgme_source_status",
            "get_acgme_program",
            "search_acgme_programs",
            "get_residency_programs",
            "get_teaching_intensity",
        ),
        "labor-activity": ("search_union_activity",),
        "operations-throughput": (
            "get_snf_nursing_hprd",
            "get_public_throughput_profile",
            "compare_public_throughput",
            "get_ed_volume_profile",
            "get_or_procedure_volume_profile",
        ),
    },
    "financial-intelligence": {
        "nonprofit-finance": ("search_form990", "get_form990_details"),
        "sec-muni-finance": (
            "search_sec_filings",
            "get_sec_filing",
            "search_muni_bonds",
            "get_muni_bond_details",
            "parse_audited_financial_pdf",
        ),
        "public-financial-health": (
            "get_public_financial_health_profile",
            "get_uncompensated_care_profile",
            "get_charity_care_profile",
            "get_bad_debt_profile",
        ),
    },
    "discovery": {
        "dataset-discovery": (
            "list_datasets",
            "get_dataset",
            "get_dataset_schema",
            "get_dataset_source",
            "get_cache_status",
            "validate_dataset_catalog",
        ),
        "workflow-discovery": (
            "list_runbooks",
            "get_runbook",
            "list_workflows",
            "get_workflow_plan",
            "list_presets",
            "get_preset_plan",
        ),
    },
    "research-trials": {
        "nih-reporter": ("search_nih_projects", "get_nih_project", "profile_research_funding"),
        "clinical-trials": (
            "search_clinical_trials",
            "get_clinical_trial",
            "inventory_clinical_trial_sponsors",
            "inventory_clinical_trial_sites",
            "profile_research_activity",
        ),
    },
}


def clusters_for_server(server_id: str) -> dict[str, tuple[str, ...]]:
    """Return capability clusters for a server, if it needs gating."""

    return TOOL_CLUSTERS.get(server_id, {})
