"""Tests for executable task-first workflow plans."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import shared.utils.workflows as workflows
from shared.utils.mcp_response import REPORT_SOURCE_METADATA_FIELDS, validate_report_ingest_payload
from shared.utils.workflows import (
    build_workflow_plan,
    format_workflow_plan,
    list_workflow_plans,
    parse_workflow_inputs,
    validate_workflow_contracts,
    validate_workflow_tool_references,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_list_workflow_plans_includes_flagship_screening() -> None:
    payload = list_workflow_plans()
    workflow_ids = {workflow["workflow_id"] for workflow in payload["workflows"]}

    assert "compliance_exclusion_screening" in workflow_ids
    assert "hospital_competitive_profile" in workflow_ids
    for workflow in payload["workflows"]:
        assert workflow["required_identifiers"], workflow
        assert workflow["identity_join_keys"], workflow
        assert workflow["identity_strategy"], workflow
        assert workflow["required_sources"], workflow
        assert workflow["source_resolution"], workflow
        assert {row["source_id"] for row in workflow["source_resolution"]} >= set(workflow["required_sources"])
        assert workflow["recommended_servers"], workflow
        assert workflow["step_count"] >= 1, workflow
        assert workflow["report_fact_row_count"] >= 1, workflow
        assert workflow["validation"]["tool_references"]["status"] == "ok", workflow
        assert workflow["validation"]["tool_references"]["issue_count"] == 0, workflow
        assert workflow["validation"]["tool_references"]["method"] == "registry_module_signature_ast", workflow
        assert workflow["validation"]["report_contracts"]["status"] == "ok", workflow
        assert workflow["validation"]["report_contracts"]["issue_count"] == 0, workflow
        assert workflow["validation"]["report_contracts"]["method"] == "workflow_report_contract_static", workflow
        examples = workflow["examples"]
        assert examples["inputs"]
        assert examples["cli_command"].startswith(f"hc-mcp workflow {workflow['workflow_id']} --input ")
        assert examples["json_command"].startswith(f"hc-mcp workflow {workflow['workflow_id']} --inputs-json ")
        assert examples["mcp_tool_call"]["server"] == "discovery"
        assert examples["mcp_tool_call"]["tool"] == "get_workflow_plan"
        assert examples["mcp_tool_call"]["arguments"]["workflow_id"] == workflow["workflow_id"]
    system_reconciliation = next(
        workflow for workflow in payload["workflows"] if workflow["workflow_id"] == "system_reconciliation"
    )
    aliases = {row["source_id"]: row for row in system_reconciliation["source_resolution"] if row["status"] == "alias"}
    assert aliases["public_web"]["canonical_dataset_ids"] == ["web_intelligence"]
    assert "ahrq_system_id" in system_reconciliation["identity_join_keys"]
    assert any("CCN/NPI/PECOS" in item for item in system_reconciliation["identity_strategy"])


def test_build_workflow_plan_returns_tool_sequence_evidence_and_identity(monkeypatch) -> None:
    monkeypatch.delenv("SAM_GOV_API_KEY", raising=False)

    plan = build_workflow_plan(
        "compliance_exclusion_screening",
        inputs={"npi": "1234567893", "entity_name": "Thomas Jefferson University Hospitals"},
    )

    assert plan["workflow_id"] == "compliance_exclusion_screening"
    assert plan["readiness"]["status"] == "ready"
    assert plan["steps"][0]["server"] == "public-records"
    assert plan["steps"][0]["stdio_command"] == "hc-mcp public-records"
    assert plan["steps"][1]["tool"] == "check_leie_npi"
    assert plan["steps"][1]["mcp_call"] == {
        "server": "public-records",
        "tool": "check_leie_npi",
        "qualified_tool": "public-records.check_leie_npi",
        "arguments_template": {"npi": "1234567893"},
        "resolved_arguments": {"npi": "1234567893"},
    }
    assert plan["steps"][1]["identity_contract"]["consumes"] == ["npi"]
    assert "npi" in plan["steps"][1]["identity_contract"]["produces"]
    assert "result.identity" in plan["steps"][1]["identity_contract"]["output_paths"]
    assert "result.identity_map" in plan["steps"][1]["identity_contract"]["output_paths"]
    assert plan["steps"][1]["evidence_contract"]["result_evidence_path"] == "result.evidence"
    assert "result.records[].evidence" in plan["steps"][1]["evidence_contract"]["row_evidence_paths"]
    assert "source_url" in plan["steps"][1]["evidence_contract"]["required_receipt_fields"]
    assert plan["steps"][2]["mcp_call"]["arguments_template"]["entity_name"] == "Thomas Jefferson University Hospitals"
    assert plan["evidence"]["match_basis"] == "workflow_id_exact"
    assert plan["identity"]["npi"] == "1234567893"
    assert plan["identity_map"]["join_keys"][0]["field"] == "npi"
    assert plan["identity_map"]["join_keys"][0]["status"] == "provided"
    assert "public-records.check_leie_npi" in plan["identity_map"]["join_keys"][0]["used_by"]
    assert {row["field"] for row in plan["identity_map"]["join_keys"]} >= {
        "npi",
        "ccn",
        "entity_name",
        "state",
        "uei",
        "cage_code",
        "pecos_enrollment_id",
    }
    assert plan["identity_map"]["merge_policy"]["helper"] == (
        "shared.utils.healthcare_identity.merge_healthcare_identities"
    )
    assert "npi" in plan["identity_map"]["merge_policy"]["exact_identifier_fields"]
    assert plan["identity_map"]["merge_policy"]["merge_rule"] == "merge_exact_identifiers_only_when_non_conflicting"
    assert plan["identity_map"]["conflict_policy"]
    assert plan["workflow_contract_validation"]["status"] == "ok"
    assert plan["tool_reference_validation"]["status"] == "ok"
    assert {
        row["source_id"]: row["status"]
        for row in plan["source_resolution"]
    }["cms_pecos_public_provider_enrollment"] == "registry_dataset"
    assert plan["steps"][1]["tool_reference"]["status"] == "ok"
    assert plan["steps"][1]["tool_reference"]["module"] == "servers.public_records.server"
    assert plan["readiness"]["optional_unavailable"][0]["tool"] == "search_sam_exclusions"
    assert plan["readiness"]["optional_unavailable"][0]["missing_env"] == ["SAM_GOV_API_KEY"]
    sam_step = {step["tool"]: step for step in plan["steps"]}["search_sam_exclusions"]
    assert sam_step["execution_readiness"]["status"] == "optional_unavailable"
    assert sam_step["execution_readiness"]["blocking"] is False
    assert plan["report_fact_rows"]
    assert plan["report_ingest_contract"]["fact_rows"]
    fact_rows_by_label = {row["label"]: row for row in plan["report_ingest_contract"]["fact_rows"]}
    assert fact_rows_by_label["LEIE entity-name potential matches"]["value_path"] == (
        "public_records.search_leie_entity.records"
    )
    assert fact_rows_by_label["LEIE entity-name potential matches"]["identity_fields"] == [
        "entity_name",
        "state",
        "npi",
    ]
    assert fact_rows_by_label["SAM.gov exclusion status"]["value_path"] == (
        "public_records.search_sam_exclusions.records"
    )
    assert fact_rows_by_label["SAM.gov exclusion status"]["identity_fields"] == [
        "entity_name",
        "state",
        "npi",
        "uei",
        "cage_code",
    ]
    pecos_fact = fact_rows_by_label["PECOS enrollment join keys"]
    assert pecos_fact["value_path"] == "provider_enrollment.search_provider_enrollment.enrollments"
    assert pecos_fact["evidence_path"] == "provider_enrollment.search_provider_enrollment.enrollments[].evidence"
    assert pecos_fact["identity_map_path"] == "provider_enrollment.search_provider_enrollment.identity_map"
    leie_fact = plan["report_ingest_contract"]["fact_rows"][0]
    assert leie_fact["identity_path"] == "public_records.check_leie_npi.identity"
    assert leie_fact["identity_map_path"] == "public_records.check_leie_npi.identity_map"
    assert leie_fact["evidence_path"] == "public_records.check_leie_npi.evidence"
    assert leie_fact["source_metadata_path"] == "public_records.check_leie_npi.source_metadata"
    assert "cache_freshness" in leie_fact["required_evidence_fields"]
    validate_report_ingest_payload(plan["report_ingest_contract"])
    json.dumps(plan)


def test_parse_workflow_inputs_merges_json_and_key_value_overrides() -> None:
    inputs = parse_workflow_inputs(
        inputs_json='{"ccn": "390223", "facility_name": "Old Name"}',
        input_items=["facility_name=Thomas Jefferson University Hospital", "measure=clabsi_sir"],
    )

    assert inputs == {
        "ccn": "390223",
        "facility_name": "Thomas Jefferson University Hospital",
        "measure": "clabsi_sir",
    }


def test_hc_mcp_workflow_cli_accepts_concrete_inputs() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "servers._launcher",
            "workflow",
            "quality_measure_lookup",
            "--input",
            "ccn=390223",
            "--inputs-json",
            '{"measure": "clabsi_sir"}',
            "--json",
        ],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    plan = json.loads(result.stdout)

    assert plan["readiness"]["status"] == "ready"
    assert plan["examples"]["inputs"] == {
        "dataset_id": "cms_hospital_quality",
        "ccn": "390223",
        "measure": "clabsi_sir",
    }
    assert plan["examples"]["cli_command"] == (
        "hc-mcp workflow quality_measure_lookup --input dataset_id=cms_hospital_quality "
        "--input ccn=390223 --input measure=clabsi_sir"
    )
    assert plan["examples"]["json_command"] == (
        "hc-mcp workflow quality_measure_lookup --inputs-json "
        "'{\"ccn\":\"390223\",\"dataset_id\":\"cms_hospital_quality\",\"measure\":\"clabsi_sir\"}' --json"
    )
    assert plan["examples"]["mcp_tool_call"] == {
        "server": "discovery",
        "tool": "get_workflow_plan",
        "arguments": {
            "workflow_id": "quality_measure_lookup",
            "inputs": {
                "dataset_id": "cms_hospital_quality",
                "ccn": "390223",
                "measure": "clabsi_sir",
            },
        },
    }
    assert plan["steps"][1]["mcp_call"]["arguments_template"] == {
        "ccn": "390223",
        "measure": "clabsi_sir",
        "measure_id": "<measure_id>",
    }
    assert plan["steps"][1]["mcp_call"]["resolved_arguments"] == {
        "ccn": "390223",
        "measure": "clabsi_sir",
    }
    assert plan["identity_map"]["join_keys"][0]["value"] == "390223"


def test_hc_mcp_workflow_cli_rejects_inputs_without_workflow_name() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "servers._launcher",
            "workflow",
            "--input",
            "ccn=390223",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 2
    assert "--input and --inputs-json require a workflow name" in result.stderr


def test_hospital_workflow_identity_map_links_cross_server_ccn_consumers() -> None:
    plan = build_workflow_plan(
        "hospital_competitive_profile",
        inputs={"ccn": "390223", "facility_name": "Thomas Jefferson University Hospital"},
    )
    by_field = {row["field"]: row for row in plan["identity_map"]["join_keys"]}

    assert by_field["ccn"]["value"] == "390223"
    assert by_field["ccn"]["status"] == "provided"
    assert "hospital-quality.get_quality_scores" in by_field["ccn"]["used_by"]
    assert "financial-intelligence.get_public_financial_health_profile" in by_field["ccn"]["used_by"]
    assert "workforce-analytics.get_hospital_staffing_productivity" in by_field["ccn"]["used_by"]
    by_tool = {step["tool"]: step for step in plan["steps"]}
    assert by_tool["get_quality_scores"]["identity_contract"]["consumes"] == ["ccn"]
    assert "ccn" in by_tool["get_quality_scores"]["identity_contract"]["produces"]
    assert "result.identity" in by_tool["get_quality_scores"]["identity_contract"]["output_paths"]
    assert "result.identity_map" in by_tool["get_quality_scores"]["identity_contract"]["output_paths"]
    assert by_tool["get_public_financial_health_profile"]["identity_contract"]["match_policy"] == "exact_identifier_required_for_report_fact"
    assert "result.identity_map" in by_tool["get_public_financial_health_profile"]["identity_contract"]["output_paths"]
    financial_paths = by_tool["get_public_financial_health_profile"]["evidence_contract"]["row_evidence_paths"]
    assert "result.hcris.metric_evidence.*" in financial_paths
    assert "result.form990_schedule_h.metric_evidence.*" in financial_paths
    assert "result.ahrq_hfmd.metric_evidence.*" in financial_paths
    assert "result.identity_map" in by_tool["get_hospital_staffing_productivity"]["identity_contract"]["output_paths"]
    staffing_paths = by_tool["get_hospital_staffing_productivity"]["evidence_contract"]["row_evidence_paths"]
    assert "result.departments[].evidence" in staffing_paths
    assert "result.bed_source.selected_candidate_evidence" in staffing_paths
    assert "result.bed_source.candidates[].evidence" in staffing_paths
    system_step = by_tool["get_system_facilities"]
    assert system_step["blocking"] is False
    assert system_step["mcp_call"]["arguments_template"] == {
        "system_id": "<system_id>",
        "facility_type": "<facility_type>",
    }
    facts = {row["label"]: row for row in plan["report_ingest_contract"]["fact_rows"]}
    assert facts["System affiliation"]["value_path"] == (
        "health_system_profiler.get_system_facilities.inpatient_facilities"
    )
    assert facts["System affiliation"]["evidence_path"] == (
        "health_system_profiler.get_system_facilities.inpatient_facilities[].evidence"
    )
    assert facts["System affiliation"]["identity_map_path"] == (
        "health_system_profiler.get_system_facilities.identity_map"
    )
    assert facts["Staffing productivity"]["value_path"] == (
        "workforce_analytics.get_hospital_staffing_productivity.departments"
    )
    assert facts["Staffing productivity"]["evidence_path"] == (
        "workforce_analytics.get_hospital_staffing_productivity.departments[].evidence"
    )
    assert facts["Staffing productivity"]["identity_map_path"] == (
        "workforce_analytics.get_hospital_staffing_productivity.identity_map"
    )
    assert any(row["label"] == "Public financial profile" for row in plan["report_fact_rows"])
    assert any(claim["match_policy"] == "exact_identifier_required_for_report_fact" for claim in plan["identity_map"]["source_claims"])
    assert any("produces" in claim and "output_paths" in claim for claim in plan["identity_map"]["source_claims"])


def test_named_facility_quality_and_finance_workflows_expose_identity_contracts() -> None:
    facility = build_workflow_plan("facility_profile", inputs={"ccn": "390223", "facility_name": "Jefferson"})
    quality = build_workflow_plan("quality_profile", inputs={"ccn": "390223", "measure_id": "HAI_1_SIR"})
    finance = build_workflow_plan("finance_profile", inputs={"ccn": "390223", "entity_name": "Jefferson"})

    assert facility["workflow_id"] == "facility_profile"
    assert quality["workflow_id"] == "quality_profile"
    assert finance["workflow_id"] == "finance_profile"

    facility_by_field = {row["field"]: row for row in facility["identity_map"]["join_keys"]}
    quality_by_field = {row["field"]: row for row in quality["identity_map"]["join_keys"]}
    finance_by_field = {row["field"]: row for row in finance["identity_map"]["join_keys"]}

    assert facility_by_field["ccn"]["value"] == "390223"
    assert "cms-facility.get_facility" in facility_by_field["ccn"]["used_by"]
    assert "service-area.compute_service_area" in facility_by_field["ccn"]["used_by"]
    assert any(row["label"] == "Service-area context" for row in facility["report_fact_rows"])
    facility_facts = {row["label"]: row for row in facility["report_ingest_contract"]["fact_rows"]}
    facility_system_fact = facility_facts["System affiliation context"]
    assert facility_system_fact["value_path"] == (
        "health_system_profiler.get_system_facilities.inpatient_facilities"
    )
    assert facility_system_fact["evidence_path"] == (
        "health_system_profiler.get_system_facilities.inpatient_facilities[].evidence"
    )
    system_step = {step["tool"]: step for step in facility["steps"]}["get_system_facilities"]
    assert system_step["mcp_call"]["arguments_template"] == {
        "system_id": "<system_id>",
        "facility_type": "<facility_type>",
    }
    assert "result.inpatient_facilities[].evidence" in system_step["evidence_contract"]["row_evidence_paths"]
    assert "result.sub_entities[].evidence" in system_step["evidence_contract"]["row_evidence_paths"]
    market_scan = build_workflow_plan(
        "market_community_health_scan",
        inputs={"zip_code": "19107", "zcta": "19107", "market": "Philadelphia"},
    )
    market_by_tool = {step["tool"]: step for step in market_scan["steps"]}
    market_facts = {row["label"]: row for row in market_scan["report_ingest_contract"]["fact_rows"]}
    assert "result.market_profile.aggregated_measures[].evidence" in market_by_tool["get_market_community_profile"][
        "evidence_contract"
    ]["row_evidence_paths"]
    assert "result.adjacent_zcta_rows[].evidence" in market_by_tool["get_zcta_adjacency"]["evidence_contract"][
        "row_evidence_paths"
    ]
    assert market_facts["Community health estimate"]["value_path"] == (
        "community_health.get_market_community_profile.market_profile.aggregated_measures"
    )
    assert market_facts["Community health estimate"]["evidence_path"] == (
        "community_health.get_market_community_profile.market_profile.aggregated_measures[].evidence"
    )
    assert market_facts["Adjacent ZCTA topology"]["evidence_path"] == (
        "geo_demographics.get_zcta_adjacency.adjacent_zcta_rows[].evidence"
    )
    assert market_facts["Access score"]["value_path"] == "drive_time.compute_accessibility_score.results"
    assert market_facts["Access score"]["evidence_path"] == (
        "drive_time.compute_accessibility_score.results[].evidence"
    )
    assert market_facts["Access score"]["identity_map_path"] == (
        "drive_time.compute_accessibility_score.identity_map"
    )
    assert "result.results[].evidence" in market_by_tool["compute_accessibility_score"]["evidence_contract"][
        "row_evidence_paths"
    ]
    market_by_field = {row["field"]: row for row in market_scan["identity_map"]["join_keys"]}
    assert "drive-time.compute_accessibility_score" in market_by_field["demand_id"]["used_by"]
    assert "drive-time.compute_accessibility_score" in market_by_field["catchment_minutes"]["used_by"]
    assert "demand_id" in market_by_tool["compute_accessibility_score"]["identity_contract"]["produces"]

    assert quality_by_field["ccn"]["value"] == "390223"
    assert "hospital-quality.get_quality_measure_rows" in quality_by_field["ccn"]["used_by"]
    assert quality_by_field["measure_id"]["status"] == "provided"
    assert any(row["label"] == "Exact CMS quality measure row" for row in quality["report_fact_rows"])
    quality_facts = {row["label"]: row for row in quality["report_ingest_contract"]["fact_rows"]}
    assert quality_facts["CMS readmission context"]["value_path"] == (
        "hospital_quality.get_readmission_data.conditions"
    )
    assert quality_facts["CMS readmission context"]["evidence_path"] == (
        "hospital_quality.get_readmission_data.conditions[].evidence"
    )
    assert quality_facts["CMS HAC safety domain context"]["value_path"] == (
        "hospital_quality.get_safety_scores.domain_evidence"
    )
    assert quality_facts["CMS HAC safety domain context"]["evidence_path"] == (
        "hospital_quality.get_safety_scores.domain_evidence[].evidence"
    )
    assert quality_facts["CMS HCAHPS patient-experience domain context"]["value_path"] == (
        "hospital_quality.get_patient_experience.domains"
    )
    assert quality_facts["CMS HCAHPS patient-experience domain context"]["evidence_path"] == (
        "hospital_quality.get_patient_experience.domains[].evidence"
    )
    assert quality_facts["Exact CMS quality measure row"]["evidence_path"] == (
        "hospital_quality.get_quality_measure_rows.rows[].evidence"
    )
    quality_measure_step = {step["tool"]: step for step in quality["steps"]}["get_quality_measure_rows"]
    assert "result.rows[].evidence" in quality_measure_step["evidence_contract"]["row_evidence_paths"]
    safety_step = {step["tool"]: step for step in quality["steps"]}["get_safety_scores"]
    readmission_step = {step["tool"]: step for step in quality["steps"]}["get_readmission_data"]
    patient_experience_step = {step["tool"]: step for step in quality["steps"]}["get_patient_experience"]
    assert "result.domain_evidence[].evidence" in safety_step["evidence_contract"]["row_evidence_paths"]
    assert "result.conditions[].evidence" in readmission_step["evidence_contract"]["row_evidence_paths"]
    assert "result.domains[].evidence" in patient_experience_step["evidence_contract"]["row_evidence_paths"]

    assert finance_by_field["ccn"]["value"] == "390223"
    assert "financial-intelligence.get_public_financial_health_profile" in finance_by_field["ccn"]["used_by"]
    assert "financial-intelligence.get_uncompensated_care_profile" in finance_by_field["ccn"]["used_by"]
    assert "financial-intelligence.get_charity_care_profile" in finance_by_field["ccn"]["used_by"]
    assert "financial-intelligence.get_bad_debt_profile" in finance_by_field["ccn"]["used_by"]
    assert "hospital-quality.get_financial_profile" in finance_by_field["ccn"]["used_by"]
    assert any(row["label"] == "CMS cost-report operating context" for row in finance["report_fact_rows"])
    finance_facts = {row["label"]: row for row in finance["report_ingest_contract"]["fact_rows"]}
    assert finance_facts["Public financial health profile"]["identity_map_path"] == (
        "financial_intelligence.get_public_financial_health_profile.identity_map"
    )
    assert finance_facts["Public financial source metric"]["evidence_path"] == (
        "financial_intelligence.get_public_financial_health_profile.hcris.metric_evidence"
    )
    assert finance_facts["Public financial source metric"]["source_metadata_path"] == (
        "financial_intelligence.get_public_financial_health_profile.hcris.source_metadata"
    )
    assert finance_facts["Promoted uncompensated-care metric"]["evidence_path"] == (
        "financial_intelligence.get_uncompensated_care_profile.metric_evidence"
    )
    assert finance_facts["Promoted uncompensated-care metric"]["identity_map_path"] == (
        "financial_intelligence.get_uncompensated_care_profile.identity_map"
    )
    assert finance_facts["Promoted charity-care metric"]["evidence_path"] == (
        "financial_intelligence.get_charity_care_profile.metric_evidence"
    )
    assert finance_facts["Promoted bad-debt metric"]["evidence_path"] == (
        "financial_intelligence.get_bad_debt_profile.metric_evidence"
    )
    finance_by_tool = {step["tool"]: step for step in finance["steps"]}
    for tool_name in ("get_uncompensated_care_profile", "get_charity_care_profile", "get_bad_debt_profile"):
        assert "result.metric_evidence.*" in finance_by_tool[tool_name]["evidence_contract"]["row_evidence_paths"]
        assert finance_by_tool[tool_name]["blocking"] is False
    assert finance_facts["Public throughput denominator"]["identity_map_path"] == (
        "workforce_analytics.get_public_throughput_profile.identity_map"
    )
    throughput_paths = {step["tool"]: step for step in finance["steps"]}["get_public_throughput_profile"][
        "evidence_contract"
    ]["row_evidence_paths"]
    assert "result.bed_source.selected_candidate_evidence" in throughput_paths
    assert "result.bed_source.candidates[].evidence" in throughput_paths
    assert "result.metric_evidence.*" in throughput_paths
    assert finance_facts["Public throughput metric"]["evidence_path"] == (
        "workforce_analytics.get_public_throughput_profile.metric_evidence"
    )
    assert finance_facts["Public throughput metric"]["source_metadata_path"] == (
        "workforce_analytics.get_public_throughput_profile.evidence"
    )
    assert finance_facts["CMS cost-report operating context"]["identity_map_path"] == (
        "hospital_quality.get_financial_profile.identity_map"
    )
    assert finance_facts["Public financial health profile"]["evidence_path"] == (
        "financial_intelligence.get_public_financial_health_profile.evidence"
    )

    for plan in (facility, quality, finance):
        assert plan["identity_map"]["resolution_plan"]
        assert plan["report_ingest_contract"]["fact_rows"]
        validate_report_ingest_payload(plan["report_ingest_contract"])


def test_system_reconciliation_workflow_has_ordered_identity_resolution_plan() -> None:
    plan = build_workflow_plan(
        "system_reconciliation",
        inputs={
            "query": "Jefferson Health",
            "system_name": "Jefferson Health",
            "system_slug": "jefferson-health",
            "system_id": "SYS_JEFFERSON",
            "ccn": "390223",
            "state": "PA",
        },
    )
    by_field = {row["field"]: row for row in plan["identity_map"]["join_keys"]}
    by_tool = {step["tool"]: step for step in plan["steps"]}
    resolution_by_tool = {
        resolution["tool"]: resolution
        for resolution in plan["identity_map"]["resolution_plan"]
    }

    assert plan["workflow_id"] == "system_reconciliation"
    assert by_field["system_id"]["value"] == "SYS_JEFFERSON"
    assert by_field["ahrq_system_id"]["value"] == "SYS_JEFFERSON"
    assert by_field["ccn"]["value"] == "390223"
    assert by_tool["reconcile_system_facilities"]["mcp_call"]["arguments_template"] == {
        "system_slug": "jefferson-health",
        "as_of_date": "<as_of_date>",
    }
    assert resolution_by_tool["reconcile_system_facilities"]["merge_action"] == "merge_on_exact_identifier"
    assert "ccn" in resolution_by_tool["reconcile_system_facilities"]["exact_join_fields"]
    assert resolution_by_tool["scrape_system_profile"]["merge_action"] == "record_candidate_alias_requires_source_review"
    assert resolution_by_tool["scrape_system_profile"]["exact_join_fields"] == []
    assert "canonical_name" in resolution_by_tool["scrape_system_profile"]["candidate_fields"]
    assert "result.identity_map" in by_tool["scrape_system_profile"]["identity_contract"]["output_paths"]
    assert "result.locations[].evidence" in by_tool["scrape_system_profile"]["evidence_contract"]["row_evidence_paths"]
    assert "result.items[].evidence" in by_tool["scrape_system_profile"]["evidence_contract"]["row_evidence_paths"]
    source_resolution = {row["source_id"]: row for row in plan["source_resolution"]}
    assert source_resolution["public_web"]["status"] == "alias"
    assert source_resolution["public_web"]["canonical_dataset_ids"] == ["web_intelligence"]
    assert "candidate alias" in source_resolution["public_web"]["caveat"]
    assert any(row["label"] == "Public web alias context" for row in plan["report_fact_rows"])
    web_fact = {
        row["label"]: row
        for row in plan["report_ingest_contract"]["fact_rows"]
    }["Public web alias context"]
    assert web_fact["value_path"] == "web_intelligence.scrape_system_profile.locations"
    assert web_fact["evidence_path"] == "web_intelligence.scrape_system_profile.locations[].evidence"
    assert web_fact["identity_map_path"] == "web_intelligence.scrape_system_profile.identity_map"
    pecos_fact = {
        row["label"]: row
        for row in plan["report_ingest_contract"]["fact_rows"]
    }["PECOS enrollment cross-check"]
    assert pecos_fact["value_path"] == "provider_enrollment.search_provider_enrollment.enrollments"
    assert pecos_fact["evidence_path"] == "provider_enrollment.search_provider_enrollment.enrollments[].evidence"
    validate_report_ingest_payload(plan["report_ingest_contract"])


def test_ownership_chow_workflow_uses_cross_server_identity_resolution(monkeypatch) -> None:
    monkeypatch.delenv("SAM_GOV_API_KEY", raising=False)

    plan = build_workflow_plan(
        "ownership_chow_trace",
        inputs={
            "ccn": "390223",
            "facility_name": "Thomas Jefferson University Hospital",
            "system_id": "SYS_JEFFERSON",
            "owner_name": "Example Owner LLC",
            "entity_name": "Example Owner LLC",
            "state": "PA",
        },
    )
    by_field = {row["field"]: row for row in plan["identity_map"]["join_keys"]}
    by_tool = {step["tool"]: step for step in plan["steps"]}
    facts = {row["label"]: row for row in plan["report_ingest_contract"]["fact_rows"]}
    resolution_by_tool = {
        resolution["tool"]: resolution
        for resolution in plan["identity_map"]["resolution_plan"]
    }

    assert plan["workflow_id"] == "ownership_chow_trace"
    assert plan["readiness"]["status"] == "ready"
    assert plan["readiness"]["optional_unavailable"][0]["tool"] == "search_sam_exclusions"
    assert by_field["ccn"]["value"] == "390223"
    assert by_field["owner_name"]["value"] == "Example Owner LLC"
    assert by_field["entity_name"]["value"] == "Example Owner LLC"
    assert "cms-facility.get_facility" in by_field["ccn"]["used_by"]
    assert "provider-enrollment.get_facility_ownership" in by_field["ccn"]["used_by"]
    assert "provider-enrollment.profile_provider_control" in by_field["ccn"]["used_by"]
    assert "health-system-profiler.get_system_facilities" in by_field["ahrq_system_id"]["used_by"]

    assert by_tool["get_facility"]["mcp_call"]["arguments_template"]["ccn"] == "390223"
    assert by_tool["get_system_facilities"]["mcp_call"]["arguments_template"] == {
        "system_id": "SYS_JEFFERSON",
        "facility_type": "<facility_type>",
    }
    assert by_tool["get_facility_ownership"]["evidence_contract"]["row_evidence_paths"] == [
        "result.owners[].evidence"
    ]
    assert by_tool["search_change_of_ownership"]["evidence_contract"]["row_evidence_paths"] == [
        "result.events[].evidence"
    ]
    assert by_tool["profile_provider_control"]["mcp_call"]["arguments_template"] == {
        "ccn": "390223",
        "npi": "<npi>",
    }
    assert by_tool["profile_provider_control"]["execution_readiness"]["blocking"] is False
    assert by_tool["profile_provider_control"]["evidence_contract"]["row_evidence_paths"] == [
        "result.enrollment[].evidence",
        "result.ownership[].evidence",
        "result.chow_history[].evidence",
        "result.owner_network.nodes[].evidence",
        "result.owner_network.edges[].evidence",
    ]
    assert "result.ownership[].identity" in by_tool["profile_provider_control"]["identity_contract"]["output_paths"]
    assert "result.owner_network.nodes[]" in by_tool["profile_provider_control"]["identity_contract"]["output_paths"]
    assert by_tool["search_sam_exclusions"]["execution_readiness"]["status"] == "optional_unavailable"
    assert by_tool["search_sam_exclusions"]["mcp_call"]["arguments_template"]["entity_name"] == "Example Owner LLC"

    assert resolution_by_tool["get_facility"]["merge_action"] == "merge_on_exact_identifier"
    assert resolution_by_tool["get_system_facilities"]["merge_action"] == "merge_on_exact_identifier"
    assert resolution_by_tool["trace_owner_network"]["merge_action"] == "merge_on_exact_identifier"
    assert facts["Facility identity"]["identity_map_path"] == "cms_facility.get_facility.identity_map"
    assert facts["Facility identity"]["evidence_path"] == "cms_facility.get_facility.evidence"
    assert facts["System affiliation context"]["identity_map_path"] == (
        "health_system_profiler.get_system_facilities.identity_map"
    )
    assert facts["System affiliation context"]["value_path"] == (
        "health_system_profiler.get_system_facilities.inpatient_facilities"
    )
    assert facts["System affiliation context"]["evidence_path"] == (
        "health_system_profiler.get_system_facilities.inpatient_facilities[].evidence"
    )
    assert facts["Active owner/control rows"]["evidence_path"] == (
        "provider_enrollment.get_facility_ownership.owners[].evidence"
    )
    assert facts["CHOW history"]["evidence_path"] == (
        "provider_enrollment.search_change_of_ownership.events[].evidence"
    )
    assert facts["Provider-control ownership profile"]["evidence_path"] == (
        "provider_enrollment.profile_provider_control.ownership[].evidence"
    )
    assert facts["Provider-control ownership profile"]["identity_map_path"] == (
        "provider_enrollment.profile_provider_control.identity_map"
    )
    assert facts["Provider-control owner network"]["evidence_path"] == (
        "provider_enrollment.profile_provider_control.owner_network.nodes[].evidence"
    )
    assert facts["Owner/entity public exclusion context"]["evidence_path"] == (
        "public_records.search_sam_exclusions.evidence"
    )
    validate_report_ingest_payload(plan["report_ingest_contract"])


def test_research_workflow_points_to_row_level_receipts() -> None:
    plan = build_workflow_plan("research_trials_activity_profile", inputs={"organization": "Example Health"})
    by_tool = {step["tool"]: step for step in plan["steps"]}
    facts = {row["label"]: row for row in plan["report_ingest_contract"]["fact_rows"]}

    assert plan["readiness"]["status"] == "ready"
    assert by_tool["profile_research_funding"]["mcp_call"]["arguments_template"]["org_name"] == "Example Health"
    assert by_tool["inventory_clinical_trial_sponsors"]["mcp_call"]["arguments_template"]["sponsor"] == "Example Health"
    assert by_tool["inventory_clinical_trial_sites"]["mcp_call"]["arguments_template"]["location"] == "Example Health"
    assert "result.projects[].evidence" in by_tool["profile_research_funding"]["evidence_contract"]["row_evidence_paths"]
    assert "result.records[].evidence" in by_tool["inventory_clinical_trial_sponsors"]["evidence_contract"]["row_evidence_paths"]
    assert "result.records[].evidence" in by_tool["inventory_clinical_trial_sites"]["evidence_contract"]["row_evidence_paths"]
    assert facts["NIH funding profile"]["value_path"] == "research_trials.profile_research_funding.projects"
    assert facts["NIH funding profile"]["evidence_path"] == "research_trials.profile_research_funding.projects[].evidence"
    assert facts["ClinicalTrials sponsor inventory"]["evidence_path"] == (
        "research_trials.inventory_clinical_trial_sponsors.records[].evidence"
    )
    assert facts["ClinicalTrials site inventory"]["evidence_path"] == (
        "research_trials.inventory_clinical_trial_sites.records[].evidence"
    )
    validate_report_ingest_payload(plan["report_ingest_contract"])


def test_referral_readiness_workflow_includes_drive_time_competition_receipts() -> None:
    plan = build_workflow_plan(
        "referral_leakage_readiness",
        inputs={"ccn": "390223", "npi": "1234567893", "market": "Philadelphia", "lat": "39.95", "lon": "-75.16"},
    )
    by_field = {row["field"]: row for row in plan["identity_map"]["join_keys"]}
    by_tool = {step["tool"]: step for step in plan["steps"]}
    facts = {row["label"]: row for row in plan["report_ingest_contract"]["fact_rows"]}

    assert by_field["lat"]["value"] == "39.95"
    assert by_field["lon"]["value"] == "-75.16"
    assert "drive-time.find_competing_facilities" in by_field["lat"]["used_by"]
    assert by_tool["find_competing_facilities"]["mcp_call"]["arguments_template"] == {
        "lat": "39.95",
        "lon": "-75.16",
        "radius_minutes": "<radius_minutes>",
        "facility_type": "<facility_type>",
    }
    assert "result.facilities[].evidence" in by_tool["find_competing_facilities"]["evidence_contract"]["row_evidence_paths"]
    assert facts["Drive-time competition context"]["evidence_path"] == (
        "drive_time.find_competing_facilities.facilities[].evidence"
    )
    assert facts["Drive-time competition context"]["identity_map_path"] == (
        "drive_time.find_competing_facilities.identity_map"
    )
    validate_report_ingest_payload(plan["report_ingest_contract"])


def test_workflow_tool_references_match_registry_modules() -> None:
    validation = validate_workflow_tool_references()

    assert validation["status"] == "ok"
    assert validation["issue_count"] == 0
    assert validation["method"] == "registry_module_signature_ast"
    assert validation["steps"]["financial-intelligence.get_public_financial_health_profile"]["module"] == (
        "servers.financial_intelligence.server"
    )
    assert validation["steps"]["health-system-profiler.get_system_facilities"]["arguments"] == [
        "facility_type",
        "system_id",
    ]
    assert validation["steps"]["research-trials.inventory_clinical_trial_sponsors"]["status"] == "ok"


def test_workflow_tool_reference_validation_rejects_invalid_arguments(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["facility_profile"]
    bad_step = replace(base.steps[0], optional_inputs=("facility_name",))
    bad_workflow = replace(base, workflow_id="bad_arguments", steps=(bad_step, *base.steps[1:]))
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_arguments", bad_workflow)

    validation = validate_workflow_tool_references("bad_arguments")

    assert validation["status"] == "issues_found"
    assert validation["issues"][0]["status"] == "tool_argument_not_found"
    assert validation["issues"][0]["invalid_arguments"] == ["facility_name"]


def test_workflow_report_contracts_reference_declared_steps_and_identity_keys() -> None:
    validation = validate_workflow_contracts()

    assert validation["status"] == "ok"
    assert validation["issue_count"] == 0
    assert validation["method"] == "workflow_report_contract_static"
    assert validation["workflows"]["compliance_exclusion_screening"]["fact_row_count"] >= 1


def test_workflow_contracts_validate_required_sources_against_registry_or_aliases(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["quality_measure_lookup"]
    bad_workflow = replace(base, workflow_id="bad_source", required_sources=("not_a_source",))
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_source", bad_workflow)

    validation = validate_workflow_contracts("bad_source")

    assert validation["status"] == "issues_found"
    assert any(issue["status"] == "unknown_required_source" for issue in validation["issues"])


def test_workflow_source_aliases_point_to_registry_datasets() -> None:
    registry_dataset_ids = workflows._registry_dataset_ids()

    for source_id, alias in workflows.WORKFLOW_SOURCE_ALIASES.items():
        assert alias["canonical_dataset_ids"], source_id
        assert set(alias["canonical_dataset_ids"]) <= registry_dataset_ids, source_id
        assert alias["source_type"], source_id
        assert alias["caveat"], source_id


def test_workflow_contracts_detect_duplicate_literal_keys_before_python_overwrites_them() -> None:
    source = '''
WORKFLOW_DEFINITIONS = {
    "bad_workflow": WorkflowDefinition(
        workflow_id="bad_workflow",
        report_fact_rows=(
            {
                "label": "Bad row",
                "value_path": "first.path",
                "value_path": "second.path",
                "required_evidence": "example receipt",
                "identity_fields": ("ccn",),
            },
        ),
    )
}
'''

    issues = workflows._duplicate_literal_key_issues_from_source(source, workflow_ids=["bad_workflow"])

    assert issues == [
        {
            "workflow_id": "bad_workflow",
            "status": "duplicate_workflow_literal_key",
            "key": "value_path",
            "line": 9,
            "message": (
                "Workflow definition contains duplicate literal key 'value_path' near line 9; "
                "Python silently keeps the last value."
            ),
        }
    ]


def test_workflow_contracts_validate_registry_preset_membership(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["quality_measure_lookup"]
    bad_workflow = replace(
        base,
        workflow_id="bad_registry_membership",
        recommended_servers=("hospital-quality",),
    )
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_registry_membership", bad_workflow)
    monkeypatch.setitem(workflows.WORKFLOW_PRESETS, "bad_registry_membership", ("hospital-quality", "discovery"))

    validation = validate_workflow_contracts("bad_registry_membership")
    statuses = {issue["status"] for issue in validation["issues"]}

    assert validation["status"] == "issues_found"
    assert "workflow_recommended_servers_drift" in statuses
    assert "workflow_step_server_not_recommended" in statuses


def test_workflow_contracts_reject_registry_preset_without_definition(monkeypatch) -> None:
    monkeypatch.setitem(workflows.WORKFLOW_PRESETS, "orphan_workflow", ("discovery",))

    validation = validate_workflow_contracts()

    assert validation["status"] == "issues_found"
    assert any(
        issue["status"] == "registry_preset_missing_workflow_definition"
        and issue["workflow_id"] == "orphan_workflow"
        for issue in validation["issues"]
    )


def test_workflow_report_contracts_reject_drifted_fact_row_paths(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["compliance_exclusion_screening"]
    bad_row = {
        **base.report_fact_rows[0],
        "evidence_path": "provider_enrollment.search_provider_enrollment.evidence",
        "identity_path": "provider_enrollment.search_provider_enrollment.identity",
    }
    bad_workflow = replace(base, workflow_id="bad_contract", report_fact_rows=(bad_row,))
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_contract", bad_workflow)

    validation = validate_workflow_contracts("bad_contract")
    statuses = {issue["status"] for issue in validation["issues"]}

    assert validation["status"] == "issues_found"
    assert "evidence_path_step_mismatch" in statuses
    assert "identity_path_step_mismatch" in statuses
    assert "evidence_path_not_in_step_contract" in statuses


def test_workflow_report_contracts_reject_result_level_receipt_for_nested_fact(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["market_community_health_scan"]
    bad_row = {
        **base.report_fact_rows[-1],
        "evidence_path": "drive_time.compute_accessibility_score.evidence",
    }
    bad_workflow = replace(base, workflow_id="bad_nested_receipt", report_fact_rows=(bad_row,))
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_nested_receipt", bad_workflow)

    validation = validate_workflow_contracts("bad_nested_receipt")

    assert validation["status"] == "issues_found"
    assert any(issue["status"] == "result_level_evidence_for_row_fact" for issue in validation["issues"])


def test_workflow_report_contracts_require_identity_map_path(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["quality_measure_lookup"]
    bad_row = {
        "label": "Dataset source context without identity map",
        "value_path": "discovery.get_dataset_source",
        "evidence_path": "discovery.get_dataset_source.evidence",
        "identity_path": "discovery.get_dataset_source.identity",
        "source_metadata_path": "discovery.get_dataset_source.source_metadata",
        "required_evidence": "workflow registry receipt",
        "identity_fields": ("ccn",),
    }
    bad_workflow = replace(base, workflow_id="bad_missing_identity_map", report_fact_rows=(bad_row,))
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_missing_identity_map", bad_workflow)

    validation = validate_workflow_contracts("bad_missing_identity_map")

    assert validation["status"] == "issues_found"
    assert any(issue["status"] == "missing_identity_map_path" for issue in validation["issues"])


def test_workflow_report_contracts_validate_identity_paths_against_step_contract(monkeypatch) -> None:
    base = workflows.WORKFLOW_DEFINITIONS["quality_measure_lookup"]
    bad_row = {
        **base.report_fact_rows[0],
        "identity_map_path": "hospital_quality.get_quality_measure_rows.unadvertised_identity_map",
    }
    bad_workflow = replace(base, workflow_id="bad_identity_contract", report_fact_rows=(bad_row,))
    monkeypatch.setitem(workflows.WORKFLOW_DEFINITIONS, "bad_identity_contract", bad_workflow)

    validation = validate_workflow_contracts("bad_identity_contract")

    assert validation["status"] == "issues_found"
    assert any(issue["status"] == "identity_map_path_not_in_step_contract" for issue in validation["issues"])


def test_workflow_readiness_reports_blocking_registry_env(monkeypatch) -> None:
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)

    plan = build_workflow_plan("hospital_competitive_profile", inputs={"ccn": "390223"})

    assert plan["readiness"]["status"] == "needs_configuration"
    assert plan["readiness"]["missing_required_env"] == ["SEC_USER_AGENT"]
    financial_step = {step["tool"]: step for step in plan["steps"]}["get_public_financial_health_profile"]
    assert financial_step["execution_readiness"]["status"] == "missing_configuration"
    assert financial_step["execution_readiness"]["missing_env"] == ["SEC_USER_AGENT"]


def test_all_workflows_have_identity_strategy_and_report_rows() -> None:
    for workflow_id in list_workflow_plans()["workflows"]:
        plan = build_workflow_plan(workflow_id["workflow_id"])

        assert plan["identity_join_keys"], workflow_id
        assert plan["identity_map"]["identity_strategy"], workflow_id
        assert plan["identity_map"]["source_claims"], workflow_id
        assert plan["identity_map"]["resolution_plan"], workflow_id
        assert plan["source_resolution"], workflow_id
        assert plan["report_fact_rows"], workflow_id
        assert plan["report_ingest_contract"]["fact_rows"], workflow_id
        validation_modes = plan["report_ingest_contract"]["validation_modes"]
        assert validation_modes["template"]["arguments"] == {
            "require_content": False,
            "allow_placeholders": True,
            "require_identity_context": False,
        }
        assert validation_modes["final_report"]["arguments"] == {
            "require_content": True,
            "allow_placeholders": False,
            "require_identity_context": True,
        }
        assert "require_identity_context=True" in validation_modes["final_report"]["python_call"]
        validate_report_ingest_payload(plan["report_ingest_contract"])
        for step in plan["steps"]:
            assert step["stdio_command"] == f"hc-mcp {step['server']}", workflow_id
            assert step["mcp_call"]["server"] == step["server"], workflow_id
            assert step["mcp_call"]["tool"] == step["tool"], workflow_id
            assert step["mcp_call"]["qualified_tool"] == f"{step['server']}.{step['tool']}", workflow_id
            assert isinstance(step["mcp_call"]["arguments_template"], dict), workflow_id
            assert isinstance(step["mcp_call"]["resolved_arguments"], dict), workflow_id
            assert isinstance(step["input_groups"], list), workflow_id
            assert isinstance(step["identity_contract"]["consumes"], list), workflow_id
            assert isinstance(step["identity_contract"]["produces"], list), workflow_id
            assert step["identity_contract"]["output_paths"], workflow_id
            assert step["identity_contract"]["match_policy"], workflow_id
            assert step["execution_readiness"]["status"] in {
                "ready",
                "needs_inputs",
                "missing_configuration",
                "optional_unavailable",
                "review_sources",
            }, workflow_id
            assert isinstance(step["execution_readiness"]["missing_inputs"], list), workflow_id
            assert isinstance(step["execution_readiness"]["source_checks"], list), workflow_id
        for resolution in plan["identity_map"]["resolution_plan"]:
            assert resolution["merge_action"] in {
                "merge_on_exact_identifier",
                "record_candidate_alias_requires_source_review",
                "context_only_no_entity_merge",
            }, workflow_id
            assert resolution["evidence_path"] == "result.evidence", workflow_id
            assert resolution["identity_output_paths"], workflow_id
        for row in plan["report_ingest_contract"]["fact_rows"]:
            assert set(REPORT_SOURCE_METADATA_FIELDS) <= set(row), workflow_id
            assert row["contract_status"] == "template_requires_tool_execution"
            assert row["identity_path"], workflow_id
            assert row["identity_map_path"], workflow_id
            assert row["identity_fields"], workflow_id


def test_workflow_examples_satisfy_required_planner_inputs(monkeypatch) -> None:
    monkeypatch.setenv("SEC_USER_AGENT", "tests@example.com")

    for workflow_id, workflow in workflows.WORKFLOW_DEFINITIONS.items():
        example_inputs = workflows.WORKFLOW_EXAMPLE_INPUTS.get(workflow_id)
        assert example_inputs, workflow_id

        plan = build_workflow_plan(workflow_id, inputs=example_inputs)

        assert plan["examples"]["inputs"] == example_inputs
        assert plan["readiness"]["missing_inputs"] == [], workflow_id
        assert plan["readiness"]["status"] != "needs_inputs", workflow_id
        for step in plan["steps"]:
            if step["blocking"]:
                assert step["execution_readiness"]["missing_inputs"] == [], (
                    workflow_id,
                    step["tool"],
                    step["execution_readiness"]["missing_inputs"],
                )
            for value in step["mcp_call"]["resolved_arguments"].values():
                assert not (isinstance(value, str) and value.startswith("<") and value.endswith(">"))
        for required_identifier in workflow.required_identifiers:
            assert required_identifier not in plan["readiness"]["missing_inputs"]


def test_build_workflow_plan_reports_missing_inputs_and_unknown_workflow() -> None:
    plan = build_workflow_plan("quality_measure_lookup")

    assert plan["readiness"]["status"] == "needs_inputs"
    assert "ccn" in plan["readiness"]["missing_inputs"]

    missing = build_workflow_plan("does-not-exist")
    assert missing["error"] == "workflow_not_found"
    assert "quality_measure_lookup" in missing["available_workflows"]


def test_format_workflow_plan_is_operator_readable() -> None:
    plan = build_workflow_plan("quality_measure_lookup", inputs={"ccn": "390223", "measure": "clabsi_sir"})
    text = format_workflow_plan(plan)

    assert "Quality Measure Lookup" in text
    assert "hospital-quality.get_quality_measure_rows" in text
    assert "MCP call: get_quality_measure_rows" in text
    assert '"ccn": "390223"' in text
    assert '"measure": "clabsi_sir"' in text
    assert "Workflow scope:" in text
    assert "required identifiers: ccn, measure" in text
    assert "required sources: cms_hospital_quality" in text
    assert "source resolution:" in text
    assert "cms_hospital_quality: registry_dataset -> cms_hospital_quality" in text
    assert "source caveat: Canonical registry dataset ID." in text
    assert "recommended servers: hospital-quality, discovery" in text
    assert "Planner validation:" in text
    assert "tool references: ok (0 issues; registry_module_signature_ast)" in text
    assert "report contracts: ok (0 issues; workflow_report_contract_static)" in text
    assert "identity: consumes ccn, measure_id; preserves ccn, measure_id" in text
    assert "execution readiness:" in text
    assert "tool reference: ok (servers.hospital_quality.server)" in text
    assert "source: cms_hospital_quality" in text
    assert "Identity map:" in text
    assert "Identity resolution:" in text
    assert "Report fact rows:" in text
    assert "Exact CMS quality measure row" in text
    assert "evidence path: hospital_quality.get_quality_measure_rows.rows[].evidence" in text
    assert "source metadata path: hospital_quality.get_quality_measure_rows.source_metadata" in text
    assert "identity path: hospital_quality.get_quality_measure_rows.identity" in text
    assert "identity map path: hospital_quality.get_quality_measure_rows.identity_map" in text
    assert "Report validation:" in text
    assert "template: validate_report_ingest_payload(payload)" in text
    assert (
        "final_report: validate_report_ingest_payload(payload, require_content=True, "
        "allow_placeholders=False, require_identity_context=True)"
    ) in text
    assert "Adjacent HRRP/HAC/PHC4" in text


def test_format_workflow_plan_exposes_source_caveats_for_aliases() -> None:
    plan = build_workflow_plan(
        "system_reconciliation",
        inputs={"query": "Jefferson Health", "system_slug": "jefferson-health"},
    )
    text = format_workflow_plan(plan)

    assert "public_web: alias -> web_intelligence" in text
    assert "source caveat: Public web pages are candidate alias/context evidence" in text
    assert "source caveat: public_web: Public web pages are candidate alias/context evidence" in text


def test_format_workflow_plan_exposes_report_contract_paths_for_every_workflow() -> None:
    for workflow in list_workflow_plans()["workflows"]:
        plan = build_workflow_plan(workflow["workflow_id"])
        text = format_workflow_plan(plan)
        fact_count = len(plan["report_ingest_contract"]["fact_rows"])

        assert fact_count, workflow
        assert text.count("evidence path:") == fact_count, workflow
        assert text.count("source metadata path:") == fact_count, workflow
        assert text.count("identity path:") == fact_count, workflow
        assert text.count("identity map path:") == fact_count, workflow


def test_format_workflow_list_is_task_first() -> None:
    text = workflows._format_workflow_list(list_workflow_plans())

    assert "compliance_exclusion_screening" in text
    assert "identifiers: npi or entity_name" in text
    assert "identity keys: npi, ccn, entity_name, state, uei, cage_code, pecos_enrollment_id, owner_id" in text
    assert "sources: hhs_oig_leie, sam_gov_exclusions, cms_pecos_public_provider_enrollment" in text
    assert "source aliases: public_web->web_intelligence" in text
    assert "source aliases: public_financial_health->ahrq_hfmd+cms_cost_report+nj_hospital_public_data+state_health_data" in text
    assert "servers: public-records, provider-enrollment, live-gateway; steps:" in text
    assert "report rows:" in text
    assert "validation: tools ok (0 issues); reports ok (0 issues)" in text
    assert "Run: hc-mcp workflow <workflow_id>" in text
