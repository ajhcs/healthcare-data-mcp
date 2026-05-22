"""Tests for the metadata-only discovery MCP server."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from servers.discovery import server
from shared.utils.server_registry import CURATED_PRESETS, SERVER_BY_ID, SERVER_REGISTRY, WORKFLOW_PRESETS


def test_catalog_lists_expected_dataset_groups() -> None:
    payload = server.dataset_catalog_payload()
    dataset_ids = {dataset["dataset_id"] for dataset in payload["datasets"]}

    assert payload["dataset_count"] == len(server.DATASET_CATALOG)
    assert "cms_hospital_general_info" in dataset_ids
    assert "cms_medicare_claims_pufs" in dataset_ids
    assert "public_records" in dataset_ids
    assert "cms_pecos_public_provider_enrollment" in dataset_ids
    assert "cms_pecos_hospital_enrollments" in dataset_ids
    assert "cms_pecos_hospital_owners" in dataset_ids
    assert "cms_pecos_hospital_chow" in dataset_ids
    assert "cms_pecos_snf_enrollments" in dataset_ids
    assert "cms_pecos_snf_owners" in dataset_ids
    assert "cms_pecos_snf_chow" in dataset_ids
    assert "cdc_places" in dataset_ids
    assert "nih_reporter_projects" in dataset_ids
    assert "clinicaltrials_gov" in dataset_ids
    assert "hhs_oig_leie" in dataset_ids
    assert "sam_gov_exclusions" in dataset_ids
    assert "state_health_data" in dataset_ids
    assert "phc4_public_reports" in dataset_ids
    assert "ahrq_hfmd" in dataset_ids
    assert "pa_hospital_reports" in dataset_ids
    assert "nj_hospital_public_data" in dataset_ids
    assert "de_hospital_discharge" in dataset_ids
    assert "mcp_metadata_surfaces" in dataset_ids
    assert "healthcare-data://datasets/{dataset_id}/schema" in payload["resource_templates"]


def test_dataset_schema_and_source_are_json_serializable() -> None:
    schema = server.dataset_schema_payload("cms_provider_of_services")
    source = server.dataset_source_payload("cms_provider_of_services")

    assert schema["grain"] == "one row per certified provider location"
    assert "PRVDR_NUM" in schema["schema"]["identity_fields"]
    assert "ccn" in schema["schema"]["join_keys"]
    assert source["source_system"] == "CMS quarterly POS public use file"
    assert any(url.startswith("https://data.cms.gov/") for url in source["source_urls"])
    assert "pos_q4_2025.csv" in source["cache_files"]
    assert source["server_capabilities"][0]["server_id"] == "health-system-profiler"
    assert source["server_capabilities"][0]["module"] == SERVER_BY_ID["health-system-profiler"].module

    json.dumps(schema)
    json.dumps(source)


def test_dataset_catalog_server_capabilities_are_registry_backed() -> None:
    payload = server.dataset_catalog_payload()
    registry_ids = {spec.server_id for spec in SERVER_REGISTRY}

    for dataset in payload["datasets"]:
        assert set(dataset["server"]) <= registry_ids
        assert dataset["server_capabilities"]
        assert {capability["server_id"] for capability in dataset["server_capabilities"]} == set(dataset["server"])
        for capability in dataset["server_capabilities"]:
            spec = SERVER_BY_ID[capability["server_id"]]
            assert capability["module"] == spec.module
            assert capability["port"] == spec.port
            assert capability["gateway_exposure"] == list(spec.gateway_exposure)
            assert capability["profiles"] == list(spec.profiles)
            assert capability["workflow_roles"] == list(spec.workflow_roles)
            assert capability["dataset_ids"] == list(spec.dataset_ids)
            assert capability["safety_notes"] == list(spec.safety_notes)


def test_discovery_dataset_catalog_contracts_cover_metadata_exposed_registry_servers() -> None:
    validation = server.validate_dataset_catalog_contracts()
    expected_metadata_servers = {spec.server_id for spec in SERVER_REGISTRY if "metadata" in spec.gateway_exposure}

    assert validation["status"] == "ok"
    assert validation["issue_count"] == 0
    assert validation["method"] == "registry_discovery_dataset_catalog"
    assert set(validation["covered_servers"]) >= expected_metadata_servers
    assert validation["covered_servers"]["discovery"] == ["mcp_metadata_surfaces"]
    assert validation["covered_servers"]["gateway"] == ["mcp_metadata_surfaces"]
    for spec in SERVER_REGISTRY:
        if "metadata" in spec.gateway_exposure:
            assert spec.dataset_ids
            for dataset_id in spec.dataset_ids:
                assert dataset_id in server.DATASET_CATALOG
                assert spec.server_id in server.DATASET_CATALOG[dataset_id]["server"]


def test_discovery_dataset_catalog_contracts_reject_registry_dataset_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    drifted = replace(SERVER_BY_ID["hospital-quality"], dataset_ids=("does_not_exist",))
    monkeypatch.setattr(server, "SERVER_REGISTRY", (drifted,))
    monkeypatch.setattr(server, "SERVER_BY_ID", {"hospital-quality": drifted})

    validation = server.validate_dataset_catalog_contracts()

    assert validation["status"] == "issues_found"
    statuses = {issue["status"] for issue in validation["issues"]}
    assert "declared_dataset_missing_from_catalog" in statuses
    assert "dataset_not_declared_for_server" in statuses


def test_dataset_metadata_exposes_full_registry_capability_contract() -> None:
    metadata = server.dataset_metadata_payload("cms_hospital_quality")
    capability = metadata["server_capabilities"][0]
    spec = SERVER_BY_ID["hospital-quality"]

    assert capability["server_id"] == "hospital-quality"
    assert capability["cache_needs"] == list(spec.cache_needs)
    assert capability["dataset_ids"] == list(spec.dataset_ids)
    assert capability["zero_config"] is spec.zero_config
    assert capability["required_env"] == [
        {"name": key.name, "required": key.required, "description": key.description}
        for key in spec.required_env
    ]
    assert capability["optional_env"] == [
        {"name": key.name, "required": key.required, "description": key.description}
        for key in spec.optional_env
    ]


def test_april_2026_expansion_dataset_metadata() -> None:
    leie_schema = server.dataset_schema_payload("hhs_oig_leie")
    leie_source = server.dataset_source_payload("hhs_oig_leie")
    sam_source = server.dataset_source_payload("sam_gov_exclusions")
    places_schema = server.dataset_schema_payload("cdc_places")
    pecos_source = server.dataset_source_payload("cms_pecos_hospital_owners")
    snf_chow_source = server.dataset_source_payload("cms_pecos_snf_chow")

    assert "NPI" in leie_schema["schema"]["identity_fields"]
    assert "public-records/leie_current.parquet" in leie_source["cache_files"]
    assert any("UPDATED.csv" in url for url in leie_source["source_urls"])
    assert any("open.gsa.gov/api/exclusions-api" in url for url in sam_source["source_urls"])
    assert places_schema["schema"]["join_keys"] == ["location_id", "state", "county_fips", "zcta"]
    assert "provider-enrollment/hospital_all_owners.parquet" in pecos_source["cache_files"]
    assert "provider-enrollment/snf_chow_owner_information.parquet" in snf_chow_source["cache_files"]


def test_unknown_dataset_returns_available_ids() -> None:
    payload = server.dataset_metadata_payload("does_not_exist")

    assert payload["error"] == "Unknown dataset_id: does_not_exist"
    assert "cms_hsaf" in payload["available_dataset_ids"]


def test_cache_status_uses_supplied_cache_root(tmp_path) -> None:
    cache_file = tmp_path / "hospital_general_info.csv"
    cache_file.write_text("facility_id,facility_name\n390001,Example Hospital\n", encoding="utf-8")

    payload = server.cache_status_payload(cache_root=tmp_path)
    matching = [
        entry
        for entry in payload["entries"]
        if entry["relative_path"] == "hospital_general_info.csv"
    ]

    assert payload["cache_root"] == str(tmp_path)
    assert matching
    assert matching[0]["status"] == "ready"
    assert matching[0]["size_bytes"] > 0
    assert payload["summary"]["missing"] > 0


def test_cache_status_includes_leie_cache_ttl(tmp_path) -> None:
    payload = server.cache_status_payload(cache_root=tmp_path)
    entries = {
        entry["relative_path"]: entry
        for entry in payload["entries"]
        if entry["dataset_id"] == "hhs_oig_leie"
    }

    assert entries["public-records/leie_current.csv"]["ttl_days"] == 31
    assert entries["public-records/leie_current.parquet"]["status"] == "missing"
    assert entries["public-records/leie_current.meta.json"]["ttl_days"] == 31


def test_cache_status_includes_public_state_health_caches(tmp_path) -> None:
    payload = server.cache_status_payload(cache_root=tmp_path)
    paths = {entry["relative_path"]: entry for entry in payload["entries"]}

    assert paths["state-health-data/phc4/report_index.json"]["dataset_id"] == "phc4_public_reports"
    assert paths["state-health-data/pa-hospital-reports/artifact_index.json"]["status"] == "missing"
    assert paths["state-health-data/pa-hospital-reports/artifact_metadata.csv"]["dataset_id"] == "pa_hospital_reports"
    assert paths["state-health-data/pa-doh-hospital-extract/normalized.parquet"]["dataset_id"] == "pa_hospital_reports"
    assert paths["state-health-data/pa-doh-hospital-extract/normalized.meta.json"]["status"] == "missing"
    assert paths["state-health-data/nj-hospital-public-data/artifact_index.json"]["dataset_id"] == "nj_hospital_public_data"
    assert paths["state-health-data/de-hospital-discharge/artifact_index.json"]["dataset_id"] == "de_hospital_discharge"


@pytest.mark.asyncio
async def test_fastmcp_resources_are_registered() -> None:
    resources = await server.mcp.list_resources()
    templates = await server.mcp.list_resource_templates()

    resource_uris = {str(resource.uri) for resource in resources}
    template_uris = {str(template.uriTemplate) for template in templates}

    assert "healthcare-data://datasets/catalog" in resource_uris
    assert "healthcare-data://cache/status" in resource_uris
    assert "healthcare-data://workflows/catalog" in resource_uris
    assert "healthcare-data://presets/catalog" in resource_uris
    assert "healthcare-data://datasets/{dataset_id}/schema" in template_uris
    assert "healthcare-data://runbooks/{runbook_id}" in template_uris
    assert "healthcare-data://workflows/{workflow_id}" in template_uris
    assert "healthcare-data://presets/{preset_id}" in template_uris


@pytest.mark.asyncio
async def test_fastmcp_tools_expose_discovery_payloads() -> None:
    tools = await server.mcp.list_tools()
    tool_names = {tool.name for tool in tools}

    assert {
        "list_datasets",
        "get_dataset",
        "get_dataset_schema",
        "get_dataset_source",
        "get_cache_status",
        "validate_dataset_catalog",
        "list_runbooks",
        "get_runbook",
        "list_workflows",
        "get_workflow_plan",
        "list_presets",
        "get_preset_plan",
    } <= tool_names

    catalog = await server.list_datasets(query="PLACES", limit=5)
    dataset = await server.get_dataset("cdc_places")
    schema = await server.get_dataset_schema("cdc_places")
    source = await server.get_dataset_source("cdc_places")
    validation = await server.validate_dataset_catalog()
    runbooks = await server.list_runbooks()
    workflows = await server.list_workflows()
    workflow = await server.get_workflow_plan("quality_measure_lookup", {"ccn": "390223", "measure": "clabsi_sir"})
    presets = await server.list_presets()
    preset = await server.get_preset_plan("market-strategy")

    assert catalog["matched_count"] >= 1
    assert dataset["dataset_id"] == "cdc_places"
    assert schema["dataset_id"] == "cdc_places"
    assert source["dataset_id"] == "cdc_places"
    assert validation["status"] == "ok"
    assert runbooks["runbook_count"] == len(server.RUNBOOKS)
    assert workflows["workflow_count"] >= 7
    quality_workflow = next(item for item in workflows["workflows"] if item["workflow_id"] == "quality_measure_lookup")
    assert quality_workflow["required_identifiers"] == ["ccn", "measure or measure_id"]
    assert quality_workflow["identity_join_keys"] == ["ccn", "measure_id"]
    assert quality_workflow["required_sources"] == ["cms_hospital_quality"]
    assert quality_workflow["recommended_servers"] == ["hospital-quality", "discovery"]
    assert quality_workflow["step_count"] == 2
    assert quality_workflow["report_fact_row_count"] == 1
    assert quality_workflow["validation"]["tool_references"]["status"] == "ok"
    assert quality_workflow["validation"]["report_contracts"]["status"] == "ok"
    assert workflow["workflow_id"] == "quality_measure_lookup"
    assert workflow["examples"]["inputs"] == {
        "dataset_id": "cms_hospital_quality",
        "ccn": "390223",
        "measure": "clabsi_sir",
    }
    assert workflow["examples"]["mcp_tool_call"]["arguments"]["workflow_id"] == "quality_measure_lookup"
    assert workflow["steps"][1]["tool"] == "get_quality_measure_rows"
    assert presets["preset_count"] == len(CURATED_PRESETS)
    assert {row["preset_id"] for row in presets["presets"]} == set(CURATED_PRESETS)
    assert preset["preset_id"] == "market-strategy"
    assert preset["servers"]
    assert "hospital_competitive_profile" in preset["workflow_ids"]
    assert {workflow["workflow_id"] for workflow in preset["workflow_summaries"]} == set(preset["workflow_ids"])
    system_reconciliation = next(
        workflow for workflow in preset["workflow_summaries"] if workflow["workflow_id"] == "system_reconciliation"
    )
    assert "ahrq_system_id" in system_reconciliation["identity_join_keys"]
    assert any(
        source["source_id"] == "public_web" and source["canonical_dataset_ids"] == ["web_intelligence"]
        for source in system_reconciliation["source_resolution"]
    )


def test_discovery_preset_catalog_is_registry_backed() -> None:
    payload = server.preset_catalog_payload()
    preset_ids = {preset["preset_id"] for preset in payload["presets"]}

    assert payload["preset_count"] == len(CURATED_PRESETS)
    assert preset_ids == set(CURATED_PRESETS)
    for preset in payload["presets"]:
        registry_preset = CURATED_PRESETS[preset["preset_id"]]
        assert preset["server_count"] == len(registry_preset.server_ids)
        assert preset["workflow_ids"] == list(registry_preset.workflow_ids)

    compliance = server.preset_plan_payload("compliance")
    compliance_server_ids = {row["server_id"] for row in compliance["servers"]}
    assert compliance_server_ids == set(CURATED_PRESETS["compliance"].server_ids)
    assert "compliance_exclusion_screening" in compliance["workflow_ids"]
    assert all(row["stdio_command"] == f"hc-mcp {row['server_id']}" for row in compliance["servers"])
    assert all(row["http_url"] == f"http://127.0.0.1:{SERVER_BY_ID[row['server_id']].port}/mcp" for row in compliance["servers"])


def test_discovery_workflow_catalog_covers_registry_workflows() -> None:
    payload = server.workflow_catalog_payload()
    workflow_ids = {workflow["workflow_id"] for workflow in payload["workflows"]}
    workflows = {workflow["workflow_id"]: workflow for workflow in payload["workflows"]}

    assert set(WORKFLOW_PRESETS) <= workflow_ids
    for workflow_id in WORKFLOW_PRESETS:
        summary = workflows[workflow_id]
        plan = server.workflow_plan_payload(workflow_id)
        assert plan["workflow_id"] == workflow_id
        assert plan["recommended_servers"] == list(WORKFLOW_PRESETS[workflow_id])
        assert summary["identity_join_keys"] == plan["identity_join_keys"]
        assert summary["identity_strategy"] == plan["identity_strategy"]
        assert summary["source_resolution"] == plan["source_resolution"]
        assert plan["evidence"]["dataset_id"] == f"workflow:{workflow_id}"
        assert plan["examples"]["inputs"]
        assert plan["examples"]["mcp_tool_call"]["tool"] == "get_workflow_plan"
    system_summary = workflows["system_reconciliation"]
    public_web = {
        row["source_id"]: row
        for row in system_summary["source_resolution"]
    }["public_web"]
    assert public_web["status"] == "alias"
    assert public_web["canonical_dataset_ids"] == ["web_intelligence"]


@pytest.mark.asyncio
async def test_fastmcp_prompts_render_common_workflows() -> None:
    prompts = await server.mcp.list_prompts()
    prompt_names = {prompt.name for prompt in prompts}

    assert "healthcare_market_scan" in prompt_names
    assert "referral_leakage_review" in prompt_names

    result = await server.mcp.get_prompt(
        "service_line_opportunity",
        {"service_line": "orthopedics", "market": "Philadelphia", "anchor_ccn": "390001"},
    )
    text = result.messages[0].content.text

    assert "orthopedics" in text
    assert "Philadelphia" in text
    assert "CCN 390001" in text
    assert "claims PUF" in text


def test_resource_functions_return_json_strings() -> None:
    catalog = json.loads(server.dataset_catalog())
    source = json.loads(server.dataset_source("cms_hsaf"))
    runbooks = json.loads(server.cache_runbooks())
    workflows = json.loads(server.workflow_catalog())

    assert catalog["dataset_count"] == len(server.DATASET_CATALOG)
    assert source["dataset_id"] == "cms_hsaf"
    assert runbooks["runbook_count"] == len(server.RUNBOOKS)
    assert workflows["workflow_count"] >= 7
