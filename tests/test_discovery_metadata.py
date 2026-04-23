"""Tests for the metadata-only discovery MCP server."""

from __future__ import annotations

import json

import pytest

from servers.discovery import server


def test_catalog_lists_expected_dataset_groups() -> None:
    payload = server.dataset_catalog_payload()
    dataset_ids = {dataset["dataset_id"] for dataset in payload["datasets"]}

    assert payload["dataset_count"] == len(server.DATASET_CATALOG)
    assert "cms_hospital_general_info" in dataset_ids
    assert "cms_medicare_claims_pufs" in dataset_ids
    assert "public_records" in dataset_ids
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

    json.dumps(schema)
    json.dumps(source)


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


@pytest.mark.asyncio
async def test_fastmcp_resources_are_registered() -> None:
    resources = await server.mcp.list_resources()
    templates = await server.mcp.list_resource_templates()

    resource_uris = {str(resource.uri) for resource in resources}
    template_uris = {str(template.uriTemplate) for template in templates}

    assert "healthcare-data://datasets/catalog" in resource_uris
    assert "healthcare-data://cache/status" in resource_uris
    assert "healthcare-data://datasets/{dataset_id}/schema" in template_uris
    assert "healthcare-data://runbooks/{runbook_id}" in template_uris


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

    assert catalog["dataset_count"] == len(server.DATASET_CATALOG)
    assert source["dataset_id"] == "cms_hsaf"
    assert runbooks["runbook_count"] == len(server.RUNBOOKS)
