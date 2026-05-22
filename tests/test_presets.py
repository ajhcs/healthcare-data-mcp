"""Tests for curated task-first presets."""

from __future__ import annotations

import json

from shared.utils.presets import build_preset_plan, format_preset_plan, list_presets
from shared.utils.server_registry import CURATED_PRESETS, SERVER_BY_ID


def test_list_presets_includes_distribution_profiles() -> None:
    payload = list_presets()
    preset_ids = {preset["preset_id"] for preset in payload["presets"]}

    assert {"compliance", "market-strategy", "research", "metadata-only"} <= preset_ids


def test_build_preset_plan_returns_registry_backed_commands_and_env() -> None:
    plan = build_preset_plan("market-strategy")

    assert plan["preset_id"] == "market-strategy"
    assert plan["servers"]
    assert {server["server_id"] for server in plan["servers"]} == set(CURATED_PRESETS["market-strategy"].server_ids)
    assert {workflow["workflow_id"] for workflow in plan["workflow_summaries"]} == set(
        CURATED_PRESETS["market-strategy"].workflow_ids
    )
    for server in plan["servers"]:
        spec = SERVER_BY_ID[server["server_id"]]
        assert server["stdio_command"] == f"hc-mcp {spec.server_id}"
        assert server["http_url"] == f"http://127.0.0.1:{spec.port}/mcp"
        assert server["dataset_ids"] == list(spec.dataset_ids)
    system_reconciliation = next(
        workflow for workflow in plan["workflow_summaries"] if workflow["workflow_id"] == "system_reconciliation"
    )
    assert system_reconciliation["plan_command"] == "hc-mcp workflow system_reconciliation --json"
    assert "ahrq_system_id" in system_reconciliation["identity_join_keys"]
    assert any("CCN/NPI/PECOS" in item for item in system_reconciliation["identity_strategy"])
    assert system_reconciliation["validation"]["tool_references"]["status"] == "ok"
    assert system_reconciliation["validation"]["report_contracts"]["status"] == "ok"
    public_web = {
        source["source_id"]: source
        for source in system_reconciliation["source_resolution"]
    }["public_web"]
    assert public_web["status"] == "alias"
    assert public_web["canonical_dataset_ids"] == ["web_intelligence"]
    assert "SEC_USER_AGENT" in plan["required_env"]
    assert "BLS_API_KEY" in plan["optional_env"]
    json.dumps(plan)


def test_build_preset_plan_reports_unknown_preset() -> None:
    plan = build_preset_plan("missing")

    assert plan["error"] == "preset_not_found"
    assert "compliance" in plan["available_presets"]


def test_format_preset_plan_is_operator_readable() -> None:
    plan = build_preset_plan("compliance")
    text = format_preset_plan(plan)

    assert "Compliance Screening" in text
    assert "hc-mcp public-records" in text
    assert "hc-mcp workflow compliance_exclusion_screening" in text
    assert "identity keys: npi, ccn, entity_name, state, uei, cage_code, pecos_enrollment_id, owner_id" in text
    assert "sources" in text
    assert "report rows" in text
