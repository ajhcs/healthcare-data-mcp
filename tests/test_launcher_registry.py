"""Tests for the hc-mcp launcher registry."""

from __future__ import annotations

import os

from shared.utils.env_file import load_env_file
from shared.utils.server_registry import CURATED_PRESETS, SERVER_BY_ID, SERVER_REGISTRY, WORKFLOW_PRESETS
from servers._launcher import SERVERS


def test_launcher_includes_metadata_servers() -> None:
    assert SERVERS["discovery"].module == "servers.discovery.server"
    assert SERVERS["discovery"].port == 8015
    assert SERVERS["gateway"].module == "servers.gateway.server"
    assert SERVERS["gateway"].port == 8016
    assert SERVERS["live-gateway"].module == "servers.live_gateway.server"
    assert SERVERS["live-gateway"].port == 8020
    assert SERVERS["provider-enrollment"].module == "servers.provider_enrollment.server"
    assert SERVERS["provider-enrollment"].port == 8017
    assert SERVERS["community-health"].module == "servers.community_health.server"
    assert SERVERS["community-health"].port == 8018
    assert SERVERS["research-trials"].module == "servers.research_trials.server"
    assert SERVERS["research-trials"].port == 8019


def test_launcher_ports_are_unique() -> None:
    ports = [spec.port for spec in SERVERS.values()]

    assert len(ports) == len(set(ports))


def test_launcher_uses_canonical_server_registry() -> None:
    assert set(SERVERS) == set(SERVER_BY_ID)
    for server_id, launcher_spec in SERVERS.items():
        registry_spec = SERVER_BY_ID[server_id]
        assert launcher_spec.module == registry_spec.module
        assert launcher_spec.port == registry_spec.port
        assert launcher_spec.description == registry_spec.description


def test_server_registry_has_product_metadata() -> None:
    assert len(SERVER_REGISTRY) == len(SERVERS)
    for spec in SERVER_REGISTRY:
        assert spec.server_id
        assert spec.module.startswith("servers.")
        assert spec.port > 0
        assert spec.description
        assert spec.profiles
        if "metadata" in spec.gateway_exposure:
            assert spec.dataset_ids

    assert "compliance_exclusion_screening" in WORKFLOW_PRESETS
    assert "public-records" in WORKFLOW_PRESETS["compliance_exclusion_screening"]

    assert {"compliance", "market-strategy", "research", "metadata-only"} <= set(CURATED_PRESETS)
    for preset in CURATED_PRESETS.values():
        assert preset.server_ids
        assert set(preset.server_ids) <= set(SERVER_BY_ID)
        assert set(preset.workflow_ids) <= set(WORKFLOW_PRESETS)


def test_server_registry_workflow_roles_match_workflow_preset_membership() -> None:
    for spec in SERVER_REGISTRY:
        expected_roles = {workflow_id for workflow_id, server_ids in WORKFLOW_PRESETS.items() if spec.server_id in server_ids}

        assert set(spec.workflow_roles) == expected_roles, spec.server_id


def test_env_loader_supports_launcher_env_file(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("SAM_GOV_API_KEY=from_file\n", encoding="utf-8")
    monkeypatch.delenv("SAM_GOV_API_KEY", raising=False)

    load_env_file(env_file)

    assert os.environ["SAM_GOV_API_KEY"] == "from_file"
