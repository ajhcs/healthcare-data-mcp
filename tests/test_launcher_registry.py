"""Tests for the hc-mcp launcher registry."""

from __future__ import annotations

import os

from shared.utils.env_file import load_env_file
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


def test_env_loader_supports_launcher_env_file(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("SAM_GOV_API_KEY=from_file\n", encoding="utf-8")
    monkeypatch.delenv("SAM_GOV_API_KEY", raising=False)

    load_env_file(env_file)

    assert os.environ["SAM_GOV_API_KEY"] == "from_file"
