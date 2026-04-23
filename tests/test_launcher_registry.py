"""Tests for the hc-mcp launcher registry."""

from __future__ import annotations

from servers._launcher import SERVERS


def test_launcher_includes_metadata_servers() -> None:
    assert SERVERS["discovery"].module == "servers.discovery.server"
    assert SERVERS["discovery"].port == 8015
    assert SERVERS["gateway"].module == "servers.gateway.server"
    assert SERVERS["gateway"].port == 8016
    assert SERVERS["provider-enrollment"].module == "servers.provider_enrollment.server"
    assert SERVERS["provider-enrollment"].port == 8017
    assert SERVERS["community-health"].module == "servers.community_health.server"
    assert SERVERS["community-health"].port == 8018
    assert SERVERS["research-trials"].module == "servers.research_trials.server"
    assert SERVERS["research-trials"].port == 8019


def test_launcher_ports_are_unique() -> None:
    ports = [spec.port for spec in SERVERS.values()]

    assert len(ports) == len(set(ports))
