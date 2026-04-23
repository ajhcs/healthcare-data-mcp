"""Tests for the hc-mcp launcher registry."""

from __future__ import annotations

from servers._launcher import SERVERS


def test_launcher_includes_metadata_servers() -> None:
    assert SERVERS["discovery"].module == "servers.discovery.server"
    assert SERVERS["discovery"].port == 8015
    assert SERVERS["gateway"].module == "servers.gateway.server"
    assert SERVERS["gateway"].port == 8016


def test_launcher_ports_are_unique() -> None:
    ports = [spec.port for spec in SERVERS.values()]

    assert len(ports) == len(set(ports))
