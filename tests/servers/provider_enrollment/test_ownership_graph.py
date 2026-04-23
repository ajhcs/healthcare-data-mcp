from __future__ import annotations

from servers.provider_enrollment.ownership_graph import build_ownership_graph, shared_owner_summary, trace_owner_network


def sample_owner_rows() -> list[dict]:
    return [
        {
            "owner_name": "Jefferson Parent LLC",
            "owner_associate_id": "OWN1",
            "owner_pac_id": "PAC1",
            "ccn": "390001",
            "facility_name": "Jefferson Hospital",
            "state": "PA",
            "provider_category": "hospital",
            "role_text": "Direct owner",
            "percentage_ownership": "100",
            "is_active": True,
        },
        {
            "owner_name": "Jefferson Parent LLC",
            "owner_associate_id": "OWN1",
            "owner_pac_id": "PAC1",
            "ccn": "390002",
            "facility_name": "Jefferson Suburban",
            "state": "PA",
            "provider_category": "hospital",
            "role_text": "Direct owner",
            "percentage_ownership": "80",
            "is_active": True,
        },
        {
            "owner_name": "Old Owner LLC",
            "owner_associate_id": "OLD1",
            "ccn": "390001",
            "facility_name": "Jefferson Hospital",
            "state": "PA",
            "provider_category": "hospital",
            "is_active": False,
        },
    ]


def test_build_ownership_graph_includes_only_active_edges() -> None:
    graph = build_ownership_graph(sample_owner_rows())

    assert "owner:OWN1" in graph.nodes
    assert "facility:390001" in graph.nodes
    assert graph.has_edge("owner:OWN1", "facility:390001")
    assert "owner:OLD1" not in graph.nodes


def test_trace_owner_network_caps_depth_and_returns_shared_owner_summary() -> None:
    result = trace_owner_network(sample_owner_rows(), owner_name="Jefferson Parent", depth=99, limit=10)

    node_ids = {node["id"] for node in result["nodes"]}
    assert "owner:OWN1" in node_ids
    assert "facility:390001" in node_ids
    assert len(result["edges"]) == 2
    assert result["shared_owners"][0]["facility_count"] == 2


def test_trace_owner_network_defaults_invalid_bounds() -> None:
    result = trace_owner_network(sample_owner_rows(), owner_name="Jefferson Parent", depth="bad", limit="bad")  # type: ignore[arg-type]

    assert result["nodes"]
    assert all(node["depth"] <= 1 for node in result["nodes"])


def test_shared_owner_summary_ignores_single_facility_owners() -> None:
    summary = shared_owner_summary(sample_owner_rows())

    assert summary == [{"owner_id": "owner:OWN1", "owner_name": "Jefferson Parent LLC", "facility_count": 2}]
