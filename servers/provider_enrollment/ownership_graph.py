"""Bounded NetworkX ownership graph helpers for provider enrollment data."""

from __future__ import annotations

from collections import Counter
from typing import Any

import networkx as nx

from shared.utils.identity import normalize_enrollment_id, normalize_name


def build_ownership_graph(owner_rows: list[dict[str, Any]]) -> nx.DiGraph:
    """Build an owner-to-facility directed graph from active ownership rows."""

    graph = nx.DiGraph()
    for row in owner_rows:
        if not row.get("is_active", True):
            continue

        owner_id, facility_id = source_graph_ids(row)
        if not owner_id or not facility_id:
            continue

        graph.add_node(
            owner_id,
            kind="owner",
            label=str(row.get("owner_name") or row.get("owner_associate_id") or ""),
            owner_associate_id=str(row.get("owner_associate_id") or ""),
            owner_pac_id=str(row.get("owner_pac_id") or ""),
            provider_category=str(row.get("provider_category") or ""),
            state=str(row.get("state") or ""),
        )
        graph.add_node(
            facility_id,
            kind="facility",
            label=str(row.get("facility_name") or row.get("ccn") or row.get("enrollment_id") or ""),
            ccn=str(row.get("ccn") or ""),
            enrollment_id=str(row.get("enrollment_id") or ""),
            provider_category=str(row.get("provider_category") or ""),
            state=str(row.get("state") or ""),
        )
        graph.add_edge(
            owner_id,
            facility_id,
            relationship="owns_or_controls",
            active=True,
            role_code=str(row.get("role_code") or ""),
            role_text=str(row.get("role_text") or ""),
            percentage_ownership=str(row.get("percentage_ownership") or ""),
            association_date=str(row.get("association_date") or ""),
        )

    return graph


def source_graph_ids(row: dict[str, Any]) -> tuple[str, str]:
    """Return the graph owner/facility ids generated from a source ownership row."""

    return _owner_node_id(row), _facility_node_id(row)


def trace_owner_network(
    owner_rows: list[dict[str, Any]],
    *,
    owner_name: str = "",
    owner_associate_id: str = "",
    state: str = "",
    provider_category: str = "",
    depth: int = 1,
    limit: int = 100,
) -> dict[str, Any]:
    """Return a bounded graph around matching owners.

    Depth is capped at 3. The traversal treats owner/facility links as
    undirected for discovery while preserving directed ownership edges in the
    returned payload.
    """

    graph = build_ownership_graph(owner_rows)
    depth = _bounded_int(depth, default=1, minimum=1, maximum=3)
    limit = _bounded_int(limit, default=100, minimum=1, maximum=250)
    seeds = _matching_owner_nodes(
        graph,
        owner_name=owner_name,
        owner_associate_id=owner_associate_id,
        state=state,
        provider_category=provider_category,
    )
    if not seeds:
        return {"nodes": [], "edges": [], "shared_owners": []}

    undirected = graph.to_undirected()
    selected: set[str] = set()
    node_depths: dict[str, int] = {}
    for seed in seeds:
        lengths = nx.single_source_shortest_path_length(undirected, seed, cutoff=depth)
        for node, node_depth in lengths.items():
            selected.add(node)
            node_depths[node] = min(node_depths.get(node, node_depth), node_depth)
            if len(selected) >= limit:
                break
        if len(selected) >= limit:
            break

    subgraph = graph.subgraph(selected)
    nodes = [
        {
            "id": node_id,
            "kind": attrs.get("kind", ""),
            "label": attrs.get("label", ""),
            "depth": node_depths.get(node_id, 0),
            "attributes": {key: value for key, value in attrs.items() if key not in {"kind", "label"}},
        }
        for node_id, attrs in sorted(subgraph.nodes(data=True), key=lambda item: (node_depths.get(item[0], 0), item[0]))
    ]
    edges = [
        {
            "source": source,
            "target": target,
            "relationship": attrs.get("relationship", "owns_or_controls"),
            "active": bool(attrs.get("active", True)),
            "attributes": {
                key: value for key, value in attrs.items() if key not in {"relationship", "active"}
            },
        }
        for source, target, attrs in subgraph.edges(data=True)
    ][:limit]

    return {
        "nodes": nodes[:limit],
        "edges": edges,
        "shared_owners": shared_owner_summary(owner_rows, limit=10),
    }


def shared_owner_summary(owner_rows: list[dict[str, Any]], *, limit: int = 10) -> list[dict[str, Any]]:
    """Summarize owners connected to more than one facility."""

    facilities_by_owner: dict[str, set[str]] = {}
    labels: dict[str, str] = {}
    for row in owner_rows:
        if not row.get("is_active", True):
            continue
        owner_id = _owner_node_id(row)
        facility_id = _facility_node_id(row)
        if not owner_id or not facility_id:
            continue
        facilities_by_owner.setdefault(owner_id, set()).add(facility_id)
        labels.setdefault(owner_id, str(row.get("owner_name") or owner_id))

    counts = Counter({owner_id: len(facilities) for owner_id, facilities in facilities_by_owner.items()})
    return [
        {"owner_id": owner_id, "owner_name": labels.get(owner_id, ""), "facility_count": count}
        for owner_id, count in counts.most_common(limit)
        if count > 1
    ]


def _matching_owner_nodes(
    graph: nx.DiGraph,
    *,
    owner_name: str,
    owner_associate_id: str,
    state: str,
    provider_category: str,
) -> list[str]:
    owner_query = normalize_name(owner_name)
    associate_query = normalize_enrollment_id(owner_associate_id) if owner_associate_id else ""
    seeds: list[str] = []
    for node_id, attrs in graph.nodes(data=True):
        if attrs.get("kind") != "owner":
            continue
        if state and str(attrs.get("state") or "").upper() != state.upper():
            continue
        if provider_category and str(attrs.get("provider_category") or "") != provider_category:
            continue
        if associate_query and str(attrs.get("owner_associate_id") or "") == associate_query:
            seeds.append(node_id)
            continue
        if owner_query and owner_query in normalize_name(attrs.get("label", "")):
            seeds.append(node_id)
    return seeds


def _owner_node_id(row: dict[str, Any]) -> str:
    associate_id = normalize_enrollment_id(row.get("owner_associate_id") or "")
    if associate_id:
        return f"owner:{associate_id}"
    owner_name = normalize_name(row.get("owner_name") or "", remove_legal_suffixes=True)
    state = str(row.get("state") or "").upper()
    return f"owner:name:{owner_name}:{state}" if owner_name else ""


def _facility_node_id(row: dict[str, Any]) -> str:
    ccn = str(row.get("ccn") or "")
    if ccn:
        return f"facility:{ccn}"
    enrollment_id = normalize_enrollment_id(row.get("enrollment_id") or "")
    if enrollment_id:
        return f"facility:enrollment:{enrollment_id}"
    facility_name = normalize_name(row.get("facility_name") or "", remove_legal_suffixes=True)
    state = str(row.get("state") or "").upper()
    return f"facility:name:{facility_name}:{state}" if facility_name else ""


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))
