"""Core PSA/SSA computation engine."""

from collections import deque

import pandas as pd


def derive_service_area(
    discharge_data: pd.DataFrame,
    psa_threshold: float = 0.75,
    ssa_threshold: float = 0.95,
    adjacency_graph: dict[str, set[str]] | None = None,
) -> dict:
    """Derive Primary and Secondary Service Areas from discharge data.

    Args:
        discharge_data: DataFrame with columns [zip_code, discharges].
        psa_threshold: Cumulative discharge fraction for PSA (default 0.75).
        ssa_threshold: Cumulative discharge fraction for SSA boundary (default 0.95).
        adjacency_graph: Optional dict mapping each ZIP to its set of adjacent ZIPs.

    Returns:
        Dict with psa_zips, ssa_zips, and summary statistics.
    """
    df = discharge_data.copy()
    df = df.groupby("zip_code", as_index=False)["discharges"].sum()
    df = df.sort_values("discharges", ascending=False).reset_index(drop=True)

    total = int(df["discharges"].sum())
    if total == 0:
        return {
            "psa_zips": [],
            "psa_discharge_count": 0,
            "psa_pct": 0.0,
            "ssa_zips": [],
            "ssa_discharge_count": 0,
            "ssa_pct": 0.0,
            "remaining_zips_count": len(df),
            "total_discharges": 0,
        }

    df["cumulative"] = df["discharges"].cumsum()
    df["cumulative_pct"] = df["cumulative"] / total

    # Find PSA cutoff: include all ZIPs up to and including the row that crosses the threshold
    psa_mask = df["cumulative_pct"] <= psa_threshold
    if not psa_mask.all():
        first_over = psa_mask.idxmin()
        psa_mask.iloc[first_over] = True

    psa_zips = df.loc[psa_mask, "zip_code"].tolist()

    # Optional contiguity enforcement
    if adjacency_graph is not None:
        psa_zips = enforce_contiguity(psa_zips, adjacency_graph)

    psa_discharges = int(df[df["zip_code"].isin(psa_zips)]["discharges"].sum())

    # SSA: continue accumulating from remaining ZIPs
    remaining = df[~df["zip_code"].isin(psa_zips)].copy().reset_index(drop=True)
    if remaining.empty:
        return {
            "psa_zips": psa_zips,
            "psa_discharge_count": psa_discharges,
            "psa_pct": round(psa_discharges / total, 4),
            "ssa_zips": [],
            "ssa_discharge_count": 0,
            "ssa_pct": 0.0,
            "remaining_zips_count": 0,
            "total_discharges": total,
        }

    remaining["running_total"] = remaining["discharges"].cumsum() + psa_discharges
    remaining["running_pct"] = remaining["running_total"] / total

    ssa_mask = remaining["running_pct"] <= ssa_threshold
    if not ssa_mask.all():
        first_over_idx = ssa_mask.idxmin()
        ssa_mask.iloc[first_over_idx] = True

    ssa_zips = remaining.loc[ssa_mask, "zip_code"].tolist()
    ssa_discharges = int(remaining.loc[ssa_mask, "discharges"].sum())

    remaining_count = len(remaining) - len(ssa_zips)

    return {
        "psa_zips": psa_zips,
        "psa_discharge_count": psa_discharges,
        "psa_pct": round(psa_discharges / total, 4),
        "ssa_zips": ssa_zips,
        "ssa_discharge_count": ssa_discharges,
        "ssa_pct": round(ssa_discharges / total, 4),
        "remaining_zips_count": remaining_count,
        "total_discharges": total,
    }


def enforce_contiguity(zips: list[str], adjacency: dict[str, set[str]]) -> list[str]:
    """Keep only ZIPs that form a contiguous cluster with the highest-volume ZIP.

    Uses BFS from the first ZIP (highest discharges, since list is pre-sorted) to find
    the largest connected component within the candidate set.

    Args:
        zips: Candidate ZIP codes (sorted by discharges descending).
        adjacency: Mapping from ZIP to set of adjacent ZIPs.

    Returns:
        Filtered list of ZIPs forming a contiguous cluster.
    """
    if not zips:
        return []

    zip_set = set(zips)
    seed = zips[0]  # Highest-volume ZIP

    visited: set[str] = set()
    queue: deque[str] = deque([seed])
    visited.add(seed)

    while queue:
        current = queue.popleft()
        neighbors = adjacency.get(current, set())
        for nbr in neighbors:
            if nbr in zip_set and nbr not in visited:
                visited.add(nbr)
                queue.append(nbr)

    # Preserve original ordering (by discharge volume)
    return [z for z in zips if z in visited]


def compute_market_share(hsaf_df: pd.DataFrame, zip_code: str, limit: int = 20,
                         name_lookup: dict[str, str] | None = None) -> dict:
    """Compute hospital market share for a given ZIP code.

    Args:
        hsaf_df: Full HSAF DataFrame with columns [ccn, facility_name, zip_code, discharges].
        zip_code: The beneficiary ZIP code to analyze.
        limit: Max hospitals to return.
        name_lookup: Optional CCN → facility name mapping for enrichment.

    Returns:
        Dict with zip_code, total_discharges, and list of hospital share entries.
    """
    zip_code = str(zip_code).strip().zfill(5)
    zip_df = hsaf_df[hsaf_df["zip_code"] == zip_code].copy()

    if zip_df.empty:
        return {"zip_code": zip_code, "total_discharges": 0, "hospitals": []}

    # Aggregate by hospital (in case of duplicate rows)
    agg = zip_df.groupby(["ccn", "facility_name"], as_index=False)["discharges"].sum()
    agg = agg.sort_values("discharges", ascending=False).reset_index(drop=True)

    # Enrich with facility names from external lookup if available
    if name_lookup:
        agg["facility_name"] = agg["ccn"].map(
            lambda c: name_lookup.get(c, "")
        ).fillna("")

    total = int(agg["discharges"].sum())
    hospitals = []
    for _, row in agg.head(limit).iterrows():
        hospitals.append({
            "ccn": row["ccn"],
            "facility_name": row["facility_name"],
            "discharges": int(row["discharges"]),
            "market_share_pct": round(row["discharges"] / total * 100, 2) if total else 0.0,
        })

    return {"zip_code": zip_code, "total_discharges": total, "hospitals": hospitals}
