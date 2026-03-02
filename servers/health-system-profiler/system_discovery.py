"""AHRQ Compendium-based health system discovery.

Fuzzy search against system names, resolve system_id -> CCN list.
"""

import logging

import pandas as pd
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)


def fuzzy_search_systems(
    query: str,
    systems_df: pd.DataFrame,
    limit: int = 10,
    score_cutoff: float = 60.0,
) -> list[dict]:
    """Fuzzy search health system names from AHRQ Compendium.

    Uses rapidfuzz token_set_ratio for robust matching against abbreviations,
    partial names, and reordered tokens.

    Args:
        query: User's search string (e.g. "Jefferson Health", "LVHN").
        systems_df: AHRQ system file DataFrame.
        limit: Maximum results to return.
        score_cutoff: Minimum fuzzy match score (0-100).

    Returns:
        List of dicts with system_id, name, hq_city, hq_state, hospital_count.
    """
    if systems_df.empty or "health_sys_name" not in systems_df.columns:
        return []

    names = systems_df["health_sys_name"].tolist()
    matches = process.extract(
        query,
        names,
        scorer=fuzz.token_set_ratio,
        limit=limit,
        score_cutoff=score_cutoff,
        processor=lambda s: s.lower() if isinstance(s, str) else s,
    )

    results = []
    for name, score, idx in matches:
        row = systems_df.iloc[idx]
        results.append({
            "system_id": str(row.get("health_sys_id", "")),
            "name": str(row.get("health_sys_name", "")),
            "hq_city": str(row.get("health_sys_city", "")),
            "hq_state": str(row.get("health_sys_state", "")),
            "hospital_count": int(row.get("hosp_count", 0)),
            "match_score": round(score, 1),
        })

    return results


def resolve_system_ccns(system_id: str, hospitals_df: pd.DataFrame) -> list[str]:
    """Get all CCNs for a given AHRQ system_id.

    Args:
        system_id: AHRQ health_sys_id.
        hospitals_df: AHRQ hospital linkage DataFrame.

    Returns:
        List of 6-char CCN strings.
    """
    if hospitals_df.empty or "health_sys_id" not in hospitals_df.columns:
        return []

    matches = hospitals_df[hospitals_df["health_sys_id"] == system_id]
    if "ccn" not in matches.columns:
        return []

    return matches["ccn"].tolist()
