"""Shared cache TTL utilities.

Provides a single ``is_cache_valid`` function used across all servers
to decide whether a locally-cached file is still fresh.

Recommended TTL defaults:
- 90 days  -- bulk CMS CSV / Parquet datasets (quarterly refresh cadence)
-  30 days -- web-scraped content, crosswalk files
-   7 days -- live API responses, nightly-updated sources
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

__all__ = ["is_cache_valid"]


def is_cache_valid(path: Path, max_age_days: float = 90) -> bool:
    """Return *True* if *path* exists and its mtime is within *max_age_days*.

    Uses UTC timestamps to avoid timezone drift issues.

    Parameters
    ----------
    path:
        File to check.
    max_age_days:
        Maximum acceptable age in fractional days.  Pass ``0`` to force
        a refresh on every call.
    """
    if not path.exists():
        return False
    age_days = (
        datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    ) / 86_400
    return age_days < max_age_days
