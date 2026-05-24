"""Shared cache TTL and metadata utilities.

Provides a single ``is_cache_valid`` function used across all servers
to decide whether a locally-cached file is still fresh.

Recommended TTL defaults:
- 90 days  -- bulk CMS CSV / Parquet datasets (quarterly refresh cadence)
-  30 days -- web-scraped content, crosswalk files
-   7 days -- live API responses, nightly-updated sources
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
from typing import Any

__all__ = [
    "CacheMetadata",
    "cache_age_days",
    "cache_metadata_path",
    "cache_status",
    "is_cache_valid",
    "read_cache_metadata",
    "write_atomic_bytes",
    "write_cache_metadata",
]


@dataclass(slots=True)
class CacheMetadata:
    """Small sidecar record for cache provenance and freshness audits."""

    source_url: str = ""
    fetched_at: str = ""
    content_length: int | None = None
    cache_key: str = ""
    ttl_days: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CacheMetadata":
        known = {field_name: data[field_name] for field_name in cls.__dataclass_fields__ if field_name in data}
        known["extra"] = dict(known.get("extra") or {})
        return cls(**known)


def cache_metadata_path(path: Path) -> Path:
    """Return the JSON sidecar path for a cache artifact."""

    return path.with_name(f"{path.name}.meta.json")


def cache_age_days(path: Path) -> float | None:
    """Return cache file age in days, or ``None`` when the file is missing."""

    if not path.exists():
        return None
    return (datetime.now(timezone.utc).timestamp() - path.stat().st_mtime) / 86_400


def is_cache_valid(path: Path, max_age_days: float = 90, *, min_size_bytes: int = 1) -> bool:
    """Return *True* if *path* exists and its mtime is within *max_age_days*.

    Uses UTC timestamps to avoid timezone drift issues.

    Parameters
    ----------
    path:
        File to check.
    max_age_days:
        Maximum acceptable age in fractional days.  Pass ``0`` to force
        a refresh on every call.
    min_size_bytes:
        Minimum acceptable file size. Empty/truncated cache files are treated
        as invalid by default.
    """
    if not path.exists():
        return False
    if path.is_file() and path.stat().st_size < min_size_bytes:
        return False
    age_days = cache_age_days(path) or 0
    return age_days < max_age_days


def read_cache_metadata(path: Path) -> CacheMetadata | None:
    """Read a cache sidecar if present and well-formed."""

    metadata_path = cache_metadata_path(path)
    if not metadata_path.exists():
        return None
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return CacheMetadata.from_dict(payload)
    except TypeError:
        return None


def write_cache_metadata(path: Path, metadata: CacheMetadata) -> Path:
    """Write a stable JSON sidecar next to a cache artifact."""

    metadata_path = cache_metadata_path(path)
    metadata_path.write_text(
        json.dumps(metadata.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metadata_path


def write_atomic_bytes(path: Path, content: bytes) -> None:
    """Atomically replace a cache file with downloaded bytes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        tmp_path = Path(handle.name)
        handle.write(content)
    tmp_path.replace(path)


def cache_status(path: Path, *, ttl_days: float | None = 90) -> dict[str, Any]:
    """Return filesystem and sidecar status for a cache artifact."""

    payload: dict[str, Any] = {
        "path": str(path),
        "ttl_days": ttl_days,
        "status": "missing",
    }
    if not path.exists():
        return payload

    age = cache_age_days(path) or 0
    stat = path.stat()
    metadata = read_cache_metadata(path)
    payload.update(
        {
            "status": "stale" if ttl_days is not None and age > ttl_days else "ready",
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "age_days": round(age, 2),
            "metadata_path": str(cache_metadata_path(path)),
            "metadata_status": "ready" if metadata else "missing",
        }
    )
    if metadata:
        payload["metadata"] = metadata.to_dict()
    return payload
