"""Shared CMS download and cache helpers."""

from __future__ import annotations

import re
from pathlib import Path

from shared.utils.http_client import resilient_request

CMS_API_BASE = "https://data.cms.gov"
DATA_DIR = Path.home() / ".healthcare-data-mcp" / "cache"
DATA_DIR.mkdir(parents=True, exist_ok=True)

_SAFE_CACHE_KEY = re.compile(r"[^A-Za-z0-9_.-]+")


def get_cache_path(cache_key: str, *, suffix: str = ".csv") -> Path:
    """Return a normalized path in the shared healthcare-data-mcp cache."""
    normalized = _SAFE_CACHE_KEY.sub("_", cache_key).strip("._")
    if not normalized:
        raise ValueError("cache_key must contain at least one safe character")
    if suffix and not suffix.startswith("."):
        suffix = f".{suffix}"
    return DATA_DIR / f"{normalized}{suffix}"


async def cms_download_csv(url: str, *, cache_key: str, force: bool = False, timeout: float = 300.0) -> Path:
    """Download a CSV-like CMS artifact to the shared cache and return its path."""
    cache_path = get_cache_path(cache_key, suffix=".csv")
    if cache_path.exists() and not force:
        return cache_path

    response = await resilient_request("GET", url, timeout=timeout)
    content_type = response.headers.get("content-type", "").lower()
    if response.status_code == 202 or (
        "html" in content_type or response.content[:200].lstrip().lower().startswith(b"<!doctype")
    ):
        raise RuntimeError(f"CMS download did not return CSV data for {url}")

    cache_path.write_bytes(response.content)
    return cache_path
