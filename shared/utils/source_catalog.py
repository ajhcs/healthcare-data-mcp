"""Shared source catalog resolution and manifest helpers."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from shared.utils.cache import write_atomic_json
from shared.utils.http_client import resilient_request

CMS_DATA_JSON_URL = "https://data.cms.gov/data.json"
SOCRATA_CATALOG_URL = "https://api.us.socrata.com/api/catalog/v1"


@dataclass(slots=True)
class SourceManifest:
    """Small JSON-serializable record for cached public data provenance."""

    source_url: str = ""
    landing_page: str = ""
    dataset_id: str = ""
    title: str = ""
    modified: str = ""
    fetched_at: str = ""
    etag: str = ""
    last_modified: str = ""
    record_count: int | None = None
    checksum: str = ""
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceManifest":
        known = {field_name: data[field_name] for field_name in cls.__dataclass_fields__ if field_name in data}
        known["extra"] = dict(known.get("extra") or {})
        return cls(**known)


def read_source_manifest(path: str | Path) -> SourceManifest:
    """Read a source manifest from JSON."""
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Source manifest must be a JSON object: {path}")
    return SourceManifest.from_dict(data)


def write_source_manifest(manifest: SourceManifest, path: str | Path) -> Path:
    """Write a source manifest as stable, human-readable JSON."""
    manifest_path = Path(path)
    write_atomic_json(manifest_path, manifest.to_dict())
    return manifest_path


async def fetch_cms_catalog(url: str = CMS_DATA_JSON_URL) -> dict[str, Any]:
    """Fetch the CMS DCAT catalog JSON."""
    response = await resilient_request("GET", url, timeout=60.0)
    return response.json()


async def fetch_socrata_catalog(
    query: str,
    *,
    domain: str | None = None,
    release: str | None = None,
    url: str = SOCRATA_CATALOG_URL,
    limit: int = 25,
) -> dict[str, Any]:
    """Fetch Socrata catalog matches for a CDC/open-data title."""
    search = " ".join(part for part in (query, release or "") if part).strip()
    params: dict[str, Any] = {"search": search, "only": "datasets", "limit": limit}
    if domain:
        params["domains"] = domain
        params["search_context"] = domain

    response = await resilient_request("GET", url, params=params, timeout=60.0)
    return response.json()


def load_catalog(path: str | Path) -> dict[str, Any]:
    """Load a JSON catalog fixture or cached catalog file."""
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Catalog must be a JSON object: {path}")
    return data


def resolve_cms_dataset(
    title: str,
    *,
    landing_page_slug: str | None = None,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | Path | None = None,
) -> SourceManifest:
    """Resolve a CMS dataset from ``data.cms.gov/data.json`` by title.

    ``catalog_data`` and ``catalog_path`` make tests and offline loaders fully
    deterministic. When multiple datasets match, a matching landing-page slug
    wins, followed by the most recent ``modified`` value.
    """
    catalog = _catalog_from_inputs(catalog_data, catalog_path)
    datasets = _as_list(catalog.get("dataset"))
    if not datasets:
        raise LookupError("CMS catalog did not contain any datasets")

    target_title = _normalize_text(title)
    slug = _normalize_slug(landing_page_slug)
    matches: list[tuple[tuple[int, str, int], dict[str, Any]]] = []

    for index, dataset in enumerate(datasets):
        if not isinstance(dataset, dict):
            continue
        dataset_title = str(dataset.get("title") or "")
        title_score = _title_match_score(target_title, _normalize_text(dataset_title))
        if title_score <= 0:
            continue

        landing_page = _first_string(dataset.get("landingPage")) or _first_string(dataset.get("accessURL"))
        slug_score = 0
        if slug:
            page_slug = _normalize_slug(landing_page)
            slug_score = 50 if page_slug == slug or page_slug.endswith(f"-{slug}") else -10

        modified = str(dataset.get("modified") or "")
        matches.append(((title_score + slug_score, modified, -index), dataset))

    if not matches:
        raise LookupError(f"No CMS dataset matched title: {title}")

    matches.sort(key=lambda item: item[0], reverse=True)
    return _cms_manifest(matches[0][1])


def resolve_socrata_dataset(
    title: str,
    *,
    release: str | None = None,
    domain: str | None = None,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | Path | None = None,
) -> SourceManifest:
    """Resolve a Socrata dataset by title, optional release text, and domain."""
    catalog = _catalog_from_inputs(catalog_data, catalog_path)
    results = _as_list(catalog.get("results") or catalog.get("result") or catalog.get("datasets"))
    if not results:
        raise LookupError("Socrata catalog did not contain any results")

    target_title = _normalize_text(title)
    target_release = _normalize_text(release or "")
    target_domain = _normalize_domain(domain or "")
    matches: list[tuple[tuple[int, str, int], dict[str, Any]]] = []

    for index, result in enumerate(results):
        if not isinstance(result, dict):
            continue
        resource = result.get("resource") if isinstance(result.get("resource"), dict) else result
        name = str(resource.get("name") or result.get("name") or result.get("title") or "")
        title_score = _title_match_score(target_title, _normalize_text(name))
        if title_score <= 0:
            continue

        result_domain = _normalize_domain(str(resource.get("domain") or result.get("domain") or ""))
        if target_domain and result_domain and result_domain != target_domain:
            continue

        release_score = 0
        if target_release:
            searchable = _normalize_text(" ".join(_collect_strings(result)))
            if target_release not in searchable:
                continue
            release_score = 25

        updated = str(resource.get("updatedAt") or resource.get("metadata_updated_at") or "")
        matches.append(((title_score + release_score, updated, -index), result))

    if not matches:
        filters = []
        if release:
            filters.append(f"release={release}")
        if domain:
            filters.append(f"domain={domain}")
        suffix = f" ({', '.join(filters)})" if filters else ""
        raise LookupError(f"No Socrata dataset matched title: {title}{suffix}")

    matches.sort(key=lambda item: item[0], reverse=True)
    return _socrata_manifest(matches[0][1])


def _catalog_from_inputs(catalog_data: dict[str, Any] | None, catalog_path: str | Path | None) -> dict[str, Any]:
    if catalog_data is not None:
        return catalog_data
    if catalog_path is not None:
        return load_catalog(catalog_path)
    raise ValueError("Provide catalog_data or catalog_path; network fetching is explicit")


def _cms_manifest(dataset: dict[str, Any]) -> SourceManifest:
    landing_page = _first_string(dataset.get("landingPage")) or _first_string(dataset.get("accessURL"))
    distribution = _preferred_distribution(_as_list(dataset.get("distribution")))
    source_url = _distribution_url(distribution) or landing_page

    return SourceManifest(
        source_url=source_url,
        landing_page=landing_page,
        dataset_id=str(dataset.get("identifier") or dataset.get("@id") or ""),
        title=str(dataset.get("title") or ""),
        modified=str(dataset.get("modified") or ""),
        record_count=_int_or_none(dataset.get("record_count") or dataset.get("recordCount")),
        checksum=_checksum(distribution),
        extra={
            "publisher": dataset.get("publisher"),
            "distribution_title": distribution.get("title") if isinstance(distribution, dict) else "",
        },
    )


def _socrata_manifest(result: dict[str, Any]) -> SourceManifest:
    resource = result.get("resource") if isinstance(result.get("resource"), dict) else result
    domain = str(resource.get("domain") or result.get("domain") or "")
    dataset_id = str(resource.get("id") or result.get("id") or "")
    landing_page = str(
        result.get("permalink")
        or result.get("link")
        or (f"https://{domain}/d/{dataset_id}" if domain and dataset_id else "")
    )
    source_url = f"https://{domain}/resource/{dataset_id}.json" if domain and dataset_id else landing_page

    return SourceManifest(
        source_url=source_url,
        landing_page=landing_page,
        dataset_id=dataset_id,
        title=str(resource.get("name") or result.get("title") or ""),
        modified=str(resource.get("updatedAt") or resource.get("metadata_updated_at") or ""),
        record_count=_int_or_none(
            resource.get("row_count")
            or resource.get("rows")
            or resource.get("record_count")
            or result.get("row_count")
        ),
        extra={"domain": domain, "resource_type": resource.get("type") or ""},
    )


def _preferred_distribution(distributions: list[Any]) -> dict[str, Any]:
    dicts = [item for item in distributions if isinstance(item, dict)]
    if not dicts:
        return {}

    def key(distribution: dict[str, Any]) -> tuple[int, int]:
        values = " ".join(
            str(distribution.get(name) or "")
            for name in ("format", "mediaType", "title", "downloadURL", "accessURL")
        ).lower()
        has_download = bool(_first_string(distribution.get("downloadURL")))
        csv_like = any(marker in values for marker in ("csv", "comma-separated", "text/csv"))
        return (1 if csv_like else 0, 1 if has_download else 0)

    return max(dicts, key=key)


def _distribution_url(distribution: dict[str, Any]) -> str:
    return _first_string(distribution.get("downloadURL")) or _first_string(distribution.get("accessURL"))


def _checksum(distribution: dict[str, Any]) -> str:
    checksum = distribution.get("checksum") if isinstance(distribution, dict) else ""
    if isinstance(checksum, dict):
        return str(checksum.get("value") or checksum.get("@value") or "")
    return str(checksum or "")


def _title_match_score(target: str, candidate: str) -> int:
    if not target or not candidate:
        return 0
    if target == candidate:
        return 100
    if target in candidate:
        return 90
    if candidate in target:
        return 80
    target_tokens = set(target.split())
    candidate_tokens = set(candidate.split())
    if target_tokens and target_tokens.issubset(candidate_tokens):
        return 75
    return 0


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", value.casefold())).strip()


def _normalize_slug(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(str(value))
    text = parsed.path if parsed.scheme else str(value)
    return re.sub(r"[^a-z0-9-]+", "-", text.casefold()).strip("-/")


def _normalize_domain(value: str) -> str:
    value = value.casefold().strip()
    if "://" in value:
        value = urlparse(value).netloc
    return value.strip("/")


def _first_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                return item
    return ""


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _collect_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        strings: list[str] = []
        for child in value.values():
            strings.extend(_collect_strings(child))
        return strings
    if isinstance(value, list):
        strings = []
        for child in value:
            strings.extend(_collect_strings(child))
        return strings
    return []


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
