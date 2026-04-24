"""CDC PLACES Socrata catalog resolution and query helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from shared.utils.http_client import resilient_request
from shared.utils.source_catalog import (
    SourceManifest,
    fetch_socrata_catalog,
    load_catalog,
    resolve_socrata_dataset,
)

PLACES_DOMAIN = "data.cdc.gov"
PLACES_RELEASE = "2025 release"


@dataclass(frozen=True, slots=True)
class PlacesDatasetSpec:
    """Catalog search inputs for a CDC PLACES dataset."""

    geography_type: str
    title: str
    release: str = PLACES_RELEASE
    domain: str = PLACES_DOMAIN


PLACES_DATASETS: dict[str, PlacesDatasetSpec] = {
    "county": PlacesDatasetSpec("county", "PLACES: Local Data for Better Health, County Data"),
    "place": PlacesDatasetSpec("place", "PLACES: Local Data for Better Health, Place Data"),
    "tract": PlacesDatasetSpec("tract", "PLACES: Local Data for Better Health, Census Tract Data"),
    "zcta": PlacesDatasetSpec("zcta", "PLACES: Local Data for Better Health, ZCTA Data"),
    "data_dictionary": PlacesDatasetSpec(
        "data_dictionary",
        "PLACES: Local Data for Better Health, Data Dictionary",
    ),
}

DEFAULT_SELECT_FIELDS = (
    "year",
    "stateabbr",
    "statedesc",
    "locationname",
    "datasource",
    "category",
    "measure",
    "data_value_unit",
    "data_value_type",
    "data_value",
    "low_confidence_limit",
    "high_confidence_limit",
    "totalpop18plus",
    "totalpopulation",
    "geolocation",
    "locationid",
    "categoryid",
    "measureid",
    "datavaluetypeid",
    "short_question_text",
)


def normalize_geography_type(geography_type: str) -> str:
    """Return the canonical PLACES geography key."""
    normalized = (geography_type or "").strip().lower().replace("-", "_")
    aliases = {
        "zip": "zcta",
        "zipcode": "zcta",
        "zip_code": "zcta",
        "census_tract": "tract",
        "county_fips": "county",
        "city": "place",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in PLACES_DATASETS or normalized == "data_dictionary":
        valid = ", ".join(k for k in PLACES_DATASETS if k != "data_dictionary")
        raise ValueError(f"Unsupported PLACES geography_type '{geography_type}'. Use one of: {valid}")
    return normalized


def resolve_places_dataset(
    geography_type: str,
    *,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | None = None,
) -> SourceManifest:
    """Resolve a PLACES dataset from a Socrata catalog fixture or cached catalog."""
    key = "data_dictionary" if geography_type == "data_dictionary" else normalize_geography_type(geography_type)
    spec = PLACES_DATASETS[key]
    catalog = catalog_data if catalog_data is not None else load_catalog(catalog_path) if catalog_path else None
    if catalog is None:
        raise ValueError("Provide catalog_data or catalog_path; live resolution is explicit")
    return resolve_socrata_dataset(
        spec.title,
        release=spec.release,
        domain=spec.domain,
        catalog_data=catalog,
    )


async def resolve_places_dataset_live(geography_type: str) -> SourceManifest:
    """Resolve a PLACES dataset by querying the live Socrata catalog."""
    key = "data_dictionary" if geography_type == "data_dictionary" else normalize_geography_type(geography_type)
    spec = PLACES_DATASETS[key]
    catalog = await fetch_socrata_catalog(spec.title, domain=spec.domain, release=spec.release, limit=25)
    return resolve_places_dataset(key, catalog_data=catalog)


def resource_url(manifest: SourceManifest) -> str:
    """Return a Socrata API resource URL for a manifest."""
    if manifest.source_url:
        return manifest.source_url
    domain = str(manifest.extra.get("domain") or PLACES_DOMAIN)
    if not manifest.dataset_id:
        raise ValueError("Manifest is missing a Socrata dataset_id")
    return f"https://{domain}/resource/{manifest.dataset_id}.json"


def build_places_query(
    *,
    location_ids: Iterable[str] | None = None,
    state: str | None = None,
    measure_ids: Iterable[str] | None = None,
    data_value_types: Iterable[str] | None = None,
    search: str | None = None,
    select: Iterable[str] | str | None = None,
    order: str | None = "locationname, measureid, data_value_type",
    limit: int = 5000,
    offset: int = 0,
) -> dict[str, Any]:
    """Build Socrata API query parameters for CDC PLACES rows.

    The returned params are safe to pass to Socrata as query string values.
    """
    params: dict[str, Any] = {
        "$limit": _bounded_int(limit, default=5000, minimum=1, maximum=50000),
        "$offset": _bounded_int(offset, default=0, minimum=0, maximum=10_000_000),
    }

    if select:
        params["$select"] = ", ".join(select) if not isinstance(select, str) else select
    else:
        params["$select"] = ", ".join(DEFAULT_SELECT_FIELDS)

    clauses: list[str] = []
    ids = [_quote(value) for value in _clean_tokens(location_ids)]
    if ids:
        clauses.append(f"locationid in({', '.join(ids)})")

    measures = [_quote(value.upper()) for value in _clean_tokens(measure_ids)]
    if measures:
        clauses.append(f"upper(measureid) in({', '.join(measures)})")

    value_types = [_quote(value) for value in _clean_tokens(data_value_types)]
    if value_types:
        clauses.append(f"data_value_type in({', '.join(value_types)})")

    state_code = (state or "").strip().upper()
    if state_code:
        clauses.append(f"upper(stateabbr) = {_quote(state_code)}")

    search_text = (search or "").strip()
    if search_text:
        escaped = _escape_like(search_text.upper())
        clauses.append(f"(upper(locationname) like '%{escaped}%' or locationid like '%{escaped}%')")

    if clauses:
        params["$where"] = " and ".join(clauses)
    if order:
        params["$order"] = order

    return params


async def fetch_places_rows(
    manifest: SourceManifest,
    *,
    location_ids: Iterable[str] | None = None,
    state: str | None = None,
    measure_ids: Iterable[str] | None = None,
    data_value_types: Iterable[str] | None = None,
    search: str | None = None,
    select: Iterable[str] | str | None = None,
    order: str | None = "locationname, measureid, data_value_type",
    limit: int = 5000,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """Fetch PLACES rows from Socrata using a resolved manifest."""
    params = build_places_query(
        location_ids=location_ids,
        state=state,
        measure_ids=measure_ids,
        data_value_types=data_value_types,
        search=search,
        select=select,
        order=order,
        limit=limit,
        offset=offset,
    )
    response = await resilient_request("GET", resource_url(manifest), params=params, timeout=60.0)
    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Socrata PLACES response was not a JSON array")
    return [row for row in data if isinstance(row, dict)]


def source_metadata(manifest: SourceManifest, *, geography_type: str | None = None) -> dict[str, Any]:
    """Return compact source metadata for tool responses."""
    return {
        "name": "CDC PLACES: Local Data for Better Health",
        "dataset_title": manifest.title,
        "dataset_id": manifest.dataset_id,
        "geography_type": geography_type,
        "release": PLACES_RELEASE,
        "source_url": manifest.source_url,
        "landing_page": manifest.landing_page,
        "modified": manifest.modified,
        "record_count": manifest.record_count,
        "domain": manifest.extra.get("domain", PLACES_DOMAIN),
        "interpretation": (
            "PLACES values are model-based community estimates for geographic areas, "
            "not patient-level facts or clinical quality measures."
        ),
    }


def _clean_tokens(values: Iterable[str] | None) -> list[str]:
    if values is None:
        return []
    seen: set[str] = set()
    clean: list[str] = []
    for value in values:
        token = str(value or "").strip()
        if token and token not in seen:
            seen.add(token)
            clean.append(token)
    return clean


def _quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _escape_like(value: str) -> str:
    return str(value).replace("'", "''").replace("%", "\\%").replace("_", "\\_")


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))
