"""Loader and normalization helpers for CDC PLACES community-health data."""

from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from shared.utils.source_catalog import SourceManifest, read_source_manifest, write_source_manifest

from .socrata_client import (
    PLACES_RELEASE,
    build_places_query,
    fetch_places_rows,
    normalize_geography_type,
    resolve_places_dataset,
    resolve_places_dataset_live,
    source_metadata,
)

COMMUNITY_ESTIMATE_NOTE = (
    "CDC PLACES values are model-based community estimates for geographic areas; "
    "they are not patient-level facts."
)

DEFAULT_CACHE_DIR = Path(os.environ.get("PLACES_CACHE_DIR", Path.home() / ".healthcare-data-mcp" / "cache" / "community-health"))


def normalize_places_record(
    row: dict[str, Any],
    *,
    geography_type: str,
    source: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize one PLACES row across county, place, tract, and ZCTA datasets."""
    geography = normalize_geography_type(geography_type)
    data_value = _to_float(row.get("data_value"))
    low = _to_float(row.get("low_confidence_limit"))
    high = _to_float(row.get("high_confidence_limit"))
    adult_pop = _to_int(row.get("totalpop18plus"))
    total_pop = _to_int(row.get("totalpopulation"))
    latitude, longitude = _parse_geolocation(row.get("geolocation"))

    missing = []
    if data_value is None:
        missing.append("data_value")
    if low is None or high is None:
        missing.append("confidence_interval")
    if adult_pop is None and total_pop is None:
        missing.append("population")

    return {
        "year": _to_int(row.get("year")) or row.get("year"),
        "geography_type": geography,
        "location_id": _text(row.get("locationid")),
        "location_name": _text(row.get("locationname")),
        "state_abbr": _text(row.get("stateabbr")).upper() or None,
        "state_name": _text(row.get("statedesc")) or None,
        "category": _text(row.get("category")),
        "category_id": _text(row.get("categoryid")),
        "measure": _text(row.get("measure")),
        "measure_id": _text(row.get("measureid")).upper(),
        "short_question_text": _text(row.get("short_question_text")),
        "data_source": _text(row.get("datasource")),
        "data_value_type": _text(row.get("data_value_type")),
        "data_value_type_id": _text(row.get("datavaluetypeid")),
        "value_unit": _text(row.get("data_value_unit")),
        "data_value": data_value,
        "confidence_interval": {"low": low, "high": high},
        "population": {"adult_18_plus": adult_pop, "total": total_pop},
        "geolocation": {"latitude": latitude, "longitude": longitude} if latitude is not None and longitude is not None else None,
        "source": source,
        "notes": _notes(missing),
    }


def normalize_measure_metadata(row: dict[str, Any]) -> dict[str, Any]:
    """Normalize a PLACES measure metadata row or infer metadata from a data row."""
    return {
        "measure_id": _first_text(row, "measureid", "measure_id", "MeasureId", "MeasureID").upper(),
        "measure": _first_text(row, "measure", "measure_name", "Measure"),
        "short_question_text": _first_text(row, "short_question_text", "Short_Question_Text"),
        "category": _first_text(row, "category", "Category"),
        "category_id": _first_text(row, "categoryid", "category_id", "CategoryID"),
        "data_value_type": _first_text(row, "data_value_type", "Data_Value_Type"),
        "data_value_type_id": _first_text(row, "datavaluetypeid", "data_value_type_id", "DataValueTypeID"),
        "value_unit": _first_text(row, "data_value_unit", "Data_Value_Unit"),
        "source_note": COMMUNITY_ESTIMATE_NOTE,
    }


def build_measure_metadata(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return distinct normalized measure metadata records."""
    measures: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        metadata = normalize_measure_metadata(row)
        measure_id = metadata["measure_id"]
        if not measure_id:
            continue
        key = (measure_id, metadata["data_value_type_id"] or metadata["data_value_type"])
        measures.setdefault(key, metadata)
    return sorted(measures.values(), key=lambda item: (item["category"], item["measure_id"], item["data_value_type"]))


def load_rows(path: str | Path) -> list[dict[str, Any]]:
    """Load PLACES fixture/cache rows from JSON, JSONL, CSV, or Parquet."""
    data_path = Path(path)
    suffix = data_path.suffix.lower()
    if suffix == ".json":
        data = json.loads(data_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data = data.get("rows") or data.get("results") or []
        return [row for row in data if isinstance(row, dict)]
    if suffix == ".jsonl":
        return [json.loads(line) for line in data_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if suffix in {".parquet", ".pq"}:
        try:
            return pd.read_parquet(data_path).to_dict(orient="records")
        except ImportError:
            import duckdb

            with duckdb.connect(":memory:") as con:
                return con.execute("SELECT * FROM read_parquet(?)", [str(data_path)]).fetchdf().to_dict(orient="records")
    return pd.read_csv(data_path, dtype=str, keep_default_na=False).to_dict(orient="records")


def write_parquet_cache(rows: Iterable[dict[str, Any]], path: str | Path) -> Path:
    """Write PLACES rows to Parquet for fixture/bulk workflows."""
    cache_path = Path(path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(list(rows))
    try:
        df.to_parquet(cache_path, compression="zstd", index=False)
    except ImportError:
        import duckdb

        with duckdb.connect(":memory:") as con:
            con.register("places_rows", df)
            con.execute("COPY places_rows TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(cache_path)])
    return cache_path


def filter_rows(
    rows: Iterable[dict[str, Any]],
    *,
    location_ids: Iterable[str] | None = None,
    state: str | None = None,
    measure_ids: Iterable[str] | None = None,
    data_value_types: Iterable[str] | None = None,
    search: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """Apply the same basic filters used by the Socrata loader to local rows."""
    id_set = {_norm(value) for value in location_ids or [] if _norm(value)}
    state_norm = _norm(state)
    measure_set = {_norm(value) for value in measure_ids or [] if _norm(value)}
    value_type_set = {_norm(value) for value in data_value_types or [] if _norm(value)}
    search_norm = _norm(search)

    bounded_limit = _bounded_int(limit, default=5000, minimum=1, maximum=50000)
    matched: list[dict[str, Any]] = []
    for row in rows:
        if id_set and _norm(row.get("locationid")) not in id_set:
            continue
        if state_norm and _norm(row.get("stateabbr")) != state_norm:
            continue
        if measure_set and _norm(row.get("measureid")) not in measure_set:
            continue
        if value_type_set and _norm(row.get("data_value_type")) not in value_type_set:
            continue
        if search_norm:
            haystack = f"{row.get('locationname', '')} {row.get('locationid', '')}".upper()
            if search_norm not in haystack:
                continue
        matched.append(row)
        if len(matched) >= bounded_limit:
            break
    return matched


async def get_places_rows(
    geography_type: str,
    *,
    location_ids: Iterable[str] | None = None,
    state: str | None = None,
    measure_ids: Iterable[str] | None = None,
    data_value_types: Iterable[str] | None = None,
    search: str | None = None,
    limit: int = 5000,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | None = None,
) -> tuple[list[dict[str, Any]], SourceManifest]:
    """Load PLACES rows from configured fixture/cache or Socrata."""
    geography = normalize_geography_type(geography_type)
    manifest = _manifest_from_env(geography, catalog_data=catalog_data, catalog_path=catalog_path)
    fixture_path = _env_path(f"PLACES_{geography.upper()}_ROWS_PATH") or _env_path("PLACES_ROWS_PATH")
    if fixture_path:
        rows = filter_rows(
            load_rows(fixture_path),
            location_ids=location_ids,
            state=state,
            measure_ids=measure_ids,
            data_value_types=data_value_types,
            search=search,
            limit=limit,
        )
        return rows, manifest

    if manifest.dataset_id:
        rows = await fetch_places_rows(
            manifest,
            location_ids=location_ids,
            state=state,
            measure_ids=measure_ids,
            data_value_types=data_value_types,
            search=search,
            limit=limit,
        )
        return rows, manifest

    live_manifest = await resolve_places_dataset_live(geography)
    rows = await fetch_places_rows(
        live_manifest,
        location_ids=location_ids,
        state=state,
        measure_ids=measure_ids,
        data_value_types=data_value_types,
        search=search,
        limit=limit,
    )
    return rows, live_manifest


async def normalized_places_rows(geography_type: str, **filters: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return normalized PLACES rows and compact source metadata."""
    rows, manifest = await get_places_rows(geography_type, **filters)
    source = source_metadata(manifest, geography_type=normalize_geography_type(geography_type))
    return [normalize_places_record(row, geography_type=geography_type, source=source) for row in rows], source


def summarize_locations(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return unique locations present in normalized records."""
    locations: dict[str, dict[str, Any]] = {}
    for record in records:
        location_id = record.get("location_id")
        if not location_id:
            continue
        locations.setdefault(
            location_id,
            {
                "location_id": location_id,
                "location_name": record.get("location_name"),
                "geography_type": record.get("geography_type"),
                "state_abbr": record.get("state_abbr"),
                "state_name": record.get("state_name"),
                "population": record.get("population"),
                "geolocation": record.get("geolocation"),
            },
        )
    return sorted(locations.values(), key=lambda item: (item.get("state_abbr") or "", item.get("location_name") or ""))


def group_profile(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Build a single-location profile from normalized PLACES records."""
    record_list = list(records)
    if not record_list:
        return {
            "measures": [],
            "missing_data_notes": ["No PLACES rows matched the requested filters."],
            "interpretation": COMMUNITY_ESTIMATE_NOTE,
        }
    location = summarize_locations(record_list)[0]
    missing_notes = sorted({note for record in record_list for note in record.get("notes", []) if note})
    return {
        "location": location,
        "measures": sorted(record_list, key=lambda item: (item.get("category") or "", item.get("measure_id") or "")),
        "missing_data_notes": missing_notes,
        "interpretation": COMMUNITY_ESTIMATE_NOTE,
    }


def aggregate_market_profile(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate county/ZCTA PLACES rows for a first-release service-area profile."""
    by_measure: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    locations = summarize_locations(records)
    for record in records:
        if record.get("data_value") is None:
            continue
        key = (record.get("measure_id") or "", record.get("data_value_type_id") or record.get("data_value_type") or "")
        if key[0]:
            by_measure[key].append(record)

    measures = []
    for (measure_id, value_type_key), items in sorted(by_measure.items()):
        weighted_total = 0.0
        weight_sum = 0
        values = []
        for item in items:
            value = item["data_value"]
            weight = (item.get("population") or {}).get("adult_18_plus") or (item.get("population") or {}).get("total")
            values.append(value)
            if weight:
                weighted_total += value * weight
                weight_sum += int(weight)
        first = items[0]
        measures.append(
            {
                "measure_id": measure_id,
                "measure": first.get("measure"),
                "category": first.get("category"),
                "data_value_type": first.get("data_value_type"),
                "data_value_type_id": value_type_key,
                "value_unit": first.get("value_unit"),
                "locations_reporting": len(items),
                "weighted_average": round(weighted_total / weight_sum, 3) if weight_sum else None,
                "simple_average": round(sum(values) / len(values), 3) if values else None,
                "weight_basis": "adult_18_plus population" if weight_sum else "unweighted",
            }
        )

    return {
        "geographic_basis": sorted({location["geography_type"] for location in locations}),
        "locations": locations,
        "aggregated_measures": measures,
        "missing_data_notes": [] if measures else ["No measure values were available for aggregation."],
        "interpretation": COMMUNITY_ESTIMATE_NOTE,
    }


def _manifest_from_env(
    geography_type: str,
    *,
    catalog_data: dict[str, Any] | None,
    catalog_path: str | None,
) -> SourceManifest:
    env_manifest = _env_path(f"PLACES_{geography_type.upper()}_MANIFEST_PATH")
    if env_manifest:
        return read_source_manifest(env_manifest)

    env_catalog = catalog_path or _env_path("PLACES_CATALOG_PATH")
    if catalog_data is not None or env_catalog:
        return resolve_places_dataset(geography_type, catalog_data=catalog_data, catalog_path=env_catalog)

    return SourceManifest(
        title=f"CDC PLACES {geography_type.title()} Data ({PLACES_RELEASE})",
        extra={"domain": "data.cdc.gov", "release": PLACES_RELEASE},
    )


def write_manifest(manifest: SourceManifest, path: str | Path) -> Path:
    """Write a PLACES source manifest."""
    return write_source_manifest(manifest, path)


def socrata_query_preview(**filters: Any) -> dict[str, Any]:
    """Expose query building for tests and diagnostics."""
    return build_places_query(**filters)


def _env_path(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value else None


def _first_text(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value
    return ""


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm(value: Any) -> str:
    return _text(value).upper()


def _to_float(value: Any) -> float | None:
    text = _text(value)
    if not text or text in {"*", "NA", "N/A", "null", "None"}:
        return None
    try:
        return float(text.replace(",", ""))
    except (TypeError, ValueError):
        return None


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _to_int(value: Any) -> int | None:
    number = _to_float(value)
    return int(number) if number is not None else None


def _parse_geolocation(value: Any) -> tuple[float | None, float | None]:
    if not value:
        return None, None
    if isinstance(value, dict):
        coordinates = value.get("coordinates")
        if isinstance(coordinates, list | tuple) and len(coordinates) >= 2:
            longitude = _to_float(coordinates[0])
            latitude = _to_float(coordinates[1])
            return latitude, longitude
        return _to_float(value.get("latitude")), _to_float(value.get("longitude"))
    if isinstance(value, str) and "," in value:
        left, right = value.strip("() ").split(",", 1)
        return _to_float(left), _to_float(right)
    return None, None


def _notes(missing: list[str]) -> list[str]:
    notes = [COMMUNITY_ESTIMATE_NOTE]
    notes.extend(f"Missing {field} in source row." for field in missing)
    return notes
