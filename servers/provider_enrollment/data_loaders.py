"""CMS provider enrollment loaders, normalization, and Parquet cache queries."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pandas as pd

from shared.utils.cache import write_atomic_parquet
from shared.utils.http_client import resilient_request
from shared.utils.identity import (
    normalize_ccn,
    normalize_enrollment_id,
    normalize_name,
    normalize_npi,
    normalize_pac_id,
    normalize_state,
)
from shared.utils.source_catalog import (
    SourceManifest,
    fetch_cms_catalog,
    read_source_manifest,
    resolve_cms_dataset,
    write_source_manifest,
)

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".healthcare-data-mcp" / "cache" / "provider-enrollment"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True, slots=True)
class ProviderEnrollmentDataset:
    """CMS dataset definition used for source resolution and query routing."""

    key: str
    title: str
    landing_page_slug: str
    provider_category: str
    record_type: str


DATASETS: dict[str, ProviderEnrollmentDataset] = {
    "medicare_ffs_public_provider_enrollment": ProviderEnrollmentDataset(
        key="medicare_ffs_public_provider_enrollment",
        title="Medicare Fee-For-Service Public Provider Enrollment",
        landing_page_slug="medicare-fee-for-service-public-provider-enrollment",
        provider_category="medicare_ffs",
        record_type="enrollment",
    ),
    "hospital_enrollments": ProviderEnrollmentDataset(
        key="hospital_enrollments",
        title="Hospital Enrollments",
        landing_page_slug="hospital-enrollments",
        provider_category="hospital",
        record_type="facility_enrollment",
    ),
    "hospital_all_owners": ProviderEnrollmentDataset(
        key="hospital_all_owners",
        title="Hospital All Owners",
        landing_page_slug="hospital-all-owners",
        provider_category="hospital",
        record_type="owner",
    ),
    "hospital_chow": ProviderEnrollmentDataset(
        key="hospital_chow",
        title="Hospital Change of Ownership",
        landing_page_slug="hospital-change-of-ownership",
        provider_category="hospital",
        record_type="chow",
    ),
    "hospital_chow_owner_information": ProviderEnrollmentDataset(
        key="hospital_chow_owner_information",
        title="Hospital Change of Ownership - Owner Information",
        landing_page_slug="hospital-change-of-ownership-owner-information",
        provider_category="hospital",
        record_type="chow_owner",
    ),
    "snf_enrollments": ProviderEnrollmentDataset(
        key="snf_enrollments",
        title="Skilled Nursing Facility Enrollments",
        landing_page_slug="skilled-nursing-facility-enrollments",
        provider_category="snf",
        record_type="facility_enrollment",
    ),
    "snf_all_owners": ProviderEnrollmentDataset(
        key="snf_all_owners",
        title="Skilled Nursing Facility All Owners",
        landing_page_slug="skilled-nursing-facility-all-owners",
        provider_category="snf",
        record_type="owner",
    ),
    "snf_chow": ProviderEnrollmentDataset(
        key="snf_chow",
        title="Skilled Nursing Facility Change of Ownership",
        landing_page_slug="skilled-nursing-facility-change-of-ownership",
        provider_category="snf",
        record_type="chow",
    ),
    "snf_chow_owner_information": ProviderEnrollmentDataset(
        key="snf_chow_owner_information",
        title="Skilled Nursing Facility Change of Ownership - Owner Information",
        landing_page_slug="skilled-nursing-facility-change-of-ownership-owner-information",
        provider_category="snf",
        record_type="chow_owner",
    ),
}

ENROLLMENT_DATASET_KEYS = tuple(key for key, dataset in DATASETS.items() if "enrollment" in dataset.record_type)
OWNER_DATASET_KEYS = tuple(key for key, dataset in DATASETS.items() if dataset.record_type == "owner")
CHOW_DATASET_KEYS = tuple(key for key, dataset in DATASETS.items() if dataset.record_type.startswith("chow"))

_NPI_COLUMNS = ("npi", "national_provider_identifier", "provider_npi")
_PAC_COLUMNS = ("pac_id", "pecos_associate_control_id", "associate_control_id", "provider_pac_id")
_ENROLLMENT_COLUMNS = ("enrollment_id", "medicare_enrollment_id", "pecos_enrollment_id", "enrlmt_id")
_ASSOCIATE_COLUMNS = ("associate_id", "association_id", "owner_associate_id", "org_associate_id")
_CCN_COLUMNS = (
    "ccn",
    "cms_certification_number",
    "certification_number",
    "provider_number",
    "provider_ccn",
    "facility_ccn",
    "medicare_id",
)
_STATE_COLUMNS = (
    "state",
    "state_code",
    "provider_state",
    "practice_location_state",
    "facility_state",
    "organization_state",
    "owner_state",
)
_PROVIDER_NAME_COLUMNS = (
    "provider_name",
    "organization_name",
    "legal_business_name",
    "individual_provider_name",
    "provider_organization_name",
    "doing_business_as_name",
)
_FACILITY_NAME_COLUMNS = (
    "facility_name",
    "hospital_name",
    "skilled_nursing_facility_name",
    "provider_name",
    "organization_name",
    "doing_business_as_name",
)
_OWNER_NAME_COLUMNS = (
    "owner_name",
    "owner_organization_name",
    "organization_name",
    "individual_owner_name",
    "owner_full_name",
    "managing_employee_name",
)


def dataset_cache_path(dataset_key: str, *, cache_dir: str | Path | None = None) -> Path:
    """Return the Parquet cache path for a dataset key."""

    return Path(cache_dir or _CACHE_DIR) / f"{dataset_key}.parquet"


def dataset_manifest_path(dataset_key: str, *, cache_dir: str | Path | None = None) -> Path:
    """Return the source manifest path for a dataset key."""

    return Path(cache_dir or _CACHE_DIR) / f"{dataset_key}.meta.json"


def resolve_dataset_manifest(
    dataset_key: str,
    *,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | Path | None = None,
) -> SourceManifest:
    """Resolve one provider-enrollment CMS dataset from a CMS data.json catalog."""

    dataset = _dataset(dataset_key)
    manifest = resolve_cms_dataset(
        dataset.title,
        landing_page_slug=dataset.landing_page_slug,
        catalog_data=catalog_data,
        catalog_path=catalog_path,
    )
    manifest.extra.update({"dataset_key": dataset.key, "provider_category": dataset.provider_category})
    return manifest


async def ensure_dataset_cached(
    dataset_key: str,
    *,
    cache_dir: str | Path | None = None,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | Path | None = None,
    force_refresh: bool = False,
) -> SourceManifest:
    """Resolve, download, normalize, and cache one CMS dataset.

    Tests can pass ``catalog_data`` plus fixture URLs. Network fetching is only
    used when the caller omits both catalog inputs.
    """

    parquet_path = dataset_cache_path(dataset_key, cache_dir=cache_dir)
    manifest_path = dataset_manifest_path(dataset_key, cache_dir=cache_dir)
    if parquet_path.exists() and manifest_path.exists() and not force_refresh:
        return read_source_manifest(manifest_path)

    if catalog_data is None and catalog_path is None:
        catalog_data = await fetch_cms_catalog()
    manifest = resolve_dataset_manifest(dataset_key, catalog_data=catalog_data, catalog_path=catalog_path)
    response = await resilient_request("GET", manifest.source_url, timeout=300.0)
    manifest.etag = manifest.etag or response.headers.get("etag", "")
    manifest.last_modified = manifest.last_modified or response.headers.get("last-modified", "")
    frame = parse_cms_payload(response.content, source_url=manifest.source_url)
    return cache_dataframe(dataset_key, frame, manifest, cache_dir=cache_dir)


async def ensure_all_datasets_cached(
    *,
    cache_dir: str | Path | None = None,
    catalog_data: dict[str, Any] | None = None,
    catalog_path: str | Path | None = None,
    force_refresh: bool = False,
) -> list[SourceManifest]:
    """Resolve, download, normalize, and cache all provider-enrollment datasets."""

    if catalog_data is None and catalog_path is None:
        catalog_data = await fetch_cms_catalog()

    manifests: list[SourceManifest] = []
    for dataset_key in DATASETS:
        manifests.append(
            await ensure_dataset_cached(
                dataset_key,
                cache_dir=cache_dir,
                catalog_data=catalog_data,
                catalog_path=catalog_path,
                force_refresh=force_refresh,
            )
        )
    return manifests


def cache_csv(
    dataset_key: str,
    csv_path: str | Path,
    manifest: SourceManifest | None = None,
    *,
    cache_dir: str | Path | None = None,
) -> SourceManifest:
    """Normalize a local CSV fixture or download and cache it as Parquet."""

    frame = read_cms_csv(csv_path)
    return cache_dataframe(dataset_key, frame, manifest or _fixture_manifest(dataset_key, csv_path), cache_dir=cache_dir)


def cache_records(
    dataset_key: str,
    records: list[dict[str, Any]],
    manifest: SourceManifest | None = None,
    *,
    cache_dir: str | Path | None = None,
) -> SourceManifest:
    """Normalize API-style JSON records and cache them as Parquet."""

    frame = pd.DataFrame(records, dtype=str).fillna("")
    return cache_dataframe(dataset_key, frame, manifest or _fixture_manifest(dataset_key, "records"), cache_dir=cache_dir)


def cache_dataframe(
    dataset_key: str,
    frame: pd.DataFrame,
    manifest: SourceManifest,
    *,
    cache_dir: str | Path | None = None,
) -> SourceManifest:
    """Normalize a DataFrame and write Parquet plus a source manifest."""

    dataset = _dataset(dataset_key)
    normalized = normalize_frame(frame, dataset_key=dataset.key)
    parquet_path = dataset_cache_path(dataset_key, cache_dir=cache_dir)
    manifest_path = dataset_manifest_path(dataset_key, cache_dir=cache_dir)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    write_atomic_parquet(parquet_path, normalized, compression="zstd", index=False)

    hydrated = SourceManifest.from_dict(manifest.to_dict())
    hydrated.record_count = len(normalized)
    hydrated.fetched_at = hydrated.fetched_at or _now_iso()
    hydrated.checksum = hydrated.checksum or _file_sha256(parquet_path)
    hydrated.extra.update(
        {
            "dataset_key": dataset.key,
            "provider_category": dataset.provider_category,
            "record_type": dataset.record_type,
            "cache_path": str(parquet_path),
        }
    )
    write_source_manifest(hydrated, manifest_path)
    return hydrated


def parse_cms_payload(content: bytes, *, source_url: str = "") -> pd.DataFrame:
    """Parse a CMS CSV, JSON API response, or Excel payload into a DataFrame."""

    suffix = source_url.lower()
    if suffix.endswith((".xlsx", ".xls")):
        return pd.read_excel(BytesIO(content), dtype=str).fillna("")

    text = content.decode("utf-8-sig", errors="replace")
    stripped = text.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        raw = json.loads(text)
        if isinstance(raw, dict):
            records = raw.get("results") or raw.get("data") or raw.get("items") or raw.get("rows") or []
        else:
            records = raw
        return pd.DataFrame(records, dtype=str).fillna("")

    return pd.read_csv(StringIO(text), dtype=str, keep_default_na=False, low_memory=False).fillna("")


def read_cms_csv(csv_path: str | Path) -> pd.DataFrame:
    """Read a CMS CSV fixture or downloaded file as all strings."""

    return pd.read_csv(csv_path, dtype=str, keep_default_na=False, low_memory=False, encoding_errors="replace").fillna("")


def normalize_frame(frame: pd.DataFrame, *, dataset_key: str) -> pd.DataFrame:
    """Preserve original CMS columns and add normalized snake-case/query aliases."""

    dataset = _dataset(dataset_key)
    normalized = frame.copy()
    normalized.columns = [str(column).strip() for column in normalized.columns]
    for column in normalized.columns:
        normalized[column] = normalized[column].map(_clean_value)

    original_columns = list(normalized.columns)
    for column in original_columns:
        alias = snake_case(column)
        if alias and alias not in normalized.columns:
            normalized[alias] = normalized[column]

    normalized["source_dataset_key"] = dataset.key
    normalized["provider_category"] = dataset.provider_category
    normalized["record_type"] = dataset.record_type
    normalized["npi"] = _series_from_candidates(normalized, _NPI_COLUMNS).map(lambda value: normalize_npi(value) or "")
    normalized["pac_id"] = _series_from_candidates(normalized, _PAC_COLUMNS).map(lambda value: normalize_pac_id(value) or "")
    normalized["enrollment_id"] = _series_from_candidates(normalized, _ENROLLMENT_COLUMNS).map(
        lambda value: normalize_enrollment_id(value) or ""
    )
    normalized["associate_id"] = _series_from_candidates(normalized, _ASSOCIATE_COLUMNS).map(
        lambda value: normalize_enrollment_id(value) or ""
    )
    normalized["ccn"] = _series_from_candidates(normalized, _CCN_COLUMNS).map(lambda value: normalize_ccn(value) or "")
    normalized["state"] = _series_from_candidates(normalized, _STATE_COLUMNS).map(lambda value: normalize_state(value) or "")
    normalized["provider_type"] = _series_from_candidates(
        normalized,
        ("provider_type", "provider_supplier_type", "medicare_provider_type", "provider_category"),
    )
    normalized["provider_name"] = _name_series(normalized, _PROVIDER_NAME_COLUMNS)
    normalized["facility_name"] = _name_series(normalized, _FACILITY_NAME_COLUMNS)
    normalized["owner_name"] = _name_series(normalized, _OWNER_NAME_COLUMNS)
    normalized["owner_associate_id"] = _series_from_candidates(
        normalized,
        ("owner_associate_id", "associate_id", "organization_associate_id", "individual_associate_id"),
    ).map(lambda value: normalize_enrollment_id(value) or "")
    normalized["owner_pac_id"] = _series_from_candidates(
        normalized,
        ("owner_pac_id", "pac_id", "owner_pecos_associate_control_id"),
    ).map(lambda value: normalize_pac_id(value) or "")
    normalized["owner_type"] = _series_from_candidates(
        normalized,
        ("owner_type", "organization_type", "individual_or_organization", "owner_entity_type"),
    )
    normalized["role_code"] = _series_from_candidates(normalized, ("role_code", "association_role_code", "owner_role_code"))
    normalized["role_text"] = _series_from_candidates(
        normalized,
        ("role_text", "association_role_text", "owner_role_text", "role_description"),
    )
    normalized["percentage_ownership"] = _series_from_candidates(
        normalized,
        ("percentage_ownership", "ownership_percentage", "ownership_or_control_interest_percentage"),
    )
    normalized["association_date"] = _series_from_candidates(
        normalized,
        ("association_date", "effective_date", "ownership_effective_date", "start_date"),
    )
    normalized["association_end_date"] = _series_from_candidates(
        normalized,
        ("association_end_date", "end_date", "termination_date", "ownership_end_date"),
    )
    normalized["is_active"] = normalized["association_end_date"].map(lambda value: not bool(str(value).strip()))
    normalized["private_equity"] = _series_from_candidates(
        normalized,
        ("private_equity", "private_equity_company", "private_equity_flag"),
    )
    normalized["reit"] = _series_from_candidates(normalized, ("reit", "real_estate_investment_trust", "reit_flag"))
    normalized["holding_company"] = _series_from_candidates(
        normalized,
        ("holding_company", "holding_company_flag", "parent_holding_company"),
    )
    normalized["transaction_date"] = _series_from_candidates(
        normalized,
        ("transaction_date", "change_date", "chow_date", "effective_date"),
    )
    normalized["effective_date"] = _series_from_candidates(normalized, ("effective_date", "transaction_effective_date"))
    normalized["change_type"] = _series_from_candidates(
        normalized,
        ("change_type", "transaction_type", "chow_type", "change_of_ownership_type"),
    )

    return normalized.fillna("")


def load_cached_frame(dataset_key: str, *, cache_dir: str | Path | None = None) -> pd.DataFrame:
    """Load one cached Parquet dataset, returning an empty frame when missing."""

    path = dataset_cache_path(dataset_key, cache_dir=cache_dir)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path).fillna("")


def load_cached_frames(
    dataset_keys: tuple[str, ...] | list[str],
    *,
    cache_dir: str | Path | None = None,
) -> pd.DataFrame:
    """Load and concatenate cached datasets."""

    frames = [load_cached_frame(key, cache_dir=cache_dir) for key in dataset_keys]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).fillna("")


def source_metadata_for_keys(
    dataset_keys: tuple[str, ...] | list[str],
    *,
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Read source manifests for datasets that have cached metadata."""

    metadata: list[dict[str, Any]] = []
    for key in dataset_keys:
        path = dataset_manifest_path(key, cache_dir=cache_dir)
        if not path.exists():
            continue
        manifest = read_source_manifest(path)
        payload = manifest.to_dict()
        payload["dataset_key"] = key
        payload["source_name"] = "CMS Provider Enrollment"
        payload["source_period"] = _source_period_for_manifest(manifest)
        payload["retrieved_at"] = manifest.fetched_at
        payload["source_modified"] = manifest.modified or manifest.last_modified
        payload["cache_status"] = "ready" if dataset_cache_path(key, cache_dir=cache_dir).exists() else "missing"
        payload["cache_freshness"] = _cache_freshness_for_manifest(manifest)
        payload["entity_scope"] = _entity_scope_for_key(key)
        payload["query"] = {"dataset_key": key}
        payload["cache_key"] = key
        payload["confidence"] = "source_manifest"
        payload["cache_path"] = manifest.extra.get("cache_path") or str(dataset_cache_path(key, cache_dir=cache_dir))
        metadata.append(payload)
    return metadata


def source_evidence_for_row(row: dict[str, Any], *, cache_dir: str | Path | None = None) -> dict[str, Any]:
    """Return report-ingest source evidence fields for one normalized cached row."""

    dataset_key = str(row.get("source_dataset_key") or row.get("dataset_key") or "")
    payload = source_metadata_for_keys((dataset_key,), cache_dir=cache_dir)
    if payload:
        metadata = payload[0]
        return {
            "source_name": metadata.get("source_name", "CMS Provider Enrollment"),
            "source_url": metadata.get("source_url", ""),
            "dataset_id": dataset_key,
            "source_period": metadata.get("source_period", ""),
            "landing_page": metadata.get("landing_page", ""),
            "retrieved_at": metadata.get("retrieved_at") or metadata.get("fetched_at", ""),
            "source_modified": metadata.get("source_modified") or metadata.get("modified", ""),
            "cache_status": metadata.get("cache_status", ""),
            "cache_freshness": metadata.get("cache_freshness", ""),
            "entity_scope": metadata.get("entity_scope") or _entity_scope_for_key(dataset_key),
            "query": {"dataset_key": dataset_key},
            "cache_key": dataset_key,
            "confidence": "source_row",
        }

    return {
        "source_name": "CMS Provider Enrollment",
        "source_url": "",
        "dataset_id": dataset_key,
        "source_period": "",
        "landing_page": "",
        "retrieved_at": "",
        "source_modified": "",
        "cache_status": "missing_manifest",
        "cache_freshness": "source manifest is missing; cache freshness cannot be verified",
        "entity_scope": _entity_scope_for_key(dataset_key),
        "query": {"dataset_key": dataset_key} if dataset_key else {},
        "cache_key": dataset_key,
        "confidence": "missing_manifest",
    }


def search_enrollments(
    *,
    npi: str = "",
    provider_name: str = "",
    state: str = "",
    provider_type: str = "",
    limit: int = 25,
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Search cached Medicare FFS and facility enrollment rows."""

    frame = load_cached_frames(ENROLLMENT_DATASET_KEYS, cache_dir=cache_dir)
    if frame.empty:
        return []

    filtered = frame
    npi_norm = normalize_npi(npi) if npi else ""
    state_norm = normalize_state(state) if state else ""
    if npi_norm:
        filtered = filtered[filtered["npi"] == npi_norm]
    if provider_name:
        query = normalize_name(provider_name)
        filtered = filtered[
            filtered["provider_name"].map(lambda value: query in normalize_name(value))
            | filtered["facility_name"].map(lambda value: query in normalize_name(value))
        ]
    if state_norm:
        filtered = filtered[filtered["state"] == state_norm]
    if provider_type:
        type_query = normalize_name(provider_type)
        filtered = filtered[filtered["provider_type"].map(lambda value: type_query in normalize_name(value))]

    return _rows(filtered.head(_bounded_limit(limit, 100)))


def get_enrollment_detail(
    *,
    npi: str = "",
    enrollment_id: str = "",
    associate_id: str = "",
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Fetch enrollment rows by exact NPI, enrollment ID, or associate ID."""

    frame = load_cached_frames(ENROLLMENT_DATASET_KEYS, cache_dir=cache_dir)
    if frame.empty:
        return []
    predicates: list[pd.Series] = []
    npi_norm = normalize_npi(npi) if npi else ""
    enrollment_norm = normalize_enrollment_id(enrollment_id) if enrollment_id else ""
    associate_norm = normalize_enrollment_id(associate_id) if associate_id else ""
    if npi_norm:
        predicates.append(frame["npi"] == npi_norm)
    if enrollment_norm:
        predicates.append(frame["enrollment_id"] == enrollment_norm)
    if associate_norm:
        predicates.append(frame["associate_id"] == associate_norm)
    if not predicates:
        return []

    mask = predicates[0]
    for predicate in predicates[1:]:
        mask = mask | predicate
    return _rows(frame[mask].head(100))


def query_ownership(
    *,
    ccn: str = "",
    facility_name: str = "",
    state: str = "",
    provider_category: str = "",
    include_indirect: bool = True,
    limit: int = 50,
    enrollment_ids: list[str] | None = None,
    owner_name: str = "",
    owner_associate_id: str = "",
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Query active owner/facility relationships from all-owner datasets."""

    frame = load_cached_frames(OWNER_DATASET_KEYS, cache_dir=cache_dir)
    if frame.empty:
        return []

    filtered = frame
    ccn_norm = normalize_ccn(ccn) if ccn else ""
    state_norm = normalize_state(state) if state else ""
    owner_associate_norm = normalize_enrollment_id(owner_associate_id) if owner_associate_id else ""
    enrollment_set = {normalize_enrollment_id(value) for value in enrollment_ids or [] if normalize_enrollment_id(value)}

    if ccn_norm:
        filtered = filtered[filtered["ccn"] == ccn_norm]
    if facility_name:
        query = normalize_name(facility_name)
        filtered = filtered[filtered["facility_name"].map(lambda value: query in normalize_name(value))]
    if state_norm:
        filtered = filtered[filtered["state"] == state_norm]
    if provider_category:
        filtered = filtered[filtered["provider_category"] == snake_case(provider_category)]
    if enrollment_set:
        filtered = filtered[filtered["enrollment_id"].isin(enrollment_set)]
    if owner_name:
        query = normalize_name(owner_name)
        filtered = filtered[filtered["owner_name"].map(lambda value: query in normalize_name(value))]
    if owner_associate_norm:
        filtered = filtered[filtered["owner_associate_id"] == owner_associate_norm]
    if not include_indirect:
        filtered = filtered[
            filtered["role_text"].map(lambda value: "INDIRECT" not in normalize_name(value))
            & filtered["role_code"].map(lambda value: "INDIRECT" not in normalize_name(value))
        ]

    active = filtered[filtered["is_active"].astype(bool)] if "is_active" in filtered.columns else filtered
    return _rows(active.head(_bounded_limit(limit, 200)))


def query_chow(
    *,
    ccn: str = "",
    facility_name: str = "",
    state: str = "",
    provider_category: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 50,
    enrollment_ids: list[str] | None = None,
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Search CHOW and CHOW owner-information history records."""

    frame = load_cached_frames(CHOW_DATASET_KEYS, cache_dir=cache_dir)
    if frame.empty:
        return []

    filtered = frame
    ccn_norm = normalize_ccn(ccn) if ccn else ""
    state_norm = normalize_state(state) if state else ""
    enrollment_set = {normalize_enrollment_id(value) for value in enrollment_ids or [] if normalize_enrollment_id(value)}
    if ccn_norm:
        filtered = filtered[filtered["ccn"] == ccn_norm]
    if facility_name:
        query = normalize_name(facility_name)
        filtered = filtered[filtered["facility_name"].map(lambda value: query in normalize_name(value))]
    if state_norm:
        filtered = filtered[filtered["state"] == state_norm]
    if provider_category:
        filtered = filtered[filtered["provider_category"] == snake_case(provider_category)]
    if enrollment_set:
        filtered = filtered[filtered["enrollment_id"].isin(enrollment_set)]
    if start_date:
        filtered = filtered[filtered["transaction_date"].astype(str) >= start_date]
    if end_date:
        filtered = filtered[filtered["transaction_date"].astype(str) <= end_date]

    return _rows(filtered.head(_bounded_limit(limit, 200)))


def row_to_raw(row: dict[str, Any]) -> dict[str, Any]:
    """Return original-ish CMS fields plus aliases for audit/debug display."""

    excluded = {"is_active"}
    return {key: _json_value(value) for key, value in row.items() if key not in excluded}


def snake_case(value: Any) -> str:
    """Normalize arbitrary CMS column names to lower snake-case."""

    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return re.sub(r"_+", "_", text).strip("_")


def _dataset(dataset_key: str) -> ProviderEnrollmentDataset:
    try:
        return DATASETS[dataset_key]
    except KeyError as exc:
        raise KeyError(f"Unknown provider enrollment dataset: {dataset_key}") from exc


def _entity_scope_for_key(dataset_key: str) -> str:
    if not dataset_key:
        return ""
    try:
        dataset = _dataset(dataset_key)
    except KeyError:
        return dataset_key
    return f"{dataset.provider_category}:{dataset.record_type}"


def _source_period_for_manifest(manifest: SourceManifest) -> str:
    return (
        manifest.modified
        or manifest.last_modified
        or manifest.fetched_at
        or "latest cached CMS provider-enrollment public file"
    )


def _cache_freshness_for_manifest(manifest: SourceManifest) -> str:
    parts = ["ready"]
    if manifest.fetched_at:
        parts.append(f"fetched_at={manifest.fetched_at}")
    if manifest.modified or manifest.last_modified:
        parts.append(f"source_modified={manifest.modified or manifest.last_modified}")
    if manifest.record_count is not None:
        parts.append(f"record_count={manifest.record_count}")
    return "; ".join(parts)


def _fixture_manifest(dataset_key: str, source: str | Path) -> SourceManifest:
    dataset = _dataset(dataset_key)
    return SourceManifest(
        source_url=str(source),
        landing_page=f"fixture://{dataset.key}",
        dataset_id=f"fixture-{dataset.key}",
        title=dataset.title,
        modified="",
        fetched_at=_now_iso(),
        extra={"dataset_key": dataset.key, "provider_category": dataset.provider_category},
    )


def _series_from_candidates(frame: pd.DataFrame, candidates: tuple[str, ...]) -> pd.Series:
    columns = {snake_case(column): column for column in frame.columns}
    for candidate in candidates:
        column = columns.get(snake_case(candidate))
        if column is not None:
            return frame[column].astype(str).fillna("")
    return pd.Series([""] * len(frame), index=frame.index, dtype=str)


def _name_series(frame: pd.DataFrame, candidates: tuple[str, ...]) -> pd.Series:
    series = _series_from_candidates(frame, candidates)
    if series.map(bool).any():
        return series

    first = _series_from_candidates(frame, ("first_name", "owner_first_name", "provider_first_name"))
    middle = _series_from_candidates(frame, ("middle_name", "midname", "owner_middle_name"))
    last = _series_from_candidates(frame, ("last_name", "lastname", "owner_last_name", "provider_last_name"))
    return (first + " " + middle + " " + last).map(lambda value: " ".join(str(value).split()))


def _rows(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return [{key: _json_value(value) for key, value in row.items()} for row in frame.to_dict(orient="records")]


def _json_value(value: Any) -> Any:
    if pd.isna(value):
        return ""
    if isinstance(value, (bool, int, float, str)) or value is None:
        return value
    return str(value)


def _clean_value(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _bounded_limit(limit: int, maximum: int) -> int:
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        parsed = 25
    return max(1, min(parsed, maximum))


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
