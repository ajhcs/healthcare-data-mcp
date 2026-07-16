"""Typed Scale roster and bed-basis acquisition and bundle-input builder.

The acquisition stage freezes allowlisted public artifacts and verifies each
configured fact against the frozen bytes.  The build stage consumes only that
frozen snapshot, so a later pinned checkout never depends on live web content.
"""

from __future__ import annotations

import csv
import hashlib
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from bs4 import BeautifulSoup
from pydantic import JsonValue
from pypdf import PdfReader

from shared.acquisition.scale_roster_bed_models import (
    CONNECTOR_VERSION,
    PARSER_VERSION,
    WORKFLOW_ID,
    AcquisitionSpec,
    EntitySpec,
    ExtractedFact,
    FactSpec,
    FrozenAcquisition,
    FrozenArtifact,
    SourceSpec,
    is_allowlisted_https_url,
)
from shared.contracts.public_evidence import PublicEvidenceBundleInput, canonical_sha256
from shared.utils.cache import write_atomic_bytes, write_atomic_json
from shared.utils.http_client import resilient_request


@dataclass(frozen=True)
class ParsedCsv:
    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


async def acquire(spec: AcquisitionSpec, *, cache_root: Path, cache_run_id: str) -> FrozenAcquisition:
    """Download reviewed sources, freeze raw bytes, and verify configured facts."""

    if re.fullmatch(r"[A-Za-z0-9._-]+", cache_run_id) is None:
        raise ValueError("cache_run_id must be a portable path segment")
    run_root = cache_root / WORKFLOW_ID / cache_run_id
    staging_root = run_root.with_name(f".{cache_run_id}.partial")
    if run_root.exists() or staging_root.exists():
        raise FileExistsError(f"cache run already exists: {cache_run_id}")
    staging_root.mkdir(parents=True, exist_ok=False)
    artifacts: list[FrozenArtifact] = []
    paths_by_source: dict[str, Path] = {}
    source_by_id = {item.source_id: item for item in spec.sources}
    try:
        for source in spec.sources:
            response = await resilient_request(
                "GET",
                source.url,
                timeout=300.0,
                follow_redirects=True,
                headers={"User-Agent": "AJHCS-healthcare-data-mcp/0.4 public-evidence-acquisition"},
            )
            media_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            if not is_allowlisted_https_url(str(response.url)):
                raise ValueError(f"redirect escaped the reviewed source allowlist for {source.source_id}")
            if not _media_type_matches(source.expected_media_type, media_type):
                raise ValueError(f"unexpected media type for {source.source_id}: {media_type}")
            suffix = _suffix_for_media(media_type, source.parser_kind)
            path = staging_root / f"{source.source_id}{suffix}"
            write_atomic_bytes(path, response.content)
            paths_by_source[source.source_id] = path
            checksum = _bytes_sha256(response.content)
            modified = _http_datetime(response.headers.get("last-modified"))
            artifacts.append(
                FrozenArtifact(
                    source_id=source.source_id,
                    artifact_id=f"artifact:{source.source_id}:{checksum.removeprefix('sha256:')[:16]}",
                    source_url=source.url,
                    final_url=str(response.url),
                    retrieved_at=datetime.now(timezone.utc),
                    source_modified=modified,
                    media_type=media_type or source.expected_media_type,
                    checksum_sha256=checksum,
                    content_length=len(response.content),
                    cache_run_id=cache_run_id,
                    portable_uri=f"hc-cache://{WORKFLOW_ID}/{cache_run_id}/{path.name}",
                    connector_version=CONNECTOR_VERSION,
                    parser_version=PARSER_VERSION,
                    schema_fingerprint=canonical_sha256(
                        {"parser_kind": source.parser_kind, "parser_version": PARSER_VERSION}
                    ),
                )
            )
        parsed_by_source = {
            source.source_id: _parse_source(paths_by_source[source.source_id], source.parser_kind)
            for source in spec.sources
        }
        extracted = []
        for fact in spec.facts:
            if fact.missingness is not None:
                continue
            source = source_by_id[fact.source_id or ""]
            value = _extract_from_parsed(fact, parsed_by_source[source.source_id])
            extracted.append(
                ExtractedFact(
                    fact_id=fact.fact_id,
                    value=value,
                    normalized_content_checksum=canonical_sha256(_normalized_fact_payload(fact, value)),
                )
            )
        frozen = FrozenAcquisition(
            acquired_at=datetime.now(timezone.utc),
            cache_run_id=cache_run_id,
            artifacts=artifacts,
            extracted_facts=extracted,
        )
        staging_root.replace(run_root)
        return frozen
    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise


def verify_frozen_bytes(
    spec: AcquisitionSpec,
    frozen: FrozenAcquisition,
    *,
    cache_root: Path,
) -> FrozenAcquisition:
    """Reparse frozen cache bytes, rejecting artifact or normalized-row drift."""

    _validate_frozen_against_spec(spec, frozen)
    source_by_id = {item.source_id: item for item in spec.sources}
    parsed_by_source: dict[str, str | ParsedCsv] = {}
    extracted: list[ExtractedFact] = []
    for artifact in frozen.artifacts:
        path = _cache_path(cache_root, artifact.portable_uri)
        raw = path.read_bytes()
        if len(raw) != artifact.content_length or _bytes_sha256(raw) != artifact.checksum_sha256:
            raise ValueError(f"frozen artifact content drift for {artifact.artifact_id}")
        source = source_by_id[artifact.source_id]
        parsed_by_source[artifact.source_id] = _parse_source(path, source.parser_kind)
    for fact in spec.facts:
        if fact.missingness is not None:
            continue
        source = source_by_id[fact.source_id or ""]
        value = _extract_from_parsed(fact, parsed_by_source[source.source_id])
        extracted.append(
            ExtractedFact(
                fact_id=fact.fact_id,
                value=value,
                normalized_content_checksum=canonical_sha256(_normalized_fact_payload(fact, value)),
            )
        )
    reparsed = frozen.model_copy(update={"extracted_facts": extracted})
    if reparsed.extracted_facts != frozen.extracted_facts:
        raise ValueError("frozen parser output drifted from the recorded normalized rows")
    return reparsed


def build_bundle_input(spec: AcquisitionSpec, frozen: FrozenAcquisition) -> PublicEvidenceBundleInput:
    """Build a deterministic Public Evidence Bundle input from frozen acquisition state."""

    source_specs = {item.source_id: item for item in spec.sources}
    artifacts = {item.source_id: item for item in frozen.artifacts}
    extracted = {item.fact_id: item for item in frozen.extracted_facts}
    _validate_frozen_against_spec(spec, frozen)
    observations: list[dict[str, JsonValue]] = []
    coverage: list[dict[str, JsonValue]] = []
    sources: list[dict[str, JsonValue]] = []
    observation_by_fact: dict[str, str] = {}
    receipt_by_fact: dict[str, str] = {}
    for fact in sorted(spec.facts, key=lambda item: item.fact_id):
        coverage_id = f"coverage:{fact.fact_id}"
        if fact.missingness is not None:
            coverage.append(
                {
                    "coverage_id": coverage_id,
                    "entity_ref": fact.entity_id,
                    "measure_id": fact.measure_id,
                    "status": fact.missingness,
                    "observation_refs": [],
                    "reason": fact.missingness_reason,
                }
            )
            continue
        item = extracted[fact.fact_id]
        expected_checksum = canonical_sha256(_normalized_fact_payload(fact, item.value))
        if item.normalized_content_checksum != expected_checksum:
            raise ValueError(f"normalized row checksum drift for fact {fact.fact_id}")
        source_spec = source_specs[fact.source_id or ""]
        artifact = artifacts[source_spec.source_id]
        observation_id = f"observation:{fact.fact_id}"
        receipt_id = f"receipt:{fact.fact_id}"
        observation_by_fact[fact.fact_id] = observation_id
        receipt_by_fact[fact.fact_id] = receipt_id
        period: dict[str, JsonValue] = {"label": fact.period_label}
        if fact.period_start:
            period["start"] = fact.period_start
        if fact.period_end:
            period["end"] = fact.period_end
        observations.append(
            {
                "observation_id": observation_id,
                "measure_id": fact.measure_id,
                "value_type": fact.value_type,
                "value": item.value,
                "unit": fact.unit,
                "period": period,
                "denominator_scope": fact.denominator_scope,
                "entity_ref": fact.entity_id,
                "receipt_refs": [receipt_id],
                "derivation_class": "source_reported",
                "caveat": fact.caveat,
                "dependency_cluster_ids": fact.dependency_cluster_ids or [f"dependency:{source_spec.source_id}"],
            }
        )
        coverage.append(
            {
                "coverage_id": coverage_id,
                "entity_ref": fact.entity_id,
                "measure_id": fact.measure_id,
                "status": "populated",
                "observation_refs": [observation_id],
                "reason": "Frozen source row was verified and has a matching receipt.",
            }
        )
        artifact_payload = _artifact_payload(artifact)
        sources.append(
            {
                "source_id": f"source:{fact.fact_id}",
                "registry_id": source_spec.registry_id,
                "registry_version": source_spec.registry_version,
                "receipt": {
                    "receipt_id": receipt_id,
                    "source_name": source_spec.source_name,
                    "source_url": artifact.final_url,
                    "dataset_id": source_spec.dataset_id,
                    "source_period": source_spec.source_period,
                    "landing_page": source_spec.landing_page,
                    "retrieved_at": artifact.retrieved_at,
                    "source_modified": artifact.source_modified,
                    "cache_status": "frozen_verified",
                    "cache_freshness": f"Frozen in cache run {frozen.cache_run_id}",
                    "entity_scope": fact.entity_id,
                    "query": {
                        "workflow": WORKFLOW_ID,
                        "fact_id": fact.fact_id,
                        "source_field": fact.table_value_field or "named regex group: value",
                        "table_match": fact.table_match,
                    },
                    "cache_key": artifact.portable_uri,
                    "match_basis": fact.match_basis,
                    "confidence": fact.confidence,
                    "caveat": fact.caveat,
                    "next_step": fact.next_step,
                    "acquisition_method": CONNECTOR_VERSION,
                    "rights_classification": source_spec.rights_classification,
                    "row_locator": fact.row_locator,
                    "artifact": artifact_payload,
                    "parent_receipt_ids": [],
                },
                "content_checksum": item.normalized_content_checksum,
                "access_rights": source_spec.rights_classification,
            }
        )
    conflicts = []
    for conflict in sorted(spec.conflicts, key=lambda item: item.conflict_id):
        conflicts.append(
            {
                "conflict_id": conflict.conflict_id,
                "conflict_type": conflict.conflict_type,
                "entity_refs": conflict.entity_ids,
                "observation_refs": [observation_by_fact[item] for item in conflict.fact_ids if item in observation_by_fact],
                "receipt_refs": [receipt_by_fact[item] for item in conflict.fact_ids if item in receipt_by_fact],
                "status": conflict.status,
                "rationale": conflict.rationale,
            }
        )
    return PublicEvidenceBundleInput.model_validate(
        {
            "bundle_id": spec.bundle_id,
            "producer": {"repo": "healthcare-data-mcp", "version": spec.producer_version, "commit": "0" * 40},
            "created_at": frozen.acquired_at,
            "request": {
                "workflow": WORKFLOW_ID,
                "parameters": {"acquisition_cutoff": frozen.acquired_at.isoformat(), "no_scale_score": True},
            },
            "scope": {"systems": spec.systems, "market": spec.market, "periods": spec.periods},
            "entities": [_entity_payload(item, source_specs, spec.facts, artifacts) for item in sorted(spec.entities, key=lambda value: value.entity_id)],
            "observations": observations,
            "sources": sources,
            "coverage": coverage,
            "conflicts": conflicts,
            "input_artifacts": [_artifact_payload(item) for item in sorted(frozen.artifacts, key=lambda value: value.artifact_id)],
        }
    )


def write_frozen_acquisition(path: Path, frozen: FrozenAcquisition) -> None:
    write_atomic_json(path, frozen.model_dump(mode="json"))


def write_bundle_input(path: Path, bundle_input: PublicEvidenceBundleInput) -> None:
    write_atomic_json(path, bundle_input.model_dump(mode="json"))


def _entity_payload(
    entity: EntitySpec,
    source_specs: dict[str, SourceSpec],
    facts: list[FactSpec],
    artifacts: dict[str, FrozenArtifact],
) -> dict[str, JsonValue]:
    aliases = []
    identity_fact = next(
        (fact for fact in facts if fact.entity_id == entity.entity_id and fact.measure_id == "system_identity" and fact.source_id),
        None,
    )
    for alias in entity.aliases:
        row: dict[str, JsonValue] = {"source_name": "Reviewed public identity source", "name": alias}
        if identity_fact is not None:
            source = source_specs[identity_fact.source_id or ""]
            artifact = artifacts[source.source_id]
            row.update({"source_name": source.source_name, "source_url": artifact.final_url, "retrieved_at": artifact.retrieved_at.isoformat()})
        aliases.append(row)
    unresolved = list(entity.unresolved_identifiers)
    if entity.state_license_id:
        unresolved.append({"identifier_type": "state_license_id", "identifier": entity.state_license_id})
    return {
        "entity_id": entity.entity_id,
        "canonical_name": entity.canonical_name,
        "entity_type": entity.entity_type,
        "ccn": entity.ccn,
        "owner_id": entity.owner_entity_id,
        "address": entity.address,
        "zip_code": entity.zip_code,
        "aliases": aliases,
        "match_decisions": [],
        "conflicts": entity.identity_conflicts,
        "unresolved_identifiers": unresolved,
    }


def _artifact_payload(artifact: FrozenArtifact) -> dict[str, JsonValue]:
    return {
        "artifact_id": artifact.artifact_id,
        "checksum_sha256": artifact.checksum_sha256,
        "media_type": artifact.media_type,
        "uri": artifact.portable_uri,
        "cache_run_id": artifact.cache_run_id,
        "connector": CONNECTOR_VERSION,
        "connector_version": artifact.connector_version,
        "parser_version": artifact.parser_version,
        "schema_fingerprint": artifact.schema_fingerprint,
    }


def _extract_text(path: Path, parser_kind: str) -> str:
    if parser_kind == "pdf":
        return "\n".join(page.extract_text() or "" for page in PdfReader(path).pages)
    raw = path.read_bytes()
    text = raw.decode("utf-8", errors="replace")
    if parser_kind == "html":
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    return text


def _extract_fact(fact: FactSpec, path: Path, parser_kind: str) -> JsonValue:
    return _extract_from_parsed(fact, _parse_source(path, parser_kind))


def _parse_source(path: Path, parser_kind: str) -> str | ParsedCsv:
    if parser_kind == "csv":
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError(f"CSV source has no header: {path.name}")
            return ParsedCsv(tuple(reader.fieldnames), tuple(dict(row) for row in reader))
    return _extract_text(path, parser_kind)


def _extract_from_parsed(fact: FactSpec, parsed: str | ParsedCsv) -> JsonValue:
    if fact.table_match:
        if not isinstance(parsed, ParsedCsv):
            raise ValueError(f"tabular extractor requires CSV source for fact {fact.fact_id}")
        return _extract_table_value(fact, parsed)
    if isinstance(parsed, ParsedCsv):
        raise ValueError(f"CSV source requires a structured table extractor for fact {fact.fact_id}")
    return _extract_pattern_value(fact, parsed)


def _extract_table_value(fact: FactSpec, parsed: ParsedCsv) -> JsonValue:
    reader_fields = set(parsed.fieldnames)
    required = {*fact.table_match, fact.table_value_field}
    missing = sorted(required - reader_fields)
    if missing:
        raise ValueError(f"CSV source missing field(s) for fact {fact.fact_id}: {', '.join(missing)}")
    rows = [
        row
        for row in parsed.rows
        if all((row.get(key) or "").strip() == value for key, value in fact.table_match.items())
    ]
    if len(rows) != 1:
        raise ValueError(f"tabular identity for fact {fact.fact_id} matched {len(rows)} rows; expected exactly one")
    raw = fact.literal_value if fact.literal_value is not None else rows[0][fact.table_value_field]
    return _coerce_value(fact, raw)


def _extract_pattern_value(fact: FactSpec, text: str) -> JsonValue:
    match = re.search(fact.extraction_pattern or "", text, flags=re.IGNORECASE | re.MULTILINE | re.DOTALL)
    if match is None:
        raise ValueError(f"source pattern did not match fact {fact.fact_id}")
    raw: JsonValue = match.groupdict().get("value", match.group(0))
    if fact.literal_value is not None:
        raw = fact.literal_value
    return _coerce_value(fact, raw)


def _coerce_value(fact: FactSpec, raw: JsonValue) -> JsonValue:
    if fact.value_type == "integer":
        cleaned = re.sub(r"[^0-9.-]", "", str(raw))
        numeric = float(cleaned)
        if not numeric.is_integer() or numeric < 0:
            raise ValueError(f"bed/count fact {fact.fact_id} must be a non-negative integer")
        return int(numeric)
    if fact.value_type == "boolean":
        if isinstance(raw, bool):
            return raw
        normalized = str(raw).strip().casefold()
        if normalized not in {"true", "false"}:
            raise ValueError(f"boolean fact {fact.fact_id} did not extract true/false")
        return normalized == "true"
    return str(raw).strip()


def _normalized_fact_payload(fact: FactSpec, value: JsonValue) -> dict[str, JsonValue]:
    return {
        "fact_id": fact.fact_id,
        "entity_id": fact.entity_id,
        "measure_id": fact.measure_id,
        "value_type": fact.value_type,
        "value": value,
        "unit": fact.unit,
        "period_label": fact.period_label,
        "period_start": fact.period_start,
        "period_end": fact.period_end,
        "denominator_scope": fact.denominator_scope,
        "row_locator": fact.row_locator,
    }


def _validate_frozen_against_spec(spec: AcquisitionSpec, frozen: FrozenAcquisition) -> None:
    source_ids = {item.source_id for item in spec.sources}
    frozen_source_ids = {item.source_id for item in frozen.artifacts}
    if source_ids != frozen_source_ids:
        raise ValueError("frozen artifact sources must exactly match the acquisition specification")
    source_by_id = {item.source_id: item for item in spec.sources}
    for artifact in frozen.artifacts:
        source = source_by_id[artifact.source_id]
        if artifact.source_url != source.url:
            raise ValueError(f"frozen source URL drift for {artifact.source_id}")
        expected_fingerprint = canonical_sha256(
            {"parser_kind": source.parser_kind, "parser_version": PARSER_VERSION}
        )
        if artifact.schema_fingerprint != expected_fingerprint:
            raise ValueError(f"frozen parser schema drift for {artifact.source_id}")
        checksum_fragment = artifact.checksum_sha256.removeprefix("sha256:")[:16]
        if artifact.artifact_id != f"artifact:{artifact.source_id}:{checksum_fragment}":
            raise ValueError(f"artifact identity/checksum conflict for {artifact.source_id}")
    expected_fact_ids = {item.fact_id for item in spec.facts if item.missingness is None}
    frozen_fact_ids = {item.fact_id for item in frozen.extracted_facts}
    if expected_fact_ids != frozen_fact_ids:
        raise ValueError("frozen extracted facts must exactly match all populated fact specifications")


def _cache_path(cache_root: Path, portable_uri: str) -> Path:
    prefix = "hc-cache://"
    if not portable_uri.startswith(prefix):
        raise ValueError("frozen cache locator is not portable")
    relative = Path(portable_uri.removeprefix(prefix))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("frozen cache locator escapes the cache root")
    return cache_root / relative


def _bytes_sha256(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _media_type_matches(expected: str, actual: str) -> bool:
    if not actual:
        return False
    if expected == actual:
        return True
    aliases = {
        "text/csv": {"application/csv", "application/octet-stream", "text/plain"},
        "application/pdf": {"application/octet-stream"},
        "text/html": {"application/xhtml+xml"},
    }
    return actual in aliases.get(expected, set())


def _suffix_for_media(media_type: str, parser_kind: str) -> str:
    return {"pdf": ".pdf", "csv": ".csv", "html": ".html", "text": ".txt"}[parser_kind]


def _http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    from email.utils import parsedate_to_datetime

    parsed = parsedate_to_datetime(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def load_spec(path: Path) -> AcquisitionSpec:
    return AcquisitionSpec.model_validate_json(path.read_text(encoding="utf-8"))


def load_frozen(path: Path) -> FrozenAcquisition:
    return FrozenAcquisition.model_validate_json(path.read_text(encoding="utf-8"))


__all__ = [
    "CONNECTOR_VERSION",
    "PARSER_VERSION",
    "WORKFLOW_ID",
    "AcquisitionSpec",
    "FrozenAcquisition",
    "acquire",
    "build_bundle_input",
    "load_frozen",
    "load_spec",
    "verify_frozen_bytes",
    "write_bundle_input",
    "write_frozen_acquisition",
]
