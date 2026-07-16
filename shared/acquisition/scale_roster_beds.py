"""Typed connector and cache verifier for Scale roster/bed source artifacts.

The acquisition stage freezes allowlisted public artifacts and verifies each
configured fact against frozen bytes, so a pinned checkout never depends on
live web content or dirty working-tree artifacts.
"""

from __future__ import annotations

import hashlib
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from shared.acquisition.scale_roster_bed_builder import build_bundle_input, write_bundle_input
from shared.acquisition.scale_roster_bed_models import (
    CONNECTOR_VERSION,
    PARSER_VERSION,
    WORKFLOW_ID,
    AcquisitionSpec,
    ExtractedFact,
    FrozenAcquisition,
    FrozenArtifact,
    is_allowlisted_https_url,
)
from shared.acquisition.scale_roster_bed_parser import (
    ParsedCsv,
    _extract_fact,
    _extract_from_parsed,
    _parse_source,
    _verify_absence_checks,
)
from shared.acquisition.scale_roster_bed_validation import (
    normalized_fact_payload as _normalized_fact_payload,
)
from shared.acquisition.scale_roster_bed_validation import (
    validate_frozen_against_spec as _validate_frozen_against_spec,
)
from shared.contracts.public_evidence import canonical_sha256
from shared.utils.cache import write_atomic_bytes, write_atomic_json
from shared.utils.http_client import resilient_request


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
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; AJHCS-healthcare-data-mcp/0.4; public-evidence-acquisition)"
                },
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
                        {
                            "encoding": source.encoding,
                            "header_row": source.header_row,
                            "parser_kind": source.parser_kind,
                            "parser_version": PARSER_VERSION,
                        }
                    ),
                )
            )
        parsed_by_source = {
            source.source_id: _parse_source(
                paths_by_source[source.source_id], source.parser_kind, source.encoding, source.header_row
            )
            for source in spec.sources
        }
        _verify_absence_checks(spec, parsed_by_source)
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
        parsed_by_source[artifact.source_id] = _parse_source(
            path, source.parser_kind, source.encoding, source.header_row
        )
    _verify_absence_checks(spec, parsed_by_source)
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
    return {"pdf": ".pdf", "csv": ".csv", "xlsx": ".xlsx", "html": ".html", "text": ".txt"}[parser_kind]


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


def write_frozen_acquisition(path: Path, frozen: FrozenAcquisition) -> None:
    write_atomic_json(path, frozen.model_dump(mode="json"))


__all__ = [
    "CONNECTOR_VERSION",
    "PARSER_VERSION",
    "WORKFLOW_ID",
    "AcquisitionSpec",
    "FrozenAcquisition",
    "_extract_fact",
    "acquire",
    "build_bundle_input",
    "load_frozen",
    "load_spec",
    "verify_frozen_bytes",
    "write_bundle_input",
    "write_frozen_acquisition",
]
