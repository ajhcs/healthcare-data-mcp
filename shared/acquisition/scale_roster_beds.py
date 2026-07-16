"""Typed connector and offline parser for Scale roster/bed source artifacts.

The acquisition stage freezes allowlisted public artifacts and verifies each
configured fact against frozen bytes, so a pinned checkout never depends on
live web content or dirty working-tree artifacts.
"""

from __future__ import annotations

import csv
import hashlib
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree
from zipfile import ZipFile

from bs4 import BeautifulSoup
from pydantic import JsonValue
from pypdf import PdfReader

from shared.acquisition.scale_roster_bed_builder import build_bundle_input, write_bundle_input
from shared.acquisition.scale_roster_bed_models import (
    CONNECTOR_VERSION,
    PARSER_VERSION,
    WORKFLOW_ID,
    AcquisitionSpec,
    ExtractedFact,
    FactSpec,
    FrozenAcquisition,
    FrozenArtifact,
    is_allowlisted_https_url,
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


def _extract_text(path: Path, parser_kind: str, encoding: str = "utf-8-sig") -> str:
    if parser_kind == "pdf":
        return "\n".join(page.extract_text() or "" for page in PdfReader(path).pages)
    raw = path.read_bytes()
    text = raw.decode(encoding, errors="strict")
    if parser_kind == "html":
        return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
    return text


def _extract_fact(
    fact: FactSpec,
    path: Path,
    parser_kind: str,
    encoding: str = "utf-8-sig",
    header_row: int = 1,
) -> JsonValue:
    return _extract_from_parsed(fact, _parse_source(path, parser_kind, encoding, header_row))


def _parse_source(
    path: Path,
    parser_kind: str,
    encoding: str = "utf-8-sig",
    header_row: int = 1,
) -> str | ParsedCsv:
    if parser_kind == "csv":
        with path.open(encoding=encoding, newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError(f"CSV source has no header: {path.name}")
            return ParsedCsv(tuple(reader.fieldnames), tuple(dict(row) for row in reader))
    if parser_kind == "xlsx":
        return _parse_xlsx(path, header_row)
    return _extract_text(path, parser_kind, encoding)


def _parse_xlsx(path: Path, header_row: int) -> ParsedCsv:
    """Read one worksheet without allowing workbook formulas or external links."""

    namespace = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    with ZipFile(path) as workbook:
        names = set(workbook.namelist())
        if "xl/worksheets/sheet1.xml" not in names:
            raise ValueError(f"XLSX source has no first worksheet: {path.name}")
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            root = ElementTree.fromstring(workbook.read("xl/sharedStrings.xml"))
            shared = [
                "".join(node.text or "" for node in item.iter(f"{{{namespace}}}t"))
                for item in root.findall(f"{{{namespace}}}si")
            ]
        worksheet = ElementTree.fromstring(workbook.read("xl/worksheets/sheet1.xml"))

    rows: dict[int, dict[str, str]] = {}
    for row in worksheet.findall(f".//{{{namespace}}}sheetData/{{{namespace}}}row"):
        row_number = int(row.attrib["r"])
        cells: dict[str, str] = {}
        for cell in row.findall(f"{{{namespace}}}c"):
            reference = cell.attrib.get("r", "")
            match = re.match(r"[A-Z]+", reference)
            if match is None:
                raise ValueError(f"XLSX source has an invalid cell reference: {reference}")
            value_node = cell.find(f"{{{namespace}}}v")
            value = "" if value_node is None else value_node.text or ""
            if cell.attrib.get("t") == "s" and value:
                index = int(value)
                if index >= len(shared):
                    raise ValueError(f"XLSX shared-string index is invalid in {reference}")
                value = shared[index]
            elif cell.attrib.get("t") == "inlineStr":
                value = "".join(node.text or "" for node in cell.iter(f"{{{namespace}}}t"))
            cells[match.group(0)] = value.strip()
        rows[row_number] = cells

    header_cells = rows.get(header_row)
    if header_cells is None:
        raise ValueError(f"XLSX source is missing configured header row {header_row}: {path.name}")
    headers = [value for _, value in sorted(header_cells.items(), key=lambda item: _column_number(item[0])) if value]
    if len(headers) < 2 or len(set(headers)) != len(headers):
        raise ValueError(f"XLSX source has missing or duplicate headers: {path.name}")
    columns = {column: value for column, value in header_cells.items() if value}
    parsed_rows = tuple(
        {header: cells.get(column, "") for column, header in columns.items()}
        for number, cells in sorted(rows.items())
        if number > header_row and any(cells.get(column, "") for column in columns)
    )
    return ParsedCsv(tuple(headers), parsed_rows)


def _column_number(label: str) -> int:
    value = 0
    for character in label:
        value = value * 26 + ord(character) - ord("A") + 1
    return value


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


def _verify_absence_checks(spec: AcquisitionSpec, parsed_by_source: dict[str, str | ParsedCsv]) -> None:
    for fact in spec.facts:
        for check in fact.absence_checks:
            parsed = parsed_by_source[check.source_id]
            if not isinstance(parsed, ParsedCsv):
                raise ValueError(f"absence check requires a tabular source for fact {fact.fact_id}")
            missing = set(check.table_match) - set(parsed.fieldnames)
            if missing:
                raise ValueError(f"absence check source fields drifted for fact {fact.fact_id}: {sorted(missing)}")
            matches = [
                row
                for row in parsed.rows
                if all((row.get(key) or "").strip() == value for key, value in check.table_match.items())
            ]
            if matches:
                raise ValueError(f"unavailable_public fact {fact.fact_id} now has a matching public row")


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
    "acquire",
    "build_bundle_input",
    "load_frozen",
    "load_spec",
    "verify_frozen_bytes",
    "write_bundle_input",
    "write_frozen_acquisition",
]
