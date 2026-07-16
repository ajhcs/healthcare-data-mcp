"""Offline parsers and row extractors for frozen Scale roster/bed artifacts."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree
from zipfile import ZipFile

from bs4 import BeautifulSoup
from pydantic import JsonValue
from pypdf import PdfReader

from shared.acquisition.scale_roster_bed_models import AcquisitionSpec, FactSpec


@dataclass(frozen=True)
class ParsedCsv:
    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


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


__all__ = [
    "ParsedCsv",
    "_extract_fact",
    "_extract_from_parsed",
    "_parse_source",
    "_verify_absence_checks",
]
