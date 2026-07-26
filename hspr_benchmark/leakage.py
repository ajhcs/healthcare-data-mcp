"""Fail-closed structural and sealed-key leakage audit for HSPR packets."""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

SCHEMA = {
    "$": {"schema_version", "as_of", "systems"},
    "system": {
        "system_id",
        "canonical_name",
        "aliases",
        "legal_entities",
        "relationships",
        "identifiers",
        "effective_from",
        "effective_to",
        "ambiguity_warnings",
        "identity_provenance",
    },
    "legal_entity": {"name", "entity_type"},
    "relationship": {"subject", "predicate", "object", "effective_from", "effective_to"},
    "identifier": {"type", "identifier", "entity", "effective_from", "effective_to"},
    "provenance": {"claim", "source_url", "accessed"},
}
FORBIDDEN_KEY = re.compile(
    r"(?i)(amount|financial|metric|measurement|answer|gold|score|revenue|income|assets|report|filing|locator|page|audit|tolerance|hint|trend)"
)
FORBIDDEN_TEXT = re.compile(
    r"(?i)(\$\s*\d|\b(?:revenue|operating income|total assets)\b\s*[:=]|audited financial statements|annual report|form 10-k|form 990)"
)


def _url(value: object) -> str:
    parsed = urlsplit(str(value))
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", ""))


def _schema_findings(packet: dict[str, Any]) -> list[str]:
    findings = []
    unknown = set(packet) - SCHEMA["$"]
    if unknown:
        findings.append(f"$: unknown keys {sorted(unknown)}")
    for i, system in enumerate(packet.get("systems", [])):
        unknown = set(system) - SCHEMA["system"]
        if unknown:
            findings.append(f"$.systems[{i}]: unknown keys {sorted(unknown)}")
        for field, kind in (
            ("legal_entities", "legal_entity"),
            ("relationships", "relationship"),
            ("identifiers", "identifier"),
            ("identity_provenance", "provenance"),
        ):
            for j, item in enumerate(system.get(field, [])):
                extra = set(item) - SCHEMA[kind]
                if extra:
                    findings.append(f"$.systems[{i}].{field}[{j}]: unknown keys {sorted(extra)}")
                for key in item:
                    if FORBIDDEN_KEY.search(key):
                        findings.append(f"$.systems[{i}].{field}[{j}].{key}: answer-like key")
        for text in _scalars(system):
            if isinstance(text, str) and FORBIDDEN_TEXT.search(text):
                findings.append(f"$.systems[{i}]: answer-like financial text")
    return findings


def _scalars(value: Any):
    if isinstance(value, dict):
        for child in value.values():
            yield from _scalars(child)
    elif isinstance(value, list):
        for child in value:
            yield from _scalars(child)
    elif value is not None:
        yield value


def _gold_records(gold: Any) -> list[dict[str, Any]]:
    if isinstance(gold, dict) and isinstance(gold.get("records"), list):
        return gold["records"]
    if isinstance(gold, list):
        return gold
    raise ValueError("sealed key must contain records")


def _key_findings(packet: dict[str, Any], gold: Any) -> list[str]:
    packet_text = json.dumps(packet, sort_keys=True).lower()
    packet_urls = {_url(v) for v in _scalars(packet) if isinstance(v, str) and v.startswith(("http://", "https://"))}
    findings = []
    for record in _gold_records(gold):
        values = [record.get("validated_value")]
        closest = record.get("reported_fallback") or record.get("closest_reported_subtotal") or {}
        values.append(closest.get("validated_value", closest.get("value_usd")))
        for value in values:
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                continue
            variants = {str(int(value))}
            for divisor in (1_000, 1_000_000, 1_000_000_000):
                scaled = value / divisor
                if scaled >= 1:
                    variants.add(f"{scaled:g}".lower())
                    variants.add(f"{scaled:.12f}".rstrip("0").rstrip("."))
                    if float(scaled).is_integer():
                        variants.add(f"{int(scaled):,}".lower())
            if any(re.search(rf"(?<!\d){re.escape(item)}(?!\d)", packet_text) for item in variants):
                findings.append(f"financial amount overlap for {record.get('question_id')}")
        source = record.get("primary_source")
        source_url = source.get("url") if isinstance(source, dict) else source
        if source_url and _url(source_url) in packet_urls:
            findings.append(f"financial source URL overlap for {record.get('question_id')}")
        locator = re.findall(r"[a-z0-9]+", str(record.get("exact_locator", "")).lower())
        distinctive = [" ".join(locator[i : i + 5]) for i in range(max(0, len(locator) - 4))]
        if any(phrase in re.sub(r"[^a-z0-9]+", " ", packet_text) for phrase in distinctive):
            findings.append(f"locator n-gram overlap for {record.get('question_id')}")
    return findings


def audit_registry_packet(packet_path: Path, sealed_gold_path: Path | None = None) -> dict[str, Any]:
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    findings = _schema_findings(packet)
    if sealed_gold_path is not None:
        findings.extend(_key_findings(packet, json.loads(sealed_gold_path.read_text(encoding="utf-8"))))
    return {
        "passed": not findings,
        "findings": sorted(set(findings)),
        "packet_sha256": hashlib.sha256(packet_path.read_bytes()).hexdigest(),
    }
