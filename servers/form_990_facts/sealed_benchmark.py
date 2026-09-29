"""Deterministic scoring and telemetry helpers for the sealed model benchmark."""

from __future__ import annotations

from collections import Counter
import json
import re
from typing import Any


PLUGIN_SECTION = b'[plugins."form-990-facts@personal"]'
TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "cache_write_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
)
METRIC_KEYS = (
    "total_revenue",
    "total_expenses",
    "net_assets",
    "top_reported_executive",
)
PROVENANCE_KEYS = (
    "source_url",
    "object_id",
    "xml_sha256",
    "metric_key",
    "reported_label",
    "units",
    "xml_field_paths",
)


def replace_plugin_enabled(config: bytes, *, enabled: bool) -> bytes:
    """Toggle only the exact personal plugin section in Codex config bytes."""

    pattern = re.compile(rb'(\[plugins\."form-990-facts@personal"\]\r?\nenabled\s*=\s*)(true|false)')
    replacement = rb"\g<1>" + (b"true" if enabled else b"false")
    updated, count = pattern.subn(replacement, config)
    if count != 1:
        raise ValueError("Expected exactly one Form 990 plugin enablement section")
    return updated


def parse_json_answer(text: str) -> dict[str, Any]:
    """Parse a schema-constrained final response, tolerating one Markdown fence."""

    candidate = text.strip()
    if candidate.startswith("```") and candidate.endswith("```"):
        lines = candidate.splitlines()
        candidate = "\n".join(lines[1:-1]).strip()
    value = json.loads(candidate)
    if not isinstance(value, dict):
        raise ValueError("Benchmark response must be a JSON object")
    return value


def _canonical_bytes(value: Any) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def summarize_jsonl(jsonl: str) -> dict[str, Any]:
    """Summarize observable calls, result bytes, tokens, and final text."""

    calls: Counter[str] = Counter()
    seen_items: set[str] = set()
    tokens = {field: 0 for field in TOKEN_FIELDS}
    mcp_result_bytes = 0
    final_text = ""
    events = 0
    for line in jsonl.splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        events += 1
        if event.get("type") == "turn.completed":
            usage = event.get("usage") or {}
            for field in TOKEN_FIELDS:
                tokens[field] += int(usage.get(field) or 0)
        if event.get("type") != "item.completed":
            continue
        item = event.get("item") or {}
        item_id = str(item.get("id") or f"anonymous-{events}")
        item_type = str(item.get("type") or "")
        if item_type in {"mcp_tool_call", "web_search", "command_execution"}:
            if item_id not in seen_items:
                calls[item_type] += 1
                seen_items.add(item_id)
            if item_type == "mcp_tool_call" and "result" in item:
                mcp_result_bytes += _canonical_bytes(item["result"])
        if item_type == "agent_message":
            final_text = str(item.get("text") or "")
    return {
        "events": events,
        "calls": dict(sorted(calls.items())),
        "tokens": tokens,
        "mcp_result_bytes": mcp_result_bytes,
        "final_text": final_text,
        "final_response_bytes": len(final_text.encode("utf-8")),
        "jsonl_bytes": len(jsonl.encode("utf-8")),
    }


def _normalized_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").casefold())


def _fact_equal(field: str, actual: Any, expected: Any) -> bool:
    if field in {"legal_filer", "executive_name", "executive_title"}:
        return _normalized_text(actual) == _normalized_text(expected)
    return actual == expected


def score_answer(answer: dict[str, Any], gold: dict[str, Any]) -> dict[str, Any]:
    """Score facts, provenance, coverage, and scope honesty separately."""

    answer_executive = answer.get("top_reported_executive") or {}
    gold_executive = gold.get("top_reported_executive") or {}
    fact_pairs = {
        "legal_filer": (answer.get("legal_filer"), gold.get("legal_filer")),
        "ein": (answer.get("ein"), gold.get("ein")),
        "tax_period_end": (answer.get("tax_period_end"), gold.get("tax_period_end")),
        "form_type": (answer.get("form_type"), gold.get("form_type")),
        "total_revenue": (answer.get("total_revenue"), gold.get("total_revenue")),
        "total_expenses": (answer.get("total_expenses"), gold.get("total_expenses")),
        "net_assets": (answer.get("net_assets"), gold.get("net_assets")),
        "executive_name": (answer_executive.get("name"), gold_executive.get("name")),
        "executive_title": (answer_executive.get("title"), gold_executive.get("title")),
        "executive_compensation": (
            answer_executive.get("total_reported_compensation"),
            gold_executive.get("total_reported_compensation"),
        ),
    }
    fact_checks = {field: _fact_equal(field, actual, expected) for field, (actual, expected) in fact_pairs.items()}

    answer_provenance = answer.get("provenance") or {}
    gold_provenance = gold.get("provenance") or {}
    provenance_checks: dict[str, bool] = {}
    for metric_key in METRIC_KEYS:
        actual = answer_provenance.get(metric_key)
        expected = gold_provenance.get(metric_key)
        for field in PROVENANCE_KEYS:
            check_key = f"{metric_key}.{field}"
            if actual is None or expected is None:
                provenance_checks[check_key] = actual is None and expected is None
            elif field == "xml_field_paths":
                provenance_checks[check_key] = sorted(actual.get(field) or []) == sorted(expected.get(field) or [])
            else:
                provenance_checks[check_key] = actual.get(field) == expected.get(field)

    coverage_checks = {
        metric_key: metric_key in answer and metric_key in (answer.get("provenance") or {})
        for metric_key in METRIC_KEYS
    }
    caveat = str(answer.get("scope_caveat") or "").casefold()
    scope_honest = "exact" in caveat and any(
        boundary in caveat for boundary in ("brand", "affiliate", "roll-up", "rollup")
    )
    return {
        "fact_correct": sum(fact_checks.values()),
        "fact_total": len(fact_checks),
        "fact_checks": fact_checks,
        "provenance_correct": sum(provenance_checks.values()),
        "provenance_total": len(provenance_checks),
        "provenance_checks": provenance_checks,
        "coverage_correct": sum(coverage_checks.values()),
        "coverage_total": len(coverage_checks),
        "coverage_checks": coverage_checks,
        "scope_honest": scope_honest,
    }
