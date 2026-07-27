"""Sealed scoring and clustered paired analysis."""

from __future__ import annotations

import math
import random
import re
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlsplit, urlunsplit

SCALE_MULTIPLIERS = {"ones": 1.0, "thousands": 1_000.0, "millions": 1_000_000.0, "billions": 1_000_000_000.0}


def _norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def _url(value: object) -> str:
    parsed = urlsplit(str(value))
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", ""))


def _tolerance(gold: dict[str, Any]) -> tuple[float, float]:
    raw = gold.get("tolerance") or {}
    if isinstance(raw, (int, float)):
        return float(raw), float(raw)
    return float(raw.get("relative", raw.get("relative_fraction", 0))), float(raw.get("absolute_usd", 0))


def _similarity(left: object, right: object) -> float:
    normalized_left, normalized_right = _norm(left), _norm(right)
    left_tokens, right_tokens = set(normalized_left.split()), set(normalized_right.split())
    jaccard = len(left_tokens & right_tokens) / len(left_tokens | right_tokens) if left_tokens and right_tokens else 0
    return max(jaccard, SequenceMatcher(None, normalized_left, normalized_right).ratio())


def _period(value: object) -> object:
    """Ignore schema-required null placeholders while preserving period meaning."""
    if not isinstance(value, dict):
        return value
    return {key: item for key, item in value.items() if item is not None}


def _units(value: object) -> tuple[str, str] | None:
    if not isinstance(value, dict):
        return None
    currency = str(value.get("currency", "")).upper()
    scale = str(value.get("scale", value.get("normalized_scale", "ones"))).lower()
    if currency != "USD" or scale not in SCALE_MULTIPLIERS:
        return None
    return currency, scale


def _base_amount(value: object, units: object) -> float | None:
    parsed = _units(units)
    if value is None or parsed is None:
        return None
    try:
        return float(value) * SCALE_MULTIPLIERS[parsed[1]]
    except (TypeError, ValueError):
        return None


def _perimeter_correct(observed: object, expected: object) -> bool:
    if not isinstance(expected, dict):
        return _norm(observed) == _norm(expected)
    text = _norm(observed)
    if expected.get("source_defined_consolidation") and "consolidat" not in text:
        return False
    candidates = [expected.get("description", ""), *expected.get("includes", [])]
    return any(_similarity(text, candidate) >= 0.45 for candidate in candidates)


def _caveat_coverage(observed: list[str], required: list[object]) -> float:
    if not required:
        return 1.0
    text = " ".join(observed)
    matched = 0
    for item in required:
        requirement = item.get("text", "") if isinstance(item, dict) else str(item)
        matched += _similarity(text, requirement) >= 0.32
    return matched / len(required)


def score_answer(answer: dict[str, Any], gold: dict[str, Any]) -> dict[str, Any]:
    expected_status = "reported" if gold.get("requested_metric_available", True) else "unavailable_not_reported"
    status_correct = answer.get("answer_status", "reported") == expected_status
    expected, observed = gold.get("validated_value"), answer.get("value")
    expected_base = _base_amount(expected, gold.get("units"))
    observed_base = _base_amount(observed, answer.get("units"))
    relative, absolute = _tolerance(gold)
    financial = expected is None and observed is None
    if expected_base is not None and observed_base is not None:
        financial = math.isclose(observed_base, expected_base, rel_tol=relative, abs_tol=absolute)
    fallback = gold.get("reported_fallback") or gold.get("closest_reported_subtotal") or {}
    if expected_status == "unavailable_not_reported" and fallback:
        observed_fallback = answer.get("closest_reported_subtotal") or {}
        _, fallback_absolute = _tolerance(fallback)
        fallback_value = _base_amount(
            fallback.get("validated_value", fallback.get("value_usd")),
            fallback.get("units", gold.get("units")),
        )
        observed_fallback_value = _base_amount(observed_fallback.get("value"), observed_fallback.get("units"))
        fallback_label = fallback.get("metric", fallback.get("label"))
        financial = financial and fallback_value is not None and observed_fallback_value is not None
        if financial:
            financial = math.isclose(observed_fallback_value, fallback_value, rel_tol=0, abs_tol=fallback_absolute)
        financial = financial and _norm(observed_fallback.get("label")) == _norm(fallback_label)
    entity = _perimeter_correct(answer.get("reporting_perimeter", ""), gold.get("entity_perimeter"))
    period = _period(answer.get("period")) == _period(gold.get("period"))
    units = _units(answer.get("units")) is not None and _units(gold.get("units")) is not None
    source = gold.get("primary_source") or {}
    allowed_sources = (
        [source.get("url"), *source.get("accepted_equivalent_urls", [])] if isinstance(source, dict) else [source]
    )
    source_correct = _url(answer.get("primary_source_url")) in {_url(item) for item in allowed_sources if item}
    locator_quality = _similarity(answer.get("exact_locator", ""), gold.get("exact_locator", ""))
    caveat_quality = _caveat_coverage(answer.get("caveats", []), gold.get("required_caveats", []))
    false_aggregation = bool(answer.get("aggregated_entities")) and not bool(gold.get("aggregation_permitted"))
    fully_correct = financial and status_correct and entity and period and units and not false_aggregation
    return {
        "financial_correct": financial and status_correct and period and units,
        "entity_perimeter_correct": entity,
        "provenance_quality": (float(source_correct) + locator_quality) / 2,
        "caveat_quality": caveat_quality,
        "false_aggregation": false_aggregation,
        "fully_correct": fully_correct,
    }


def paired_cluster_bootstrap(
    rows: list[dict[str, Any]], metric: str, arm_a: str, arm_b: str, *, seed: int = 20260726, draws: int = 10000
) -> dict[str, float]:
    clusters: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        clusters[str(row["system_id"])][str(row["arm_id"])].append(float(row[metric]))
    differences = [
        sum(arms[arm_a]) / len(arms[arm_a]) - sum(arms[arm_b]) / len(arms[arm_b])
        for arms in clusters.values()
        if arms[arm_a] and arms[arm_b]
    ]
    if not differences:
        raise ValueError("no paired system clusters")
    rng = random.Random(seed)
    samples = sorted(sum(rng.choice(differences) for _ in differences) / len(differences) for _ in range(draws))
    return {
        "paired_difference": sum(differences) / len(differences),
        "ci95_low": samples[int(draws * 0.025)],
        "ci95_high": samples[int(draws * 0.975)],
        "system_clusters": len(differences),
    }
