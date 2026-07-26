"""Sealed scoring and clustered paired analysis."""

from __future__ import annotations

import math
import random
import re
from collections import defaultdict
from typing import Any
from urllib.parse import urlsplit, urlunsplit


def _norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def _url(value: object) -> str:
    parsed = urlsplit(str(value))
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", ""))


def _tolerance(gold: dict[str, Any]) -> tuple[float, float]:
    raw = gold.get("tolerance") or {}
    if isinstance(raw, (int, float)):
        return float(raw), float(raw)
    return float(raw.get("relative_fraction", 0)), float(raw.get("absolute_usd", 0))


def _concept_coverage(texts: list[str], concepts: list[list[str]]) -> float:
    if not concepts:
        return 1.0
    haystack = _norm(" ".join(texts))
    matched = sum(any(_norm(term) in haystack for term in alternatives) for alternatives in concepts)
    return matched / len(concepts)


def score_answer(answer: dict[str, Any], gold: dict[str, Any]) -> dict[str, Any]:
    expected_status = gold.get("answer_status", "reported")
    status_correct = answer.get("answer_status", "reported") == expected_status
    expected = gold.get("validated_value")
    observed = answer.get("value")
    relative, absolute = _tolerance(gold)
    financial = expected is None and observed is None
    if expected is not None and observed is not None:
        financial = math.isclose(float(observed), float(expected), rel_tol=relative, abs_tol=absolute)
    closest = gold.get("closest_reported_subtotal") or {}
    if expected_status == "unavailable_not_reported" and closest:
        observed_closest = answer.get("closest_reported_subtotal") or {}
        _, closest_abs = _tolerance(closest)
        financial = (
            financial
            and math.isclose(
                float(observed_closest.get("value", math.nan)),
                float(closest["value_usd"]),
                rel_tol=0,
                abs_tol=closest_abs,
            )
            and _norm(observed_closest.get("label")) == _norm(closest["label"])
        )
    rules = gold.get("perimeter_rules") or {}
    perimeter_text = _norm(answer.get("reporting_perimeter", ""))
    required_all = [_norm(term) for term in rules.get("required_all", [])]
    required_any = [_norm(term) for term in rules.get("required_any", [])]
    forbidden = [_norm(term) for term in rules.get("forbidden", [])]
    entity = all(term in perimeter_text for term in required_all)
    entity = entity and (not required_any or any(term in perimeter_text for term in required_any))
    entity = entity and not any(term in perimeter_text for term in forbidden)
    if not rules:
        entity = _norm(answer.get("reporting_perimeter")) == _norm(gold.get("entity_perimeter"))
    period = answer.get("period") == gold.get("period")
    units = answer.get("units") == gold.get("units")
    allowed_sources = gold.get("accepted_source_urls") or [
        gold.get("primary_source", {}).get("url")
        if isinstance(gold.get("primary_source"), dict)
        else gold.get("primary_source")
    ]
    source = _url(answer.get("primary_source_url")) in {_url(item) for item in allowed_sources if item}
    locator_quality = _concept_coverage([str(answer.get("exact_locator", ""))], gold.get("locator_concepts", []))
    caveat_quality = _concept_coverage(answer.get("caveats", []), gold.get("caveat_concepts", []))
    if not gold.get("caveat_concepts"):
        caveat_quality = _concept_coverage(
            answer.get("caveats", []), [[item] for item in gold.get("required_caveats", [])]
        )
    expected_consolidation = bool(gold.get("consolidation_expected"))
    false_aggregation = bool(answer.get("aggregated_entities")) and not expected_consolidation
    return {
        "financial_correct": financial and status_correct and period and units,
        "entity_perimeter_correct": entity,
        "provenance_quality": (float(source) + locator_quality) / 2,
        "caveat_quality": caveat_quality,
        "false_aggregation": false_aggregation,
        "fully_correct": financial and status_correct and entity and period and units and not false_aggregation,
    }


def paired_cluster_bootstrap(
    rows: list[dict[str, Any]], metric: str, arm_a: str, arm_b: str, *, seed: int = 20260726, draws: int = 10000
) -> dict[str, float]:
    clusters: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        clusters[str(row["system_id"])][str(row["arm_id"])].append(float(row[metric]))
    differences = []
    for arms in clusters.values():
        if arms[arm_a] and arms[arm_b]:
            differences.append(sum(arms[arm_a]) / len(arms[arm_a]) - sum(arms[arm_b]) / len(arms[arm_b]))
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
