"""Conservative organization matching helpers for research activity profiles."""

from __future__ import annotations

import re
from collections.abc import Iterable

from rapidfuzz import fuzz

from .models import OrganizationMatchDecision

_LEGAL_SUFFIXES = {
    "THE",
    "INC",
    "INCORPORATED",
    "LLC",
    "LTD",
    "LIMITED",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
}
_TYPE_TOKENS = {
    "UNIVERSITY",
    "HOSPITAL",
    "HEALTH",
    "CLINIC",
    "FOUNDATION",
    "INSTITUTE",
    "CENTER",
    "CENTRE",
    "SYSTEM",
    "SCHOOL",
    "COLLEGE",
    "MEDICAL",
}


def normalize_org_name(name: str) -> str:
    """Normalize an organization name without erasing important entity-type words."""
    cleaned = re.sub(r"[^A-Z0-9& ]+", " ", name.upper().replace("&", " AND "))
    tokens = [token for token in cleaned.split() if token not in _LEGAL_SUFFIXES]
    return " ".join(tokens)


def _type_signature(name: str) -> set[str]:
    return {token for token in normalize_org_name(name).split() if token in _TYPE_TOKENS}


def _distinct_names(names: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for name in names:
        norm = normalize_org_name(name)
        if norm and norm not in seen:
            seen.add(norm)
            result.append(name)
    return result


def decide_organization_match(
    *,
    query_name: str = "",
    query_uei: str = "",
    nih_candidates: Iterable[tuple[str, str]] = (),
    trial_candidates: Iterable[str] = (),
) -> OrganizationMatchDecision:
    """Return a conservative cross-source organization matching decision.

    UEI exact matches are accepted. Name-only matching requires one unambiguous
    canonical organization name. Similar-but-distinct entity types, such as a
    university and health system with the same brand root, are marked ambiguous.
    """
    normalized_query = normalize_org_name(query_name)
    normalized_uei = query_uei.strip().upper()

    uei_matches = [(name, uei.strip().upper()) for name, uei in nih_candidates if normalized_uei and uei.strip().upper() == normalized_uei]
    if uei_matches:
        name, uei = uei_matches[0]
        return OrganizationMatchDecision(
            status="matched",
            query_name=query_name,
            query_uei=normalized_uei,
            matched_name=name,
            matched_uei=uei,
            confidence="identifier",
            rationale="Matched by exact NIH organization UEI.",
        )

    candidate_names = _distinct_names([name for name, _uei in nih_candidates] + list(trial_candidates))
    if not normalized_query:
        return OrganizationMatchDecision(
            status="unmatched",
            query_name=query_name,
            query_uei=normalized_uei,
            rationale="No organization name or UEI was supplied for cross-source matching.",
        )

    exact = [name for name in candidate_names if normalize_org_name(name) == normalized_query]
    if len(exact) == 1:
        return OrganizationMatchDecision(
            status="matched",
            query_name=query_name,
            query_uei=normalized_uei,
            matched_name=exact[0],
            confidence="exact_name",
            rationale="Matched by exact normalized organization name.",
        )
    if len(exact) > 1:
        return OrganizationMatchDecision(
            status="ambiguous",
            query_name=query_name,
            query_uei=normalized_uei,
            confidence="none",
            rationale="Multiple source records have the same normalized organization name.",
            ambiguous_candidates=exact,
        )

    scored: list[tuple[str, int]] = []
    for name in candidate_names:
        score = fuzz.token_set_ratio(normalized_query, normalize_org_name(name))
        if score >= 95:
            scored.append((name, score))

    if len(scored) == 1:
        name = scored[0][0]
        query_types = _type_signature(query_name)
        candidate_types = _type_signature(name)
        if query_types and candidate_types and query_types != candidate_types:
            return OrganizationMatchDecision(
                status="ambiguous",
                query_name=query_name,
                query_uei=normalized_uei,
                rationale="High name similarity but conflicting organization type terms.",
                ambiguous_candidates=[name],
            )
        return OrganizationMatchDecision(
            status="matched",
            query_name=query_name,
            query_uei=normalized_uei,
            matched_name=name,
            confidence="high_name_similarity",
            rationale="Only one high-similarity organization candidate was present.",
        )

    if scored:
        return OrganizationMatchDecision(
            status="ambiguous",
            query_name=query_name,
            query_uei=normalized_uei,
            rationale="Multiple high-similarity organization candidates were present.",
            ambiguous_candidates=[name for name, _score in sorted(scored, key=lambda item: item[1], reverse=True)],
        )

    return OrganizationMatchDecision(
        status="unmatched",
        query_name=query_name,
        query_uei=normalized_uei,
        rationale="No conservative organization match was found.",
    )

