"""
Fuzzy column recognition for non-standard hospital CSV files.

When hospital CSV files don't use exact CMS column names, the importer
fails to map columns. This module provides fuzzy matching via keyword
heuristics and difflib similarity, plus data normalization utilities.
"""

import re
from difflib import SequenceMatcher
from typing import Optional


# ---------------------------------------------------------------------------
# Keyword heuristic tables
# ---------------------------------------------------------------------------

# Columns that should be skipped (metadata, not charge data)
_SKIP_PATTERNS: list[tuple[re.Pattern, None]] = [
    (re.compile(r"\b(hospital|facility)[_ ]?(name)?\b", re.IGNORECASE), None),
]

# Maps a compiled regex to the internal column name it should resolve to.
# Order matters: more specific patterns are checked first.
_KEYWORD_RULES: list[tuple[re.Pattern, str]] = [
    # Description / procedure
    (
        re.compile(
            r"\b(description|procedure|service_description|item_description"
            r"|service)\b",
            re.IGNORECASE,
        ),
        "description",
    ),
    # Code type (must precede generic code rule)
    (
        re.compile(r"\b(code[_ ]?type|type[_ ]?of[_ ]?code)\b", re.IGNORECASE),
        "code_1_type",
    ),
    # Code
    (
        re.compile(
            r"\b(cpt[_ ]?code|hcpcs[_ ]?code|procedure[_ ]?code|cpt|hcpcs|code)\b",
            re.IGNORECASE,
        ),
        "code_1",
    ),
    # Payer
    (
        re.compile(
            r"\b(payer|payer[_ ]?name|insurer|insurance|insurance[_ ]?company"
            r"|carrier)\b",
            re.IGNORECASE,
        ),
        "payer_name",
    ),
    # Plan
    (
        re.compile(r"\b(plan|plan[_ ]?name|benefit[_ ]?plan)\b", re.IGNORECASE),
        "plan_name",
    ),
    # Setting
    (
        re.compile(
            r"\b(setting|care[_ ]?setting|place[_ ]?of[_ ]?service)\b",
            re.IGNORECASE,
        ),
        "setting",
    ),
    # Billing class
    (
        re.compile(r"\b(billing[_ ]?class|billing[_ ]?code)\b", re.IGNORECASE),
        "billing_class",
    ),
]

# Charge-type rules require two-keyword matching to disambiguate subtypes.
_CHARGE_RULES: list[tuple[re.Pattern, re.Pattern, str]] = [
    # Gross charge
    (
        re.compile(r"\bgross\b", re.IGNORECASE),
        re.compile(r"\b(charge|price)\b", re.IGNORECASE),
        "gross_charge",
    ),
    # Cash / discounted cash
    (
        re.compile(r"\bcash\b", re.IGNORECASE),
        re.compile(r"\b(price|charge|discount)\b", re.IGNORECASE),
        "discounted_cash",
    ),
    # Negotiated dollar
    (
        re.compile(r"\bnegotiated\b", re.IGNORECASE),
        re.compile(r"\b(dollar|rate|amount|price)\b", re.IGNORECASE),
        "negotiated_dollar",
    ),
]

# Payer abbreviation expansions (applied during normalization).
# Each entry is (compiled regex, replacement string).
_PAYER_ABBREVIATIONS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bBCBS\b", re.IGNORECASE), "Blue Cross Blue Shield"),
    (re.compile(r"\bUHC\b", re.IGNORECASE), "UnitedHealthcare"),
    (re.compile(r"\bCIGNA\b", re.IGNORECASE), "Cigna"),
    # Common full-name variants that should collapse to the canonical form
    (
        re.compile(r"\bUnited\s+Health\s*care\b", re.IGNORECASE),
        "UnitedHealthcare",
    ),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fuzzy_match_columns(
    headers: list[str], known_mapping: dict[str, str]
) -> dict[str, str]:
    """Return a mapping from raw CSV header -> internal column name.

    Algorithm
    ---------
    1. Exact match against *known_mapping* keys (case-insensitive, stripped).
    2. Keyword heuristic patterns for common column naming variants.
    3. ``difflib.SequenceMatcher`` with ratio > 0.7 against known_mapping keys.

    Unmapped columns are excluded from the returned dict.
    """
    result: dict[str, str] = {}
    used_targets: set[str] = set()  # internal names already claimed

    # Build a case-insensitive lookup for pass 1
    lower_mapping: dict[str, tuple[str, str]] = {
        k.strip().lower(): (k, v) for k, v in known_mapping.items()
    }

    remaining: list[str] = []

    # ------------------------------------------------------------------
    # Pass 1 – exact match (case-insensitive, whitespace-stripped)
    # ------------------------------------------------------------------
    for header in headers:
        key = header.strip().lower()
        if key in lower_mapping:
            _, target = lower_mapping[key]
            if target not in used_targets:
                result[header] = target
                used_targets.add(target)
        else:
            remaining.append(header)

    # ------------------------------------------------------------------
    # Pass 2 – keyword heuristic matching
    # ------------------------------------------------------------------
    still_remaining: list[str] = []
    for header in remaining:
        matched = _keyword_match(header, used_targets)
        if matched is not None:
            result[header] = matched
            used_targets.add(matched)
        else:
            still_remaining.append(header)

    # ------------------------------------------------------------------
    # Pass 3 – difflib fuzzy matching (ratio > 0.7)
    # ------------------------------------------------------------------
    for header in still_remaining:
        best_target: Optional[str] = None
        best_ratio: float = 0.0
        stripped = header.strip().lower()
        for known_key, target in known_mapping.items():
            if target in used_targets:
                continue
            ratio = SequenceMatcher(None, stripped, known_key.lower()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_target = target
        if best_ratio > 0.7 and best_target is not None:
            result[header] = best_target
            used_targets.add(best_target)

    return result


def normalize_payer_name(name: str) -> str:
    """Normalize a single payer name.

    * Strip whitespace
    * Expand common abbreviations (BCBS, UHC, CIGNA)
    * Title case
    * Collapse multiple spaces
    * Remove trailing punctuation (periods, commas)
    """
    text = name.strip()
    if not text:
        return text

    # Expand abbreviations and normalize known variant forms
    for pattern, expansion in _PAYER_ABBREVIATIONS:
        text = pattern.sub(expansion, text)

    # Title case
    text = text.title()

    # Collapse multiple spaces
    text = re.sub(r"\s{2,}", " ", text)

    # Remove trailing punctuation
    text = text.rstrip(".,;:")

    return text


def deduplicate_payer_names(names: list[str]) -> list[str]:
    """Normalize each name, deduplicate, and return sorted unique list."""
    seen: set[str] = set()
    unique: list[str] = []
    for raw in names:
        normalized = normalize_payer_name(raw)
        if not normalized:
            continue
        key = normalized.lower()
        if key not in seen:
            seen.add(key)
            unique.append(normalized)
    return sorted(unique)


def format_icd10_code(code: str) -> str:
    """Format an ICD-10 code with proper decimal placement.

    * Strip whitespace, uppercase.
    * If no decimal and length > 3, insert '.' after 3rd character.
    * If already has decimal, return as-is (uppercased/stripped).
    """
    code = code.strip().upper()
    if "." in code:
        return code
    if len(code) > 3:
        return code[:3] + "." + code[3:]
    return code


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _keyword_match(header: str, used_targets: set[str]) -> Optional[str]:
    """Apply keyword heuristics to *header*.  Return internal name or None."""

    # Check skip patterns first
    for pattern, _ in _SKIP_PATTERNS:
        if pattern.search(header):
            return None

    # Check charge rules (two-keyword pairs)
    for primary, secondary, target in _CHARGE_RULES:
        if target not in used_targets and primary.search(header) and secondary.search(header):
            return target

    # Generic single-keyword fallback for "charge", "price", "rate", "amount"
    if re.search(r"\b(charge|price|rate|amount)\b", header, re.IGNORECASE):
        if "negotiated_dollar" not in used_targets:
            return "negotiated_dollar"

    # Check standard keyword rules
    for pattern, target in _KEYWORD_RULES:
        if target not in used_targets and pattern.search(header):
            return target

    return None
