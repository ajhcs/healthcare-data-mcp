"""Agent-facing input mistake detection helpers for MCP tools."""

from __future__ import annotations

from dataclasses import dataclass
from difflib import get_close_matches
import re
from typing import Any, Iterable


PLACEHOLDER_RE = re.compile(
    r"(^<[^>]+>$)|(\{[^}]+\})|(^YOUR_[A-Z0-9_]+$)|(^[A-Z0-9_]*PLACEHOLDER[A-Z0-9_]*$)|(^\$[A-Z0-9_]+$)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class Mistake:
    """Detected caller mistake with a stable error type and recovery guidance."""

    error_type: str
    message: str
    fix_hint: str
    data: dict[str, Any]


def detect_placeholder(value: Any, *, parameter: str = "value") -> Mistake | None:
    """Return a placeholder mistake when an agent copied a template token."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if PLACEHOLDER_RE.search(cleaned):
        return Mistake(
            "PLACEHOLDER_INPUT",
            f"{parameter} looks like a template placeholder, not a real value.",
            f"Replace {cleaned!r} with a real public identifier or omit the optional argument.",
            {"parameter": parameter, "value": cleaned},
        )
    return None


def detect_name_used_for_exact_id(value: Any, *, parameter: str, expected: str) -> Mistake | None:
    """Detect likely organization/person names supplied to exact identifier fields."""

    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if any(character.isalpha() for character in cleaned) and " " in cleaned:
        return Mistake(
            "NAME_USED_FOR_EXACT_ID",
            f"{parameter} expects {expected}, but the value looks like a name.",
            "Use the matching search tool first, then call the exact lookup with the returned identifier.",
            {"parameter": parameter, "value": cleaned, "expected": expected},
        )
    return None


def fuzzy_options(value: str, options: Iterable[str], *, limit: int = 5) -> list[str]:
    """Return close option suggestions for typo recovery."""

    cleaned = value.strip().lower()
    option_list = sorted(str(option) for option in options)
    lower_map = {option.lower(): option for option in option_list}
    matches = get_close_matches(cleaned, list(lower_map), n=limit, cutoff=0.55)
    return [lower_map[match] for match in matches]

