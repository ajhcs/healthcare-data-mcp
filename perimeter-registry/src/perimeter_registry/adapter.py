from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

_EIN_PATTERN = re.compile(r"^\d{2}-\d{7}$")


@dataclass(frozen=True)
class Form990Query:
    """Future request to 990 Evidence after this registry selects one filer."""

    ein: str
    tax_period_year: int

    def __post_init__(self) -> None:
        if not _EIN_PATTERN.fullmatch(self.ein):
            raise ValueError("ein must use NN-NNNNNNN format")
        if not 1900 <= self.tax_period_year <= 2100:
            raise ValueError("tax_period_year is outside the supported contract range")


@dataclass(frozen=True)
class Form990Fact:
    name: str
    value: str | Decimal
    unit: str | None
    source_url: str
    locator: str


@dataclass(frozen=True)
class Form990Evidence:
    ein: str
    tax_period_year: int
    facts: tuple[Form990Fact, ...]
    filing_source_url: str


class Form990EvidenceAdapter(Protocol):
    """Narrow future boundary; this package ships no implementation or network call."""

    def fetch(self, query: Form990Query) -> Form990Evidence:
        """Return official-XML facts for exactly one EIN and tax-period year."""
        ...
