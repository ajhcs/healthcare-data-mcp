"""Neutral identifiers for the frozen six-system Scale roster."""

from types import MappingProxyType
from typing import NamedTuple


class AhrqSystemIdentity(NamedTuple):
    """Exact source-local AHRQ identity bound to one product system."""

    health_sys_id: str
    source_name: str

SYSTEM_SLUGS = (
    "christianacare",
    "jefferson-health",
    "temple-health",
    "penn-medicine",
    "cooper-university-health-care",
    "main-line-health",
)

SYSTEM_NAMES = {
    "christianacare": "ChristianaCare",
    "jefferson-health": "Jefferson Health",
    "temple-health": "Temple Health",
    "penn-medicine": "Penn Medicine",
    "cooper-university-health-care": "Cooper University Health Care",
    "main-line-health": "Main Line Health",
}

SYSTEM_AHRQ_IDENTITIES = MappingProxyType(
    {
        "christianacare": AhrqSystemIdentity("HSI00000218", "ChristianaCare"),
        "jefferson-health": AhrqSystemIdentity("HSI00000048", "Jefferson Health"),
        "temple-health": AhrqSystemIdentity("HSI00001065", "Temple University Health System"),
        "penn-medicine": AhrqSystemIdentity(
            "HSI00000820",
            "University of Pennsylvania Health System",
        ),
        "cooper-university-health-care": AhrqSystemIdentity(
            "HSI00001079",
            "Cooper University Health Care",
        ),
        "main-line-health": AhrqSystemIdentity("HSI00000608", "Main Line Health"),
    }
)

__all__ = [
    "AhrqSystemIdentity",
    "SYSTEM_AHRQ_IDENTITIES",
    "SYSTEM_NAMES",
    "SYSTEM_SLUGS",
]
