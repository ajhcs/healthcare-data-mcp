"""Explicit exception groups for recoverable data and integration failures."""

from __future__ import annotations

import duckdb
import httpx

EXPECTED_OPERATIONAL_EXCEPTIONS = (
    ArithmeticError,
    AttributeError,
    EOFError,
    ImportError,
    LookupError,
    OSError,
    RuntimeError,
    SyntaxError,
    TypeError,
    ValueError,
    duckdb.Error,
    httpx.HTTPError,
)
"""Failures callers may safely translate into bounded error or fallback results."""
