"""
Query limit policy engine for bounded, deterministic data access.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryPolicyError(Exception):
    """Policy or validation error raised by query services."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def to_dict(self) -> dict[str, Any]:
        """Return machine-readable error details."""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class QueryLimits:
    """Configurable limits for row and aggregate queries."""

    default_page_size: int = 100
    max_page_size: int = 1000
    max_page_number: int = 10000
    max_rows_per_call: int = 1000
    max_group_by_fields: int = 4
    max_group_keys: int = 500
    max_query_seconds: float = 5.0


class QueryLimitEngine:
    """Enforces query bounds before and after execution."""

    def __init__(self, limits: QueryLimits | None = None):
        self.limits = limits or QueryLimits()

    def normalize_paging(
        self, page: int | None = None, page_size: int | None = None
    ) -> tuple[int, int]:
        """Validate and normalize paging parameters."""
        if page is None:
            page = 1
        if page_size is None:
            page_size = self.limits.default_page_size

        if page < 1:
            raise QueryPolicyError(
                code="INVALID_ARGUMENT",
                message="Page must be >= 1",
                details={"page": page},
            )

        if page > self.limits.max_page_number:
            raise QueryPolicyError(
                code="POLICY_LIMIT_EXCEEDED",
                message="Page exceeds max_page_number",
                details={
                    "page": page,
                    "max_page_number": self.limits.max_page_number,
                },
            )

        if page_size < 1:
            raise QueryPolicyError(
                code="INVALID_ARGUMENT",
                message="Page size must be >= 1",
                details={"page_size": page_size},
            )

        if page_size > self.limits.max_page_size:
            raise QueryPolicyError(
                code="POLICY_LIMIT_EXCEEDED",
                message="Page size exceeds max_page_size",
                details={
                    "page_size": page_size,
                    "max_page_size": self.limits.max_page_size,
                },
            )

        if page_size > self.limits.max_rows_per_call:
            page_size = self.limits.max_rows_per_call

        return page, page_size

    def ensure_page_in_bounds(
        self, total_count: int, page: int, page_size: int
    ) -> None:
        """Validate that requested page is in range for total_count."""
        if total_count < 0:
            raise QueryPolicyError(
                code="INTERNAL_ERROR",
                message="Total count cannot be negative",
                details={"total_count": total_count},
            )

        if total_count == 0:
            return

        offset = (page - 1) * page_size
        if offset >= total_count:
            total_pages = ((total_count - 1) // page_size) + 1
            raise QueryPolicyError(
                code="INVALID_ARGUMENT",
                message="Requested page is out of bounds",
                details={
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages,
                },
            )

    def validate_group_by(self, group_by: list[str]) -> None:
        """Validate grouping request size."""
        if len(group_by) > self.limits.max_group_by_fields:
            raise QueryPolicyError(
                code="POLICY_LIMIT_EXCEEDED",
                message="Too many group_by fields",
                details={
                    "group_by_fields": len(group_by),
                    "max_group_by_fields": self.limits.max_group_by_fields,
                },
            )

    def validate_group_count(self, group_count: int) -> None:
        """Validate aggregate output cardinality."""
        if group_count > self.limits.max_group_keys:
            raise QueryPolicyError(
                code="POLICY_LIMIT_EXCEEDED",
                message="Grouped result exceeds max_group_keys",
                details={
                    "group_count": group_count,
                    "max_group_keys": self.limits.max_group_keys,
                },
            )

    def validate_runtime(self, elapsed_seconds: float) -> None:
        """Validate runtime budget."""
        if elapsed_seconds > self.limits.max_query_seconds:
            raise QueryPolicyError(
                code="QUERY_TIMEOUT",
                message="Query exceeded max_query_seconds",
                details={
                    "elapsed_seconds": elapsed_seconds,
                    "max_query_seconds": self.limits.max_query_seconds,
                },
            )

