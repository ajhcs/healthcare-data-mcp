"""
Query service abstraction for row and aggregate workflows.
"""

from dataclasses import dataclass
from time import perf_counter
from typing import Any, Optional

from .query_limits import QueryLimitEngine, QueryPolicyError


@dataclass
class QueryPage:
    """Standard paged query result."""

    rows: list[dict[str, Any]]
    total_count: int
    page: int
    page_size: int


class QueryService:
    """Headless query service used by UI and MCP layers."""

    ALLOWED_GROUP_BY_FIELDS = {
        "hospital_id": "c.hospital_id",
        "hospital_name": "h.name",
        "payer_name": "p.name",
        "plan_name": "pl.name",
        "code_1": "c.code1",
        "code_1_type": "c.code1_type",
        "setting": "c.setting",
        "billing_class": "c.billing_class",
    }

    ALLOWED_METRICS = {
        "count": "COUNT(*) AS count",
        "min_negotiated_dollar": "MIN(c.negotiated_dollar) AS min_negotiated_dollar",
        "max_negotiated_dollar": "MAX(c.negotiated_dollar) AS max_negotiated_dollar",
        "avg_negotiated_dollar": "AVG(c.negotiated_dollar) AS avg_negotiated_dollar",
        "median_negotiated_dollar": "MEDIAN(c.negotiated_dollar) AS median_negotiated_dollar",
    }

    def __init__(self, db: Any, limit_engine: QueryLimitEngine | None = None):
        self.db = db
        self.limit_engine = limit_engine or QueryLimitEngine()

    def search_rows(
        self,
        query: str = "",
        hospital_ids: Optional[list[int]] = None,
        payer: str | None = None,
        setting: str | None = None,
        code_type: str | None = None,
        code: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        page: int = 1,
        page_size: int = 100,
    ) -> QueryPage:
        """Execute a bounded row-level search."""
        page, page_size = self.limit_engine.normalize_paging(page, page_size)

        started = perf_counter()
        result = self.db.search(
            query=query,
            hospital_ids=hospital_ids,
            payer=payer,
            setting=setting,
            code_type=code_type,
            code=code,
            min_price=min_price,
            max_price=max_price,
            page=page,
            page_size=page_size,
        )
        elapsed = perf_counter() - started
        self.limit_engine.validate_runtime(elapsed)
        self.limit_engine.ensure_page_in_bounds(result.total_count, page, page_size)

        rows = sorted(
            result.rows,
            key=lambda row: (
                str(row.get("description") or "").lower(),
                str(row.get("code_1") or ""),
                int(row.get("hospital_id") or 0),
                int(row.get("id") or 0),
            ),
        )
        return QueryPage(
            rows=rows,
            total_count=result.total_count,
            page=page,
            page_size=page_size,
        )

    def query_aggregates(
        self,
        query: str = "",
        hospital_ids: Optional[list[int]] = None,
        payer: str | None = None,
        setting: str | None = None,
        code_type: str | None = None,
        code: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
        group_by: Optional[list[str]] = None,
        metrics: Optional[list[str]] = None,
        page: int = 1,
        page_size: int = 100,
    ) -> QueryPage:
        """Execute grouped aggregate query with policy enforcement."""
        if not hasattr(self.db, "conn") or self.db.conn is None:
            raise QueryPolicyError(
                code="INTERNAL_ERROR",
                message="Database connection is not available",
            )

        page, page_size = self.limit_engine.normalize_paging(page, page_size)
        group_by = group_by or []
        metrics = metrics or ["count", "avg_negotiated_dollar"]

        self.limit_engine.validate_group_by(group_by)
        self._validate_group_by_fields(group_by)
        metric_sql = self._build_metric_sql(metrics)

        conditions, params = self._build_conditions(
            query=query,
            hospital_ids=hospital_ids,
            payer=payer,
            setting=setting,
            code_type=code_type,
            code=code,
            min_price=min_price,
            max_price=max_price,
        )
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        base_from = """
            FROM charges c
            JOIN hospitals h ON c.hospital_id = h.id
            LEFT JOIN descriptions d ON c.description_id = d.id
            LEFT JOIN payers p ON c.payer_id = p.id
            LEFT JOIN plans pl ON c.plan_id = pl.id
        """

        started = perf_counter()
        rows: list[dict[str, Any]]
        total_count: int

        if group_by:
            group_exprs = [self.ALLOWED_GROUP_BY_FIELDS[field] for field in group_by]
            group_select = ", ".join(
                [f"{expr} AS {name}" for name, expr in zip(group_by, group_exprs)]
            )
            group_clause = ", ".join(group_exprs)
            order_clause = ", ".join(group_exprs)

            count_sql = f"""
                SELECT COUNT(*) FROM (
                    SELECT 1
                    {base_from}
                    WHERE {where_clause}
                    GROUP BY {group_clause}
                ) grouped
            """
            total_count = self.db.conn.execute(count_sql, params).fetchone()[0]
            self.limit_engine.validate_group_count(total_count)
            self.limit_engine.ensure_page_in_bounds(total_count, page, page_size)

            offset = (page - 1) * page_size
            select_sql = f"""
                SELECT
                    {group_select},
                    {metric_sql}
                {base_from}
                WHERE {where_clause}
                GROUP BY {group_clause}
                ORDER BY {order_clause}
                LIMIT ? OFFSET ?
            """
            result = self.db.conn.execute(select_sql, [*params, page_size, offset])
            columns = [col[0] for col in result.description]
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
        else:
            count_sql = f"SELECT COUNT(*) {base_from} WHERE {where_clause}"
            source_rows = self.db.conn.execute(count_sql, params).fetchone()[0]
            total_count = 1 if source_rows > 0 else 0
            self.limit_engine.ensure_page_in_bounds(total_count, page, page_size)

            if total_count == 0:
                rows = []
            else:
                select_sql = f"""
                    SELECT
                        {metric_sql}
                    {base_from}
                    WHERE {where_clause}
                """
                result = self.db.conn.execute(select_sql, params)
                columns = [col[0] for col in result.description]
                rows = [dict(zip(columns, row)) for row in result.fetchall()]

        elapsed = perf_counter() - started
        self.limit_engine.validate_runtime(elapsed)
        return QueryPage(rows=rows, total_count=total_count, page=page, page_size=page_size)

    def _validate_group_by_fields(self, group_by: list[str]) -> None:
        for field in group_by:
            if field not in self.ALLOWED_GROUP_BY_FIELDS:
                raise QueryPolicyError(
                    code="INVALID_ARGUMENT",
                    message=f"Unsupported group_by field: {field}",
                    details={"field": field},
                )

    def _build_metric_sql(self, metrics: list[str]) -> str:
        metric_parts: list[str] = []
        for metric in metrics:
            sql = self.ALLOWED_METRICS.get(metric)
            if sql is None:
                raise QueryPolicyError(
                    code="INVALID_ARGUMENT",
                    message=f"Unsupported metric: {metric}",
                    details={"metric": metric},
                )
            metric_parts.append(sql)
        return ", ".join(metric_parts)

    def _build_conditions(
        self,
        query: str = "",
        hospital_ids: Optional[list[int]] = None,
        payer: str | None = None,
        setting: str | None = None,
        code_type: str | None = None,
        code: str | None = None,
        min_price: float | None = None,
        max_price: float | None = None,
    ) -> tuple[list[str], list[Any]]:
        conditions: list[str] = []
        params: list[Any] = []

        if query:
            conditions.append("LOWER(d.text) LIKE LOWER(?)")
            params.append(f"%{query}%")

        if hospital_ids:
            placeholders = ",".join("?" * len(hospital_ids))
            conditions.append(f"c.hospital_id IN ({placeholders})")
            params.extend(hospital_ids)

        if payer:
            conditions.append("p.name = ?")
            params.append(payer)

        if setting:
            conditions.append("c.setting = ?")
            params.append(setting)

        if code_type:
            conditions.append("c.code1_type = ?")
            params.append(code_type)

        if code:
            conditions.append("(c.code1 LIKE ? OR c.code2 LIKE ?)")
            params.extend([f"%{code}%", f"%{code}%"])

        if min_price is not None:
            conditions.append("c.negotiated_dollar >= ?")
            params.append(min_price)

        if max_price is not None:
            conditions.append("c.negotiated_dollar <= ?")
            params.append(max_price)

        return conditions, params

