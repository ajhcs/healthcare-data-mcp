"""LAN-only, read-only MCP interface for pre-ingested exact Form 990 facts."""

from __future__ import annotations

import os
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from shared.utils.mcp_resources import register_standard_resources

from .fact_store import Form990FactStore, validate_lan_bind_host
from .operator_ingest import default_database_path


_transport = os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict[str, Any] = {
    "name": "form-990-facts",
    "instructions": (
        "Read-only lookup for pre-ingested official IRS Form 990 facts. "
        "Use only with a known legal-filer EIN and tax-period year; preserve "
        "returned provenance and do not infer brands, affiliates, or comparisons."
    ),
}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = validate_lan_bind_host(os.environ.get("MCP_HOST", "127.0.0.1"))
    _mcp_kwargs["port"] = int(os.environ.get("MCP_PORT", "8022"))

mcp = FastMCP(**_mcp_kwargs)
register_standard_resources(mcp, "form-990-facts")


@mcp.tool(
    structured_output=True,
    annotations=ToolAnnotations(
        title="Get exact Form 990 facts",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
def get_form_990_facts(ein: str, tax_year: int) -> dict[str, Any]:
    """Retrieve exact pre-ingested Form 990 facts for one legal filer and tax year.

    Discovery
    ---------
    Requires an operator-populated local IRS e-file XML store.

    When to use
    -----------
    Use only with a known legal-filer EIN and tax year.

    Parameters
    ----------
    ein: Exact nine-digit filer EIN. tax_year: Tax-period end year.

    Returns
    -------
    Exact facts, missing-data statuses, and field-level IRS XML provenance.

    Do / Don't
    ----------
    Do preserve provenance. Don't infer brands, affiliates, or comparisons.

    Examples
    --------
    ``get_form_990_facts("811244422", 2024)``

    Common mistakes
    ---------------
    Treating a legal-filer return as a consolidated natural-brand result.
    """

    return Form990FactStore(default_database_path()).get_facts(ein=ein, tax_year=tax_year)


if __name__ == "__main__":
    mcp.run(transport=_transport)
