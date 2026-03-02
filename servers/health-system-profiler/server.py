"""Health System Profiler MCP Server.

Returns complete health system profiles in 1-3 tool calls by combining
AHRQ Compendium, CMS Provider of Services, NPPES, and HSAF data.
"""

import json
import logging
import os as _os
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs = {"name": "health-system-profiler"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8007"))
mcp = FastMCP(**_mcp_kwargs)


@mcp.tool()
async def search_health_systems(query: str) -> str:
    """Search for health systems by name. Stub."""
    return json.dumps({"error": "Not implemented"})


if __name__ == "__main__":
    mcp.run(transport=_transport)
