"""Web Intelligence & OSINT MCP Server.

Provides tools for health system competitive intelligence via web search,
executive profiling, EHR detection, and news monitoring. Port 8014.
"""

import json
import logging
import os as _os

from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

_transport = _os.environ.get("MCP_TRANSPORT", "stdio")
_mcp_kwargs: dict = {"name": "web-intelligence"}
if _transport in ("sse", "streamable-http"):
    _mcp_kwargs["host"] = "0.0.0.0"
    _mcp_kwargs["port"] = int(_os.environ.get("MCP_PORT", "8014"))
mcp = FastMCP(**_mcp_kwargs)


if __name__ == "__main__":
    mcp.run(transport=_transport)  # type: ignore[arg-type]
