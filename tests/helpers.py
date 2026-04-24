"""Test helpers for direct MCP tool invocation."""

from __future__ import annotations

import json
from typing import Any


def parse_tool_result(result: Any) -> Any:
    """Return a direct tool result as data for legacy JSON-string and structured tools."""
    if isinstance(result, str | bytes | bytearray):
        result = json.loads(result)
    if isinstance(result, dict) and isinstance(result.get("error"), dict):
        normalized = dict(result)
        normalized["error"] = str(result["error"].get("message", result["error"]))
        return normalized
    return result
