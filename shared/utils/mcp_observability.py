"""Non-secret MCP tool call metrics for local and HTTP servers."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable
from functools import wraps
import inspect
import json
import logging
import time
from typing import Any, TypeVar

from shared.utils.mcp_response import to_structured

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])
_MAX_EVENTS = 200
_EVENTS: deque[dict[str, Any]] = deque(maxlen=_MAX_EVENTS)
_COUNTERS: dict[str, dict[str, int]] = defaultdict(lambda: {"calls": 0, "errors": 0})


def observe_tool(server_id: str, tool_name: str | None = None) -> Callable[[F], F]:
    """Decorate a tool function with lightweight non-secret timing metrics."""

    def decorator(func: F) -> F:
        name = tool_name or getattr(func, "__name__", "unknown_tool")

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                started = time.monotonic()
                try:
                    result = await func(*args, **kwargs)
                except Exception as exc:
                    _record(server_id, name, started, outcome="error", error_type=type(exc).__name__)
                    raise
                _record(server_id, name, started, outcome="ok", result=result)
                return result

            return async_wrapper  # type: ignore[return-value]

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            started = time.monotonic()
            try:
                result = func(*args, **kwargs)
            except Exception as exc:
                _record(server_id, name, started, outcome="error", error_type=type(exc).__name__)
                raise
            _record(server_id, name, started, outcome="ok", result=result)
            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def _record(
    server_id: str,
    tool_name: str,
    started: float,
    *,
    outcome: str,
    result: Any | None = None,
    error_type: str = "",
) -> None:
    duration_ms = round((time.monotonic() - started) * 1000, 2)
    counters = _COUNTERS[f"{server_id}.{tool_name}"]
    counters["calls"] += 1
    if outcome != "ok":
        counters["errors"] += 1
    event = {
        "server": server_id,
        "tool": tool_name,
        "outcome": outcome,
        "duration_ms": duration_ms,
        "error_type": error_type,
        "result_size_bytes": _json_size(result) if result is not None else 0,
        "dataset_id": _first_nested_value(result, "dataset_id") if result is not None else "",
        "cache_status": _first_nested_value(result, "cache_status") if result is not None else "",
    }
    _EVENTS.append(event)
    logger.info("mcp_tool_metric %s", json.dumps(event, sort_keys=True, default=str))


def tooling_metrics_payload(server_id: str | None = None) -> dict[str, Any]:
    """Return recent non-secret tool metrics."""

    events = [event for event in _EVENTS if server_id is None or event["server"] == server_id]
    counters = {
        key: value
        for key, value in sorted(_COUNTERS.items())
        if server_id is None or key.startswith(f"{server_id}.")
    }
    return {"event_count": len(events), "events": events[-50:], "counters": counters}


def _json_size(value: Any) -> int:
    try:
        return len(json.dumps(to_structured(value), default=str))
    except Exception:
        return 0


def _first_nested_value(value: Any, key: str) -> str:
    if isinstance(value, dict):
        if key in value and value[key] not in ("", None):
            return str(value[key])
        for child in value.values():
            found = _first_nested_value(child, key)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _first_nested_value(child, key)
            if found:
                return found
    return ""
