"""Small .env parser/writer used by local MCP launchers and setup tools."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path


ASSIGNMENT_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")


@dataclass(frozen=True)
class EnvLine:
    """One parsed line from a dotenv-style file."""

    kind: str
    raw: str
    key: str = ""
    value: str = ""


def parse_env_text(text: str) -> list[EnvLine]:
    """Parse simple KEY=value dotenv text while preserving comments and blanks."""
    lines: list[EnvLine] = []
    for raw_line in text.splitlines():
        match = ASSIGNMENT_RE.match(raw_line)
        if not match:
            lines.append(EnvLine("raw", raw_line))
            continue

        key, value = match.groups()
        lines.append(EnvLine("assignment", raw_line, key, _unquote_value(value.strip())))
    return lines


def read_env_file(path: str | Path) -> dict[str, str]:
    """Read a dotenv file into a dictionary. Missing files return an empty dict."""
    env_path = Path(path)
    if not env_path.exists():
        return {}
    values: dict[str, str] = {}
    for line in parse_env_text(env_path.read_text(encoding="utf-8")):
        if line.kind == "assignment":
            values[line.key] = line.value
    return values


def write_env_file(
    path: str | Path,
    updates: dict[str, str],
    *,
    template_path: str | Path | None = None,
    preserve_existing: bool = True,
) -> Path:
    """Write updates to a dotenv file, preserving comments and unknown keys."""
    env_path = Path(path)
    source_path = env_path if env_path.exists() and preserve_existing else Path(template_path) if template_path else env_path
    lines = parse_env_text(source_path.read_text(encoding="utf-8")) if source_path.exists() else []

    seen: set[str] = set()
    rendered: list[str] = []
    for line in lines:
        if line.kind != "assignment":
            rendered.append(line.raw)
            continue
        seen.add(line.key)
        value = updates.get(line.key, line.value)
        rendered.append(f"{line.key}={quote_env_value(value)}")

    for key in sorted(set(updates) - seen):
        rendered.append(f"{key}={quote_env_value(updates[key])}")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    env_path.write_text("\n".join(rendered).rstrip() + "\n", encoding="utf-8")
    return env_path


def quote_env_value(value: str) -> str:
    """Return a dotenv-safe representation for a value."""
    if value == "":
        return ""
    if re.search(r"\s|#|['\"]", value):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def load_env_file(path: str | Path | None = None, *, override: bool = False) -> Path | None:
    """Load KEY=value pairs into ``os.environ``.

    Search order:
    1. explicit ``path``
    2. ``HC_MCP_ENV_FILE``
    3. ``.env`` in the current working directory

    Existing environment variables win unless ``override`` is true.
    """
    env_path = _resolve_env_path(path)
    if env_path is None or not env_path.exists():
        return None

    for key, value in read_env_file(env_path).items():
        if override or key not in os.environ:
            os.environ[key] = value
    return env_path


def _resolve_env_path(path: str | Path | None) -> Path | None:
    if path:
        return Path(path).expanduser()
    configured = os.environ.get("HC_MCP_ENV_FILE", "").strip()
    if configured:
        return Path(configured).expanduser()
    candidate = Path.cwd() / ".env"
    return candidate if candidate.exists() else None


def _unquote_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        inner = value[1:-1]
        if value[0] == '"':
            return inner.replace('\\"', '"').replace("\\\\", "\\")
        return inner
    return value
