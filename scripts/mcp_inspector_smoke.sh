#!/usr/bin/env bash
# CI-friendly MCP Inspector smoke checks for stdio servers.

set -euo pipefail

INSPECTOR_PACKAGE="${INSPECTOR_PACKAGE:-@modelcontextprotocol/inspector}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"

if [ -z "${PYTHON_BIN:-}" ]; then
  if [ -x ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    PYTHON_BIN="$(command -v python)"
  fi
fi

run_inspector() {
  local output_path="$1"
  shift

  timeout "$TIMEOUT_SECONDS" npx --yes "$INSPECTOR_PACKAGE" --cli "$@" >"$output_path"
  "$PYTHON_BIN" -m json.tool "$output_path" >/dev/null
}

assert_json_field() {
  local output_path="$1"
  local expression="$2"

  "$PYTHON_BIN" - "$output_path" "$expression" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
expression = sys.argv[2]
allowed = {"any": any, "len": len, "set": set}
if not eval(expression, {"__builtins__": {}}, {"payload": payload, **allowed}):
    raise SystemExit(f"Inspector smoke assertion failed: {expression}")
PY
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

run_inspector "$tmpdir/discovery-tools.json" \
  --method tools/list \
  --transport stdio \
  -- "$PYTHON_BIN" -m servers._launcher discovery
assert_json_field "$tmpdir/discovery-tools.json" \
  "any(tool.get('name') == 'list_workflows' for tool in payload.get('tools', []))"

run_inspector "$tmpdir/discovery-workflows.json" \
  --method tools/call \
  --tool-name list_workflows \
  --transport stdio \
  -- "$PYTHON_BIN" -m servers._launcher discovery
assert_json_field "$tmpdir/discovery-workflows.json" \
  "payload.get('structuredContent', {}).get('workflow_count', 0) >= 7"

run_inspector "$tmpdir/gateway-tools.json" \
  --method tools/list \
  --transport stdio \
  -- "$PYTHON_BIN" -m servers._launcher gateway
assert_json_field "$tmpdir/gateway-tools.json" \
  "{tool.get('name') for tool in payload.get('tools', [])} >= {'search', 'fetch'}"

run_inspector "$tmpdir/live-gateway-tools.json" \
  --method tools/list \
  --transport stdio \
  -- "$PYTHON_BIN" -m servers._launcher live-gateway
assert_json_field "$tmpdir/live-gateway-tools.json" \
  "any(tool.get('name') == 'list_live_tools' for tool in payload.get('tools', []))"

run_inspector "$tmpdir/live-gateway-inventory.json" \
  --method tools/call \
  --tool-name list_live_tools \
  --transport stdio \
  -- "$PYTHON_BIN" -m servers._launcher live-gateway
assert_json_field "$tmpdir/live-gateway-inventory.json" \
  "payload.get('structuredContent', {}).get('gateway') == 'live-gateway' and payload.get('structuredContent', {}).get('tool_count', 0) >= 1"

echo "MCP Inspector smoke passed"
