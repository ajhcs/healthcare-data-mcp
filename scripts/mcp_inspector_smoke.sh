#!/usr/bin/env bash
# CI-friendly MCP Inspector smoke checks for stdio servers.

set -euo pipefail

INSPECTOR_PACKAGE="${INSPECTOR_PACKAGE:-@modelcontextprotocol/inspector@2.0.0}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-30}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -z "${PYTHON_BIN:-}" ]; then
  if [ -x ".venv/bin/python" ]; then
    PYTHON_BIN=".venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    PYTHON_BIN="$(command -v python)"
  fi
fi

if [ -z "${HC_MCP_BIN:-}" ]; then
  HC_MCP_BIN="$(command -v hc-mcp)"
fi

run_inspector() {
  local output_path="$1"
  local server_id="$2"
  shift 2
  local inspector_env=(-e "PYTHONPATH=$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}")
  if [ -n "${SEC_USER_AGENT:-}" ]; then
    inspector_env+=(-e "SEC_USER_AGENT=$SEC_USER_AGENT")
  fi

  timeout "$TIMEOUT_SECONDS" npx --yes "$INSPECTOR_PACKAGE" --cli \
    "$HC_MCP_BIN" "$server_id" "${inspector_env[@]}" --format json --transport stdio "$@" >"$output_path"
  "$PYTHON_BIN" -m json.tool "$output_path" >/dev/null
}

assert_json_field() {
  local output_path="$1"
  local expression="$2"

  "$PYTHON_BIN" - "$output_path" "$expression" <<'PY'
import json
import sys
from pathlib import Path

envelope = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
payload = envelope.get("result", envelope)
expression = sys.argv[2]
allowed = {"any": any, "len": len, "set": set}
if not eval(expression, {"__builtins__": {}}, {"payload": payload, **allowed}):
    raise SystemExit(f"Inspector smoke assertion failed: {expression}")
PY
}

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

run_inspector "$tmpdir/discovery-tools.json" "discovery" \
  --method tools/list
assert_json_field "$tmpdir/discovery-tools.json" \
  "any(tool.get('name') == 'list_workflows' for tool in payload.get('tools', []))"

run_inspector "$tmpdir/discovery-workflows.json" "discovery" \
  --method tools/call \
  --tool-name list_workflows
assert_json_field "$tmpdir/discovery-workflows.json" \
  "payload.get('structuredContent', {}).get('workflow_count', 0) >= 7"

run_inspector "$tmpdir/gateway-tools.json" "gateway" \
  --method tools/list
assert_json_field "$tmpdir/gateway-tools.json" \
  "{tool.get('name') for tool in payload.get('tools', [])} >= {'search', 'fetch'}"

run_inspector "$tmpdir/live-gateway-tools.json" "live-gateway" \
  --method tools/list
assert_json_field "$tmpdir/live-gateway-tools.json" \
  "any(tool.get('name') == 'list_live_tools' for tool in payload.get('tools', []))"

run_inspector "$tmpdir/live-gateway-inventory.json" "live-gateway" \
  --method tools/call \
  --tool-name list_live_tools
assert_json_field "$tmpdir/live-gateway-inventory.json" \
  "payload.get('structuredContent', {}).get('gateway') == 'live-gateway' and payload.get('structuredContent', {}).get('tool_count', 0) >= 1"

echo "MCP Inspector smoke passed"
