#!/usr/bin/env bash
# Legacy compatibility wrapper for healthcare-data-mcp setup.
#
# The maintained setup surfaces are:
#   hc-mcp doctor       read-only readiness check
#   hc-mcp-setup ...    explicit env/cache setup and import actions

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

find_python() {
  for cmd in python3 python; do
    if command -v "$cmd" >/dev/null 2>&1; then
      "$cmd" - <<'PY' >/dev/null 2>&1 && {
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
        printf '%s\n' "$cmd"
        return 0
      }
    fi
  done
  return 1
}

usage() {
  cat <<'EOF'
Healthcare Data MCP setup wrapper

This legacy script is intentionally read-only when run without arguments.

Readiness:
  scripts/setup.sh
  scripts/setup.sh --doctor
  hc-mcp doctor

Explicit setup actions:
  scripts/setup.sh --interactive
  scripts/setup.sh --validate-only
  scripts/setup.sh --cache-status
  scripts/setup.sh --acquire-public-caches

All non-doctor arguments are passed through to hc-mcp-setup. Use
`hc-mcp-setup --help` for the full maintained command surface.
EOF
}

run_doctor() {
  if command -v hc-mcp >/dev/null 2>&1; then
    hc-mcp doctor
    return
  fi

  local python_cmd
  python_cmd="$(find_python)" || {
    echo "Python 3.11+ is required to run hc-mcp doctor." >&2
    return 1
  }
  PYTHONPATH="$PROJECT_ROOT" "$python_cmd" -m servers._launcher doctor
}

run_setup() {
  if command -v hc-mcp-setup >/dev/null 2>&1; then
    hc-mcp-setup "$@"
    return
  fi

  local python_cmd
  python_cmd="$(find_python)" || {
    echo "Python 3.11+ is required to run hc-mcp-setup." >&2
    return 1
  }
  PYTHONPATH="$PROJECT_ROOT" "$python_cmd" -m shared.setup_wizard "$@"
}

if [ "$#" -eq 0 ]; then
  echo "scripts/setup.sh is a compatibility wrapper. Running read-only hc-mcp doctor."
  echo "Use scripts/setup.sh --interactive or hc-mcp-setup --interactive for writes."
  run_doctor
  exit 0
fi

case "$1" in
  --help|-h)
    usage
    ;;
  --doctor)
    shift
    if [ "$#" -ne 0 ]; then
      echo "--doctor does not accept additional arguments." >&2
      exit 2
    fi
    run_doctor
    ;;
  *)
    run_setup "$@"
    ;;
esac
