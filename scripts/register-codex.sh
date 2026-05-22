#!/usr/bin/env bash
# Register all Healthcare Data MCP servers with Codex CLI.
#
# Usage:
#   ./scripts/register-codex.sh              # stdio mode (pip install)
#   ./scripts/register-codex.sh --http       # HTTP mode (docker compose)
#   ./scripts/register-codex.sh --dry-run    # Preview registry-backed commands
#   ./scripts/register-codex.sh --remove     # Remove all registrations

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"
ENV_FILE="${HC_MCP_ENV_FILE:-$PROJECT_DIR/.env}"
if [ ! -f "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(which python3 2>/dev/null || which python 2>/dev/null)"
fi

if [ -z "${PYTHON_BIN:-}" ]; then
  echo "Error: python3 not found."
  exit 1
fi

mapfile -t SERVER_ROWS < <(
  PYTHONPATH="$PROJECT_DIR" "$PYTHON_BIN" - <<'PY'
from shared.utils.server_registry import SERVER_REGISTRY
for spec in SERVER_REGISTRY:
    print(f"hc-{spec.server_id}\t{spec.server_id}\t{spec.port}")
PY
)

MODE="--stdio"
REMOVE=0
DRY_RUN=0

for arg in "$@"; do
  case "$arg" in
    --stdio) MODE="--stdio" ;;
    --http) MODE="--http" ;;
    --remove) REMOVE=1 ;;
    --dry-run) DRY_RUN=1 ;;
    --help|-h)
      sed -n '2,9p' "$0"
      exit 0
      ;;
    *)
      echo "Error: unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

if [ "$DRY_RUN" = "1" ]; then
  echo "Dry run: no Codex config changes will be made."
  echo "Registry entries: ${#SERVER_ROWS[@]}"
  echo "Mode: $MODE"
  if [ "$REMOVE" = "1" ]; then
    printf '%s\n' "${SERVER_ROWS[@]}" | sort | while IFS=$'\t' read -r name server_id port; do
      echo "  would remove: codex mcp remove $name"
    done
  else
    printf '%s\n' "${SERVER_ROWS[@]}" | sort | while IFS=$'\t' read -r name server_id port; do
      if [ "$MODE" = "--http" ]; then
        echo "  would add: codex mcp add $name --url http://localhost:$port/mcp"
      elif [ -f "$ENV_FILE" ]; then
        echo "  would add: codex mcp add $name --env PYTHONPATH=$PROJECT_DIR --env HC_MCP_ENV_FILE=$ENV_FILE -- $PYTHON_BIN -m servers._launcher $server_id"
      else
        echo "  would add: codex mcp add $name --env PYTHONPATH=$PROJECT_DIR -- $PYTHON_BIN -m servers._launcher $server_id"
      fi
    done
  fi
  exit 0
fi

if ! command -v codex &>/dev/null; then
  echo "Error: codex CLI not found. Install from https://github.com/openai/codex"
  echo "Use --dry-run to preview registry-backed registrations without Codex installed."
  exit 1
fi

if [ "$REMOVE" = "1" ]; then
  echo "Removing all hc-* servers from Codex..."
  for row in "${SERVER_ROWS[@]}"; do
    IFS=$'\t' read -r name server_id port <<< "$row"
    codex mcp remove "$name" 2>/dev/null && echo "  Removed $name" || true
  done
  echo "Done."
  exit 0
fi

echo "Registering Healthcare Data MCP servers with Codex CLI..."
echo "  Mode: $MODE"
if [ "$MODE" != "--http" ] && [ -f "$ENV_FILE" ]; then
  echo "  Env file: $ENV_FILE"
fi
echo ""

printf '%s\n' "${SERVER_ROWS[@]}" | sort | while IFS=$'\t' read -r name server_id port; do
  # Remove existing registration first (idempotent)
  codex mcp remove "$name" 2>/dev/null || true

  if [ "$MODE" = "--http" ]; then
    codex mcp add "$name" --url "http://localhost:$port/mcp" && \
      echo "  + $name -> http://localhost:$port/mcp" || \
      echo "  ! $name failed"
  else
    env_args=(--env "PYTHONPATH=$PROJECT_DIR")
    if [ -f "$ENV_FILE" ]; then
      env_args+=(--env "HC_MCP_ENV_FILE=$ENV_FILE")
    fi
    codex mcp add "$name" \
      "${env_args[@]}" \
      -- "$PYTHON_BIN" -m servers._launcher "$server_id" && \
      echo "  + $name -> stdio ($server_id)" || \
      echo "  ! $name failed"
  fi
done

echo ""
echo "Done. Verify with: codex mcp list"
