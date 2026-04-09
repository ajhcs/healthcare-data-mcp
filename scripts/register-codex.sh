#!/usr/bin/env bash
# Register all Healthcare Data MCP servers with Codex CLI.
#
# Usage:
#   ./scripts/register-codex.sh              # stdio mode (pip install)
#   ./scripts/register-codex.sh --http       # HTTP mode (docker compose)
#   ./scripts/register-codex.sh --remove     # Remove all registrations

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

declare -A SERVERS=(
  [hc-service-area]="servers.service_area.server:8002"
  [hc-geo-demographics]="servers.geo_demographics.server:8003"
  [hc-drive-time]="servers.drive_time.server:8004"
  [hc-hospital-quality]="servers.hospital_quality.server:8005"
  [hc-cms-facility]="servers.cms_facility.server:8006"
  [hc-health-system-profiler]="servers.health_system_profiler.server:8007"
  [hc-financial-intelligence]="servers.financial_intelligence.server:8008"
  [hc-price-transparency]="servers.price_transparency.server:8009"
  [hc-physician-referral-network]="servers.physician_referral_network.server:8010"
  [hc-workforce-analytics]="servers.workforce_analytics.server:8011"
  [hc-claims-analytics]="servers.claims_analytics.server:8012"
  [hc-public-records]="servers.public_records.server:8013"
  [hc-web-intelligence]="servers.web_intelligence.server:8014"
)

if ! command -v codex &>/dev/null; then
  echo "Error: codex CLI not found. Install from https://github.com/openai/codex"
  exit 1
fi

MODE="${1:---stdio}"

if [ "$MODE" = "--remove" ]; then
  echo "Removing all hc-* servers from Codex..."
  for name in "${!SERVERS[@]}"; do
    codex mcp remove "$name" 2>/dev/null && echo "  Removed $name" || true
  done
  echo "Done."
  exit 0
fi

PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python"
if [ ! -f "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(which python3 2>/dev/null || which python 2>/dev/null)"
fi

echo "Registering Healthcare Data MCP servers with Codex CLI..."
echo "  Mode: $MODE"
echo ""

for name in $(echo "${!SERVERS[@]}" | tr ' ' '\n' | sort); do
  IFS=':' read -r module port <<< "${SERVERS[$name]}"

  # Remove existing registration first (idempotent)
  codex mcp remove "$name" 2>/dev/null || true

  if [ "$MODE" = "--http" ]; then
    codex mcp add "$name" --url "http://localhost:$port/mcp" && \
      echo "  + $name -> http://localhost:$port/mcp" || \
      echo "  ! $name failed"
  else
    codex mcp add "$name" \
      --env "PYTHONPATH=$PROJECT_DIR" \
      -- "$PYTHON_BIN" -m "$module" && \
      echo "  + $name -> stdio ($module)" || \
      echo "  ! $name failed"
  fi
done

echo ""
echo "Done. Verify with: codex mcp list"
