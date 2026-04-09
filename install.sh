#!/usr/bin/env bash
# Healthcare Data MCP — Universal Installer
# Detects MCP clients, installs servers, registers with each client.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/.../install.sh | bash
#   # or
#   ./install.sh
#
# Options:
#   --docker     Force Docker installation (skip pip)
#   --pip        Force pip installation (skip Docker)
#   --no-register  Install only, don't register with MCP clients
#   --help       Show this help

set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────────────

REPO_URL="https://github.com/ajhcs/healthcare-data-mcp.git"
PACKAGE_NAME="healthcare-data-mcp"
INSTALL_DIR="${HEALTHCARE_MCP_DIR:-$HOME/.healthcare-data-mcp}"

# Server name → module → port → requires_key (0=no, 1=optional)
declare -A SERVER_MODULES=(
  [hc-service-area]="servers.service_area.server"
  [hc-geo-demographics]="servers.geo_demographics.server"
  [hc-drive-time]="servers.drive_time.server"
  [hc-hospital-quality]="servers.hospital_quality.server"
  [hc-cms-facility]="servers.cms_facility.server"
  [hc-health-system-profiler]="servers.health_system_profiler.server"
  [hc-financial-intelligence]="servers.financial_intelligence.server"
  [hc-price-transparency]="servers.price_transparency.server"
  [hc-physician-referral-network]="servers.physician_referral_network.server"
  [hc-workforce-analytics]="servers.workforce_analytics.server"
  [hc-claims-analytics]="servers.claims_analytics.server"
  [hc-public-records]="servers.public_records.server"
  [hc-web-intelligence]="servers.web_intelligence.server"
)

declare -A SERVER_PORTS=(
  [hc-service-area]=8002
  [hc-geo-demographics]=8003
  [hc-drive-time]=8004
  [hc-hospital-quality]=8005
  [hc-cms-facility]=8006
  [hc-health-system-profiler]=8007
  [hc-financial-intelligence]=8008
  [hc-price-transparency]=8009
  [hc-physician-referral-network]=8010
  [hc-workforce-analytics]=8011
  [hc-claims-analytics]=8012
  [hc-public-records]=8013
  [hc-web-intelligence]=8014
)

# Servers that work with zero API keys
NO_KEY_SERVERS=(
  hc-service-area
  hc-hospital-quality
  hc-cms-facility
  hc-health-system-profiler
  hc-price-transparency
  hc-physician-referral-network
  hc-claims-analytics
)

# ── Colors ───────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${BLUE}[info]${NC}  $*"; }
ok()    { echo -e "${GREEN}[ok]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
err()   { echo -e "${RED}[err]${NC}   $*" >&2; }
header() { echo -e "\n${BOLD}${CYAN}── $* ──${NC}\n"; }

# ── Argument parsing ─────────────────────────────────────────────────────────

MODE=""
REGISTER=1
for arg in "$@"; do
  case "$arg" in
    --docker)      MODE="docker" ;;
    --pip)         MODE="pip" ;;
    --no-register) REGISTER=0 ;;
    --help|-h)
      head -12 "$0" | tail -10
      exit 0
      ;;
  esac
done

# ── Dependency detection ─────────────────────────────────────────────────────

header "Healthcare Data MCP — Installer"

has() { command -v "$1" &>/dev/null; }

detect_python() {
  for cmd in python3 python; do
    if has "$cmd"; then
      local ver
      ver=$($cmd --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
      local major minor
      major=$(echo "$ver" | cut -d. -f1)
      minor=$(echo "$ver" | cut -d. -f2)
      if [ "$major" -ge 3 ] && [ "$minor" -ge 11 ]; then
        echo "$cmd"
        return 0
      fi
    fi
  done
  return 1
}

PYTHON_CMD=""
DOCKER_OK=0
CLIENTS_FOUND=()

info "Checking prerequisites..."

if PYTHON_CMD=$(detect_python); then
  ok "Python: $($PYTHON_CMD --version 2>&1)"
else
  warn "Python 3.11+ not found"
fi

if has docker && docker info &>/dev/null 2>&1; then
  DOCKER_OK=1
  ok "Docker: $(docker --version | head -1)"
else
  warn "Docker not available"
fi

if has claude; then
  CLIENTS_FOUND+=("claude-code")
  ok "Claude Code detected"
fi

if has opencode; then
  CLIENTS_FOUND+=("opencode")
  ok "OpenCode detected"
fi

if has codex; then
  CLIENTS_FOUND+=("codex")
  ok "Codex CLI detected"
fi

if [ -d "$HOME/.openclaw" ] || has openclaw; then
  CLIENTS_FOUND+=("openclaw")
  ok "OpenClaw detected"
fi

if [ ${#CLIENTS_FOUND[@]} -eq 0 ]; then
  warn "No MCP clients detected. Servers will be installed but not registered."
  REGISTER=0
fi

echo ""

# ── Choose install method ────────────────────────────────────────────────────

if [ -z "$MODE" ]; then
  echo -e "${BOLD}Installation method:${NC}"
  echo "  1) pip install (local Python, stdio transport)"
  echo "  2) Docker Compose (containerized, HTTP transport)"
  echo ""

  if [ -n "$PYTHON_CMD" ] && [ "$DOCKER_OK" -eq 1 ]; then
    read -rp "Choose [1/2] (default: 1): " choice
    case "$choice" in
      2) MODE="docker" ;;
      *) MODE="pip" ;;
    esac
  elif [ -n "$PYTHON_CMD" ]; then
    info "Only pip available."
    MODE="pip"
  elif [ "$DOCKER_OK" -eq 1 ]; then
    info "Only Docker available."
    MODE="docker"
  else
    err "Need Python 3.11+ or Docker. Install one and re-run."
    exit 1
  fi
fi

# ── Install ──────────────────────────────────────────────────────────────────

header "Installing via $MODE"

if [ "$MODE" = "pip" ]; then
  if [ -z "$PYTHON_CMD" ]; then
    err "pip mode requires Python 3.11+."
    exit 1
  fi

  # Clone or update
  if [ -d "$INSTALL_DIR/.git" ]; then
    info "Updating existing installation..."
    git -C "$INSTALL_DIR" pull --ff-only
  elif [ -d "$INSTALL_DIR" ]; then
    info "Directory exists, installing into it..."
  else
    info "Cloning repository..."
    git clone --depth 1 "$REPO_URL" "$INSTALL_DIR" 2>/dev/null || {
      err "Git clone failed. Re-run with network access or clone the repository manually."
      exit 1
    }
  fi

  # Create venv if needed
  VENV_DIR="$INSTALL_DIR/.venv"
  if [ ! -d "$VENV_DIR" ]; then
    info "Creating virtual environment..."
    $PYTHON_CMD -m venv "$VENV_DIR"
  fi

  # Install
  info "Installing Python dependencies..."
  "$VENV_DIR/bin/pip" install --quiet --upgrade pip
  "$VENV_DIR/bin/pip" install --quiet -e "$INSTALL_DIR"
  ok "Python package installed"

  PYTHON_BIN="$VENV_DIR/bin/python"

elif [ "$MODE" = "docker" ]; then
  if [ "$DOCKER_OK" -ne 1 ]; then
    err "Docker mode requires Docker to be installed and running."
    exit 1
  fi

  # Clone if needed
  if [ ! -d "$INSTALL_DIR/.git" ]; then
    info "Cloning repository..."
    git clone --depth 1 "$REPO_URL" "$INSTALL_DIR" 2>/dev/null || {
      err "Could not clone repository."
      exit 1
    }
  fi

  info "Building Docker images..."
  docker compose -f "$INSTALL_DIR/docker-compose.zero-config.yml" build --quiet

  info "Starting zero-config servers (7 servers, no API keys needed)..."
  docker compose -f "$INSTALL_DIR/docker-compose.zero-config.yml" up -d

  ok "Docker containers running"
fi

# ── Environment setup ────────────────────────────────────────────────────────

header "Environment Configuration"

ENV_FILE="$INSTALL_DIR/.env"
if [ ! -f "$ENV_FILE" ] && [ -f "$INSTALL_DIR/.env.example" ]; then
  cp "$INSTALL_DIR/.env.example" "$ENV_FILE"
  info "Created .env from .env.example"
fi

echo -e "${BOLD}Optional API keys improve some servers. Press Enter to skip any.${NC}\n"

prompt_key() {
  local varname="$1" description="$2" url="$3"
  local current
  current=$(grep "^${varname}=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- || echo "")

  if [ -n "$current" ] && [ "$current" != "" ]; then
    ok "$varname already set"
    return
  fi

  echo -e "  ${CYAN}$description${NC}"
  echo -e "  Free signup: $url"
  read -rp "  $varname= " value
  if [ -n "$value" ]; then
    sed -i "s|^${varname}=.*|${varname}=${value}|" "$ENV_FILE" 2>/dev/null || \
      echo "${varname}=${value}" >> "$ENV_FILE"
    ok "Set $varname"
  fi
  echo ""
}

prompt_key "CENSUS_API_KEY" \
  "Census Bureau (geo-demographics)" \
  "https://api.census.gov/data/key_signup.html"

prompt_key "HUD_API_TOKEN" \
  "HUD USPS Crosswalk (geo-demographics)" \
  "https://www.huduser.gov/portal/dataset/uspszip-api.html"

prompt_key "ORS_API_KEY" \
  "OpenRouteService (drive-time isochrones)" \
  "https://openrouteservice.org/dev/#/signup"

prompt_key "SEC_USER_AGENT" \
  "SEC EDGAR fair-access header (financial-intelligence, required for that server)" \
  "https://www.sec.gov/os/accessing-edgar-data"

prompt_key "BLS_API_KEY" \
  "Bureau of Labor Statistics (workforce analytics)" \
  "https://data.bls.gov/registrationEngine/"

prompt_key "SAM_GOV_API_KEY" \
  "SAM.gov Entity API (public records)" \
  "https://sam.gov/content/entity-information"

prompt_key "CHPL_API_KEY" \
  "ONC CHPL API (public records)" \
  "https://chpl.healthit.gov/#/resources/api"

prompt_key "GOOGLE_CSE_API_KEY" \
  "Google Custom Search API (web intelligence)" \
  "https://developers.google.com/custom-search/v1/introduction"

prompt_key "GOOGLE_CSE_ID" \
  "Google Programmable Search Engine ID (web intelligence)" \
  "https://programmablesearchengine.google.com/controlpanel/all"

prompt_key "PROXYCURL_API_KEY" \
  "Proxycurl API (optional LinkedIn enrichment for web intelligence)" \
  "https://nubela.co/proxycurl/"

# ── Register with MCP clients ───────────────────────────────────────────────

if [ "$REGISTER" -eq 1 ]; then
  header "Registering with MCP Clients"

  for client in "${CLIENTS_FOUND[@]}"; do
    case "$client" in

      claude-code)
        info "Claude Code: .mcp.json already in project root."
        if [ "$INSTALL_DIR" != "$(pwd)" ]; then
          info "  Copy to your project: cp $INSTALL_DIR/.mcp.json /your/project/"
        fi
        ok "Claude Code configured"
        ;;

      opencode)
        info "OpenCode: opencode.jsonc already in project root."
        if [ "$INSTALL_DIR" != "$(pwd)" ]; then
          info "  Copy to your project: cp $INSTALL_DIR/opencode.jsonc /your/project/"
        fi
        ok "OpenCode configured"
        ;;

      codex)
        info "Registering servers with Codex CLI..."
        if [ "$MODE" = "docker" ]; then
          # HTTP mode for Docker
          for name in "${NO_KEY_SERVERS[@]}"; do
            port="${SERVER_PORTS[$name]}"
            codex mcp add "$name" --url "http://localhost:$port/mcp" 2>/dev/null && \
              ok "  Codex: $name (HTTP :$port)" || \
              warn "  Codex: $name failed (may already exist)"
          done
        else
          # stdio mode for pip
          for name in "${!SERVER_MODULES[@]}"; do
            module="${SERVER_MODULES[$name]}"
            codex mcp add "$name" -- "$PYTHON_BIN" -m "$module" 2>/dev/null && \
              ok "  Codex: $name (stdio)" || \
              warn "  Codex: $name failed (may already exist)"
          done
        fi
        ok "Codex CLI configured"
        ;;

      openclaw)
        OPENCLAW_CONFIG="$HOME/.openclaw/openclaw.json"
        if [ -f "$OPENCLAW_CONFIG" ]; then
          info "OpenClaw: merging into existing config..."
          warn "  Manual merge recommended — see $INSTALL_DIR/openclaw.json"
        else
          mkdir -p "$HOME/.openclaw"
          cp "$INSTALL_DIR/openclaw.json" "$OPENCLAW_CONFIG"
          ok "OpenClaw: installed config at $OPENCLAW_CONFIG"
        fi
        ;;

    esac
  done
fi

# ── Validation ───────────────────────────────────────────────────────────────

header "Validation"

PASS=0
FAIL=0

if [ "$MODE" = "pip" ]; then
  info "Testing stdio server startup..."
  for name in "${NO_KEY_SERVERS[@]}"; do
    module="${SERVER_MODULES[$name]}"
    if timeout 10 "$PYTHON_BIN" -c "
import importlib, sys
mod = importlib.import_module('${module}'.rsplit('.', 1)[0])
print('OK')
sys.exit(0)
" &>/dev/null; then
      ok "  $name module loads"
      ((PASS++))
    else
      warn "  $name module failed to load"
      ((FAIL++))
    fi
  done

elif [ "$MODE" = "docker" ]; then
  info "Checking Docker container health..."
  sleep 5
  for name in "${NO_KEY_SERVERS[@]}"; do
    port="${SERVER_PORTS[$name]}"
    if curl -sf "http://localhost:$port/mcp" --max-time 5 &>/dev/null || \
       python3 -c "import socket; s=socket.create_connection(('localhost',$port),5); s.close()" 2>/dev/null; then
      ok "  $name (port $port) responding"
      ((PASS++))
    else
      warn "  $name (port $port) not yet ready"
      ((FAIL++))
    fi
  done
fi

# ── Summary ──────────────────────────────────────────────────────────────────

header "Installation Complete"

echo -e "${BOLD}Results:${NC}"
echo -e "  Install method: ${CYAN}$MODE${NC}"
echo -e "  Install path:   ${CYAN}$INSTALL_DIR${NC}"
echo -e "  Servers:        ${GREEN}$PASS passed${NC}, ${YELLOW}$FAIL warnings${NC}"

if [ ${#CLIENTS_FOUND[@]} -gt 0 ]; then
  echo -e "  MCP clients:    ${GREEN}${CLIENTS_FOUND[*]}${NC}"
fi

echo ""
echo -e "${BOLD}Quick start:${NC}"
if [ "$MODE" = "pip" ]; then
  echo "  cd $INSTALL_DIR"
  echo "  # Edit .env with your API keys (optional)"
  echo "  # Claude Code picks up .mcp.json automatically"
  echo "  # For Docker: docker compose up -d"
elif [ "$MODE" = "docker" ]; then
  echo "  # 7 zero-config servers are already running!"
  echo "  # For all 13 servers: cd $INSTALL_DIR && cp .env.example .env"
  echo "  # Edit .env, then: docker compose up -d"
fi

echo ""
echo -e "${BOLD}Servers with zero config:${NC}"
for name in "${NO_KEY_SERVERS[@]}"; do
  port="${SERVER_PORTS[$name]}"
  echo -e "  ${GREEN}$name${NC} (port $port)"
done

echo ""
echo -e "${BOLD}Servers needing API keys:${NC}"
echo -e "  ${YELLOW}hc-geo-demographics${NC}  — Census + HUD keys"
echo -e "  ${YELLOW}hc-drive-time${NC}        — OpenRouteService key"
echo -e "  ${YELLOW}hc-financial-intelligence${NC} — SEC_USER_AGENT header"
echo -e "  ${YELLOW}hc-workforce-analytics${NC} — BLS key"
echo -e "  ${YELLOW}hc-public-records${NC}    — SAM.gov + CHPL keys"
echo -e "  ${YELLOW}hc-web-intelligence${NC}  — Google CSE + Proxycurl keys"
echo ""
