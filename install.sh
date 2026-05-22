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
#   --dry-run    Check prerequisites and planned mode without writing files
#   --help       Show this help

set -euo pipefail

# ── Constants ────────────────────────────────────────────────────────────────

REPO_URL="https://github.com/ajhcs/healthcare-data-mcp.git"
PACKAGE_NAME="healthcare-data-mcp"
INSTALL_DIR="${HEALTHCARE_MCP_DIR:-$HOME/.healthcare-data-mcp}"
SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR=""
if [ -f "$SCRIPT_PATH" ]; then
  SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
fi

# Server metadata is loaded from shared/utils/server_registry.py whenever a
# checkout is available. This keeps installer registration and validation aligned
# with the launcher, Compose files, docs, and MCPB packaging.
declare -A SERVER_MODULES=()
declare -A SERVER_IDS=()
declare -A SERVER_PORTS=()
declare -A ENV_DESCRIPTIONS=()
declare -A ENV_REQUIRED=()
declare -A ENV_SERVERS=()
NO_KEY_SERVERS=()
ENV_KEYS=()

load_server_registry() {
  local registry_dir="$1"
  local python_cmd="$2"

  if [ -z "$python_cmd" ] || [ ! -f "$registry_dir/shared/utils/server_registry.py" ]; then
    return 1
  fi

  SERVER_MODULES=()
  SERVER_IDS=()
  SERVER_PORTS=()
  ENV_DESCRIPTIONS=()
  ENV_REQUIRED=()
  ENV_SERVERS=()
  NO_KEY_SERVERS=()
  ENV_KEYS=()

  local rows row row_type name server_id module port zero_config required description servers
  mapfile -t rows < <(
    PYTHONPATH="$registry_dir" "$python_cmd" - <<'PY'
from collections import defaultdict

from shared.utils.server_registry import SERVER_REGISTRY

env_servers = defaultdict(list)
env_descriptions = {}
env_required = defaultdict(bool)

for spec in SERVER_REGISTRY:
    print(f"SERVER\thc-{spec.server_id}\t{spec.server_id}\t{spec.module}\t{spec.port}\t{int(spec.zero_config)}")
    for key in (*spec.required_env, *spec.optional_env):
        env_servers[key.name].append(spec.server_id)
        env_descriptions.setdefault(key.name, key.description.replace("\t", " "))
        env_required[key.name] = env_required[key.name] or key.required

for name in sorted(env_servers):
    print(
        "ENV\t"
        f"{name}\t"
        f"{int(env_required[name])}\t"
        f"{env_descriptions.get(name, '')}\t"
        f"{', '.join(sorted(env_servers[name]))}"
    )
PY
  )

  for row in "${rows[@]}"; do
    IFS=$'\t' read -r row_type name server_id module port zero_config <<< "$row"
    if [ "$row_type" = "SERVER" ]; then
      SERVER_IDS["$name"]="$server_id"
      SERVER_MODULES["$name"]="$module"
      SERVER_PORTS["$name"]="$port"
      if [ "$zero_config" = "1" ]; then
        NO_KEY_SERVERS+=("$name")
      fi
    elif [ "$row_type" = "ENV" ]; then
      required="$server_id"
      description="$module"
      servers="$port"
      ENV_KEYS+=("$name")
      ENV_REQUIRED["$name"]="$required"
      ENV_DESCRIPTIONS["$name"]="$description"
      ENV_SERVERS["$name"]="$servers"
    fi
  done
}

load_zero_config_compose_registry() {
  local registry_dir="$1"
  local compose_file="$registry_dir/docker-compose.zero-config.yml"

  if [ ! -f "$compose_file" ]; then
    return 1
  fi

  SERVER_MODULES=()
  SERVER_IDS=()
  SERVER_PORTS=()
  ENV_DESCRIPTIONS=()
  ENV_REQUIRED=()
  ENV_SERVERS=()
  NO_KEY_SERVERS=()
  ENV_KEYS=()

  local current_name="" current_module="" line
  while IFS= read -r line; do
    if [[ "$line" =~ ^[[:space:]]{2}([a-z0-9-]+):$ ]]; then
      current_name="hc-${BASH_REMATCH[1]}"
      current_module=""
      SERVER_IDS["$current_name"]="${BASH_REMATCH[1]}"
    elif [[ "$line" =~ command:[[:space:]]python[[:space:]]-m[[:space:]]([^[:space:]]+) ]]; then
      current_module="${BASH_REMATCH[1]}"
      if [ -n "$current_name" ]; then
        SERVER_MODULES["$current_name"]="$current_module"
      fi
    elif [[ "$line" =~ 127\.0\.0\.1:([0-9]+):[0-9]+ ]]; then
      if [ -n "$current_name" ]; then
        SERVER_PORTS["$current_name"]="${BASH_REMATCH[1]}"
        NO_KEY_SERVERS+=("$current_name")
      fi
    fi
  done < "$compose_file"

  [ "${#NO_KEY_SERVERS[@]}" -gt 0 ]
}

load_available_local_registry() {
  local python_cmd="$1"
  local candidate

  if [ -z "$python_cmd" ]; then
    return 1
  fi

  for candidate in "$(pwd)" "$SCRIPT_DIR"; do
    if [ -n "$candidate" ] && load_server_registry "$candidate" "$python_cmd"; then
      ok "Server registry: loaded ${#SERVER_MODULES[@]} entries from $candidate"
      return 0
    fi
  done

  return 1
}

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

format_csv() {
  local out="" item
  for item in "$@"; do
    if [ -n "$out" ]; then
      out+=", "
    fi
    out+="$item"
  done
  printf '%s' "$out"
}

usage() {
  cat <<'EOF'
# Healthcare Data MCP — Universal Installer
# Detects MCP clients, installs servers, registers with each client.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/.../install.sh | bash
#   # or
#   ./install.sh
#
# Options:
#   --docker       Force Docker installation (skip pip)
#   --pip          Force pip installation (skip Docker)
#   --no-register  Install only, don't register with MCP clients
#   --dry-run      Check prerequisites and planned mode without writing files
#   --help         Show this help
EOF
}

# ── Argument parsing ─────────────────────────────────────────────────────────

MODE=""
REGISTER=1
DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --docker)      MODE="docker" ;;
    --pip)         MODE="pip" ;;
    --no-register) REGISTER=0 ;;
    --dry-run)     DRY_RUN=1 ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      err "Unknown installer option: $arg"
      usage >&2
      exit 2
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

load_available_local_registry "$PYTHON_CMD" || true

echo ""

if [ "$DRY_RUN" -eq 1 ]; then
  header "Dry Run"
  info "Install directory: $INSTALL_DIR"
  info "Requested mode: ${MODE:-auto-detect}"
  info "Registration: $([ "$REGISTER" -eq 1 ] && echo enabled || echo disabled)"
  if [ "${#SERVER_MODULES[@]}" -gt 0 ]; then
    info "Server registry entries: ${#SERVER_MODULES[@]}"
    info "Registry environment keys: ${#ENV_KEYS[@]}"
    info "Zero-config servers: $(format_csv "${NO_KEY_SERVERS[@]}")"
    if [ "${#ENV_KEYS[@]}" -gt 0 ]; then
      info "Registry environment key names: $(format_csv "${ENV_KEYS[@]}")"
    fi
  else
    info "Server registry entries: unavailable until the repository checkout exists"
  fi
  ok "Dry run completed without cloning, installing, writing config, or registering clients"
  exit 0
fi

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
  if ! load_server_registry "$INSTALL_DIR" "$PYTHON_BIN"; then
    err "Could not load server registry from $INSTALL_DIR"
    exit 1
  fi

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

  if [ -n "$PYTHON_CMD" ]; then
    load_server_registry "$INSTALL_DIR" "$PYTHON_CMD" || load_zero_config_compose_registry "$INSTALL_DIR" || {
      err "Could not load server registry from $INSTALL_DIR"
      exit 1
    }
  else
    load_zero_config_compose_registry "$INSTALL_DIR" || {
      err "Could not load zero-config server metadata from $INSTALL_DIR/docker-compose.zero-config.yml"
      exit 1
    }
  fi

  info "Starting zero-config servers (registry-selected servers with no required API keys)..."
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

echo -e "${BOLD}Registry-defined environment keys enable optional and key-required tools. Press Enter to skip any.${NC}\n"

prompt_key() {
  local varname="$1" description="$2" servers="$3" required="$4"
  local current label
  current=$(grep "^${varname}=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- || echo "")
  label="optional"
  if [ "$required" = "1" ]; then
    label="required for listed server"
  fi

  if [ -n "$current" ] && [ "$current" != "" ]; then
    ok "$varname already set"
    return
  fi

  echo -e "  ${CYAN}$varname${NC} ($label)"
  echo -e "  $description"
  echo -e "  Servers: $servers"
  read -rp "  $varname= " value
  if [ -n "$value" ]; then
    sed -i "s|^${varname}=.*|${varname}=${value}|" "$ENV_FILE" 2>/dev/null || \
      echo "${varname}=${value}" >> "$ENV_FILE"
    ok "Set $varname"
  fi
  echo ""
}

prompt_registry_keys() {
  if [ "${#ENV_KEYS[@]}" -eq 0 ]; then
    warn "Registry environment metadata unavailable; skipping interactive key prompts."
    return
  fi

  local varname
  for varname in "${ENV_KEYS[@]}"; do
    prompt_key "$varname" "${ENV_DESCRIPTIONS[$varname]}" "${ENV_SERVERS[$varname]}" "${ENV_REQUIRED[$varname]}"
  done
}

prompt_registry_keys

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
            server_id="${SERVER_IDS[$name]:-${name#hc-}}"
            env_args=()
            if [ -f "$ENV_FILE" ]; then
              env_args+=(--env "HC_MCP_ENV_FILE=$ENV_FILE")
            fi
            codex mcp add "$name" "${env_args[@]}" -- "$PYTHON_BIN" -m servers._launcher "$server_id" 2>/dev/null && \
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
  echo "  # Zero-config servers are already running!"
  echo "  # For all registry servers: cd $INSTALL_DIR && cp .env.example .env"
  echo "  # Edit .env, then: docker compose up -d"
fi

echo ""
echo -e "${BOLD}Servers with zero config:${NC}"
for name in "${NO_KEY_SERVERS[@]}"; do
  port="${SERVER_PORTS[$name]}"
  echo -e "  ${GREEN}$name${NC} (port $port)"
done

echo ""
echo -e "${BOLD}Servers with key-enhanced tools:${NC}"
if [ "${#ENV_KEYS[@]}" -eq 0 ]; then
  echo -e "  ${YELLOW}Run hc-mcp doctor for registry-backed environment guidance.${NC}"
else
  for varname in "${ENV_KEYS[@]}"; do
    echo -e "  ${YELLOW}$varname${NC} — ${ENV_SERVERS[$varname]}"
  done
fi
echo ""
