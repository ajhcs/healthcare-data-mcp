#!/usr/bin/env bash
# setup.sh -- Interactive setup for healthcare-data-mcp
# Checks prerequisites, configures API keys, and reports server readiness.

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors and formatting
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

info()  { echo -e "${BLUE}[INFO]${NC} $1"; }
ok()    { echo -e "${GREEN}[OK]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail()  { echo -e "${RED}[FAIL]${NC} $1"; }

header() {
    echo ""
    echo -e "${BOLD}=== $1 ===${NC}"
    echo ""
}

# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/.env"
ENV_EXAMPLE="$PROJECT_ROOT/.env.example"

echo ""
echo -e "${BOLD}Healthcare Data MCP -- Setup${NC}"
echo "13 servers, 68 tools, public healthcare data analytics"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Check Python version
# ---------------------------------------------------------------------------
header "Step 1: Checking prerequisites"

PYTHON_CMD=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        PYTHON_CMD="$cmd"
        break
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    fail "Python not found. Install Python 3.11+ and re-run."
    exit 1
fi

PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PYTHON_MAJOR=$($PYTHON_CMD -c 'import sys; print(sys.version_info.major)')
PYTHON_MINOR=$($PYTHON_CMD -c 'import sys; print(sys.version_info.minor)')

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 11 ]); then
    fail "Python $PYTHON_VERSION found, but 3.11+ is required."
    echo "  Install Python 3.11+ from https://www.python.org/downloads/"
    exit 1
fi

ok "Python $PYTHON_VERSION ($PYTHON_CMD)"

# Check pip
if $PYTHON_CMD -m pip --version &>/dev/null; then
    ok "pip available"
else
    fail "pip not available. Install with: $PYTHON_CMD -m ensurepip"
    exit 1
fi

# Check git
if command -v git &>/dev/null; then
    ok "git $(git --version | cut -d' ' -f3)"
else
    warn "git not found (not required, but recommended)"
fi

# Check Docker (optional)
if command -v docker &>/dev/null; then
    ok "docker available (enables Docker Compose deployment)"
    DOCKER_AVAILABLE=true
else
    info "docker not found (optional -- only needed for Docker Compose deployment)"
    DOCKER_AVAILABLE=false
fi

# ---------------------------------------------------------------------------
# Step 2: Install Python dependencies
# ---------------------------------------------------------------------------
header "Step 2: Installing Python dependencies"

cd "$PROJECT_ROOT"

if [ -d ".venv" ]; then
    info "Virtual environment already exists at .venv"
    read -rp "  Re-use it? [Y/n] " reuse_venv
    if [[ "${reuse_venv:-Y}" =~ ^[Nn] ]]; then
        info "Removing old virtual environment..."
        rm -rf .venv
        $PYTHON_CMD -m venv .venv
        ok "Created fresh .venv"
    fi
else
    info "Creating virtual environment..."
    $PYTHON_CMD -m venv .venv
    ok "Created .venv"
fi

# Activate venv for the rest of the script
source .venv/bin/activate

info "Installing dependencies (this may take a minute)..."
pip install -q -e ".[dev]" 2>&1 | tail -1
ok "Dependencies installed"

# ---------------------------------------------------------------------------
# Step 3: Configure environment variables
# ---------------------------------------------------------------------------
header "Step 3: Configuring API keys"

if [ -f "$ENV_FILE" ]; then
    info ".env file already exists."
    read -rp "  Overwrite with fresh copy from .env.example? [y/N] " overwrite
    if [[ "${overwrite:-N}" =~ ^[Yy] ]]; then
        cp "$ENV_EXAMPLE" "$ENV_FILE"
        ok "Copied .env.example -> .env"
    else
        info "Keeping existing .env"
    fi
else
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    ok "Created .env from .env.example"
fi

echo ""
info "The following API keys are optional. Press Enter to skip any key."
info "Servers work without keys but some tools will be unavailable."
echo ""

# Helper to prompt for a key and write it to .env
prompt_key() {
    local key_name="$1"
    local description="$2"
    local signup_url="$3"
    local current=""

    # Read current value from .env
    if grep -q "^${key_name}=" "$ENV_FILE" 2>/dev/null; then
        current=$(grep "^${key_name}=" "$ENV_FILE" | cut -d'=' -f2-)
    fi

    if [ -n "$current" ] && [ "$current" != "" ]; then
        info "$key_name is already set."
        read -rp "  Replace it? [y/N] " replace
        if [[ ! "${replace:-N}" =~ ^[Yy] ]]; then
            return 0
        fi
    fi

    echo -e "  ${BOLD}$key_name${NC}"
    echo "  $description"
    echo -e "  Register: ${BLUE}$signup_url${NC}"
    read -rp "  Enter key (or press Enter to skip): " value

    if [ -n "$value" ]; then
        # Update in-place using sed (portable across Linux/macOS)
        if grep -q "^${key_name}=" "$ENV_FILE" 2>/dev/null; then
            sed -i "s|^${key_name}=.*|${key_name}=${value}|" "$ENV_FILE"
        else
            echo "${key_name}=${value}" >> "$ENV_FILE"
        fi
        ok "$key_name saved"
    else
        info "$key_name skipped"
    fi
    echo ""
}

prompt_key "CENSUS_API_KEY" \
    "Census Bureau API key. Enables population demographics, income, insurance coverage by ZIP." \
    "https://api.census.gov/data/key_signup.html"

prompt_key "HUD_API_TOKEN" \
    "HUD USPS Crosswalk token. Enables ZIP-to-county/tract/CBSA crosswalks." \
    "https://www.huduser.gov/portal/dataset/uspszip-api.html"

prompt_key "ORS_API_KEY" \
    "OpenRouteService key. Enables drive-time isochrone polygon generation." \
    "https://openrouteservice.org/dev/#/signup"

prompt_key "SEC_USER_AGENT" \
    "SEC EDGAR user agent (format: 'AppName email@example.com'). Required for SEC API fair access." \
    "https://www.sec.gov/os/accessing-edgar-data"

prompt_key "BLS_API_KEY" \
    "Bureau of Labor Statistics API v2 key. Enables healthcare occupation wage and employment data." \
    "https://www.bls.gov/developers/home.htm"

prompt_key "SAM_GOV_API_KEY" \
    "SAM.gov API key. Enables federal contract opportunity search." \
    "https://sam.gov/content/entity-registration"

prompt_key "CHPL_API_KEY" \
    "ONC CHPL API key. Enables EHR certification product lookup." \
    "https://chpl.healthit.gov/#/resources/api"

prompt_key "GOOGLE_CSE_API_KEY" \
    "Google Custom Search API key. Powers web intelligence: executive profiles, EHR detection, news." \
    "https://developers.google.com/custom-search/v1/introduction"

prompt_key "GOOGLE_CSE_ID" \
    "Google Custom Search Engine ID. Created alongside the CSE API key." \
    "https://programmablesearchengine.google.com/controlpanel/all"

prompt_key "PROXYCURL_API_KEY" \
    "Proxycurl API key. Enables LinkedIn enrichment for executive profiles (paid service)." \
    "https://nubela.co/proxycurl/"

# ---------------------------------------------------------------------------
# Step 4: Validate keys
# ---------------------------------------------------------------------------
header "Step 4: Validating API keys"

# Source .env for validation
set -a
source "$ENV_FILE"
set +a

validate_census() {
    if [ -z "${CENSUS_API_KEY:-}" ]; then
        echo "skip"
        return
    fi
    local status
    status=$(python3 -c "
import urllib.request, json, sys
try:
    url = 'https://api.census.gov/data/2023/acs/acs5?get=NAME&for=zip+code+tabulation+area:60614&key=${CENSUS_API_KEY}'
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    if len(data) > 1:
        print('valid')
    else:
        print('invalid')
except Exception as e:
    print(f'error:{e}')
" 2>/dev/null)
    echo "$status"
}

validate_bls() {
    if [ -z "${BLS_API_KEY:-}" ]; then
        echo "skip"
        return
    fi
    local status
    status=$(python3 -c "
import urllib.request, json, sys
try:
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/OEUN000000000000029114103'
    data = json.dumps({'seriesid': ['OEUN000000000000029114103'], 'registrationkey': '${BLS_API_KEY}'}).encode()
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    resp = urllib.request.urlopen(req, timeout=10)
    result = json.loads(resp.read())
    if result.get('status') == 'REQUEST_SUCCEEDED':
        print('valid')
    else:
        print('invalid')
except Exception as e:
    print(f'error:{e}')
" 2>/dev/null)
    echo "$status"
}

# Run validations
for key_check in CENSUS_API_KEY BLS_API_KEY; do
    val="${!key_check:-}"
    if [ -z "$val" ]; then
        info "$key_check: not set (skipped)"
        continue
    fi

    case "$key_check" in
        CENSUS_API_KEY)
            result=$(validate_census)
            ;;
        BLS_API_KEY)
            result=$(validate_bls)
            ;;
    esac

    case "$result" in
        valid)   ok "$key_check: validated successfully" ;;
        invalid) warn "$key_check: key was rejected by the API -- check for typos" ;;
        skip)    info "$key_check: not set" ;;
        *)       warn "$key_check: could not validate ($result)" ;;
    esac
done

# Non-validatable keys: just report presence
for key_check in HUD_API_TOKEN ORS_API_KEY SEC_USER_AGENT SAM_GOV_API_KEY CHPL_API_KEY GOOGLE_CSE_API_KEY GOOGLE_CSE_ID PROXYCURL_API_KEY; do
    val="${!key_check:-}"
    if [ -n "$val" ]; then
        ok "$key_check: configured"
    else
        info "$key_check: not set"
    fi
done

# ---------------------------------------------------------------------------
# Step 5: Server readiness report
# ---------------------------------------------------------------------------
header "Step 5: Server readiness"

echo ""
printf "  %-30s %-12s %s\n" "SERVER" "STATUS" "NOTES"
printf "  %-30s %-12s %s\n" "------" "------" "-----"

report_server() {
    local name="$1"
    local status="$2"
    local notes="$3"

    local color="$GREEN"
    if [ "$status" = "DEGRADED" ]; then
        color="$YELLOW"
    elif [ "$status" = "MISSING" ]; then
        color="$RED"
    fi
    printf "  %-30s ${color}%-12s${NC} %s\n" "$name" "$status" "$notes"
}

# Core servers (no keys needed)
report_server "cms-facility" "READY" "5 tools, no keys required"
report_server "service-area" "READY" "4 tools, no keys required"
report_server "hospital-quality" "READY" "6 tools, no keys required"
report_server "health-system-profiler" "READY" "3 tools, no keys required"
report_server "claims-analytics" "READY" "5 tools, no keys required"
report_server "price-transparency" "READY" "5 tools, no keys required"
report_server "physician-referral-network" "READY" "5 tools, no keys required"

# Servers with optional keys
if [ -n "${CENSUS_API_KEY:-}" ] || [ -n "${HUD_API_TOKEN:-}" ]; then
    notes=""
    [ -z "${CENSUS_API_KEY:-}" ] && notes="Census demographics unavailable"
    [ -z "${HUD_API_TOKEN:-}" ] && notes="${notes:+$notes; }ZIP crosswalk unavailable"
    if [ -z "$notes" ]; then
        report_server "geo-demographics" "READY" "6 tools, all keys present"
    else
        report_server "geo-demographics" "DEGRADED" "$notes"
    fi
else
    report_server "geo-demographics" "DEGRADED" "CENSUS_API_KEY and HUD_API_TOKEN missing"
fi

if [ -n "${ORS_API_KEY:-}" ]; then
    report_server "drive-time" "READY" "5 tools, isochrones enabled"
else
    report_server "drive-time" "DEGRADED" "4/5 tools work; isochrones need ORS_API_KEY"
fi

if [ -n "${SEC_USER_AGENT:-}" ]; then
    report_server "financial-intelligence" "READY" "6 tools, SEC user agent set"
else
    report_server "financial-intelligence" "DEGRADED" "Set SEC_USER_AGENT to your app + email"
fi

if [ -n "${BLS_API_KEY:-}" ]; then
    report_server "workforce-analytics" "READY" "7 tools, BLS v2 API enabled"
else
    report_server "workforce-analytics" "DEGRADED" "BLS limited to v1 (fewer requests/day)"
fi

pr_notes=""
[ -z "${SAM_GOV_API_KEY:-}" ] && pr_notes="SAM.gov search unavailable"
[ -z "${CHPL_API_KEY:-}" ] && pr_notes="${pr_notes:+$pr_notes; }CHPL EHR lookup unavailable"
if [ -z "$pr_notes" ]; then
    report_server "public-records" "READY" "6 tools, all keys present"
elif [ -n "${SAM_GOV_API_KEY:-}" ] || [ -n "${CHPL_API_KEY:-}" ]; then
    report_server "public-records" "DEGRADED" "$pr_notes"
else
    report_server "public-records" "DEGRADED" "4/6 tools work; SAM + CHPL keys missing"
fi

wi_notes=""
if [ -n "${GOOGLE_CSE_API_KEY:-}" ] && [ -n "${GOOGLE_CSE_ID:-}" ]; then
    report_server "web-intelligence" "READY" "5 tools, web search enabled"
else
    report_server "web-intelligence" "DEGRADED" "Needs GOOGLE_CSE_API_KEY + GOOGLE_CSE_ID"
fi

# ---------------------------------------------------------------------------
# Step 6: Optional smoke test
# ---------------------------------------------------------------------------
header "Step 6: Smoke test (optional)"

echo ""
read -rp "Run a quick smoke test to verify servers start? [y/N] " run_smoke

if [[ "${run_smoke:-N}" =~ ^[Yy] ]]; then
    info "Testing server imports..."

    IMPORT_PASS=0
    IMPORT_FAIL=0

    for server_dir in "$PROJECT_ROOT"/servers/*/; do
        server_name=$(basename "$server_dir")
        # Convert hyphen/underscore dir name to Python module path
        module_name=$(echo "$server_name" | tr '-' '_')

        result=$(python3 -c "import servers.${module_name}.server" 2>&1) && {
            ok "servers.${module_name}.server imports cleanly"
            IMPORT_PASS=$((IMPORT_PASS + 1))
        } || {
            fail "servers.${module_name}.server failed to import"
            echo "    $result" | head -3
            IMPORT_FAIL=$((IMPORT_FAIL + 1))
        }
    done

    echo ""
    info "Import results: $IMPORT_PASS passed, $IMPORT_FAIL failed"

    if [ "$IMPORT_FAIL" -eq 0 ]; then
        ok "All 13 servers import successfully"
    else
        warn "Some servers failed to import. Check the errors above."
    fi
else
    info "Skipping smoke test."
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
header "Setup complete"

READY_COUNT=$(grep -c "READY" <<< "$(
    [ -n "${CENSUS_API_KEY:-}" ] && [ -n "${HUD_API_TOKEN:-}" ] && echo "READY" || echo "DEGRADED"
    [ -n "${ORS_API_KEY:-}" ] && echo "READY" || echo "DEGRADED"
    [ -n "${BLS_API_KEY:-}" ] && echo "READY" || echo "DEGRADED"
    echo "READY"; echo "READY"; echo "READY"; echo "READY"; echo "READY"; echo "READY"; echo "READY"
    [ -n "${GOOGLE_CSE_API_KEY:-}" ] && [ -n "${GOOGLE_CSE_ID:-}" ] && echo "READY" || echo "DEGRADED"
    [ -n "${SAM_GOV_API_KEY:-}" ] && [ -n "${CHPL_API_KEY:-}" ] && echo "READY" || echo "DEGRADED"
    [ -n "${SEC_USER_AGENT:-}" ] && echo "READY" || echo "DEGRADED"
)" 2>/dev/null || echo "7")

echo "  $READY_COUNT/13 servers at full capacity"
echo ""
echo "  Next steps:"
echo "    1. Activate the venv:  source .venv/bin/activate"
echo "    2. Run a server:       python -m servers.cms_facility.server"
echo "    3. Or use Docker:      docker compose up -d"
echo "    4. Configure your MCP client (see README.md)"
echo ""
