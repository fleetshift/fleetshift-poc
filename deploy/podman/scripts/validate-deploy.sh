#!/usr/bin/env bash
set -euo pipefail

# ══════════════════════════════════════════════════════════════
# FleetShift Deploy Validation — Full Test Suite
# ══════════════════════════════════════════════════════════════
#
# Validates all deployment combinations, state file persistence,
# preconditions, and the end-to-end attestation flow.
#
# Usage:
#   ./validate-deploy.sh              # run all phases
#   ./validate-deploy.sh --from 3     # resume from phase 3
#   ./validate-deploy.sh --phase 5    # run only phase 5
#
# ══════════════════════════════════════════════════════════════

cd "$(git rev-parse --show-toplevel)"

PASSED=0
FAILED=0
SKIPPED=0
FAILURES=()
STATE_FILE="deploy/podman/.active-deploy"
AUTH_JSON="$HOME/.config/fleetshift/auth.json"
FROM_PHASE=0
ONLY_PHASE=""

for arg in "$@"; do
  case "$arg" in
    --from)   shift; FROM_PHASE="$1"; shift ;;
    --phase)  shift; ONLY_PHASE="$1"; shift ;;
  esac
done

# ── Helpers ──────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

phase_header() {
  local num="$1"; shift
  echo ""
  echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════════${NC}"
  echo -e "${BOLD}${CYAN}  PHASE ${num}: $*${NC}"
  echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════════════${NC}"
  echo ""
}

should_run() {
  local phase="$1"
  if [ -n "$ONLY_PHASE" ]; then
    [ "$phase" = "$ONLY_PHASE" ]
  else
    [ "$phase" -ge "$FROM_PHASE" ]
  fi
}

pass() {
  echo -e "  ${GREEN}PASS${NC}: $1"
  PASSED=$((PASSED + 1))
}

fail() {
  echo -e "  ${RED}FAIL${NC}: $1"
  FAILED=$((FAILED + 1))
  FAILURES+=("$1")
}

skip() {
  echo -e "  ${YELLOW}SKIP${NC}: $1"
  SKIPPED=$((SKIPPED + 1))
}

assert_file_exists() {
  if [ -f "$1" ]; then pass "$2"; else fail "$2 — file not found: $1"; fi
}

assert_file_missing() {
  if [ ! -f "$1" ]; then pass "$2"; else fail "$2 — file should not exist: $1"; fi
}

assert_file_contains() {
  local file="$1" pattern="$2" desc="$3"
  if grep -q "$pattern" "$file" 2>/dev/null; then pass "$desc"; else fail "$desc — expected '$pattern' in $file"; fi
}

assert_output_contains() {
  local output="$1" pattern="$2" desc="$3"
  if echo "$output" | grep -q "$pattern"; then pass "$desc"; else fail "$desc — expected '$pattern' in output"; fi
}

assert_output_not_contains() {
  local output="$1" pattern="$2" desc="$3"
  if ! echo "$output" | grep -q "$pattern"; then pass "$desc"; else fail "$desc — did NOT expect '$pattern' in output"; fi
}

assert_json_field() {
  local file="$1" field="$2" expected="$3" desc="$4"
  local actual
  actual=$(jq -r "$field" "$file" 2>/dev/null)
  if [ "$actual" = "$expected" ]; then
    pass "$desc"
  else
    fail "$desc — expected '$expected', got '$actual'"
  fi
}

pause_for_user() {
  echo ""
  echo -e "${YELLOW}  ⏸  $1${NC}"
  read -rp "  Press ENTER to continue (or ctrl-C to abort)... "
  echo ""
}

kill_tree() {
  local pid=$1
  for child in $(pgrep -P "$pid" 2>/dev/null); do
    kill_tree "$child"
  done
  kill "$pid" 2>/dev/null || true
}

ensure_down() {
  echo "  Ensuring stack is clean (removing volumes for fresh state)..."
  task pd:clean 2>/dev/null || true
  rm -f "$STATE_FILE"
}

# ── Pre-flight ───────────────────────────────────────────────

echo -e "${BOLD}FleetShift Deploy Validation${NC}"
echo "Working directory: $(pwd)"
echo ""

if ! command -v task &>/dev/null; then
  echo -e "${RED}ERROR: 'task' not found. Install go-task first.${NC}"
  exit 1
fi

if ! command -v jq &>/dev/null; then
  echo -e "${RED}ERROR: 'jq' not found. Install jq first.${NC}"
  exit 1
fi

if [ ! -f .env ]; then
  echo -e "${RED}ERROR: .env not found. Copy .env.template to .env first.${NC}"
  exit 1
fi

if [ ! -f bin/fleetctl ]; then
  echo -e "${YELLOW}WARNING: bin/fleetctl not found. Run 'task build:all' first for attestation tests.${NC}"
fi

ensure_down

# ──────────────────────────────────────────────────────────────
# PHASE 0: Precondition validation (no containers)
# ──────────────────────────────────────────────────────────────

if should_run 0; then
  phase_header 0 "Precondition validation (no containers needed)"

  echo "0a. Taskfile parses correctly"
  OUTPUT=$(task --list 2>&1)
  assert_output_contains "$OUTPUT" "podman:test-attestation" "test-attestation task listed"
  assert_output_contains "$OUTPUT" "podman:cli-setup" "cli-setup task listed"

  echo ""
  echo "0b. Invalid AUTH rejected"
  OUTPUT=$(task pd:up AUTH=bogus 2>&1 || true)
  assert_output_contains "$OUTPUT" "AUTH must be local or external" "AUTH=bogus rejected"

  echo ""
  echo "0c. Invalid DB rejected"
  OUTPUT=$(task pd:up DB=mongo 2>&1 || true)
  assert_output_contains "$OUTPUT" "DB must be sqlite or postgres" "DB=mongo rejected"

  echo ""
  echo "0d. reset-keycloak rejected with external auth"
  OUTPUT=$(task pd:reset-keycloak AUTH=external 2>&1 || true)
  assert_output_contains "$OUTPUT" "reset-keycloak only works with AUTH=local" "reset-keycloak blocked for external"

  echo ""
  echo "0e. No state file — defaults are correct"
  rm -f "$STATE_FILE"
  OUTPUT=$(task pd:up --dry 2>&1)
  assert_output_contains "$OUTPUT" "AUTH=local" "default AUTH=local"
  assert_output_contains "$OUTPUT" "DB=sqlite" "default DB=sqlite"

  echo ""
  echo "0f. gcphcp renderer writes disabled placeholder"
  TMP_GCPHCP=$(mktemp)
  ./deploy/render-gcphcp-config.sh --output "$TMP_GCPHCP"
  assert_file_contains "$TMP_GCPHCP" "gcphcp is disabled for this deployment" "disabled gcphcp placeholder rendered"
  rm -f "$TMP_GCPHCP"

  echo ""
  echo "0g. gcphcp renderer writes enabled config"
  TMP_GCPHCP=$(mktemp)
  env \
    GCPHCP_ENABLED=true \
    GCPHCP_GATEWAY_URL="https://example.invalid" \
    GCPHCP_GATEWAY_AUDIENCE="example-audience" \
    GCPHCP_TARGET_ID="gcphcp-example" \
    GCPHCP_GCP_PROJECT="example-project" \
    GCPHCP_GCP_REGION="us-central1" \
    GCPHCP_WORKFORCE_POOL="example-pool" \
    GCPHCP_WORKFORCE_PROVIDER="example-provider" \
    GCPHCP_BROKER_SA_EMAIL="broker@example-project.iam.gserviceaccount.com" \
    ./deploy/render-gcphcp-config.sh --output "$TMP_GCPHCP"
  assert_file_contains "$TMP_GCPHCP" '^gateway:$' "enabled gcphcp gateway section rendered"
  assert_file_contains "$TMP_GCPHCP" 'gcp_project: "example-project"' "enabled gcphcp target rendered"
  rm -f "$TMP_GCPHCP"
fi

# ──────────────────────────────────────────────────────────────
# PHASE 1: Default demo mode (local auth + sqlite)
# ──────────────────────────────────────────────────────────────

if should_run 1; then
  phase_header 1 "Default demo mode (local auth + sqlite)"
  ensure_down

  echo "1a. Starting default stack..."
  task pd:up
  echo ""

  echo "1b. State file written"
  assert_file_exists "$STATE_FILE" "state file created"
  assert_file_contains "$STATE_FILE" "^AUTH=local$" "state: AUTH=local"
  assert_file_contains "$STATE_FILE" "^DB=sqlite$" "state: DB=sqlite"

  echo ""
  echo "1c. cli-setup picks up local auth (no args)"
  task pd:cli-setup
  assert_file_exists "$AUTH_JSON" "auth.json created"
  assert_json_field "$AUTH_JSON" ".issuer_url" "http://keycloak:8180/auth/realms/fleetshift" "issuer_url is local keycloak"
  assert_json_field "$AUTH_JSON" ".client_id" "fleetshift-cli" "client_id correct"
  assert_json_field "$AUTH_JSON" ".key_enrollment_client_id" "fleetshift-signing" "enrollment client correct"

  echo ""
  echo "1d. status shows correct containers (no args)"
  OUTPUT=$(task pd:status 2>&1)
  assert_output_contains "$OUTPUT" "keycloak" "keycloak container running (local auth)"

  echo ""
  echo "1e. logs works (no args) — checking for 3 seconds"
  task pd:logs &
  LOGS_PID=$!
  sleep 3
  kill_tree "$LOGS_PID"
  wait "$LOGS_PID" 2>/dev/null || true
  pass "logs streamed without error"

  echo ""
  echo "1f. reset-keycloak allowed with local auth (no args)"
  OUTPUT=$(task pd:reset-keycloak --dry 2>&1)
  assert_output_not_contains "$OUTPUT" "precondition not met" "reset-keycloak allowed for local"

  echo ""
  echo "1g. Stopping stack"
  task pd:down
  assert_file_missing "$STATE_FILE" "state file removed by d:down"
fi

# ──────────────────────────────────────────────────────────────
# PHASE 2: Demo + external auth override
# ──────────────────────────────────────────────────────────────

if should_run 2; then
  phase_header 2 "Demo + external auth override"
  ensure_down

  echo "2a. Starting stack with AUTH=external..."
  task pd:up AUTH=external
  echo ""

  echo "2b. State file written"
  assert_file_contains "$STATE_FILE" "^AUTH=external$" "state: AUTH=external"
  assert_file_contains "$STATE_FILE" "^DB=sqlite$" "state: DB=sqlite"

  echo ""
  echo "2c. cli-setup picks up external auth (NO args)"
  task pd:cli-setup
  ISSUER=$(jq -r .issuer_url "$AUTH_JSON")
  if echo "$ISSUER" | grep -q "keycloak:8180"; then
    fail "issuer_url is local keycloak — state file not working"
  else
    pass "issuer_url is external: $ISSUER"
  fi

  echo ""
  echo "2d. status uses correct compose files (NO args)"
  OUTPUT=$(task pd:status 2>&1)
  assert_output_not_contains "$OUTPUT" "keycloak" "keycloak NOT running (external auth)"

  echo ""
  echo "2e. reset-keycloak blocked (NO args — reads state AUTH=external)"
  OUTPUT=$(task pd:reset-keycloak 2>&1 || true)
  assert_output_contains "$OUTPUT" "reset-keycloak only works with AUTH=local" "reset-keycloak blocked via state file"

  echo ""
  echo "2f. CLI override takes precedence over state file"
  OUTPUT=$(task pd:reset-keycloak --dry AUTH=local 2>&1 || true)
  assert_output_not_contains "$OUTPUT" "precondition not met" "CLI AUTH=local overrides state AUTH=external"

  echo ""
  echo "2g. Stopping stack"
  task pd:down
  assert_file_missing "$STATE_FILE" "state file removed by d:down"
fi

# ──────────────────────────────────────────────────────────────
# PHASE 3: Demo + postgres override
# ──────────────────────────────────────────────────────────────

if should_run 3; then
  phase_header 3 "Demo + postgres override"
  ensure_down

  echo "3a. Starting stack with DB=postgres..."
  task pd:up DB=postgres
  echo ""

  echo "3b. State file written"
  assert_file_contains "$STATE_FILE" "^AUTH=local$" "state: AUTH=local"
  assert_file_contains "$STATE_FILE" "^DB=postgres$" "state: DB=postgres"

  echo ""
  echo "3c. status shows correct containers (NO args)"
  OUTPUT=$(task pd:status 2>&1)
  assert_output_contains "$OUTPUT" "postgres" "postgres container running"
  assert_output_contains "$OUTPUT" "keycloak" "keycloak container running"

  echo ""
  echo "3d. Stopping stack"
  task pd:down
fi

# ──────────────────────────────────────────────────────────────
# PHASE 4: Demo + external auth + postgres
# ──────────────────────────────────────────────────────────────

if should_run 4; then
  phase_header 4 "Demo + external auth + postgres"
  ensure_down

  echo "4a. Starting stack with AUTH=external DB=postgres..."
  task pd:up AUTH=external DB=postgres
  echo ""

  echo "4b. State file written"
  assert_file_contains "$STATE_FILE" "^AUTH=external$" "state: AUTH=external"
  assert_file_contains "$STATE_FILE" "^DB=postgres$" "state: DB=postgres"

  echo ""
  echo "4c. cli-setup picks up external (NO args)"
  task pd:cli-setup
  ISSUER=$(jq -r .issuer_url "$AUTH_JSON")
  if echo "$ISSUER" | grep -q "keycloak:8180"; then
    fail "issuer_url is local — state not working"
  else
    pass "issuer_url is external: $ISSUER"
  fi

  echo ""
  echo "4d. status shows correct containers (NO args)"
  OUTPUT=$(task pd:status 2>&1)
  assert_output_contains "$OUTPUT" "postgres" "postgres container running"

  echo ""
  echo "4e. Stopping stack"
  task pd:down
fi

# ──────────────────────────────────────────────────────────────
# PHASE 5: Prod mode
# ──────────────────────────────────────────────────────────────

if should_run 5; then
  phase_header 5 "Prod mode (defaults to external + postgres)"
  ensure_down

  echo "5a. Starting stack in prod mode..."
  task pd:up DEPLOY_MODE=prod
  echo ""

  echo "5b. State file shows prod defaults"
  assert_file_contains "$STATE_FILE" "^AUTH=external$" "state: AUTH=external"
  assert_file_contains "$STATE_FILE" "^DB=postgres$" "state: DB=postgres"

  echo ""
  echo "5c. cli-setup uses external (NO args)"
  task pd:cli-setup
  ISSUER=$(jq -r .issuer_url "$AUTH_JSON")
  if echo "$ISSUER" | grep -q "keycloak:8180"; then
    fail "issuer_url is local — prod mode not resolving external"
  else
    pass "issuer_url is external: $ISSUER"
  fi

  echo ""
  echo "5d. Stopping stack"
  task pd:down
fi

# ──────────────────────────────────────────────────────────────
# PHASE 6: Rebuild preserves state
# ──────────────────────────────────────────────────────────────

if should_run 6; then
  phase_header 6 "Rebuild preserves state"
  ensure_down

  echo "6a. Starting with AUTH=external..."
  task pd:up AUTH=external
  assert_file_contains "$STATE_FILE" "^AUTH=external$" "initial state: AUTH=external"

  echo ""
  echo "6b. Rebuilding (stop + up cycle)..."
  task pd:rebuild
  echo ""

  echo "6c. State survived rebuild"
  assert_file_contains "$STATE_FILE" "^AUTH=external$" "state after rebuild: AUTH=external"

  echo ""
  echo "6d. cli-setup still works without args"
  task pd:cli-setup
  ISSUER=$(jq -r .issuer_url "$AUTH_JSON")
  if echo "$ISSUER" | grep -q "keycloak:8180"; then
    fail "issuer_url is local — rebuild broke state"
  else
    pass "issuer_url is external after rebuild: $ISSUER"
  fi

  echo ""
  echo "6e. Stopping stack"
  task pd:down
fi

# ──────────────────────────────────────────────────────────────
# PHASE 7: d:clean removes state
# ──────────────────────────────────────────────────────────────

if should_run 7; then
  phase_header 7 "d:clean removes state"
  ensure_down

  echo "7a. Starting with overrides..."
  task pd:up AUTH=external DB=postgres
  assert_file_exists "$STATE_FILE" "state file exists"

  echo ""
  echo "7b. Cleaning..."
  task pd:clean
  assert_file_missing "$STATE_FILE" "state file removed by d:clean"
fi

# ──────────────────────────────────────────────────────────────
# PHASE 8: Full attestation — local auth
# ──────────────────────────────────────────────────────────────

if should_run 8; then
  phase_header 8 "Full attestation flow — local auth"
  ensure_down

  if [ ! -f bin/fleetctl ]; then
    skip "fleetctl not built — run 'task build:all' first"
  else
    echo "8a. Starting default stack..."
    task pd:up
    echo ""

    echo "8b. Running test-attestation (local auth)..."
    echo ""
    echo -e "${YELLOW}  This is interactive — you will need to:${NC}"
    echo "    1. Log in via browser when prompted"
    echo "    2. Enroll signing key (another browser login)"
    echo "    3. Add SSH signing key to GitHub"
    echo "    4. Wait for kind cluster + configmap deployment"
    echo ""
    pause_for_user "Ready to start local auth attestation?"

    task pd:test-attestation
    pass "local auth attestation completed"

    echo ""
    echo "8c. Cleaning up for next attestation..."
    task pd:clean
  fi
fi

# ──────────────────────────────────────────────────────────────
# PHASE 9: Full attestation — external auth
# ──────────────────────────────────────────────────────────────

if should_run 9; then
  phase_header 9 "Full attestation flow — external auth"
  ensure_down

  if [ ! -f bin/fleetctl ]; then
    skip "fleetctl not built — run 'task build:all' first"
  else
    echo "9a. Starting stack with AUTH=external..."
    task pd:up AUTH=external
    echo ""

    echo "9b. Running test-attestation (external auth, NO args — reads state)..."
    echo ""
    echo -e "${YELLOW}  This is interactive — same steps as Phase 8.${NC}"
    echo "  Use --reuse-key if you already enrolled a signing key."
    echo ""
    pause_for_user "Ready to start external auth attestation?"

    task pd:test-attestation -- --reuse-key
    pass "external auth attestation completed"

    echo ""
    echo "9c. Cleaning up..."
    task pd:clean
  fi
fi

# ──────────────────────────────────────────────────────────────
# PHASE 10: Dev mode — source builds + hot-reload
# ──────────────────────────────────────────────────────────────

if should_run 10; then
  phase_header 10 "Dev mode — source builds + hot-reload"
  ensure_down

  # Dev mode builds UI containers from $UI_DIR (the fleetshift-user-interface repo)
  UI_DIR_VALUE=$(grep '^UI_DIR=' .env 2>/dev/null | cut -d= -f2 || true)
  UI_DIR_ABS=""
  if [ -n "$UI_DIR_VALUE" ]; then
    UI_DIR_ABS=$(cd deploy/podman && realpath "$UI_DIR_VALUE" 2>/dev/null || true)
  fi

  if [ -z "$UI_DIR_ABS" ] || [ ! -d "$UI_DIR_ABS" ]; then
    skip "10: UI_DIR not found (${UI_DIR_VALUE:-unset}) — dev mode requires the UI repo"
  else
    echo "10a. Starting in dev mode (builds from source)..."
    DEV_LOG=$(mktemp)
    task pd:dev 2>&1 | tee "$DEV_LOG"
    echo ""

    echo "10b. State file persists DEV=true"
    assert_file_contains "$STATE_FILE" "^AUTH=local$" "state: AUTH=local"
    assert_file_contains "$STATE_FILE" "^DB=sqlite$" "state: DB=sqlite"
    assert_file_contains "$STATE_FILE" "^DEV=true$" "state: DEV=true"

    echo ""
    echo "10c. Images built from source (not pulled)"
    if grep -qE 'STEP [0-9]+/' "$DEV_LOG"; then
      pass "build steps detected in compose output"
    else
      fail "no build steps in output — images may have been pulled instead of built"
    fi
    rm -f "$DEV_LOG"

    echo ""
    echo "10d. Follow-up status works via state file (no args)"
    OUTPUT=$(task pd:status 2>&1)
    assert_output_contains "$OUTPUT" "fleetshift-server" "status shows server container"

    echo ""
    echo "10e. Stopping stack"
    task pd:down
    assert_file_missing "$STATE_FILE" "state file removed by d:down"
  fi
fi

# ──────────────────────────────────────────────────────────────
# RESULTS
# ──────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}  RESULTS${NC}"
echo -e "${BOLD}══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${GREEN}Passed${NC}:  $PASSED"
echo -e "  ${RED}Failed${NC}:  $FAILED"
echo -e "  ${YELLOW}Skipped${NC}: $SKIPPED"

if [ ${#FAILURES[@]} -gt 0 ]; then
  echo ""
  echo -e "${RED}  Failures:${NC}"
  for f in "${FAILURES[@]}"; do
    echo -e "    - $f"
  done
fi

echo ""
if [ "$FAILED" -eq 0 ]; then
  echo -e "${GREEN}${BOLD}  ALL TESTS PASSED${NC}"
  exit 0
else
  echo -e "${RED}${BOLD}  $FAILED TEST(S) FAILED${NC}"
  exit 1
fi
