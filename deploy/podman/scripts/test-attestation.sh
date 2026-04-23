#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# ------------------------------------------------------------------
# FleetShift Attestation Flow — End-to-End Test
#
# Tests the full signing and attestation pipeline:
#   1. Configures CLI auth
#   2. Logs in via browser (PKCE flow)
#   3. Enrolls signing key
#   4. Pauses for user to add SSH key to GitHub
#   5. Creates a signed kind cluster deployment
#   6. Deploys a signed ConfigMap to the managed cluster
#
# Prerequisites:
#   - Stack running (make up)
#   - fleetctl built (make build in repo root)
#
# Usage:
#   ./test-attestation.sh
#   ./test-attestation.sh --headless
#   ./test-attestation.sh --headless --reuse-key
# ------------------------------------------------------------------

load_env

HEADLESS=false
REUSE_KEY=false
FLEETCTL="${DEPLOY_DIR}/../bin/fleetctl"

for arg in "$@"; do
  case "$arg" in
    --headless)  HEADLESS=true ;;
    --reuse-key) REUSE_KEY=true ;;
  esac
done

log()  { printf '\n\033[1;34m>>> %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# --- Pre-flight checks -----------------------------------------------

[ -x "$FLEETCTL" ] || die "fleetctl not found at ${FLEETCTL}. Run 'make build' in the repo root."

GITHUB_USERNAME="${DEV_USER_GITHUB:-}"
if [ -z "$GITHUB_USERNAME" ]; then
  read -rp "  GitHub username: " GITHUB_USERNAME
  [ -n "$GITHUB_USERNAME" ] || die "GitHub username is required for attestation"
fi

log "Checking FleetShift server is reachable"
if ! curl -sf "http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT:-8085}/v1/deployments" >/dev/null 2>&1; then
  die "FleetShift server not reachable on :${FLEETSHIFT_SERVER_HTTP_PORT:-8085}. Run 'make up' first."
fi
echo "  Server is up."

KC_URL="http://${KC_HOSTNAME:-localhost}:${KC_HTTP_PORT:-8180}/auth"
log "Checking Keycloak is reachable"
if ! curl -sf "$KC_URL/realms/fleetshift" >/dev/null 2>&1; then
  die "Keycloak not reachable at ${KC_URL}. Run 'make up' first."
fi
echo "  Keycloak is up."

# --- Headless keyring unlock ------------------------------------------

if $HEADLESS; then
  command -v dbus-launch >/dev/null || die "dbus-x11 not installed. Run: apt-get install -y dbus-x11 gnome-keyring"
  command -v gnome-keyring-daemon >/dev/null || die "gnome-keyring not installed. Run: apt-get install -y dbus-x11 gnome-keyring"
  log "Unlocking keyring for headless environment"
  eval "$(dbus-launch --sh-syntax)"
  echo "" | gnome-keyring-daemon --unlock --components=secrets
fi

# --- Configure CLI auth -----------------------------------------------

log "Configuring CLI auth"
"$SCRIPT_DIR/cli-setup.sh"

# --- Login -------------------------------------------------------------

log "Logging in (opens browser)"
if [ -n "${DEV_USER_USERNAME:-}" ]; then
  echo "  Use credentials: ${DEV_USER_USERNAME} / ${DEV_USER_PASSWORD:-<your password>}"
fi
"$FLEETCTL" auth login

# --- Enroll signing key ------------------------------------------------

log "Enrolling signing key"
echo "  This uses the 'fleetshift-signing' OIDC client for purpose-scoped enrollment tokens."
if [ -n "${DEV_USER_USERNAME:-}" ]; then
  echo "  Log in with the same credentials: ${DEV_USER_USERNAME} / ${DEV_USER_PASSWORD:-<your password>}"
fi

if $REUSE_KEY; then
  echo "  --reuse-key: reusing existing signing key from keyring"
  "$FLEETCTL" auth enroll-signing --reuse-key
else
  "$FLEETCTL" auth enroll-signing

  echo ""
  echo "  ================================================================"
  echo "  ACTION REQUIRED: Add your SSH signing key to GitHub"
  echo "  ================================================================"
  echo ""
  echo "  The public key was printed above by enroll-signing."
  echo "  Copy it, then:"
  echo ""
  echo "    1. Go to https://github.com/settings/keys"
  echo "    2. Click 'New SSH key', set 'Key type' to 'Signing Key'"
  echo "    3. Paste the public key and save"
  echo ""
  echo "  Why: Delivery agents fetch your public key directly from GitHub"
  echo "  at verification time. The platform never holds your public key."
  echo ""
  echo "  Registry will resolve: github.com/users/${GITHUB_USERNAME}/ssh_signing_keys"
  echo ""

  read -rp "  Have you added the SSH signing key to GitHub? [yes/no] " confirm
  [ "$confirm" = "yes" ] || die "Add the signing key to GitHub first, then re-run with --reuse-key."
fi

# --- Create kind cluster (signed) -------------------------------------

log "Creating signed kind cluster deployment"
echo '{"name": "my-oidc-cluster"}' | "$FLEETCTL" deployment create \
    --id my-oidc-cluster \
    --manifest-file - \
    --resource-type api.kind.cluster \
    --placement-type static \
    --target-ids kind-local \
    --sign

log "Waiting for kind cluster to become active..."
MAX_WAIT=120
ELAPSED=0
while true; do
  STATE=$("$FLEETCTL" deployment get my-oidc-cluster -ojson 2>/dev/null \
    | jq -r '.state // empty')
  if [ "$STATE" = "STATE_ACTIVE" ]; then
    break
  fi
  sleep 5
  ELAPSED=$((ELAPSED + 5))
  if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
    die "Kind cluster not active within ${MAX_WAIT}s (state: ${STATE})"
  fi
  printf "  %ds — state: %s\n" "$ELAPSED" "${STATE:-unknown}"
done
log "Kind cluster is active"

# --- Deploy ConfigMap (signed) ----------------------------------------

log "Deploying signed ConfigMap to managed cluster"
echo '{
  "apiVersion": "v1",
  "kind": "ConfigMap",
  "metadata": {"name": "test-config", "namespace": "default"},
  "data": {"key": "value"}
}' | "$FLEETCTL" deployment create \
    --id configmap-my-oidc-cluster \
    --manifest-file - \
    --resource-type kubernetes \
    --placement-type static \
    --target-ids k8s-my-oidc-cluster \
    --sign

# --- Done --------------------------------------------------------------

log "Attestation flow complete"
echo ""
echo "  Signed deployments created:"
echo "    - my-oidc-cluster (kind cluster)"
echo "    - configmap-my-oidc-cluster (ConfigMap)"
echo ""
echo "  Attestation chain:"
echo "    CLI signs deployment intent (ECDSA P-256)"
echo "    → Server verifies against GitHub SSH keys"
echo "    → Server builds SignerAssertion (registry pointer)"
echo "    → Delivery agent independently re-verifies"
echo "      by fetching keys from GitHub (not the platform)"
echo ""
echo "  Registry: https://api.github.com/users/${GITHUB_USERNAME}/ssh_signing_keys"
echo ""
