#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Start the FleetShift all-in-one stack. Called by 'task podman:up'.
#
# One container (deploy/aio) runs the API, UI, and peer Dex under s6. Auth is
# selected inside the container from the root .env, which compose ingests:
#   - OIDC_ISSUER_URL unset → built-in Dex sandbox IdP (https://127.0.0.1:5556/dex)
#   - OIDC_ISSUER_URL set   → that external issuer; peer Dex parks and never serves
#
# Env vars (COMPOSE_FILES, DEV, BUILD, PODMAN_SOCKET) are set by the Taskfile.

ensure_podman_ready

podman network exists kind 2>/dev/null || podman network create kind

if [ "${DEV:-}" = "true" ] || [ "${BUILD:-}" = "true" ]; then
  echo "==> Building all-in-one image from source (task image:aio)"
  (cd "$ROOT_DIR" && task image:aio)
  # Drop cached web assets so web-builder repopulates from the fresh image.
  podman volume rm -f web-assets podman_web-assets 2>/dev/null || true
fi

echo "==> Starting FleetShift stack"
UP_ARGS=(-d)
if [ "${DEV:-}" = "true" ] || [ "${BUILD:-}" = "true" ]; then
  UP_ARGS+=(--build)
fi
compose up "${UP_ARGS[@]}"

http_port="${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"

echo ""
echo "==> FleetShift stack is running!"
echo "    FleetShift:      http://localhost:${http_port}"

if [ -z "${OIDC_ISSUER_URL:-}" ]; then
  # Dex-on: copy the sandbox CA so fleetctl can trust the loopback issuer.
  mkdir -p "$COMPOSE_DIR/.certs"
  echo "==> Copying Dex sandbox CA to .certs/ca.crt (for fleetctl)"
  ca_deadline=$((SECONDS + 30))
  until compose cp fleetshift-server:/data/sandbox/pki/ca.crt "$COMPOSE_DIR/.certs/ca.crt" 2>/dev/null; do
    if (( SECONDS >= ca_deadline )); then
      echo "    WARN: sandbox CA not ready yet. Copy it later with:" >&2
      echo "      podman compose cp fleetshift-server:/data/sandbox/pki/ca.crt deploy/podman/.certs/ca.crt" >&2
      break
    fi
    sleep 1
  done

  cat <<EOF

  Built-in Dex sandbox IdP (no OIDC_ISSUER_URL set):
    Issuer:  https://127.0.0.1:5556/dex
    Users:   ops@fleetshift.local / fleetshift-ops
             dev@fleetshift.local / fleetshift-dev

  Configure fleetctl:
    bin/fleetctl auth setup \\
      --issuer-url https://127.0.0.1:5556/dex \\
      --client-id fleetshift-cli \\
      --key-enrollment-client-id fleetshift-signing \\
      --oidc-ca-file deploy/podman/.certs/ca.crt \\
      --scopes 'openid,profile,email,audience:server:client_id:fleetshift'
    bin/fleetctl auth login
EOF
else
  echo ""
  echo "    External OIDC issuer: ${OIDC_ISSUER_URL}"
  echo "    (peer Dex is parked; the container uses your external issuer)"
fi

echo ""
echo "    Run 'task podman:logs' to tail container output."
echo "    Run 'task podman:status' to check container health."
echo "    Run 'task --list' to see all available commands."
