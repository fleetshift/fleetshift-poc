#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Start the FleetShift all-in-one stack. Called by 'task podman:up'.
#
# One container (deploy/aio) runs the TLS edge, API, UI, and peer Dex under s6.
# Auth is selected inside the container from the root .env, which compose ingests:
#   - OIDC_ISSUER_URL unset → built-in Dex at
#     https://fleetshift-sandbox.localhost:8085/idp
#   - OIDC_ISSUER_URL set   → that external issuer; peer Dex parks and never serves
#
# Env vars (COMPOSE_FILES, DEV, BUILD, PODMAN_SOCKET) are set by the Taskfile.

ensure_podman_ready

podman network exists kind 2>/dev/null || podman network create kind

if [ "${DEV:-}" = "true" ] || [ "${BUILD:-}" = "true" ]; then
  echo "==> Building all-in-one image from source (task image:aio)"
  (cd "$ROOT_DIR" && task image:aio)
fi

echo "==> Starting FleetShift stack"
UP_ARGS=(-d)
if [ "${DEV:-}" = "true" ] || [ "${BUILD:-}" = "true" ]; then
  UP_ARGS+=(--build)
fi
compose up "${UP_ARGS[@]}"

http_port="${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"
public_origin="https://fleetshift-sandbox.localhost:${http_port}"

echo ""
echo "==> FleetShift stack is running!"
echo "    FleetShift:      ${public_origin}  (opens /app after the certificate warning)"

if [ -z "${OIDC_ISSUER_URL:-}" ]; then
  # Dex-on: copy the sandbox CA so fleetctl can trust the public issuer.
  # The browser needs no host CA install — accept the top-level warning once.
  echo "==> Copying Dex sandbox CA to .certs/ca.crt (for fleetctl)"
  if ! copy_sandbox_ca; then
    echo "    WARN: sandbox CA not ready yet. Copy it later with:" >&2
    echo "      podman compose cp fleetshift-server:/data/sandbox/pki/ca.crt deploy/podman/.certs/ca.crt" >&2
  fi

  cat <<EOF

  Built-in Dex sandbox IdP (no OIDC_ISSUER_URL set):
    Issuer:  ${public_origin}/idp
    Users:   ops@fleetshift.local / fleetshift-ops
             dev@fleetshift.local / fleetshift-dev

  Open ${public_origin} and accept the browser certificate warning
  (unknown sandbox CA). Dex is same-origin under /idp; port 5556 is not published.

  If this volume previously ran the old :5556 Dex issuer, reset it first:
    task pd:clean

  Configure fleetctl:
    bin/fleetctl auth setup \\
      --issuer-url ${public_origin}/idp \\
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
  echo "    Register ${public_origin}/app/auth/callback on that IdP."
fi

echo ""
echo "    Run 'task podman:logs' to tail container output."
echo "    Run 'task podman:status' to check container health."
echo "    Run 'task --list' to see all available commands."
