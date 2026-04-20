#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load .env if present, fall back to template
if [ -f "$DEPLOY_DIR/.env" ]; then
  set -a; source "$DEPLOY_DIR/.env"; set +a
elif [ -f "$DEPLOY_DIR/.env.template" ]; then
  echo "No .env found, using .env.template defaults"
  set -a; source "$DEPLOY_DIR/.env.template"; set +a
else
  echo "ERROR: No .env or .env.template found in $DEPLOY_DIR" >&2
  exit 1
fi

NETWORK=fleetshift
REALM_JSON="${DEPLOY_DIR}/../keycloak/fleetshift-realm.json"

echo "==> Creating podman network: $NETWORK"
podman network exists "$NETWORK" 2>/dev/null || podman network create "$NETWORK"

echo "==> Creating volumes"
podman volume exists fleetshift-data 2>/dev/null || podman volume create fleetshift-data
podman volume exists fleetshift-mock-servers-db 2>/dev/null || podman volume create fleetshift-mock-servers-db
podman volume exists keycloak-certs 2>/dev/null || podman volume create keycloak-certs
podman volume exists ui-plugins-dist 2>/dev/null || podman volume create ui-plugins-dist

# ── Keycloak (demo mode) ──────────────────────────────────────────

if [ "${DEMO_MODE:-true}" = "true" ]; then
  echo "==> [init] Generating TLS certs"
  podman run --rm \
    --network "$NETWORK" \
    --name keycloak-certs \
    -v keycloak-certs:/certs \
    alpine:3 \
    /bin/sh -c '
      if [ -f /certs/keycloak.crt ]; then
        echo "TLS certs already exist, skipping generation"
        exit 0
      fi
      apk add --no-cache openssl > /dev/null
      echo "Generating CA..."
      openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout /certs/ca.key -out /certs/ca.crt \
        -days 3650 -subj "/CN=FleetShift Local CA" 2>/dev/null
      echo "Generating Keycloak server certificate..."
      openssl req -newkey rsa:2048 -nodes \
        -keyout /certs/keycloak.key -out /tmp/keycloak.csr \
        -subj "/CN=keycloak" 2>/dev/null
      openssl x509 -req -in /tmp/keycloak.csr \
        -CA /certs/ca.crt -CAkey /certs/ca.key -CAcreateserial \
        -out /certs/keycloak.crt -days 3650 \
        -extfile <(printf "subjectAltName=DNS:keycloak,DNS:localhost") 2>/dev/null
      rm -f /tmp/keycloak.csr /certs/ca.srl
      chmod 644 /certs/*.crt
      chmod 644 /certs/*.key
      echo "TLS certs generated in /certs/"
    '

  echo "==> Starting keycloak"
  podman run -d \
    --network "$NETWORK" \
    --name keycloak \
    -p "${KC_HTTPS_PORT}:${KC_HTTPS_PORT}" \
    -p "${KC_HTTP_PORT}:${KC_HTTP_PORT}" \
    -v keycloak-certs:/opt/keycloak/certs:ro \
    -v "${REALM_JSON}:/opt/keycloak/data/import/fleetshift-realm.json:ro" \
    -e KC_BOOTSTRAP_ADMIN_USERNAME="${KC_BOOTSTRAP_ADMIN_USERNAME}" \
    -e KC_BOOTSTRAP_ADMIN_PASSWORD="${KC_BOOTSTRAP_ADMIN_PASSWORD}" \
    -e KC_HTTP_ENABLED=true \
    -e KC_HTTP_PORT="${KC_HTTP_PORT}" \
    -e KC_HTTPS_PORT="${KC_HTTPS_PORT}" \
    -e KC_HTTPS_CERTIFICATE_FILE=/opt/keycloak/certs/keycloak.crt \
    -e KC_HTTPS_CERTIFICATE_KEY_FILE=/opt/keycloak/certs/keycloak.key \
    -e KC_HTTP_RELATIVE_PATH=/auth \
    -e KC_HOSTNAME="${KC_HOSTNAME}" \
    -e KC_HOSTNAME_PORT="${KC_HTTP_PORT}" \
    -e KC_HOSTNAME_STRICT=false \
    quay.io/keycloak/keycloak:26.2 \
    start-dev --import-realm

  echo "    Waiting for keycloak to be healthy..."
  until podman exec keycloak bash -c 'exec 3<>/dev/tcp/localhost/'"${KC_HTTP_PORT}" 2>/dev/null; do
    sleep 2
  done
  echo "    Keycloak is ready."
fi

# ── FleetShift Server ─────────────────────────────────────────────

echo "==> Starting fleetshift-server"
podman run -d \
  --network "$NETWORK" \
  --name fleetshift-server \
  -p "${FLEETSHIFT_SERVER_HTTP_PORT}:${FLEETSHIFT_SERVER_HTTP_PORT}" \
  -p "${FLEETSHIFT_SERVER_GRPC_PORT}:${FLEETSHIFT_SERVER_GRPC_PORT}" \
  -v fleetshift-data:/data \
  -v keycloak-certs:/certs:ro \
  -v /run/user/"$(id -u)"/podman/podman.sock:/var/run/docker.sock \
  -v /tmp:/tmp \
  -e CONTAINER_HOST=keycloak \
  -e OIDC_HTTPS_PORT="${KC_HTTPS_PORT}" \
  -e KIND_EXPERIMENTAL_DOCKER_NETWORK="$NETWORK" \
  "${IMAGE_REGISTRY}/fleetshift-server:${IMAGE_TAG}" \
  serve \
    --http-addr=":${FLEETSHIFT_SERVER_HTTP_PORT}" \
    --grpc-addr=":${FLEETSHIFT_SERVER_GRPC_PORT}" \
    --db=/data/fleetshift.db \
    --log-level="${FLEETSHIFT_LOG_LEVEL}" \
    --oidc-ca-file=/certs/ca.crt

echo "    Waiting for fleetshift-server to be healthy..."
until curl -sf "http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT}/v1/deployments" >/dev/null 2>&1; do
  sleep 2
done
echo "    FleetShift server is ready."

# ── Auth Setup (init) ─────────────────────────────────────────────

if [ "${DEMO_MODE:-true}" = "true" ]; then
  echo "==> [init] Registering OIDC auth method"
  podman run --rm \
    --network "$NETWORK" \
    --name auth-setup \
    "${IMAGE_REGISTRY}/fleetshift-server:${IMAGE_TAG}" \
    /bin/sh -c '
      fleetctl auth setup \
        --server fleetshift-server:'"${FLEETSHIFT_SERVER_GRPC_PORT}"' \
        --issuer-url '"${OIDC_ISSUER_URL}"' \
        --client-id '"${OIDC_CLIENT_ID}"' \
        --audience '"${OIDC_AUDIENCE}"' \
      && echo "Auth method registered." \
      || echo "Auth setup failed (may already exist) — continuing."
    '
fi

# ── Mock UI Plugins ───────────────────────────────────────────────

echo "==> Starting fleetshift-mock-ui-plugins"
podman run -d \
  --network "$NETWORK" \
  --name fleetshift-mock-ui-plugins \
  -p 8001:8001 \
  -v ui-plugins-dist:/opt/app-root/src/packages/mock-ui-plugins/dist \
  "${IMAGE_REGISTRY}/fleetshift-mock-ui-plugins:${IMAGE_TAG}" \
  npm run serve:dist -w packages/mock-ui-plugins

echo "    Waiting for fleetshift-mock-ui-plugins..."
until podman exec fleetshift-mock-ui-plugins bash -c 'exec 3<>/dev/tcp/localhost/8001' 2>/dev/null; do
  sleep 2
done
echo "    Mock UI plugins ready."

# ── Mock Servers ──────────────────────────────────────────────────

echo "==> Starting fleetshift-mock-servers"
podman run -d \
  --network "$NETWORK" \
  --name fleetshift-mock-servers \
  -p 4000:4000 \
  -v fleetshift-mock-servers-db:/opt/app-root/src/packages/mock-servers/data \
  -v ui-plugins-dist:/opt/app-root/src/packages/mock-ui-plugins/dist:ro \
  -v keycloak-certs:/certs:ro \
  -e MODE="${MODE}" \
  -e MANAGEMENT_API_TARGET="${MANAGEMENT_API_TARGET}" \
  -e KEYCLOAK_URL="${KEYCLOAK_URL}" \
  -e K8S_TLS_INSECURE="${K8S_TLS_INSECURE}" \
  -e NODE_EXTRA_CA_CERTS=/certs/ca.crt \
  "${IMAGE_REGISTRY}/fleetshift-mock-servers:${IMAGE_TAG}"

echo "    Waiting for fleetshift-mock-servers..."
until podman exec fleetshift-mock-servers bash -c 'exec 3<>/dev/tcp/localhost/4000' 2>/dev/null; do
  sleep 2
done
echo "    Mock servers ready."

# ── GUI ───────────────────────────────────────────────────────────

echo "==> Starting fleetshift-gui"
podman run -d \
  --network "$NETWORK" \
  --name fleetshift-gui \
  -p 3000:3000 \
  -v keycloak-certs:/certs:ro \
  -e API_PROXY_TARGET="${API_PROXY_TARGET}" \
  -e KEYCLOAK_URL="${KEYCLOAK_URL}" \
  -e NODE_EXTRA_CA_CERTS=/certs/ca.crt \
  "${IMAGE_REGISTRY}/fleetshift-gui:${IMAGE_TAG}"

echo ""
echo "==> FleetShift stack is running!"
echo "    GUI:             http://localhost:3000"
echo "    Mock API:        http://localhost:4000"
echo "    FleetShift API:  http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT}"
echo "    Mock Plugins:    http://localhost:8001"
if [ "${DEMO_MODE:-true}" = "true" ]; then
  echo "    Keycloak Admin:  https://keycloak:${KC_HTTPS_PORT}"
  echo "    Keycloak (HTTP): http://localhost:${KC_HTTP_PORT}"
fi
