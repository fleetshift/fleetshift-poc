#!/usr/bin/env bash
set -euo pipefail

# Generate TLS certificates for the local Keycloak instance using mkcert.
# Called by 'task podman:cert-init'. Skips if certs already exist (idempotent).

CERT_DIR="$(cd "$(dirname "$0")/.." && pwd)/.certs"
CERT_FILE="$CERT_DIR/keycloak.crt"
KEY_FILE="$CERT_DIR/keycloak.key"
CA_FILE="$CERT_DIR/ca.crt"

mkdir -p "$CERT_DIR"

CAROOT="$(mkcert -CAROOT)"
CAROOT_CERT="$CAROOT/rootCA.pem"
CAROOT_KEY="$CAROOT/rootCA-key.pem"

echo "==> Installing CA into system trust store..."
mkcert -install

if [ -f "$CERT_FILE" ] && [ -f "$KEY_FILE" ] && [ -f "$CA_FILE" ]; then
  if [ -f "$CAROOT_CERT" ] && cmp -s "$CA_FILE" "$CAROOT_CERT"; then
    echo "TLS certs already exist in $CERT_DIR, skipping generation"
    exit 0
  fi
  echo "==> Existing cert bundle does not match current mkcert CA — regenerating..."
fi

if [ ! -f "$CAROOT_KEY" ]; then
  echo "==> CA key missing — removing old CA and generating fresh one..."
  mkcert -uninstall 2>/dev/null || true
  rm -f "$CAROOT_CERT" "$CAROOT_KEY"
  echo "==> Installing CA into system trust store..."
  mkcert -install
fi

echo "==> Generating Keycloak certificate..."
mkcert -key-file "$KEY_FILE" \
       -cert-file "$CERT_FILE" \
       keycloak

echo "==> Copying CA cert for container mounting..."
cp "$CAROOT_CERT" "$CA_FILE"

echo "==> Deleting CA private key..."
rm -f "$CAROOT_KEY"

chmod 644 "$KEY_FILE" "$CERT_FILE" "$CA_FILE"

echo "TLS certs generated in $CERT_DIR/"
