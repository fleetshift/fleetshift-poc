#!/usr/bin/env bash
set -euo pipefail

# Generate TLS certificates for the local Keycloak instance using mkcert.
# Called by 'task cert-init'. Skips if certs already exist (idempotent).

CERT_DIR="$(cd "$(dirname "$0")/.." && pwd)/.certs"

if [ -f "$CERT_DIR/keycloak.crt" ]; then
  echo "TLS certs already exist in $CERT_DIR, skipping generation"
  exit 0
fi

mkdir -p "$CERT_DIR"

CAROOT="$(mkcert -CAROOT)"
if [ ! -f "$CAROOT/rootCA-key.pem" ]; then
  echo "==> CA key missing — removing old CA and generating fresh one..."
  mkcert -uninstall 2>/dev/null || true
  rm -f "$CAROOT/rootCA.pem" "$CAROOT/rootCA-key.pem"
fi

echo "==> Installing CA into system trust store..."
mkcert -install

echo "==> Generating Keycloak certificate..."
mkcert -key-file "$CERT_DIR/keycloak.key" \
       -cert-file "$CERT_DIR/keycloak.crt" \
       keycloak

echo "==> Copying CA cert for container mounting..."
cp "$(mkcert -CAROOT)/rootCA.pem" "$CERT_DIR/ca.crt"

echo "==> Deleting CA private key..."
rm -f "$(mkcert -CAROOT)/rootCA-key.pem"

chmod 644 "$CERT_DIR/keycloak.key"
chmod 644 "$CERT_DIR/keycloak.crt" "$CERT_DIR/ca.crt"

echo "TLS certs generated in $CERT_DIR/"
echo "CA private key has been deleted — regenerate with 'task cert-init' if needed."
