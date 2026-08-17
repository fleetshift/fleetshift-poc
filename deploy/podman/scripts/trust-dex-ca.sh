#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "$0")" && pwd)/common.sh"

# Trust the built-in Dex sandbox CA so a browser accepts the loopback OIDC
# issuer at https://127.0.0.1:5556/dex without the manual "accept unsafe
# certificate" click. This is the browser-side equivalent of fleetctl's
# --oidc-ca-file (see start.sh).
#
# The sandbox CA is copied to deploy/podman/.certs/ca.crt by start.sh; this
# script installs that CA into the OS trust store:
#   - macOS: login keychain, trusted for SSL only (no sudo; one GUI password
#            prompt). Idempotent — skips if the same CA is already trusted.
#   - Linux: prints manual instructions (distro trust stores vary too much to
#            automate safely).
#
# Usage:
#   ./scripts/trust-dex-ca.sh            # trust the sandbox CA
#   ./scripts/trust-dex-ca.sh --remove   # untrust / remove it

CA_CERT="$COMPOSE_DIR/.certs/ca.crt"
CA_CN="FleetShift Sandbox CA"

# ensure_ca_present makes sure .certs/ca.crt exists, copying it out of the
# running server container if it's missing.
ensure_ca_present() {
  [ -f "$CA_CERT" ] && return 0
  echo "==> .certs/ca.crt missing; copying it from the running server container"
  ensure_podman_ready
  if ! copy_sandbox_ca; then
    echo "ERROR: could not copy the sandbox CA. Is the stack running (task pd:up)?" >&2
    echo "  Expected: fleetshift-server:/data/sandbox/pki/ca.crt" >&2
    exit 1
  fi
}

# ca_fingerprint prints the SHA-1 fingerprint of the on-disk CA, hex, no colons,
# uppercased — matching the format `security find-certificate -Z` reports.
ca_fingerprint() {
  openssl x509 -in "$CA_CERT" -noout -fingerprint -sha1 \
    | sed 's/.*=//; s/://g' | tr 'a-f' 'A-F'
}

# macos_already_trusted reports whether a cert with our CN and the exact same
# fingerprint is already in the login keychain (so a rotated CA re-installs).
macos_already_trusted() {
  local want; want="$(ca_fingerprint)"
  security find-certificate -a -c "$CA_CN" -Z login.keychain-db 2>/dev/null \
    | awk '/SHA-1 hash:/ {print $3}' | tr 'a-f' 'A-F' | grep -qx "$want"
}

trust_macos() {
  ensure_ca_present
  if macos_already_trusted; then
    echo "==> Dex sandbox CA already trusted in the login keychain. Nothing to do."
    return 0
  fi
  echo "==> Adding Dex sandbox CA to the login keychain, trusted for SSL"
  echo "    (macOS will prompt for your login password)"
  security add-trusted-cert -r trustRoot -p ssl \
    -k "$HOME/Library/Keychains/login.keychain-db" "$CA_CERT"
  echo "==> Done. Fully quit and reopen your browser, then log in at" \
       "http://localhost:${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"
}

remove_macos() {
  echo "==> Removing Dex sandbox CA ('$CA_CN') from the login keychain"
  # delete-certificate removes one match per call; loop until none remain.
  while security find-certificate -c "$CA_CN" login.keychain-db &>/dev/null; do
    security delete-certificate -c "$CA_CN" login.keychain-db &>/dev/null || break
  done
  echo "==> Done."
}

trust_linux() {
  ensure_ca_present
  cat >&2 <<EOF
Automatic trust on Linux is distro-specific. Install the CA manually:

  System store (curl, fleetctl, and Chromium read this):
    # Fedora / RHEL:
    sudo cp "$CA_CERT" /etc/pki/ca-trust/source/anchors/fleetshift-sandbox-ca.crt
    sudo update-ca-trust
    # Debian / Ubuntu:
    sudo cp "$CA_CERT" /usr/local/share/ca-certificates/fleetshift-sandbox-ca.crt
    sudo update-ca-certificates

  Firefox (its own NSS store):
    certutil -A -n "$CA_CN" -t "C,," -i "$CA_CERT" \\
      -d sql:\$HOME/.mozilla/firefox/<your-profile>
EOF
}

main() {
  local action="trust"
  [ "${1:-}" = "--remove" ] && action="remove"

  case "$(uname -s)" in
    Darwin)
      if [ "$action" = "remove" ]; then remove_macos; else trust_macos; fi
      ;;
    Linux)
      if [ "$action" = "remove" ]; then
        echo "Remove the CA from wherever you installed it (see trust output)." >&2
      else
        trust_linux
      fi
      ;;
    *)
      echo "Unsupported OS: $(uname -s). Trust $CA_CERT in your browser manually." >&2
      exit 1
      ;;
  esac
}

main "$@"
