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
# In-container path of the Dex-on sandbox CA (see deploy/aio sandboxpki.go).
SANDBOX_CA_PATH="/data/sandbox/pki/ca.crt"
# Default AIO image; a container off it may be started by compose OR raw podman run.
AIO_IMAGE="${FLEETSHIFT_SERVER_IMAGE:-quay.io/stolostron/fleetshift:latest}"

# refresh_ca makes sure .certs/ca.crt matches the CA the *currently running*
# server container serves, re-copying it whenever it's missing or stale.
#
# A fresh container mints a new sandbox CA: a raw `podman run` with no volume
# (README AIO section), or `pd:clean`, wipes /data/sandbox/pki. A .certs/ca.crt
# cached from an earlier container then goes stale and the browser rejects the
# new leaf even after `pd:trust-cert` — so we always compare against the live
# CA and replace on mismatch. Handles both launch variants:
#   - raw `podman run ... quay.io/stolostron/fleetshift` (README AIO section)
#   - the compose stack (`task pd:up` / `task pd:dev`)
# Both run the same image, so a `podman cp` from the matching container covers
# either; a compose-service copy is the fallback for a custom image tag. With no
# container running, fall back to any existing copy (can't detect staleness).
refresh_ca() {
  ensure_podman_ready
  mkdir -p "$(dirname "$CA_CERT")"
  local tmp; tmp="$(mktemp)"

  # Variant 1: any running container off the AIO image (raw run or compose).
  local cid
  # `|| true`: under `set -o pipefail`, head closing the pipe early can leave the
  # pipeline non-zero (SIGPIPE to podman ps); tolerate it so an empty result
  # falls through to Variant 2 instead of tripping `set -e`.
  cid="$(podman ps -q --filter "ancestor=$AIO_IMAGE" 2>/dev/null | head -1 || true)"
  if [ -n "$cid" ] && podman cp "$cid:$SANDBOX_CA_PATH" "$tmp" 2>/dev/null; then
    :
  # Variant 2: compose stack with a custom image tag — copy via the service name.
  elif compose cp "fleetshift-server:$SANDBOX_CA_PATH" "$tmp" 2>/dev/null; then
    :
  else
    rm -f "$tmp"
    if [ -f "$CA_CERT" ]; then
      echo "==> No running server container; using existing $CA_CERT (may be stale)" >&2
      return 0
    fi
    echo "ERROR: could not copy the sandbox CA. Start the AIO first, then retry:" >&2
    echo "  compose:  task pd:up" >&2
    echo "  raw run:  podman run -d -p 127.0.0.1:8085:8085 -p 127.0.0.1:50051:50051 \\" >&2
    echo "              -p 127.0.0.1:5556:5556 $AIO_IMAGE" >&2
    echo "  or copy manually: podman cp <container>:$SANDBOX_CA_PATH $CA_CERT" >&2
    exit 1
  fi

  if [ -f "$CA_CERT" ] && cmp -s "$tmp" "$CA_CERT"; then
    rm -f "$tmp"   # already current
    return 0
  fi
  mv "$tmp" "$CA_CERT"
  echo "==> Refreshed .certs/ca.crt from the live server container"
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
  refresh_ca
  if macos_already_trusted; then
    echo "==> Dex sandbox CA already trusted in the login keychain. Nothing to do."
    return 0
  fi
  # A cert with our CN but a different fingerprint is a stale CA from a prior
  # container — remove it first so we don't leave a dead trust entry behind
  # (and so the browser doesn't pick the wrong one of two same-CN roots).
  if security find-certificate -c "$CA_CN" login.keychain-db &>/dev/null; then
    echo "==> Removing previously trusted (now stale) sandbox CA"
    remove_macos >/dev/null
  fi
  echo "==> Adding Dex sandbox CA to the login keychain, trusted for SSL"
  echo "    (macOS will prompt for your login password)"
  security add-trusted-cert -r trustRoot -p ssl \
    -k "$HOME/Library/Keychains/login.keychain-db" "$CA_CERT"
  # 127.0.0.1, not localhost: the Dex leaf SAN is the IP only (see sandboxpki.go).
  echo "==> Done. Fully quit and reopen your browser, then log in at" \
       "http://127.0.0.1:${FLEETSHIFT_SERVER_HTTP_PORT:-8085}"
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
  refresh_ca
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
