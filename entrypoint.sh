#!/bin/sh
set -e

DEX_PID=""

cleanup() {
    if [ -n "$DEX_PID" ]; then
        kill "$DEX_PID" 2>/dev/null || true
        wait "$DEX_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

if [ "${EMBEDDED_IDP:-false}" = "true" ]; then
    echo "Starting embedded Dex IDP on :5556..."
    dex-idp dex &
    DEX_PID=$!

    # Wait for dex to be ready (up to 10 seconds)
    for i in $(seq 1 20); do
        if curl -sf http://localhost:5556/dex/.well-known/openid-configuration > /dev/null 2>&1; then
            echo "Dex IDP ready."
            break
        fi
        if [ "$i" -eq 20 ]; then
            echo "ERROR: Dex IDP failed to start within 10 seconds." >&2
            exit 1
        fi
        sleep 0.5
    done

    # Default OIDC issuer to embedded dex if not explicitly set
    export OIDC_ISSUER_URL="${OIDC_ISSUER_URL:-http://localhost:5556/dex}"
fi

exec fleetshift "$@"
