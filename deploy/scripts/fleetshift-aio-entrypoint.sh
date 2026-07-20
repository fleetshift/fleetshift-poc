#!/usr/bin/env bash
# All-in-one image entrypoint: optionally render GCP HCP config, then exec fleetshift.
set -euo pipefail

is_truthy() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

is_falsey() {
  case "$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    0|false|no|off)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

has_optional_gcphcp_overrides() {
  [ -n "${GCPHCP_GATEWAY_AUDIENCE:-}" ] ||
    [ -n "${GCPHCP_TARGET_ID:-}" ] ||
    [ -n "${GCPHCP_GCP_PROJECT:-}" ] ||
    [ -n "${GCPHCP_GCP_REGION:-}" ] ||
    [ -n "${GCPHCP_WORKFORCE_POOL:-}" ] ||
    [ -n "${GCPHCP_WORKFORCE_PROVIDER:-}" ] ||
    [ -n "${GCPHCP_BROKER_SA_EMAIL:-}" ]
}

fail_missing_gateway() {
  cat >&2 <<'EOF'
ERROR: GCP HCP was requested but GCPHCP_GATEWAY_URL is not set.
Provide the CLS gateway URL, for example:
  podman run ... -e GCPHCP_GATEWAY_URL=https://your-cls-gateway ...
Or mount a full config with -e GCPHCP_CONFIG=/path/to/gcphcp.yaml.
EOF
  exit 1
}

ensure_addons() {
  local desired="$1"
  if [ -z "${FLEETSHIFT_SERVER_ADDONS:-}" ]; then
    export FLEETSHIFT_SERVER_ADDONS="$desired"
  fi
}

RENDERER="${RENDER_GCPHCP_CONFIG:-/usr/local/bin/render-gcphcp-config.sh}"
CONFIG_OUT="${GCPHCP_CONFIG_OUT:-/data/gcphcp.yaml}"

# Non-serve commands pass through unchanged.
if [ "${1:-}" != "serve" ]; then
  exec fleetshift "$@"
fi

enabled_raw="${GCPHCP_ENABLED-}"

if [ -n "$enabled_raw" ] && is_falsey "$enabled_raw"; then
  ensure_addons "kind,kubernetes"
  exec fleetshift "$@"
fi

if [ -n "${GCPHCP_CONFIG:-}" ]; then
  ensure_addons "kind,kubernetes,gcphcp"
  exec fleetshift "$@"
fi

if [ -n "${GCPHCP_GATEWAY_URL:-}" ]; then
  export GCPHCP_ENABLED=true
  "$RENDERER" --output "$CONFIG_OUT"
  export GCPHCP_CONFIG="$CONFIG_OUT"
  ensure_addons "kind,kubernetes,gcphcp"
  exec fleetshift "$@"
fi

if [ -n "$enabled_raw" ] && is_truthy "$enabled_raw"; then
  fail_missing_gateway
fi

if has_optional_gcphcp_overrides; then
  cat >&2 <<'EOF'
ERROR: GCP HCP optional overrides were set without GCPHCP_GATEWAY_URL or GCPHCP_CONFIG.
Set the gateway URL to activate gcphcp, or unset the GCPHCP_* overrides.
EOF
  exit 1
fi

ensure_addons "kind,kubernetes"
exec fleetshift "$@"
