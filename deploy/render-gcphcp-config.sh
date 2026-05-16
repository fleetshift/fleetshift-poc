#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: render-gcphcp-config.sh --output <path>

Renders a gcphcp YAML config file from environment variables.
When GCPHCP_ENABLED is not truthy, writes a disabled placeholder file.
EOF
  exit 1
}

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

require_env() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "ERROR: ${name} is required when GCPHCP_ENABLED=true" >&2
    exit 1
  fi
}

yaml_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

OUTPUT_PATH=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      [ "$#" -ge 2 ] || usage
      OUTPUT_PATH="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[ -n "$OUTPUT_PATH" ] || usage

mkdir -p "$(dirname "$OUTPUT_PATH")"

if ! is_truthy "${GCPHCP_ENABLED:-false}"; then
  cat > "$OUTPUT_PATH" <<'EOF'
# gcphcp is disabled for this deployment.
EOF
  exit 0
fi

require_env GCPHCP_GATEWAY_URL
require_env GCPHCP_GATEWAY_AUDIENCE
require_env GCPHCP_TARGET_ID
require_env GCPHCP_GCP_PROJECT
require_env GCPHCP_GCP_REGION
require_env GCPHCP_WORKFORCE_POOL
require_env GCPHCP_WORKFORCE_PROVIDER
require_env GCPHCP_BROKER_SA_EMAIL

cat > "$OUTPUT_PATH" <<EOF
gateway:
  url: "$(yaml_escape "${GCPHCP_GATEWAY_URL}")"
  audience: "$(yaml_escape "${GCPHCP_GATEWAY_AUDIENCE}")"
targets:
  - id: "$(yaml_escape "${GCPHCP_TARGET_ID}")"
    gcp_project: "$(yaml_escape "${GCPHCP_GCP_PROJECT}")"
    region: "$(yaml_escape "${GCPHCP_GCP_REGION}")"
    workforce_pool: "$(yaml_escape "${GCPHCP_WORKFORCE_POOL}")"
    workforce_provider: "$(yaml_escape "${GCPHCP_WORKFORCE_PROVIDER}")"
    broker_sa_email: "$(yaml_escape "${GCPHCP_BROKER_SA_EMAIL}")"
EOF
