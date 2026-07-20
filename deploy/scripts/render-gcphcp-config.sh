#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: render-gcphcp-config.sh --output <path>

Renders a gcphcp YAML config file from environment variables.
When GCPHCP_ENABLED is not truthy, writes a disabled placeholder file.

Only GCPHCP_GATEWAY_URL is required when enabled. The remaining values use
built-in POC defaults unless overridden by a nonempty environment variable.
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

require_gateway_url() {
  if [ -z "${GCPHCP_GATEWAY_URL:-}" ]; then
    cat >&2 <<'EOF'
ERROR: GCPHCP_GATEWAY_URL is required when GCPHCP_ENABLED=true.
Set it in .env (for example GCPHCP_GATEWAY_URL=https://your-cls-gateway)
or pass it with podman -e GCPHCP_GATEWAY_URL=https://your-cls-gateway.
EOF
    exit 1
  fi
}

yaml_escape() {
  local s="$1"
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\n'/\\n}
  s=${s//$'\r'/\\r}
  s=${s//$'\t'/\\t}
  printf '%s' "$s"
}

# Built-in defaults for optional GCP HCP settings. Unset and empty values
# are treated identically. Keep this block as the single runtime source of
# defaults for deploy/podman, deploy/kubernetes, and the all-in-one image.
default_gateway_audience="32555940559.apps.googleusercontent.com"
default_target_id="gcphcp"
default_gcp_project="gcp-ome-poc"
default_gcp_region="us-central1"
default_workforce_pool="ome-hcp"
default_workforce_provider="ome-oidc"
default_broker_sa_email="hcp-idtoken-broker@gcp-ome-poc.iam.gserviceaccount.com"

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

require_gateway_url

gateway_audience="${GCPHCP_GATEWAY_AUDIENCE:-$default_gateway_audience}"
target_id="${GCPHCP_TARGET_ID:-$default_target_id}"
gcp_project="${GCPHCP_GCP_PROJECT:-$default_gcp_project}"
gcp_region="${GCPHCP_GCP_REGION:-$default_gcp_region}"
workforce_pool="${GCPHCP_WORKFORCE_POOL:-$default_workforce_pool}"
workforce_provider="${GCPHCP_WORKFORCE_PROVIDER:-$default_workforce_provider}"
broker_sa_email="${GCPHCP_BROKER_SA_EMAIL:-$default_broker_sa_email}"

cat > "$OUTPUT_PATH" <<EOF
gateway:
  url: "$(yaml_escape "${GCPHCP_GATEWAY_URL}")"
  audience: "$(yaml_escape "${gateway_audience}")"
targets:
  - id: "$(yaml_escape "${target_id}")"
    gcp_project: "$(yaml_escape "${gcp_project}")"
    region: "$(yaml_escape "${gcp_region}")"
    workforce_pool: "$(yaml_escape "${workforce_pool}")"
    workforce_provider: "$(yaml_escape "${workforce_provider}")"
    broker_sa_email: "$(yaml_escape "${broker_sa_email}")"
EOF
