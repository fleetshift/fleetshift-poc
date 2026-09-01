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
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

yaml_escape() {
  local value="$1"
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  value=${value//$'\n'/\\n}
  value=${value//$'\r'/\\r}
  value=${value//$'\t'/\\t}
  printf '%s' "$value"
}

output_path=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      [ "$#" -ge 2 ] || usage
      output_path="$2"
      shift 2
      ;;
    *) usage ;;
  esac
done
[ -n "$output_path" ] || usage

mkdir -p "$(dirname "$output_path")"
if ! is_truthy "${GCPHCP_ENABLED:-false}"; then
  printf '%s\n' '# gcphcp is disabled for this deployment.' > "$output_path"
  exit 0
fi

[ -n "${GCPHCP_GATEWAY_URL:-}" ] || {
  printf '%s\n' 'ERROR: GCPHCP_GATEWAY_URL is required when GCPHCP_ENABLED=true.' >&2
  exit 1
}

gateway_audience="${GCPHCP_GATEWAY_AUDIENCE:-32555940559.apps.googleusercontent.com}"
target_id="${GCPHCP_TARGET_ID:-gcphcp}"
gcp_project="${GCPHCP_GCP_PROJECT:-gcp-ome-poc}"
gcp_region="${GCPHCP_GCP_REGION:-us-central1}"
workforce_pool="${GCPHCP_WORKFORCE_POOL:-ome-hcp}"
workforce_provider="${GCPHCP_WORKFORCE_PROVIDER:-ome-oidc}"
broker_sa_email="${GCPHCP_BROKER_SA_EMAIL:-hcp-idtoken-broker@gcp-ome-poc.iam.gserviceaccount.com}"

cat > "$output_path" <<EOF
gateway:
  url: "$(yaml_escape "${GCPHCP_GATEWAY_URL}")"
  audience: "$(yaml_escape "$gateway_audience")"
targets:
  - id: "$(yaml_escape "$target_id")"
    gcp_project: "$(yaml_escape "$gcp_project")"
    region: "$(yaml_escape "$gcp_region")"
    workforce_pool: "$(yaml_escape "$workforce_pool")"
    workforce_provider: "$(yaml_escape "$workforce_provider")"
    broker_sa_email: "$(yaml_escape "$broker_sa_email")"
EOF
