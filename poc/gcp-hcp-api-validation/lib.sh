#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR="${SCRIPT_DIR}/tmp"
mkdir -p "${TMP_DIR}"

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_step()  { echo -e "${BLUE}▶ $*${NC}"; }
log_ok()    { echo -e "${GREEN}✓ $*${NC}"; }
log_fail()  { echo -e "${RED}✗ $*${NC}"; }
log_warn()  { echo -e "${YELLOW}⚠ $*${NC}"; }
log_header(){ echo -e "\n${BLUE}════════════════════════════════════════${NC}"; echo -e "${BLUE}  $*${NC}"; echo -e "${BLUE}════════════════════════════════════════${NC}\n"; }

load_config() {
    local config_file="${SCRIPT_DIR}/config.env"
    if [[ ! -f "${config_file}" ]]; then
        log_fail "config.env not found. Copy config.env.example to config.env and fill in your values."
        exit 1
    fi
    # shellcheck source=/dev/null
    source "${config_file}"

    local required_vars=("$@")
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log_fail "Required config variable ${var} is not set in config.env"
            exit 1
        fi
    done
}

decode_jwt() {
    local token="$1"
    local payload
    payload=$(echo "${token}" | cut -d'.' -f2)
    # Add padding if needed
    local padding=$(( 4 - ${#payload} % 4 ))
    if (( padding < 4 )); then
        payload="${payload}$(printf '=%.0s' $(seq 1 "${padding}"))"
    fi
    echo "${payload}" | base64 -d 2>/dev/null | jq .
}

is_jwt() {
    local token="$1"
    # Google opaque access tokens start with ya29. and have dots but are NOT JWTs
    if [[ "${token}" == ya29.* ]]; then
        return 1
    fi
    # A JWT has exactly 3 dot-separated segments, each being valid base64url
    local dots
    dots=$(echo "${token}" | tr -cd '.' | wc -c)
    if [[ "${dots}" -ne 2 ]]; then
        return 1
    fi
    # Verify the second segment (payload) is decodable as JSON
    local payload
    payload=$(echo "${token}" | cut -d'.' -f2)
    local padded="${payload}"
    local pad_len=$(( 4 - ${#payload} % 4 ))
    if (( pad_len < 4 )); then
        padded="${payload}$(printf '=%.0s' $(seq 1 "${pad_len}"))"
    fi
    echo "${padded}" | base64 -d 2>/dev/null | jq . &>/dev/null
}

save_response() {
    local name="$1"
    local http_code="$2"
    local body="$3"
    echo "${body}" > "${TMP_DIR}/${name}.body.json"
    echo "${http_code}" > "${TMP_DIR}/${name}.http_code"
    log_step "Response saved to tmp/${name}.body.json (HTTP ${http_code})"
}

load_broker_auth() {
    local broker_token_file="${TMP_DIR}/broker_idtoken.jwt"
    local email_file="${TMP_DIR}/user_email.txt"

    if [[ ! -f "${broker_token_file}" ]]; then
        log_fail "Broker ID token not found. Run scripts 01-03 first."
        exit 1
    fi
    if [[ ! -f "${email_file}" ]]; then
        log_fail "User email not found. Run scripts 01-02 first."
        exit 1
    fi

    BROKER_TOKEN=$(cat "${broker_token_file}")
    USER_EMAIL=$(cat "${email_file}")

    log_ok "Loaded broker token and user email (${USER_EMAIL})"
}

api_call() {
    local method="$1"
    local path="$2"
    local name="$3"
    local body="${4:-}"

    local curl_args=(-s -w "\n%{http_code}"
        -H "Authorization: Bearer ${BROKER_TOKEN}"
        -H "X-User-Email: ${USER_EMAIL}"
        -H "Content-Type: application/json")
    if [[ -n "${body}" ]]; then
        curl_args+=(-d "${body}")
    fi

    local response
    response=$(curl -X "${method}" "${curl_args[@]}" "${GATEWAY_URL}${path}")
    local http_code
    http_code=$(echo "${response}" | tail -1)
    local resp_body
    resp_body=$(echo "${response}" | sed '$d')

    save_response "${name}" "${http_code}" "${resp_body}" >&2
    echo "${http_code}"
}

load_infra_configs() {
    local iam_file="${TMP_DIR}/iam-config.json"
    local infra_file="${TMP_DIR}/infra-config.json"
    local signing_key_file="${TMP_DIR}/signing-key-base64.txt"

    for f in "${iam_file}" "${infra_file}" "${signing_key_file}"; do
        if [[ ! -f "${f}" ]]; then
            log_fail "Required file not found: ${f}"
            log_fail "Run 08-setup-infra.sh first."
            exit 1
        fi
    done

    IAM_CONFIG=$(cat "${iam_file}")
    INFRA_CONFIG=$(cat "${infra_file}")
    SIGNING_KEY_BASE64=$(cat "${signing_key_file}")

    log_ok "Loaded infra configs (IAM, network, signing key)"
}
