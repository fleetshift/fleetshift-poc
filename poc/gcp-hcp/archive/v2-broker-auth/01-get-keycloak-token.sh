#!/usr/bin/env bash
# Stage 1: Fetch a JWT from Keycloak via the OAuth2 Authorization Code flow with PKCE.
# Opens a browser URL for login, receives the callback on localhost:8888.
# Saves the raw JWT to tmp/keycloak_token.jwt for use by later stages.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"
load_config KEYCLOAK_URL KEYCLOAK_REALM KEYCLOAK_CLIENT_ID

TOKEN_ENDPOINT="${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token"
AUTH_ENDPOINT="${KEYCLOAK_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/auth"
REDIRECT_URI="http://localhost:8888/callback"
LISTEN_PORT=8888

log_header "Stage 1: Get Keycloak Token (Authorization Code + PKCE)"

log_step "Auth endpoint: ${AUTH_ENDPOINT}"
log_step "Client ID: ${KEYCLOAK_CLIENT_ID}"

# --- Generate PKCE parameters ---
read -r CODE_VERIFIER CODE_CHALLENGE STATE < <(python3 -c "
import hashlib, base64, os
verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b'=').decode()
challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode()
state = os.urandom(16).hex()
print(verifier, challenge, state)
")

# --- Build authorization URL ---
ENCODED_REDIRECT=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${REDIRECT_URI}'))")
AUTH_URL="${AUTH_ENDPOINT}?response_type=code&client_id=${KEYCLOAK_CLIENT_ID}&redirect_uri=${ENCODED_REDIRECT}&scope=openid%20email&state=${STATE}&code_challenge=${CODE_CHALLENGE}&code_challenge_method=S256"

echo ""
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  Open this URL in your browser:"
echo ""
echo "    ${AUTH_URL}"
echo ""
echo "  Log in with your Keycloak credentials."
echo "  Waiting for callback on localhost:${LISTEN_PORT}..."
echo ""
echo "════════════════════════════════════════════════════════════"
echo ""

# --- Start a temporary HTTP server to catch the callback ---
# Writes auth code to a temp file to avoid stdout/grep issues with set -e.
CALLBACK_CODE_FILE="${TMP_DIR}/auth_code.txt"
CALLBACK_STATE_FILE="${TMP_DIR}/auth_state.txt"
CALLBACK_ERROR_FILE="${TMP_DIR}/auth_error.txt"
rm -f "${CALLBACK_CODE_FILE}" "${CALLBACK_STATE_FILE}" "${CALLBACK_ERROR_FILE}"

python3 -c "
import http.server
import urllib.parse
import sys

class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)

        if 'code' in params:
            code = params['code'][0]
            state = params.get('state', [''])[0]
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(b'<html><body><h2>Authentication successful!</h2><p>You can close this tab.</p></body></html>')
            with open('${CALLBACK_CODE_FILE}', 'w') as f:
                f.write(code)
            with open('${CALLBACK_STATE_FILE}', 'w') as f:
                f.write(state)
        elif 'error' in params:
            error = params['error'][0]
            desc = params.get('error_description', [''])[0]
            self.send_response(400)
            self.send_header('Content-Type', 'text/html')
            self.end_headers()
            self.wfile.write(f'<html><body><h2>Error: {error}</h2><p>{desc}</p></body></html>'.encode())
            with open('${CALLBACK_ERROR_FILE}', 'w') as f:
                f.write(f'{error}: {desc}')
        else:
            self.send_response(400)
            self.end_headers()
            with open('${CALLBACK_ERROR_FILE}', 'w') as f:
                f.write('unknown error')

    def log_message(self, format, *args):
        pass

server = http.server.HTTPServer(('127.0.0.1', ${LISTEN_PORT}), CallbackHandler)
server.timeout = 120
server.handle_request()
"

# --- Parse callback result ---
if [[ -f "${CALLBACK_ERROR_FILE}" ]]; then
    log_fail "Authentication failed: $(cat "${CALLBACK_ERROR_FILE}")"
    exit 1
fi

if [[ ! -f "${CALLBACK_CODE_FILE}" ]]; then
    log_fail "No authorization code received (timed out after 120s)"
    exit 1
fi

AUTH_CODE=$(cat "${CALLBACK_CODE_FILE}")
CALLBACK_STATE=$(cat "${CALLBACK_STATE_FILE}" 2>/dev/null || echo "")

if [[ "${CALLBACK_STATE}" != "${STATE}" ]]; then
    log_fail "State mismatch — possible CSRF attack"
    exit 1
fi

log_ok "Authorization code received"

# --- Exchange code for token ---
log_step "Exchanging authorization code for token..."

TOKEN_RESPONSE=$(curl -s -X POST "${TOKEN_ENDPOINT}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=authorization_code" \
    -d "client_id=${KEYCLOAK_CLIENT_ID}" \
    -d "code=${AUTH_CODE}" \
    -d "redirect_uri=${REDIRECT_URI}" \
    -d "code_verifier=${CODE_VERIFIER}")

if echo "${TOKEN_RESPONSE}" | jq -e '.error' &>/dev/null; then
    log_fail "Token exchange failed:"
    echo "${TOKEN_RESPONSE}" | jq .
    exit 1
fi

ACCESS_TOKEN=$(echo "${TOKEN_RESPONSE}" | jq -r '.access_token')
if [[ -z "${ACCESS_TOKEN}" || "${ACCESS_TOKEN}" == "null" ]]; then
    log_fail "No access_token in response:"
    echo "${TOKEN_RESPONSE}" | jq .
    exit 1
fi

log_ok "Token received"

# Save raw token
echo -n "${ACCESS_TOKEN}" > "${TMP_DIR}/keycloak_token.jwt"
log_ok "Token saved to tmp/keycloak_token.jwt"

# --- Decode and display ---
log_step "Token claims:"
if is_jwt "${ACCESS_TOKEN}"; then
    decode_jwt "${ACCESS_TOKEN}"

    ISS=$(decode_jwt "${ACCESS_TOKEN}" | jq -r '.iss // "missing"')
    SUB=$(decode_jwt "${ACCESS_TOKEN}" | jq -r '.sub // "missing"')
    EMAIL=$(decode_jwt "${ACCESS_TOKEN}" | jq -r '.email // "missing"')
    AUD=$(decode_jwt "${ACCESS_TOKEN}" | jq -r '.aud // "missing"')
    EXP=$(decode_jwt "${ACCESS_TOKEN}" | jq -r '.exp // "missing"')
    EXP_HUMAN=""
    if [[ "${EXP}" != "missing" ]]; then
        EXP_HUMAN=" ($(date -d "@${EXP}" 2>/dev/null || date -r "${EXP}" 2>/dev/null || echo "?"))"
    fi

    echo ""
    log_step "Quick summary:"
    echo "  iss:   ${ISS}"
    echo "  sub:   ${SUB}"
    echo "  email: ${EMAIL}"
    echo "  aud:   ${AUD}"
    echo "  exp:   ${EXP}${EXP_HUMAN}"
else
    log_warn "Token is not a JWT (no dot-separated segments). Raw value:"
    echo "${ACCESS_TOKEN}" | head -c 200
    echo ""
fi

echo ""
log_ok "Stage 1 complete"
log_step "Next: run 02-sts-exchange.sh"
