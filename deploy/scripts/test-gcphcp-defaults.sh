#!/usr/bin/env bash
# Contract tests for GCP HCP defaults, renderer, entrypoint, and dotenv precedence.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RENDERER="${SCRIPT_DIR}/render-gcphcp-config.sh"
ENTRYPOINT="${SCRIPT_DIR}/fleetshift-aio-entrypoint.sh"
# shellcheck source=common.sh
source "${SCRIPT_DIR}/common.sh"

PASS=0
FAIL=0
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

assert_eq() {
  local name="$1" got="$2" want="$3"
  if [ "$got" = "$want" ]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    echo "FAIL: ${name}" >&2
    echo "  got:  ${got}" >&2
    echo "  want: ${want}" >&2
  fi
}

assert_contains() {
  local name="$1" haystack="$2" needle="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    echo "FAIL: ${name}" >&2
    echo "  missing: ${needle}" >&2
    echo "  in: ${haystack}" >&2
  fi
}

assert_file_eq() {
  local name="$1" path="$2" want="$3"
  local got
  got="$(cat "$path")"
  assert_eq "$name" "$got" "$want"
}

assert_file_absent() {
  local name="$1" path="$2"
  if [ ! -e "$path" ]; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
    echo "FAIL: ${name} — expected absent file: ${path}" >&2
  fi
}

DEFAULT_YAML="$(cat <<'EOF'
gateway:
  url: "https://cls-gateway.example.invalid"
  audience: "32555940559.apps.googleusercontent.com"
targets:
  - id: "gcphcp"
    gcp_project: "gcp-ome-poc"
    region: "us-central1"
    workforce_pool: "ome-hcp"
    workforce_provider: "ome-oidc"
    broker_sa_email: "hcp-idtoken-broker@gcp-ome-poc.iam.gserviceaccount.com"
EOF
)"

# --- Renderer: disabled developer mode requires no GCP HCP values ---
out="${TMP_ROOT}/disabled.yaml"
env -i PATH="$PATH" HOME="$HOME" \
  GCPHCP_ENABLED=false \
  "$RENDERER" --output "$out"
got="$(cat "$out")"
assert_contains "renderer disabled placeholder" "$got" "gcphcp is disabled"

# --- Renderer: URL-only input renders seven defaults ---
out="${TMP_ROOT}/url-only.yaml"
env -i PATH="$PATH" HOME="$HOME" \
  GCPHCP_ENABLED=true \
  GCPHCP_GATEWAY_URL="https://cls-gateway.example.invalid" \
  "$RENDERER" --output "$out"
assert_file_eq "renderer url-only defaults" "$out" "$DEFAULT_YAML"

# --- Renderer: every optional override works ---
out="${TMP_ROOT}/overrides.yaml"
env -i PATH="$PATH" HOME="$HOME" \
  GCPHCP_ENABLED=true \
  GCPHCP_GATEWAY_URL="https://cls-gateway.example.invalid" \
  GCPHCP_GATEWAY_AUDIENCE="custom-audience.apps.googleusercontent.com" \
  GCPHCP_TARGET_ID="custom-target" \
  GCPHCP_GCP_PROJECT="custom-project" \
  GCPHCP_GCP_REGION="europe-west1" \
  GCPHCP_WORKFORCE_POOL="custom-pool" \
  GCPHCP_WORKFORCE_PROVIDER="custom-provider" \
  GCPHCP_BROKER_SA_EMAIL="broker@custom-project.iam.gserviceaccount.com" \
  "$RENDERER" --output "$out"
want="$(cat <<'EOF'
gateway:
  url: "https://cls-gateway.example.invalid"
  audience: "custom-audience.apps.googleusercontent.com"
targets:
  - id: "custom-target"
    gcp_project: "custom-project"
    region: "europe-west1"
    workforce_pool: "custom-pool"
    workforce_provider: "custom-provider"
    broker_sa_email: "broker@custom-project.iam.gserviceaccount.com"
EOF
)"
assert_file_eq "renderer optional overrides" "$out" "$want"

# --- Renderer: empty optional variables fall back to defaults ---
out="${TMP_ROOT}/empty-optional.yaml"
env -i PATH="$PATH" HOME="$HOME" \
  GCPHCP_ENABLED=true \
  GCPHCP_GATEWAY_URL="https://cls-gateway.example.invalid" \
  GCPHCP_GATEWAY_AUDIENCE="" \
  GCPHCP_TARGET_ID="" \
  GCPHCP_GCP_PROJECT="" \
  GCPHCP_GCP_REGION="" \
  GCPHCP_WORKFORCE_POOL="" \
  GCPHCP_WORKFORCE_PROVIDER="" \
  GCPHCP_BROKER_SA_EMAIL="" \
  "$RENDERER" --output "$out"
assert_file_eq "renderer empty optionals use defaults" "$out" "$DEFAULT_YAML"

# --- Renderer: missing URL is an actionable error ---
out="${TMP_ROOT}/missing-url.yaml"
set +e
err="$(
  env -i PATH="$PATH" HOME="$HOME" \
    GCPHCP_ENABLED=true \
    "$RENDERER" --output "$out" 2>&1
)"
status=$?
set -e
assert_eq "renderer missing URL exit status" "$status" "1"
assert_contains "renderer missing URL mentions GCPHCP_GATEWAY_URL" "$err" "GCPHCP_GATEWAY_URL"
assert_contains "renderer missing URL mentions .env" "$err" ".env"
assert_contains "renderer missing URL mentions podman -e" "$err" "podman -e"

# --- Renderer: blank URL is an actionable error ---
out="${TMP_ROOT}/blank-url.yaml"
set +e
err="$(
  env -i PATH="$PATH" HOME="$HOME" \
    GCPHCP_ENABLED=true \
    GCPHCP_GATEWAY_URL="" \
    "$RENDERER" --output "$out" 2>&1
)"
status=$?
set -e
assert_eq "renderer blank URL exit status" "$status" "1"
assert_contains "renderer blank URL mentions GCPHCP_GATEWAY_URL" "$err" "GCPHCP_GATEWAY_URL"

# --- Renderer: YAML escaping remains safe ---
out="${TMP_ROOT}/escape.yaml"
env -i PATH="$PATH" HOME="$HOME" \
  GCPHCP_ENABLED=true \
  GCPHCP_GATEWAY_URL='https://example.invalid/path?q="quote"\and' \
  GCPHCP_TARGET_ID=$'weird\tid' \
  "$RENDERER" --output "$out"
got="$(cat "$out")"
assert_contains "renderer escapes quotes" "$got" 'url: "https://example.invalid/path?q=\"quote\"\\and"'
assert_contains "renderer escapes tabs" "$got" 'id: "weird\tid"'

# --- Dotenv precedence: process environment wins over .env ---
env_file="${TMP_ROOT}/dotenv.env"
cat >"$env_file" <<'EOF'
FROM_FILE=file-value
SHARED=file-shared
FILE_ONLY=only-in-file
EOF
unset FROM_FILE FILE_ONLY
export SHARED=caller-shared
load_dotenv "$env_file"
assert_eq "dotenv file value loads when unset" "${FROM_FILE}" "file-value"
assert_eq "dotenv caller SHARED wins" "${SHARED}" "caller-shared"
assert_eq "dotenv FILE_ONLY loads" "${FILE_ONLY}" "only-in-file"

export FROM_FILE=caller-value
load_dotenv "$env_file"
assert_eq "dotenv caller FROM_FILE wins" "${FROM_FILE}" "caller-value"
unset FROM_FILE SHARED FILE_ONLY

fake_bin="${TMP_ROOT}/fake-bin"
mkdir -p "$fake_bin"
cat >"${fake_bin}/fleetshift" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'ARGS:%s\n' "$*"
env | grep -E '^(GCPHCP_CONFIG|FLEETSHIFT_SERVER_ADDONS|GCPHCP_ENABLED)=' | sort || true
if [ -n "${GCPHCP_CONFIG:-}" ] && [ -f "${GCPHCP_CONFIG}" ]; then
  echo "CONFIG_FILE_PRESENT=1"
  echo "CONFIG_BEGIN"
  cat "${GCPHCP_CONFIG}"
  echo "CONFIG_END"
else
  echo "CONFIG_FILE_PRESENT=0"
fi
EOF
chmod +x "${fake_bin}/fleetshift"

AIO_STATUS=0
AIO_OUT=""
AIO_ERR=""

run_aio() {
  local outfile="${TMP_ROOT}/aio-out.txt"
  local -a env_args=()
  local -a cmd_args=()
  local parsing_env=1
  for arg in "$@"; do
    if [ "$parsing_env" -eq 1 ] && [ "$arg" = "--" ]; then
      parsing_env=0
      continue
    fi
    if [ "$parsing_env" -eq 1 ]; then
      env_args+=("$arg")
    else
      cmd_args+=("$arg")
    fi
  done
  rm -f "${TMP_ROOT}/aio-gcphcp.yaml"
  set +e
  env -i \
    PATH="${fake_bin}:/usr/bin:/bin" \
    HOME="$HOME" \
    RENDER_GCPHCP_CONFIG="${RENDERER}" \
    GCPHCP_CONFIG_OUT="${TMP_ROOT}/aio-gcphcp.yaml" \
    "${env_args[@]}" \
    "$ENTRYPOINT" "${cmd_args[@]}" >"$outfile" 2>"${outfile}.err"
  AIO_STATUS=$?
  set -e
  AIO_OUT="$(cat "$outfile")"
  AIO_ERR="$(cat "${outfile}.err")"
}

# --- Entrypoint: no GCP HCP values → kind,kubernetes, no config file ---
run_aio -- serve --http-addr :8085
assert_eq "entrypoint no-intent exit" "$AIO_STATUS" "0"
assert_contains "entrypoint no-intent args" "$AIO_OUT" "ARGS:serve --http-addr :8085"
assert_contains "entrypoint no-intent addons" "$AIO_OUT" "FLEETSHIFT_SERVER_ADDONS=kind,kubernetes"
assert_contains "entrypoint no-intent no config export" "$AIO_OUT" "CONFIG_FILE_PRESENT=0"
assert_file_absent "entrypoint no-intent generates no file" "${TMP_ROOT}/aio-gcphcp.yaml"

# --- Entrypoint: URL-only activates gcphcp with defaults ---
run_aio GCPHCP_GATEWAY_URL=https://cls-gateway.example.invalid -- serve --http-addr :8085
assert_eq "entrypoint url-only exit" "$AIO_STATUS" "0"
assert_contains "entrypoint url-only addons" "$AIO_OUT" "FLEETSHIFT_SERVER_ADDONS=kind,kubernetes,gcphcp"
assert_contains "entrypoint url-only config path" "$AIO_OUT" "GCPHCP_CONFIG=${TMP_ROOT}/aio-gcphcp.yaml"
assert_contains "entrypoint url-only config present" "$AIO_OUT" "CONFIG_FILE_PRESENT=1"
assert_contains "entrypoint url-only default target" "$AIO_OUT" 'id: "gcphcp"'
assert_contains "entrypoint url-only default project" "$AIO_OUT" 'gcp_project: "gcp-ome-poc"'

# --- Entrypoint: explicit GCPHCP_CONFIG uses that file ---
explicit_cfg="${TMP_ROOT}/explicit.yaml"
cat >"$explicit_cfg" <<'EOF'
gateway:
  url: "https://explicit.example.invalid"
  audience: "explicit-audience.apps.googleusercontent.com"
targets:
  - id: "explicit"
    gcp_project: "explicit-project"
    region: "us-west1"
    workforce_pool: "explicit-pool"
    workforce_provider: "explicit-provider"
    broker_sa_email: "broker@explicit-project.iam.gserviceaccount.com"
EOF
run_aio GCPHCP_CONFIG="$explicit_cfg" -- serve
assert_eq "entrypoint explicit config exit" "$AIO_STATUS" "0"
assert_contains "entrypoint explicit config addons" "$AIO_OUT" "FLEETSHIFT_SERVER_ADDONS=kind,kubernetes,gcphcp"
assert_contains "entrypoint explicit config path" "$AIO_OUT" "GCPHCP_CONFIG=${explicit_cfg}"
assert_contains "entrypoint explicit config content" "$AIO_OUT" 'id: "explicit"'
assert_file_absent "entrypoint explicit config does not render replacement" "${TMP_ROOT}/aio-gcphcp.yaml"

# --- Entrypoint: ENABLED=true without URL/config fails ---
run_aio GCPHCP_ENABLED=true -- serve
assert_eq "entrypoint enabled without url exit" "$AIO_STATUS" "1"
assert_contains "entrypoint enabled without url error" "$AIO_ERR" "GCPHCP_GATEWAY_URL"

# --- Entrypoint: partial optional overrides without URL/config fail ---
run_aio GCPHCP_GCP_PROJECT=custom-project -- serve
assert_eq "entrypoint partial override exit" "$AIO_STATUS" "1"
assert_contains "entrypoint partial override error" "$AIO_ERR" "GCPHCP_GATEWAY_URL"

# --- Entrypoint: ENABLED=false keeps gcphcp disabled ---
run_aio GCPHCP_ENABLED=false GCPHCP_GATEWAY_URL=https://cls-gateway.example.invalid -- serve
assert_eq "entrypoint explicit disable exit" "$AIO_STATUS" "0"
assert_contains "entrypoint explicit disable addons" "$AIO_OUT" "FLEETSHIFT_SERVER_ADDONS=kind,kubernetes"
assert_contains "entrypoint explicit disable no config" "$AIO_OUT" "CONFIG_FILE_PRESENT=0"
assert_file_absent "entrypoint explicit disable generates no file" "${TMP_ROOT}/aio-gcphcp.yaml"

# --- Entrypoint: preserves caller FLEETSHIFT_SERVER_ADDONS and args ---
run_aio \
  GCPHCP_GATEWAY_URL=https://cls-gateway.example.invalid \
  FLEETSHIFT_SERVER_ADDONS=kubernetes,gcphcp \
  -- serve --addons kubernetes,gcphcp --log-level debug
assert_eq "entrypoint preserve addons exit" "$AIO_STATUS" "0"
assert_contains "entrypoint preserves --addons" "$AIO_OUT" "ARGS:serve --addons kubernetes,gcphcp --log-level debug"
assert_contains "entrypoint preserves env addons" "$AIO_OUT" "FLEETSHIFT_SERVER_ADDONS=kubernetes,gcphcp"

# --- Entrypoint: non-serve commands pass through ---
run_aio -- version
assert_eq "entrypoint passthrough exit" "$AIO_STATUS" "0"
assert_contains "entrypoint passthrough args" "$AIO_OUT" "ARGS:version"

# --- Entrypoint: optional overrides reach rendered config ---
run_aio \
  GCPHCP_GATEWAY_URL=https://cls-gateway.example.invalid \
  GCPHCP_TARGET_ID=override-target \
  GCPHCP_GCP_REGION=europe-west1 \
  -- serve
assert_eq "entrypoint overrides exit" "$AIO_STATUS" "0"
assert_contains "entrypoint override target" "$AIO_OUT" 'id: "override-target"'
assert_contains "entrypoint override region" "$AIO_OUT" 'region: "europe-west1"'

echo ""
echo "Passed: ${PASS}"
echo "Failed: ${FAIL}"
if [ "$FAIL" -ne 0 ]; then
  exit 1
fi
