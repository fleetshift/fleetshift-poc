#!/usr/bin/env bash
# Shared helpers for deploy scripts. Source this, don't execute it.

# Source a dotenv file without overriding variables already present in the
# process environment. Explicitly exported caller values win over file values.
load_dotenv() {
  local env_file="$1"
  local caller_env
  caller_env="$(export -p | sed 's/^declare -x /export /')"
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  eval "${caller_env}"
}

# Run a command with a deadline. Prefers GNU timeout, then gtimeout
# (Homebrew coreutils on macOS), then perl alarm. Fails closed if none
# are available so callers never hang indefinitely.
run_with_timeout() {
  local secs=$1; shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$secs" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$secs" "$@"
  elif command -v perl >/dev/null 2>&1; then
    perl -e 'alarm shift; exec @ARGV' -- "$secs" "$@"
  else
    echo "ERROR: no timeout helper found (need timeout, gtimeout, or perl)." >&2
    return 127
  fi
}

# Fail fast unless oc can authenticate within a few seconds.
require_oc_login() {
  # Keep stderr so missing-timeout errors from run_with_timeout are visible.
  # Use `|| st=$?` so set -e callers don't exit before we can message.
  local st=0
  run_with_timeout 5 oc whoami >/dev/null || st=$?
  if [[ $st -ne 0 ]]; then
    # 127 = no timeout helper (already reported by run_with_timeout)
    if [[ $st -ne 127 ]]; then
      echo "ERROR: Not logged in to OpenShift. Run 'oc login' first." >&2
    fi
    exit 1
  fi
}
