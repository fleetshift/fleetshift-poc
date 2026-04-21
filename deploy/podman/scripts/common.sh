#!/usr/bin/env bash
# Shared helpers for deploy scripts. Source this, don't execute it.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$(cd "$COMPOSE_DIR/.." && pwd)"

load_env() {
  if [ ! -f "$DEPLOY_DIR/.env" ]; then
    if [ -f "$DEPLOY_DIR/.env.template" ]; then
      echo "No .env found, creating from .env.template"
      cp "$DEPLOY_DIR/.env.template" "$DEPLOY_DIR/.env"
    else
      echo "ERROR: No .env or .env.template found in $DEPLOY_DIR" >&2
      exit 1
    fi
  fi
  set -a; source "$DEPLOY_DIR/.env"; set +a
}

compose() {
  local -a cmd=(podman compose -f "$COMPOSE_DIR/docker-compose.yml" --env-file "$DEPLOY_DIR/.env")
  if [ "${DEMO_MODE:-true}" = "true" ]; then
    cmd+=(--profile demo)
  fi
  "${cmd[@]}" "$@"
}

detect_podman_socket() {
  if [ -z "${PODMAN_SOCKET:-}" ]; then
    PODMAN_SOCKET=$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null | sed 's|^unix://||' || echo "/run/user/$(id -u)/podman/podman.sock")
  fi
}

generate_password() {
  openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16
}
