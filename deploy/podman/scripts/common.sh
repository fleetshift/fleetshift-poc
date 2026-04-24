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

COMPOSE_FILES=()

resolve_profile() {
  local profile="${PROFILE:-demo}"

  case "$profile" in
    demo)
      DB_BACKEND="${DB:-sqlite}"
      AUTH_MODE="${AUTH:-local}"
      ;;
    prod)
      DB_BACKEND="${DB:-postgres}"
      AUTH_MODE="${AUTH:-external}"
      ;;
    *)
      echo "ERROR: Unknown profile '$profile'. Valid profiles: demo, prod" >&2
      exit 1
      ;;
  esac

  COMPOSE_FILES=("-f" "$COMPOSE_DIR/compose.yaml")

  case "$DB_BACKEND" in
    sqlite)   COMPOSE_FILES+=("-f" "$COMPOSE_DIR/overrides/sqlite.yaml") ;;
    postgres) COMPOSE_FILES+=("-f" "$COMPOSE_DIR/overrides/postgres.yaml") ;;
    *)
      echo "ERROR: Unknown DB backend '$DB_BACKEND'. Valid options: sqlite, postgres" >&2
      exit 1
      ;;
  esac

  case "$AUTH_MODE" in
    local)    COMPOSE_FILES+=("-f" "$COMPOSE_DIR/overrides/local-keycloak.yaml") ;;
    external) ;;
    *)
      echo "ERROR: Unknown AUTH mode '$AUTH_MODE'. Valid options: local, external" >&2
      exit 1
      ;;
  esac

  echo "==> Profile: $profile (db=$DB_BACKEND, auth=$AUTH_MODE)"
}

compose() {
  if [ ${#COMPOSE_FILES[@]} -eq 0 ]; then
    resolve_profile
  fi
  podman compose "${COMPOSE_FILES[@]}" --env-file "$DEPLOY_DIR/.env" "$@"
}

detect_podman_socket() {
  if [ -z "${PODMAN_SOCKET:-}" ]; then
    PODMAN_SOCKET=$(podman info --format '{{.Host.RemoteSocket.Path}}' 2>/dev/null | sed 's|^unix://||' || echo "/run/user/$(id -u)/podman/podman.sock")
  fi
}

generate_password() {
  openssl rand -base64 32 | tr -dc 'a-zA-Z0-9' | head -c 16
}
