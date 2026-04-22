#!/usr/bin/env bash
set -euo pipefail

SQLITE_DIR="internal/infrastructure/sqlite/migrations"
POSTGRES_DIR="internal/infrastructure/postgres/migrations"

sqlite_nums=$(ls "$SQLITE_DIR"/*.sql 2>/dev/null | sed 's/.*\///' | sed 's/_.*//' | sort)
postgres_nums=$(ls "$POSTGRES_DIR"/*.sql 2>/dev/null | sed 's/.*\///' | sed 's/_.*//' | sort)

if [ "$sqlite_nums" != "$postgres_nums" ]; then
  echo "ERROR: Migration number mismatch between sqlite and postgres"
  echo ""
  echo "SQLite migrations:"
  echo "$sqlite_nums"
  echo ""
  echo "Postgres migrations:"
  echo "$postgres_nums"
  echo ""
  diff <(echo "$sqlite_nums") <(echo "$postgres_nums") || true
  exit 1
fi

echo "OK: Migration parity check passed ($(echo "$sqlite_nums" | wc -l) migrations in each)"
