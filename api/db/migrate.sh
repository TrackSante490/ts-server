#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"

MIGR_DIR="${MIGR_DIR:-/migrations}"

echo "Waiting for DB..."
until pg_isready -d "$DATABASE_URL" >/dev/null 2>&1; do
  sleep 1
done

echo "Ensuring schema_migrations table exists..."
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
SQL

echo "Running pending migrations in $MIGR_DIR ..."
# Only run files like 001_*.sql, 002_*.sql etc (sorted)
for f in $(ls -1 "$MIGR_DIR"/*.sql 2>/dev/null | sort); do
  base="$(basename "$f")"

  applied="$(psql "$DATABASE_URL" -tA -v ON_ERROR_STOP=1 -c \
    "SELECT 1 FROM schema_migrations WHERE filename='$base' LIMIT 1;")"

  if [[ "$applied" == "1" ]]; then
    echo "✓ Skipping already applied: $base"
    continue
  fi

  echo "==> Applying: $base"
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$f"

  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
    "INSERT INTO schema_migrations(filename) VALUES ('$base');"

  echo "✓ Applied: $base"
done

echo "All migrations done."