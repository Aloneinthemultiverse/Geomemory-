#!/usr/bin/env bash
# First start: create the cluster, user, database and extensions. Later
# starts: just run PostgreSQL. Settings come from POSTGRES_USER,
# POSTGRES_PASSWORD and POSTGRES_DB (defaults: geomemory).
set -euo pipefail
USER_="${POSTGRES_USER:-geomemory}"
PASS="${POSTGRES_PASSWORD:-geomemory}"
DB="${POSTGRES_DB:-geomemory}"
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "geomemory-db: initialising $PGDATA"
  initdb -D "$PGDATA" -U postgres --encoding=UTF8 --locale=C.UTF-8 --auth-local=trust --auth-host=scram-sha-256 >/dev/null
  echo "listen_addresses = '*'" >> "$PGDATA/postgresql.conf"
  echo "host all all 0.0.0.0/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
  echo "host all all ::/0 scram-sha-256" >> "$PGDATA/pg_hba.conf"
  pg_ctl -D "$PGDATA" -o "-c listen_addresses=''" -w start >/dev/null
  psql -v ON_ERROR_STOP=1 -U postgres -q <<SQL
CREATE ROLE "$USER_" LOGIN SUPERUSER PASSWORD '$PASS';
CREATE DATABASE "$DB" OWNER "$USER_";
SQL
  psql -v ON_ERROR_STOP=1 -U postgres -d "$DB" -q -f /docker-entrypoint-initdb.d/init.sql
  pg_ctl -D "$PGDATA" -m fast -w stop >/dev/null
  echo "geomemory-db: ready (user $USER_, database $DB)"
fi
exec postgres -D "$PGDATA"
