#!/usr/bin/env bash
#
# Create (or refresh) the integration-test database for this service.
#
#   ./scripts/setup-test-db.sh              # create if absent
#   ./scripts/setup-test-db.sh --recreate   # drop and rebuild from the live schema
#
# The DB-backed tests must not run against `gridtokenx_trading`: the running
# trading-service is attached to it, and its SettlementWorker/MatcherWorker
# mutate fixture rows mid-test (this made settlement_cas_retry_test fail ~1 run
# in 4). `tests/common/test_db_url()` therefore always targets `<name>_test`;
# this script provisions it.
#
# Schema is cloned from the source database because this service does not own
# the trading schema — `migrations/` here is empty and the DB is provisioned
# externally (superproject `just migrate` / IAM). Cloning keeps the test schema
# identical to what the service actually runs against, with no data copied.
#
# Env:
#   PG_CONTAINER   docker container running Postgres   (default gridtokenx-postgres)
#   PG_USER        role owning both databases           (default gridtokenx_user)
#   SOURCE_DB      schema to clone from                 (default gridtokenx_trading)
#   TEST_DB        database to create                   (default ${SOURCE_DB}_test)

set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-gridtokenx-postgres}"
PG_USER="${PG_USER:-gridtokenx_user}"
SOURCE_DB="${SOURCE_DB:-gridtokenx_trading}"
TEST_DB="${TEST_DB:-${SOURCE_DB}_test}"
RECREATE=0
[ "${1:-}" = "--recreate" ] && RECREATE=1

psql_db() { docker exec -i "$PG_CONTAINER" psql -q -U "$PG_USER" -d "$1" "${@:2}"; }

if ! docker ps --format '{{.Names}}' | grep -qx "$PG_CONTAINER"; then
  echo "error: Postgres container '$PG_CONTAINER' is not running (try: just orb-up)" >&2
  exit 1
fi

exists=$(psql_db postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${TEST_DB}';")

if [ -n "$exists" ] && [ "$RECREATE" = "1" ]; then
  echo "dropping ${TEST_DB}"
  # Terminate leftover sessions first; a single stale connection makes DROP fail.
  psql_db postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${TEST_DB}';" >/dev/null
  psql_db postgres -c "DROP DATABASE ${TEST_DB};"
  exists=""
fi

if [ -z "$exists" ]; then
  echo "creating ${TEST_DB}"
  psql_db postgres -c "CREATE DATABASE ${TEST_DB};"
  echo "cloning schema from ${SOURCE_DB} (no data)"
  # TEMPLATE would be simpler but requires zero connections to the source, and
  # the running service holds a pool open. pg_dump needs no such exclusivity.
  # Not piped through the host: keeping it inside the container avoids any local
  # pg_dump/server version mismatch.
  docker exec "$PG_CONTAINER" bash -lc \
    "pg_dump -U ${PG_USER} --schema-only ${SOURCE_DB} | psql -q -U ${PG_USER} -d ${TEST_DB}"
else
  echo "${TEST_DB} already exists (use --recreate to rebuild)"
fi

tables=$(psql_db "$TEST_DB" -tAc "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';")
echo "${TEST_DB}: ${tables} tables"
if [ "$tables" -eq 0 ]; then
  echo "error: no tables — is ${SOURCE_DB} migrated?" >&2
  exit 1
fi
echo "ready: cargo test -p trading-service"
