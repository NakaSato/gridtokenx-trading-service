#!/usr/bin/env bash
#
# Stamp an expiry on orders that were written without one.
#
#   ./scripts/backfill-order-expiry.sh            # dry run: report only, no writes
#   ./scripts/backfill-order-expiry.sh --apply    # perform the UPDATE
#
# Why these rows exist: `expires_at` was missing from both INSERT column lists in
# `trading-persistence/src/repositories/order.rs`, so it was silently dropped on
# every insert. Orders created through REST, gRPC or the recurring evaluator
# landed with NULL, and the ReaperWorker — which matches `expires_at < now()` —
# could never reap them. They rest in the book forever, and the matcher re-reads
# the whole active book every cycle, so they cost something on every single cycle.
#
# Only rows that are STILL OPEN and have NULL expiry are touched. Terminal orders
# (filled/cancelled/expired) are left alone: their NULL is now historical record,
# and rewriting it would falsify what the order actually was.
#
# The stamped value is `created_at + TTL`, not `now() + TTL`: these orders were
# *intended* to expire relative to placement, so an order placed two days ago
# becomes immediately reapable rather than being granted a fresh lease. Rows are
# left for the ReaperWorker to transition, so expiry emits its normal
# `OrderUpdate` outbox event instead of this script mutating status directly.
#
# Env:
#   PG_CONTAINER  Postgres container   (default gridtokenx-postgres)
#   PG_USER       role                 (default gridtokenx_user)
#   DB            database             (default gridtokenx_trading)
#   TTL_SECS      lifetime from creation (default 900 = ORDER_DEFAULT_TTL_SECS)

set -euo pipefail

PG_CONTAINER="${PG_CONTAINER:-gridtokenx-postgres}"
PG_USER="${PG_USER:-gridtokenx_user}"
DB="${DB:-gridtokenx_trading}"
TTL_SECS="${TTL_SECS:-900}"
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

q() { docker exec -i "$PG_CONTAINER" psql -q -U "$PG_USER" -d "$DB" -tAc "$1"; }

OPEN="status IN ('pending','active','partially_filled')"
TARGET="${OPEN} AND expires_at IS NULL"

echo "database: ${DB}   ttl: ${TTL_SECS}s from created_at"
echo
echo "open orders with no expiry, by age:"
q "SELECT '  ' || side || '  created ' || created_at::date
        || '  would expire ' || (created_at + interval '${TTL_SECS} seconds')::timestamptz(0)
        || CASE WHEN created_at + interval '${TTL_SECS} seconds' <= now()
                THEN '  (immediately reapable)' ELSE '  (still live)' END
   FROM trading_orders WHERE ${TARGET} ORDER BY created_at;"

TOTAL=$(q "SELECT count(*) FROM trading_orders WHERE ${TARGET};")
PAST=$(q "SELECT count(*) FROM trading_orders WHERE ${TARGET} AND created_at + interval '${TTL_SECS} seconds' <= now();")
echo
echo "would update ${TOTAL} row(s); ${PAST} become reapable on the reaper's next tick"

if [ "$TOTAL" = "0" ]; then
  echo "nothing to do"
  exit 0
fi

if [ "$APPLY" != "1" ]; then
  echo
  echo "DRY RUN — no changes written. Re-run with --apply to perform the update."
  exit 0
fi

echo
echo "applying..."
# Guarded by the same predicate, so re-running is a no-op: once stamped, a row no
# longer has NULL expiry and is not matched again.
UPDATED=$(q "WITH upd AS (
    UPDATE trading_orders
       SET expires_at = created_at + interval '${TTL_SECS} seconds'
     WHERE ${TARGET}
     RETURNING 1)
  SELECT count(*) FROM upd;")
echo "updated ${UPDATED} row(s)"
echo "remaining open orders with no expiry: $(q "SELECT count(*) FROM trading_orders WHERE ${TARGET};")"
echo "the ReaperWorker will transition the lapsed ones within its 10s cadence"
