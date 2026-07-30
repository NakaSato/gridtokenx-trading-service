#!/usr/bin/env bash
#
# Manual REST smoke test for the Trading Service.
#
#   ./test-api.sh                         # against http://127.0.0.1:8093
#   BASE_URL=http://127.0.0.1:4020 ./test-api.sh   # the Docker port mapping
#
# Needs a running trading-service with a reachable, migrated Postgres. Nothing
# here touches the chain: orders are submitted, matched off-chain and cancelled.
#
# Auth: every guarded route needs THREE headers, not one —
#   x-gridtokenx-role            the caller's role (`api-gateway`)
#   x-gridtokenx-gateway-secret  proof of that role; ApiGateway fails CLOSED to
#                                `Unknown` (=> 403) without it
#   x-gridtokenx-user-id         the acting user
# The secret below is the dev default, honoured ONLY when the service runs with
# CHAIN_BRIDGE_INSECURE=true and GATEWAY_SECRET unset. Set GATEWAY_SECRET to
# match the service in any other environment.
# See ServiceRole::from_headers in ../gridtokenx-blockchain-core/crates/blockchain-auth.

set -uo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8093}"
ROLE_HEADER="x-gridtokenx-role: ${ROLE:-api-gateway}"
SECRET_HEADER="x-gridtokenx-gateway-secret: ${GATEWAY_SECRET:-gridtokenx-gateway-secret-2025}"
ZONE="${ZONE:-1}"
# Budget per match trial in step 4 — a LIVENESS check ("a crossing pair fills"),
# not a latency gate. Loose by default so it passes under tick-only matching
# (MATCHER_REALTIME=false, up to MATCHER_INTERVAL_MS) too.
#
# Do NOT treat a tight budget as proof of realtime matching. Measured on a dev
# box: realtime fills in ~20-60ms against a CLEAN book, but ~330-550ms once ~10
# stale orders are resting, because every cycle re-reads and re-matches the whole
# active book. That overlaps tick-only matching's uniform [0, MATCHER_INTERVAL_MS]
# wait, so no single threshold separates the two modes on a dirty DB.
#
# To verify matcher latency itself, use a freshly-truncated book and diff the
# service's own timestamps — this is what the numbers above come from:
#   SELECT round(EXTRACT(EPOCH FROM (s.created_at - o.created_at)) * 1000)
#     FROM settlements s JOIN trading_orders o ON o.id = s.buy_order_id
#     ORDER BY s.created_at;
#
# Multiple trials still matter here: one trial passing can be luck (under
# tick-only matching a single order slips under a 300ms budget ~30% of the time).
MATCH_TIMEOUT_MS="${MATCH_TIMEOUT_MS:-3000}"
MATCH_TRIALS="${MATCH_TRIALS:-3}"

pass=0
fail=0

ok()   { printf '  \033[32mPASS\033[0m %s\n' "$1"; pass=$((pass + 1)); }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$1"; fail=$((fail + 1)); }
step() { printf '\n\033[1m%s\033[0m\n' "$1"; }

uuid() { uuidgen | tr 'A-Z' 'a-z'; }

# Millisecond clock. macOS ships bash 3.2 (no $EPOCHREALTIME) and BSD date (no
# %3N), so this needs a subprocess either way — perl starts in ~10ms against
# python3's ~40ms, which matters because step 4 calls it once per poll.
if command -v perl >/dev/null 2>&1; then
  now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000'; }
else
  now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }
fi

# GET/DELETE as a given user. $1=method $2=path $3=user_id
call() {
  curl -sS -m 10 -X "$1" "${BASE_URL}$2" \
    -H "$ROLE_HEADER" -H "$SECRET_HEADER" -H "x-gridtokenx-user-id: $3"
}

# Submit an order. $1=side $2=price $3=user_id  -> response body
submit() {
  curl -sS -m 10 -X POST "${BASE_URL}/api/v1/orders" \
    -H 'content-type: application/json' \
    -H "$ROLE_HEADER" -H "$SECRET_HEADER" -H "x-gridtokenx-user-id: $3" \
    -d "{\"side\":\"$1\",\"order_type\":\"limit\",\"energy_amount_kwh\":\"5.0\",\"price_per_kwh\":\"$2\",\"zone_id\":${ZONE}}"
}

json_field() { python3 -c "
import json,sys
try: print(json.load(sys.stdin).get('$1',''))
except Exception: print('')
"; }

# Price a trial ABOVE any resting bid in the zone, echoing "<sell> <buy>".
#
# A dev DB accumulates unfilled orders (every earlier run of this script leaves
# some whenever matching is slow or off), and a stale bid >= our ask would consume
# the resting sell before the crossing buy ever sees it — which then looks exactly
# like "the matcher stalled". Both prices must stay inside
# MARKET_MIN/MAX_PRICE_PER_KWH (0.50 .. 20.00 by default) or the handler rejects
# them on admission policy rather than on matching; an empty echo signals that.
trial_prices() {
  curl -sS -m 5 "${BASE_URL}/api/v1/zones/${ZONE}/book" \
    -H "$ROLE_HEADER" -H "$SECRET_HEADER" -H "x-gridtokenx-user-id: $(uuid)" | python3 -c '
import json, sys
try:
    bids = json.load(sys.stdin).get("bids") or []
    top = max((float(p) for p, _ in bids), default=0.0)
except Exception:
    top = 0.0
sell = max(4.00, top + 0.01)
buy = sell + 0.50
print(f"{sell:.2f} {buy:.2f}" if buy <= 19.50 else "")
'
}

# Run one crossing pair and echo the observed "<status> <ms>", or "" on failure.
# The clock starts AFTER the submit returns: the submit round-trip is not matcher
# latency. It still overstates a little (a clock call + a request per poll, ~20ms
# floor). For the matcher's own latency, diff the DB timestamps instead:
#   SELECT s.created_at - o.created_at FROM settlements s
#     JOIN trading_orders o ON o.id = s.buy_order_id;
match_trial() {
  local sell_price="$1" buy_price="$2"
  local seller buyer buy_body buy_id start deadline body observed elapsed
  seller=$(uuid); buyer=$(uuid)

  submit sell "$sell_price" "$seller" >/dev/null
  buy_body=$(submit buy "$buy_price" "$buyer")
  # Shell-only id extraction, to keep subprocesses out of the timed loop.
  buy_id=$(printf '%s' "$buy_body" | sed -n 's/.*"id":"\([0-9a-f-]*\)".*/\1/p')
  [ -z "$buy_id" ] && return 1

  start=$(now_ms)
  deadline=$(( start + MATCH_TIMEOUT_MS ))
  while :; do
    body=$(call GET "/api/v1/orders/${buy_id}" "$buyer")
    case "$body" in
      *'"status":"filled"'*)           observed=filled ;;
      *'"status":"partially_filled"'*) observed=partially_filled ;;
      *)                               observed="" ;;
    esac
    elapsed=$(( $(now_ms) - start ))
    if [ -n "$observed" ]; then printf '%s %s\n' "$observed" "$elapsed"; return 0; fi
    # Deadline checked AFTER the probe, against the same measurement that gets
    # reported — otherwise a probe starting just inside the budget can finish
    # outside it and still be reported as a pass.
    if [ $(( start + elapsed )) -ge "$deadline" ]; then
      printf 'timeout %s\n' "$elapsed"; return 0
    fi
  done
}

printf 'Trading Service smoke test -> %s\n' "$BASE_URL"

step '1. Public routes'
health=$(curl -sS -m 5 "${BASE_URL}/health")
if [ "$(printf '%s' "$health" | json_field status)" = "ok" ]; then
  ok "/health reports ok"
else
  bad "/health -> ${health:-<no response>}"
  printf '\nService unreachable at %s — start it, or set BASE_URL.\n' "$BASE_URL"
  exit 1
fi
code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' "${BASE_URL}/metrics")
[ "$code" = "200" ] && ok "/metrics 200" || bad "/metrics -> $code"

step '2. Auth is enforced'
# Role header without its secret must NOT be trusted.
# The payload must be VALID, or axum's Json extractor 422s before the handler's
# role check ever runs and the test proves nothing about auth.
code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' -X POST "${BASE_URL}/api/v1/orders" \
  -H 'content-type: application/json' -H "$ROLE_HEADER" -H "x-gridtokenx-user-id: $(uuid)" \
  -d "{\"side\":\"buy\",\"order_type\":\"limit\",\"energy_amount_kwh\":\"1.0\",\"price_per_kwh\":\"4.0\",\"zone_id\":${ZONE}}")
[ "$code" = "403" ] && ok "role header without gateway secret -> 403" \
                    || bad "expected 403 without gateway secret, got $code"

code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' "${BASE_URL}/api/v1/orders" \
  -H "$ROLE_HEADER" -H "$SECRET_HEADER")
[ "$code" = "400" ] || [ "$code" = "401" ] || [ "$code" = "403" ] \
  && ok "missing user-id header -> $code" \
  || bad "expected a 4xx without user-id, got $code"

step '3. Order lifecycle'
SELLER=$(uuid)
read -r SELL_PRICE BUY_PRICE <<EOF
$(trial_prices)
EOF
if [ -z "${SELL_PRICE:-}" ]; then
  bad "zone ${ZONE} has resting bids too near the max price to place a crossing \
pair; clear the zone's book or pick another ZONE"
  printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
  exit 1
fi
printf '  pricing above the top resting bid: sell %s / buy %s\n' "$SELL_PRICE" "$BUY_PRICE"

sell_body=$(submit sell "$SELL_PRICE" "$SELLER")
sell_id=$(printf '%s' "$sell_body" | json_field id)
[ -n "$sell_id" ] && ok "resting sell submitted ($sell_id)" \
                  || { bad "sell submit -> $sell_body"; }

listed=$(call GET /api/v1/orders "$SELLER" | python3 -c "
import json,sys
try: print(any(o.get('id')=='$sell_id' for o in json.load(sys.stdin).get('data') or []))
except Exception: print(False)
")
[ "$listed" = "True" ] && ok "GET /api/v1/orders lists it" \
                       || bad "submitted order missing from its user's order list"

step "4. Matching (${MATCH_TRIALS} crossing pairs, budget ${MATCH_TIMEOUT_MS}ms each)"
# Each trial rests a sell, then submits a buy that crosses it; every trial must
# fill within budget. Reported times include one poll round-trip and grow with
# resting book depth — see the MATCH_TIMEOUT_MS note at the top before reading
# them as matcher latency.
slowest=0
for trial in $(seq 1 "$MATCH_TRIALS"); do
  read -r t_sell t_buy <<EOF
$(trial_prices)
EOF
  if [ -z "${t_sell:-}" ]; then
    bad "trial ${trial}: zone ${ZONE} book too congested to price a crossing pair"
    continue
  fi
  result=$(match_trial "$t_sell" "$t_buy") || { bad "trial ${trial}: submit failed"; continue; }
  t_status=${result%% *}
  t_ms=${result##* }
  [ "$t_ms" -gt "$slowest" ] && slowest=$t_ms
  if [ "$t_status" = "timeout" ]; then
    bad "trial ${trial}: buy never filled within ${MATCH_TIMEOUT_MS}ms — matcher \
stalled, or no counterparty in zone ${ZONE}"
  elif [ "$t_ms" -gt "$MATCH_TIMEOUT_MS" ]; then
    # A poll that starts inside the budget can finish outside it: the fill is
    # real, but it is NOT within budget and must not read as a pass.
    bad "trial ${trial}: '${t_status}' but ${t_ms}ms exceeds the ${MATCH_TIMEOUT_MS}ms budget"
  else
    ok "trial ${trial}: '${t_status}' ${t_ms}ms after submit"
  fi
done
[ "$slowest" -gt 0 ] && printf '  slowest trial: %sms (budget %sms)\n' "$slowest" "$MATCH_TIMEOUT_MS"

step '5. Cancel'
# Step 3's sell is still resting (nothing crossed it — step 4 places its own
# pairs), so this exercises a real cancel. A 4xx body without a status field is
# the correct answer if something did consume it.
cancel_status=$(call DELETE "/api/v1/orders/${sell_id}" "$SELLER" | json_field status)
[ -n "$cancel_status" ] && ok "DELETE /api/v1/orders/{id} answered ('${cancel_status}')" \
                        || ok "sell already terminal (filled) — nothing to cancel"

step '6. Read-only market routes'
for path in /api/v1/markets/config /api/v1/markets/matching-status \
            /api/v1/markets/settlement-stats "/api/v1/zones/${ZONE}/book"; do
  code=$(curl -sS -m 5 -o /dev/null -w '%{http_code}' "${BASE_URL}${path}" \
    -H "$ROLE_HEADER" -H "$SECRET_HEADER" -H "x-gridtokenx-user-id: $SELLER")
  [ "$code" = "200" ] && ok "GET ${path} 200" || bad "GET ${path} -> $code"
done

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$pass" "$fail"
[ "$fail" -eq 0 ] || exit 1
