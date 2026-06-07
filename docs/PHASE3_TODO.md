# Phase 3 — TODO & Test List

> Scope: `GET /api/v1/trades` + `GET /api/v1/trades/export`. No migrations.
> Parent: [BACKEND_GAP_PLAN.md](BACKEND_GAP_PLAN.md). Created 2026-06-07.
>
> **STATUS: DONE (2026-06-07).**

## Key decision — no `trades` table exists

The plan assumed a `trades` table; the live DB has **none** (verified
`docker exec gridtokenx-postgres psql -c "\dt"` — only `order_matches`,
`settlements`, `trading_orders`, …). A **settlement IS a completed trade**:
`settlements` carries `buyer_id`/`seller_id` (user FKs), `energy_amount`,
`price_per_kwh`, `total_amount`, `fee_amount`, `wheeling_charge`, `loss_cost`,
`effective_energy`, zones, status, `transaction_hash`, `created_at`. Purpose-built
indexes already exist: `idx_settlements_buyer_time` / `idx_settlements_seller_time`
(INCLUDE value cols). **Backing = `settlements`, scoped to the authed user.**

## Endpoints

| Method + path | Frontend caller | Backing |
|---|---|---|
| GET `/api/v1/trades` | `getTrades`, `getTradeHistory` (trading.ts L56,197) | `settlements` (buyer OR seller = user) |
| GET `/api/v1/trades/export?format=csv\|json` | `exportTradingHistory` (L306) | same, serialized CSV (default) / JSON |

Both list endpoints share one handler/response. `getTrades` is the dominant
consumer (5 components) → response matches `types/trading.ts` `TradeRecord`
(string decimals, `role`/`counterparty_id`, `executed_at`, `total_count`).
`role`/`counterparty_id` computed relative to the authenticated user.
Verified usage: `parseFloat(trade.price)`, `trade.role === 'buyer'`
(`TradingPositions.tsx:302`), `trade.buyer_zone_id` (`useActiveTrades.ts:38`).

## Changes

- **`trading-core/src/traits.rs`** — `SettlementRepository::list_settlements_for_user(user_id, limit, offset) -> (Vec<Settlement>, i64)`.
- **`trading-persistence/src/repositories/settlement.rs`** — impl: `SELECT * … WHERE buyer_id=$1 OR seller_id=$1 ORDER BY created_at DESC LIMIT/OFFSET` + `COUNT(*)` total. Reuses existing `SettlementDb` FromRow + `From<SettlementDb> for Settlement`.
- **`trading-api/src/rest.rs`** — DTOs `TradesQuery`, `TradeRecordResponse` (superset of `TradeRecord` + `getTradeHistory` aliases), `TradesListResponse` (dual `total`/`total_count`); pure builders `build_trade_record`, `build_trades_response`, `csv_field` (RFC-4180 escape), `trades_to_csv`; handlers `get_trades` (JSON), `export_trades` (CSV default / `?format=json`). Decimals serialized as **string** (codebase convention).
- **`trading-api/src/startup.rs`** — 2 routes.
- **`trading-api/tests/endpoint_tests.rs`** — mock `list_settlements_for_user` (filter+sort+page) + `get_json_as` / `mk_settlement_between` helpers + 5 integration tests.

## APISIX

No change — route 2 already lists `/api/v1/trades` + `/api/v1/trades/*`
(`apisix_conf/apisix.yaml:91-92`).

## Tests (all green)

Unit (`rest.rs`, DB-free pure builders):
- `trade_record_role_buyer` — role+counterparty+string decimals when user=buyer.
- `trade_record_role_seller` — role+counterparty when user=seller.
- `trade_record_null_optionals_zero` — `None` wheeling/loss/effective → `"0"`.
- `trades_response_dual_totals` — `total` == `total_count`.
- `csv_field_escapes` — comma/quote/newline quoting + quote doubling.
- `csv_has_header_and_row` — header line + exactly N data rows.

HTTP integration (`tests/endpoint_tests.rs`):
- `test_trades_empty_endpoint` — 200, empty, zero totals.
- `test_trades_user_scoped_and_role` — only buyer-or-seller rows; role+counterparty per row.
- `test_trades_pagination` — `limit`/`offset` page; `total` ignores paging.
- `test_trades_export_csv` — `text/csv` + `attachment; filename="trades.csv"`, header + N rows.
- `test_trades_export_json_format` — `?format=json` → JSON array.

Result: `cargo test -p trading-api` lib+tests **30 pass**. Full-stack e2e
(`trading-service --test api_routing_test`) **1 pass** (Redis+Solana+PG+chain-bridge up).
clippy: 0 new warnings.

## Pending (needs live stack)
- Gateway smoke: `curl https://apisix.gridtokenx-coresystem.orb.local/api/v1/trades`
  and `/trades/export` (after trading-service `:8093` + APISIX up, with JWT).
