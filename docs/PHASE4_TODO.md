# Phase 4 — TODO & Test List

> Scope: `POST/GET /api/v1/price-alerts` + `DELETE /api/v1/price-alerts/{id}`. No migrations.
> Parent: [BACKEND_GAP_PLAN.md](BACKEND_GAP_PLAN.md). Created 2026-06-07.
>
> **STATUS: DONE (2026-06-07).**

## Key decision — `symbol` has no column → stored in `note`

Frontend `createPriceAlert` sends `{ symbol, target_price, condition }`
(`lib/api/trading.ts:223`), and the wire type `PriceAlert` (`types/features.ts:85`)
reads back `{ id, user_id, symbol, target_price, condition, is_active, created_at }`.
The live `price_alerts` table has **no `symbol` column** (verified
`docker exec gridtokenx-postgres psql -c "\d price_alerts"`): only
`target_price`, `condition` (`alert_condition` enum), `status` (`alert_status`),
`triggered_at/_price`, `repeat`, `note VARCHAR(200)`, `created_at`, `updated_at`.

Resolution (plan option **a**): store `symbol` in `note`; echo `note → symbol`
on read. `is_active` is derived (`status == active`). The energy market is single-
symbol, so no schema churn — avoids an IAM migration.

## Endpoints

| Method + path | Frontend caller | Backing |
|---|---|---|
| POST `/api/v1/price-alerts` | `createPriceAlert` (trading.ts:223) | INSERT, status defaults `active` |
| GET `/api/v1/price-alerts` | `listPriceAlerts` (trading.ts:235) | user-scoped, newest first |
| DELETE `/api/v1/price-alerts/{id}` | `deletePriceAlert` (trading.ts:242) | owner-scoped delete; 404 if no row |

All handlers require role `ApiGateway|Admin` and scope to `UserContext.user_id`.
`condition` parsed case-insensitively (`above|below|crosses`); bad value → 400.
`target_price` parsed via `Decimal::from_str`; bad value → 400. Decimals as strings.

## Changes

- **`trading-core/src/types.rs`** — enums `AlertCondition` (above/below/crosses,
  `alert_condition`) + `AlertStatus` (active/triggered/cancelled, `alert_status`),
  same derive set as `OrderSide` (sqlx::Type + strum).
- **`trading-core/src/models.rs`** — `PriceAlert` (domain row) + `NewPriceAlert` (create input).
- **`trading-core/src/traits.rs`** — `PriceAlertRepository` trait:
  `create_price_alert` / `list_price_alerts_for_user` / `delete_price_alert(id, user) -> bool`.
- **`trading-persistence/src/repositories/price_alert.rs`** (new) — `PriceAlertDb`
  FromRow + `From<PriceAlertDb> for PriceAlert`; runtime `sqlx::query_as` impl
  (INSERT … RETURNING *, SELECT … ORDER BY created_at DESC, DELETE … rows_affected).
  Registered in `repositories/mod.rs`.
- **`trading-api/src/rest.rs`** — DTOs `CreatePriceAlertRequest`, `PriceAlertResponse`;
  pure builder `build_price_alert_response` (note→symbol, status→is_active);
  handlers `create_price_alert` / `list_price_alerts` / `delete_price_alert`.
- **`trading-api/src/startup.rs`** — 2 routes (POST+GET on collection, DELETE on item).
- **`trading-api/src/state.rs`**, **`bin/trading-service/src/builder.rs`**,
  **`bin/trading-service/src/main.rs`**, **`bin/.../tests/api_routing_test.rs`** —
  wire `price_alert_repo: Arc<dyn PriceAlertRepository>` through `Infrastructure` → `AppState`.
- **`trading-api/tests/endpoint_tests.rs`** — `MockSystem.price_alerts` field +
  `PriceAlertRepository` mock impl + `post_json_as` helper + 6 integration tests.

### Drive-by clippy fix

`build_matching_status` (Phase 1) had two `sell_min.unwrap()`/`buy_max.unwrap()`
that tripped `clippy::unwrap_used = deny`. Rewrote as a single `match (buy_max,
sell_min)` guard — no behavior change, lint clean.

## APISIX

Added `/api/v1/price-alerts` + `/api/v1/price-alerts/*` to route 2 (trading
upstream) in superproject `apisix_conf/apisix.yaml`.

## Tests (all green)

Unit (`rest.rs`, DB-free builder):
- `price_alert_active_maps_symbol_and_decimal` — note→symbol, decimal→string, is_active.
- `price_alert_triggered_inactive_empty_symbol` — null note → "", status≠active → is_active false.

HTTP integration (`tests/endpoint_tests.rs`):
- `test_price_alert_create_maps_symbol_and_active` — POST echoes symbol/condition, is_active.
- `test_price_alert_create_then_list_newest_first` — list ordering.
- `test_price_alert_list_user_scoped` — other user's alert not visible.
- `test_price_alert_delete_roundtrip` — create → delete 200 → list empty.
- `test_price_alert_delete_foreign_404` — unknown id → 404.
- `test_price_alert_bad_condition_400` — invalid condition → 400.

Result: `cargo test -p trading-api` lib+tests **38 pass** (was 30). Full-stack e2e
(`trading-service --test api_routing_test`, PG+Redis+Solana+chain-bridge up) **1 pass**.
`cargo clippy -p trading-api` **0 errors**.

## Pending (needs live stack)
- Gateway smoke: `curl -X POST/GET/DELETE
  https://apisix.gridtokenx-coresystem.orb.local/api/v1/price-alerts` (after
  trading-service `:8093` + APISIX up, with JWT).
