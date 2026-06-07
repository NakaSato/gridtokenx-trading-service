# Phase 1 — TODO & Test List

> Scope: 3 read-only market endpoints. No DB writes, no migrations.
> Parent plan: [BACKEND_GAP_PLAN.md](BACKEND_GAP_PLAN.md). Created 2026-06-07.
>
> **STATUS: DONE (2026-06-07).** Decision: **B** (Config + env vars).
> - `MarketConfig` added `crates/trading-core/src/config/mod.rs` (env `MARKET_*`, `#[serde(default)]`).
> - Handlers `get_market_config` / `get_p2p_market_prices` / `get_matching_status` +
>   pure `build_matching_status` in `crates/trading-api/src/rest.rs`.
> - Routes registered `crates/trading-api/src/startup.rs`.
> - APISIX route 2 re-added `markets/config`, `markets/matching-status`, `markets/p2p/*`.
> - Tests: 6 unit (matching-status states + config invariants) — all green (20 total pass).
> - `cargo check`/`clippy` clean. Pending: gateway smoke after APISIX reload.

## Endpoints

| Method + path | Frontend caller (`lib/api/trading.ts`) | Backing |
|---|---|---|
| GET `/api/v1/markets/config` | `getMarketConfig` (L184-194) | `P2PConfig` (new) |
| GET `/api/v1/markets/p2p/market-prices` | `getP2PMarketPrices` (L170-180) | `P2PConfig` (new) |
| GET `/api/v1/markets/matching-status` | `getMatchingStatus` (L109-120) | `order_repo` (exists) |

## Ground truth (verified)

- Price constants: wheeling `0.02` cross-zone, loss `1.01` intra / `1.03` cross — hardcoded
  `crates/trading-logic/src/energy.rs:40,52`.
- `base_price_thb_kwh`, `grid_import/export_price`, `transaction_fee_bps`,
  `min/max_price_per_kwh`, `loss_allocation_model` — **defined nowhere yet**.
- Stub `crates/trading-logic/src/p2p_config.rs` (15B placeholder) = intended home.
- `OrderRepository::get_active_buy_orders()` / `get_active_sell_orders()` →
  `Vec<TradingOrder>`, `crates/trading-core/src/traits.rs:75,78`.
- `AppState` holds `config`, `order_repo`, `matcher` — `crates/trading-api/src/state.rs`.
- Handler pattern + DTOs live in `crates/trading-api/src/rest.rs`; routes in
  `crates/trading-api/src/startup.rs:80-192`.

## Decision gate (blocks #1, #2)

- [ ] Price source: **(A)** hardcode constants in `p2p_config.rs` (fast), or
  **(B)** add fields to `Config` + env vars (configurable). _Recommend A for Phase 1._

## TODO

### Setup
- [ ] Fill `p2p_config.rs` stub: `P2PConfig` struct + `default()` holding base/import/export
  price, fee_bps, min/max price, `wheeling_charges` map, `loss_factors` map,
  `loss_allocation_model`. Reuse energy.rs values (wheeling 0.02, loss 1.01/1.03).
- [ ] Export `p2p_config` from `crates/trading-logic/src/lib.rs`.

### #1 — GET `/api/v1/markets/config`
- [ ] Response DTO in `rest.rs`: `{base_price_thb_kwh, grid_import_price_thb_kwh,
  grid_export_price_thb_kwh, transaction_fee_bps, min_price_per_kwh, max_price_per_kwh}`.
- [ ] Handler `get_market_config(State)` → reads `P2PConfig`, returns `Json`.
- [ ] Route `.route("/api/v1/markets/config", get(get_market_config))` in `startup.rs`.

### #2 — GET `/api/v1/markets/p2p/market-prices`
- [ ] Response DTO: `{base_price_thb_kwh, grid_import_price_thb_kwh, grid_export_price_thb_kwh,
  loss_allocation_model, wheeling_charges: map, loss_factors: map}`.
- [ ] Handler `get_p2p_market_prices(State)` → `P2PConfig`, returns `Json`.
- [ ] Route `.route("/api/v1/markets/p2p/market-prices", get(...))`.

### #3 — GET `/api/v1/markets/matching-status`
- [ ] Response DTO: `{pending_buy_orders, pending_sell_orders, pending_matches,
  buy_price_range:{min,max}, sell_price_range:{min,max}, can_match, match_reason}`.
- [ ] Handler `get_matching_status(State)`: call `order_repo.get_active_buy_orders()` +
  `get_active_sell_orders()`; compute counts, min/max price per side;
  `can_match = buy_max >= sell_min`; `pending_matches` = crossable pair count;
  `match_reason` string.
- [ ] Route `.route("/api/v1/markets/matching-status", get(...))`.
- [ ] Empty-side guard: ranges `{0,0}`, `can_match=false`, reason `"no orders"`.

### Wire + verify
- [ ] `cargo check -p trading-api` clean.
- [ ] `cargo clippy -p trading-api -- -D warnings`.
- [ ] Re-add 3 routes to APISIX `apisix_conf/apisix.yaml` route 2
  (`markets/config`, `markets/p2p/*`, `markets/matching-status`) + reload.

## Test list

### Unit (`rest.rs` `#[cfg(test)]` or `crates/trading-api/tests/endpoint_tests.rs`)
- [ ] `matching_status` both sides empty → counts 0, ranges {0,0}, `can_match=false`, reason="no orders".
- [ ] `matching_status` buys only → pending_sell=0, can_match=false, reason="no sell liquidity".
- [ ] `matching_status` sells only → mirror of buys-only.
- [ ] `matching_status` crossing book (buy_max ≥ sell_min) → `can_match=true`, `pending_matches>0`, ranges correct.
- [ ] `matching_status` non-crossing (buy_max < sell_min) → `can_match=false`, `pending_matches=0`.
- [ ] `matching_status` price-range calc: multiple orders/side → min/max correct.
- [ ] `P2PConfig::default()` invariants: `min_price < max_price`, `fee_bps >= 0`,
  intra-zone wheeling=0, cross-zone wheeling=0.02, loss factors 1.01/1.03.

### HTTP integration (axum router + mock repos)
- [ ] GET `markets/config` → 200, body has all 6 fields, types correct.
- [ ] GET `markets/p2p/market-prices` → 200, `wheeling_charges` + `loss_factors` non-empty,
  `loss_allocation_model` string present.
- [ ] GET `markets/matching-status` → 200, JSON shape matches frontend type exactly (snake_case).
- [ ] All 3 routes registered (no 404) via router oneshot.

### Contract (frontend parity)
- [ ] Field names snake_case match `trading.ts` L109-120 (matching-status), L170-180 (p2p-prices),
  L184-194 (config). No missing/extra keys.

### Gateway smoke (post-deploy, manual)
- [ ] `curl https://apisix.gridtokenx-coresystem.orb.local/api/v1/markets/config` → 200 JSON.
- [ ] same for `/markets/p2p/market-prices`, `/markets/matching-status`.
