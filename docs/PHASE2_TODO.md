# Phase 2 — TODO & Test List

> Scope: `markets/settlement-stats` + `markets/orderbook`. No migrations.
> Parent: [BACKEND_GAP_PLAN.md](BACKEND_GAP_PLAN.md). Created 2026-06-07.
>
> **STATUS: DONE (2026-06-07).** Decision: orderbook = **cross-zone merge**.

## Endpoints

| Method + path | Frontend caller | Backing |
|---|---|---|
| GET `/api/v1/markets/settlement-stats` | `getSettlementStats` (trading.ts L124-133) | `settlements` table |
| GET `/api/v1/markets/orderbook` | `getP2POrderBook` (L94-98) | `trading_orders` table |

## Changes

- **`trading-core/src/models.rs`** — `SettlementStats` struct (`confirmed_count` = DB `completed`).
- **`trading-core/src/traits.rs`** — `SettlementRepository::get_settlement_stats`,
  `OrderRepository::get_all_active_orders`.
- **`trading-persistence/src/repositories/settlement.rs`** — `get_settlement_stats` SQL
  (`COUNT(*) FILTER` per status + `COALESCE(SUM(total_amount) FILTER WHERE completed,0)`),
  `SettlementStatsRow` FromRow.
- **`trading-persistence/src/repositories/order.rs`** — `get_all_active_orders` (all zones).
- **`trading-api/src/rest.rs`** — DTOs `SettlementStatsResponse`, `P2POrderBookResponse`;
  pure `build_settlement_stats_response`, `build_p2p_orderbook` (price-level aggregate, bids
  desc / asks asc); handlers `get_settlement_stats`, `get_p2p_orderbook`.
- **`trading-api/src/startup.rs`** — 2 routes.
- **`trading-api/tests/endpoint_tests.rs`** — mock impls extended.
- **`apisix_conf/apisix.yaml`** (superproject) — route 2 += `markets/settlement-stats`,
  `markets/orderbook`.

## Ground truth

- DB `settlements.status` CHECK IN (`pending`,`processing`,`completed`,`failed`) — frontend
  "confirmed" = DB "completed". Value col = `total_amount`.
- sqlx is runtime (`query_as::<_,T>`), no offline cache / DB needed to compile.
- `OrderBookEntry{side, energy_amount, price_per_kwh, zone_id, ...}` `models.rs:386`.

## Tests (all green)

Unit (`rest.rs`, DB-free pure builders):
- `settlement_stats_mapping` — field map + value f64.
- `orderbook_empty` — `{asks:[],bids:[]}`.
- `orderbook_split_and_sort` — buys→bids desc, sells→asks asc.
- `orderbook_aggregates_price_level` — same-price amounts summed.

HTTP integration (`tests/endpoint_tests.rs`, axum router + data-driven mock):
- `test_markets_config_endpoint` — 200, 6 contract fields, defaults (4.5 / 50bps / 20.0).
- `test_p2p_market_prices_endpoint` — 200, wheeling/loss maps, model="proportional".
- `test_matching_status_empty_endpoint` — 200, zeros, reason="no orders".
- `test_matching_status_crossing_endpoint` — seeded buy/sell cross → can_match, pending_matches=1.
- `test_settlement_stats_empty_endpoint` — 200, all zero.
- `test_settlement_stats_endpoint` — seeded mixed statuses → counts + total (completed only) = 60.
- `test_orderbook_empty_endpoint` — 200, empty arrays.
- `test_orderbook_endpoint` — seeded → bids desc / asks asc / price-level aggregate.

Mock made data-driven: `get_all_active_orders`, `get_active_buy/sell_orders`,
`get_settlement_stats` now read seeded `self.orders` / `self.settlements`.
Added `setup_test_state_with_mock` + factories `mk_order` / `mk_settlement`.

Result: `cargo test` lib **22 pass** + endpoint_tests **9 pass** (1 prior + 8 new).
`clippy --tests` 0 warnings. Note: `test_api_routing_e2e` still needs live Postgres (infra).

## Pending (needs live stack)
- Gateway smoke: `curl https://apisix.gridtokenx-coresystem.orb.local/api/v1/markets/settlement-stats`
  and `/markets/orderbook` (after `just orb-rebuild` + `just db-up`).
