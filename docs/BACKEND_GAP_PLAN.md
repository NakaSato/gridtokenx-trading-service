# Backend Gap Plan — Trading Service REST endpoints

> Status: PLAN (pre-implementation). Created 2026-06-07.
> Driver: frontend `gridtokenx-trading` calls 9 REST paths that have **no route** in
> `trading-api`. DB schema and most repos/models already exist — gap is the REST/repo
> wiring layer, not data design.

## Context

- Frontend contracts: `gridtokenx-trading/lib/api/trading.ts`.
- Backend router (source of truth): `crates/trading-api/src/startup.rs:80-192`.
- DB schema owned by IAM (shared Postgres): tables `price_alerts`,
  `recurring_orders`, `conditional_orders`, `trades`, `settlements`, `trading_orders`
  **already migrated** (`gridtokenx-iam-service/migrations/*`). **No new migrations needed.**
- Config source: `crates/trading-core/src/config/mod.rs` (`Config`), `models.rs`
  (`ZoneConfig.wheeling_charge:465`, `wheeling_charge:314`, `loss_factor:315`),
  energy logic `crates/trading-logic/src/energy.rs` (`calculate_wheeling_charge:32`,
  `calculate_loss_factor:45`).

## Missing endpoints (frontend → backend reality)

| # | Method + path | Frontend caller | Backing today | New work |
|---|---|---|---|---|
| 1 | GET `/api/v1/markets/config` | `getMarketConfig` | `Config` + zone cfg | handler only |
| 2 | GET `/api/v1/markets/p2p/market-prices` | `getP2PMarketPrices` | `Config`, `ZoneConfig`, energy | handler only |
| 3 | GET `/api/v1/markets/matching-status` | `getMatchingStatus` | `order.get_active_buy_orders/get_active_sell_orders` exist | handler aggregates |
| 4 | GET `/api/v1/markets/settlement-stats` | `getSettlementStats` | `settlements` table; `get_pending_settlements` exists | + repo `get_settlement_stats` |
| 5 | GET `/api/v1/markets/orderbook` | `getP2POrderBook` | `order.get_active_orders_by_zone` (per-zone) | + repo `get_all_active_orders`, handler |
| 6 | GET `/api/v1/trades`, GET `/api/v1/trades/export` | `getTrades`, `getTradeHistory`, `exportTradingHistory` | ✅ DONE — `settlements` table (no `trades` table exists) | repo `list_settlements_for_user`, 2 handlers (json + csv) |
| 7 | POST/GET `/api/v1/price-alerts`, DELETE `/api/v1/price-alerts/{id}` | `createPriceAlert`/`listPriceAlerts`/`deletePriceAlert` | ✅ DONE — new `price_alert.rs` repo; `symbol` stored in `note` (no column) | repo create/list/delete, 3 handlers, route |
| 8 | POST/GET `/api/v1/orders/recurring`, GET/DELETE `/{id}`, POST `/{id}/pause`, POST `/{id}/resume` | recurring CRUD | ✅ DONE — repo +5 methods; `recurring_repo` wired to AppState (was absent); `next_execution_at` helper; decimals as strings | repo +5 methods, 6 handlers, 4 routes |
| 9 | (optional) recurring/alert execution workers | n/a (frontend = CRUD only) | stubs `recurring_evaluator.rs`, `trigger_evaluator.rs` (15B placeholders) | deferred |

### Contract mismatches to resolve

- **`price_alerts` has no `symbol` column** (frontend `createPriceAlert` sends `symbol`,
  `target_price`, `condition`). Options: (a) store `symbol` in existing `note VARCHAR(200)`,
  (b) ignore (single energy market), (c) add column via new IAM migration. Recommend (b)/(a) —
  avoid schema churn. Table cols: `target_price DECIMAL`, `condition alert_condition`,
  `status alert_status`, `triggered_at/price`, `repeat`, `note` (`add_price_alerts.sql`).
- **`recurring_orders`** matches frontend `CreateRecurringOrderRequest` 1:1 (side, energy_amount,
  max/min_price_per_kwh, interval_type, interval_value, max_executions, name, description).
- Frontend `getOrderBook(zoneId)` already uses `/api/v1/zones/{z}/book` (exists). The P2P
  `getP2POrderBook` (#5) is the cross-zone aggregate — confirm whether product wants
  all-zones merge or alias to zone 1.

## Implementation phases (suggested order, low→high risk)

**Phase 1 — read-only market endpoints (#1,#2,#3)** — no DB writes, no migrations.
Handlers in `rest.rs`, register in `startup.rs`. Wire `Config`/zone cfg into `AppState` if
not already present (`state.rs`). ~3 handlers, 0 repo methods.

**Phase 2 — settlement + orderbook reads (#4,#5)** — ✅ DONE (2026-06-07, see
[PHASE2_TODO.md](PHASE2_TODO.md)). Added `SettlementRepository::get_settlement_stats`
(counts by status + `SUM(total_amount)` where completed) and
`OrderRepository::get_all_active_orders`. 2 handlers, cross-zone merge for orderbook.

**Phase 3 — trades (#6)** — ✅ DONE (2026-06-07, see [PHASE3_TODO.md](PHASE3_TODO.md)).
**Plan assumption wrong: no `trades` table exists** — backed by `settlements` instead
(a settlement = a completed trade; has buyer/seller user FKs + indexed time cols).
Added `SettlementRepository::list_settlements_for_user(user_id, limit, offset)`; JSON
handler `get_trades` + CSV/JSON export handler `export_trades` (hand-rolled RFC-4180 CSV,
no new dep). 2 handlers, 1 repo method. Routes `/api/v1/trades`, `/api/v1/trades/export`
(APISIX route 2 already covers both).

**Phase 4 — price-alerts CRUD (#7)** — ✅ DONE (2026-06-07, see [PHASE4_TODO.md](PHASE4_TODO.md)).
New `price_alert.rs` repo (`create_price_alert`/`list_price_alerts_for_user`/`delete_price_alert`)
+ enums `AlertCondition`/`AlertStatus`, models `PriceAlert`/`NewPriceAlert`. 3 handlers, routes
`/api/v1/price-alerts(/{id})`. **`symbol` mismatch resolved**: stored in `note` (no column),
echoed back on read; `is_active` derived from status. APISIX route 2 updated.

**Phase 5 — recurring CRUD (#8)** — ✅ DONE (2026-06-07, see [PHASE5_TODO.md](PHASE5_TODO.md)).
Extended `recurring.rs` with `create/list_for_user/get/delete/set_status`. 6 handlers
(CRUD + pause/resume = `set_recurring_status`), 4 routes under `/api/v1/orders/recurring`.
New pure helper `trading_core::recurring::next_execution_at`. `recurring_repo` wired to
`AppState` (was absent). Decimals string-in/string-out (`serde-float` workaround). APISIX
`orders/*` wildcard already covers — no gateway change.

**Phase 6 (deferred) — workers (#9)** — implement `recurring_evaluator` (execute due orders via
existing matcher path) and `trigger_evaluator` (fire price alerts → noti). Not required for
frontend to function; CRUD alone unblocks UI.

## After backend lands

Re-add the APISIX routes reverted on 2026-06-07 (superproject `apisix_conf/apisix.yaml`):
- route 2 (trading upstream): `markets/config`, `markets/orderbook`, `markets/matching-status`,
  `markets/settlement-stats`, `markets/p2p/*`, `price-alerts(/*)`, `trades(/*)`,
  `orders/recurring*` (covered by existing `orders/*`? verify — recurring is a sub-path of
  `/api/v1/orders/` so existing `/api/v1/orders/*` wildcard already routes it).
- Verify each through gateway (`curl https://apisix.gridtokenx-coresystem.orb.local/...`).

> Note: `/api/v1/orders/recurring*` is already covered by route 2's `/api/v1/orders/*` —
> no new APISIX route needed once the backend handler exists.

## Effort estimate

| Phase | Handlers | Repo methods | Migrations | Risk |
|---|---|---|---|---|
| 1 | 3 | 0 | 0 | low |
| 2 | 2 | 2 | 0 | low |
| 3 | 2 | 1 | 0 | med (csv) |
| 4 | 3 | 3 | 0 | low |
| 5 | 6 | 5 | 0 | med |
| 6 | — | workers | 0 | high (defer) |

Phases 1-5 unblock all frontend features. Phase 6 optional (automation).
