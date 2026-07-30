# Dual-Mechanism Trading (CDA + Uniform-Price Auction)

> Status: **Implemented** (Phases 0–5 shipped + verified end-to-end) · Owner: WiT
> · Proposed 2026-06-25 · Implemented 2026-06-26/27
> Scope: `gridtokenx-trading-service` (Rust matching/settlement backend)
>
> Both mechanisms are live: orders carry a `market_segment`; `Realtime` orders
> match continuously (CDA), `Interval` orders clear per-zone at a uniform price
> on the 15-min epoch boundary. Verified by a chained live-Postgres test
> (`bin/trading-service/tests/interval_clearing_e2e_test.rs`: place → persist →
> elapse → clear → settle). Phase-by-phase status is marked inline in §4 below.

## 1. Motivation

Two distinct classes of grid participant need two distinct market mechanisms:

| Mechanism | Target participants | Why |
|---|---|---|
| **CDA — Continuous Double Auction** (realtime) | large BESS (battery storage), EV-chargers | Flexible, fast-reacting assets benefit from continuous price discovery and immediate fills. |
| **Uniform-Price Auction** (15-min interval) | prosumers, consumers (meter-based) | Periodic meter settlement; a single clearing price per interval is fairer and matches 15-min metering cadence. Avoids penalising small participants on micro-timing. |

## 2. Current state (verified)

- **CDA exists and runs.** Pure engine `MatchingEngine::match_cycle` (`crates/trading-engine/src/engine.rs:35`); driven by `MatcherService::run_matching_cycle` (`crates/trading-logic/src/matcher_service.rs:62`); spawned as `MatcherWorker` (`bin/trading-service/src/main.rs`), which cycles on order arrival with a 1s fallback tick.
- ~~**Uniform-price does NOT exist.**~~ **Now implemented** — pure engine `UniformAuction::clear_cycle` (`crates/trading-engine/src/uniform_auction.rs`), orchestrated by `ClearingService` (`crates/trading-logic/src/clearing.rs`), driven by `ClearingWorker` (`crates/trading-logic/src/workers/clearing.rs`). The on-chain `trigger_market_clearing` instruction is still NOT used — settlement reuses the per-trade swap path (see §5.4 decision).
- **Schema already anticipates clearing.** `MarketEpoch` (`crates/trading-core/src/models.rs:328`) carries `clearing_price`, `total_volume`, `matched_orders`, `status: EpochStatus` — fields only a uniform-price clearing fills. `OrderMatch` (`models.rs:341`) is keyed by `epoch_id`.
- **No routing dimension.** `OrderType` (`crates/trading-core/src/types.rs:27`) is only `Limit | Market`. `TradingOrder` (`models.rs:22`) has `epoch_id`, `zone_id`, `meter_id` but **no field that selects CDA vs uniform-price**. `get_active_buy_orders` / `get_active_sell_orders` (`crates/trading-core/src/traits.rs:90,93`) return ALL active orders — CDA would currently consume prosumer orders too.

## 3. Routing decision

**Add a `market_segment` enum** (`Realtime | Interval`) to the order. Explicit and self-describing; the order carries its own mechanism rather than relying on a nullable field (`meter_id`) or a runtime role lookup. Default `Realtime` so existing rows and behaviour are unchanged.

```
order intake ──set market_segment──┐
                                    ├─ Realtime ─▶ MatcherService (CDA, 1s)        ─┐
                                    └─ Interval ─▶ ClearingService (uniform, 15m)  ─┤
                                                                                    ▼
                                                    shared settlement / outbox / Chain Bridge swap
```

Two engines, **one** downstream settlement path — no duplication of blockchain/outbox logic.

## 4. Phased plan

### Phase 0 — Routing dimension (`market_segment`) — ✅ DONE
> Enum/field/intake landed `f96a2dd`; the DB column + persistence round-trip
> landed later (IAM migration `20260627000000_add_market_segment.sql` = `a06311e`;
> persistence read/write = `84d63de`). The column was initially deferred and the
> `From<TradingOrderDb>` defaulted the segment — a real gap (interval orders read
> back as `realtime`) closed in `84d63de`, verified by `test_market_segment_round_trips`.

- `trading-core/src/types.rs`: new `enum MarketSegment { Realtime, Interval }` (sqlx type, `Display`, default `Realtime`).
- `trading-core/src/models.rs:22` `TradingOrder`: add `pub market_segment: MarketSegment`.
- DB: `market_segment` column on `orders`, default `'realtime'` (zero behaviour change for existing rows). Schema is owned externally (service `CLAUDE.md` — DB provisioned by superproject `just migrate`/IAM); coordinate the migration there.
- Persistence: map column in `crates/trading-persistence/src/repositories/` using the existing **runtime** `sqlx::query(...)` pattern (no `query!` macros, no `.sqlx/`).
- Order intake (`crates/trading-api/src/rest.rs` / handlers): set segment from request or participant; default `Realtime`.
- **Test:** round-trip an `Interval` order through the repo; assert segment persists.

### Phase 1 — Split the CDA feed — ✅ DONE (`f96a2dd`)
> Implemented as the in-memory filter variant: `run_matching_cycle` filters the
> existing feed to `Realtime`. Tests `run_matching_cycle_ignores_interval_segment`,
> `run_matching_cycle_does_not_cross_segments`.

- `OrderRepository` (`traits.rs:90,93`): add `get_active_buy_orders_by_segment` / `_sell_` (preferred — non-breaking) or filter existing to `Realtime`.
- `MatcherService::run_matching_cycle` (`matcher_service.rs:62`) consumes `Realtime` only.
- **Test:** mixed-segment fixture → CDA cycle ignores `Interval` orders.

### Phase 2 — Pure uniform-price engine — ✅ DONE (`8e61579`)
> Shipped as `UniformAuction::clear_cycle` (per-zone two-pointer sweep; `p*` =
> `i64::midpoint(bid, ask)` of the marginal crossing). 8 pure tests incl.
> midpoint clear, flat-marginal time-priority proration, per-zone isolation, tie,
> self-trade skip, expiry.

- New `crates/trading-engine/src/uniform_auction.rs`, symmetric to `engine.rs` (sync, zero I/O, zero DB — keep the hot path pure):
  ```rust
  pub fn clear_epoch(
      buys: &mut [FastOrder],
      sells: &mut [FastOrder],
      buy_metadata: &[OrderMetadata],
      sell_metadata: &[OrderMetadata],
      topology: &dyn TopologySnapshot,
  ) -> (ClearingResult, CycleStats)
  ```
  Algorithm:
  1. Demand curve = buys sorted price **descending**; supply = sells sorted price **ascending**.
  2. Walk both, accumulate cumulative quantity; find the marginal crossing → **single clearing price `p*`** (last price where cumulative demand ≥ cumulative supply).
  3. All buys with `bid ≥ p*` and sells with `ask ≤ p*` fill **at `p*`** (uniform marginal price); prorate the marginal order.
  4. **Per-zone clearing** — one `p*` per zone. A single grid-wide price breaks under wheeling charge + line loss (see `engine.rs:96-116`).
- Reuse `FastPrice`, `FastOrder`, `MIN_TRADE_AMOUNT`, `TopologySnapshot` from the engine crate.
- **Tests (pure, fast):** single crossing; flat marginal (many orders at `p*` → proration); no-cross (empty result); per-zone isolation; tie at `p*`. Pin the economics before any I/O exists.

### Phase 3 — Orchestrator (`clearing.rs`) — ✅ DONE (`a5e9875`)
> `ClearingService::run_epoch_clearing(epoch_id)` clears one epoch's `Interval`
> orders and persists via the **shared** path: the settlement-row-first /
> match-row / aggregated-fill logic was extracted into
> `clearing_support::{to_fast_orders, order_totals, persist_matches}` and is used
> by BOTH the matcher and the clearing service (no duplicated outbox logic).
> Epoch lifecycle write-back moved to the Phase-4 worker (§5.2 decision).

- Fill the placeholder with `ClearingService::run_epoch_clearing(epoch_id)`:
  1. Load `MarketEpoch`; guard `status == Open`.
  2. Fetch `Interval` orders for the epoch (and zone); convert to `FastOrder` (mirror `matcher_service.rs:68-131`).
  3. Call `uniform_auction::clear_epoch`.
  4. Persist fills + settlements via the **existing** settlement path — settlement row first, then match row, atomic via outbox (`clearing_support.rs:92-99`).
  5. Write back `MarketEpoch.clearing_price`, `total_volume`, `matched_orders`; set `status = Cleared`.
- Settlement reuses `BlockchainSettlement::execute_atomic_settlement` (token swap) — **no new on-chain path**.
- **Test:** in-memory repos, one epoch; assert uniform price + epoch row updated.

### Phase 4 — `ClearingWorker` — ✅ DONE (`6d9a537`, live test `e7a13ed`)
- New worker `crates/trading-logic/src/workers/clearing.rs`, spawned in `main.rs` beside `MatcherWorker`.
- **Polls every 60s** (revised from boundary-firing): `ClearingService::clear_due_epochs` selects epochs whose window has elapsed (`status='active' AND end_time <= NOW()`) and closes each — **even empty ones** — so none linger `active`. Polling is robust to restart/clock drift; the boundary is detected, not depended upon.
- Epoch lifecycle owned here (§5.2): `mark_epoch_cleared` flips `active → cleared` guarded by `WHERE status='active'` → idempotent; stamps `clearing_price` (single-zone only; multi-zone/empty → NULL), `total_volume`, `matched_orders`.
- Graceful-degrade: a failed cycle is logged; the loop continues.
- **Tests:** `clears_and_closes_due_epochs`, `closes_empty_due_epoch`; live SQL (due predicate + idempotent close) in `epoch_clearing_integration_test`.

### Phase 5 — API / observability — ✅ DONE (`91a0b33`)
- REST: `GET /api/v1/markets/clearing-epochs?limit=N` (newest first, clamp 1..=100) → per-epoch `clearing_price` / `total_volume` / `matched_orders`, backed by `OrderRepository::list_recent_cleared_epochs`. Tests: `test_clearing_epochs_endpoint`, live `test_list_recent_cleared_epochs_e2e`.
- Metrics: `record_clearing_cycle(zones, matches, volume)` in `crates/trading-infra/src/metrics.rs`, emitted per epoch in `clear_due_epochs`.

## 5. Decisions taken (was: open for review)

1. **Cross-zone trades in the uniform auction → clear PER-ZONE; no cross-zone in the batch.** `clear_cycle` partitions orders by zone and clears each independently at its own `p*`. Cross-zone balancing stays the CDA matcher's job (where wheeling/loss pricing already lives). A residual unmatched in a zone simply doesn't clear this epoch.
2. **Epoch lifecycle owner → the `ClearingWorker`.** It selects elapsed epochs and flips `active → cleared` (idempotent `WHERE status='active'` guard). `get_or_create_active_epoch` (intake) opens the next epoch on demand. `ClearingService` stays a pure "clear one epoch, persist trades" unit; the worker owns open/close + write-back.
3. **Marginal-order proration → time priority.** When several orders sit at `p*`, the marginal fill goes by earliest-arrival (price-time), not pro-rata-by-size. Test `flat_marginal_prorates_by_time_priority`.
4. **On-chain granularity → per-trade settlement only.** Each cleared trade settles via the existing `BlockchainSettlement::execute_atomic_settlement` swap (shared with the matcher). The epoch-level on-chain `trigger_market_clearing` instruction is **not** emitted — no separate on-chain clearing record. Revisit only if an on-chain audit trail of the epoch price is required.

> Additional decisions made during implementation:
> - **`market`-order-type → IOC default; `interval` requires GTC.** A market order with no explicit TIF defaults to IOC (fill-or-drop); `Interval` orders reject IOC/FOK (batch clearing has no "immediate" semantics, and the CDA IOC sweep never sees interval orders). Both enforced at intake (REST 400 / gRPC InvalidArgument).
> - **Expiry reaping** (`expire_stale_orders`) runs in the matcher cycle and is segment-agnostic, so interval orders that never clear are reaped on `expires_at` like any other.

## 6. Non-goals

- No change to CDA economics in this proposal. (The separate buy-side priority question has since been **fixed**: `MatchingEngine::match_cycle` now enforces highest-bid-first, then time, then id — `engine.rs:44`; regression test `cda_buy_price_priority_highest_bid_wins` in `engine.rs` is green.)
- Trading still never mints tokens; settlement remains a swap through Chain Bridge.
