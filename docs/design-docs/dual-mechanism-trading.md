# Proposal — Dual-Mechanism Trading (CDA + Uniform-Price Auction)

> Status: **Proposal** (plan only, no code) · Owner: WiT · Date: 2026-06-25
> Scope: `gridtokenx-trading-service` (Rust matching/settlement backend)

## 1. Motivation

Two distinct classes of grid participant need two distinct market mechanisms:

| Mechanism | Target participants | Why |
|---|---|---|
| **CDA — Continuous Double Auction** (realtime) | large BESS (battery storage), EV-chargers | Flexible, fast-reacting assets benefit from continuous price discovery and immediate fills. |
| **Uniform-Price Auction** (15-min interval) | prosumers, consumers (meter-based) | Periodic meter settlement; a single clearing price per interval is fairer and matches 15-min metering cadence. Avoids penalising small participants on micro-timing. |

## 2. Current state (verified)

- **CDA exists and runs.** Pure engine `MatchingEngine::match_cycle` (`crates/trading-engine/src/engine.rs:35`); driven by `MatcherService::run_matching_cycle` (`crates/trading-logic/src/matcher_service.rs:31`); spawned as `MatcherWorker` every 1s (`bin/trading-service/src/main.rs`).
- **Uniform-price does NOT exist.** `crates/trading-logic/src/clearing.rs` is `// Placeholder`. The on-chain `trigger_market_clearing` instruction exists in the Anchor program but nothing in trading-logic calls it.
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

### Phase 0 — Routing dimension (`market_segment`)
- `trading-core/src/types.rs`: new `enum MarketSegment { Realtime, Interval }` (sqlx type, `Display`, default `Realtime`).
- `trading-core/src/models.rs:22` `TradingOrder`: add `pub market_segment: MarketSegment`.
- DB: `market_segment` column on `orders`, default `'realtime'` (zero behaviour change for existing rows). Schema is owned externally (service `CLAUDE.md` — DB provisioned by superproject `just migrate`/IAM); coordinate the migration there.
- Persistence: map column in `crates/trading-persistence/src/repositories/` using the existing **runtime** `sqlx::query(...)` pattern (no `query!` macros, no `.sqlx/`).
- Order intake (`crates/trading-api/src/rest.rs` / handlers): set segment from request or participant; default `Realtime`.
- **Test:** round-trip an `Interval` order through the repo; assert segment persists.

### Phase 1 — Split the CDA feed
- `OrderRepository` (`traits.rs:90,93`): add `get_active_buy_orders_by_segment` / `_sell_` (preferred — non-breaking) or filter existing to `Realtime`.
- `MatcherService::run_matching_cycle` (`matcher_service.rs:31`) consumes `Realtime` only.
- **Test:** mixed-segment fixture → CDA cycle ignores `Interval` orders.

### Phase 2 — Pure uniform-price engine
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

### Phase 3 — Orchestrator (`clearing.rs`)
- Fill the placeholder with `ClearingService::run_epoch_clearing(epoch_id)`:
  1. Load `MarketEpoch`; guard `status == Open`.
  2. Fetch `Interval` orders for the epoch (and zone); convert to `FastOrder` (mirror `matcher_service.rs:40-103`).
  3. Call `uniform_auction::clear_epoch`.
  4. Persist fills + settlements via the **existing** settlement path — settlement row first, then match row, atomic via outbox (`matcher_service.rs:165-190`).
  5. Write back `MarketEpoch.clearing_price`, `total_volume`, `matched_orders`; set `status = Cleared`.
- Settlement reuses `BlockchainSettlement::execute_atomic_settlement` (token swap) — **no new on-chain path**.
- **Test:** in-memory repos, one epoch; assert uniform price + epoch row updated.

### Phase 4 — `ClearingWorker`
- New worker in `crates/trading-logic/src/workers/`, spawned in `main.rs` beside `MatcherWorker`.
- Fires at the epoch boundary (15-min, aligned to `MarketEpoch.end_time`), not a fixed poll. On fire: close current epoch → `run_epoch_clearing` → open next.
- Graceful-degrade on repeated failure (mirror `SupplySyncWorker`).
- **Test:** boundary trigger calls clearing once per epoch; idempotent when already `Cleared`.

### Phase 5 — API / observability
- REST: expose epoch clearing result (price, volume) in `rest.rs`.
- Metrics: `record_clearing_cycle` beside `record_matching_cycle` (`crates/trading-infra/src/metrics.rs:141`).

## 5. Open decisions for design review

1. **Cross-zone trades in the uniform auction** — drop unmatched residual, or allow a single price plus wheeling adjustment?
2. **Epoch lifecycle owner** — does `ClearingWorker` open/close `MarketEpoch` rows, or does upstream (meter-service) own the epoch boundary?
3. **Marginal-order proration** — pro-rata by size vs. time priority for the partially-filled marginal order.
4. **On-chain granularity** — settle per-trade only (reuse), or also emit the epoch-level `trigger_market_clearing` instruction for an on-chain clearing record?

## 6. Non-goals

- No change to CDA economics in this proposal. (The separate buy-side priority question has since been **fixed**: `MatchingEngine::match_cycle` now enforces highest-bid-first, then time, then id — `engine.rs:44`; regression test `cda_buy_price_priority_highest_bid_wins` in `engine.rs` is green.)
- Trading still never mints tokens; settlement remains a swap through Chain Bridge.
