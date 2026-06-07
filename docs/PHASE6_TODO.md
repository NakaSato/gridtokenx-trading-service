# Phase 6 — Recurring & Price-Alert Workers

> Status: ✅ DONE (2026-06-07).
> Driver: [BACKEND_GAP_PLAN.md](BACKEND_GAP_PLAN.md) #9 — automation behind the
> recurring-order and price-alert CRUD shipped in Phases 4–5. Not required for the
> frontend (CRUD alone unblocks the UI); this turns the stored rules into action.

## What landed

### Recurring-order execution — `RecurringEvaluator`
- `crates/trading-logic/src/recurring_evaluator.rs` (was a 1-line placeholder).
- `run_cycle()`: `get_due_recurring_orders(now)` → for each rule, place a `Limit`
  `TradingOrder` via `OrderRepository::insert_order_with_event` (same atomic
  order+outbox path as the REST create handler, `rest.rs:160`), then
  `update_after_execution(id, next_execution_at(now, ..), total+1)`.
- Price: buys use `max_price_per_kwh`, sells use `min_price_per_kwh`; a rule with
  no bound for its side is skipped (logged) without advancing.
- The matcher worker picks the placed orders up on its next cycle — this service
  never matches directly.
- Failure isolation: one bad rule logs + continues; epoch resolved once per batch.

### Price-alert triggering — `TriggerEvaluator`
- `crates/trading-logic/src/trigger_evaluator.rs` (was a 1-line placeholder).
- `run_cycle()`: derive current price = midpoint of best bid / best ask from
  `OrderRepository::get_all_active_orders()` (falls back to the one side present;
  empty book → no-op). Scan `get_active_alerts()`; for each whose condition the
  price satisfies, `mark_triggered(id, price)` + publish
  `Event::PriceAlertTriggered` (outbox → notification service).
- Conditions: `Above` = `price >= target`, `Below` = `price <= target`,
  `Crosses` = within a 0.1% band of target (service is stateless — no prior
  price to detect a true crossing; documented approximation). Unit-tested.

### Supporting changes
- `Event::PriceAlertTriggered(PriceAlertTriggeredPayload)` added to
  `trading-core::events` (+ `outbox_event_type` tag + kafka topic routing to
  `triggers`).
- `PriceAlertRepository::{get_active_alerts, mark_triggered}` added to the trait
  and the Postgres impl. `mark_triggered` sets `triggered_at`/`triggered_price`;
  one-shot alerts → `triggered`, repeating alerts stay `active`.
- Worker loops `RecurringEvaluatorWorker` / `TriggerEvaluatorWorker`
  (`workers/recurring.rs`, `workers/trigger.rs`), `MatcherWorker` pattern.
- Wired in `builder.rs` (`AppServices`) and spawned in `main.rs`
  (recurring every 60s, trigger every 30s).

## Not done / out of scope
- No new migrations, no APISIX/gateway change (no new routes — workers are
  internal).
- Intervals are hardcoded constants in `main.rs` (60s / 30s); promote to env/config
  if tuning is needed.
- `Crosses` is approximated (band), not a true edge-trigger — would need
  persisted last-price to do properly.

## Verification
- `cargo build --workspace` — 0 errors.
- `cargo test -p trading-logic trigger_evaluator` — 3 passed (condition logic).
- `cargo test -p trading-api --test endpoint_tests` — 28 passed (mock updated for
  the 2 new trait methods).
- `cargo test -p trading-service --test api_routing_test` — 1 passed.
