# Phase 5 — TODO & Test List

> Scope: recurring-order CRUD under `/api/v1/orders/recurring` (+ pause/resume).
> No migrations. Parent: [BACKEND_GAP_PLAN.md](BACKEND_GAP_PLAN.md). Created 2026-06-07.
>
> **STATUS: DONE (2026-06-07).**

## Endpoints

| Method + path | Frontend caller (`lib/api/trading.ts`) | Backend action |
|---|---|---|
| POST `/api/v1/orders/recurring` | `createRecurringOrder` (:249) | INSERT, compute `next_execution_at`, status `active` |
| GET `/api/v1/orders/recurring` | `listRecurringOrders` (:271) | user-scoped, newest first |
| GET `/api/v1/orders/recurring/{id}` | `getRecurringOrder` (:278) | owner-scoped one row; 404 |
| DELETE `/api/v1/orders/recurring/{id}` | `cancelRecurringOrder` (:285) | owner delete; 404 if no row |
| POST `/api/v1/orders/recurring/{id}/pause` | `pauseRecurringOrder` (:292) | `set_status(Paused)`; 404 |
| POST `/api/v1/orders/recurring/{id}/resume` | `resumeRecurringOrder` (:299) | `set_status(Active)`; 404 |

All handlers require role `ApiGateway|Admin` and scope to `UserContext.user_id`.
Wire contract matches frontend `RecurringOrder` (`types/features.ts:99`) 1:1.

## Key decisions

- **Decimals in/out are strings.** The workspace `rust_decimal` enables
  `serde-float`, so a JSON *string* would fail to deserialize into `Decimal` and
  serialization would emit *numbers*. Mirroring the `submit_order`/price-alert
  pattern, the REST DTO `CreateRecurringRequest` takes `String` fields parsed via
  `Decimal::from_str` (bad → 400), and `RecurringOrderWire` emits `.to_string()`
  decimals — exact contract match, no trailing-zero drift.
- **`next_execution_at` computed at create.** New pure helper
  `trading_core::recurring::next_execution_at(from, interval_type, interval_value)`:
  Hourly/Daily/Weekly via `Duration`; Monthly via `checked_add_months` (clamps
  end-of-month, e.g. Jan 31 +1mo → Feb 28). `interval_value` clamped to ≥ 1.
- **`interval_value` defaults to 1**; DB CHECK enforces `> 0`, handler rejects `< 1` → 400.
- **No `symbol`/`start_at`/`end_at`** — frontend `createRecurringOrder` strips these
  before sending; backend ignores. `session_token` accepted (forward-compat) but
  not persisted by the CRUD path.
- **First-time repo wiring.** `recurring_repo` was never on `AppState` before; wired
  through `Infrastructure` → `AppState` (state.rs, builder.rs, main.rs, api_routing_test.rs).

## Changes

- **`trading-core/src/recurring.rs`** (new) — pure `next_execution_at` + 6 unit tests.
  Registered in `lib.rs`.
- **`trading-core/src/models.rs`** — `NewRecurringOrder` create-input struct.
- **`trading-core/src/traits.rs`** — extended `RecurringOrderRepository` with
  `create_recurring_order` / `list_recurring_orders_for_user` / `get_recurring_order` /
  `delete_recurring_order(id,user)->bool` / `set_recurring_status(id,user,status)->bool`.
- **`trading-persistence/src/repositories/recurring.rs`** — impl of the 5 new methods
  (runtime `sqlx`, owner-scoped; INSERT … RETURNING *, SELECT … ORDER BY created_at DESC,
  DELETE … rows_affected, UPDATE status). Already exported in `mod.rs`.
- **`trading-api/src/rest.rs`** — DTO `CreateRecurringRequest`, wire `RecurringOrderWire`,
  builder `build_recurring_response`, parsers `parse_side`/`parse_interval`/`parse_opt_decimal`,
  6 handlers (+ shared `set_recurring_status_handler`).
- **`trading-api/src/startup.rs`** — 4 routes (collection POST+GET; item GET+DELETE;
  pause; resume).
- **`trading-api/src/state.rs`**, **`bin/trading-service/src/builder.rs`**,
  **`bin/trading-service/src/main.rs`**, **`bin/.../tests/api_routing_test.rs`** —
  wire `recurring_repo: Arc<dyn RecurringOrderRepository>`.
- **`trading-api/tests/endpoint_tests.rs`** — `MockSystem.recurring` field +
  `RecurringOrderRepository` mock impl + `recurring_repo` in AppState + 8 integration tests.

## APISIX

No change. Route 2's existing `/api/v1/orders/*` wildcard already covers all six
recurring paths (`apisix_conf/apisix.yaml`). Verified by inspection.

## Tests (all green)

trading-core (`recurring.rs`, 6): hourly/daily/weekly/monthly advance,
end-of-month clamp, zero/negative interval clamps to 1.

trading-api unit (`rest.rs`, 2): `recurring_response_maps_decimals_and_enums`,
`recurring_response_paused_status`.

trading-api HTTP integration (`tests/endpoint_tests.rs`, 8):
- `test_recurring_create_maps_fields` — POST echoes fields, status active, next_exec set.
- `test_recurring_create_then_list_newest_first` — list ordering.
- `test_recurring_list_user_scoped` — other user's order not visible.
- `test_recurring_get_roundtrip_and_foreign_404` — GET one + foreign 404.
- `test_recurring_pause_resume_flips_status` — pause→paused, resume→active.
- `test_recurring_delete_roundtrip` — create → delete 200 → list empty.
- `test_recurring_pause_foreign_404` — unknown id → 404.
- `test_recurring_bad_interval_400` — invalid interval_type → 400.

Result: `cargo test -p trading-api` lib+tests **48 pass** (was 38).
`cargo clippy -p trading-core -p trading-persistence -p trading-api` **0 errors**.
Full-stack e2e (`trading-service --test api_routing_test`) **1 pass**.

**Live SQL verified** (`docker exec gridtokenx-postgres psql`): INSERT (columns +
`order_side`/`interval_type` casts + `status` default `active`), UPDATE
(`recurring_status` cast), DELETE — round-trip on a real `users` FK row, then
cleaned up.

## Pending (needs live stack)
- Gateway smoke: `curl` POST/GET/DELETE/pause/resume
  `https://apisix.gridtokenx-coresystem.orb.local/api/v1/orders/recurring`
  (after trading-service `:8093` + APISIX up, with JWT).
