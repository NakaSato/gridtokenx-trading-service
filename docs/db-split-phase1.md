# Trading Service — DB-per-service migration, Phase 1 (authoring)

> Status: **authored, not cut over** · 2026-07-15
> Superproject design doc: `../../docs/design-docs/db-per-service-migration.md` (§3.1, §5 Phase 1)

This document records what Phase 1 **authored** inside `gridtokenx-trading-service/` and what
the superproject ("main thread") must still do to actually cut Trading over to a physical
`gridtokenx_trading` database. **Nothing here has been deployed**: no DB created, no migrations
run, no `TRADING_DATABASE_URL` change, no compose/pgdog edits, no commit.

---

## 1. What was authored

### 1.1 Migrations (`migrations/`)

Trading shipped **no** migrations before now (`migrations/` held only `.keep`; see this service's
`CLAUDE.md` — "the trading schema is not owned by this service"). Phase 1 reverses that.

| File | Contents |
|------|----------|
| `20260715000000_trading_initial_schema.sql` | **Exact current DDL** of the 25 Trading-owned tables, extracted verbatim from a full `pg_dump` of the shared `gridtokenx` DB. 19 enum types · 3 trigger functions · 2 sequences · 25 tables · 31 PK/UNIQUE constraints · 132 indexes · 14 **intra-trading** FKs · 12 triggers · comments. **Self-contained**: the 14 cross-domain FKs to `users`/`meters` present in the shared DB were dropped (columns kept as plain UUID/text). |
| `20260715000001_trading_phase1_local_models.sql` | **Authored** helper tables (not in the dump): the missing owned table `vpp_cluster_members`, two relocated audit tables, and two cross-domain read-models. |

**Owned tables in the initial schema (25):**
`trading_orders`, `trading_orders_archive`, `settlements`, `settlements_archive`,
`order_matches`, `market_epochs`, `market_epochs_archive`, `recurring_orders`,
`recurring_order_executions`, `outbox_events`, `price_alerts`, `vpp_clusters`,
`vpp_dispatch_history`, `futures_products`, `futures_orders`, `futures_positions`,
`carbon_credits`, `carbon_transactions`, `p2p_orders`, `p2p_config`, `p2p_config_audit`,
`swap_transactions`, `liquidity_pools`, `platform_revenue`, `escrow_records`.

**Cross-domain FKs dropped** (would violate DB-per-service; columns retained, values resolved
via service calls / read-models):
`trading_orders.user_id→users`, `trading_orders.meter_id→meters`,
`settlements.buyer_id/seller_id→users`, `recurring_orders.user_id→users`,
`price_alerts.user_id→users`, `futures_orders.user_id→users`,
`futures_positions.user_id→users`, `escrow_records.user_id→users`,
`swap_transactions.user_id→users`, `p2p_orders.user_id→users`,
`carbon_credits.user_id→users`, `carbon_transactions.from_user_id/to_user_id→users`.

**FKs kept** (both ends Trading-owned): order_matches→trading_orders/market_epochs/settlements,
settlements→market_epochs, trading_orders→market_epochs, escrow_records→trading_orders,
platform_revenue→settlements, futures_orders/positions→futures_products,
recurring_order_executions→recurring_orders/trading_orders, swap_transactions→liquidity_pools,
vpp_dispatch_history→vpp_clusters.

> **Schema drift noted:** `vpp_cluster_members` is queried by `trading-persistence/vpp.rs` but is
> **absent from the current shared-DB dump** — authored here from the repository's SELECT list.
> Likewise `vpp.rs` selects `vpp_clusters.target_soc_percentage` / `dispatch_mode` and
> `meters.rated_power_kw` / `rated_capacity_kwh`, none of which exist in the dumped `vpp_clusters`
> / `meters`. Those columns are provided by `meter_read_model` here; the `vpp_clusters` extras
> are left as-is (out of Phase-1 scope) and will surface as `NULL`/errors only if that code path
> runs against a DB lacking them — same as today.

### 1.2 Coupling removal (the four §3.1 sites)

| Site | Access | Phase-1 action |
|------|--------|----------------|
| `crates/trading-infra/src/audit/mod.rs` (single + batch insert, `get_user_events`) | WRITE+READ IAM `user_activities` | **Relocated** to Trading-owned `trading_user_activities`. Code now points at the local table. |
| `crates/trading-infra/src/blockchain/wallet/audit_logger.rs` (`log_operation` insert, `get_user_audit_log` select) | WRITE+READ IAM `wallet_audit_log` | **Relocated** to Trading-owned `trading_wallet_audit_log`. Code now points at the local table. |
| `crates/trading-infra/src/blockchain/rpc/service.rs` (`get_user_primary_wallet`) | READ IAM `user_wallets` | **Left in place** + `TODO(db-split)` comment. Read-model `iam_wallet_read_model` authored, not yet wired. |
| `crates/trading-persistence/src/repositories/vpp.rs` (`get_member_association`, `get_cluster_members`) | JOIN metering `meters` | **Left in place** + `TODO(db-split)` comments. Read-model `meter_read_model` authored, not yet wired. |

The two audit **writes** are simple relocations (audit is per-service; no cross-service data
needed), so the code change ships now. The two **reads** need event-fed read-models before the
SQL can change, so they stay and are marked — cutover flips them.

---

## 2. Read-model feeds (design — to build before cutover)

Both read-models follow **event-carried state transfer**: the owning service emits domain events
via its outbox → NATS; Trading maintains a local table in `gridtokenx_trading`.

### 2.1 `iam_wallet_read_model` ← IAM `user.wallet.*`

- **Backfill (first boot):** snapshot IAM `user_wallets` → upsert
  `(user_id, wallet_address, is_primary, blockchain_registered, user_account_pda, shard_id)`.
  One-shot, idempotent (PK `(user_id, wallet_address)`).
- **Steady state:** subscribe to IAM wallet events (`user.wallet.created`,
  `user.wallet.updated`, `user.wallet.primary_changed`, `user.wallet.registered`). Each event
  upserts the row and maintains the "one primary per user" invariant (partial unique index
  `uq_iam_wallet_read_model_primary`).
- **Consumer read:** `get_user_primary_wallet` switches to
  `SELECT wallet_address FROM iam_wallet_read_model WHERE user_id = $1 AND is_primary`.
- **Fallback:** if the read-model has no row (event lag on a brand-new user), fall back to the
  existing IAM gRPC identity gateway (already wired as `IdentityGateway`) rather than the removed
  cross-DB query.

### 2.2 `meter_read_model` ← meter events

- **Backfill (first boot):** snapshot metering `meters` → upsert
  `(serial_number, meter_id, user_id, zone_id, status, rated_power_kw, rated_capacity_kwh)`.
- **Steady state:** subscribe to meter lifecycle events (`meter.registered`, `meter.verified`,
  `meter.updated`, `meter.decommissioned`) emitted by IAM/meter-service; upsert by
  `serial_number` (PK).
- **Consumer read:** the VPP joins in `vpp.rs` replace `LEFT JOIN meters met ON m.meter_id =
  met.serial_number` with `LEFT JOIN meter_read_model met ON m.meter_id = met.serial_number`
  (`rated_power_kw` / `rated_capacity_kwh` come from the read-model).
- Note: `meters.rated_power_kw` / `rated_capacity_kwh` are not in the current dump; the producing
  service must include them in the event payload (or Phase 2 metering migration must add them).

---

## 3. Cutover steps remaining (main thread / superproject)

Authoring is complete; **none** of the following were done here (out of scope by instruction):

1. **Create the physical DB** `gridtokenx_trading` + a least-privilege login role.
2. **pgdog route** — add `[[databases]] name = "gridtokenx_trading"` (no `database_name` alias, per
   the `gridtokenx_noti` reference model) in `pgdog.toml`.
3. **compose** — any `docker-compose.yml` wiring / fix the misleading `:916` "schema" comment.
4. **Apply migrations** — run the two files in `migrations/` against `gridtokenx_trading`
   (via `sqlx migrate` or a `sqlx::migrate!` call added to boot — currently the service has none).
5. **Build the read-model feeds** (§2) — backfill jobs + NATS consumers — **before** flipping the
   two `TODO(db-split)` read sites.
6. **Cut `TRADING_DATABASE_URL`** over from the shared `gridtokenx` pooler to `gridtokenx_trading`.
7. **Verify** — `just e2e` + the Trading integration suite (`repository_integration_test`,
   `settlement_integration_test`), then flip the two read sites and re-verify.
8. **Rollback lever:** point `TRADING_DATABASE_URL` back at `gridtokenx` (no source tables are
   dropped in Phase 1).
