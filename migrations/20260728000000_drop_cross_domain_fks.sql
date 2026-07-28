-- Trading Service — drop cross-domain foreign keys (DB-per-service repair)
-- ---------------------------------------------------------------------------
-- WHY: `20260715000000_trading_initial_schema.sql:7` declares the Trading
-- schema "Self-contained: NO foreign keys to IAM (`users`) or metering
-- (`meters`) tables" — and it creates neither table nor constraint. But the
-- live `gridtokenx_trading` database was provisioned as a CLONE of the old
-- shared `gridtokenx` DB rather than from these migrations (no
-- `_sqlx_migrations` table exists), so it dragged along IAM's `users` /
-- metering's `meters` tables plus every legacy FK that referenced them.
--
-- SYMPTOM: `users` in the clone is a stale snapshot — real accounts live in
-- `gridtokenx_iam.users` now. Every authenticated order insert therefore fails:
--
--   ERROR: insert or update on table "trading_orders" violates foreign key
--          constraint "trading_orders_user_id_fkey"
--   DETAIL: Key (user_id)=(...) is not present in table "users".
--
-- The service code is already decoupled (trading-persistence holds no `users`
-- read/write/JOIN; `repositories/order.rs` just binds the JWT's user_id) — only
-- these constraints block it.
--
-- SCOPE: Trading-OWNED tables only (the 25 in
-- `scripts/db-split/phase1-trading-cutover.sh` TRADING_TABLES). user_id /
-- meter_id stay as plain uuid columns whose referential integrity is owned by
-- the emitting service, exactly like the `-- NOT an FK across the DB boundary`
-- columns in `20260715000001_trading_phase1_local_models.sql`.
--
-- NOT IN SCOPE: the ~60 other cloned IAM/metering/blockchain tables (and their
-- FKs) that Trading should not own at all. Harmless but untidy; removing them
-- is a separate, destructive cleanup.
--
-- Idempotent (IF EXISTS) and a no-op on a database built from these migrations,
-- where none of these constraints were ever created.
-- ---------------------------------------------------------------------------

-- trading_orders → users, meters
ALTER TABLE public.trading_orders
    DROP CONSTRAINT IF EXISTS trading_orders_user_id_fkey,
    DROP CONSTRAINT IF EXISTS trading_orders_meter_id_fkey;

-- settlements → users (both counterparties)
ALTER TABLE public.settlements
    DROP CONSTRAINT IF EXISTS settlements_buyer_id_fkey,
    DROP CONSTRAINT IF EXISTS settlements_seller_id_fkey;

-- futures → users
ALTER TABLE public.futures_orders
    DROP CONSTRAINT IF EXISTS futures_orders_user_id_fkey;
ALTER TABLE public.futures_positions
    DROP CONSTRAINT IF EXISTS futures_positions_user_id_fkey;

-- p2p / recurring / alerts / escrow → users
ALTER TABLE public.p2p_orders
    DROP CONSTRAINT IF EXISTS p2p_orders_user_id_fkey;
ALTER TABLE public.recurring_orders
    DROP CONSTRAINT IF EXISTS recurring_orders_user_id_fkey;
ALTER TABLE public.price_alerts
    DROP CONSTRAINT IF EXISTS price_alerts_user_id_fkey;
ALTER TABLE public.escrow_records
    DROP CONSTRAINT IF EXISTS escrow_records_user_id_fkey;

-- carbon → users
ALTER TABLE public.carbon_credits
    DROP CONSTRAINT IF EXISTS carbon_credits_user_id_fkey;
ALTER TABLE public.carbon_transactions
    DROP CONSTRAINT IF EXISTS carbon_transactions_from_user_id_fkey,
    DROP CONSTRAINT IF EXISTS carbon_transactions_to_user_id_fkey;

-- swap → users
ALTER TABLE public.swap_transactions
    DROP CONSTRAINT IF EXISTS swap_transactions_user_id_fkey;

-- Indexes the dropped FKs were NOT backing: Postgres never auto-indexes the
-- referencing side, so the existing explicit indexes on these columns (created
-- by the initial schema) are unaffected and still serve the lookups.
