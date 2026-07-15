-- Trading Service — Phase 1 local tables (DB-per-service migration)
-- ---------------------------------------------------------------------------
-- Authored (NOT extracted from the shared-DB dump). These tables let Trading
-- own its data end-to-end after the split, replacing four cross-domain SQL
-- couplings (see docs/db-split-phase1.md §"Coupling removal").
--
--   1. vpp_cluster_members     — owned VPP child table. Referenced by
--      trading-persistence vpp.rs but ABSENT from the current shared-DB dump
--      (schema drift); authored here from the columns the repository selects.
--   2. trading_user_activities — Trading-owned audit table. Relocates the
--      cross-domain WRITE that today targets IAM `user_activities`.
--   3. trading_wallet_audit_log — Trading-owned wallet-audit table. Relocates
--      the cross-domain WRITE that today targets IAM `wallet_audit_log`.
--   4. iam_wallet_read_model    — local read-model of IAM `user_wallets`, fed by
--      IAM `user.wallet.*` NATS events (+ boot backfill). Replaces the direct
--      READ at trading-infra rpc/service.rs (get_user_primary_wallet).
--   5. meter_read_model         — local read-model of metering `meters`, fed by
--      meter NATS events (+ boot backfill). Replaces the `meters` JOIN in
--      trading-persistence vpp.rs (serial_number → rated_power/capacity).
-- ---------------------------------------------------------------------------

-- ===========================================================================
-- 1. vpp_cluster_members — owned VPP membership table (authored; missing from dump)
--    Columns mirror the SELECT list in trading-persistence vpp.rs
--    (get_member_association / get_cluster_members).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS public.vpp_cluster_members (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    cluster_id text NOT NULL,
    meter_id text NOT NULL,           -- metering serial_number; resolved via meter_read_model, NOT an FK
    contribution_weight double precision DEFAULT 1.0 NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    joined_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT vpp_cluster_members_pkey PRIMARY KEY (id),
    CONSTRAINT vpp_cluster_members_cluster_id_fkey
        FOREIGN KEY (cluster_id) REFERENCES public.vpp_clusters(cluster_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_vpp_cluster_members_cluster ON public.vpp_cluster_members (cluster_id);
CREATE INDEX IF NOT EXISTS idx_vpp_cluster_members_meter ON public.vpp_cluster_members (meter_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_vpp_cluster_members_cluster_meter
    ON public.vpp_cluster_members (cluster_id, meter_id);

-- ===========================================================================
-- 2. trading_user_activities — Trading-owned audit (relocated WRITE)
--    Column-compatible with IAM public.user_activities so the existing
--    INSERT/SELECT in trading-infra audit/mod.rs works verbatim against it.
--    Plain (non-partitioned) table; Trading's audit volume does not need range
--    partitioning today.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS public.trading_user_activities (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid,                     -- IAM user id; NOT an FK across the DB boundary
    activity_type character varying(50) NOT NULL,
    description text,
    ip_address inet,
    user_agent text,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT trading_user_activities_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_trading_user_activities_user
    ON public.trading_user_activities (user_id);
CREATE INDEX IF NOT EXISTS idx_trading_user_activities_created
    ON public.trading_user_activities (created_at);
CREATE INDEX IF NOT EXISTS idx_trading_user_activities_user_created
    ON public.trading_user_activities (user_id, created_at DESC);

-- ===========================================================================
-- 3. trading_wallet_audit_log — Trading-owned wallet audit (relocated WRITE)
--    Column-compatible with IAM public.wallet_audit_log so the existing
--    INSERT/SELECT in trading-infra blockchain/wallet/audit_logger.rs works
--    verbatim against it.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS public.trading_wallet_audit_log (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,            -- IAM user id; NOT an FK across the DB boundary
    operation text NOT NULL,
    success boolean DEFAULT true NOT NULL,
    ip_address inet,
    user_agent text,
    error_message text,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT trading_wallet_audit_log_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_trading_wallet_audit_log_user
    ON public.trading_wallet_audit_log (user_id);
CREATE INDEX IF NOT EXISTS idx_trading_wallet_audit_log_created
    ON public.trading_wallet_audit_log (created_at);

-- ===========================================================================
-- 4. iam_wallet_read_model — local read-model of IAM public.user_wallets
--    Populated by: (a) boot backfill snapshot, (b) IAM `user.wallet.*` NATS
--    events (event-carried state transfer). Read by
--    get_user_primary_wallet (trading-infra rpc/service.rs) once cutover lands.
--    Only the columns Trading actually needs to resolve a signer are mirrored.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS public.iam_wallet_read_model (
    user_id uuid NOT NULL,
    wallet_address character varying(64) NOT NULL,
    is_primary boolean DEFAULT false NOT NULL,
    blockchain_registered boolean DEFAULT false NOT NULL,
    user_account_pda character varying(88),
    shard_id smallint,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT iam_wallet_read_model_pkey PRIMARY KEY (user_id, wallet_address)
);
-- Fast "primary wallet for user" lookup (mirrors the WHERE user_id=$1 AND is_primary=true query).
CREATE UNIQUE INDEX IF NOT EXISTS uq_iam_wallet_read_model_primary
    ON public.iam_wallet_read_model (user_id) WHERE is_primary;

-- ===========================================================================
-- 5. meter_read_model — local read-model of metering public.meters
--    Populated by: (a) boot backfill snapshot, (b) meter NATS events.
--    Read by the VPP membership join (trading-persistence vpp.rs) once cutover
--    lands. `rated_power_kw` / `rated_capacity_kwh` are the columns vpp.rs
--    selects via the `meters` join (they live on the metering side today).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS public.meter_read_model (
    serial_number character varying(100) NOT NULL,
    meter_id uuid NOT NULL,
    user_id uuid NOT NULL,
    zone_id integer,
    status character varying(50) DEFAULT 'active'::character varying,
    rated_power_kw double precision,
    rated_capacity_kwh double precision,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT meter_read_model_pkey PRIMARY KEY (serial_number)
);
CREATE INDEX IF NOT EXISTS idx_meter_read_model_meter_id ON public.meter_read_model (meter_id);
CREATE INDEX IF NOT EXISTS idx_meter_read_model_user_id ON public.meter_read_model (user_id);
