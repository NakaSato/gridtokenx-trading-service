-- Trading Service — initial schema (Phase 1 of DB-per-service migration)
-- ---------------------------------------------------------------------------
-- Source of truth: exact CURRENT DDL of Trading-owned objects, extracted from a
-- full pg_dump of the shared `gridtokenx` database (2026-07-15). Authored to
-- stand up the physical `gridtokenx_trading` database. See docs/db-split-phase1.md.
--
-- Self-contained: NO foreign keys to IAM (`users`) or metering (`meters`) tables.
-- 14 cross-domain FKs present in the shared DB were intentionally DROPPED here
-- (user_id / meter_id / buyer_id / seller_id / from_user_id / to_user_id → users,
--  meter_id → meters). Those columns remain as plain UUID/text columns; the
-- values are resolved via IAM/metering services (gRPC) or local read-models
-- (see the companion 20260715000001 migration), never via SQL joins.
--
-- Objects: 19 enum types · 3 trigger functions · 2 sequences · 25 tables ·
--          31 PK/UNIQUE constraints · 132 indexes · 14 intra-trading FKs ·
--          12 triggers · table/column comments.
--
-- Extraction method preserves the dump's `public.` schema qualification and the
-- object ordering required for a clean apply: types → functions → sequences →
-- tables → sequence-ownership → column-defaults → constraints → indexes →
-- intra-trading FKs → triggers → comments.
-- ---------------------------------------------------------------------------


-- ======================================================================
-- ENUM TYPES (19)
-- ======================================================================

-- TYPE: alert_condition
CREATE TYPE public.alert_condition AS ENUM (
    'above',
    'below',
    'crosses'
);

-- TYPE: alert_status
CREATE TYPE public.alert_status AS ENUM (
    'active',
    'triggered',
    'cancelled'
);

-- TYPE: carbon_status
CREATE TYPE public.carbon_status AS ENUM (
    'active',
    'retired',
    'transferred'
);

-- TYPE: carbon_transaction_status
CREATE TYPE public.carbon_transaction_status AS ENUM (
    'pending',
    'completed',
    'failed',
    'cancelled'
);

-- TYPE: epoch_status
CREATE TYPE public.epoch_status AS ENUM (
    'pending',
    'active',
    'cleared',
    'settled'
);

-- TYPE: futures_order_side
CREATE TYPE public.futures_order_side AS ENUM (
    'long',
    'short'
);

-- TYPE: futures_order_status
CREATE TYPE public.futures_order_status AS ENUM (
    'pending',
    'open',
    'filled',
    'cancelled',
    'liquidated'
);

-- TYPE: futures_order_type
CREATE TYPE public.futures_order_type AS ENUM (
    'market',
    'limit'
);

-- TYPE: interval_type
CREATE TYPE public.interval_type AS ENUM (
    'hourly',
    'daily',
    'weekly',
    'monthly'
);

-- TYPE: market_segment
CREATE TYPE public.market_segment AS ENUM (
    'realtime',
    'interval'
);

-- TYPE: order_side
CREATE TYPE public.order_side AS ENUM (
    'buy',
    'sell'
);

-- TYPE: order_status
CREATE TYPE public.order_status AS ENUM (
    'pending',
    'active',
    'partially_filled',
    'filled',
    'settled',
    'cancelled',
    'expired'
);

-- TYPE: order_type
CREATE TYPE public.order_type AS ENUM (
    'limit',
    'market'
);

-- TYPE: p2p_order_side
CREATE TYPE public.p2p_order_side AS ENUM (
    'buy',
    'sell'
);

-- TYPE: p2p_order_status
CREATE TYPE public.p2p_order_status AS ENUM (
    'open',
    'filled',
    'cancelled'
);

-- TYPE: recurring_status
CREATE TYPE public.recurring_status AS ENUM (
    'active',
    'paused',
    'completed',
    'cancelled'
);

-- TYPE: time_in_force
CREATE TYPE public.time_in_force AS ENUM (
    'gtc',
    'fok',
    'ioc'
);

-- TYPE: trigger_status
CREATE TYPE public.trigger_status AS ENUM (
    'pending',
    'triggered',
    'cancelled',
    'expired'
);

-- TYPE: trigger_type
CREATE TYPE public.trigger_type AS ENUM (
    'stop_loss',
    'take_profit',
    'trailing_stop'
);


-- ======================================================================
-- FUNCTIONS (3)
-- ======================================================================

-- FUNCTION: log_p2p_config_change()
CREATE FUNCTION public.log_p2p_config_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.config_value IS DISTINCT FROM NEW.config_value THEN
        INSERT INTO p2p_config_audit (config_key, old_value, new_value, changed_by)
        VALUES (OLD.config_key, OLD.config_value, NEW.config_value, NEW.updated_by);
    END IF;
    RETURN NEW;
END;
$$;

-- FUNCTION: update_blockchain_status_on_hash()
CREATE FUNCTION public.update_blockchain_status_on_hash() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- Only auto-advance pending -> submitted on first tx-signature set; never
    -- downgrade a status the application explicitly set (e.g. 'confirmed').
    IF NEW.blockchain_tx_signature IS NOT NULL
       AND OLD.blockchain_tx_signature IS NULL
       AND NEW.blockchain_status = 'pending' THEN
        NEW.blockchain_status = 'submitted';
        NEW.blockchain_submitted_at = NOW();
    END IF;

    RETURN NEW;
END;
$$;

-- FUNCTION: update_updated_at_column()
CREATE FUNCTION public.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


-- ======================================================================
-- SEQUENCES (2)
-- ======================================================================

-- SEQUENCE: p2p_config_audit_id_seq
CREATE SEQUENCE public.p2p_config_audit_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- SEQUENCE: p2p_config_id_seq
CREATE SEQUENCE public.p2p_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


-- ======================================================================
-- TABLES (25)
-- ======================================================================

-- TABLE: settlements
CREATE TABLE public.settlements (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    epoch_id uuid NOT NULL,
    buyer_id uuid NOT NULL,
    seller_id uuid NOT NULL,
    energy_amount numeric(20,8) NOT NULL,
    price_per_kwh numeric(20,8) NOT NULL,
    total_amount numeric(20,8) NOT NULL,
    fee_amount numeric(20,8) DEFAULT 0 NOT NULL,
    net_amount numeric(20,8) NOT NULL,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    transaction_hash character varying(128),
    processed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    retry_count integer DEFAULT 0,
    blockchain_tx_signature character varying(88),
    blockchain_tx_type character varying(50) DEFAULT 'settlement'::character varying,
    blockchain_status character varying(20) DEFAULT 'pending'::character varying,
    blockchain_attempts integer DEFAULT 0,
    blockchain_last_error text,
    blockchain_submitted_at timestamp with time zone,
    blockchain_confirmed_at timestamp with time zone,
    buy_order_id uuid,
    sell_order_id uuid,
    wheeling_charge numeric(20,8) DEFAULT 0,
    loss_factor numeric(20,8) DEFAULT 0,
    loss_cost numeric(20,8) DEFAULT 0,
    effective_energy numeric(20,8) DEFAULT 0,
    buyer_zone_id integer,
    seller_zone_id integer,
    seller_session_token character varying(128),
    buyer_session_token character varying(128),
    erc_certificate_id character varying(255),
    erc_transfer_tx character varying(255),
    error_message text,
    next_retry_at timestamp with time zone,
    buy_signature text,
    sell_signature text,
    buy_payload bytea,
    sell_payload bytea,
    settlement_type character varying(30) DEFAULT 'standard'::character varying,
    imbalance_gap_kwh numeric(20,8),
    imbalance_price numeric(20,8),
    blockchain_tx_hash character varying(88),
    blockchain_error text,
    trade_id uuid,
    CONSTRAINT chk_settlement_status CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying, 'completed'::character varying, 'failed'::character varying, 'permanently_failed'::character varying])::text[]))),
    CONSTRAINT chk_settlement_type CHECK (((settlement_type)::text = ANY ((ARRAY['standard'::character varying, 'imbalance_correction'::character varying, 'vpp_reserve_credit'::character varying, 'grid_penalty'::character varying])::text[])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='25', autovacuum_vacuum_cost_delay='10');

-- TABLE: trading_orders
CREATE TABLE public.trading_orders (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    epoch_id uuid,
    order_type public.order_type NOT NULL,
    energy_amount numeric(20,8) NOT NULL,
    price_per_kwh numeric(20,8) NOT NULL,
    filled_amount numeric(20,8) DEFAULT 0,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    transaction_hash character varying(66),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    settled_at timestamp with time zone,
    side public.order_side,
    kwh_amount numeric(20,8),
    expires_at timestamp with time zone,
    blockchain_tx_signature character varying(88),
    blockchain_tx_type character varying(50),
    blockchain_status character varying(20) DEFAULT 'pending'::character varying,
    blockchain_attempts integer DEFAULT 0,
    blockchain_last_error text,
    blockchain_submitted_at timestamp with time zone,
    blockchain_confirmed_at timestamp with time zone,
    order_pda text,
    filled_at timestamp with time zone,
    zone_id integer,
    refund_tx_signature character varying(88),
    meter_id uuid,
    trigger_price numeric(20,8),
    trigger_type public.trigger_type,
    trigger_status public.trigger_status DEFAULT 'pending'::public.trigger_status,
    triggered_at timestamp with time zone,
    trailing_offset numeric(20,8),
    session_token character varying(128),
    last_peak_price numeric(20,8),
    signature text,
    payload_bytes bytea,
    is_confidential boolean DEFAULT false,
    order_index bigint,
    blockchain_tx_hash character varying(88),
    blockchain_error text,
    retry_count integer DEFAULT 0,
    next_retry_at timestamp with time zone DEFAULT now(),
    time_in_force public.time_in_force DEFAULT 'gtc'::public.time_in_force NOT NULL,
    limit_price numeric(20,8),
    market_segment public.market_segment DEFAULT 'realtime'::public.market_segment NOT NULL,
    CONSTRAINT chk_energy_amount CHECK ((energy_amount > (0)::numeric)),
    CONSTRAINT chk_price CHECK ((price_per_kwh > (0)::numeric))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='25', autovacuum_vacuum_cost_delay='10');

-- TABLE: carbon_credits
CREATE TABLE public.carbon_credits (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    amount numeric(20,8) NOT NULL,
    source character varying(50) NOT NULL,
    source_reference_id uuid,
    status public.carbon_status DEFAULT 'active'::public.carbon_status,
    description character varying(200),
    created_at timestamp with time zone DEFAULT now()
);

-- TABLE: carbon_transactions
CREATE TABLE public.carbon_transactions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    from_user_id uuid,
    to_user_id uuid NOT NULL,
    amount numeric(20,8) NOT NULL,
    price_per_credit numeric(20,8),
    total_value numeric(20,8),
    status public.carbon_transaction_status DEFAULT 'completed'::public.carbon_transaction_status,
    notes character varying(200),
    created_at timestamp with time zone DEFAULT now()
);

-- TABLE: escrow_records
CREATE TABLE public.escrow_records (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    order_id uuid,
    amount numeric(20,8) NOT NULL,
    asset_type character varying(20) NOT NULL,
    escrow_type character varying(20) NOT NULL,
    status character varying(20) DEFAULT 'locked'::character varying NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT chk_escrow_asset CHECK (((asset_type)::text = ANY ((ARRAY['currency'::character varying, 'energy'::character varying])::text[]))),
    CONSTRAINT chk_escrow_status CHECK (((status)::text = ANY ((ARRAY['locked'::character varying, 'released'::character varying, 'refunded'::character varying])::text[])))
);

-- TABLE: futures_orders
CREATE TABLE public.futures_orders (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    product_id uuid NOT NULL,
    side public.futures_order_side NOT NULL,
    order_type public.futures_order_type NOT NULL,
    quantity numeric(20,8) NOT NULL,
    price numeric(20,8) NOT NULL,
    leverage integer DEFAULT 1 NOT NULL,
    status public.futures_order_status DEFAULT 'pending'::public.futures_order_status NOT NULL,
    filled_quantity numeric(20,8) DEFAULT 0,
    average_fill_price numeric(20,8),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- TABLE: futures_positions
CREATE TABLE public.futures_positions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    product_id uuid NOT NULL,
    side public.futures_order_side NOT NULL,
    quantity numeric(20,8) NOT NULL,
    entry_price numeric(20,8) NOT NULL,
    current_price numeric(20,8) NOT NULL,
    leverage integer DEFAULT 1 NOT NULL,
    margin_used numeric(20,8) NOT NULL,
    unrealized_pnl numeric(20,8) DEFAULT 0,
    liquidation_price numeric(20,8),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- TABLE: futures_products
CREATE TABLE public.futures_products (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    symbol character varying(20) NOT NULL,
    base_asset character varying(10) NOT NULL,
    quote_asset character varying(10) NOT NULL,
    contract_size numeric(20,8) NOT NULL,
    expiration_date timestamp with time zone NOT NULL,
    current_price numeric(20,8) NOT NULL,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- TABLE: liquidity_pools
CREATE TABLE public.liquidity_pools (
    id uuid NOT NULL,
    name character varying(255) NOT NULL,
    token_a character varying(255) NOT NULL,
    token_b character varying(255) NOT NULL,
    reserve_a numeric(20,9) DEFAULT 0 NOT NULL,
    reserve_b numeric(20,9) DEFAULT 0 NOT NULL,
    fee_rate numeric(5,4) DEFAULT 0.0030 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- TABLE: market_epochs
CREATE TABLE public.market_epochs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    epoch_number bigint NOT NULL,
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone NOT NULL,
    status public.epoch_status DEFAULT 'pending'::public.epoch_status NOT NULL,
    clearing_price numeric(20,8),
    total_volume numeric(20,8) DEFAULT 0,
    total_orders bigint DEFAULT 0,
    matched_orders bigint DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
)
WITH (autovacuum_vacuum_scale_factor='0.1', autovacuum_analyze_scale_factor='0.05', autovacuum_vacuum_threshold='25', autovacuum_analyze_threshold='10');

-- TABLE: market_epochs_archive
CREATE TABLE public.market_epochs_archive (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    epoch_number bigint NOT NULL,
    start_time timestamp with time zone NOT NULL,
    end_time timestamp with time zone NOT NULL,
    status public.epoch_status DEFAULT 'pending'::public.epoch_status NOT NULL,
    clearing_price numeric(20,8),
    total_volume numeric(20,8) DEFAULT 0,
    total_orders bigint DEFAULT 0,
    matched_orders bigint DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- TABLE: order_matches
CREATE TABLE public.order_matches (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    epoch_id uuid NOT NULL,
    buy_order_id uuid NOT NULL,
    sell_order_id uuid NOT NULL,
    matched_amount numeric(20,8) NOT NULL,
    match_price numeric(20,8) NOT NULL,
    match_time timestamp with time zone DEFAULT now(),
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    settlement_id uuid,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    zone_id integer,
    CONSTRAINT chk_match_status CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'settled'::character varying, 'failed'::character varying])::text[]))),
    CONSTRAINT chk_matched_amount CHECK ((matched_amount > (0)::numeric))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02', autovacuum_vacuum_threshold='50', autovacuum_analyze_threshold='25', autovacuum_vacuum_cost_delay='10');

-- TABLE: outbox_events
CREATE TABLE public.outbox_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    status text DEFAULT 'PENDING'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_attempt_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

-- TABLE: p2p_config
CREATE TABLE public.p2p_config (
    id integer NOT NULL,
    config_key character varying(100) NOT NULL,
    config_value numeric(20,8) NOT NULL,
    config_type character varying(20) DEFAULT 'decimal'::character varying NOT NULL,
    description text,
    category character varying(50) DEFAULT 'general'::character varying NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by integer
);

-- TABLE: p2p_config_audit
CREATE TABLE public.p2p_config_audit (
    id integer NOT NULL,
    config_key character varying(100) NOT NULL,
    old_value numeric(20,8),
    new_value numeric(20,8),
    changed_at timestamp with time zone DEFAULT now() NOT NULL,
    changed_by integer,
    change_reason text
);

-- TABLE: p2p_orders
CREATE TABLE public.p2p_orders (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    side public.p2p_order_side NOT NULL,
    amount numeric NOT NULL,
    price_per_kwh numeric NOT NULL,
    filled_amount numeric DEFAULT 0 NOT NULL,
    status public.p2p_order_status DEFAULT 'open'::public.p2p_order_status NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- TABLE: platform_revenue
CREATE TABLE public.platform_revenue (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    settlement_id uuid NOT NULL,
    amount numeric(20,8) NOT NULL,
    revenue_type character varying(20) NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT chk_revenue_type CHECK (((revenue_type)::text = ANY ((ARRAY['platform_fee'::character varying, 'wheeling_charge'::character varying, 'loss_cost'::character varying])::text[])))
);

-- TABLE: price_alerts
CREATE TABLE public.price_alerts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    target_price numeric(20,8) NOT NULL,
    condition public.alert_condition NOT NULL,
    status public.alert_status DEFAULT 'active'::public.alert_status,
    triggered_at timestamp with time zone,
    triggered_price numeric(20,8),
    repeat boolean DEFAULT false,
    note character varying(200),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

-- TABLE: recurring_order_executions
CREATE TABLE public.recurring_order_executions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    recurring_order_id uuid NOT NULL,
    trading_order_id uuid,
    executed_at timestamp with time zone DEFAULT now(),
    status character varying(20) NOT NULL,
    error_message text,
    energy_amount numeric(20,8),
    price_per_kwh numeric(20,8),
    created_at timestamp with time zone DEFAULT now(),
    blockchain_status character varying(20) DEFAULT 'unprocessed'::character varying,
    blockchain_tx_hash character varying(88),
    blockchain_error text,
    retry_count integer DEFAULT 0,
    next_retry_at timestamp with time zone DEFAULT now()
);

-- TABLE: recurring_orders
CREATE TABLE public.recurring_orders (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    side public.order_side NOT NULL,
    energy_amount numeric(20,8) NOT NULL,
    max_price_per_kwh numeric(20,8),
    min_price_per_kwh numeric(20,8),
    interval_type public.interval_type NOT NULL,
    interval_value integer DEFAULT 1,
    next_execution_at timestamp with time zone NOT NULL,
    last_executed_at timestamp with time zone,
    status public.recurring_status DEFAULT 'active'::public.recurring_status,
    total_executions integer DEFAULT 0,
    max_executions integer,
    name character varying(100),
    description text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    session_token character varying(128),
    CONSTRAINT recurring_orders_interval_value_check CHECK ((interval_value > 0))
);

-- TABLE: settlements_archive
CREATE TABLE public.settlements_archive (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    epoch_id uuid NOT NULL,
    buyer_id uuid NOT NULL,
    seller_id uuid NOT NULL,
    energy_amount numeric(20,8) NOT NULL,
    price_per_kwh numeric(20,8) NOT NULL,
    total_amount numeric(20,8) NOT NULL,
    fee_amount numeric(20,8) DEFAULT 0 NOT NULL,
    net_amount numeric(20,8) NOT NULL,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    transaction_hash character varying(66),
    processed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    retry_count integer DEFAULT 0,
    blockchain_tx_signature character varying(88),
    blockchain_tx_type character varying(50) DEFAULT 'settlement'::character varying,
    blockchain_status character varying(20) DEFAULT 'pending'::character varying,
    blockchain_attempts integer DEFAULT 0,
    blockchain_last_error text,
    blockchain_submitted_at timestamp with time zone,
    blockchain_confirmed_at timestamp with time zone,
    CONSTRAINT chk_settlement_status CHECK (((status)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying, 'completed'::character varying, 'failed'::character varying])::text[])))
);

-- TABLE: swap_transactions
CREATE TABLE public.swap_transactions (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    pool_id uuid NOT NULL,
    input_token character varying(255) NOT NULL,
    input_amount numeric(20,9) NOT NULL,
    output_token character varying(255) NOT NULL,
    output_amount numeric(20,9) NOT NULL,
    fee_amount numeric(20,9) NOT NULL,
    slippage_tolerance numeric(5,4),
    status character varying(50) NOT NULL,
    tx_hash character varying(255),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

-- TABLE: trading_orders_archive
CREATE TABLE public.trading_orders_archive (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    user_id uuid NOT NULL,
    epoch_id uuid,
    order_type character varying(10) NOT NULL,
    energy_amount numeric(20,8) NOT NULL,
    price_per_kwh numeric(20,8) NOT NULL,
    filled_amount numeric(20,8) DEFAULT 0,
    status public.order_status DEFAULT 'pending'::public.order_status NOT NULL,
    transaction_hash character varying(66),
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    settled_at timestamp with time zone,
    side public.order_side,
    kwh_amount numeric(20,8),
    expires_at timestamp with time zone,
    blockchain_tx_signature character varying(88),
    blockchain_tx_type character varying(50),
    blockchain_status character varying(20) DEFAULT 'pending'::character varying,
    blockchain_attempts integer DEFAULT 0,
    blockchain_last_error text,
    blockchain_submitted_at timestamp with time zone,
    blockchain_confirmed_at timestamp with time zone,
    CONSTRAINT chk_energy_amount CHECK ((energy_amount > (0)::numeric)),
    CONSTRAINT chk_price CHECK ((price_per_kwh > (0)::numeric))
);

-- TABLE: vpp_clusters
CREATE TABLE public.vpp_clusters (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    cluster_id text NOT NULL,
    zone_id integer,
    total_capacity_kwh double precision DEFAULT 0.0 NOT NULL,
    current_stored_kwh double precision DEFAULT 0.0 NOT NULL,
    soc_percentage double precision DEFAULT 0.0 NOT NULL,
    flex_up_kw double precision DEFAULT 0.0 NOT NULL,
    flex_down_kw double precision DEFAULT 0.0 NOT NULL,
    health_score double precision DEFAULT 100.0 NOT NULL,
    resource_count integer DEFAULT 0 NOT NULL,
    last_update timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now()
);

-- TABLE: vpp_dispatch_history
CREATE TABLE public.vpp_dispatch_history (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    cluster_id text NOT NULL,
    amount_kw double precision NOT NULL,
    status text NOT NULL,
    pda_address text,
    "timestamp" timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now()
);


-- ======================================================================
-- SEQUENCE OWNERSHIP (2)
-- ======================================================================

-- SEQUENCE OWNED BY: p2p_config_audit_id_seq
ALTER SEQUENCE public.p2p_config_audit_id_seq OWNED BY public.p2p_config_audit.id;

-- SEQUENCE OWNED BY: p2p_config_id_seq
ALTER SEQUENCE public.p2p_config_id_seq OWNED BY public.p2p_config.id;


-- ======================================================================
-- COLUMN DEFAULTS (2)
-- ======================================================================

-- DEFAULT: p2p_config id
ALTER TABLE ONLY public.p2p_config ALTER COLUMN id SET DEFAULT nextval('public.p2p_config_id_seq'::regclass);

-- DEFAULT: p2p_config_audit id
ALTER TABLE ONLY public.p2p_config_audit ALTER COLUMN id SET DEFAULT nextval('public.p2p_config_audit_id_seq'::regclass);


-- ======================================================================
-- PRIMARY KEY / UNIQUE / CHECK CONSTRAINTS (31)
-- ======================================================================

-- CONSTRAINT: carbon_credits carbon_credits_pkey
ALTER TABLE ONLY public.carbon_credits
    ADD CONSTRAINT carbon_credits_pkey PRIMARY KEY (id);

-- CONSTRAINT: carbon_transactions carbon_transactions_pkey
ALTER TABLE ONLY public.carbon_transactions
    ADD CONSTRAINT carbon_transactions_pkey PRIMARY KEY (id);

-- CONSTRAINT: escrow_records escrow_records_pkey
ALTER TABLE ONLY public.escrow_records
    ADD CONSTRAINT escrow_records_pkey PRIMARY KEY (id);

-- CONSTRAINT: futures_orders futures_orders_pkey
ALTER TABLE ONLY public.futures_orders
    ADD CONSTRAINT futures_orders_pkey PRIMARY KEY (id);

-- CONSTRAINT: futures_positions futures_positions_pkey
ALTER TABLE ONLY public.futures_positions
    ADD CONSTRAINT futures_positions_pkey PRIMARY KEY (id);

-- CONSTRAINT: futures_positions futures_positions_user_id_product_id_side_key
ALTER TABLE ONLY public.futures_positions
    ADD CONSTRAINT futures_positions_user_id_product_id_side_key UNIQUE (user_id, product_id, side);

-- CONSTRAINT: futures_products futures_products_pkey
ALTER TABLE ONLY public.futures_products
    ADD CONSTRAINT futures_products_pkey PRIMARY KEY (id);

-- CONSTRAINT: futures_products futures_products_symbol_key
ALTER TABLE ONLY public.futures_products
    ADD CONSTRAINT futures_products_symbol_key UNIQUE (symbol);

-- CONSTRAINT: liquidity_pools liquidity_pools_pkey
ALTER TABLE ONLY public.liquidity_pools
    ADD CONSTRAINT liquidity_pools_pkey PRIMARY KEY (id);

-- CONSTRAINT: market_epochs_archive market_epochs_archive_epoch_number_key
ALTER TABLE ONLY public.market_epochs_archive
    ADD CONSTRAINT market_epochs_archive_epoch_number_key UNIQUE (epoch_number);

-- CONSTRAINT: market_epochs_archive market_epochs_archive_pkey
ALTER TABLE ONLY public.market_epochs_archive
    ADD CONSTRAINT market_epochs_archive_pkey PRIMARY KEY (id);

-- CONSTRAINT: market_epochs market_epochs_epoch_number_key
ALTER TABLE ONLY public.market_epochs
    ADD CONSTRAINT market_epochs_epoch_number_key UNIQUE (epoch_number);

-- CONSTRAINT: market_epochs market_epochs_pkey
ALTER TABLE ONLY public.market_epochs
    ADD CONSTRAINT market_epochs_pkey PRIMARY KEY (id);

-- CONSTRAINT: order_matches order_matches_pkey
ALTER TABLE ONLY public.order_matches
    ADD CONSTRAINT order_matches_pkey PRIMARY KEY (id);

-- CONSTRAINT: outbox_events outbox_events_pkey
ALTER TABLE ONLY public.outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (id);

-- CONSTRAINT: p2p_config_audit p2p_config_audit_pkey
ALTER TABLE ONLY public.p2p_config_audit
    ADD CONSTRAINT p2p_config_audit_pkey PRIMARY KEY (id);

-- CONSTRAINT: p2p_config p2p_config_config_key_key
ALTER TABLE ONLY public.p2p_config
    ADD CONSTRAINT p2p_config_config_key_key UNIQUE (config_key);

-- CONSTRAINT: p2p_config p2p_config_pkey
ALTER TABLE ONLY public.p2p_config
    ADD CONSTRAINT p2p_config_pkey PRIMARY KEY (id);

-- CONSTRAINT: p2p_orders p2p_orders_pkey
ALTER TABLE ONLY public.p2p_orders
    ADD CONSTRAINT p2p_orders_pkey PRIMARY KEY (id);

-- CONSTRAINT: platform_revenue platform_revenue_pkey
ALTER TABLE ONLY public.platform_revenue
    ADD CONSTRAINT platform_revenue_pkey PRIMARY KEY (id);

-- CONSTRAINT: price_alerts price_alerts_pkey
ALTER TABLE ONLY public.price_alerts
    ADD CONSTRAINT price_alerts_pkey PRIMARY KEY (id);

-- CONSTRAINT: recurring_order_executions recurring_order_executions_pkey
ALTER TABLE ONLY public.recurring_order_executions
    ADD CONSTRAINT recurring_order_executions_pkey PRIMARY KEY (id);

-- CONSTRAINT: recurring_orders recurring_orders_pkey
ALTER TABLE ONLY public.recurring_orders
    ADD CONSTRAINT recurring_orders_pkey PRIMARY KEY (id);

-- CONSTRAINT: settlements_archive settlements_archive_pkey
ALTER TABLE ONLY public.settlements_archive
    ADD CONSTRAINT settlements_archive_pkey PRIMARY KEY (id);

-- CONSTRAINT: settlements settlements_pkey
ALTER TABLE ONLY public.settlements
    ADD CONSTRAINT settlements_pkey PRIMARY KEY (id);

-- CONSTRAINT: swap_transactions swap_transactions_pkey
ALTER TABLE ONLY public.swap_transactions
    ADD CONSTRAINT swap_transactions_pkey PRIMARY KEY (id);

-- CONSTRAINT: trading_orders_archive trading_orders_archive_pkey
ALTER TABLE ONLY public.trading_orders_archive
    ADD CONSTRAINT trading_orders_archive_pkey PRIMARY KEY (id);

-- CONSTRAINT: trading_orders trading_orders_pkey
ALTER TABLE ONLY public.trading_orders
    ADD CONSTRAINT trading_orders_pkey PRIMARY KEY (id);

-- CONSTRAINT: vpp_clusters vpp_clusters_cluster_id_key
ALTER TABLE ONLY public.vpp_clusters
    ADD CONSTRAINT vpp_clusters_cluster_id_key UNIQUE (cluster_id);

-- CONSTRAINT: vpp_clusters vpp_clusters_pkey
ALTER TABLE ONLY public.vpp_clusters
    ADD CONSTRAINT vpp_clusters_pkey PRIMARY KEY (id);

-- CONSTRAINT: vpp_dispatch_history vpp_dispatch_history_pkey
ALTER TABLE ONLY public.vpp_dispatch_history
    ADD CONSTRAINT vpp_dispatch_history_pkey PRIMARY KEY (id);


-- ======================================================================
-- INDEXES (132)
-- ======================================================================

-- INDEX: idx_carbon_credits_source
CREATE INDEX idx_carbon_credits_source ON public.carbon_credits USING btree (source, created_at);

-- INDEX: idx_carbon_credits_user
CREATE INDEX idx_carbon_credits_user ON public.carbon_credits USING btree (user_id, status);

-- INDEX: idx_carbon_transactions_from
CREATE INDEX idx_carbon_transactions_from ON public.carbon_transactions USING btree (from_user_id, created_at);

-- INDEX: idx_carbon_transactions_to
CREATE INDEX idx_carbon_transactions_to ON public.carbon_transactions USING btree (to_user_id, created_at);

-- INDEX: idx_escrow_records_order
CREATE INDEX idx_escrow_records_order ON public.escrow_records USING btree (order_id);

-- INDEX: idx_escrow_records_order_status
CREATE INDEX idx_escrow_records_order_status ON public.escrow_records USING btree (order_id, status) WHERE ((status)::text = 'locked'::text);

-- INDEX: idx_escrow_records_status
CREATE INDEX idx_escrow_records_status ON public.escrow_records USING btree (status);

-- INDEX: idx_escrow_records_user
CREATE INDEX idx_escrow_records_user ON public.escrow_records USING btree (user_id);

-- INDEX: idx_futures_orders_status
CREATE INDEX idx_futures_orders_status ON public.futures_orders USING btree (status);

-- INDEX: idx_futures_orders_user
CREATE INDEX idx_futures_orders_user ON public.futures_orders USING btree (user_id);

-- INDEX: idx_futures_positions_user
CREATE INDEX idx_futures_positions_user ON public.futures_positions USING btree (user_id);

-- INDEX: idx_futures_products_active
CREATE INDEX idx_futures_products_active ON public.futures_products USING btree (is_active);

-- INDEX: idx_market_epochs_active
CREATE INDEX idx_market_epochs_active ON public.market_epochs USING btree (start_time DESC, end_time DESC) WHERE (status = ANY (ARRAY['active'::public.epoch_status, 'pending'::public.epoch_status]));

-- INDEX: idx_market_epochs_archive_number
CREATE INDEX idx_market_epochs_archive_number ON public.market_epochs_archive USING btree (epoch_number);

-- INDEX: idx_market_epochs_archive_time
CREATE INDEX idx_market_epochs_archive_time ON public.market_epochs_archive USING btree (start_time);

-- INDEX: idx_market_epochs_number
CREATE INDEX idx_market_epochs_number ON public.market_epochs USING btree (epoch_number);

-- INDEX: idx_market_epochs_status
CREATE INDEX idx_market_epochs_status ON public.market_epochs USING btree (status);

-- INDEX: idx_market_epochs_status_end
CREATE INDEX idx_market_epochs_status_end ON public.market_epochs USING btree (status, end_time DESC, start_time DESC);

-- INDEX: idx_market_epochs_status_time
CREATE INDEX idx_market_epochs_status_time ON public.market_epochs USING btree (status, start_time DESC);

-- INDEX: idx_market_epochs_time
CREATE INDEX idx_market_epochs_time ON public.market_epochs USING btree (start_time, end_time);

-- INDEX: idx_order_matches_buy_order
CREATE INDEX idx_order_matches_buy_order ON public.order_matches USING btree (buy_order_id);

-- INDEX: idx_order_matches_epoch
CREATE INDEX idx_order_matches_epoch ON public.order_matches USING btree (epoch_id);

-- INDEX: idx_order_matches_epoch_status
CREATE INDEX idx_order_matches_epoch_status ON public.order_matches USING btree (epoch_id, status);

-- INDEX: idx_order_matches_match_time
CREATE INDEX idx_order_matches_match_time ON public.order_matches USING brin (match_time);

-- INDEX: idx_order_matches_orders
CREATE INDEX idx_order_matches_orders ON public.order_matches USING btree (buy_order_id, sell_order_id);

-- INDEX: idx_order_matches_sell_order
CREATE INDEX idx_order_matches_sell_order ON public.order_matches USING btree (sell_order_id);

-- INDEX: idx_order_matches_settlement_covering
CREATE INDEX idx_order_matches_settlement_covering ON public.order_matches USING btree (epoch_id, status, buy_order_id, sell_order_id, matched_amount, match_price) WHERE ((status)::text = 'pending'::text);

-- INDEX: idx_order_matches_status
CREATE INDEX idx_order_matches_status ON public.order_matches USING btree (status);

-- INDEX: idx_order_matches_zone_id
CREATE INDEX idx_order_matches_zone_id ON public.order_matches USING btree (zone_id);

-- INDEX: idx_order_matches_zone_time
CREATE INDEX idx_order_matches_zone_time ON public.order_matches USING btree (zone_id, match_time DESC) WHERE (zone_id IS NOT NULL);

-- INDEX: idx_outbox_events_pending
CREATE INDEX idx_outbox_events_pending ON public.outbox_events USING btree (status, created_at);

-- INDEX: idx_p2p_config_active
CREATE INDEX idx_p2p_config_active ON public.p2p_config USING btree (is_active);

-- INDEX: idx_p2p_config_audit_key
CREATE INDEX idx_p2p_config_audit_key ON public.p2p_config_audit USING btree (config_key);

-- INDEX: idx_p2p_config_audit_time
CREATE INDEX idx_p2p_config_audit_time ON public.p2p_config_audit USING btree (changed_at);

-- INDEX: idx_p2p_config_category
CREATE INDEX idx_p2p_config_category ON public.p2p_config USING btree (category);

-- INDEX: idx_p2p_config_key
CREATE INDEX idx_p2p_config_key ON public.p2p_config USING btree (config_key);

-- INDEX: idx_p2p_orders_status
CREATE INDEX idx_p2p_orders_status ON public.p2p_orders USING btree (status);

-- INDEX: idx_p2p_orders_user_id
CREATE INDEX idx_p2p_orders_user_id ON public.p2p_orders USING btree (user_id);

-- INDEX: idx_platform_revenue_date
CREATE INDEX idx_platform_revenue_date ON public.platform_revenue USING btree (created_at);

-- INDEX: idx_platform_revenue_settlement
CREATE INDEX idx_platform_revenue_settlement ON public.platform_revenue USING btree (settlement_id);

-- INDEX: idx_platform_revenue_type
CREATE INDEX idx_platform_revenue_type ON public.platform_revenue USING btree (revenue_type);

-- INDEX: idx_price_alerts_active
CREATE INDEX idx_price_alerts_active ON public.price_alerts USING btree (status, target_price) WHERE (status = 'active'::public.alert_status);

-- INDEX: idx_price_alerts_user
CREATE INDEX idx_price_alerts_user ON public.price_alerts USING btree (user_id, status);

-- INDEX: idx_recurring_executions_blockchain_status
CREATE INDEX idx_recurring_executions_blockchain_status ON public.recurring_order_executions USING btree (blockchain_status) WHERE ((blockchain_status)::text <> 'success'::text);

-- INDEX: idx_recurring_executions_order
CREATE INDEX idx_recurring_executions_order ON public.recurring_order_executions USING btree (recurring_order_id, executed_at DESC);

-- INDEX: idx_recurring_executions_retry_at
CREATE INDEX idx_recurring_executions_retry_at ON public.recurring_order_executions USING btree (next_retry_at) WHERE ((blockchain_status)::text = 'failed_retry'::text);

-- INDEX: idx_recurring_orders_next_exec
CREATE INDEX idx_recurring_orders_next_exec ON public.recurring_orders USING btree (next_execution_at) WHERE (status = 'active'::public.recurring_status);

-- INDEX: idx_recurring_orders_session
CREATE INDEX idx_recurring_orders_session ON public.recurring_orders USING btree (session_token) WHERE (session_token IS NOT NULL);

-- INDEX: idx_recurring_orders_user
CREATE INDEX idx_recurring_orders_user ON public.recurring_orders USING btree (user_id, status);

-- INDEX: idx_settlements_archive_created
CREATE INDEX idx_settlements_archive_created ON public.settlements_archive USING btree (created_at);

-- INDEX: idx_settlements_archive_epoch
CREATE INDEX idx_settlements_archive_epoch ON public.settlements_archive USING btree (epoch_id);

-- INDEX: idx_settlements_blockchain_status
CREATE INDEX idx_settlements_blockchain_status ON public.settlements USING btree (blockchain_status);

-- INDEX: idx_settlements_blockchain_submitted
CREATE INDEX idx_settlements_blockchain_submitted ON public.settlements USING btree (blockchain_submitted_at);

-- INDEX: idx_settlements_buyer
CREATE INDEX idx_settlements_buyer ON public.settlements USING btree (buyer_id);

-- INDEX: idx_settlements_buyer_created
CREATE INDEX idx_settlements_buyer_created ON public.settlements USING btree (buyer_id, created_at DESC);

-- INDEX: idx_settlements_buyer_time
CREATE INDEX idx_settlements_buyer_time ON public.settlements USING btree (buyer_id, created_at DESC) INCLUDE (energy_amount, price_per_kwh, total_amount, status);

-- INDEX: idx_settlements_buyer_zone
CREATE INDEX idx_settlements_buyer_zone ON public.settlements USING btree (buyer_zone_id, created_at DESC) WHERE (buyer_zone_id IS NOT NULL);

-- INDEX: idx_settlements_epoch
CREATE INDEX idx_settlements_epoch ON public.settlements USING btree (epoch_id);

-- INDEX: idx_settlements_next_retry_at
CREATE INDEX idx_settlements_next_retry_at ON public.settlements USING btree (next_retry_at) WHERE ((status)::text = 'pending'::text);

-- INDEX: idx_settlements_pending
CREATE INDEX idx_settlements_pending ON public.settlements USING btree (status, created_at) WHERE ((status)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying])::text[]));

-- INDEX: idx_settlements_retry_at
CREATE INDEX idx_settlements_retry_at ON public.settlements USING btree (next_retry_at) WHERE ((blockchain_status)::text = 'failed_retry'::text);

-- INDEX: idx_settlements_retry_count
CREATE INDEX idx_settlements_retry_count ON public.settlements USING btree (retry_count);

-- INDEX: idx_settlements_seller
CREATE INDEX idx_settlements_seller ON public.settlements USING btree (seller_id);

-- INDEX: idx_settlements_seller_created
CREATE INDEX idx_settlements_seller_created ON public.settlements USING btree (seller_id, created_at DESC);

-- INDEX: idx_settlements_seller_time
CREATE INDEX idx_settlements_seller_time ON public.settlements USING btree (seller_id, created_at DESC) INCLUDE (energy_amount, price_per_kwh, total_amount, status);

-- INDEX: idx_settlements_seller_zone
CREATE INDEX idx_settlements_seller_zone ON public.settlements USING btree (seller_zone_id, created_at DESC) WHERE (seller_zone_id IS NOT NULL);

-- INDEX: idx_settlements_status
CREATE INDEX idx_settlements_status ON public.settlements USING btree (status);

-- INDEX: idx_settlements_transaction
CREATE INDEX idx_settlements_transaction ON public.settlements USING btree (transaction_hash);

-- INDEX: idx_settlements_tx_signature
CREATE INDEX idx_settlements_tx_signature ON public.settlements USING btree (blockchain_tx_signature);

-- INDEX: idx_settlements_tx_type
CREATE INDEX idx_settlements_tx_type ON public.settlements USING btree (blockchain_tx_type);

-- INDEX: idx_settlements_type
CREATE INDEX idx_settlements_type ON public.settlements USING btree (settlement_type);

-- INDEX: idx_swap_pool_id
CREATE INDEX idx_swap_pool_id ON public.swap_transactions USING btree (pool_id);

-- INDEX: idx_swap_user_id
CREATE INDEX idx_swap_user_id ON public.swap_transactions USING btree (user_id);

-- INDEX: idx_trading_orders_active
CREATE INDEX idx_trading_orders_active ON public.trading_orders USING btree (status, price_per_kwh) WHERE (status = ANY (ARRAY['pending'::public.order_status, 'active'::public.order_status, 'partially_filled'::public.order_status]));

-- INDEX: idx_trading_orders_archive_created
CREATE INDEX idx_trading_orders_archive_created ON public.trading_orders_archive USING btree (created_at);

-- INDEX: idx_trading_orders_archive_user
CREATE INDEX idx_trading_orders_archive_user ON public.trading_orders_archive USING btree (user_id);

-- INDEX: idx_trading_orders_blockchain_status
CREATE INDEX idx_trading_orders_blockchain_status ON public.trading_orders USING btree (blockchain_status);

-- INDEX: idx_trading_orders_blockchain_submitted
CREATE INDEX idx_trading_orders_blockchain_submitted ON public.trading_orders USING btree (blockchain_submitted_at);

-- INDEX: idx_trading_orders_created
CREATE INDEX idx_trading_orders_created ON public.trading_orders USING btree (created_at);

-- INDEX: idx_trading_orders_epoch
CREATE INDEX idx_trading_orders_epoch ON public.trading_orders USING btree (epoch_id);

-- INDEX: idx_trading_orders_epoch_side_status_price
CREATE INDEX idx_trading_orders_epoch_side_status_price ON public.trading_orders USING btree (epoch_id, side, status, price_per_kwh DESC, created_at) WHERE ((status = ANY (ARRAY['pending'::public.order_status, 'active'::public.order_status, 'partially_filled'::public.order_status])) AND (price_per_kwh IS NOT NULL));

-- INDEX: idx_trading_orders_id_status
CREATE INDEX idx_trading_orders_id_status ON public.trading_orders USING btree (id, status, side, energy_amount, filled_amount, price_per_kwh);

-- INDEX: idx_trading_orders_meter
CREATE INDEX idx_trading_orders_meter ON public.trading_orders USING btree (meter_id);

-- INDEX: idx_trading_orders_order_pda
CREATE INDEX idx_trading_orders_order_pda ON public.trading_orders USING btree (order_pda);

-- INDEX: idx_trading_orders_retry_at
CREATE INDEX idx_trading_orders_retry_at ON public.trading_orders USING btree (next_retry_at) WHERE ((blockchain_status)::text = 'failed_retry'::text);

-- INDEX: idx_trading_orders_session
CREATE INDEX idx_trading_orders_session ON public.trading_orders USING btree (session_token) WHERE (session_token IS NOT NULL);

-- INDEX: idx_trading_orders_side
CREATE INDEX idx_trading_orders_side ON public.trading_orders USING btree (side);

-- INDEX: idx_trading_orders_signature
CREATE UNIQUE INDEX idx_trading_orders_signature ON public.trading_orders USING btree (signature) WHERE (signature IS NOT NULL);

-- INDEX: idx_trading_orders_status
CREATE INDEX idx_trading_orders_status ON public.trading_orders USING btree (status);

-- INDEX: idx_trading_orders_trigger_pending
CREATE INDEX idx_trading_orders_trigger_pending ON public.trading_orders USING btree (trigger_type, trigger_status, trigger_price) WHERE ((trigger_type IS NOT NULL) AND (trigger_status = 'pending'::public.trigger_status));

-- INDEX: idx_trading_orders_tx_signature
CREATE INDEX idx_trading_orders_tx_signature ON public.trading_orders USING btree (blockchain_tx_signature);

-- INDEX: idx_trading_orders_tx_type
CREATE INDEX idx_trading_orders_tx_type ON public.trading_orders USING btree (blockchain_tx_type);

-- INDEX: idx_trading_orders_type
CREATE INDEX idx_trading_orders_type ON public.trading_orders USING btree (order_type);

-- INDEX: idx_trading_orders_type_status_price
CREATE INDEX idx_trading_orders_type_status_price ON public.trading_orders USING btree (order_type, status, price_per_kwh) WHERE (status = ANY (ARRAY['active'::public.order_status, 'partially_filled'::public.order_status]));

-- INDEX: idx_trading_orders_user
CREATE INDEX idx_trading_orders_user ON public.trading_orders USING btree (user_id);

-- INDEX: idx_trading_orders_user_conditional
CREATE INDEX idx_trading_orders_user_conditional ON public.trading_orders USING btree (user_id, trigger_type, trigger_status) WHERE (trigger_type IS NOT NULL);

-- INDEX: idx_trading_orders_user_covering
CREATE INDEX idx_trading_orders_user_covering ON public.trading_orders USING btree (user_id, status) INCLUDE (order_type, energy_amount, price_per_kwh, created_at);

-- INDEX: idx_trading_orders_user_created
CREATE INDEX idx_trading_orders_user_created ON public.trading_orders USING btree (user_id, created_at DESC);

-- INDEX: idx_trading_orders_zone_active
CREATE INDEX idx_trading_orders_zone_active ON public.trading_orders USING btree (zone_id, status, price_per_kwh) WHERE (status = ANY (ARRAY['pending'::public.order_status, 'active'::public.order_status, 'partially_filled'::public.order_status]));

-- INDEX: idx_vpp_clusters_zone
CREATE INDEX idx_vpp_clusters_zone ON public.vpp_clusters USING btree (zone_id);

-- INDEX: idx_vpp_dispatch_timestamp
CREATE INDEX idx_vpp_dispatch_timestamp ON public.vpp_dispatch_history USING btree ("timestamp");

-- INDEX: market_epochs_archive_epoch_number_idx
CREATE INDEX market_epochs_archive_epoch_number_idx ON public.market_epochs_archive USING btree (epoch_number);

-- INDEX: market_epochs_archive_start_time_end_time_idx
CREATE INDEX market_epochs_archive_start_time_end_time_idx ON public.market_epochs_archive USING btree (start_time, end_time);

-- INDEX: market_epochs_archive_status_idx
CREATE INDEX market_epochs_archive_status_idx ON public.market_epochs_archive USING btree (status);

-- INDEX: market_epochs_archive_status_start_time_idx
CREATE INDEX market_epochs_archive_status_start_time_idx ON public.market_epochs_archive USING btree (status, start_time DESC);

-- INDEX: settlements_archive_blockchain_status_idx
CREATE INDEX settlements_archive_blockchain_status_idx ON public.settlements_archive USING btree (blockchain_status);

-- INDEX: settlements_archive_blockchain_submitted_at_idx
CREATE INDEX settlements_archive_blockchain_submitted_at_idx ON public.settlements_archive USING btree (blockchain_submitted_at);

-- INDEX: settlements_archive_blockchain_tx_signature_idx
CREATE INDEX settlements_archive_blockchain_tx_signature_idx ON public.settlements_archive USING btree (blockchain_tx_signature);

-- INDEX: settlements_archive_blockchain_tx_type_idx
CREATE INDEX settlements_archive_blockchain_tx_type_idx ON public.settlements_archive USING btree (blockchain_tx_type);

-- INDEX: settlements_archive_buyer_id_created_at_idx
CREATE INDEX settlements_archive_buyer_id_created_at_idx ON public.settlements_archive USING btree (buyer_id, created_at DESC);

-- INDEX: settlements_archive_buyer_id_idx
CREATE INDEX settlements_archive_buyer_id_idx ON public.settlements_archive USING btree (buyer_id);

-- INDEX: settlements_archive_epoch_id_idx
CREATE INDEX settlements_archive_epoch_id_idx ON public.settlements_archive USING btree (epoch_id);

-- INDEX: settlements_archive_retry_count_idx
CREATE INDEX settlements_archive_retry_count_idx ON public.settlements_archive USING btree (retry_count);

-- INDEX: settlements_archive_seller_id_created_at_idx
CREATE INDEX settlements_archive_seller_id_created_at_idx ON public.settlements_archive USING btree (seller_id, created_at DESC);

-- INDEX: settlements_archive_seller_id_idx
CREATE INDEX settlements_archive_seller_id_idx ON public.settlements_archive USING btree (seller_id);

-- INDEX: settlements_archive_status_created_at_idx
CREATE INDEX settlements_archive_status_created_at_idx ON public.settlements_archive USING btree (status, created_at) WHERE ((status)::text = ANY ((ARRAY['pending'::character varying, 'processing'::character varying])::text[]));

-- INDEX: settlements_archive_status_idx
CREATE INDEX settlements_archive_status_idx ON public.settlements_archive USING btree (status);

-- INDEX: settlements_archive_transaction_hash_idx
CREATE INDEX settlements_archive_transaction_hash_idx ON public.settlements_archive USING btree (transaction_hash);

-- INDEX: trading_orders_archive_blockchain_status_idx
CREATE INDEX trading_orders_archive_blockchain_status_idx ON public.trading_orders_archive USING btree (blockchain_status);

-- INDEX: trading_orders_archive_blockchain_submitted_at_idx
CREATE INDEX trading_orders_archive_blockchain_submitted_at_idx ON public.trading_orders_archive USING btree (blockchain_submitted_at);

-- INDEX: trading_orders_archive_blockchain_tx_signature_idx
CREATE INDEX trading_orders_archive_blockchain_tx_signature_idx ON public.trading_orders_archive USING btree (blockchain_tx_signature);

-- INDEX: trading_orders_archive_blockchain_tx_type_idx
CREATE INDEX trading_orders_archive_blockchain_tx_type_idx ON public.trading_orders_archive USING btree (blockchain_tx_type);

-- INDEX: trading_orders_archive_created_at_idx
CREATE INDEX trading_orders_archive_created_at_idx ON public.trading_orders_archive USING btree (created_at);

-- INDEX: trading_orders_archive_epoch_id_idx
CREATE INDEX trading_orders_archive_epoch_id_idx ON public.trading_orders_archive USING btree (epoch_id);

-- INDEX: trading_orders_archive_order_type_idx
CREATE INDEX trading_orders_archive_order_type_idx ON public.trading_orders_archive USING btree (order_type);

-- INDEX: trading_orders_archive_order_type_status_price_per_kwh_idx
CREATE INDEX trading_orders_archive_order_type_status_price_per_kwh_idx ON public.trading_orders_archive USING btree (order_type, status, price_per_kwh) WHERE (status = ANY (ARRAY['active'::public.order_status, 'partially_filled'::public.order_status]));

-- INDEX: trading_orders_archive_side_idx
CREATE INDEX trading_orders_archive_side_idx ON public.trading_orders_archive USING btree (side);

-- INDEX: trading_orders_archive_status_idx
CREATE INDEX trading_orders_archive_status_idx ON public.trading_orders_archive USING btree (status);

-- INDEX: trading_orders_archive_status_price_per_kwh_idx
CREATE INDEX trading_orders_archive_status_price_per_kwh_idx ON public.trading_orders_archive USING btree (status, price_per_kwh) WHERE (status = ANY (ARRAY['pending'::public.order_status, 'active'::public.order_status, 'partially_filled'::public.order_status]));

-- INDEX: trading_orders_archive_user_id_created_at_idx
CREATE INDEX trading_orders_archive_user_id_created_at_idx ON public.trading_orders_archive USING btree (user_id, created_at DESC);

-- INDEX: trading_orders_archive_user_id_idx
CREATE INDEX trading_orders_archive_user_id_idx ON public.trading_orders_archive USING btree (user_id);

-- INDEX: trading_orders_archive_user_id_status_order_type_energy_amo_idx
CREATE INDEX trading_orders_archive_user_id_status_order_type_energy_amo_idx ON public.trading_orders_archive USING btree (user_id, status) INCLUDE (order_type, energy_amount, price_per_kwh, created_at);


-- ======================================================================
-- FK CONSTRAINTS (intra-trading only) (14)
-- ======================================================================

-- FK CONSTRAINT: escrow_records escrow_records_order_id_fkey
ALTER TABLE ONLY public.escrow_records
    ADD CONSTRAINT escrow_records_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.trading_orders(id) ON DELETE CASCADE;

-- FK CONSTRAINT: order_matches fk_order_matches_settlement
ALTER TABLE ONLY public.order_matches
    ADD CONSTRAINT fk_order_matches_settlement FOREIGN KEY (settlement_id) REFERENCES public.settlements(id) ON DELETE SET NULL;

-- FK CONSTRAINT: futures_orders futures_orders_product_id_fkey
ALTER TABLE ONLY public.futures_orders
    ADD CONSTRAINT futures_orders_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.futures_products(id) ON DELETE CASCADE;

-- FK CONSTRAINT: futures_positions futures_positions_product_id_fkey
ALTER TABLE ONLY public.futures_positions
    ADD CONSTRAINT futures_positions_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.futures_products(id) ON DELETE CASCADE;

-- FK CONSTRAINT: order_matches order_matches_buy_order_id_fkey
ALTER TABLE ONLY public.order_matches
    ADD CONSTRAINT order_matches_buy_order_id_fkey FOREIGN KEY (buy_order_id) REFERENCES public.trading_orders(id) ON DELETE CASCADE;

-- FK CONSTRAINT: order_matches order_matches_epoch_id_fkey
ALTER TABLE ONLY public.order_matches
    ADD CONSTRAINT order_matches_epoch_id_fkey FOREIGN KEY (epoch_id) REFERENCES public.market_epochs(id) ON DELETE CASCADE;

-- FK CONSTRAINT: order_matches order_matches_sell_order_id_fkey
ALTER TABLE ONLY public.order_matches
    ADD CONSTRAINT order_matches_sell_order_id_fkey FOREIGN KEY (sell_order_id) REFERENCES public.trading_orders(id) ON DELETE CASCADE;

-- FK CONSTRAINT: platform_revenue platform_revenue_settlement_id_fkey
ALTER TABLE ONLY public.platform_revenue
    ADD CONSTRAINT platform_revenue_settlement_id_fkey FOREIGN KEY (settlement_id) REFERENCES public.settlements(id) ON DELETE CASCADE;

-- FK CONSTRAINT: recurring_order_executions recurring_order_executions_recurring_order_id_fkey
ALTER TABLE ONLY public.recurring_order_executions
    ADD CONSTRAINT recurring_order_executions_recurring_order_id_fkey FOREIGN KEY (recurring_order_id) REFERENCES public.recurring_orders(id) ON DELETE CASCADE;

-- FK CONSTRAINT: recurring_order_executions recurring_order_executions_trading_order_id_fkey
ALTER TABLE ONLY public.recurring_order_executions
    ADD CONSTRAINT recurring_order_executions_trading_order_id_fkey FOREIGN KEY (trading_order_id) REFERENCES public.trading_orders(id);

-- FK CONSTRAINT: settlements settlements_epoch_id_fkey
ALTER TABLE ONLY public.settlements
    ADD CONSTRAINT settlements_epoch_id_fkey FOREIGN KEY (epoch_id) REFERENCES public.market_epochs(id) ON DELETE CASCADE;

-- FK CONSTRAINT: swap_transactions swap_transactions_pool_id_fkey
ALTER TABLE ONLY public.swap_transactions
    ADD CONSTRAINT swap_transactions_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES public.liquidity_pools(id);

-- FK CONSTRAINT: trading_orders trading_orders_epoch_id_fkey
ALTER TABLE ONLY public.trading_orders
    ADD CONSTRAINT trading_orders_epoch_id_fkey FOREIGN KEY (epoch_id) REFERENCES public.market_epochs(id) ON DELETE SET NULL;

-- FK CONSTRAINT: vpp_dispatch_history vpp_dispatch_history_cluster_id_fkey
ALTER TABLE ONLY public.vpp_dispatch_history
    ADD CONSTRAINT vpp_dispatch_history_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES public.vpp_clusters(cluster_id);


-- ======================================================================
-- TRIGGERS (12)
-- ======================================================================

-- TRIGGER: p2p_config p2p_config_audit_trigger
CREATE TRIGGER p2p_config_audit_trigger AFTER UPDATE ON public.p2p_config FOR EACH ROW EXECUTE FUNCTION public.log_p2p_config_change();

-- TRIGGER: escrow_records update_escrow_records_updated_at
CREATE TRIGGER update_escrow_records_updated_at BEFORE UPDATE ON public.escrow_records FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: futures_orders update_futures_orders_updated_at
CREATE TRIGGER update_futures_orders_updated_at BEFORE UPDATE ON public.futures_orders FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: futures_positions update_futures_positions_updated_at
CREATE TRIGGER update_futures_positions_updated_at BEFORE UPDATE ON public.futures_positions FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: futures_products update_futures_products_updated_at
CREATE TRIGGER update_futures_products_updated_at BEFORE UPDATE ON public.futures_products FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: market_epochs update_market_epochs_updated_at
CREATE TRIGGER update_market_epochs_updated_at BEFORE UPDATE ON public.market_epochs FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: order_matches update_order_matches_updated_at
CREATE TRIGGER update_order_matches_updated_at BEFORE UPDATE ON public.order_matches FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: price_alerts update_price_alerts_updated_at
CREATE TRIGGER update_price_alerts_updated_at BEFORE UPDATE ON public.price_alerts FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: settlements update_settlements_blockchain_status
CREATE TRIGGER update_settlements_blockchain_status BEFORE UPDATE ON public.settlements FOR EACH ROW EXECUTE FUNCTION public.update_blockchain_status_on_hash();

-- TRIGGER: settlements update_settlements_updated_at
CREATE TRIGGER update_settlements_updated_at BEFORE UPDATE ON public.settlements FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

-- TRIGGER: trading_orders update_trading_orders_blockchain_status
CREATE TRIGGER update_trading_orders_blockchain_status BEFORE UPDATE ON public.trading_orders FOR EACH ROW EXECUTE FUNCTION public.update_blockchain_status_on_hash();

-- TRIGGER: trading_orders update_trading_orders_updated_at
CREATE TRIGGER update_trading_orders_updated_at BEFORE UPDATE ON public.trading_orders FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();


-- ======================================================================
-- COMMENTS (51)
-- ======================================================================

-- COMMENT: COLUMN settlements.blockchain_status
COMMENT ON COLUMN public.settlements.blockchain_status IS 'On-chain processing status: unprocessed, pending, success, failed_retry, failed_fatal';

-- COMMENT: COLUMN settlements.wheeling_charge
COMMENT ON COLUMN public.settlements.wheeling_charge IS 'Zone-based transmission fee (THB)';

-- COMMENT: COLUMN settlements.loss_factor
COMMENT ON COLUMN public.settlements.loss_factor IS 'Technical loss percentage (decimal)';

-- COMMENT: COLUMN settlements.loss_cost
COMMENT ON COLUMN public.settlements.loss_cost IS 'Monetized energy loss (THB)';

-- COMMENT: COLUMN settlements.effective_energy
COMMENT ON COLUMN public.settlements.effective_energy IS 'Energy received by buyer after losses (kWh)';

-- COMMENT: COLUMN settlements.seller_session_token
COMMENT ON COLUMN public.settlements.seller_session_token IS 'Session token used for password-less signing of energy transfers';

-- COMMENT: COLUMN settlements.buyer_session_token
COMMENT ON COLUMN public.settlements.buyer_session_token IS 'Session token used for password-less signing of payments (if applicable)';

-- COMMENT: COLUMN settlements.erc_certificate_id
COMMENT ON COLUMN public.settlements.erc_certificate_id IS 'ID of the ERC certificate transferred in this settlement';

-- COMMENT: COLUMN settlements.erc_transfer_tx
COMMENT ON COLUMN public.settlements.erc_transfer_tx IS 'Solana transaction signature for the ERC transfer';

-- COMMENT: COLUMN settlements.error_message
COMMENT ON COLUMN public.settlements.error_message IS 'Detailed error message from failed settlement attempts';

-- COMMENT: COLUMN settlements.next_retry_at
COMMENT ON COLUMN public.settlements.next_retry_at IS 'Timestamp for the next scheduled blockchain retry';

-- COMMENT: COLUMN settlements.buy_signature
COMMENT ON COLUMN public.settlements.buy_signature IS 'On-chain signature for the buy order';

-- COMMENT: COLUMN settlements.sell_signature
COMMENT ON COLUMN public.settlements.sell_signature IS 'On-chain signature for the sell order';

-- COMMENT: COLUMN settlements.buy_payload
COMMENT ON COLUMN public.settlements.buy_payload IS 'Serialized payload bytes for the buy order';

-- COMMENT: COLUMN settlements.sell_payload
COMMENT ON COLUMN public.settlements.sell_payload IS 'Serialized payload bytes for the sell order';

-- COMMENT: COLUMN settlements.settlement_type
COMMENT ON COLUMN public.settlements.settlement_type IS 'Distinguishes between trade-based and physical-correction settlements';

-- COMMENT: COLUMN settlements.imbalance_gap_kwh
COMMENT ON COLUMN public.settlements.imbalance_gap_kwh IS 'The physical energy difference being settled (Metered - Comitted)';

-- COMMENT: COLUMN trading_orders.blockchain_status
COMMENT ON COLUMN public.trading_orders.blockchain_status IS 'On-chain processing status: unprocessed, pending, success, failed_retry, failed_fatal';

-- COMMENT: COLUMN trading_orders.meter_id
COMMENT ON COLUMN public.trading_orders.meter_id IS 'The specific meter that generated the energy (for sell orders)';

-- COMMENT: COLUMN trading_orders.trigger_price
COMMENT ON COLUMN public.trading_orders.trigger_price IS 'Price that triggers the conditional order';

-- COMMENT: COLUMN trading_orders.trigger_type
COMMENT ON COLUMN public.trading_orders.trigger_type IS 'Type of conditional order: stop_loss, take_profit, trailing_stop';

-- COMMENT: COLUMN trading_orders.trigger_status
COMMENT ON COLUMN public.trading_orders.trigger_status IS 'Status of the trigger: pending, triggered, cancelled, expired';

-- COMMENT: COLUMN trading_orders.triggered_at
COMMENT ON COLUMN public.trading_orders.triggered_at IS 'Timestamp when the order was triggered';

-- COMMENT: COLUMN trading_orders.trailing_offset
COMMENT ON COLUMN public.trading_orders.trailing_offset IS 'Price offset for trailing stop orders';

-- COMMENT: COLUMN trading_orders.session_token
COMMENT ON COLUMN public.trading_orders.session_token IS 'Session token used for password-less signing when order is triggered';

-- COMMENT: COLUMN trading_orders.last_peak_price
COMMENT ON COLUMN public.trading_orders.last_peak_price IS 'The highest (for sell) or lowest (for buy) price observed since a trailing stop order was activated';

-- COMMENT: COLUMN trading_orders.next_retry_at
COMMENT ON COLUMN public.trading_orders.next_retry_at IS 'Timestamp for the next scheduled blockchain retry';

-- COMMENT: COLUMN trading_orders.time_in_force
COMMENT ON COLUMN public.trading_orders.time_in_force IS 'Order time-in-force policy: gtc (Good-Til-Cancelled), fok (Fill-or-Kill), ioc (Immediate-or-Cancel)';

-- COMMENT: COLUMN trading_orders.limit_price
COMMENT ON COLUMN public.trading_orders.limit_price IS 'Optional explicit limit price for the order (conditional/advanced order types); base price is price_per_kwh';

-- COMMENT: COLUMN trading_orders.market_segment
COMMENT ON COLUMN public.trading_orders.market_segment IS 'Matching mechanism: realtime (continuous CDA matcher) or interval (15-min uniform-price clearing)';

-- COMMENT: TABLE carbon_credits
COMMENT ON TABLE public.carbon_credits IS 'Carbon credits earned from renewable energy production';

-- COMMENT: COLUMN carbon_credits.amount
COMMENT ON COLUMN public.carbon_credits.amount IS 'Amount in metric tons of CO2 equivalent';

-- COMMENT: COLUMN carbon_credits.source
COMMENT ON COLUMN public.carbon_credits.source IS 'Source of credits: solar, wind, meter_production, purchase';

-- COMMENT: TABLE carbon_transactions
COMMENT ON TABLE public.carbon_transactions IS 'Carbon credit transfers between users';

-- COMMENT: TABLE market_epochs_archive
COMMENT ON TABLE public.market_epochs_archive IS 'Archive for settled market epochs older than 90 days';

-- COMMENT: TABLE outbox_events
COMMENT ON TABLE public.outbox_events IS 'Transactional outbox for trading-service domain events (OutboxWorker drains it).';

-- COMMENT: TABLE p2p_config
COMMENT ON TABLE public.p2p_config IS 'Dynamic P2P market configuration - editable by admin without contract redeployment';

-- COMMENT: TABLE p2p_config_audit
COMMENT ON TABLE public.p2p_config_audit IS 'Audit trail for all P2P config changes';

-- COMMENT: TABLE price_alerts
COMMENT ON TABLE public.price_alerts IS 'User-defined price alerts that trigger notifications';

-- COMMENT: COLUMN price_alerts.condition
COMMENT ON COLUMN public.price_alerts.condition IS 'Trigger when price goes above, below, or crosses the target';

-- COMMENT: COLUMN price_alerts.repeat
COMMENT ON COLUMN public.price_alerts.repeat IS 'If true, re-arm the alert after triggering';

-- COMMENT: COLUMN price_alerts.updated_at
COMMENT ON COLUMN public.price_alerts.updated_at IS 'Timestamp of the last modification to the alert';

-- COMMENT: COLUMN recurring_order_executions.blockchain_status
COMMENT ON COLUMN public.recurring_order_executions.blockchain_status IS 'On-chain processing status: unprocessed, pending, success, failed_retry, failed_fatal';

-- COMMENT: COLUMN recurring_order_executions.next_retry_at
COMMENT ON COLUMN public.recurring_order_executions.next_retry_at IS 'Timestamp for the next scheduled blockchain retry';

-- COMMENT: TABLE recurring_orders
COMMENT ON TABLE public.recurring_orders IS 'DCA-style recurring orders that execute at scheduled intervals';

-- COMMENT: COLUMN recurring_orders.interval_type
COMMENT ON COLUMN public.recurring_orders.interval_type IS 'How often to execute: hourly, daily, weekly, monthly';

-- COMMENT: COLUMN recurring_orders.interval_value
COMMENT ON COLUMN public.recurring_orders.interval_value IS 'Execute every N intervals (e.g., every 2 days)';

-- COMMENT: COLUMN recurring_orders.max_executions
COMMENT ON COLUMN public.recurring_orders.max_executions IS 'Maximum number of executions (NULL = unlimited)';

-- COMMENT: COLUMN recurring_orders.session_token
COMMENT ON COLUMN public.recurring_orders.session_token IS 'Session token used for password-less signing of recurring executions';

-- COMMENT: TABLE settlements_archive
COMMENT ON TABLE public.settlements_archive IS 'Archive for completed settlements older than 90 days';

-- COMMENT: TABLE trading_orders_archive
COMMENT ON TABLE public.trading_orders_archive IS 'Archive for settled trading orders older than 90 days';
