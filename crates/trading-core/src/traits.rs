//! Trait definitions for dependency injection.
//!
//! These traits define the boundaries between crates. Domain logic
//! depends only on these traits — concrete implementations live
//! in `trading-persistence` and `trading-infra`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::error::ApiError;
use crate::events::Event;
use crate::models::{
    ConditionalOrder, MarketEpoch, MarketPrice, OrderBookEntry, OrderMatch, RecurringOrder,
    Settlement, SettlementStats, TradingOrder,
};
use crate::types::OrderStatus;

/// Result alias for trait methods.
pub type TraitResult<T> = std::result::Result<T, ApiError>;

/// One side (buyer or seller) of a matched trade, for the atomic
/// [`SettlementRepository::persist_matched_trade`] path. The fill is applied
/// *incrementally* (`filled_amount += delta`), so an order that crosses several
/// times in one cycle accumulates correctly across the per-trade transactions.
#[derive(Debug, Clone)]
pub struct TradeFill {
    /// The order being filled.
    pub order_id: Uuid,
    /// Owner of the order, stamped onto the emitted `OrderUpdate` event. `None`
    /// only when the caller could not resolve it.
    pub user_id: Option<Uuid>,
    /// Amount to add to `filled_amount` for this trade.
    pub delta: Decimal,
    /// Zone tag carried onto the `OrderUpdate` event.
    pub zone_id: Option<i32>,
}

// ── Repository Traits ────────────────────────────────────────────────────────

/// Order persistence operations.
#[async_trait]
pub trait OrderRepository: Send + Sync {
    /// Insert a new order into the database.
    async fn insert_order(&self, order: &TradingOrder) -> TraitResult<()>;

    /// Atomically insert an order and its domain event into the outbox in a
    /// single transaction. Use this on the order-placement path so the event
    /// can never be lost relative to the state change (the separate
    /// `insert_order` + `EventPublisher::publish` calls are two transactions
    /// and are NOT atomic — a crash between them drops the event).
    async fn insert_order_with_event(&self, order: &TradingOrder, event: &Event)
        -> TraitResult<()>;

    /// Store the wallet Ed25519 signature that authorises this order for
    /// per-user-escrow settlement (config `per_user_escrow_settlement`).
    ///
    /// Written to the existing `trading_orders.signature` column. The settlement
    /// builder reads it back and re-derives the signed payload from the order row,
    /// so the raw bytes need not be stored alongside it.
    ///
    /// Defaults to a no-op so the in-memory/mock repositories used in unit tests
    /// need not implement it — only the Postgres repository persists anything.
    /// A new *production* repository MUST override this, or signed orders will
    /// silently fail to settle.
    async fn set_wallet_signature(&self, _order_id: Uuid, _signature: &str) -> TraitResult<()> {
        Ok(())
    }

    /// Return the id of the current active market epoch, creating one if none
    /// is open. Orders must be stamped with this on placement: `order_matches`
    /// and `settlements` both have a NOT NULL FK to `market_epochs(id)`, so a
    /// NULL/nil epoch makes the matcher's ledger inserts fail the FK. Must be
    /// race-safe against concurrent first-orders (the epoch number is UNIQUE).
    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid>;

    /// Get an order by ID.
    async fn get_order(&self, id: Uuid) -> TraitResult<Option<TradingOrder>>;

    /// Get orders by user ID with pagination.
    async fn get_orders_by_user(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> TraitResult<Vec<TradingOrder>>;

    /// Get active orders for a specific zone.
    async fn get_active_orders_by_zone(&self, zone_id: i32) -> TraitResult<Vec<OrderBookEntry>>;

    /// All active orders across every zone (P2P aggregate order book).
    async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>>;

    /// Update order status.
    async fn update_order_status(&self, id: Uuid, status: OrderStatus) -> TraitResult<()>;

    /// Persist the on-chain order PDA + index after custodial placement.
    async fn update_order_pda(
        &self,
        id: Uuid,
        order_pda: &str,
        order_index: i64,
    ) -> TraitResult<()>;

    /// Update filled amount on an order.
    async fn update_filled_amount(
        &self,
        id: Uuid,
        filled_amount: Decimal,
        status: OrderStatus,
    ) -> TraitResult<()>;

    /// Atomically update an order's fill state and write its domain event to
    /// the outbox in a single transaction, so the event can never be lost
    /// relative to the state change (see `insert_order_with_event`).
    async fn update_filled_amount_with_event(
        &self,
        id: Uuid,
        filled_amount: Decimal,
        status: OrderStatus,
        event: &Event,
    ) -> TraitResult<()>;

    /// Get all active buy orders.
    async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>>;

    /// Get all active sell orders.
    async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>>;

    /// Reap expired orders: mark every still-open order (`pending` / `active` /
    /// `partially_filled`) whose `expires_at` has passed as `Expired`, emitting
    /// an `OrderUpdate` event per order into the outbox in the same transaction.
    /// Returns the ids reaped. Idempotent — a second call finds nothing, the
    /// status no longer matches. Default is a no-op so non-persistent mocks need
    /// not implement it; the Postgres repository overrides it.
    async fn expire_stale_orders(&self, _now: DateTime<Utc>) -> TraitResult<Vec<Uuid>> {
        Ok(Vec::new())
    }

    /// Ids of epochs whose 15-minute window has elapsed but are still `active` —
    /// i.e. due for uniform-price clearing. Default is empty so non-persistent
    /// mocks need not implement it; the Postgres repository overrides it.
    async fn get_epochs_due_for_clearing(&self) -> TraitResult<Vec<Uuid>> {
        Ok(Vec::new())
    }

    /// Close an epoch after clearing: set status `cleared` and stamp the summary
    /// (single-zone clearing price, total volume, matched-order count). Guarded so
    /// it only transitions an `active` epoch — idempotent under re-runs. Default is
    /// a no-op; the Postgres repository overrides it.
    async fn mark_epoch_cleared(
        &self,
        _epoch_id: Uuid,
        _clearing_price: Option<Decimal>,
        _total_volume: Decimal,
        _matched_orders: i64,
    ) -> TraitResult<()> {
        Ok(())
    }

    /// Most-recently-cleared epochs (newest first), capped at `limit` — backs the
    /// clearing-results read endpoint. Default empty; Postgres repo overrides.
    async fn list_recent_cleared_epochs(&self, _limit: i64) -> TraitResult<Vec<MarketEpoch>> {
        Ok(Vec::new())
    }

    /// Cancel an order.
    async fn cancel_order(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool>;

    /// Bootstrap orders from database for rehydration.
    async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>>;
}

/// Settlement persistence operations.
#[async_trait]
pub trait SettlementRepository: Send + Sync {
    /// Insert a new settlement.
    async fn insert_settlement(&self, settlement: &Settlement) -> TraitResult<()>;

    /// Atomically insert a settlement and write its domain event to the outbox
    /// in a single transaction, so the event can never be lost relative to the
    /// state change.
    async fn insert_settlement_with_event(
        &self,
        settlement: &Settlement,
        event: &Event,
    ) -> TraitResult<()>;

    /// Resolve the current market epoch (creating one if none is open) so a
    /// settlement built outside the matching engine — e.g. generation-mint or
    /// the settlement engine — can reference a real `market_epochs` row instead
    /// of a nil/hardcoded UUID that would FK-fail.
    async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid>;

    /// Of `ids`, the settlements whose buy or sell order has already lapsed.
    ///
    /// Such a settlement can never land: every on-chain pairing/settlement path
    /// rejects a lapsed order (`OrderExpired`), so attempting it burns the whole
    /// retry budget and then parks the row with a misleading generic reason.
    /// Callers use this to fail it fast, with the real cause, without a doomed
    /// on-chain round trip.
    ///
    /// A NULL order expiry means "no expiry" — the same meaning as the on-chain
    /// `0` sentinel — and never counts as lapsed.
    ///
    /// Defaulted to "none known" so the pre-flight is a pure optimisation: an
    /// implementation that does not override it (test fakes) leaves the caller
    /// behaving exactly as it did before, attempting every claimed settlement.
    async fn settlements_with_lapsed_orders(&self, _ids: &[Uuid]) -> TraitResult<Vec<Uuid>> {
        Ok(Vec::new())
    }

    /// Which of `ids` reference an order that was never placed on chain.
    ///
    /// A settlement can only settle if BOTH legs have an `order_pda`: the gateway
    /// resolves them before building the batch and silently drops any settlement
    /// missing one (`"has no on-chain PDA; skipping"`). The caller then sees only
    /// an absence from `tx_results` and records the symptom — *"not included in
    /// on-chain batch result"* — while the real cause was known one layer down and
    /// thrown away.
    ///
    /// **This is permanent, not transient.** Nothing retries placement: no worker
    /// looks for `order_pda IS NULL`, so an order that failed to place never gets a
    /// PDA. Retrying such a settlement is guaranteed waste, and the five attempts
    /// it burns are what turn a placement fault into a settlement mystery.
    ///
    /// Defaulted to empty for the same reason as the lapsed-order pre-flight: a
    /// fake that does not override it leaves the caller behaving exactly as before.
    async fn settlements_missing_order_pda(&self, _ids: &[Uuid]) -> TraitResult<Vec<Uuid>> {
        Ok(Vec::new())
    }

    /// Summed fee / wheeling / loss over every `completed` settlement.
    ///
    /// The ledger side of the collector cross-check. Deliberately the FULL history,
    /// not a window: it is compared against a collector balance, which is also
    /// cumulative, and the reconciler works in deltas from a baseline so the
    /// absolute values never need to agree.
    ///
    /// Defaulted to zero so a fake that does not override it simply disables the
    /// cross-check rather than reporting a phantom drift.
    async fn completed_charge_totals(&self) -> TraitResult<crate::charges::ChargeTotals> {
        Ok(crate::charges::ChargeTotals::default())
    }

    /// The most recently completed settlements, newest first, for the charge
    /// reconciler (`charges::reconcile_charges`).
    ///
    /// Only `Completed` rows are worth auditing: a pending or failed settlement
    /// moved no money, so its charge columns describe an intention rather than an
    /// outcome. Bounded by `limit` because this runs on a timer and must never turn
    /// into a full-table scan as the ledger grows.
    ///
    /// Defaulted to empty for the same reason as the pre-flight above: a fake that
    /// does not override it makes the reconciler a no-op rather than a failure.
    async fn recent_completed_settlements(&self, _limit: i64) -> TraitResult<Vec<Settlement>> {
        Ok(Vec::new())
    }

    /// Insert an order-match ledger row (the `order_matches` table). Records the
    /// crossing of a buy/sell order; `settlement_id` links it to the settlement
    /// created for the same fill, `zone_id` tags it for sharded analytics.
    async fn insert_match(
        &self,
        m: &OrderMatch,
        settlement_id: Option<Uuid>,
        zone_id: Option<i32>,
    ) -> TraitResult<()>;

    /// Atomically insert an order-match ledger row and write its domain event
    /// to the outbox in a single transaction, so the event can never be lost
    /// relative to the state change.
    async fn insert_match_with_event(
        &self,
        m: &OrderMatch,
        settlement_id: Option<Uuid>,
        zone_id: Option<i32>,
        event: &Event,
    ) -> TraitResult<()>;

    /// Persist one matched trade **atomically**: the settlement row, its
    /// `order_matches` ledger row, both orders' incremental fills, and every
    /// associated outbox event, all in a single database transaction.
    ///
    /// This replaces the old split of "insert all settlements, then apply all
    /// fills" — which had no transaction spanning the two phases, so a failure
    /// between them left settlements committed while the orders stayed `active`,
    /// and the next matching cycle re-matched and **re-settled the same energy**
    /// (each settlement carries a fresh `trade_id`, so the on-chain nullifier
    /// never deduplicated them → double payment).
    ///
    /// Returns `Ok(true)` when the trade fully committed. Returns `Ok(false)`
    /// when either order was no longer fillable (already `expired`/`cancelled`/
    /// `filled` — the guarded fill matched 0 rows): the whole trade, including
    /// its settlement, is rolled back so nothing is orphaned, and the caller
    /// leaves the orders for the next cycle. Both `OrderUpdate` events are built
    /// from the post-update `filled_amount`/`status` returned by the fill.
    async fn persist_matched_trade(
        &self,
        settlement: &Settlement,
        order_match: &OrderMatch,
        matched_event: &Event,
        match_zone_id: Option<i32>,
        buyer: &TradeFill,
        seller: &TradeFill,
    ) -> TraitResult<bool>;

    /// Get a settlement by ID.
    async fn get_settlement(&self, id: Uuid) -> TraitResult<Option<Settlement>>;

    /// Get pending settlements for batch processing.
    async fn get_pending_settlements(&self, limit: i64) -> TraitResult<Vec<Settlement>>;

    /// Atomically claim settlements for processing: flip `pending` → `processing`
    /// for the given ids in a single statement and return only the rows actually
    /// claimed. Concurrent callers (settlement worker vs. RPC) each receive a
    /// disjoint subset, so a settlement can be minted at most once.
    async fn claim_settlements_for_processing(&self, ids: &[Uuid]) -> TraitResult<Vec<Settlement>>;

    /// Release claimed settlements back to a retryable state after a failed (or
    /// skipped) mint: increment `retry_count` and set status to `pending`, or to
    /// `permanently_failed` once `retry_count` reaches `max_retries`. Returns the
    /// number of rows moved to `permanently_failed`. Only touches rows currently
    /// `processing`, so it cannot disturb a row another worker has finalized.
    async fn reset_settlements_for_retry(
        &self,
        ids: &[Uuid],
        max_retries: i32,
        error: Option<&str>,
    ) -> TraitResult<u64>;

    /// Reclaim settlements stuck in `processing` longer than `stale_after_secs`
    /// (crash / partial failure between claim and finalize): move them back to a
    /// retryable state via the same retry accounting as
    /// [`reset_settlements_for_retry`]. Returns the number of rows reclaimed.
    async fn reclaim_stale_processing(
        &self,
        stale_after_secs: i64,
        max_retries: i32,
    ) -> TraitResult<u64>;

    /// List settlements where the user is buyer or seller (trade history),
    /// newest first. Returns the page plus the total matching count for
    /// pagination.
    async fn list_settlements_for_user(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> TraitResult<(Vec<Settlement>, i64)>;

    /// Aggregate settlement counts by status + total settled value.
    async fn get_settlement_stats(&self) -> TraitResult<SettlementStats>;

    /// Real market price over a trailing `window_hours` window: VWAP, last,
    /// high/low, volume and trade count from `completed` settlements. Returns an
    /// all-zero `MarketPrice` (`trade_count` = 0) when the window holds no trades.
    /// `window_hours <= 0` means all-time (no time bound).
    async fn get_market_price(&self, window_hours: i64) -> TraitResult<MarketPrice>;

    /// Count distinct users (buyer ∪ seller) with at least one `completed`
    /// settlement in the last `window_hours`. Real replacement for the mock
    /// `active_users` stat.
    async fn count_active_traders(&self, window_hours: i64) -> TraitResult<i64>;

    /// Update settlement status.
    async fn update_settlement_status(
        &self,
        id: Uuid,
        status: &str,
        tx_hash: Option<&str>,
        error: Option<&str>,
    ) -> TraitResult<()>;

    /// Atomically update a settlement's status and write its domain event to
    /// the outbox in a single transaction, so the event can never be lost
    /// relative to the state change.
    async fn update_settlement_status_with_event(
        &self,
        id: Uuid,
        status: &str,
        tx_hash: Option<&str>,
        error: Option<&str>,
        event: &Event,
    ) -> TraitResult<()>;
}

/// Conditional order persistence.
#[async_trait]
pub trait ConditionalOrderRepository: Send + Sync {
    async fn get_pending_conditional_orders(&self) -> TraitResult<Vec<ConditionalOrder>>;
    async fn update_trigger_status(
        &self,
        id: Uuid,
        status: &str,
        triggered_at: Option<DateTime<Utc>>,
    ) -> TraitResult<()>;
    async fn update_peak_price(&self, id: Uuid, peak_price: Decimal) -> TraitResult<()>;
}

/// Recurring order persistence.
#[async_trait]
pub trait RecurringOrderRepository: Send + Sync {
    async fn get_due_recurring_orders(
        &self,
        now: DateTime<Utc>,
    ) -> TraitResult<Vec<RecurringOrder>>;
    async fn update_after_execution(
        &self,
        id: Uuid,
        next_execution: DateTime<Utc>,
        total_executions: i32,
    ) -> TraitResult<()>;

    /// Insert a new recurring order (status defaults to `active`) and return the
    /// stored row. `next_execution_at` is computed by the caller.
    async fn create_recurring_order(
        &self,
        input: crate::models::NewRecurringOrder,
    ) -> TraitResult<RecurringOrder>;

    /// List a user's recurring orders, newest first.
    async fn list_recurring_orders_for_user(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<RecurringOrder>>;

    /// Fetch one recurring order scoped to its owner. `None` if missing/foreign.
    async fn get_recurring_order(
        &self,
        id: Uuid,
        user_id: Uuid,
    ) -> TraitResult<Option<RecurringOrder>>;

    /// Delete a recurring order scoped to its owner. Returns `true` if a row was
    /// removed (foreign/missing id yields `false`, not an error).
    async fn delete_recurring_order(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool>;

    /// Set the status of a recurring order scoped to its owner (pause/resume).
    /// Returns `true` if a row was updated.
    async fn set_recurring_status(
        &self,
        id: Uuid,
        user_id: Uuid,
        status: crate::types::RecurringStatus,
    ) -> TraitResult<bool>;
}

/// Price alert persistence (table `price_alerts`).
#[async_trait]
pub trait PriceAlertRepository: Send + Sync {
    /// Insert a new alert (status defaults to `active`) and return the stored row.
    async fn create_price_alert(
        &self,
        input: crate::models::NewPriceAlert,
    ) -> TraitResult<crate::models::PriceAlert>;

    /// List a user's alerts, newest first.
    async fn list_price_alerts_for_user(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<crate::models::PriceAlert>>;

    /// Delete an alert scoped to its owner. Returns `true` if a row was removed
    /// (so a foreign/missing id yields `false`, not an error).
    async fn delete_price_alert(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool>;

    /// All `active` alerts across every user (cross-user; the trigger evaluator
    /// scans these each cycle against the current market price).
    async fn get_active_alerts(&self) -> TraitResult<Vec<crate::models::PriceAlert>>;

    /// Record that an alert fired at `triggered_price`. Sets `triggered_at`/
    /// `triggered_price`; a one-shot alert (`repeat = false`) moves to
    /// `triggered`, a repeating one stays `active` so it can fire again.
    async fn mark_triggered(&self, id: Uuid, triggered_price: Decimal) -> TraitResult<()>;

    /// Atomically record an alert firing and write its domain event to the
    /// outbox in a single transaction, so the event can never be lost relative
    /// to the state change.
    async fn mark_triggered_with_event(
        &self,
        id: Uuid,
        triggered_price: Decimal,
        event: &Event,
    ) -> TraitResult<()>;
}

/// Futures persistence operations.
#[async_trait]
pub trait FuturesRepository: Send + Sync {
    async fn get_products(&self) -> TraitResult<Vec<crate::models::FuturesProduct>>;
    async fn get_product(&self, id: Uuid) -> TraitResult<Option<crate::models::FuturesProduct>>;
    async fn insert_order(&self, order: &crate::models::FuturesOrder) -> TraitResult<()>;
    async fn get_orders_by_user(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<crate::models::FuturesOrder>>;
    async fn get_positions_by_user(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<crate::models::FuturesPosition>>;
    async fn close_position(&self, position_id: Uuid) -> TraitResult<()>;
}

/// Carbon credits persistence operations.
#[async_trait]
pub trait CarbonRepository: Send + Sync {
    async fn get_balance(&self, user_id: Uuid) -> TraitResult<Decimal>;
    async fn get_history(&self, user_id: Uuid) -> TraitResult<Vec<crate::models::CarbonCredit>>;
    async fn get_transactions(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<crate::models::CarbonTransaction>>;
    async fn insert_transaction(&self, tx: &crate::models::CarbonTransaction) -> TraitResult<()>;
}

/// Analytics and user data operations.
#[async_trait]
pub trait AnalyticsRepository: Send + Sync {
    async fn get_user_stats(&self, user_id: Uuid) -> TraitResult<crate::models::UserAnalytics>;
    async fn get_user_transactions(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<crate::models::TransactionData>>;
}

/// Wallet and balance operations.
#[async_trait]
pub trait BalanceRepository: Send + Sync {
    async fn get_wallet_balance(&self, address: &str) -> TraitResult<Decimal>;
}

// ── Infrastructure Traits ────────────────────────────────────────────────────

/// Event publishing (Kafka / Redis Streams).
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish a domain event.
    async fn publish(&self, event: Event) -> TraitResult<()>;

    /// Publish a domain event to a specific topic.
    async fn publish_to_topic(&self, topic: &str, event: Event) -> TraitResult<()>;

    /// Subscribe to events and process them with a handler.
    async fn create_consumer_group(&self, group_name: &str) -> TraitResult<()>;

    /// Consume events from a stream/topic.
    async fn consume_events(
        &self,
        group_name: &str,
        consumer_name: &str,
        handler: Arc<
            dyn Fn(
                    Event,
                )
                    -> std::pin::Pin<Box<dyn std::future::Future<Output = TraitResult<()>> + Send>>
                + Send
                + Sync,
        >,
    ) -> TraitResult<()>;

    // Note: for real consumption, we usually start a long-running loop.
    // The trait might be better as an 'EventBus' trait.
}

/// Transactional Outbox for reliable event delivery.
#[async_trait]
pub trait OutboxRepository: Send + Sync {
    /// Insert an event into the outbox.
    async fn insert_event(&self, event: &Event) -> TraitResult<()>;

    /// Fetch pending events to be processed.
    async fn get_pending_events(&self, limit: i32) -> TraitResult<Vec<crate::models::OutboxEvent>>;

    /// Mark an event as successfully processed.
    async fn mark_processed(&self, id: Uuid) -> TraitResult<()>;

    /// Mark an event as failed and record the error.
    async fn mark_failed(&self, id: Uuid, error: &str) -> TraitResult<()>;

    /// Delete processed events older than a certain date.
    async fn cleanup_processed(&self, before: DateTime<Utc>) -> TraitResult<u64>;
}

/// Blockchain gateway for Solana operations.
#[async_trait]
pub trait BlockchainGateway: Send + Sync {
    /// Check if a user is registered on-chain.
    async fn is_user_registered(&self, user_id: Uuid) -> TraitResult<bool>;

    /// Get user's on-chain wallet address.
    async fn get_user_wallet(&self, user_id: Uuid) -> TraitResult<Option<String>>;

    /// Get on-chain **energy** (GRID/GRX, 9-dec) balance for a wallet.
    async fn get_token_balance(&self, wallet_address: &str) -> TraitResult<u64>;

    /// Get on-chain **currency** (THBC, 6-dec) balance for a wallet — the baht side
    /// of a trade.
    ///
    /// A trade has two legs and the API long exposed only the energy one, so a
    /// wallet could show the kWh it held but not the money it was paid or spent.
    ///
    /// Note the two mints differ in more than decimals: energy is Token-2022 while
    /// the currency mint is classic SPL Token, so their ATAs derive under different
    /// token programs and must never be assumed interchangeable.
    ///
    /// Defaults to 0 so mock/non-chain gateways need no override.
    async fn get_currency_balance(&self, _wallet_address: &str) -> TraitResult<u64> {
        Ok(0)
    }

    /// Get native SOL balance (in SOL, not lamports) for a wallet address.
    /// Default returns 0.0 so mock/non-chain gateways need no override.
    async fn get_sol_balance(&self, _wallet_address: &str) -> TraitResult<f64> {
        Ok(0.0)
    }

    /// Get on-chain zone configuration (multiplier, etc).
    async fn get_zone_config(&self, zone_id: i32) -> TraitResult<crate::models::ZoneConfig>;

    /// Execute multiple settlements in a single batched transaction.
    async fn execute_batched_settlements(
        &self,
        settlements: Vec<crate::models::Settlement>,
    ) -> TraitResult<Vec<crate::models::SettlementTransaction>>;

    /// Custodial order placement (Option A): record the order PDA + fund its escrow
    /// on the user's behalf, platform-signed. Returns (signature, `order_pda`). Default
    /// is unsupported so mock/non-chain gateways need no override.
    ///
    /// `expires_at_unix` is the expiry already resolved for this order by
    /// [`crate::order_policy::resolve_expires_at`], in absolute unix seconds
    /// (`0` = no expiry). It is passed on to the Order PDA rather than left to the
    /// program, which used to stamp its own 24h lifetime — so the row and the PDA
    /// state the same expiry. A past value is rejected on chain (`OrderExpired`).
    #[allow(clippy::too_many_arguments)]
    async fn place_order_on_chain(
        &self,
        _user_id: Uuid,
        _is_buy: bool,
        _energy_amount: Decimal,
        _price_per_kwh: Decimal,
        _zone_id: i32,
        _order_id_seed: u64,
        _expires_at_unix: i64,
    ) -> TraitResult<(String, String)> {
        Err(ApiError::Internal(
            "place_order_on_chain not supported by this gateway".to_string(),
        ))
    }

    /// Issue an ERC certificate on-chain.
    async fn issue_erc(
        &self,
        user_id: Uuid,
        meter_id: &str,
        energy_amount: Decimal,
    ) -> TraitResult<String>;

    /// Sync total supply from SPL Mint to `TokenInfo` PDA.
    async fn sync_total_supply(&self) -> TraitResult<String>;

    /// Execute on-chain `create_order` with custodial signing.
    ///
    /// `expires_at_unix` is the order's expiry in absolute unix seconds, `0` for
    /// none — see [`Self::place_order_on_chain`]. `create_{buy,sell}_order` stores
    /// it verbatim and rejects a past value.
    #[allow(clippy::too_many_arguments)]
    async fn execute_create_order(
        &self,
        user_id: Uuid,
        market_pubkey: &str,
        amount: u64,
        price: u64,
        order_side: &str,
        erc_id: Option<&str>,
        zone_id: u32,
        expires_at_unix: i64,
    ) -> TraitResult<(String, String, u64)>;
}

/// Cache store (Redis).
#[async_trait]
pub trait CacheStore: Send + Sync {
    async fn get(&self, key: &str) -> TraitResult<Option<String>>;
    async fn set(&self, key: &str, value: &str, ttl_secs: u64) -> TraitResult<()>;
    async fn delete(&self, key: &str) -> TraitResult<()>;
}

/// Audit logging.
#[async_trait]
pub trait AuditLog: Send + Sync {
    async fn log_action(&self, user_id: Uuid, action: &str, details: &str) -> TraitResult<()>;
}

/// Identity and Access Management gateway.
#[async_trait]
pub trait IdentityGateway: Send + Sync {
    /// Request IAM to sign a message with a user's managed wallet.
    async fn sign_message(
        &self,
        user_id: Uuid,
        wallet_address: Option<String>,
        message: Vec<u8>,
    ) -> TraitResult<Vec<u8>>;
}

// ── Read-model records (DB-per-service Phase 1) ──────────────────────────────

/// One row to upsert into the Trading-owned `iam_wallet_read_model` — a local
/// mirror of the columns of IAM `user_wallets` that Trading needs to resolve a
/// signer. Fed by IAM wallet NATS events (+ boot backfill).
#[derive(Debug, Clone)]
pub struct WalletReadModelRecord {
    pub user_id: Uuid,
    pub wallet_address: String,
    pub is_primary: bool,
    pub blockchain_registered: bool,
    pub user_account_pda: Option<String>,
    pub shard_id: Option<i16>,
}

/// One row to upsert into the Trading-owned `meter_read_model` — a local mirror
/// of the columns of metering `meters` that the VPP membership join needs. Fed
/// by meter NATS events (+ boot backfill). `rated_power_kw` / `rated_capacity_kwh`
/// have no source on the metering `meters` table and are left NULL.
#[derive(Debug, Clone)]
pub struct MeterReadModelRecord {
    pub serial_number: String,
    pub meter_id: Uuid,
    pub user_id: Uuid,
    pub zone_id: Option<i32>,
    pub status: Option<String>,
    /// Whether metering has verified possession of the device. Mirrors
    /// `meters.is_verified`; drives the sell-side gate. Deliberately NOT derived
    /// from `status` — that column carries the meter's *operating* status
    /// (`active`/`maintenance`), which is a different question.
    pub is_verified: bool,
}

/// Local read-model of IAM `user_wallets` (`iam_wallet_read_model`).
#[async_trait]
pub trait WalletReadModelRepository: Send + Sync {
    /// Upsert a wallet row (last-writer-wins on `updated_at`). When the record
    /// is marked primary, sibling wallets for the same user are demoted in the
    /// same transaction so the `(user_id) WHERE is_primary` unique index holds.
    async fn upsert_wallet(&self, rec: &WalletReadModelRecord) -> TraitResult<()>;

    /// Flip only the primary flag for an existing (user, wallet) pair — an
    /// UPDATE, not an upsert, so the other mirrored columns are never clobbered
    /// by a partial primary-changed event. Demotes siblings when promoting.
    async fn set_wallet_primary(
        &self,
        user_id: Uuid,
        wallet_address: &str,
        is_primary: bool,
    ) -> TraitResult<()>;

    /// Delete a mirrored wallet row for a (user, wallet) pair — revocation
    /// propagation from an IAM `UserWalletUnlinked` event so a deleted wallet
    /// stops resolving. Idempotent: deleting a row that is already absent is a
    /// harmless no-op, not an error.
    async fn delete_wallet(&self, user_id: Uuid, wallet_address: &str) -> TraitResult<()>;

    /// One-shot boot backfill: snapshot the current `user_wallets` into the
    /// read-model. Idempotent (`ON CONFLICT DO NOTHING`). Returns rows inserted.
    async fn backfill_wallets(&self) -> TraitResult<u64>;

    /// Lazy single-user backfill: reconcile ONE user's wallet rows from the
    /// source `user_wallets` into the read-model, then return that user's
    /// current primary `wallet_address` (or `None` if the source has no wallet
    /// for the user). This is the self-heal path for a wallet event dropped
    /// before it reached the read-model (e.g. a boot-window Kafka producer
    /// wedge): resolution can recover on demand instead of failing closed until
    /// the next restart's boot backfill. Idempotent; upserts last-writer-wins.
    async fn backfill_wallet_for(&self, user_id: Uuid) -> TraitResult<Option<String>>;
}

/// Local read-model of metering `meters` (`meter_read_model`).
#[async_trait]
pub trait MeterReadModelRepository: Send + Sync {
    /// Upsert a meter row (last-writer-wins on `updated_at`). Leaves
    /// `rated_power_kw` / `rated_capacity_kwh` untouched (no event source).
    async fn upsert_meter(&self, rec: &MeterReadModelRecord) -> TraitResult<()>;

    /// One-shot boot backfill: snapshot the current `meters` into the
    /// read-model. Idempotent (`ON CONFLICT DO NOTHING`). Returns rows inserted.
    async fn backfill_meters(&self) -> TraitResult<u64>;
}

/// Meter identity lookups.
///
/// Two id spaces exist for one meter and they are NOT interchangeable:
/// `trading_orders.meter_id` holds the metering `meters.id`, while everything
/// user-facing (the grid map's node ids, telemetry keys, grid-flow endpoints)
/// keys on `meters.serial_number`. Callers that only hold a serial must
/// translate here before touching `trading_orders`.
#[async_trait]
pub trait MeterRepository: Send + Sync {
    /// `meters.id` for a serial, or `None` when no such meter is registered.
    async fn resolve_id_by_serial(&self, serial: &str) -> TraitResult<Option<Uuid>>;

    /// `meters.id` → `serial_number` for the given ids. Ids with no meter row
    /// are absent from the map rather than erroring.
    async fn get_serials_for_ids(&self, ids: &[Uuid]) -> TraitResult<HashMap<Uuid, String>>;

    /// Identity + ownership + verification state for a meter named by serial.
    /// `None` when no such meter is mirrored here.
    async fn lookup_by_serial(&self, serial: &str) -> TraitResult<Option<MeterIdentity>>;

    /// Same as [`Self::lookup_by_serial`] for a caller that already holds the
    /// `meters.id` (the gRPC edge and any client passing `meter_id` directly).
    async fn lookup_by_id(&self, meter_id: Uuid) -> TraitResult<Option<MeterIdentity>>;

    /// Whether `user_id` owns at least one verified meter. Answers the sell-side
    /// gate for an order that names no meter — without it, omitting
    /// `meter_serial` would be a one-field bypass of the whole rule.
    async fn has_verified_meter(&self, user_id: Uuid) -> TraitResult<bool>;
}

/// A meter as the sell-side gate needs to see it: who owns it and whether
/// metering has verified possession of the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeterIdentity {
    /// `meters.id` — what `trading_orders.meter_id` stores.
    pub meter_id: Uuid,
    /// Owning user, for the ownership half of the check.
    pub user_id: Uuid,
    /// Mirror of `meters.is_verified`.
    pub is_verified: bool,
}

/// Virtual Power Plant (VPP) persistence.
#[async_trait]
pub trait VppRepository: Send + Sync {
    async fn get_cluster_by_id(
        &self,
        cluster_id: &str,
    ) -> TraitResult<Option<crate::models::VppCluster>>;
    async fn get_all_clusters(&self) -> TraitResult<Vec<crate::models::VppCluster>>;
    async fn get_member_association(
        &self,
        meter_id: &str,
    ) -> TraitResult<Option<crate::models::VppMember>>;
    async fn get_cluster_members(
        &self,
        cluster_id: &str,
    ) -> TraitResult<Vec<crate::models::VppMember>>;
    async fn update_cluster_metrics(
        &self,
        cluster_id: &str,
        stored_kwh: f64,
        soc: f64,
        flex_up: f64,
        flex_down: f64,
    ) -> TraitResult<()>;
}

/// Wake-up signal for the continuous (CDA) matcher, so a service that places
/// orders can have them matched on arrival instead of on the next fallback tick.
///
/// Deliberately sync and infallible: the only implementation stores a permit on
/// a `Notify`, which cannot fail or block, so callers on a request path pay
/// nothing. Implemented by `trading_logic::MatcherService`.
pub trait MatchTrigger: Send + Sync {
    /// Request a matching cycle. Coalescing, not queueing: several calls before
    /// the matcher wakes collapse into one cycle.
    fn request_cycle(&self);
}

/// Reads the on-chain fee/wheeling/loss schedule.
///
/// Narrower than [`BlockchainGateway`] on purpose: the only consumer is the
/// worker that keeps `charges::RefreshingChargeRates` current, and a one-method
/// trait lets that worker be unit-tested against a fake without standing up the
/// whole gateway. Implemented in the infra layer by the Chain Bridge client,
/// which reads the `Market` and `TariffConfig` accounts.
#[async_trait]
pub trait ChargeRatesSource: Send + Sync {
    /// The rates currently published on chain.
    async fn read_charge_rates(&self) -> TraitResult<crate::charges::StaticChargeRates>;
}

/// Reads the on-chain balances of the fee / wheeling / loss collector accounts.
///
/// Narrow, like [`ChargeRatesSource`], and for the same reason: the only consumer
/// is the reconciler, and everything this needs — the collector wallets, the
/// currency mint, the ATA derivation — is infra detail the pure audit must not
/// know about.
#[async_trait]
pub trait CollectorBalanceSource: Send + Sync {
    /// Current balances, in whole currency units.
    async fn collector_balances(&self) -> TraitResult<crate::charges::ChargeTotals>;

    /// The three collector token-account addresses, in fee/wheeling/loss order.
    ///
    /// Exposed so the reconciler can detect COLLISIONS. Two collectors resolving
    /// to one account means their charges are pooled; a collector colliding with
    /// the platform escrow means the charge is transferred from that account back
    /// into itself and is never collected at all. Both have happened here — all
    /// three defaulted to the dev wallet, whose ATA is `buyer_currency_escrow`,
    /// so fee, wheeling and loss were silently uncollected while the ledger
    /// recorded them correctly. No balance comparison can spot that; only
    /// comparing the addresses can.
    async fn collector_addresses(&self) -> TraitResult<[String; 3]>;
}
