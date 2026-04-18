//! Trait definitions for dependency injection.
//!
//! These traits define the boundaries between crates. Domain logic
//! depends only on these traits — concrete implementations live
//! in `trading-persistence` and `trading-infra`.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::error::ApiError;
use crate::events::Event;
use crate::models::{
    OrderBookEntry, Settlement, TradingOrder,
    ConditionalOrder, RecurringOrder,
};
use crate::types::OrderStatus;

/// Result alias for trait methods.
pub type TraitResult<T> = std::result::Result<T, ApiError>;

// ── Repository Traits ────────────────────────────────────────────────────────

/// Order persistence operations.
#[async_trait]
pub trait OrderRepository: Send + Sync {
    /// Insert a new order into the database.
    async fn insert_order(&self, order: &TradingOrder) -> TraitResult<()>;

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
    async fn get_active_orders_by_zone(
        &self,
        zone_id: i32,
    ) -> TraitResult<Vec<OrderBookEntry>>;

    /// Update order status.
    async fn update_order_status(
        &self,
        id: Uuid,
        status: OrderStatus,
    ) -> TraitResult<()>;

    /// Update filled amount on an order.
    async fn update_filled_amount(
        &self,
        id: Uuid,
        filled_amount: Decimal,
        status: OrderStatus,
    ) -> TraitResult<()>;

    /// Get all active buy orders.
    async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>>;

    /// Get all active sell orders.
    async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>>;

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

    /// Get a settlement by ID.
    async fn get_settlement(&self, id: Uuid) -> TraitResult<Option<Settlement>>;

    /// Get pending settlements for batch processing.
    async fn get_pending_settlements(
        &self,
        limit: i64,
    ) -> TraitResult<Vec<Settlement>>;

    /// Update settlement status.
    async fn update_settlement_status(
        &self,
        id: Uuid,
        status: &str,
        tx_hash: Option<&str>,
        error: Option<&str>,
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
    async fn update_peak_price(
        &self,
        id: Uuid,
        peak_price: Decimal,
    ) -> TraitResult<()>;
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
}

// ── Infrastructure Traits ────────────────────────────────────────────────────

/// Event publishing (Kafka / Redis Streams).
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish a domain event.
    async fn publish(&self, event: Event) -> TraitResult<()>;

    /// Publish a domain event to a specific topic.
    async fn publish_to_topic(&self, topic: &str, event: Event) -> TraitResult<()>;
}

/// Blockchain gateway for Solana operations.
#[async_trait]
pub trait BlockchainGateway: Send + Sync {
    /// Check if a user is registered on-chain.
    async fn is_user_registered(&self, user_id: Uuid) -> TraitResult<bool>;

    /// Get user's on-chain wallet address.
    async fn get_user_wallet(&self, user_id: Uuid) -> TraitResult<Option<String>>;

    /// Get on-chain token balance for a user.
    async fn get_token_balance(&self, wallet_address: &str) -> TraitResult<u64>;

    /// Execute a settlement on-chain.
    async fn execute_settlement(
        &self,
        settlement: &crate::models::Settlement,
    ) -> TraitResult<crate::models::SettlementTransaction>;
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
    async fn log_action(
        &self,
        user_id: Uuid,
        action: &str,
        details: &str,
    ) -> TraitResult<()>;
}

/// Virtual Power Plant (VPP) persistence.
#[async_trait]
pub trait VppRepository: Send + Sync {
    async fn get_cluster_by_id(&self, cluster_id: &str) -> TraitResult<Option<crate::models::VppCluster>>;
    async fn get_member_association(&self, meter_id: &str) -> TraitResult<Option<crate::models::VppMember>>;
    async fn update_cluster_metrics(&self, cluster_id: &str, stored_kwh: f64, soc: f64) -> TraitResult<()>;
}
