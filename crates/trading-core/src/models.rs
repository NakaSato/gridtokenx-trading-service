//! Domain model structs for the GridTokenX trading service.
//!
//! These types represent the canonical domain shapes. Database-specific
//! variants (e.g., `TradingOrderDb` with `sqlx::FromRow`) live in
//! `trading-persistence`.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;
use validator::Validate;

use crate::types::{
    AlertCondition, AlertStatus, EpochStatus, IntervalType, MarketSegment, OrderSide, OrderStatus,
    OrderType, RecurringStatus, TimeInForce, TriggerStatus, TriggerType,
};

// ── Core Order Models ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TradingOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub order_type: OrderType,
    pub side: OrderSide,
    #[schema(value_type = String)]
    pub energy_amount: Decimal,
    #[schema(value_type = String)]
    pub price_per_kwh: Decimal,
    #[schema(value_type = String)]
    pub filled_amount: Decimal,
    pub status: OrderStatus,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub epoch_id: Option<Uuid>,
    pub zone_id: Option<i32>,
    pub meter_id: Option<Uuid>,
    pub refund_tx_signature: Option<String>,
    pub order_pda: Option<String>,
    pub order_index: Option<i64>,
    pub session_token: Option<String>,
    pub blockchain_status: Option<String>,
    pub blockchain_tx_hash: Option<String>,
    pub blockchain_error: Option<String>,
    pub retry_count: i32,
    pub time_in_force: TimeInForce,
    /// Routing: CDA (`Realtime`) vs uniform-price auction (`Interval`).
    /// Defaults to `Realtime`.
    #[serde(default)]
    pub market_segment: MarketSegment,
}

// ── Escrow ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EscrowRecord {
    pub id: Uuid,
    pub user_id: Uuid,
    pub order_id: Option<Uuid>,
    #[schema(value_type = String)]
    pub amount: Decimal,
    pub asset_type: String,
    pub escrow_type: String,
    pub status: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ── Order Requests / Responses ───────────────────────────────────────────────

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateOrderRequest {
    pub side: OrderSide,
    #[schema(value_type = String, example = "10.5")]
    pub energy_amount: Decimal,
    #[schema(value_type = String, example = "0.15")]
    pub price_per_kwh: Option<Decimal>,
    pub order_type: OrderType,
    pub expiry_time: Option<DateTime<Utc>>,
    pub zone_id: Option<i32>,
    pub meter_id: Option<Uuid>,
    /// HMAC-SHA256 signature of the order parameters
    pub signature: Option<String>,
    /// Timestamp of when the signature was created
    pub timestamp: Option<i64>,
    /// Session token for wallet decryption (auto-trading)
    pub session_token: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct RelayOrderRequest {
    /// 16-byte UUID hex string (32 chars)
    #[validate(length(equal = 32))]
    pub order_id: String,
    /// User's Solana public key (Base58)
    pub user_pubkey: String,
    /// Energy amount in base units
    pub energy_amount: u64,
    /// Price per kWh in base units
    pub price_per_kwh: u64,
    /// 0 = Buy, 1 = Sell
    pub side: u8,
    /// Grid zone ID
    pub zone_id: u32,
    /// Expiration timestamp (seconds)
    pub expires_at: i64,
    /// Ed25519 signature (Base58)
    pub signature: String,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UpdateOrderRequest {
    #[schema(value_type = String)]
    pub energy_amount: Option<Decimal>,
    #[schema(value_type = String)]
    pub price_per_kwh: Option<Decimal>,
}

// ── Market Data Models ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MarketData {
    pub current_epoch: u64,
    pub epoch_start_time: DateTime<Utc>,
    pub epoch_end_time: DateTime<Utc>,
    pub status: String,
    pub order_book: OrderBook,
    pub recent_trades: Vec<Trade>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct OrderBook {
    pub sell_orders: Vec<TradingOrder>,
    pub buy_orders: Vec<TradingOrder>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct Trade {
    pub id: Uuid,
    #[schema(value_type = String)]
    pub price: Decimal,
    #[schema(value_type = String)]
    pub amount: Decimal,
    pub executed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MatchingSummary {
    pub matched_count: usize,
    #[schema(value_type = String)]
    pub total_volume: Decimal,
    pub matches: Vec<Uuid>,
}

// ── Conditional Order Models ─────────────────────────────────────────────────

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateConditionalOrderRequest {
    pub side: OrderSide,
    #[schema(value_type = String, example = "10.5")]
    pub energy_amount: Decimal,
    #[schema(value_type = String, example = "0.10")]
    pub trigger_price: Decimal,
    pub trigger_type: TriggerType,
    #[schema(value_type = String, example = "0.09")]
    pub limit_price: Option<Decimal>,
    #[schema(value_type = String, example = "0.02")]
    pub trailing_offset: Option<Decimal>,
    pub expiry_time: Option<DateTime<Utc>>,
    /// Session token for wallet decryption (auto-trading)
    pub session_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConditionalOrderResponse {
    pub id: Uuid,
    pub trigger_type: TriggerType,
    pub trigger_status: TriggerStatus,
    #[schema(value_type = String)]
    pub trigger_price: Decimal,
    pub created_at: DateTime<Utc>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ConditionalOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub side: OrderSide,
    #[schema(value_type = String)]
    pub energy_amount: Decimal,
    #[schema(value_type = String)]
    pub trigger_price: Decimal,
    pub trigger_type: TriggerType,
    pub trigger_status: TriggerStatus,
    #[schema(value_type = String)]
    pub limit_price: Option<Decimal>,
    #[schema(value_type = String)]
    pub trailing_offset: Option<Decimal>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub triggered_at: Option<DateTime<Utc>>,
    pub last_peak_price: Option<Decimal>,
}

// ── Recurring Order Models ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct CreateRecurringOrderRequest {
    pub side: OrderSide,
    #[schema(value_type = String, example = "10.0")]
    pub energy_amount: Decimal,
    #[schema(value_type = String, example = "0.20")]
    pub max_price_per_kwh: Option<Decimal>,
    #[schema(value_type = String, example = "0.10")]
    pub min_price_per_kwh: Option<Decimal>,
    pub interval_type: IntervalType,
    /// Execute every N intervals (default: 1)
    pub interval_value: Option<i32>,
    /// Maximum number of executions (null = unlimited)
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    /// Session token for wallet decryption (auto-trading)
    pub session_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RecurringOrderResponse {
    pub id: Uuid,
    pub status: RecurringStatus,
    pub next_execution_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RecurringOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub side: OrderSide,
    #[schema(value_type = String)]
    pub energy_amount: Decimal,
    #[schema(value_type = String)]
    pub max_price_per_kwh: Option<Decimal>,
    #[schema(value_type = String)]
    pub min_price_per_kwh: Option<Decimal>,
    pub interval_type: IntervalType,
    pub interval_value: i32,
    pub next_execution_at: DateTime<Utc>,
    pub last_executed_at: Option<DateTime<Utc>>,
    pub status: RecurringStatus,
    pub total_executions: i32,
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Create-input for a recurring order. `next_execution_at` is computed at
/// insert time from `interval_type`/`interval_value` (see `next_execution_at`
/// in `trading_core::recurring`).
#[derive(Debug, Clone)]
pub struct NewRecurringOrder {
    pub user_id: Uuid,
    pub side: OrderSide,
    pub energy_amount: Decimal,
    pub max_price_per_kwh: Option<Decimal>,
    pub min_price_per_kwh: Option<Decimal>,
    pub interval_type: IntervalType,
    pub interval_value: i32,
    pub next_execution_at: DateTime<Utc>,
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
}

// ── Price Alert Models ───────────────────────────────────────────────────────

/// A user-defined price alert (table `price_alerts`). The energy market has no
/// per-symbol order books, so the frontend `symbol` is stored in `note` rather
/// than a dedicated column (see BACKEND_GAP_PLAN.md §"Contract mismatches").
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PriceAlert {
    pub id: Uuid,
    pub user_id: Uuid,
    #[schema(value_type = String)]
    pub target_price: Decimal,
    pub condition: AlertCondition,
    pub status: AlertStatus,
    pub triggered_at: Option<DateTime<Utc>>,
    #[schema(value_type = String)]
    pub triggered_price: Option<Decimal>,
    pub repeat: bool,
    /// Holds the frontend `symbol` label; no `symbol` column exists.
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Input for creating a price alert. `note` carries the frontend `symbol`.
#[derive(Debug, Clone)]
pub struct NewPriceAlert {
    pub user_id: Uuid,
    pub target_price: Decimal,
    pub condition: AlertCondition,
    pub note: Option<String>,
}

#[derive(Debug, Deserialize, Validate, ToSchema)]
pub struct UpdateRecurringOrderRequest {
    #[schema(value_type = String)]
    pub energy_amount: Option<Decimal>,
    #[schema(value_type = String)]
    pub max_price_per_kwh: Option<Decimal>,
    #[schema(value_type = String)]
    pub min_price_per_kwh: Option<Decimal>,
    pub interval_type: Option<IntervalType>,
    pub interval_value: Option<i32>,
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
}

// ── Clearing / Settlement Models ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MarketEpoch {
    pub id: Uuid,
    pub epoch_number: i64,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status: EpochStatus,
    pub clearing_price: Option<Decimal>,
    pub total_volume: Option<Decimal>,
    pub total_orders: Option<i64>,
    pub matched_orders: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct OrderMatch {
    pub id: Uuid,
    pub epoch_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub matched_amount: Decimal,
    pub match_price: Decimal,
    pub match_time: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct TradeMatch {
    pub id: Uuid,
    pub match_id: Uuid,
    pub epoch_id: Uuid,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub quantity: Decimal,
    pub price: Decimal,
    pub total_value: Decimal,
    pub wheeling_charge: Decimal,
    pub loss_factor: Decimal,
    pub loss_cost: Decimal,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub matched_at: DateTime<Utc>,
    pub buyer_session_token: Option<String>,
    pub seller_session_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub enum SettlementStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    PermanentlyFailed,
}

impl std::fmt::Display for SettlementStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Processing => write!(f, "processing"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::PermanentlyFailed => write!(f, "permanently_failed"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settlement {
    pub id: Uuid,
    pub trade_id: Option<Uuid>,
    pub epoch_id: Uuid,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub energy_amount: Decimal,
    pub price: Decimal,
    pub total_amount: Decimal,
    pub fee_amount: Decimal,
    pub net_amount: Decimal,
    pub status: SettlementStatus,
    pub blockchain_tx: Option<String>,
    pub created_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub wheeling_charge: Option<Decimal>,
    pub loss_factor: Option<Decimal>,
    pub loss_cost: Option<Decimal>,
    pub effective_energy: Option<Decimal>,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub buyer_session_token: Option<String>,
    pub seller_session_token: Option<String>,
    pub erc_certificate_id: Option<String>,
    pub erc_transfer_tx: Option<String>,
    pub retry_count: i32,
    pub error_message: Option<String>,
}

/// Aggregate settlement counts by status + total settled value.
/// `confirmed_count` maps to the DB `completed` status.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SettlementStats {
    pub pending_count: i64,
    pub processing_count: i64,
    pub confirmed_count: i64,
    pub failed_count: i64,
    #[schema(value_type = String)]
    pub total_settled_value: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SettlementTransaction {
    pub settlement_id: Uuid,
    pub signature: String,
    pub slot: u64,
    pub confirmation_status: String,
}

#[derive(Debug, Clone)]
pub struct OrderBookEntry {
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub side: OrderSide,
    pub energy_amount: Decimal,
    pub original_amount: Decimal,
    pub price_per_kwh: Decimal,
    pub created_at: DateTime<Utc>,
    pub zone_id: Option<i32>,
    pub session_token: Option<String>,
    pub signature: Option<String>,
    pub payload_bytes: Option<Vec<u8>>,
    pub time_in_force: TimeInForce,
}

/// Market clearing price result from supply-demand intersection
#[derive(Debug, Clone)]
pub struct ClearingPrice {
    /// The calculated clearing price (midpoint of bid-ask spread)
    pub price: Decimal,
    /// Total volume that can be cleared at this price
    pub volume: Decimal,
    /// Number of buy orders participating
    pub buy_orders_count: usize,
    /// Number of sell orders participating
    pub sell_orders_count: usize,
    /// Best bid price
    pub best_bid: Decimal,
    /// Best ask price
    pub best_ask: Decimal,
}

// ── VPP Models ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct VppCluster {
    pub id: Uuid,
    pub cluster_id: String,
    pub zone_id: Option<i32>,
    pub total_capacity_kwh: f64,
    pub current_stored_kwh: f64,
    pub soc_percentage: f64,
    pub target_soc_percentage: f64,
    pub flex_up_kw: f64,
    pub flex_down_kw: f64,
    pub health_score: f64,
    pub resource_count: i32,
    pub dispatch_mode: String,
    pub last_update: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct VppMember {
    pub id: Uuid,
    pub cluster_id: String,
    pub meter_id: String,
    pub contribution_weight: Option<f64>,
    pub is_active: Option<bool>,
    pub joined_at: Option<DateTime<Utc>>,
    pub rated_power_kw: Option<f64>,
    pub rated_capacity_kwh: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VppMetricsUpdate {
    pub cluster_id: String,
    pub delta_energy_kwh: f64,
    pub current_power_kw: f64,
}

// ── DAO / Governance Models ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ZoneConfig {
    pub zone_id: i32,
    #[schema(value_type = String)]
    pub incentive_multiplier: Decimal,
    #[schema(value_type = String)]
    pub wheeling_charge: Decimal,
    pub maintenance_mode: bool,
    pub last_updated: DateTime<Utc>,
}

// ── Modernized Platform Models ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FuturesProduct {
    pub id: Uuid,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    #[schema(value_type = String)]
    pub contract_size: Decimal,
    pub expiration_date: DateTime<Utc>,
    #[schema(value_type = String)]
    pub current_price: Decimal,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FuturesOrder {
    pub id: Uuid,
    pub user_id: Uuid,
    pub product_id: Uuid,
    pub side: crate::types::FuturesOrderSide,
    pub order_type: crate::types::FuturesOrderType,
    #[schema(value_type = String)]
    pub quantity: Decimal,
    #[schema(value_type = String)]
    pub price: Decimal,
    pub leverage: i32,
    pub status: crate::types::FuturesOrderStatus,
    #[schema(value_type = String)]
    pub filled_quantity: Decimal,
    #[schema(value_type = String)]
    pub average_fill_price: Option<Decimal>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FuturesPosition {
    pub id: Uuid,
    pub user_id: Uuid,
    pub product_id: Uuid,
    pub side: crate::types::FuturesOrderSide,
    #[schema(value_type = String)]
    pub quantity: Decimal,
    #[schema(value_type = String)]
    pub entry_price: Decimal,
    #[schema(value_type = String)]
    pub current_price: Decimal,
    pub leverage: i32,
    #[schema(value_type = String)]
    pub margin_used: Decimal,
    #[schema(value_type = String)]
    pub unrealized_pnl: Decimal,
    #[schema(value_type = String)]
    pub liquidation_price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CarbonCredit {
    pub id: Uuid,
    pub user_id: Uuid,
    #[schema(value_type = String)]
    pub amount: Decimal,
    pub source: String,
    pub status: crate::types::CarbonStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CarbonTransaction {
    pub id: Uuid,
    pub from_user_id: Option<Uuid>,
    pub to_user_id: Uuid,
    #[schema(value_type = String)]
    pub amount: Decimal,
    #[schema(value_type = String)]
    pub price_per_credit: Option<Decimal>,
    pub status: crate::types::CarbonTransactionStatus,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserAnalytics {
    #[schema(value_type = String)]
    pub total_traded_kwh: Decimal,
    #[schema(value_type = String)]
    pub total_spent_grid: Decimal,
    #[schema(value_type = String)]
    pub total_earned_grid: Decimal,
    #[schema(value_type = String)]
    pub carbon_offset_tons: Decimal,
    pub reliability_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionData {
    pub id: Uuid,
    pub transaction_type: String,
    #[schema(value_type = String)]
    pub amount: Decimal,
    pub asset: String,
    pub status: String,
    pub timestamp: DateTime<Utc>,
    pub reference_id: Option<Uuid>,
}

// ── Outbox Models ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub enum OutboxStatus {
    Pending,
    Processed,
    Failed,
}

impl std::fmt::Display for OutboxStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Processed => write!(f, "processed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub status: OutboxStatus,
    pub attempts: i32,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}
