use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::infra::db::schema::types::{EpochStatus, OrderSide, TimeInForce};

#[derive(Debug, Clone, sqlx::FromRow)]
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

#[derive(Debug, Clone, sqlx::FromRow)]
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TradeMatch {
    pub id: Uuid,       // Unique ID for this trade event
    pub match_id: Uuid, // Reference to OrderMatch
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, sqlx::FromRow)]
pub struct Settlement {
    pub id: Uuid,
    pub epoch_id: Uuid,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub energy_amount: Decimal,
    pub price_per_kwh: Decimal,
    pub total_amount: Decimal,
    pub fee_amount: Decimal,
    pub wheeling_charge: Decimal,
    pub loss_factor: Decimal,
    pub loss_cost: Decimal,
    pub effective_energy: Decimal,
    pub buyer_zone_id: Option<i32>,
    pub seller_zone_id: Option<i32>,
    pub net_amount: Decimal,
    pub status: String,
    pub buyer_session_token: Option<String>,
    pub seller_session_token: Option<String>,
    pub buy_signature: Option<String>,
    pub sell_signature: Option<String>,
    pub buy_payload: Option<Vec<u8>>,
    pub sell_payload: Option<Vec<u8>>,
    pub retry_count: i32,
    pub error_message: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct OrderBookEntry {
    pub order_id: Uuid,
    pub user_id: Uuid,
    pub side: OrderSide,
    pub energy_amount: Decimal,   // Remaining amount
    pub original_amount: Decimal, // Original order amount
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
