//! Domain event definitions for the GridTokenX trading service.
//!
//! These events flow through the event bus (Kafka/Redis) and are consumed
//! by downstream services.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::models::Settlement;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", content = "data")]
pub enum Event {
    OrderMatched(OrderMatchedPayload),
    SettlementRequested(Settlement),
    OrderUpdate {
        id: Uuid,
        filled_amount: Decimal,
        status: String,
    },
    PeakPriceUpdate {
        id: Uuid,
        peak_price: Decimal,
    },
    TriggerExecution {
        id: Uuid,
        triggered_at: DateTime<Utc>,
    },
    OrderCreated(OrderCreatedPayload),
    ErcIssued(ErcIssuedPayload),
    SettlementProcessed(SettlementProcessedPayload),
    UserRegistered(UserRegisteredPayload),
    UserOnboarded(UserOnboardedPayload),
    UserWalletLinked(UserWalletLinkedPayload),
}

/// Lightweight payload for OrderCreated events.
/// The full `TradingOrderDb` struct lives in `trading-persistence`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCreatedPayload {
    pub id: Uuid,
    pub user_id: Uuid,
    pub order_type: String,
    pub side: String,
    pub energy_amount: Decimal,
    pub price_per_kwh: Decimal,
    pub status: String,
    pub zone_id: Option<i32>,
    pub created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserOnboardedPayload {
    pub user_id: Uuid,
    pub wallet_address: String,
    pub user_account_pda: String,
    pub transaction_signature: String,
    pub user_type: String,
    pub shard_id: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserWalletLinkedPayload {
    pub user_id: Uuid,
    pub wallet_address: String,
    pub user_account_pda: String,
    pub transaction_signature: String,
    pub shard_id: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRegisteredPayload {
    pub user_id: Uuid,
    pub username: String,
    pub email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementProcessedPayload {
    pub settlement_id: Uuid,
    pub tx_signature: String,
    pub status: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErcIssuedPayload {
    pub certificate_id: String,
    pub user_id: Uuid,
    pub energy_amount: Decimal,
    pub renewable_source: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderMatchedPayload {
    pub match_id: Uuid,
    pub epoch_id: Uuid,
    pub buy_order_id: Uuid,
    pub sell_order_id: Uuid,
    pub amount: Decimal,
    pub price: Decimal,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub zone_id: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementRequestedPayload {
    pub settlement_id: Uuid,
    pub trade_id: Uuid,
    pub amount: Decimal,
    pub price: Decimal,
    pub total_value: Decimal,
    pub buyer_id: Uuid,
    pub seller_id: Uuid,
    pub timestamp: DateTime<Utc>,
}
