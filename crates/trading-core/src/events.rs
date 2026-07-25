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
        /// Owner of the order. Carried so downstream consumers (the
        /// notification service) can route fill/cancel/expiry updates to the
        /// trader without querying this service. `None` only when the emitting
        /// path could not resolve the owner, in which case consumers skip.
        #[serde(default)]
        user_id: Option<Uuid>,
        filled_amount: Decimal,
        status: String,
        zone_id: Option<i32>,
    },
    PeakPriceUpdate {
        id: Uuid,
        peak_price: Decimal,
        zone_id: Option<i32>,
    },
    TriggerExecution {
        id: Uuid,
        triggered_at: DateTime<Utc>,
    },
    PriceAlertTriggered(PriceAlertTriggeredPayload),
    OrderCreated(OrderCreatedPayload),
    ErcIssued(ErcIssuedPayload),
    SettlementProcessed(SettlementProcessedPayload),
    UserRegistered(UserRegisteredPayload),
    UserOnboarded(UserOnboardedPayload),
    UserWalletLinked(UserWalletLinkedPayload),
    OracleReading(OracleReadingPayload),
    VppDispatched(VppDispatchedPayload),
}

impl Event {
    /// Stable string tag written to the `event_type` column of the outbox
    /// table (and used for downstream topic routing). Keep in sync with the
    /// `Event` variants — the persistence and outbox layers rely on it.
    pub fn outbox_event_type(&self) -> &'static str {
        match self {
            Event::OrderMatched(_) => "OrderMatched",
            Event::SettlementRequested(_) => "SettlementRequested",
            Event::OrderUpdate { .. } => "OrderUpdate",
            Event::PeakPriceUpdate { .. } => "PeakPriceUpdate",
            Event::TriggerExecution { .. } => "TriggerExecution",
            Event::PriceAlertTriggered(_) => "PriceAlertTriggered",
            Event::OrderCreated(_) => "OrderCreated",
            Event::ErcIssued(_) => "ErcIssued",
            Event::SettlementProcessed(_) => "SettlementProcessed",
            Event::UserRegistered(_) => "UserRegistered",
            Event::UserOnboarded(_) => "UserOnboarded",
            Event::UserWalletLinked(_) => "UserWalletLinked",
            Event::OracleReading(_) => "OracleReading",
            Event::VppDispatched(_) => "VppDispatched",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VppDispatchedPayload {
    pub cluster_id: String,
    pub target_kw: f64,
    pub members_commanded: usize,
    pub timestamp: DateTime<Utc>,
}

/// Emitted when a price alert's condition is met by the current market price
/// (see `trigger_evaluator`). Relayed via the outbox to the notification
/// service, which fans it out to the alert's owner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceAlertTriggeredPayload {
    pub alert_id: Uuid,
    pub user_id: Uuid,
    pub target_price: Decimal,
    pub triggered_price: Decimal,
    pub condition: String,
    pub triggered_at: DateTime<Utc>,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleReadingPayload {
    pub reading_id: Uuid,
    pub meter_id: String,
    pub serial_number: String,
    pub user_id: Option<Uuid>,
    pub wallet_address: Option<String>,
    pub zone_id: Option<i32>,
    pub timestamp: DateTime<Utc>,
    pub kwh: Decimal,
    pub energy_generated: Option<Decimal>,
    pub energy_consumed: Option<Decimal>,
    pub meter_signature: Option<String>,
}
