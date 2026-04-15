use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Event types we track from the blockchain
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    TokenMint,
    TokenTransfer,
    OrderCreated,
    OrderMatched,
    Settlement,
    MeterRegistered,
}

impl EventType {
    pub fn as_str(&self) -> &str {
        match self {
            EventType::TokenMint => "token_mint",
            EventType::TokenTransfer => "token_transfer",
            EventType::OrderCreated => "order_created",
            EventType::OrderMatched => "order_matched",
            EventType::Settlement => "settlement",
            EventType::MeterRegistered => "meter_registered",
        }
    }
}

/// Parsed blockchain event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockchainEvent {
    pub event_type: EventType,
    pub transaction_signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub program_id: String,
    pub event_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReplayStatus {
    pub start_slot: u64,
    pub end_slot: u64,
    pub current_slot: u64,
    pub start_time: DateTime<Utc>,
    pub status: String, // "running", "completed", "failed"
}

/// Event processor statistics
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EventProcessorStats {
    pub total_events: i64,
    pub confirmed_readings: i64,
    pub pending_confirmations: i64,
    pub total_retries: u64,
}
