use serde::{Deserialize, Serialize};
use utoipa::{ToSchema, IntoParams};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Pending,
    Submitted,
    Confirmed,
    Failed,
    Settled,
    Processing,
}

impl FromStr for TransactionStatus {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(TransactionStatus::Pending),
            "submitted" => Ok(TransactionStatus::Submitted),
            "confirmed" => Ok(TransactionStatus::Confirmed),
            "failed" => Ok(TransactionStatus::Failed),
            "settled" => Ok(TransactionStatus::Settled),
            "processing" => Ok(TransactionStatus::Processing),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TransactionStatus::Pending => "pending",
            TransactionStatus::Submitted => "submitted",
            TransactionStatus::Confirmed => "confirmed",
            TransactionStatus::Failed => "failed",
            TransactionStatus::Settled => "settled",
            TransactionStatus::Processing => "processing",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    EnergyTrade,
    TokenMint,
    TokenTransfer,
    GovernanceVote,
    OracleUpdate,
    RegistryUpdate,
    Swap,
}

impl FromStr for TransactionType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "energy_trade" => Ok(TransactionType::EnergyTrade),
            "token_mint" => Ok(TransactionType::TokenMint),
            "token_transfer" => Ok(TransactionType::TokenTransfer),
            "governance_vote" => Ok(TransactionType::GovernanceVote),
            "oracle_update" => Ok(TransactionType::OracleUpdate),
            "registry_update" => Ok(TransactionType::RegistryUpdate),
            "swap" => Ok(TransactionType::Swap),
            _ => Err(()),
        }
    }
}

impl TransactionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TransactionType::EnergyTrade => "energy_trade",
            TransactionType::TokenMint => "token_mint",
            TransactionType::TokenTransfer => "token_transfer",
            TransactionType::GovernanceVote => "governance_vote",
            TransactionType::OracleUpdate => "oracle_update",
            TransactionType::RegistryUpdate => "registry_update",
            TransactionType::Swap => "swap",
        }
    }
}

impl std::fmt::Display for TransactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TransactionType::EnergyTrade => "energy_trade",
            TransactionType::TokenMint => "token_mint",
            TransactionType::TokenTransfer => "token_transfer",
            TransactionType::GovernanceVote => "governance_vote",
            TransactionType::OracleUpdate => "oracle_update",
            TransactionType::RegistryUpdate => "registry_update",
            TransactionType::Swap => "swap",
        };
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BlockchainOperation {
    pub operation_type: TransactionType,
    pub operation_id: Uuid,
    pub user_id: Uuid,
    pub signature: Option<String>,
    pub tx_type: String,
    pub status: TransactionStatus,
    pub operation_status: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub payload: serde_json::Value,
    pub max_priority_fee: Option<u64>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct TransactionFilters {
    pub user_id: Option<Uuid>,
    pub operation_type: Option<String>,
    pub tx_type: Option<TransactionType>,
    pub status: Option<TransactionStatus>,
    pub date_from: Option<DateTime<Utc>>,
    pub date_to: Option<DateTime<Utc>>,
    pub min_attempts: Option<i32>,
    pub has_signature: Option<bool>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionResponse {
    pub transaction_type: TransactionType,
    pub operation_id: Uuid,
    pub user_id: Uuid,
    pub status: TransactionStatus,
    pub signature: Option<String>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionStats {
    pub total_count: i64,
    pub pending_count: i64,
    pub submitted_count: i64,
    pub confirmed_count: i64,
    pub failed_count: i64,
    pub settled_count: i64,
    pub processing_count: i64,
    pub avg_confirmation_time_seconds: Option<f64>,
    pub success_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionMonitoringConfig {
    pub polling_interval_ms: u64,
    pub max_retries: i32,
    pub confirmation_blocks: u32,
    pub enabled: bool,
    pub max_retry_attempts: i32,
    pub transaction_expiry_seconds: u64,
}

impl Default for TransactionMonitoringConfig {
    fn default() -> Self {
        Self {
            polling_interval_ms: 1000,
            max_retries: 3,
            confirmation_blocks: 1,
            enabled: true,
            max_retry_attempts: 5,
            transaction_expiry_seconds: 3600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionRetryRequest {
    pub operation_id: Uuid,
    pub max_attempts: Option<i32>,
    pub operation_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TransactionRetryResponse {
    pub operation_id: Uuid,
    pub success: bool,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub signature: Option<String>,
    pub status: TransactionStatus,
    pub new_attempts: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateTransactionRequest {
    pub user_id: Uuid,
    pub transaction_type: TransactionType,
    pub payload: TransactionPayload,
    pub skip_prevalidation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransactionPayload {
    EnergyTrade {
        market_pubkey: String,
        energy_amount: u64,
        price_per_kwh: u64,
        order_type: String,
        erc_certificate_id: Option<String>,
    },
    TokenMint {
        recipient: String,
        amount: u64,
    },
    TokenTransfer {
        from: String,
        to: String,
        amount: u64,
        token_mint: String,
    },
    GovernanceVote {
        proposal_id: u64,
        vote: bool,
    },
    OracleUpdate {
        price_feed_id: String,
        price: u64,
        confidence: u64,
    },
    RegistryUpdate {
        participant_id: String,
        update_data: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EnergyTradePayload {
    pub market_pubkey: String,
    pub energy_amount: u64,
    pub price_per_kwh: u64,
    pub order_type: String,
    pub erc_certificate_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TokenMintPayload {
    pub recipient: String,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TokenTransferPayload {
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub token_mint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GovernanceVotePayload {
    pub proposal_id: u64,
    pub vote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct OracleUpdatePayload {
    pub price_feed_id: String,
    pub price: u64,
    pub confidence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RegistryUpdatePayload {
    pub participant_id: String,
    pub update_data: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidationError {
    pub code: String,
    pub message: String,
    pub field: Option<String>,
}

impl ValidationError {
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            field: None,
        }
    }

    pub fn with_field(code: &str, message: &str, field: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            field: Some(field.to_string()),
        }
    }
}
