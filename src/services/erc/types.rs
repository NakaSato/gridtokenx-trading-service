use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Energy Renewable Certificate
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ErcCertificate {
    pub id: Uuid,
    pub certificate_id: String,
    pub user_id: Option<Uuid>,
    pub wallet_address: String,
    #[sqlx(default)]
    pub kwh_amount: Option<Decimal>,
    #[sqlx(default)]
    pub issue_date: Option<DateTime<Utc>>,
    pub expiry_date: Option<DateTime<Utc>>,
    #[sqlx(default)]
    pub issuer_wallet: Option<String>,
    pub status: String,
    pub blockchain_tx_signature: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub settlement_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Request to issue a new ERC
#[derive(Debug, Deserialize, Serialize)]
pub struct IssueErcRequest {
    pub wallet_address: String,
    pub meter_id: Option<String>,
    pub kwh_amount: Decimal,
    pub expiry_date: Option<DateTime<Utc>>,
    pub metadata: Option<serde_json::Value>,
}

/// Certificate transfer record
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct CertificateTransfer {
    pub id: Uuid,
    pub certificate_id: Uuid,
    pub from_wallet: String,
    pub to_wallet: String,
    pub transfer_date: DateTime<Utc>,
    pub blockchain_tx_signature: String,
    pub created_at: DateTime<Utc>,
}

/// ERC Certificate metadata for on-chain storage
#[derive(Debug, Serialize, Deserialize)]
pub struct ErcMetadata {
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub image: String,
    pub attributes: Vec<ErcAttribute>,
    pub properties: ErcProperties,
    pub external_url: String,
    pub animation_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErcAttribute {
    pub trait_type: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErcProperties {
    pub files: Vec<ErcFile>,
    pub category: String,
    pub creators: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErcFile {
    pub uri: String,
    pub r#type: String,
}

#[derive(Debug, FromRow)]
pub struct CertificateStatsRow {
    pub total_count: i64,
    pub active_kwh: Decimal,
    pub retired_kwh: Decimal,
    pub total_kwh: Decimal,
}

#[derive(Debug, Serialize)]
pub struct CertificateStats {
    pub total_certificates: i64,
    pub active_kwh: Decimal,
    pub retired_kwh: Decimal,
    pub total_kwh: Decimal,
}
