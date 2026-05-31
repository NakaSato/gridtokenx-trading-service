use serde::{Deserialize, Serialize};

/// Published to `chain.tx.submit`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxSubmitMessage {
    /// UUID — used to match the reply and for idempotency tracking
    pub correlation_id: String,
    /// NATS subject the caller subscribes to for the result
    pub reply_subject: String,
    /// bincode-serialised solana_sdk::transaction::Transaction
    pub serialized_tx: Vec<u8>,
    /// Signing key identifier — currently only "platform_admin" is authorised
    pub key_id: String,
    pub skip_preflight: bool,
    pub retry_count: u32,
    /// SPIFFE URI of the publishing service — used for application-level identity check
    pub service_identity: String,
    /// Unix milliseconds when published.
    pub created_at_ms: u64,
}

/// Published to `chain.tx.result.{correlation_id}`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxResultMessage {
    pub correlation_id: String,
    pub success: bool,
    pub signature: Option<String>,
    pub error: Option<String>,
    pub slot: u64,
}

/// Published to `chain.tx.simulate`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxSimulateMessage {
    pub correlation_id: String,
    pub reply_subject: String,
    pub serialized_tx: Vec<u8>,
    pub key_id: String,
    pub service_identity: String,
    pub created_at_ms: u64,
}

/// Published to `chain.tx.simulate.result.{correlation_id}`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxSimulateResultMessage {
    pub correlation_id: String,
    pub success: bool,
    pub compute_units_consumed: u64,
    pub error_message: String,
    pub logs: Vec<String>,
}

/// Published to `chain.tx.cancel`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxCancelMessage {
    pub correlation_id: String,
    pub reply_subject: String,
    pub service_identity: String,
    pub created_at_ms: u64,
}

/// Published to reply_subject from TxCancelMessage
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxCancelResultMessage {
    pub correlation_id: String,
    pub success: bool,
    pub error: Option<String>,
}

/// Published to `telemetry.smart_meter` or `meter.reading.mint`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MeterReadingMessage {
    pub device_id: String,
    pub wallet_address: String,
    pub energy_kwh: f64,
    pub timestamp_ms: u64,
}
