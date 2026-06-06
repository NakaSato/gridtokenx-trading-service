use serde::{Deserialize, Serialize};

/// Published to `chain.tx.submit`
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxSubmitMessage {
    /// UUID — per-attempt; routes the reply on `chain.tx.result.{correlation_id}`.
    /// NOT a dedup key (it changes every retry). See `idempotency_key`.
    pub correlation_id: String,
    /// Stable per *logical* operation — the bridge uses this for effect-level
    /// dedup (sign+submit at most once per key). Distinct from `correlation_id`.
    /// Empty = legacy/unprotected: the bridge submits without dedup. A bridge
    /// that predates this field ignores it (no worse than before).
    /// See `NATS_IDEMPOTENCY_DESIGN.md`.
    #[serde(default)]
    pub idempotency_key: String,
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
    /// True if the bridge served this result from its dedup store instead of
    /// submitting a fresh transaction. `#[serde(default)]` => an older bridge
    /// that omits it decodes as `false`. For metrics/logs only.
    #[serde(default)]
    pub deduplicated: bool,
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
