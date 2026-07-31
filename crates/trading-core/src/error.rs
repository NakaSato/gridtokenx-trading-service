//! Error types for the GridTokenX trading service.

use serde::Serialize;
use thiserror::Error;

/// Structured error response returned by API handlers.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: ErrorDetail,
    pub request_id: String,
    pub timestamp: String,
}

#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    pub code_number: u16,
    pub message: String,
    pub details: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// Unified error type used across all trading domain crates.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Authentication failed: {0}")]
    Authentication(String),

    #[error("Authorization failed: {0}")]
    Authorization(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Blockchain error: {0}")]
    Blockchain(String),

    #[error("External service error: {0}")]
    ExternalService(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Internal server error: {0}")]
    Internal(String),

    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        match err.downcast::<ApiError>() {
            Ok(api_err) => api_err,
            Err(e) => ApiError::Internal(e.to_string()),
        }
    }
}

/// Convenience `Result` alias.
pub type Result<T> = std::result::Result<T, ApiError>;

/// Did the chain *execute* this transaction and the program reject it?
///
/// The distinction matters because the two failure classes need opposite handling and
/// were previously treated alike. An `InstructionError` means the transaction reached the
/// program, which then refused it: with identical inputs that verdict is deterministic, so
/// resubmitting produces the same rejection forever. A transport failure (timeout, no
/// connection, expired blockhash) never reached that verdict and is worth retrying.
///
/// Grounded in an observed incident: 18 orders were placed into zones with no initialized
/// `ZoneMarket`, each rejected `{"InstructionError":[0,{"Custom":3007}]}`
/// (`AccountOwnedByWrongProgram`). Placement was treated as best-effort "left for retry",
/// so those orders were accepted with `order_pda = NULL`, matched by the CDA, and their 14
/// settlements then failed 5 times each before being parked `permanently_failed`. Nothing
/// retries placement — no worker looks for a NULL PDA — so "left for retry" never happened.
///
/// Matching on the serialized error text is deliberate: the gateway trait returns
/// `ApiError`, so the structured `TransactionError` is already stringified by the time any
/// caller sees it, and re-plumbing that type is a larger change than this predicate.
pub fn is_deterministic_chain_rejection(message: &str) -> bool {
    message.contains("InstructionError")
}

#[cfg(test)]
mod tests {
    use super::is_deterministic_chain_rejection;

    /// The observed zone-market rejection: executed, and refused by the program.
    #[test]
    fn program_rejection_is_deterministic() {
        assert!(is_deterministic_chain_rejection(
            "place_order_on_chain: order placement 2BjUZ4t confirmation failed: transaction 2BjUZ4t \
             failed on-chain: {\"InstructionError\":[0,{\"Custom\":3007}]}"
        ));
    }

    /// Any program error qualifies, not just 3007 — the point is that the verdict is final.
    #[test]
    fn other_instruction_errors_are_deterministic_too() {
        assert!(is_deterministic_chain_rejection(
            "{\"InstructionError\":[0,{\"Custom\":6024}]}"
        ));
        assert!(is_deterministic_chain_rejection(
            "{\"InstructionError\":[1,\"InvalidAccountData\"]}"
        ));
    }

    /// Transport failures never got a verdict, so they stay retryable — misclassifying
    /// these would turn a validator blip into a rejected customer order.
    #[test]
    fn transport_failures_are_not_deterministic() {
        for msg in [
            "error sending request for url (http://127.0.0.1:8899/): operation timed out",
            "RPC response error -32005: node is behind by 42 slots",
            "blockhash not found",
            "connection refused",
        ] {
            assert!(!is_deterministic_chain_rejection(msg), "{msg} must stay retryable");
        }
    }
}
