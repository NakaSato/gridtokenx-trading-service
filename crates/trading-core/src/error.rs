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
