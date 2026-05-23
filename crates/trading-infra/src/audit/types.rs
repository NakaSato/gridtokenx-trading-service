use chrono::Utc;
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Security and business events to be audited
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuditEvent {
    /// User successfully logged in
    UserLogin {
        user_id: Uuid,
        ip: String,
        user_agent: Option<String>,
    },
    /// User logged out
    UserLogout { user_id: Uuid },
    /// Login attempt failed
    LoginFailed {
        email: String,
        ip: String,
        reason: String,
        user_agent: Option<String>,
    },
    /// User password was changed
    PasswordChanged { user_id: Uuid, ip: String },
    /// Email verification completed
    EmailVerified { user_id: Uuid },
    /// New API key generated
    ApiKeyGenerated { user_id: Uuid, key_id: Uuid },
    /// User registered on blockchain
    BlockchainRegistration {
        user_id: Uuid,
        wallet_address: String,
    },
    /// Trading order created
    OrderCreated {
        user_id: Uuid,
        order_id: Uuid,
        order_type: String,
        amount: String,
        price: String,
    },
    /// Trading order cancelled
    OrderCancelled { user_id: Uuid, order_id: Uuid },
    /// Trading order matched
    OrderMatched {
        buyer_id: Uuid,
        seller_id: Uuid,
        order_id: Uuid,
        amount: String,
    },
    /// Unauthorized access attempt
    UnauthorizedAccess {
        ip: String,
        endpoint: String,
        user_agent: Option<String>,
    },
    /// Rate limit exceeded
    RateLimitExceeded { ip: String, endpoint: String },
    /// Sensitive data accessed
    DataAccess {
        user_id: Uuid,
        resource_type: String,
        resource_id: String,
        action: String,
    },
    /// Admin action performed
    AdminAction {
        admin_id: Uuid,
        action: String,
        target_user_id: Option<Uuid>,
        details: String,
    },
}

impl AuditEvent {
    /// Get the event type as a string for database storage
    pub fn event_type(&self) -> &'static str {
        match self {
            AuditEvent::UserLogin { .. } => "user_login",
            AuditEvent::UserLogout { .. } => "user_logout",
            AuditEvent::LoginFailed { .. } => "login_failed",
            AuditEvent::PasswordChanged { .. } => "password_changed",
            AuditEvent::EmailVerified { .. } => "email_verified",
            AuditEvent::ApiKeyGenerated { .. } => "api_key_generated",
            AuditEvent::BlockchainRegistration { .. } => "blockchain_registration",
            AuditEvent::OrderCreated { .. } => "order_created",
            AuditEvent::OrderCancelled { .. } => "order_cancelled",
            AuditEvent::OrderMatched { .. } => "order_matched",
            AuditEvent::UnauthorizedAccess { .. } => "unauthorized_access",
            AuditEvent::RateLimitExceeded { .. } => "rate_limit_exceeded",
            AuditEvent::DataAccess { .. } => "data_access",
            AuditEvent::AdminAction { .. } => "admin_action",
        }
    }

    /// Extract user_id if present in the event
    pub fn user_id(&self) -> Option<Uuid> {
        match self {
            AuditEvent::UserLogin { user_id, .. }
            | AuditEvent::UserLogout { user_id }
            | AuditEvent::PasswordChanged { user_id, .. }
            | AuditEvent::EmailVerified { user_id }
            | AuditEvent::ApiKeyGenerated { user_id, .. }
            | AuditEvent::BlockchainRegistration { user_id, .. }
            | AuditEvent::OrderCreated { user_id, .. }
            | AuditEvent::OrderCancelled { user_id, .. }
            | AuditEvent::DataAccess { user_id, .. }
            | AuditEvent::AdminAction {
                admin_id: user_id, ..
            } => Some(*user_id),
            AuditEvent::OrderMatched { buyer_id, .. } => Some(*buyer_id), // Prioritize buyer for indexing
            _ => None,
        }
    }

    /// Extract IP address if present in the event
    pub fn ip_address(&self) -> Option<&str> {
        match self {
            AuditEvent::UserLogin { ip, .. }
            | AuditEvent::LoginFailed { ip, .. }
            | AuditEvent::PasswordChanged { ip, .. }
            | AuditEvent::UnauthorizedAccess { ip, .. }
            | AuditEvent::RateLimitExceeded { ip, .. } => Some(ip.as_str()),
            _ => None,
        }
    }
}

/// Audit event database record
#[derive(Debug, Clone, Serialize, ToSchema, sqlx::FromRow)]
pub struct AuditEventRecord {
    pub id: Uuid,
    pub event_type: String,
    pub user_id: Option<Uuid>,
    #[schema(value_type = Option<String>)]
    pub ip_address: Option<IpNetwork>,
    pub event_data: serde_json::Value,
    pub created_at: Option<chrono::DateTime<Utc>>,
}
