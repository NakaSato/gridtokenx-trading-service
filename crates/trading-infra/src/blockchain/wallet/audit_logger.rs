use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use std::net::IpAddr;
use tracing::{error, info};
use uuid::Uuid;

/// Service for auditing wallet-related operations
#[derive(Clone)]
pub struct WalletAuditLogger {
    db: PgPool,
}

impl WalletAuditLogger {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }

    /// Log a wallet decryption operation
    pub async fn log_decryption(
        &self,
        user_id: Uuid,
        operation: &str,
        success: bool,
        ip_address: Option<IpAddr>,
        user_agent: Option<String>,
        error: Option<String>,
    ) -> Result<()> {
        self.log_operation(
            user_id,
            &format!("decrypt_{}", operation),
            success,
            ip_address,
            user_agent,
            error,
            None,
        )
        .await
    }

    /// Log a wallet export operation
    pub async fn log_export(
        &self,
        user_id: Uuid,
        ip_address: Option<IpAddr>,
        user_agent: Option<String>,
    ) -> Result<()> {
        info!("🔐 Wallet export requested for user: {}", user_id);
        self.log_operation(
            user_id,
            "export_wallet",
            true,
            ip_address,
            user_agent,
            None,
            Some(json!({
                "warning": "User exported private key"
            })),
        )
        .await
    }

    /// Log a key rotation operation
    pub async fn log_key_rotation(
        &self,
        user_id: Uuid,
        old_version: i32,
        new_version: i32,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        self.log_operation(
            user_id,
            "rotate_key",
            success,
            None,
            None,
            error,
            Some(json!({
                "old_version": old_version,
                "new_version": new_version
            })),
        )
        .await
    }

    /// Log a wallet creation operation
    pub async fn log_wallet_creation(
        &self,
        user_id: Uuid,
        wallet_address: &str,
        ip_address: Option<IpAddr>,
        user_agent: Option<String>,
    ) -> Result<()> {
        self.log_operation(
            user_id,
            "create_wallet",
            true,
            ip_address,
            user_agent,
            None,
            Some(json!({
                "wallet_address": wallet_address
            })),
        )
        .await
    }

    /// Generic operation logger
    async fn log_operation(
        &self,
        user_id: Uuid,
        operation: &str,
        success: bool,
        ip_address: Option<IpAddr>,
        user_agent: Option<String>,
        error_message: Option<String>,
        metadata: Option<serde_json::Value>,
    ) -> Result<()> {
        // TODO(db-split): cross-domain WRITE to IAM `wallet_audit_log`. Phase 1 keeps this SQL;
        // at cutover repoint to the Trading-owned `trading_wallet_audit_log`
        // (migrations/20260715000001_trading_phase1_local_models.sql + docs/db-split-phase1.md).
        let result = sqlx::query(
            r#"
            INSERT INTO wallet_audit_log
                (user_id, operation, success, ip_address, user_agent, error_message, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(user_id)
        .bind(operation)
        .bind(success)
        .bind(ip_address.map(|ip| ip.to_string()))
        .bind(&user_agent)
        .bind(&error_message)
        .bind(&metadata)
        .execute(&self.db)
        .await;

        match result {
            Ok(_) => {
                if !success {
                    error!(
                        "❌ Wallet operation failed - user: {}, operation: {}, error: {:?}",
                        user_id, operation, error_message
                    );
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to write audit log: {}", e);
                Err(e.into())
            }
        }
    }

    /// Get audit log for a user
    pub async fn get_user_audit_log(
        &self,
        user_id: Uuid,
        limit: i64,
    ) -> Result<Vec<WalletAuditEntry>> {
        let entries = sqlx::query_as::<_, WalletAuditEntry>(
            r#"
            SELECT 
                id,
                user_id,
                operation,
                success,
                ip_address,
                user_agent,
                error_message,
                metadata,
                created_at
            FROM wallet_audit_log
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.db)
        .await?;

        Ok(entries)
    }
}

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
pub struct WalletAuditEntry {
    pub id: Uuid,
    pub user_id: Uuid,
    pub operation: String,
    pub success: bool,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub error_message: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}
