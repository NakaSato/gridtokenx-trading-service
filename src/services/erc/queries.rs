use anyhow::{anyhow, Result};
use sqlx::PgPool;
use tracing::instrument;
use uuid::Uuid;

use crate::infra::blockchain::BlockchainService;
use crate::services::erc::types::{CertificateStats, ErcCertificate};

/// Manager for Energy Renewable Certificate queries
#[derive(Clone, Debug)]
pub struct ErcQueryManager {
    db_pool: PgPool,
    #[allow(dead_code)]
    blockchain_service: BlockchainService,
}

impl ErcQueryManager {
    /// Create a new ERC query manager
    pub fn new(db_pool: PgPool, blockchain_service: BlockchainService) -> Self {
        Self {
            db_pool,
            blockchain_service,
        }
    }

    #[instrument(skip(self))]
    pub async fn get_user_stats(&self, user_id: Uuid) -> Result<CertificateStats> {
        let total_certificates: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM erc_certificates 
            WHERE user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.db_pool)
        .await?;

        let _active_certificates: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM erc_certificates 
            WHERE user_id = $1 AND status = 'Active'
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.db_pool)
        .await?;

        let _retired_certificates: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM erc_certificates 
            WHERE user_id = $1 AND status = 'Retired'
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.db_pool)
        .await?;

        let total_energy: Option<rust_decimal::Decimal> = sqlx::query_scalar(
            r#"
            SELECT SUM(kwh_amount)
            FROM erc_certificates 
            WHERE user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.db_pool)
        .await?;

        let total_energy = total_energy.unwrap_or(rust_decimal::Decimal::ZERO);

        Ok(CertificateStats {
            total_certificates,
            active_kwh: rust_decimal::Decimal::ZERO, // Need to fetch active kwh?
            retired_kwh: rust_decimal::Decimal::ZERO, // Need to fetch retired kwh?
            total_kwh: total_energy,
        })
    }

    #[instrument(skip(self))]
    pub async fn get_certificate_by_id(&self, certificate_id: &str) -> Result<ErcCertificate> {
        let cert = sqlx::query_as::<_, ErcCertificate>(
            r#"
            SELECT
                id, certificate_id,
                user_id,
                wallet_address,
                kwh_amount,
                issue_date,
                expiry_date,
                issuer_wallet,
                status,
                blockchain_tx_signature,
                metadata,
                settlement_id,
                created_at,
                updated_at
            FROM erc_certificates
            WHERE certificate_id = $1
            "#,
        )
        .bind(certificate_id)
        .fetch_optional(&self.db_pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Certificate not found"))?;

        Ok(cert)
    }

    #[instrument(skip(self))]
    pub async fn get_my_certificates(&self, user_id: Uuid) -> Result<Vec<ErcCertificate>> {
        let certificates = sqlx::query_as::<_, ErcCertificate>(
            r#"
            SELECT
                id, certificate_id, user_id, wallet_address,
                kwh_amount, issue_date, expiry_date,
                issuer_wallet, status,
                blockchain_tx_signature, metadata, settlement_id,
                created_at,
                updated_at
            FROM erc_certificates
            WHERE user_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.db_pool)
        .await
        .map_err(|e| anyhow!("Failed to fetch user certificates: {}", e))?;

        Ok(certificates)
    }

    /// Get certificates by user ID with pagination and filtering
    #[instrument(skip(self))]
    pub async fn get_user_certificates(
        &self,
        user_id: Uuid,
        limit: i64,
        offset: i64,
        sort_by: &str,
        sort_order: &str,
        status_filter: Option<&str>,
    ) -> Result<Vec<ErcCertificate>> {
        // Need to construct dynamic query safely or use conditional
        let query = if let Some(_status) = status_filter {
            format!(
                r#"
                SELECT
                    id, certificate_id, user_id, wallet_address,
                    kwh_amount, issue_date, expiry_date,
                    issuer_wallet, status,
                    blockchain_tx_signature, metadata, settlement_id,
                    created_at, updated_at
                FROM erc_certificates
                WHERE user_id = $1 AND status = $2
                ORDER BY {} {}
                LIMIT $3 OFFSET $4
                "#,
                sort_by, sort_order
            )
        } else {
            format!(
                r#"
                SELECT
                    id, certificate_id, user_id, wallet_address,
                    kwh_amount, issue_date, expiry_date,
                    issuer_wallet, status,
                    blockchain_tx_signature, metadata, settlement_id,
                    created_at, updated_at
                FROM erc_certificates
                WHERE user_id = $1
                ORDER BY {} {}
                LIMIT $2 OFFSET $3
                "#,
                sort_by, sort_order
            )
        };

        let certificates = if let Some(status) = status_filter {
            sqlx::query_as::<_, ErcCertificate>(&query)
                .bind(user_id)
                .bind(status)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.db_pool)
                .await
                .map_err(|e| anyhow!("Failed to fetch user certificates: {}", e))?
        } else {
            sqlx::query_as::<_, ErcCertificate>(&query)
                .bind(user_id)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.db_pool)
                .await
                .map_err(|e| anyhow!("Failed to fetch user certificates: {}", e))?
        };

        Ok(certificates)
    }

    /// Count total certificates for a user
    #[instrument(skip(self))]
    pub async fn count_user_certificates(
        &self,
        user_id: Uuid,
        status_filter: Option<&str>,
    ) -> Result<i64> {
        let count = if let Some(status) = status_filter {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM erc_certificates WHERE user_id = $1 AND status = $2",
            )
            .bind(user_id)
            .bind(status)
            .fetch_one(&self.db_pool)
            .await
            .map_err(|e| anyhow!("Failed to count user certificates: {}", e))?
        } else {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM erc_certificates WHERE user_id = $1")
                .bind(user_id)
                .fetch_one(&self.db_pool)
                .await
                .map_err(|e| anyhow!("Failed to count user certificates: {}", e))?
        };

        Ok(count)
    }

    #[instrument(skip(self))]
    pub async fn get_certificates_by_wallet(
        &self,
        wallet_address: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ErcCertificate>> {
        let certificates = sqlx::query_as::<_, ErcCertificate>(
            r#"
            SELECT
                id, certificate_id,
                user_id,
                wallet_address,
                kwh_amount,
                issue_date,
                expiry_date,
                issuer_wallet,
                status,
                blockchain_tx_signature,
                metadata,
                settlement_id,
                created_at,
                updated_at
            FROM erc_certificates
            WHERE wallet_address = $1
            ORDER BY issue_date DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(wallet_address)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.db_pool)
        .await
        .map_err(|e| anyhow!("Failed to fetch certificates: {}", e))?;

        Ok(certificates)
    }

    /// Find valid active certificates for a seller that can cover a settlement amount
    #[instrument(skip(self))]
    pub async fn find_settlement_certificates(
        &self,
        user_id: Uuid,
        min_kwh_amount: rust_decimal::Decimal,
    ) -> Result<Vec<ErcCertificate>> {
        let certificates = sqlx::query_as::<_, ErcCertificate>(
            r#"
            SELECT
                id, certificate_id,
                user_id,
                wallet_address,
                kwh_amount,
                issue_date,
                expiry_date,
                issuer_wallet,
                status,
                blockchain_tx_signature,
                metadata,
                settlement_id,
                created_at,
                updated_at
            FROM erc_certificates
            WHERE user_id = $1 
              AND status = 'active'
              AND kwh_amount >= $2
            ORDER BY kwh_amount ASC, issue_date ASC
            LIMIT 5
            "#,
        )
        .bind(user_id)
        .bind(min_kwh_amount)
        .fetch_all(&self.db_pool)
        .await
        .map_err(|e| anyhow!("Failed to find settlement certificates: {}", e))?;

        Ok(certificates)
    }
}
