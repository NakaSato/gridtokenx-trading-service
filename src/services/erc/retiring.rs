use anyhow::{anyhow, Result};
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

use crate::services::erc::types::ErcCertificate;

use solana_sdk::{pubkey::Pubkey, signature::Keypair};

use crate::infra::blockchain::BlockchainService;

/// Manager for retiring ERC certificates
#[derive(Clone, Debug)]
pub struct CertificateRetiring {
    db_pool: PgPool,
    blockchain_service: BlockchainService,
}

impl CertificateRetiring {
    pub fn new(db_pool: PgPool, blockchain_service: BlockchainService) -> Self {
        Self {
            db_pool,
            blockchain_service,
        }
    }

    /// Retire certificate on-chain (Revoke)
    pub async fn retire_certificate_on_chain(
        &self,
        certificate_id: &str,
        authority: &Keypair,
        _governance_program_id: &Pubkey,
    ) -> Result<String> {
        let signature = self
            .blockchain_service
            .revoke_erc(certificate_id, "Retired via API Gateway", authority)
            .await?;

        Ok(signature.to_string())
    }

    /// Retire a certificate
    pub async fn retire_certificate(&self, certificate_uuid: Uuid) -> Result<ErcCertificate> {
        let certificate = sqlx::query_as::<_, ErcCertificate>(
            r#"
            UPDATE erc_certificates
            SET 
                status = 'retired',
                updated_at = NOW()
            WHERE id = $1
            RETURNING
                id, certificate_id, user_id, wallet_address,
                kwh_amount, issue_date, expiry_date,
                issuer_wallet, status,
                blockchain_tx_signature, metadata, settlement_id,
                created_at, updated_at
            "#,
        )
        .bind(certificate_uuid)
        .fetch_one(&self.db_pool)
        .await
        .map_err(|e| {
            if let sqlx::Error::RowNotFound = e {
                anyhow!("Certificate not found or already retired")
            } else {
                anyhow!("Failed to retire certificate: {}", e)
            }
        })?;

        info!("Certificate {} retired", certificate.certificate_id);

        Ok(certificate)
    }
}
