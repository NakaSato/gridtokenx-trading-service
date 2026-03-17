use anyhow::{anyhow, Result};
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::services::erc::types::{CertificateTransfer, ErcCertificate};
use crate::infra::blockchain::BlockchainService;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Keypair,
};

/// Manager for transferring ERC certificates
#[derive(Clone, Debug)]
pub struct CertificateTransferManager {
    db_pool: PgPool,
    blockchain_service: BlockchainService,
}

impl CertificateTransferManager {
    pub fn new(db_pool: PgPool, blockchain_service: BlockchainService) -> Self {
        Self {
            db_pool,
            blockchain_service,
        }
    }

    /// Transfer certificate on-chain
    pub async fn transfer_certificate_on_chain(
        &self,
        certificate_id: &str,
        owner: &Keypair, // Owner keypair
        to_owner_pubkey: &Pubkey,
        _governance_program_id: &Pubkey,
    ) -> Result<String> {
        let signature = self
            .blockchain_service
            .transfer_erc(certificate_id, owner, to_owner_pubkey)
            .await?;

        Ok(signature.to_string())
    }

    /// Transfer a certificate to another wallet
    pub async fn transfer_certificate(
        &self,
        certificate_uuid: Uuid,
        from_wallet: &str,
        to_wallet: &str,
        to_user_id: Uuid,
        tx_signature: &str,
    ) -> Result<(ErcCertificate, CertificateTransfer)> {
        let mut tx = self
            .db_pool
            .begin()
            .await
            .map_err(|e| anyhow!("Failed to start transaction: {}", e))?;

        // Update certificate wallet and status
        let certificate = sqlx::query_as::<_, ErcCertificate>(
            r#"
            UPDATE erc_certificates
            SET 
                wallet_address = $2,
                user_id = $3,
                updated_at = NOW()
            WHERE id = $1
            RETURNING
                id, certificate_id, user_id, wallet_address,
                kwh_amount, issue_date, expiry_date,
                issuer_wallet, status,
                blockchain_tx_signature, metadata, settlement_id,
                created_at, updated_at
            "#
        )
        .bind(certificate_uuid)
        .bind(to_wallet)
        .bind(to_user_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| anyhow!("Failed to update certificate: {}", e))?;

        // Record transfer
        let transfer = sqlx::query_as::<_, CertificateTransfer>(
            r#"
            INSERT INTO erc_certificate_transfers (
                id, certificate_id, from_wallet, to_wallet, 
                transfer_date, blockchain_tx_signature, created_at
            )
            VALUES ($1, $2, $3, $4, NOW(), $5, NOW())
            RETURNING *
            "#
        )
        .bind(Uuid::new_v4())
        .bind(certificate.id)
        .bind(from_wallet)
        .bind(to_wallet)
        .bind(tx_signature)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| anyhow!("Failed to record transfer: {}", e))?;

        tx.commit()
            .await
            .map_err(|e| anyhow!("Failed to commit transfer: {}", e))?;

        Ok((certificate, transfer))
    }
}
