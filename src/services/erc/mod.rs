pub mod issuance;
pub mod queries;
pub mod retiring;
pub mod transfer;
pub mod types;

pub use types::*;

use anyhow::Result;
use chrono::Utc;
use rust_decimal::prelude::ToPrimitive;
use solana_sdk::signature::Keypair;
use sqlx::PgPool;
use tracing::{error, info, instrument};
use uuid::Uuid;
use std::str::FromStr;

use self::issuance::AggregatedIssuance;
use self::queries::ErcQueryManager;
use self::retiring::CertificateRetiring;
use self::transfer::CertificateTransferManager;
use crate::infra::blockchain::BlockchainService;
use crate::infra::events::EventBus;
use crate::domain::events::{Event, ErcIssuedPayload};

/// Service for managing Energy Renewable Certificates
#[derive(Clone, Debug)]
pub struct ErcService {
    db_pool: PgPool,
    blockchain_service: BlockchainService,
    event_bus: EventBus,
    issuance_manager: AggregatedIssuance,
    retiring_manager: CertificateRetiring,
    transfer_manager: CertificateTransferManager,
    query_manager: ErcQueryManager,
}

impl ErcService {
    /// Create a new ERC service
    pub fn new(db_pool: PgPool, blockchain_service: BlockchainService, event_bus: EventBus) -> Self {
        let issuance_manager = AggregatedIssuance::new(db_pool.clone(), blockchain_service.clone());
        let retiring_manager =
            CertificateRetiring::new(db_pool.clone(), blockchain_service.clone());
        let transfer_manager =
            CertificateTransferManager::new(db_pool.clone(), blockchain_service.clone());
        let query_manager = ErcQueryManager::new(db_pool.clone(), blockchain_service.clone());

        Self {
            db_pool,
            blockchain_service,
            event_bus,
            issuance_manager,
            retiring_manager,
            transfer_manager,
            query_manager,
        }
    }

    /// Issue a new ERC certificate
    #[instrument(skip(self, request, issuer_wallet))]
    pub async fn issue_certificate(
        &self,
        user_id: Uuid,
        issuer_wallet: &str,
        request: IssueErcRequest,
        settlement_id: Option<Uuid>,
    ) -> Result<ErcCertificate> {
        info!("Issuing certificate for user {}", user_id);

        // 1. Generate certificate ID
        let certificate_id = self.issuance_manager.generate_certificate_id()?;

        // 2. Prepare Metadata
        let renewable_source = request.metadata.as_ref()
            .and_then(|m| m.get("renewable_source"))
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown").to_string();

        let validation_data = request.metadata.as_ref()
            .and_then(|m| m.get("validation_data"))
            .and_then(|v| v.as_str())
            .unwrap_or("").to_string();

        let energy_amount_f64 = request.kwh_amount.to_f64().unwrap_or(0.0);
        let metadata_struct = self.issuance_manager.create_certificate_metadata(
            &certificate_id, energy_amount_f64, &renewable_source, issuer_wallet, Utc::now(), request.expiry_date, &validation_data,
        )?;
        let metadata_json = serde_json::to_value(&metadata_struct)?;

        // 3. Store in DB
        let certificate = sqlx::query_as::<_, ErcCertificate>(
            r#"
            INSERT INTO erc_certificates (
                id, certificate_id, user_id, wallet_address,
                energy_amount, kwh_amount, issue_date, issuance_date, expiry_date,
                issuer_wallet, status, certificate_type, metadata, settlement_id
            )
            VALUES ($1, $2, $3, $4, $5, $5, $6, $6, $7, $8, 'active', 'REC', $9, $10)
            RETURNING
                id, certificate_id, user_id, wallet_address,
                kwh_amount, issue_date, expiry_date,
                issuer_wallet, status,
                blockchain_tx_signature, metadata, settlement_id,
                created_at, updated_at
            "#
        )
        .bind(Uuid::new_v4())
        .bind(certificate_id)
        .bind(user_id)
        .bind(request.wallet_address)
        .bind(request.kwh_amount)
        .bind(Utc::now())
        .bind(request.expiry_date)
        .bind(issuer_wallet)
        .bind(metadata_json)
        .bind(settlement_id)
        .fetch_one(&self.db_pool)
        .await?;

        // 4. Trigger Async On-Chain Issuance (O(1) API Latency)
        let self_clone = self.clone();
        let cert_clone = certificate.clone();
        let cert_id_log = cert_clone.certificate_id.clone();
        let meter_id = request.meter_id.clone().unwrap_or_else(|| "SYSTEM_AGGREGATED".to_string());
        
        tokio::spawn(async move {
            if let Err(e) = self_clone.process_onchain_issuance_async(
                cert_clone, meter_id, renewable_source, validation_data
            ).await {
                error!("❌ Background ERC issuance failed for {}: {}", cert_id_log, e);
            }
        });

        Ok(certificate)
    }

    /// Background task for on-chain ERC registration
    async fn process_onchain_issuance_async(
        &self,
        certificate: ErcCertificate,
        meter_id: String,
        renewable_source: String,
        validation_data: String,
    ) -> Result<()> {
        info!("🔗 Registering ERC {} on-chain...", certificate.certificate_id);
        
        let authority = self.blockchain_service.get_authority_keypair().await?;
        let user_wallet = solana_sdk::pubkey::Pubkey::from_str(&certificate.wallet_address)?;
        let governance_program_id = self.blockchain_service.registry_program_id()?; // Simplified for refactor

        // 1. Submit to Blockchain
        let signature = self.issue_certificate_on_chain(
            &certificate.certificate_id,
            &user_wallet,
            &meter_id,
            certificate.kwh_amount.and_then(|d| d.to_f64()).unwrap_or(0.0),
            &renewable_source,
            &validation_data,
            &authority,
            &governance_program_id,
        ).await?;

        // 2. Update DB with Signature
        self.update_certificate_tx_signature(certificate.id, &signature.to_string()).await?;

        // 3. Notify via EventBus
        if let Err(e) = self.event_bus.publish(Event::ErcIssued(ErcIssuedPayload {
            certificate_id: certificate.certificate_id,
            user_id: certificate.user_id.unwrap_or_default(),
            energy_amount: certificate.kwh_amount.unwrap_or_default(),
            renewable_source,
            timestamp: Utc::now(),
        })).await {
            error!("❌ Failed to publish ErcIssued event for {}: {}", certificate.id, e);
        }

        info!("✅ ERC {} successfully registered on-chain", certificate.id);
        Ok(())
    }

    /// Issue ERC certificate on-chain (calls governance program)
    #[instrument(skip(self, authority))]
    pub async fn issue_certificate_on_chain(
        &self,
        certificate_id: &str,
        user_wallet: &solana_sdk::pubkey::Pubkey,
        meter_id: &str,
        energy_amount: f64,
        renewable_source: &str,
        validation_data: &str,
        authority: &Keypair,
        governance_program_id: &solana_sdk::pubkey::Pubkey,
    ) -> Result<solana_sdk::signature::Signature> {
        self.issuance_manager
            .issue_certificate_on_chain(
                certificate_id,
                user_wallet,
                meter_id,
                energy_amount,
                renewable_source,
                validation_data,
                authority,
                governance_program_id,
            )
            .await
    }

    pub async fn validate_certificate_on_chain(
        &self,
        certificate_id: &str,
        governance_program_id: &solana_sdk::pubkey::Pubkey,
    ) -> Result<bool> {
        self.issuance_manager
            .validate_certificate_on_chain(certificate_id, governance_program_id)
            .await
    }

    pub fn create_certificate_metadata(
        &self,
        certificate_id: &str,
        energy_amount: f64,
        renewable_source: &str,
        issuer: &str,
        issue_date: chrono::DateTime<chrono::Utc>,
        expiry_date: Option<chrono::DateTime<chrono::Utc>>,
        validation_data: &str,
    ) -> Result<ErcMetadata> {
        self.issuance_manager.create_certificate_metadata(
            certificate_id,
            energy_amount,
            renewable_source,
            issuer,
            issue_date,
            expiry_date,
            validation_data,
        )
    }

    pub async fn update_certificate_tx_signature(
        &self,
        certificate_uuid: Uuid,
        signature: &str,
    ) -> Result<ErcCertificate> {
        self.issuance_manager
            .update_certificate_signature(certificate_uuid, signature)
            .await
    }

    // --- Transfer ---

    /// Transfer certificate
    #[instrument(skip(self))]
    pub async fn transfer_certificate(
        &self,
        certificate_uuid: Uuid,
        from_wallet: &str,
        to_wallet: &str,
        to_user_id: Uuid,
        tx_signature: &str,
    ) -> Result<(ErcCertificate, CertificateTransfer)> {
        self.transfer_manager
            .transfer_certificate(certificate_uuid, from_wallet, to_wallet, to_user_id, tx_signature)
            .await
    }

    pub async fn transfer_certificate_on_chain(
        &self,
        certificate_id: &str,
        from_keypair: &solana_sdk::signature::Keypair,
        to_owner_pubkey: &solana_sdk::pubkey::Pubkey,
        governance_program_id: &solana_sdk::pubkey::Pubkey,
    ) -> Result<String> {
        self.transfer_manager
            .transfer_certificate_on_chain(
                certificate_id,
                from_keypair,
                to_owner_pubkey,
                governance_program_id,
            )
            .await
    }

    // --- Retiring ---

    /// Retire certificate
    #[instrument(skip(self))]
    pub async fn retire_certificate(&self, certificate_uuid: Uuid) -> Result<ErcCertificate> {
        self.retiring_manager
            .retire_certificate(certificate_uuid)
            .await
    }

    pub async fn retire_certificate_on_chain(
        &self,
        certificate_id: &str,
        authority: &solana_sdk::signature::Keypair,
        governance_program_id: &solana_sdk::pubkey::Pubkey,
    ) -> Result<String> {
        self.retiring_manager
            .retire_certificate_on_chain(certificate_id, authority, governance_program_id)
            .await
    }

    // --- Statistics & Queries (Keep in main service or move if large) ---

    #[instrument(skip(self))]
    pub async fn get_user_stats(&self, user_id: Uuid) -> Result<CertificateStats> {
        self.query_manager.get_user_stats(user_id).await
    }

    #[instrument(skip(self))]
    pub async fn get_certificate_by_id(&self, certificate_id: &str) -> Result<ErcCertificate> {
        self.query_manager
            .get_certificate_by_id(certificate_id)
            .await
    }

    #[instrument(skip(self))]
    pub async fn get_my_certificates(&self, user_id: Uuid) -> Result<Vec<ErcCertificate>> {
        self.query_manager.get_my_certificates(user_id).await
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
        self.query_manager
            .get_user_certificates(user_id, limit, offset, sort_by, sort_order, status_filter)
            .await
    }

    /// Count total certificates for a user
    #[instrument(skip(self))]
    pub async fn count_user_certificates(
        &self,
        user_id: Uuid,
        status_filter: Option<&str>,
    ) -> Result<i64> {
        self.query_manager
            .count_user_certificates(user_id, status_filter)
            .await
    }

    #[instrument(skip(self))]
    pub async fn get_certificates_by_wallet(
        &self,
        wallet_address: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ErcCertificate>> {
        self.query_manager
            .get_certificates_by_wallet(wallet_address, limit, offset)
            .await
    }

    #[instrument(skip(self))]
    pub async fn find_settlement_certificates(
        &self,
        user_id: Uuid,
        min_kwh_amount: rust_decimal::Decimal,
    ) -> Result<Vec<ErcCertificate>> {
        self.query_manager
            .find_settlement_certificates(user_id, min_kwh_amount)
            .await
    }
}
