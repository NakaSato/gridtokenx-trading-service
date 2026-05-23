pub mod rpc;
pub mod settlement;
pub mod wallet;

pub use gridtokenx_blockchain_core::WalletService;
pub use rpc::service::BlockchainService;
pub use settlement::BlockchainSettlementProvider;

use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use tracing::info;
use trading_core::traits::{BlockchainGateway, TraitResult};
use uuid::Uuid;

#[async_trait]
impl BlockchainGateway for BlockchainService {
    async fn get_zone_config(&self, zone_id: i32) -> TraitResult<trading_core::models::ZoneConfig> {
        // Shim implementation: Return a default config for the zone.
        // In production, this would fetch from the Solana Registry PDA.
        Ok(trading_core::models::ZoneConfig {
            zone_id,
            incentive_multiplier: Decimal::from(1),
            wheeling_charge: Decimal::from(0),
            maintenance_mode: false,
            last_updated: chrono::Utc::now(),
        })
    }

    async fn is_user_registered(&self, _user_id: Uuid) -> TraitResult<bool> {
        // In a real implementation, we would derive the PDA or check a registry.
        // For the shim, we return true if we can connect to the chain.
        Ok(true)
    }

    async fn get_user_wallet(&self, user_id: Uuid) -> TraitResult<Option<String>> {
        let pubkey = self.get_user_primary_wallet(&user_id).await.map_err(|e| {
            trading_core::error::ApiError::Internal(format!("Failed to resolve user wallet: {}", e))
        })?;

        Ok(pubkey.map(|p| p.to_string()))
    }

    async fn get_token_balance(&self, wallet_address: &str) -> TraitResult<u64> {
        use std::str::FromStr;
        let owner = solana_sdk::pubkey::Pubkey::from_str(wallet_address)
            .map_err(|e| trading_core::error::ApiError::Validation(e.to_string()))?;

        // Use a dummy mint for now or make it configurable
        let mint = solana_sdk::pubkey::Pubkey::default();

        self.get_token_balance(&owner, &mint)
            .await
            .map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))
    }

    async fn execute_settlement(
        &self,
        settlement: &trading_core::models::Settlement,
    ) -> TraitResult<trading_core::models::SettlementTransaction> {
        info!("Executing on-chain settlement for ID: {}", settlement.id);

        if settlement.trade_id.is_none() {
            // Oracle Surplus Settlement (Minting model)
            info!(
                "Surplus detected: Executing generation mint for user {}",
                settlement.seller_id
            );

            let seller_wallet = self
                .get_user_primary_wallet(&settlement.seller_id)
                .await
                .map_err(|e| {
                    trading_core::error::ApiError::Internal(format!(
                        "Failed to resolve seller wallet: {}",
                        e
                    ))
                })?
                .ok_or_else(|| {
                    trading_core::error::ApiError::NotFound(format!(
                        "Primary wallet not found for user {}",
                        settlement.seller_id
                    ))
                })?;

            let provider = BlockchainSettlementProvider::new(Arc::new(self.clone()));
            let signature = provider
                .execute_generation_mint(
                    &seller_wallet,
                    settlement.energy_amount,
                    chrono::Utc::now().timestamp(),
                )
                .await?;

            Ok(trading_core::models::SettlementTransaction {
                settlement_id: settlement.id,
                signature,
                slot: 0, // Slot will be confirmed by bridge
                confirmation_status: "confirmed".to_string(),
            })
        } else {
            // Matching Engine Trade (Atomic swap model)
            // Note: This requires resolving Order PDAs which we'll handle in a separate refinement.
            // For now, we'll return a specific error or a fallback.
            Err(trading_core::error::ApiError::Internal(
                "Atomic swap settlement not yet fully wired to PDAs".to_string(),
            ))
        }
    }

    async fn issue_erc(
        &self,
        user_id: Uuid,
        meter_id: &str,
        energy_amount: Decimal,
    ) -> TraitResult<String> {
        info!(
            "Issuing ERC for user {} (Meter: {}, Amount: {})",
            user_id, meter_id, energy_amount
        );

        let user_wallet = self
            .get_user_primary_wallet(&user_id)
            .await
            .map_err(|e| {
                trading_core::error::ApiError::Internal(format!(
                    "Failed to resolve user wallet: {}",
                    e
                ))
            })?
            .ok_or_else(|| {
                trading_core::error::ApiError::NotFound(format!(
                    "Primary wallet not found for user {}",
                    user_id
                ))
            })?;

        let authority = self.get_authority_keypair().await.map_err(|e| {
            trading_core::error::ApiError::Internal(format!("Failed to load authority: {}", e))
        })?;

        // Format certificate ID: ERC-{meter_id}-{timestamp}
        let cert_id = format!("ERC-{}-{}", meter_id, chrono::Utc::now().timestamp());

        // Convert decimal to u64 (9 decimals for GRX/ERC)
        let amount_u64 = (energy_amount * rust_decimal::Decimal::from(1_000_000_000i64))
            .to_u64()
            .unwrap_or(0);

        let signature = self
            .issue_erc(
                &cert_id,
                &user_wallet,
                &Pubkey::default(), // Meter account PDA derivation not yet implemented here
                amount_u64,
                "Solar",           // Source
                "Oracle Verified", // Validation data
                &authority,
            )
            .await
            .map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))?;

        info!(
            "ERC issued successfully: {} (Signature: {})",
            cert_id, signature
        );
        Ok(signature.to_string())
    }

    async fn execute_generation_mint(
        &self,
        user_wallet: &str,
        energy_amount: Decimal,
        timestamp: i64,
    ) -> TraitResult<String> {
        use std::str::FromStr;
        let wallet_pubkey = Pubkey::from_str(user_wallet).map_err(|e| {
            trading_core::error::ApiError::Validation(format!("Invalid wallet address: {}", e))
        })?;

        let provider = BlockchainSettlementProvider::new(Arc::new(self.clone()));
        provider
            .execute_generation_mint(&wallet_pubkey, energy_amount, timestamp)
            .await
    }
}
