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
use solana_sdk::signature::Signer;
use std::sync::Arc;
use tracing::{info, warn};
use trading_core::traits::{BlockchainGateway, TraitResult};
use uuid::Uuid;

#[async_trait]
impl BlockchainGateway for BlockchainService {
    async fn get_zone_config(&self, zone_id: i32) -> TraitResult<trading_core::models::ZoneConfig> {
        let pda = self.instruction_builder().get_zone_config_pda(zone_id as u32)
            .map_err(|e| trading_core::error::ApiError::Internal(format!("PDA derivation error: {}", e)))?;

        match self.get_account_data(&pda).await {
            Ok(data) => {
                if data.len() < 8 + 136 {
                    return Err(trading_core::error::ApiError::Internal("Invalid ZoneConfig account data size".to_string()));
                }
                
                // Skip Anchor discriminator (8 bytes)
                let multiplier_bps = u64::from_le_bytes(data[16..24].try_into().unwrap_or([0; 8]));
                let wheeling_bps = u64::from_le_bytes(data[24..32].try_into().unwrap_or([0; 8]));
                let maintenance_mode = data[32] != 0;
                let last_updated_ts = i64::from_le_bytes(data[72..80].try_into().unwrap_or([0; 8]));

                Ok(trading_core::models::ZoneConfig {
                    zone_id,
                    incentive_multiplier: Decimal::from(multiplier_bps) / Decimal::from(10_000),
                    wheeling_charge: Decimal::from(wheeling_bps) / Decimal::from(10_000),
                    maintenance_mode,
                    last_updated: chrono::DateTime::from_timestamp(last_updated_ts, 0).unwrap_or_else(|| gridtokenx_telemetry::time::now()),
                })
            }
            Err(e) => {
                warn!("⚠️ Failed to fetch on-chain ZoneConfig for {}: {}. Falling back to default.", zone_id, e);
                Ok(trading_core::models::ZoneConfig {
                    zone_id,
                    incentive_multiplier: Decimal::from(1),
                    wheeling_charge: Decimal::from(0),
                    maintenance_mode: false,
                    last_updated: gridtokenx_telemetry::time::now(),
                })
            }
        }
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

    async fn place_order_on_chain(
        &self,
        user_id: Uuid,
        is_buy: bool,
        energy_amount: rust_decimal::Decimal,
        price_per_kwh: rust_decimal::Decimal,
        zone_id: i32,
        order_id_seed: u64,
    ) -> TraitResult<(String, String)> {
        use rust_decimal::prelude::ToPrimitive;
        use trading_core::error::ApiError;

        let wallet = self
            .get_user_primary_wallet(&user_id)
            .await
            .map_err(|e| ApiError::Internal(format!("resolve wallet: {}", e)))?
            .ok_or_else(|| ApiError::Validation(format!("no on-chain wallet for user {user_id}")))?;

        // GRID energy = 9-dec atomic; currency price = 6-dec atomic.
        let energy_atomic = (energy_amount * rust_decimal::Decimal::from(1_000_000_000i64))
            .trunc()
            .to_u64()
            .ok_or_else(|| ApiError::Validation("energy amount out of range".to_string()))?;
        let price_atomic = (price_per_kwh * rust_decimal::Decimal::from(1_000_000i64))
            .trunc()
            .to_u64()
            .ok_or_else(|| ApiError::Validation("price out of range".to_string()))?;

        let (sig, pda) = self
            .place_order_on_chain(&wallet, order_id_seed, is_buy, energy_atomic, price_atomic, zone_id as u32)
            .await
            .map_err(|e| ApiError::Internal(format!("place_order_on_chain: {}", e)))?;
        Ok((sig.to_string(), pda.to_string()))
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

    async fn execute_batched_settlements(
        &self,
        settlements: Vec<trading_core::models::Settlement>,
    ) -> TraitResult<Vec<trading_core::models::SettlementTransaction>> {
        if settlements.is_empty() {
            return Ok(Vec::new());
        }

        info!("Executing batched on-chain settlements for {} records", settlements.len());

        let mut results = Vec::new();

        // Process matching-engine trade settlements as on-chain atomic
        // energy↔currency swaps. (Generation issuance is no longer minted here —
        // it is owned by the metering/issuance service; trading only settles
        // trades.) A settlement carrying no resolvable order PDA / wallet is
        // skipped and left for retry rather than failing the whole batch.
        let trade_settlements = settlements;
        if !trade_settlements.is_empty() {
            if !self.trade_settlement_enabled {
                // Disabled until the swap path is validated against a live
                // validator. Leave these unminted so the caller releases them
                // for retry rather than dropping them silently.
                warn!(
                    "TRADE_SETTLEMENT_ENABLED=false: {} trade settlement(s) left for retry",
                    trade_settlements.len()
                );
            } else {
                let provider = BlockchainSettlementProvider::new(Arc::new(self.clone()));

                // Resolve on-chain order PDAs + party wallets. A settlement
                // missing any of them is skipped (not added to inputs) so it is
                // left for retry instead of failing the whole batch.
                let mut inputs = Vec::new();
                for settlement in &trade_settlements {
                    let buy_pda = match self.get_order_pda(&settlement.buy_order_id).await {
                        Ok(Some(p)) => p,
                        _ => {
                            warn!(
                                "Settlement {}: buy order {} has no on-chain PDA; skipping",
                                settlement.id, settlement.buy_order_id
                            );
                            continue;
                        }
                    };
                    let sell_pda = match self.get_order_pda(&settlement.sell_order_id).await {
                        Ok(Some(p)) => p,
                        _ => {
                            warn!(
                                "Settlement {}: sell order {} has no on-chain PDA; skipping",
                                settlement.id, settlement.sell_order_id
                            );
                            continue;
                        }
                    };
                    let buyer_pubkey = match self.get_user_primary_wallet(&settlement.buyer_id).await
                    {
                        Ok(Some(p)) => p,
                        _ => {
                            warn!(
                                "Settlement {}: buyer {} has no primary wallet; skipping",
                                settlement.id, settlement.buyer_id
                            );
                            continue;
                        }
                    };
                    let seller_pubkey =
                        match self.get_user_primary_wallet(&settlement.seller_id).await {
                            Ok(Some(p)) => p,
                            _ => {
                                warn!(
                                    "Settlement {}: seller {} has no primary wallet; skipping",
                                    settlement.id, settlement.seller_id
                                );
                                continue;
                            }
                        };

                    inputs.push((settlement, buy_pda, sell_pda, buyer_pubkey, seller_pubkey));
                }

                if !inputs.is_empty() {
                    let trade_results = provider.execute_batched_settlements(inputs, 0).await?;
                    results.extend(trade_results);
                }
            }
        }

        Ok(results)
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

        // Format certificate ID: ERC-{meter_id}-{timestamp}
        let cert_id = format!("ERC-{}-{}", meter_id, gridtokenx_telemetry::time::now().timestamp());

        // Convert decimal to u64 (9 decimals for GRX/ERC)
        let amount_u64 = (energy_amount * rust_decimal::Decimal::from(1_000_000_000i64))
            .to_u64()
            .unwrap_or(0);

        // Sovereign path: Trading holds only the owner Pubkey; Chain Bridge signs.
        let signature = self
            .issue_erc(
                &cert_id,
                &user_wallet,
                &Pubkey::default(), // Meter account PDA derivation not yet implemented here
                amount_u64,
                "Solar",           // Source
                "Oracle Verified", // Validation data
            )
            .await
            .map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))?;

        info!(
            "ERC issued successfully: {} (Signature: {})",
            cert_id, signature
        );
        Ok(signature.to_string())
    }

    async fn sync_total_supply(&self) -> TraitResult<String> {
        info!("Syncing total supply for Energy Token...");
        let authority = self.get_authority_keypair().await.map_err(|e| {
            trading_core::error::ApiError::Internal(format!("Failed to load authority: {}", e))
        })?;

        let signature = self
            .core
            .sync_total_supply(&authority as &(dyn Signer + Send + Sync))
            .await
            .map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))?;

        info!("Total supply synced successfully: {}", signature);
        Ok(signature.to_string())
    }

    async fn execute_create_order(
        &self,
        user_id: Uuid,
        market_pubkey: &str,
        amount: u64,
        price: u64,
        order_side: &str,
        erc_id: Option<&str>,
        zone_id: u32,
    ) -> TraitResult<(String, String, u64)> {
        let signer = self.get_custodial_signer(user_id).await.map_err(|e| {
            trading_core::error::ApiError::Internal(format!("Failed to get custodial signer: {}", e))
        })?;

        let (sig, pda, index) = self
            .execute_create_order(
                &signer,
                market_pubkey,
                amount,
                price,
                order_side,
                erc_id,
                zone_id,
            )
            .await
            .map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))?;

        Ok((sig.to_string(), pda, index))
    }
}
