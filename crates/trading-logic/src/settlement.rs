use rust_decimal::Decimal;
use std::sync::Arc;
use tracing::{error, info, warn};
use trading_core::error::ApiError;
use trading_core::models::{Settlement, SettlementStatus};
use trading_core::traits::{AuditLog, BlockchainGateway, SettlementRepository, TraitResult};
use uuid::Uuid;

/// Service for orchestrating the settlement lifecycle
pub struct SettlementService {
    repo: Arc<dyn SettlementRepository>,
    blockchain: Arc<dyn BlockchainGateway>,
    audit: Arc<dyn AuditLog>,
    platform_user_id: Uuid,
    oracle_feed_in_tariff: rust_decimal::Decimal,
    pub oracle_bridge_public_key: String,
}

impl SettlementService {
    pub fn new(
        repo: Arc<dyn SettlementRepository>,
        blockchain: Arc<dyn BlockchainGateway>,
        audit: Arc<dyn AuditLog>,
        platform_user_id: Uuid,
        oracle_feed_in_tariff: rust_decimal::Decimal,
        oracle_bridge_public_key: String,
    ) -> Self {
        Self {
            repo,
            blockchain,
            audit,
            platform_user_id,
            oracle_feed_in_tariff,
            oracle_bridge_public_key,
        }
    }

    /// Process a pending settlement on-chain
    pub async fn process_settlement(&self, settlement_id: Uuid) -> TraitResult<()> {
        info!("Processing settlement: {}", settlement_id);

        // 1. Fetch settlement from DB
        let settlement = self
            .repo
            .get_settlement(settlement_id)
            .await?
            .ok_or_else(|| ApiError::NotFound(format!("Settlement {} not found", settlement_id)))?;

        if settlement.status != SettlementStatus::Pending {
            warn!(
                "Settlement {} is already in status: {:?}",
                settlement_id, settlement.status
            );
            return Ok(());
        }

        // 2. Update status to Processing
        self.repo
            .update_settlement_status(
                settlement_id,
                &SettlementStatus::Processing.to_string(),
                None,
                None,
            )
            .await?;

        // 3. Execute on blockchain
        let result: TraitResult<trading_core::models::SettlementTransaction> =
            self.blockchain.execute_settlement(&settlement).await;

        match result {
            Ok(tx_result) => {
                info!(
                    "Settlement {} successful on-chain: {}",
                    settlement_id, tx_result.signature
                );

                // 4. Update status to Completed
                self.repo
                    .update_settlement_status(
                        settlement_id,
                        &SettlementStatus::Completed.to_string(),
                        Some(&tx_result.signature),
                        None,
                    )
                    .await?;

                // 5. Audit log
                let _ = self
                    .audit
                    .log_action(
                        settlement.buyer_id,
                        "settlement_completed",
                        &format!(
                            "Settlement {} completed on-chain with signature {}",
                            settlement_id, tx_result.signature
                        ),
                    )
                    .await;

                // 6. If Oracle settlement (no trade_id), trigger ERC issuance
                if settlement.trade_id.is_none() {
                    info!(
                        "Oracle settlement detected for {}. Triggering ERC issuance.",
                        settlement_id
                    );

                    // Meter ID resolution: In a real system we'd have this in the settlement record.
                    // For now, we'll use a placeholder or lookup.
                    // Let's assume meter_id is part of the settlement metadata eventually.
                    let meter_id = "METER-SN-PROSUMER-1";

                    match self
                        .blockchain
                        .issue_erc(settlement.seller_id, meter_id, settlement.energy_amount)
                        .await
                    {
                        Ok(erc_sig) => {
                            info!("ERC issued for settlement {}: {}", settlement_id, erc_sig);
                            // We could update the settlement record with erc_certificate_id here
                        }
                        Err(e) => {
                            error!(
                                "Failed to issue ERC for settlement {}: {}",
                                settlement_id, e
                            );
                        }
                    }
                }

                Ok(())
            }
            Err(e) => {
                error!("Settlement {} failed on-chain: {}", settlement_id, e);

                // 4. Update status to Failed
                self.repo
                    .update_settlement_status(
                        settlement_id,
                        &SettlementStatus::Failed.to_string(),
                        None,
                        Some(&e.to_string()),
                    )
                    .await?;

                // 5. Audit log
                let _ = self
                    .audit
                    .log_action(
                        settlement.buyer_id,
                        "settlement_failed",
                        &format!("Settlement {} failed on-chain: {}", settlement_id, e),
                    )
                    .await;

                Err(e)
            }
        }
    }

    /// Process all pending settlements (Batch)
    pub async fn process_pending_settlements(&self, limit: i64) -> TraitResult<usize> {
        let pending = self.repo.get_pending_settlements(limit).await?;
        let count = pending.len();

        if count > 0 {
            info!("Found {} pending settlements to process", count);
        }

        for settlement in pending {
            if let Err(e) = self.process_settlement(settlement.id).await {
                error!("Error processing settlement {}: {}", settlement.id, e);
            }
        }

        Ok(count)
    }

    /// Process a normalized oracle reading and create a settlement if surplus is detected
    pub async fn process_reading(
        &self,
        payload: trading_core::events::OracleReadingPayload,
    ) -> TraitResult<()> {
        if payload.kwh <= rust_decimal::Decimal::ZERO {
            return Ok(());
        }

        let user_id = match payload.user_id {
            Some(uid) => uid,
            None => {
                warn!(
                    "Ignoring oracle reading for meter {}: No user_id resolved",
                    payload.meter_id
                );
                return Ok(());
            }
        };

        info!(
            "Creating energy settlement for user {} from meter {}: {} kWh",
            user_id, payload.meter_id, payload.kwh
        );

        let mut feed_in_tariff = self.oracle_feed_in_tariff;

        // Island Incentive Multiplier (Microgrid DAO Governance)
        // We fetch the community-governed incentive from the blockchain.
        if let Some(zone_id) = payload.zone_id {
            match self.blockchain.get_zone_config(zone_id).await {
                Ok(config) => {
                    if config.incentive_multiplier > rust_decimal::Decimal::ONE {
                        feed_in_tariff *= config.incentive_multiplier;
                        info!(
                            "🏝️ Community incentive applied: {}x to Feed-in-Tariff for Zone {}",
                            config.incentive_multiplier, zone_id
                        );
                    }
                }
                Err(e) => {
                    // Fallback to static incentive if on-chain config fetch fails (e.g., zone not initialized)
                    if gridtokenx_blockchain_core::island::IslandRegistry::get_island_config(
                        zone_id,
                    )
                    .is_some()
                    {
                        feed_in_tariff *= rust_decimal_macros::dec!(1.15);
                        warn!("⚠️ Failed to fetch dynamic ZoneConfig ({}). Falling back to static island incentive (1.15x).", e);
                    }
                }
            }
        }

        let total_amount = payload.kwh * feed_in_tariff;

        let settlement = Settlement {
            id: Uuid::new_v4(),
            trade_id: None, // Direct oracle settlement doesn't have a trade_id from matching engine
            epoch_id: Uuid::parse_str("12345678-1234-1234-1234-123456789012").unwrap(),
            buyer_id: self.platform_user_id,
            seller_id: user_id,
            buy_order_id: Uuid::nil(),
            sell_order_id: Uuid::nil(),
            energy_amount: payload.kwh,
            price: feed_in_tariff,
            total_amount,
            fee_amount: rust_decimal::Decimal::ZERO,
            net_amount: total_amount,
            status: SettlementStatus::Pending,
            blockchain_tx: None,
            created_at: chrono::Utc::now(),
            confirmed_at: None,
            wheeling_charge: Some(rust_decimal::Decimal::ZERO),
            loss_factor: Some(rust_decimal::Decimal::ONE),
            loss_cost: Some(rust_decimal::Decimal::ZERO),
            effective_energy: Some(payload.kwh),
            buyer_zone_id: payload.zone_id,
            seller_zone_id: payload.zone_id,
            buyer_session_token: None,
            seller_session_token: None,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
        };

        self.repo.insert_settlement(&settlement).await?;

        info!("Settlement {} created for surplus energy", settlement.id);

        Ok(())
    }

    /// Execute a generation mint for verified oracle data.
    pub async fn execute_generation_mint(
        &self,
        user_id: Uuid,
        amount_kwh: Decimal,
        timestamp: i64,
    ) -> TraitResult<String> {
        info!(
            "Executing Generation Mint for user: {} ({} kWh)",
            user_id, amount_kwh
        );

        // 1. Resolve primary wallet
        let wallet = self
            .blockchain
            .get_user_wallet(user_id)
            .await?
            .ok_or_else(|| {
                ApiError::NotFound(format!("Primary wallet not found for user {}", user_id))
            })?;

        // 2. Execute on blockchain
        let tx_sig = self
            .blockchain
            .execute_generation_mint(&wallet, amount_kwh, timestamp)
            .await?;

        // 3. Audit log
        let _ = self
            .audit
            .log_action(
                user_id,
                "generation_mint",
                &format!("Minted {} energy tokens (Tx: {})", amount_kwh, tx_sig),
            )
            .await;

        Ok(tx_sig)
    }
}
