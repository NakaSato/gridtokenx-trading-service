use chrono::Utc;
use std::sync::Arc;
use tracing::{error, info, warn};
use trading_core::error::ApiError;
use trading_core::models::{Settlement, SettlementStatus};
use trading_core::traits::{AuditLog, BlockchainGateway, EventPublisher, SettlementRepository, TraitResult};
use uuid::Uuid;

/// Service for orchestrating the settlement lifecycle
pub struct SettlementService {
    repo: Arc<dyn SettlementRepository>,
    blockchain: Arc<dyn BlockchainGateway>,
    events: Arc<dyn EventPublisher>,
    audit: Arc<dyn AuditLog>,
    platform_user_id: Uuid,
    oracle_feed_in_tariff: rust_decimal::Decimal,
}

impl SettlementService {
    pub fn new(
        repo: Arc<dyn SettlementRepository>,
        blockchain: Arc<dyn BlockchainGateway>,
        events: Arc<dyn EventPublisher>,
        audit: Arc<dyn AuditLog>,
        platform_user_id: Uuid,
        oracle_feed_in_tariff: rust_decimal::Decimal,
    ) -> Self {
        Self {
            repo,
            blockchain,
            events,
            audit,
            platform_user_id,
            oracle_feed_in_tariff,
        }
    }

    pub fn platform_user_id(&self) -> Uuid {
        self.platform_user_id
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

                // 5. Publish SettlementProcessed Event
                let processed_event = trading_core::events::Event::SettlementProcessed(
                    trading_core::events::SettlementProcessedPayload {
                        settlement_id,
                        tx_signature: tx_result.signature.clone(),
                        status: SettlementStatus::Completed.to_string(),
                        timestamp: Utc::now(),
                    },
                );
                let _ = self.events.publish(processed_event).await;

                // 6. Audit log
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

        if count == 0 {
            return Ok(0);
        }

        info!("Found {} pending settlements to process in batch", count);

        // 1. Mark all as Processing
        for settlement in &pending {
            self.repo
                .update_settlement_status(
                    settlement.id,
                    &SettlementStatus::Processing.to_string(),
                    None,
                    None,
                )
                .await?;
        }

        // 2. Execute batched blockchain transaction
        match self.blockchain.execute_batched_settlements(pending.clone()).await {
            Ok(tx_results) => {
                let mut success_count = 0;
                for tx_result in tx_results {
                    info!(
                        "Settlement {} successful on-chain: {}",
                        tx_result.settlement_id, tx_result.signature
                    );

                    // Update status to Completed
                    self.repo
                        .update_settlement_status(
                            tx_result.settlement_id,
                            &SettlementStatus::Completed.to_string(),
                            Some(&tx_result.signature),
                            None,
                        )
                        .await?;
                    
                    // Publish SettlementProcessed Event
                    let processed_event = trading_core::events::Event::SettlementProcessed(
                        trading_core::events::SettlementProcessedPayload {
                            settlement_id: tx_result.settlement_id,
                            tx_signature: tx_result.signature.clone(),
                            status: SettlementStatus::Completed.to_string(),
                            timestamp: Utc::now(),
                        },
                    );
                    let _ = self.events.publish(processed_event).await;

                    success_count += 1;
                }
                Ok(success_count)
            }
            Err(e) => {
                error!("Batch settlement failed: {}", e);
                // Mark all as Failed or revert to Pending for retry
                for settlement in pending {
                    let _ = self.repo
                        .update_settlement_status(
                            settlement.id,
                            &SettlementStatus::Failed.to_string(),
                            None,
                            Some(&e.to_string()),
                        )
                        .await;
                }
                Err(e)
            }
        }
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
                    // If on-chain config fetch fails, we proceed with base tariff.
                    warn!("⚠️ Failed to fetch dynamic ZoneConfig for Zone {}: {}. Using base Feed-in-Tariff.", zone_id, e);
                }
            }
        }

        let total_amount = payload.kwh * feed_in_tariff;

        // Stamp the live market epoch (settlements.epoch_id is a NOT NULL FK to
        // market_epochs). A hardcoded/nil epoch FK-fails the insert below.
        let epoch_id = self.repo.get_or_create_active_epoch().await?;

        let settlement = Settlement {
            id: Uuid::new_v4(),
            trade_id: None, // Direct oracle settlement doesn't have a trade_id from matching engine
            epoch_id,
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

        // Publish SettlementRequested Event
        let requested_event = trading_core::events::Event::SettlementRequested(settlement.clone());
        let _ = self.events.publish(requested_event).await;

        info!("Settlement {} created for surplus energy", settlement.id);

        Ok(())
    }

    /// Execute multiple settlements on-chain in a single batched transaction.
    pub async fn execute_batched_settlements(
        &self,
        settlements: Vec<Settlement>,
    ) -> TraitResult<Vec<trading_core::models::SettlementTransaction>> {
        if settlements.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Execute on blockchain
        self.blockchain.execute_batched_settlements(settlements).await
    }
}
