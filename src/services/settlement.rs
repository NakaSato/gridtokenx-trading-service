use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};
use opentelemetry::propagation::TextMapPropagator;
use uuid::Uuid;

use crate::domain::events::{Event, SettlementProcessedPayload};
use crate::domain::trading::settlement::{
    SettlementManager, SettlementStatus, SettlementTransaction,
};
use crate::infra::blockchain::settlement::BlockchainSettlementProvider;
use crate::infra::blockchain::BlockchainService;
use crate::infra::events::EventBus;
use crate::services::erc::ErcService;

/// Orchestration service for Settlement (Coordinates Domain and Infra)
#[derive(Debug, Clone)]
pub struct SettlementService {
    manager: Arc<SettlementManager>,
    provider: Arc<BlockchainSettlementProvider>,
    event_bus: EventBus,
    erc_service: Option<Arc<ErcService>>,
    _blockchain: Arc<BlockchainService>,
}

impl SettlementService {
    pub fn new(
        manager: Arc<SettlementManager>,
        provider: Arc<BlockchainSettlementProvider>,
        event_bus: EventBus,
        erc_service: Option<Arc<ErcService>>,
        blockchain: Arc<BlockchainService>,
    ) -> Self {
        Self {
            manager,
            provider,
            event_bus,
            erc_service,
            _blockchain: blockchain,
        }
    }

    pub fn with_erc(mut self, erc: Arc<ErcService>) -> Self {
        self.erc_service = Some(erc);
        self
    }

    #[tracing::instrument(skip(self, trade), fields(trade_id = %trade.id, order_id = %trade.match_id))]
    pub async fn create_settlement(
        &self,
        trade: &crate::domain::trading::clearing::TradeMatch,
    ) -> Result<crate::domain::trading::settlement::Settlement> {
        Ok(self.manager.create_settlement_record(trade).await?)
    }

    #[tracing::instrument(skip(self), fields(settlement_id = %id))]
    pub async fn process_settlement(&self, id: Uuid) -> Result<SettlementTransaction> {
        info!("Processing settlement coordinator workflow for {}", id);

        // 1. Mark processing in DB
        self.manager
            .update_settlement_status(id, SettlementStatus::Processing)
            .await?;

        // 2. Load context to get wallets setup
        let mut contexts = self.manager.get_batch_context(&[id]).await?;
        if contexts.is_empty() {
            anyhow::bail!("Settlement context not found for {}", id);
        }
        let ctx = contexts.remove(0);
        let settlement = ctx.settlement;

        // 3. Execute Blockchain Transaction via Provider
        let tx_result = match self
            .provider
            .execute_atomic_settlement(
                &settlement,
                &ctx.buy_order_pda,
                &ctx.sell_order_pda,
                &ctx.buyer_wallet,
                &ctx.seller_wallet,
            )
            .await
        {
            Ok(result) => result,
            Err(e) => {
                let error_msg = e.to_string();
                error!("❌ On-chain settlement failed for {}: {}", id, error_msg);
                self.manager.mark_settlement_failed(id, &error_msg).await?;
                return Err(e.into());
            }
        };

        // 3.5 Transfer ERC certificate if possible
        if let Some(erc) = &self.erc_service {
            if let Ok(mut certs) = erc
                .find_settlement_certificates(settlement.seller_id, settlement.energy_amount)
                .await
            {
                if !certs.is_empty() {
                    let cert = certs.remove(0);
                    info!(
                        "Transferring ERC {} for settlement {}",
                        cert.certificate_id, id
                    );

                    let from_wallet = ctx.seller_wallet.to_string();
                    let to_wallet = ctx.buyer_wallet.to_string();

                    match erc
                        .transfer_certificate(
                            cert.id,
                            &from_wallet,
                            &to_wallet,
                            settlement.buyer_id,
                            &tx_result.signature,
                        )
                        .await
                    {
                        Ok(_) => {
                            let _ = self
                                .manager
                                .update_settlement_erc(
                                    id,
                                    &cert.certificate_id,
                                    &tx_result.signature,
                                )
                                .await;
                        }
                        Err(e) => {
                            error!("Failed to transfer ERC for settlement {}: {}", id, e);
                        }
                    }
                }
            }
        }

        // 4. Update DB as completed
        self.manager
            .update_settlement_confirmed(id, &tx_result.signature, SettlementStatus::Completed)
            .await?;

        // 5. Finalize Escrow
        self.manager.finalize_escrow(&settlement).await?;

        // 6. Notify via EventBus
        if let Err(e) = self
            .event_bus
            .publish(&Event::SettlementProcessed(SettlementProcessedPayload {
                settlement_id: id,
                tx_signature: tx_result.signature.clone(),
                status: "Completed".to_string(),
                timestamp: Utc::now(),
                otel_trace_context: settlement.trace_context.clone(),
            }))
            .await
        {
            error!(
                "❌ Failed to publish SettlementProcessed event for {}: {}",
                id, e
            );
        }

        Ok(tx_result)
    }

    /// Process all pending settlements
    #[tracing::instrument(skip(self))]
    pub async fn process_pending_settlements(&self) -> Result<usize> {
        let mut pending = self.manager.get_pending_settlements().await?;
        let total_count = pending.len();

        let max_batch_size = self.manager.config.max_batch_size;
        if pending.len() > max_batch_size {
            info!(
                "Truncating settlement batch from {} to {} for Solana size limits",
                pending.len(),
                max_batch_size
            );
            pending.truncate(max_batch_size);
        }

        let count = pending.len();

        // Use batching if multiple settlements are pending
        if count >= 2 {
            self.process_settlements_batched(pending).await?;
        } else {
            for id in pending {
                if let Err(e) = self.process_settlement(id).await {
                    error!("Failed to process settlement {}: {}", id, e);
                }
            }
        }
        Ok(total_count)
    }

    #[tracing::instrument(skip(self, ids), fields(batch_size = ids.len()))]
    pub async fn process_settlements_batched(
        &self,
        ids: Vec<Uuid>,
    ) -> Result<Vec<SettlementTransaction>> {
        let start_time = std::time::Instant::now();
        info!("🚀 Processing batch of {} settlements", ids.len());

        // 1. Mark processing in DB (Batch)
        self.manager.update_batch_status(&ids, SettlementStatus::Processing).await?;

        // 2. Load full context for all settlements (joins with orders and users for PDAs/Wallets)
        let contexts = self.manager.get_batch_context(&ids).await?;

        // 3. Prepare provider inputs
        let mut provider_inputs = Vec::new();
        for ctx in &contexts {
            provider_inputs.push((
                &ctx.settlement,
                ctx.buy_order_pda,
                ctx.sell_order_pda,
                ctx.buyer_wallet,
                ctx.seller_wallet,
            ));
        }

        // 4. Execute Batch via Blockchain Provider
        let tx_results = match self
            .provider
            .execute_batched_settlements(provider_inputs, self.manager.config.priority_fee_micro_lamports)
            .await
        {
            Ok(results) => results,
            Err(e) => {
                let error_msg = e.to_string();
                tracing::error!("Batch settlement execution failed: {}", error_msg);
                for id in &ids {
                    if let Err(mark_err) =
                        self.manager.mark_settlement_failed(*id, &error_msg).await
                    {
                        tracing::error!("Failed to mark settlement {} as failed: {}", id, mark_err);
                    }
                }
                return Err(e.into());
            }
        };

        // 5. Pipeline finalization to background task
        let service_clone = self.clone();
        let ids_clone = ids.clone();
        let contexts_clone = contexts.clone();
        let signature = tx_results[0].signature.clone();
        
        let duration_s = start_time.elapsed().as_secs_f64();
        crate::metrics::record_settlement_latency_high_fidelity(duration_s, ids.len() as u64);

        tokio::spawn(async move {
            if let Err(e) = service_clone.finalize_settlement_batch(contexts_clone, signature).await {
                tracing::error!("❌ Background finalization failed for batch {:?}: {}", ids_clone, e);
            }
        });

        Ok(tx_results)
    }

    /// Internal helper to finalize a batch of settlements after on-chain success.
    /// Handles ERC transfers, batch DB confirmation, and escrow release.
    #[tracing::instrument(skip(self, contexts), fields(signature = %signature, count = contexts.len()))]
    #[tracing::instrument(skip(self, contexts), fields(batch_size = contexts.len(), signature = %signature))]
    async fn finalize_settlement_batch(
        &self,
        contexts: Vec<crate::domain::trading::settlement::SettlementBatchContext>,
        signature: String,
    ) -> Result<()> {
        let settlements: Vec<crate::domain::trading::settlement::Settlement> = contexts.iter().map(|ctx| ctx.settlement.clone()).collect();
        let ids: Vec<Uuid> = settlements.iter().map(|s| s.id).collect();

        // 1. Handle ERC transfers (Parallel for performance)
        if let Some(erc) = &self.erc_service {
            let mut transfer_tasks = Vec::new();

            for ctx in &contexts {
                let settlement = ctx.settlement.clone();
                let erc_clone = erc.clone();
                let signature_clone = signature.clone();
                let from_wallet = ctx.seller_wallet.to_string();
                let to_wallet = ctx.buyer_wallet.to_string();
                let manager_clone = self.manager.clone();
                let trace_context = settlement.trace_context.clone();

                transfer_tasks.push(tokio::spawn(async move {
                    use opentelemetry::trace::FutureExt;
                    
                    let parent_cx = if let Some(tc) = trace_context {
                        opentelemetry::global::get_text_map_propagator(|propagator| {
                            propagator.extract(&tc)
                        })
                    } else {
                        opentelemetry::Context::current()
                    };

                    let cert_result = erc_clone
                        .find_settlement_certificates(settlement.seller_id, settlement.energy_amount)
                        .with_context(parent_cx.clone())
                        .await;
                    
                    if let Ok(mut certs) = cert_result {
                        if !certs.is_empty() {
                            let cert = certs.remove(0);
                            match erc_clone
                                .transfer_certificate(
                                    cert.id,
                                    &from_wallet,
                                    &to_wallet,
                                    settlement.buyer_id,
                                    &signature_clone,
                                )
                                .with_context(parent_cx.clone())
                                .await
                            {
                                Ok(_) => {
                                    let _ = manager_clone
                                        .update_settlement_erc(
                                            settlement.id,
                                            &cert.certificate_id,
                                            &signature_clone,
                                        )
                                        .with_context(parent_cx)
                                        .await;
                                }
                                Err(e) => {
                                    tracing::error!("Failed to transfer ERC for settlement {}: {}", settlement.id, e);
                                }
                            }
                        }
                    }
                }));
            }
            
            // Wait for all ERC transfers in this batch to finish (or timeout)
            let _ = futures::future::join_all(transfer_tasks).await;
        }

        // 2. Update DB confirmation in batch
        self.manager
            .update_batch_confirmed(&ids, &signature, SettlementStatus::Completed)
            .await?;

        // 3. Finalize Escrows in batch
        self.manager.finalize_batch_escrow(&settlements).await?;

        // 4. Notify Match results via EventBus (Batch)
        for settlement in &settlements {
            let _ = self.event_bus.publish(&Event::SettlementProcessed(SettlementProcessedPayload {
                settlement_id: settlement.id,
                tx_signature: signature.clone(),
                status: "Completed".to_string(),
                timestamp: Utc::now(),
                otel_trace_context: settlement.trace_context.clone(),
            })).await;
        }

        info!("✅ Batch of {} settlements finalized via background pipeline", ids.len());
        Ok(())
    }

    /// Update settlement status directly
    pub async fn update_settlement_status(&self, id: Uuid, status: SettlementStatus) -> Result<()> {
        Ok(self.manager.update_settlement_status(id, status).await?)
    }

    /// Update settlement confirmed status
    pub async fn update_settlement_confirmed(
        &self,
        id: Uuid,
        tx_signature: &str,
        status: SettlementStatus,
    ) -> Result<()> {
        Ok(self
            .manager
            .update_settlement_confirmed(id, tx_signature, status)
            .await?)
    }

    /// Get a settlement by ID
    pub async fn get_settlement(
        &self,
        id: Uuid,
    ) -> Result<crate::domain::trading::settlement::Settlement> {
        Ok(self.manager.get_settlement(id).await?)
    }

    /// Execute on-chain generation mint for a prosumer based on Oracle Bridge billing bins.
    /// This resolves the meter's owner wallet and delegates to the blockchain provider.
    pub async fn execute_generation_mint(
        &self,
        meter_id: Uuid,
        _meter_serial: &str,
        amount_kwh: rust_decimal::Decimal,
        timestamp: i64,
    ) -> Result<String> {
        info!("🚀 Executing generation mint for meter {}: {} kWh", meter_id, amount_kwh);

        // 1. Resolve Meter ownership (Must be in Registry)
        let record = sqlx::query(
            r#"
            SELECT m.user_id, w.address as wallet_address 
            FROM meters m
            JOIN user_wallets w ON m.user_id = w.user_id
            WHERE m.id = $1 AND w.is_primary = true
            "#,
        )
        .bind(meter_id)
        .fetch_optional(&self.manager.db_pool())
        .await?
        .ok_or_else(|| anyhow::anyhow!("Meter or primary wallet not found in registry for {}", meter_id))?;

        use sqlx::Row;
        let wallet_address: String = record.get("wallet_address");
        let user_wallet_pubkey = crate::infra::blockchain::BlockchainService::parse_pubkey(&wallet_address)
            .map_err(|e| anyhow::anyhow!("Invalid wallet pubkey: {}", e))?;

        // 2. Execute On-Chain Minting via Provider
        let tx_signature = self.provider.execute_generation_mint(
            &user_wallet_pubkey,
            amount_kwh,
            timestamp,
        ).await
        .map_err(|e| anyhow::anyhow!("On-chain generation mint failed: {}", e))?;

        Ok(tx_signature)
    }
}
