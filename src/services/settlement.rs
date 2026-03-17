use anyhow::Result;
use std::sync::Arc;
use uuid::Uuid;
use tracing::{info, error};
use chrono::Utc;

use crate::domain::trading::settlement::{SettlementManager, SettlementStatus, SettlementTransaction};
use crate::infra::blockchain::settlement::BlockchainSettlementProvider;
use crate::infra::blockchain::BlockchainService;
use crate::infra::events::EventBus;
use crate::domain::events::{Event, SettlementProcessedPayload};
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

    /// Create a settlement record from a trade match
    pub async fn create_settlement(&self, trade: &crate::domain::trading::clearing::TradeMatch) -> Result<crate::domain::trading::settlement::Settlement> {
        Ok(self.manager.create_settlement_record(trade).await?)
    }

    pub async fn process_settlement(&self, id: Uuid) -> Result<SettlementTransaction> {
        info!("Processing settlement coordinator workflow for {}", id);
        
        // 1. Mark processing in DB
        self.manager.update_settlement_status(id, SettlementStatus::Processing).await?;
        
        // 2. Load context to get wallets setup
        let mut contexts = self.manager.get_batch_context(&[id]).await?;
        if contexts.is_empty() {
            anyhow::bail!("Settlement context not found for {}", id);
        }
        let ctx = contexts.remove(0);
        let settlement = ctx.settlement;
        
        // 3. Execute Blockchain Transaction via Provider
        let tx_result = match self.provider.execute_atomic_settlement(
            &settlement,
            &ctx.buy_order_pda,
            &ctx.sell_order_pda,
            &ctx.buyer_wallet,
            &ctx.seller_wallet,
        ).await {
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
            if let Ok(mut certs) = erc.find_settlement_certificates(settlement.seller_id, settlement.energy_amount).await {
                if !certs.is_empty() {
                    let cert = certs.remove(0);
                    info!("Transferring ERC {} for settlement {}", cert.certificate_id, id);
                    
                    let from_wallet = ctx.seller_wallet.to_string();
                    let to_wallet = ctx.buyer_wallet.to_string();
                    
                    match erc.transfer_certificate(cert.id, &from_wallet, &to_wallet, settlement.buyer_id, &tx_result.signature).await {
                        Ok(_) => {
                            let _ = self.manager.update_settlement_erc(id, &cert.certificate_id, &tx_result.signature).await;
                        }
                        Err(e) => {
                            error!("Failed to transfer ERC for settlement {}: {}", id, e);
                        }
                    }
                }
            }
        }

        // 4. Update DB as completed
        self.manager.update_settlement_confirmed(id, &tx_result.signature, SettlementStatus::Completed).await?;
        
        // 5. Finalize Escrow
        self.manager.finalize_escrow(&settlement).await?;

        // 6. Notify via EventBus
        if let Err(e) = self.event_bus.publish(Event::SettlementProcessed(SettlementProcessedPayload {
            settlement_id: id,
            tx_signature: tx_result.signature.clone(),
            status: "Completed".to_string(),
            timestamp: Utc::now(),
        })).await {
            error!("❌ Failed to publish SettlementProcessed event for {}: {}", id, e);
        }
        
        Ok(tx_result)
    }

    /// Process all pending settlements
    pub async fn process_pending_settlements(&self) -> Result<usize> {
        let mut pending = self.manager.get_pending_settlements().await?;
        let total_count = pending.len();
        
        // Limit batch size to 5 to avoid Solana transaction size limits (1232 bytes)
        // Each settlement adds significant data to the transaction instructions
        if pending.len() > 5 {
            info!("Truncating settlement batch from {} to 5 for Solana size limits", pending.len());
            pending.truncate(5);
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

    /// Process a batch of settlements in a single Solana transaction
    pub async fn process_settlements_batched(&self, ids: Vec<Uuid>) -> Result<Vec<SettlementTransaction>> {
        info!("🚀 Processing batch of {} settlements", ids.len());
        
        // 1. Mark processing in DB
        for id in &ids {
            self.manager.update_settlement_status(*id, SettlementStatus::Processing).await?;
        }

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
        let tx_results = match self.provider.execute_batched_settlements(provider_inputs).await {
            Ok(results) => results,
            Err(e) => {
                let error_msg = e.to_string();
                tracing::error!("Batch settlement execution failed: {}", error_msg);
                for id in &ids {
                    if let Err(mark_err) = self.manager.mark_settlement_failed(*id, &error_msg).await {
                        tracing::error!("Failed to mark settlement {} as failed: {}", id, mark_err);
                    }
                }
                return Err(e.into());
            }
        };

        // 5. Update DB and finalize escrow for each
        for (i, tx) in tx_results.iter().enumerate() {
            let settlement = &contexts[i].settlement;
            
            // 5.1 Transfer ERC certificate if possible
            if let Some(erc) = &self.erc_service {
                if let Ok(mut certs) = erc.find_settlement_certificates(settlement.seller_id, settlement.energy_amount).await {
                    if !certs.is_empty() {
                        let cert = certs.remove(0);
                        info!("Transferring ERC {} for settlement {}", cert.certificate_id, settlement.id);
                        
                        let from_wallet = contexts[i].seller_wallet.to_string();
                        let to_wallet = contexts[i].buyer_wallet.to_string();
                        
                        match erc.transfer_certificate(cert.id, &from_wallet, &to_wallet, settlement.buyer_id, &tx.signature).await {
                            Ok(_) => {
                                let _ = self.manager.update_settlement_erc(settlement.id, &cert.certificate_id, &tx.signature).await;
                            }
                            Err(e) => {
                                error!("Failed to transfer ERC for settlement {}: {}", settlement.id, e);
                            }
                        }
                    }
                }
            }

            // Update confirmed status
            self.manager.update_settlement_confirmed(settlement.id, &tx.signature, SettlementStatus::Completed).await?;
            
            // Finalize Escrow
            self.manager.finalize_escrow(settlement).await?;
        }

        Ok(tx_results)
    }

    /// Update settlement status directly
    pub async fn update_settlement_status(&self, id: Uuid, status: SettlementStatus) -> Result<()> {
        Ok(self.manager.update_settlement_status(id, status).await?)
    }

    /// Update settlement confirmed status
    pub async fn update_settlement_confirmed(&self, id: Uuid, tx_signature: &str, status: SettlementStatus) -> Result<()> {
        Ok(self.manager.update_settlement_confirmed(id, tx_signature, status).await?)
    }

    /// Get a settlement by ID
    pub async fn get_settlement(&self, id: Uuid) -> Result<crate::domain::trading::settlement::Settlement> {
        Ok(self.manager.get_settlement(id).await?)
    }
}
