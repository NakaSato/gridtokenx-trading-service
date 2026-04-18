use std::sync::Arc;
use tracing::{info, error, warn};
use uuid::Uuid;
use trading_core::models::{Settlement, SettlementStatus};
use trading_core::traits::{SettlementRepository, BlockchainGateway, AuditLog, TraitResult};
use trading_core::error::ApiError;

/// Service for orchestrating the settlement lifecycle
pub struct SettlementService {
    repo: Arc<dyn SettlementRepository>,
    blockchain: Arc<dyn BlockchainGateway>,
    audit: Arc<dyn AuditLog>,
}

impl SettlementService {
    pub fn new(
        repo: Arc<dyn SettlementRepository>,
        blockchain: Arc<dyn BlockchainGateway>,
        audit: Arc<dyn AuditLog>,
    ) -> Self {
        Self { repo, blockchain, audit }
    }

    /// Process a pending settlement on-chain
    pub async fn process_settlement(&self, settlement_id: Uuid) -> TraitResult<()> {
        info!("Processing settlement: {}", settlement_id);

        // 1. Fetch settlement from DB
        let settlement = self.repo.get_settlement(settlement_id).await?
            .ok_or_else(|| ApiError::NotFound(format!("Settlement {} not found", settlement_id)))?;

        if settlement.status != SettlementStatus::Pending {
            warn!("Settlement {} is already in status: {:?}", settlement_id, settlement.status);
            return Ok(());
        }

        // 2. Update status to Processing
        self.repo.update_settlement_status(settlement_id, &SettlementStatus::Processing.to_string(), None, None).await?;

        // 3. Execute on blockchain
        let result: TraitResult<trading_core::models::SettlementTransaction> = self.blockchain.execute_settlement(&settlement).await;

        match result {
            Ok(tx_result) => {
                info!("Settlement {} successful on-chain: {}", settlement_id, tx_result.signature);
                
                // 4. Update status to Completed
                self.repo.update_settlement_status(
                    settlement_id, 
                    &SettlementStatus::Completed.to_string(), 
                    Some(&tx_result.signature), 
                    None
                ).await?;

                // 5. Audit log
                let _ = self.audit.log_action(
                    settlement.buyer_id,
                    "settlement_completed",
                    &format!("Settlement {} completed on-chain with signature {}", settlement_id, tx_result.signature),
                ).await;

                Ok(())
            }
            Err(e) => {
                error!("Settlement {} failed on-chain: {}", settlement_id, e);
                
                // 4. Update status to Failed
                self.repo.update_settlement_status(
                    settlement_id, 
                    &SettlementStatus::Failed.to_string(), 
                    None, 
                    Some(&e.to_string())
                ).await?;

                // 5. Audit log
                let _ = self.audit.log_action(
                    settlement.buyer_id,
                    "settlement_failed",
                    &format!("Settlement {} failed on-chain: {}", settlement_id, e),
                ).await;

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
}
