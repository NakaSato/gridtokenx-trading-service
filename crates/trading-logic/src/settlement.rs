use std::sync::Arc;
use tracing::{error, info, warn};
use trading_core::models::{Settlement, SettlementStatus};
use trading_core::traits::{AuditLog, BlockchainGateway, SettlementRepository, TraitResult};
use uuid::Uuid;

/// Service for orchestrating the settlement lifecycle
pub struct SettlementService {
    repo: Arc<dyn SettlementRepository>,
    blockchain: Arc<dyn BlockchainGateway>,
    audit: Arc<dyn AuditLog>,
    platform_user_id: Uuid,
}

impl SettlementService {
    pub fn new(
        repo: Arc<dyn SettlementRepository>,
        blockchain: Arc<dyn BlockchainGateway>,
        audit: Arc<dyn AuditLog>,
        platform_user_id: Uuid,
    ) -> Self {
        Self {
            repo,
            blockchain,
            audit,
            platform_user_id,
        }
    }

    pub fn platform_user_id(&self) -> Uuid {
        self.platform_user_id
    }

    /// Reclaim settlements orphaned in `processing` (crash / partial failure
    /// between claim and finalize) back to a retryable state. Run by the
    /// settlement worker before each claim pass. Returns rows reclaimed.
    pub async fn reclaim_stale_settlements(&self) -> TraitResult<u64> {
        self.repo
            .reclaim_stale_processing(STALE_PROCESSING_SECS, MAX_SETTLEMENT_RETRIES)
            .await
    }

    /// Process all pending settlements (Batch). Run by the settlement worker.
    pub async fn process_pending_settlements(&self, limit: i64) -> TraitResult<usize> {
        let pending = self.repo.get_pending_settlements(limit).await?;
        if pending.is_empty() {
            return Ok(0);
        }

        // Atomically claim Pending -> Processing before minting. A concurrent
        // RPC batch call (or a second worker tick) claiming the same rows
        // receives a disjoint subset, so no settlement is ever minted twice.
        let ids: Vec<Uuid> = pending.iter().map(|s| s.id).collect();
        let claimed = self.repo.claim_settlements_for_processing(&ids).await?;
        if claimed.is_empty() {
            return Ok(0);
        }

        info!("Claimed {} settlements for batch processing", claimed.len());
        let results = self.settle_claimed(claimed).await?;
        Ok(results.len())
    }

    /// Execute a specific set of settlements on-chain (RPC entrypoint).
    /// Atomically claims each so it cannot also be minted by the settlement
    /// worker; already-claimed rows are silently skipped.
    pub async fn execute_batched_settlements(
        &self,
        settlements: Vec<Settlement>,
    ) -> TraitResult<Vec<trading_core::models::SettlementTransaction>> {
        if settlements.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<Uuid> = settlements.iter().map(|s| s.id).collect();
        let claimed = self.repo.claim_settlements_for_processing(&ids).await?;
        if claimed.is_empty() {
            return Ok(Vec::new());
        }

        self.settle_claimed(claimed).await
    }

    /// Mint + finalize an already-claimed (status = `processing`) batch.
    ///
    /// Each claimed settlement ends in a non-`processing` state, so none is ever
    /// stranded:
    /// - settlements the chain minted (present in `tx_results`) are marked
    ///   `Completed` (with their `SettlementProcessed` outbox event), audited,
    ///   and — for direct oracle settlements (no `trade_id`) — have their ERC
    ///   issued;
    /// - claimed settlements the chain did **not** mint (missing from
    ///   `tx_results`, or the whole call failed) were never minted, so they are
    ///   released back to `pending` for retry (or `permanently_failed` once the
    ///   retry budget is exhausted) — never silently dropped.
    ///
    /// A post-mint DB update failure deliberately does **not** reset the row to
    /// `pending`: the tokens already exist on-chain and a retry would double
    /// mint, so the row is forced out of `processing` by other means and the
    /// failure is logged loudly.
    async fn settle_claimed(
        &self,
        claimed: Vec<Settlement>,
    ) -> TraitResult<Vec<trading_core::models::SettlementTransaction>> {
        use std::collections::{HashMap, HashSet};
        let by_id: HashMap<Uuid, Settlement> =
            claimed.iter().cloned().map(|s| (s.id, s)).collect();

        let tx_results = match self
            .blockchain
            .execute_batched_settlements(claimed.clone())
            .await
        {
            Ok(results) => results,
            Err(e) => {
                // Nothing was minted, so the whole claimed batch is safe to
                // retry. Release it from `processing` rather than burning it to a
                // terminal state on a transient failure.
                error!("Batch settlement failed: {}", e);
                let ids: Vec<Uuid> = claimed.iter().map(|s| s.id).collect();
                let _ = self
                    .repo
                    .reset_settlements_for_retry(&ids, MAX_SETTLEMENT_RETRIES, Some(&e.to_string()))
                    .await;
                for settlement in &claimed {
                    let _ = self
                        .audit
                        .log_action(
                            settlement.buyer_id,
                            "settlement_failed",
                            &format!("Settlement {} failed on-chain: {}", settlement.id, e),
                        )
                        .await;
                }
                return Err(e);
            }
        };

        // Mark every minted settlement Completed. A DB error here must not abort
        // the loop (later rows were also minted on-chain) and must not reset the
        // row to `pending` (that would re-mint) — so we never `?` out and fall
        // back to a plain status write to force it out of `processing`.
        let mut minted_ids: HashSet<Uuid> = HashSet::new();
        for tx_result in &tx_results {
            minted_ids.insert(tx_result.settlement_id);
            info!(
                "Settlement {} successful on-chain: {}",
                tx_result.settlement_id, tx_result.signature
            );

            let processed_event = trading_core::events::Event::SettlementProcessed(
                trading_core::events::SettlementProcessedPayload {
                    settlement_id: tx_result.settlement_id,
                    tx_signature: tx_result.signature.clone(),
                    status: SettlementStatus::Completed.to_string(),
                    timestamp: gridtokenx_telemetry::time::now(),
                },
            );
            if let Err(e) = self
                .repo
                .update_settlement_status_with_event(
                    tx_result.settlement_id,
                    &SettlementStatus::Completed.to_string(),
                    Some(&tx_result.signature),
                    None,
                    &processed_event,
                )
                .await
            {
                // Outbox-transactional write failed AFTER the mint. Try a plain
                // status write so the row leaves `processing` (losing only the
                // event, recoverable downstream) rather than risk the reaper
                // re-minting it.
                error!(
                    "Settlement {} minted ({}) but completion+event write failed: {}; retrying status-only",
                    tx_result.settlement_id, tx_result.signature, e
                );
                if let Err(e2) = self
                    .repo
                    .update_settlement_status(
                        tx_result.settlement_id,
                        &SettlementStatus::Completed.to_string(),
                        Some(&tx_result.signature),
                        None,
                    )
                    .await
                {
                    error!(
                        "Settlement {} minted ({}) but could not be marked Completed: {}; left in processing for manual reconciliation",
                        tx_result.settlement_id, tx_result.signature, e2
                    );
                }
                continue;
            }

            if let Some(settlement) = by_id.get(&tx_result.settlement_id) {
                let _ = self
                    .audit
                    .log_action(
                        settlement.buyer_id,
                        "settlement_completed",
                        &format!(
                            "Settlement {} completed on-chain with signature {}",
                            settlement.id, tx_result.signature
                        ),
                    )
                    .await;
            }
        }

        // Any claimed settlement the chain did not mint was never charged on
        // chain — release it back to `pending` (or `permanently_failed`) so it
        // is retried instead of stranded in `processing`.
        let unminted: Vec<Uuid> = claimed
            .iter()
            .map(|s| s.id)
            .filter(|id| !minted_ids.contains(id))
            .collect();
        if !unminted.is_empty() {
            warn!(
                "{} claimed settlement(s) were not minted on-chain; releasing for retry",
                unminted.len()
            );
            let _ = self
                .repo
                .reset_settlements_for_retry(
                    &unminted,
                    MAX_SETTLEMENT_RETRIES,
                    Some("not included in on-chain batch result"),
                )
                .await;
        }

        Ok(tx_results)
    }
}

/// Max times a settlement is retried before it is parked in `permanently_failed`
/// for manual reconciliation. Bounds churn so a permanently-broken settlement
/// cannot be reclaimed and re-attempted forever.
const MAX_SETTLEMENT_RETRIES: i32 = 5;

/// A settlement sitting in `processing` longer than this was orphaned by a
/// crash/partial failure between claim and finalize; the worker reclaims it.
const STALE_PROCESSING_SECS: i64 = 300;
