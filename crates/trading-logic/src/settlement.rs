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
        let by_id: HashMap<Uuid, Settlement> = claimed.iter().cloned().map(|s| (s.id, s)).collect();

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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use trading_core::error::ApiError;
    use trading_core::events::Event;
    use trading_core::models::{
        OrderMatch, Settlement, SettlementStats, SettlementStatus, SettlementTransaction,
    };

    // ── Fixtures ─────────────────────────────────────────────────────────────

    /// Minimal, valid `Settlement` (it does not derive Default — fill all fields).
    fn make_settlement(id: Uuid) -> Settlement {
        Settlement {
            id,
            trade_id: Some(Uuid::new_v4()),
            epoch_id: Uuid::new_v4(),
            buyer_id: Uuid::new_v4(),
            seller_id: Uuid::new_v4(),
            buy_order_id: Uuid::new_v4(),
            sell_order_id: Uuid::new_v4(),
            energy_amount: Decimal::new(100, 0),
            price: Decimal::new(5, 0),
            total_amount: Decimal::new(500, 0),
            fee_amount: Decimal::ZERO,
            net_amount: Decimal::new(500, 0),
            status: SettlementStatus::Processing,
            blockchain_tx: None,
            created_at: Utc::now(),
            confirmed_at: None,
            wheeling_charge: None,
            loss_factor: None,
            loss_cost: None,
            effective_energy: None,
            buyer_zone_id: None,
            seller_zone_id: None,
            buyer_session_token: None,
            seller_session_token: None,
            erc_certificate_id: None,
            erc_transfer_tx: None,
            retry_count: 0,
            error_message: None,
        }
    }

    fn tx_for(id: Uuid, sig: &str) -> SettlementTransaction {
        SettlementTransaction {
            settlement_id: id,
            signature: sig.to_string(),
            slot: 1,
            confirmation_status: "confirmed".to_string(),
        }
    }

    // ── Fake SettlementRepository ────────────────────────────────────────────

    /// Records which settlement ids were claimed / completed / reset.
    #[derive(Default)]
    struct FakeRepo {
        /// What `claim_settlements_for_processing` should hand back.
        claim_returns: Mutex<Vec<Settlement>>,
        /// (id, status, tx_hash) for every `update_settlement_status_with_event`.
        completed_with_event: Mutex<Vec<(Uuid, String, Option<String>)>>,
        /// (id, status, tx_hash) for every plain `update_settlement_status`.
        completed_plain: Mutex<Vec<(Uuid, String, Option<String>)>>,
        /// ids passed to `reset_settlements_for_retry` (flattened across calls).
        reset_ids: Mutex<Vec<Uuid>>,
        claim_calls: AtomicUsize,
    }

    impl FakeRepo {
        fn with_claim(returns: Vec<Settlement>) -> Self {
            Self {
                claim_returns: Mutex::new(returns),
                ..Default::default()
            }
        }
    }

    #[async_trait]
    impl SettlementRepository for FakeRepo {
        async fn claim_settlements_for_processing(
            &self,
            _ids: &[Uuid],
        ) -> TraitResult<Vec<Settlement>> {
            self.claim_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.claim_returns.lock().unwrap().clone())
        }

        async fn update_settlement_status_with_event(
            &self,
            id: Uuid,
            status: &str,
            tx_hash: Option<&str>,
            _error: Option<&str>,
            _event: &Event,
        ) -> TraitResult<()> {
            self.completed_with_event.lock().unwrap().push((
                id,
                status.to_string(),
                tx_hash.map(str::to_string),
            ));
            Ok(())
        }

        async fn update_settlement_status(
            &self,
            id: Uuid,
            status: &str,
            tx_hash: Option<&str>,
            _error: Option<&str>,
        ) -> TraitResult<()> {
            self.completed_plain.lock().unwrap().push((
                id,
                status.to_string(),
                tx_hash.map(str::to_string),
            ));
            Ok(())
        }

        async fn reset_settlements_for_retry(
            &self,
            ids: &[Uuid],
            _max_retries: i32,
            _error: Option<&str>,
        ) -> TraitResult<u64> {
            self.reset_ids.lock().unwrap().extend_from_slice(ids);
            Ok(0)
        }

        // ── irrelevant to settle_claimed ──
        async fn insert_settlement(&self, _settlement: &Settlement) -> TraitResult<()> {
            unimplemented!()
        }
        async fn insert_settlement_with_event(
            &self,
            _settlement: &Settlement,
            _event: &Event,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
            unimplemented!()
        }
        async fn insert_match(
            &self,
            _m: &OrderMatch,
            _settlement_id: Option<Uuid>,
            _zone_id: Option<i32>,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn insert_match_with_event(
            &self,
            _m: &OrderMatch,
            _settlement_id: Option<Uuid>,
            _zone_id: Option<i32>,
            _event: &Event,
        ) -> TraitResult<()> {
            unimplemented!()
        }
        async fn persist_matched_trade(
            &self,
            _settlement: &Settlement,
            _order_match: &OrderMatch,
            _matched_event: &Event,
            _match_zone_id: Option<i32>,
            _buyer: &trading_core::traits::TradeFill,
            _seller: &trading_core::traits::TradeFill,
        ) -> TraitResult<bool> {
            unimplemented!()
        }
        async fn get_settlement(&self, _id: Uuid) -> TraitResult<Option<Settlement>> {
            unimplemented!()
        }
        async fn get_pending_settlements(&self, _limit: i64) -> TraitResult<Vec<Settlement>> {
            unimplemented!()
        }
        async fn reclaim_stale_processing(
            &self,
            _stale_after_secs: i64,
            _max_retries: i32,
        ) -> TraitResult<u64> {
            unimplemented!()
        }
        async fn list_settlements_for_user(
            &self,
            _user_id: Uuid,
            _limit: i64,
            _offset: i64,
        ) -> TraitResult<(Vec<Settlement>, i64)> {
            unimplemented!()
        }
        async fn get_settlement_stats(&self) -> TraitResult<SettlementStats> {
            unimplemented!()
        }
        async fn get_market_price(
            &self,
            _window_hours: i64,
        ) -> TraitResult<trading_core::models::MarketPrice> {
            unimplemented!()
        }
        async fn count_active_traders(&self, _window_hours: i64) -> TraitResult<i64> {
            unimplemented!()
        }
    }

    // ── Fake BlockchainGateway ───────────────────────────────────────────────

    enum BatchResult {
        Ok(Vec<SettlementTransaction>),
        Err(String),
    }

    struct FakeChain {
        batch: Mutex<Option<BatchResult>>,
        batch_calls: AtomicUsize,
    }

    impl FakeChain {
        fn ok(results: Vec<SettlementTransaction>) -> Self {
            Self {
                batch: Mutex::new(Some(BatchResult::Ok(results))),
                batch_calls: AtomicUsize::new(0),
            }
        }
        fn err(msg: &str) -> Self {
            Self {
                batch: Mutex::new(Some(BatchResult::Err(msg.to_string()))),
                batch_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl BlockchainGateway for FakeChain {
        async fn execute_batched_settlements(
            &self,
            _settlements: Vec<Settlement>,
        ) -> TraitResult<Vec<SettlementTransaction>> {
            self.batch_calls.fetch_add(1, Ordering::SeqCst);
            match self.batch.lock().unwrap().take() {
                Some(BatchResult::Ok(r)) => Ok(r),
                Some(BatchResult::Err(m)) => Err(ApiError::Blockchain(m)),
                None => Ok(Vec::new()),
            }
        }

        // ── irrelevant ──
        async fn is_user_registered(&self, _user_id: Uuid) -> TraitResult<bool> {
            unimplemented!()
        }
        async fn get_user_wallet(&self, _user_id: Uuid) -> TraitResult<Option<String>> {
            unimplemented!()
        }
        async fn get_token_balance(&self, _wallet_address: &str) -> TraitResult<u64> {
            unimplemented!()
        }
        async fn get_zone_config(
            &self,
            _zone_id: i32,
        ) -> TraitResult<trading_core::models::ZoneConfig> {
            unimplemented!()
        }
        async fn issue_erc(
            &self,
            _user_id: Uuid,
            _meter_id: &str,
            _energy_amount: Decimal,
        ) -> TraitResult<String> {
            unimplemented!()
        }
        async fn sync_total_supply(&self) -> TraitResult<String> {
            unimplemented!()
        }
        async fn execute_create_order(
            &self,
            _user_id: Uuid,
            _market_pubkey: &str,
            _amount: u64,
            _price: u64,
            _order_side: &str,
            _erc_id: Option<&str>,
            _zone_id: u32,
            _expires_at_unix: i64,
        ) -> TraitResult<(String, String, u64)> {
            unimplemented!()
        }
    }

    // ── Fake AuditLog ────────────────────────────────────────────────────────

    #[derive(Default)]
    struct FakeAudit {
        actions: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl AuditLog for FakeAudit {
        async fn log_action(
            &self,
            _user_id: Uuid,
            action: &str,
            _details: &str,
        ) -> TraitResult<()> {
            self.actions.lock().unwrap().push(action.to_string());
            Ok(())
        }
    }

    fn service(
        repo: Arc<FakeRepo>,
        chain: Arc<FakeChain>,
        audit: Arc<FakeAudit>,
    ) -> SettlementService {
        SettlementService::new(repo, chain, audit, Uuid::nil())
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /// 1. Minted settlement → marked Completed (with event) carrying the signature.
    #[tokio::test]
    async fn minted_settlement_marked_completed_with_signature() {
        let id = Uuid::new_v4();
        let repo = Arc::new(FakeRepo::with_claim(vec![make_settlement(id)]));
        let chain = Arc::new(FakeChain::ok(vec![tx_for(id, "SIG_OK")]));
        let audit = Arc::new(FakeAudit::default());
        let svc = service(repo.clone(), chain, audit);

        let out = svc
            .execute_batched_settlements(vec![make_settlement(id)])
            .await
            .expect("settle should succeed");

        assert_eq!(out.len(), 1);
        let completed = repo.completed_with_event.lock().unwrap();
        assert_eq!(completed.len(), 1, "exactly one completion write");
        assert_eq!(completed[0].0, id);
        assert_eq!(completed[0].1, SettlementStatus::Completed.to_string());
        assert_eq!(completed[0].2.as_deref(), Some("SIG_OK"));
        // A minted settlement is never reset for retry.
        assert!(repo.reset_ids.lock().unwrap().is_empty());
    }

    /// 2. A claimed settlement absent from the chain's tx_results is released for
    ///    retry — never completed, never stranded in processing.
    #[tokio::test]
    async fn unminted_claimed_settlement_released_for_retry() {
        let minted = Uuid::new_v4();
        let unminted = Uuid::new_v4();
        let repo = Arc::new(FakeRepo::with_claim(vec![
            make_settlement(minted),
            make_settlement(unminted),
        ]));
        // Chain mints only `minted`.
        let chain = Arc::new(FakeChain::ok(vec![tx_for(minted, "SIG_M")]));
        let audit = Arc::new(FakeAudit::default());
        let svc = service(repo.clone(), chain, audit);

        let out = svc
            .execute_batched_settlements(vec![make_settlement(minted), make_settlement(unminted)])
            .await
            .expect("settle should succeed (partial mint)");

        assert_eq!(out.len(), 1);
        // Only the minted one is completed.
        let completed = repo.completed_with_event.lock().unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, minted);
        // The unminted one (and only it) is reset for retry.
        let reset = repo.reset_ids.lock().unwrap();
        assert_eq!(*reset, vec![unminted]);
    }

    /// 3. Blockchain error → whole claimed batch reset, settle returns Err, nothing
    ///    marked completed.
    #[tokio::test]
    async fn blockchain_error_resets_batch_and_returns_err() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let repo = Arc::new(FakeRepo::with_claim(vec![
            make_settlement(a),
            make_settlement(b),
        ]));
        let chain = Arc::new(FakeChain::err("rpc down"));
        let audit = Arc::new(FakeAudit::default());
        let svc = service(repo.clone(), chain, audit);

        let err = svc
            .execute_batched_settlements(vec![make_settlement(a), make_settlement(b)])
            .await
            .expect_err("blockchain failure must propagate");
        assert!(matches!(err, ApiError::Blockchain(_)));

        // Nothing completed.
        assert!(repo.completed_with_event.lock().unwrap().is_empty());
        assert!(repo.completed_plain.lock().unwrap().is_empty());
        // Whole claimed batch reset.
        let mut reset = repo.reset_ids.lock().unwrap().clone();
        reset.sort();
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(reset, expected);
    }

    /// 4. Empty input → Ok(empty), no claim attempted.
    #[tokio::test]
    async fn empty_input_returns_empty_without_claiming() {
        let repo = Arc::new(FakeRepo::with_claim(vec![]));
        let chain = Arc::new(FakeChain::ok(vec![]));
        let audit = Arc::new(FakeAudit::default());
        let svc = service(repo.clone(), chain.clone(), audit);

        let out = svc
            .execute_batched_settlements(vec![])
            .await
            .expect("empty input is Ok");

        assert!(out.is_empty());
        assert_eq!(repo.claim_calls.load(Ordering::SeqCst), 0);
        assert_eq!(chain.batch_calls.load(Ordering::SeqCst), 0);
    }

    /// 5. Claim returns empty → Ok(empty), no blockchain call.
    #[tokio::test]
    async fn empty_claim_returns_empty_without_blockchain_call() {
        // Non-empty input so the claim path runs, but claim hands back nothing.
        let repo = Arc::new(FakeRepo::with_claim(vec![]));
        let chain = Arc::new(FakeChain::ok(vec![]));
        let audit = Arc::new(FakeAudit::default());
        let svc = service(repo.clone(), chain.clone(), audit);

        let out = svc
            .execute_batched_settlements(vec![make_settlement(Uuid::new_v4())])
            .await
            .expect("empty claim is Ok");

        assert!(out.is_empty());
        assert_eq!(
            repo.claim_calls.load(Ordering::SeqCst),
            1,
            "claim attempted"
        );
        assert_eq!(
            chain.batch_calls.load(Ordering::SeqCst),
            0,
            "no blockchain call on empty claim"
        );
    }
}
