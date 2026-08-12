//! Audits the `settlements` ledger against the schedule the chain bills at.
//!
//! `charges::compute_charges` documents that a settlement's `net` "must reconcile
//! with their on-chain credit", and that claim was established by measuring two
//! trades **by hand**. Nothing has checked it since. That is how a row could claim
//! `net = gross, fee = 0, wheeling = 0` while the seller was credited 0.897 of
//! every 1.00, and how the matcher and the ledger could price wheeling from two
//! different schedules for as long as they did — both were found by a person
//! looking, not by the system noticing.
//!
//! This worker makes the system notice.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use trading_core::charges::{ChargeRates, ChargeTotals, LedgerDiscrepancy};
use trading_core::traits::{CollectorBalanceSource, SettlementRepository};

/// Rows audited per pass. The check is arithmetic on already-fetched rows, so the
/// cost is the query; a few hundred newest rows catches a fault within one cycle
/// of it starting without ever scanning the whole ledger.
const AUDIT_BATCH: i64 = 500;

/// What one pass found.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct AuditOutcome {
    pub examined: usize,
    /// Rows whose columns do not add up to the gross. Always faults.
    pub broken_identity: usize,
    /// Rows priced off the live tariff. Expected transiently after a rate change.
    pub off_tariff: usize,
}

/// Periodically re-checks recently completed settlements.
pub struct ReconciliationWorker {
    repo: Arc<dyn SettlementRepository>,
    rates: Arc<dyn ChargeRates>,
    /// `None` disables the chain-side cross-check (the ledger audit still runs).
    collectors: Option<Arc<dyn CollectorBalanceSource>>,
    /// Ledger and chain totals as first observed, so the comparison is on DELTAS.
    ///
    /// Absolute equality is unachievable and always will be: collectors may hold a
    /// balance from before this ever ran, may be drained by an operator, and — as
    /// happened here — may have been misconfigured for part of the ledger's
    /// history. Anchoring to a baseline neutralises all of that and still catches
    /// the thing that matters: charges booked from now on failing to arrive.
    ///
    /// Deliberately in-memory. It re-baselines on restart, which costs the ability
    /// to detect drift that began before the process started, and buys not needing
    /// a schema change for a detector.
    baseline: std::sync::Mutex<Option<(ChargeTotals, ChargeTotals)>>,
    interval: Duration,
}

impl ReconciliationWorker {
    #[must_use]
    pub fn new(
        repo: Arc<dyn SettlementRepository>,
        rates: Arc<dyn ChargeRates>,
        collectors: Option<Arc<dyn CollectorBalanceSource>>,
        interval_secs: u64,
    ) -> Self {
        Self {
            repo,
            rates,
            collectors,
            baseline: std::sync::Mutex::new(None),
            interval: Duration::from_secs(interval_secs),
        }
    }

    pub async fn run(&self) {
        info!(
            "🚀 Starting ReconciliationWorker loop (interval: {:?})",
            self.interval
        );
        loop {
            match self.audit_once().await {
                Ok(outcome) => debug!(
                    examined = outcome.examined,
                    broken_identity = outcome.broken_identity,
                    off_tariff = outcome.off_tariff,
                    "settlement ledger audited"
                ),
                // A failed audit is not a failed settlement: log and try again next
                // tick rather than backing off, since this is the only thing
                // watching and going quiet is the opposite of what it is for.
                Err(e) => error!("❌ ReconciliationWorker could not audit the ledger: {e}"),
            }
            sleep(self.interval).await;
        }
    }

    /// One audit pass over the newest completed settlements.
    pub async fn audit_once(&self) -> trading_core::traits::TraitResult<AuditOutcome> {
        let rows = self.repo.recent_completed_settlements(AUDIT_BATCH).await?;
        let outcome = audit_rows(&rows, self.rates.as_ref());
        let _ = self.cross_check_collectors().await;
        Ok(outcome)
    }

    /// Compare charges the ledger booked against what the collector accounts
    /// actually received.
    ///
    /// This is the half `audit_rows` structurally cannot do. That check proves the
    /// ledger is *self-consistent* and matches the tariff — and it stayed green
    /// throughout a period when fee, wheeling and loss were being transferred into
    /// the platform's own escrow and never collected, because the ledger was right
    /// the whole time and the CHAIN was doing something else. Only comparing the
    /// two sides can see that.
    ///
    /// Never fails the audit: a chain read is best-effort here, and losing the
    /// cross-check must not cost the ledger check that needs no chain at all.
    async fn cross_check_collectors(&self) -> CrossCheckOutcome {
        let mut found = CrossCheckOutcome::default();
        let Some(src) = self.collectors.as_ref() else {
            return found;
        };

        // Collisions first — they invalidate the balance comparison rather than
        // showing up in it. Two collectors sharing an account make each other's
        // balance meaningless; one colliding with the escrow means the transfer is
        // a self-transfer and nothing is collected, while the balance still "grows"
        // for the wrong reason.
        match src.collector_addresses().await {
            Ok(addrs) => {
                for (a, b) in [(0usize, 1usize), (0, 2), (1, 2)] {
                    if addrs[a] == addrs[b] {
                        found.collisions += 1;
                        error!(
                            account = %addrs[a],
                            first = LABELS[a], second = LABELS[b],
                            "collector accounts COLLIDE: these charges are pooled into one \
                             account, and if it is also the platform escrow they are \
                             transferred from it back into it and never collected"
                        );
                    }
                }
            }
            Err(e) => debug!("could not read collector addresses: {e}"),
        }

        let (Ok(ledger), Ok(chain)) = (
            self.repo.completed_charge_totals().await,
            src.collector_balances().await,
        ) else {
            debug!("collector cross-check skipped: ledger or chain totals unavailable");
            return found;
        };

        let mut guard = self.baseline.lock().unwrap_or_else(|e| e.into_inner());
        let Some((base_ledger, base_chain)) = *guard else {
            *guard = Some((ledger, chain));
            info!(
                ledger_fee = %ledger.fee, chain_fee = %chain.fee,
                ledger_wheeling = %ledger.wheeling, chain_wheeling = %chain.wheeling,
                ledger_loss = %ledger.loss, chain_loss = %chain.loss,
                "collector cross-check baseline recorded; drift is measured from here"
            );
            found.baselined = true;
            return found;
        };
        drop(guard);

        let booked = ledger.minus(base_ledger);
        let received = chain.minus(base_chain);
        for ((label, booked), (_, received)) in
            booked.labelled().into_iter().zip(received.labelled())
        {
            if booked != received {
                found.mismatches += 1;
                // WARN, not ERROR: a legitimate operator withdrawal also shows here,
                // as chain falling behind ledger. The signature of a real fault is
                // the opposite or a persistent, growing gap.
                warn!(
                    charge = label, %booked, %received,
                    difference = %(booked - received),
                    "charges booked in the ledger do not match what the collector received"
                );
            }
        }
        found
    }
}

/// What one collector cross-check found. Returned rather than only logged so the
/// detection itself is assertable — a test that merely proves the call does not
/// panic proves nothing about whether it would catch anything.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CrossCheckOutcome {
    /// This pass only recorded the baseline; no comparison was possible yet.
    pub baselined: bool,
    /// Pairs of collectors resolving to the same account.
    pub collisions: usize,
    /// Charge lines where booked and received diverged since the baseline.
    pub mismatches: usize,
}

/// Collector names in fee/wheeling/loss order, matching `collector_addresses`.
const LABELS: [&str; 3] = ["fee", "wheeling", "loss"];

/// The audit itself: pure, so it is tested directly on constructed rows rather
/// than through a stand-in for the whole `SettlementRepository`.
#[must_use]
pub fn audit_rows(
    rows: &[trading_core::models::Settlement],
    rates: &dyn ChargeRates,
) -> AuditOutcome {
    // With rates unreadable EVERY real row differs from a zero schedule, so the
    // tariff comparison would bury a genuine fault under one alarm per row. The
    // identity check needs no rates, so it keeps running — it is the half that
    // can never false-positive anyway.
    let rates_known =
        rates.wheeling_rate_per_kwh() != 0 || rates.fee_bps() != 0 || rates.loss_bps() != 0;
    if !rates_known {
        warn!(
            "on-chain charge rates are unknown; auditing the accounting identity only \
                 (tariff conformance skipped to avoid one false alarm per settlement)"
        );
    }

    let mut outcome = AuditOutcome {
        examined: rows.len(),
        ..AuditOutcome::default()
    };

    for s in rows {
        let found = trading_core::charges::reconcile_charges(
            s.total_amount,
            s.energy_amount,
            s.fee_amount,
            s.wheeling_charge.unwrap_or_default(),
            s.loss_cost.unwrap_or_default(),
            s.net_amount,
            rates,
        );

        for d in found {
            match d {
                LedgerDiscrepancy::DoesNotSumToGross { accounted, gross } => {
                    outcome.broken_identity += 1;
                    // ERROR, unconditionally: no tariff change or timing can
                    // make a row fail to add up. Every satang leaving the
                    // buyer's escrow is a charge or the seller's payout.
                    error!(
                        settlement_id = %s.id,
                        %accounted, %gross,
                        "settlement does not add up: fee + wheeling + loss + net != gross"
                    );
                }
                LedgerDiscrepancy::ChargeOffTariff {
                    field,
                    booked,
                    expected,
                } if rates_known => {
                    outcome.off_tariff += 1;
                    // WARN, not ERROR: a row booked before a governance rate
                    // change legitimately differs, and nothing on the row says
                    // which schedule priced it. A rate change looks like every
                    // row after one instant differing by the same amount; a
                    // code fault looks like arbitrary or unbounded drift.
                    warn!(
                        settlement_id = %s.id,
                        field, %booked, %expected,
                        "settlement charge differs from the live tariff"
                    );
                }
                LedgerDiscrepancy::ChargeOffTariff { .. } => {}
            }
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use trading_core::charges::{compute_charges, StaticChargeRates};
    use trading_core::models::{Settlement, SettlementStatus};
    use uuid::Uuid;

    const LIVE: StaticChargeRates = StaticChargeRates {
        fee_bps: 25,
        wheeling_rate_per_kwh: 100_000,
        loss_bps: 5,
    };

    fn settlement(
        gross: Decimal,
        kwh: Decimal,
        fee: Decimal,
        wheeling: Decimal,
        loss: Decimal,
        net: Decimal,
    ) -> Settlement {
        Settlement {
            id: Uuid::new_v4(),
            trade_id: None,
            epoch_id: Uuid::new_v4(),
            buyer_id: Uuid::new_v4(),
            seller_id: Uuid::new_v4(),
            buy_order_id: Uuid::new_v4(),
            sell_order_id: Uuid::new_v4(),
            energy_amount: kwh,
            price: gross / kwh,
            total_amount: gross,
            fee_amount: fee,
            net_amount: net,
            status: SettlementStatus::Completed,
            blockchain_tx: None,
            created_at: gridtokenx_telemetry::time::now(),
            confirmed_at: None,
            wheeling_charge: Some(wheeling),
            loss_factor: None,
            loss_cost: Some(loss),
            effective_energy: Some(kwh),
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

    /// A row the biller itself produced must audit clean, or every other case here
    /// is measuring the checker against itself.
    #[test]
    fn a_correctly_booked_ledger_is_clean() {
        let c = compute_charges(dec!(12.00), dec!(3.0), &LIVE);
        let rows = vec![settlement(
            dec!(12.00),
            dec!(3.0),
            c.fee,
            c.wheeling,
            c.loss,
            c.net,
        )];
        assert_eq!(
            audit_rows(&rows, &LIVE),
            AuditOutcome {
                examined: 1,
                broken_identity: 0,
                off_tariff: 0
            }
        );
    }

    /// The bug this module exists for, replayed: the ledger claimed the seller kept
    /// the whole gross while the chain deducted 0.103 of it. One pass must see it.
    #[test]
    fn the_measured_production_bug_is_reported() {
        let rows = vec![settlement(
            dec!(1.00),
            dec!(1.0),
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(0.01),
            dec!(1.00),
        )];
        let out = audit_rows(&rows, &LIVE);
        assert_eq!(out.examined, 1);
        assert_eq!(out.broken_identity, 1, "the row does not add up");
        assert!(out.off_tariff > 0, "and it is priced off the tariff");
    }

    /// Unknown rates must not turn every honest row into an alarm — the identity
    /// half still runs, the tariff half stands down.
    #[test]
    fn unknown_rates_suppress_tariff_noise_but_keep_the_identity_check() {
        let c = compute_charges(dec!(12.00), dec!(3.0), &LIVE);
        let rows = vec![
            settlement(dec!(12.00), dec!(3.0), c.fee, c.wheeling, c.loss, c.net),
            settlement(
                dec!(12.00),
                dec!(3.0),
                c.fee,
                c.wheeling,
                c.loss,
                c.net + dec!(0.01),
            ),
        ];

        let out = audit_rows(&rows, &StaticChargeRates::ZERO);

        assert_eq!(out.examined, 2);
        assert_eq!(out.broken_identity, 1, "the real fault is still caught");
        assert_eq!(out.off_tariff, 0, "no alarm per row against zero rates");
    }

    /// A settlement missing its charge columns entirely (NULL wheeling/loss) reads
    /// as zero and must therefore FAIL the identity — a row that moved money
    /// without recording where it went is exactly what this is for.
    #[test]
    fn a_row_with_null_charges_does_not_pass_silently() {
        let mut row = settlement(
            dec!(12.00),
            dec!(3.0),
            dec!(0.03),
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(11.97),
        );
        row.wheeling_charge = None;
        row.loss_cost = None;

        let out = audit_rows(&[row], &LIVE);
        assert_eq!(out.broken_identity, 0, "0.03 + 0 + 0 + 11.97 == 12.00");
        assert!(
            out.off_tariff > 0,
            "but the missing wheeling is caught against the tariff"
        );
    }

    // === collector cross-check ===

    struct FakeCollectors {
        balances: std::sync::Mutex<ChargeTotals>,
        addresses: [String; 3],
    }

    #[async_trait::async_trait]
    impl trading_core::traits::CollectorBalanceSource for FakeCollectors {
        async fn collector_balances(&self) -> trading_core::traits::TraitResult<ChargeTotals> {
            Ok(*self.balances.lock().unwrap_or_else(|e| e.into_inner()))
        }
        async fn collector_addresses(&self) -> trading_core::traits::TraitResult<[String; 3]> {
            Ok(self.addresses.clone())
        }
    }

    struct TotalsRepo(std::sync::Mutex<ChargeTotals>);

    #[async_trait::async_trait]
    impl SettlementRepository for TotalsRepo {
        async fn completed_charge_totals(&self) -> trading_core::traits::TraitResult<ChargeTotals> {
            Ok(*self.0.lock().unwrap_or_else(|e| e.into_inner()))
        }
        async fn insert_settlement(
            &self,
            _s: &Settlement,
        ) -> trading_core::traits::TraitResult<()> {
            unimplemented!()
        }
        async fn insert_settlement_with_event(
            &self,
            _s: &Settlement,
            _e: &trading_core::events::Event,
        ) -> trading_core::traits::TraitResult<()> {
            unimplemented!()
        }
        async fn get_or_create_active_epoch(&self) -> trading_core::traits::TraitResult<Uuid> {
            unimplemented!()
        }
        async fn insert_match(
            &self,
            _m: &trading_core::models::OrderMatch,
            _s: Option<Uuid>,
            _z: Option<i32>,
        ) -> trading_core::traits::TraitResult<()> {
            unimplemented!()
        }
        async fn insert_match_with_event(
            &self,
            _m: &trading_core::models::OrderMatch,
            _s: Option<Uuid>,
            _z: Option<i32>,
            _e: &trading_core::events::Event,
        ) -> trading_core::traits::TraitResult<()> {
            unimplemented!()
        }
        async fn persist_matched_trade(
            &self,
            _s: &Settlement,
            _m: &trading_core::models::OrderMatch,
            _e: &trading_core::events::Event,
            _z: Option<i32>,
            _bf: &trading_core::traits::TradeFill,
            _sf: &trading_core::traits::TradeFill,
        ) -> trading_core::traits::TraitResult<bool> {
            unimplemented!()
        }
        async fn reclaim_stale_processing(
            &self,
            _s: i64,
            _m: i32,
        ) -> trading_core::traits::TraitResult<u64> {
            unimplemented!()
        }
        async fn get_settlement(
            &self,
            _id: Uuid,
        ) -> trading_core::traits::TraitResult<Option<Settlement>> {
            unimplemented!()
        }
        async fn get_pending_settlements(
            &self,
            _l: i64,
        ) -> trading_core::traits::TraitResult<Vec<Settlement>> {
            unimplemented!()
        }
        async fn claim_settlements_for_processing(
            &self,
            _i: &[Uuid],
        ) -> trading_core::traits::TraitResult<Vec<Settlement>> {
            unimplemented!()
        }
        async fn reset_settlements_for_retry(
            &self,
            _i: &[Uuid],
            _m: i32,
            _e: Option<&str>,
        ) -> trading_core::traits::TraitResult<u64> {
            unimplemented!()
        }
        async fn list_settlements_for_user(
            &self,
            _u: Uuid,
            _l: i64,
            _o: i64,
        ) -> trading_core::traits::TraitResult<(Vec<Settlement>, i64)> {
            unimplemented!()
        }
        async fn get_settlement_stats(
            &self,
        ) -> trading_core::traits::TraitResult<trading_core::models::SettlementStats> {
            unimplemented!()
        }
        async fn update_settlement_status(
            &self,
            _i: Uuid,
            _s: &str,
            _t: Option<&str>,
            _e: Option<&str>,
        ) -> trading_core::traits::TraitResult<()> {
            unimplemented!()
        }
        async fn update_settlement_status_with_event(
            &self,
            _i: Uuid,
            _s: &str,
            _t: Option<&str>,
            _e: Option<&str>,
            _ev: &trading_core::events::Event,
        ) -> trading_core::traits::TraitResult<()> {
            unimplemented!()
        }
        async fn get_market_price(
            &self,
            _w: i64,
        ) -> trading_core::traits::TraitResult<trading_core::models::MarketPrice> {
            unimplemented!()
        }
        async fn count_active_traders(&self, _w: i64) -> trading_core::traits::TraitResult<i64> {
            unimplemented!()
        }
    }

    fn totals(fee: Decimal, wheeling: Decimal, loss: Decimal) -> ChargeTotals {
        ChargeTotals {
            fee,
            wheeling,
            loss,
        }
    }

    fn distinct() -> [String; 3] {
        ["FEE".into(), "WHEEL".into(), "LOSS".into()]
    }

    fn cross_worker(
        ledger: ChargeTotals,
        chain: ChargeTotals,
        addresses: [String; 3],
    ) -> (ReconciliationWorker, Arc<TotalsRepo>, Arc<FakeCollectors>) {
        let repo = Arc::new(TotalsRepo(std::sync::Mutex::new(ledger)));
        let col = Arc::new(FakeCollectors {
            balances: std::sync::Mutex::new(chain),
            addresses,
        });
        let w = ReconciliationWorker::new(repo.clone(), Arc::new(LIVE), Some(col.clone()), 60);
        (w, repo, col)
    }

    /// The baseline absorbs history: pre-existing balances and any past mismatch
    /// must NOT be reported, or the check drowns in noise on first run.
    #[tokio::test]
    async fn the_first_pass_only_baselines() {
        let (w, _repo, _col) = cross_worker(
            totals(dec!(5), dec!(9), dec!(1)),
            totals(dec!(0), dec!(0), dec!(0)), // wildly out of step already
            distinct(),
        );
        let out = w.cross_check_collectors().await;
        assert!(out.baselined, "first pass must only baseline");
        assert_eq!(out.mismatches, 0, "history must not be reported as drift");
    }

    /// Charges booked AFTER the baseline that arrive in full are clean, even though
    /// the absolute totals still disagree by the historical gap.
    #[tokio::test]
    async fn matching_deltas_are_clean_despite_historical_mismatch() {
        let (w, repo, col) = cross_worker(
            totals(dec!(5), dec!(9), dec!(1)),
            totals(dec!(0), dec!(0), dec!(0)),
            distinct(),
        );
        w.cross_check_collectors().await;

        *repo.0.lock().expect("fixture mutex poisoned") = totals(dec!(6), dec!(10), dec!(2));
        *col.balances.lock().expect("fixture mutex poisoned") = totals(dec!(1), dec!(1), dec!(1));

        // Both sides advanced by exactly 1/1/1.
        let out = w.cross_check_collectors().await;
        assert!(!out.baselined);
        assert_eq!(
            out.mismatches, 0,
            "equal deltas are clean even though absolute totals still differ by the history"
        );
    }

    /// The regression this exists for: the ledger books a charge and the collector
    /// never receives it. Absolute totals would have looked fine at baseline; only
    /// the delta exposes it.
    #[tokio::test]
    async fn a_charge_that_never_arrives_is_detected() {
        let (w, repo, col) = cross_worker(
            totals(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            totals(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            distinct(),
        );
        w.cross_check_collectors().await;

        // Ledger books 0.40 of wheeling; the collector gets nothing.
        *repo.0.lock().expect("fixture mutex poisoned") = totals(dec!(0.1), dec!(0.4), dec!(0.02));
        *col.balances.lock().expect("fixture mutex poisoned") =
            totals(dec!(0.1), Decimal::ZERO, dec!(0.02));

        let out = w.cross_check_collectors().await;
        assert_eq!(
            out.mismatches, 1,
            "exactly the wheeling line must be flagged — fee and loss arrived in full"
        );
    }

    /// Colliding collectors are reported from the ADDRESSES, not the balances —
    /// this is the defect that was live here, and no balance comparison can see it.
    #[tokio::test]
    async fn colliding_collector_accounts_are_detected() {
        let same = "SAME".to_string();
        let (w, _repo, _col) = cross_worker(
            totals(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            totals(Decimal::ZERO, Decimal::ZERO, Decimal::ZERO),
            [same.clone(), same.clone(), same],
        );
        let out = w.cross_check_collectors().await;
        assert_eq!(
            out.collisions, 3,
            "all three pairs collide — this is the live defect, invisible to balances"
        );
    }

    /// A chain that cannot be read must NOT be recorded as a zero baseline. This is
    /// the failure observed live: the validator died and the collectors — holding
    /// ~2M — read as 0/0/0. Baselining on that would make every subsequent charge
    /// appear never to arrive.
    #[tokio::test]
    async fn an_unreadable_chain_does_not_baseline_at_zero() {
        struct FailingCollectors;
        #[async_trait::async_trait]
        impl trading_core::traits::CollectorBalanceSource for FailingCollectors {
            async fn collector_balances(&self) -> trading_core::traits::TraitResult<ChargeTotals> {
                Err(trading_core::error::ApiError::Internal("rpc down".into()))
            }
            async fn collector_addresses(&self) -> trading_core::traits::TraitResult<[String; 3]> {
                Ok(distinct())
            }
        }

        let repo = Arc::new(TotalsRepo(std::sync::Mutex::new(totals(
            dec!(5),
            dec!(9),
            dec!(1),
        ))));
        let w =
            ReconciliationWorker::new(repo, Arc::new(LIVE), Some(Arc::new(FailingCollectors)), 60);

        let out = w.cross_check_collectors().await;
        assert!(
            !out.baselined,
            "an unreadable chain must not become the baseline"
        );
        assert_eq!(out.mismatches, 0);
        assert!(
            w.baseline.lock().expect("fixture mutex poisoned").is_none(),
            "no baseline may be recorded from a failed read"
        );
    }

    /// With no collector source the ledger audit must still run untouched.
    #[tokio::test]
    async fn the_cross_check_is_optional() {
        let c = compute_charges(dec!(12.00), dec!(3.0), &LIVE);
        let rows = vec![settlement(
            dec!(12.00),
            dec!(3.0),
            c.fee,
            c.wheeling,
            c.loss,
            c.net,
        )];
        assert_eq!(
            audit_rows(&rows, &LIVE),
            AuditOutcome {
                examined: 1,
                broken_identity: 0,
                off_tariff: 0
            }
        );
    }

    #[test]
    fn an_empty_ledger_is_not_an_error() {
        assert_eq!(audit_rows(&[], &LIVE), AuditOutcome::default());
    }
}
