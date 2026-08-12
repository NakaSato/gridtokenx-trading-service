//! Keeps the live fee/wheeling/loss schedule in step with the chain.
//!
//! These rates were read once at boot, which was fine while they only decided
//! what the `settlements` ledger *recorded*. They now also decide the landed cost
//! the matcher crosses on, the minimum settleable ask the submit edges refuse
//! below, and the quote customers are shown — so a governance rate change used to
//! desynchronise four things from the chain until someone restarted the service,
//! and a failed boot read left the price floor disabled for just as long.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use trading_core::charges::RefreshingChargeRates;
use trading_core::traits::ChargeRatesSource;

/// Cap on the backoff applied after repeated read failures, matching
/// `SupplySyncWorker` — a persistent misconfiguration should cost one warning and
/// then go quiet, not one error per tick forever.
const MAX_BACKOFF: Duration = Duration::from_secs(30 * 60);

/// Polls the on-chain tariff and publishes it to a [`RefreshingChargeRates`].
pub struct ChargeRatesWorker {
    source: Arc<dyn ChargeRatesSource>,
    rates: Arc<RefreshingChargeRates>,
    interval: Duration,
}

impl ChargeRatesWorker {
    #[must_use]
    pub fn new(
        source: Arc<dyn ChargeRatesSource>,
        rates: Arc<RefreshingChargeRates>,
        interval_secs: u64,
    ) -> Self {
        Self {
            source,
            rates,
            interval: Duration::from_secs(interval_secs),
        }
    }

    /// Poll until cancelled. Never returns under normal operation.
    pub async fn run(&self) {
        info!(
            "🚀 Starting ChargeRatesWorker loop (interval: {:?})",
            self.interval
        );
        let mut consecutive_failures: u32 = 0;

        loop {
            match self.refresh_once().await {
                Ok(()) => {
                    if consecutive_failures > 0 {
                        info!(
                            "✅ ChargeRatesWorker recovered after {} failed attempt(s)",
                            consecutive_failures
                        );
                    }
                    consecutive_failures = 0;
                    sleep(self.interval).await;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    if consecutive_failures == 1 {
                        error!(
                            "❌ ChargeRatesWorker failed to read the on-chain tariff: {e} — \
                             keeping the last known rates; further failures logged at debug"
                        );
                    } else {
                        debug!("ChargeRatesWorker failure #{consecutive_failures}: {e}");
                    }
                    let backoff = self
                        .interval
                        .saturating_mul(1u32 << consecutive_failures.min(5))
                        .min(MAX_BACKOFF);
                    sleep(backoff).await;
                }
            }
        }
    }

    /// One poll. A failure leaves the previous rates in force — reverting to zeros
    /// would silently switch the sell-price floor off on an RPC blip, which is
    /// strictly worse than serving rates that are a few minutes stale.
    pub async fn refresh_once(&self) -> trading_core::traits::TraitResult<()> {
        let next = self.source.read_charge_rates().await?;
        let previous = self.rates.store(next);
        if previous != next {
            // A rate change is a governance event; make it visible rather than
            // letting four downstream behaviours shift with no trace of why.
            warn!(
                fee_bps = next.fee_bps,
                wheeling_rate_per_kwh = next.wheeling_rate_per_kwh,
                loss_bps = next.loss_bps,
                previous_fee_bps = previous.fee_bps,
                previous_wheeling_rate_per_kwh = previous.wheeling_rate_per_kwh,
                previous_loss_bps = previous.loss_bps,
                "on-chain charge rates changed — matcher, ledger, sell-price floor and \
                 quotes now use the new schedule"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;
    use trading_core::charges::{ChargeRates, StaticChargeRates};
    use trading_core::error::ApiError;

    const LIVE: StaticChargeRates = StaticChargeRates {
        fee_bps: 25,
        wheeling_rate_per_kwh: 100_000,
        loss_bps: 5,
    };
    const RAISED: StaticChargeRates = StaticChargeRates {
        fee_bps: 25,
        wheeling_rate_per_kwh: 250_000,
        loss_bps: 5,
    };

    /// Serves a queued script of answers; the last one repeats.
    struct FakeSource(Mutex<Vec<Result<StaticChargeRates, String>>>);

    #[async_trait]
    impl ChargeRatesSource for FakeSource {
        async fn read_charge_rates(&self) -> trading_core::traits::TraitResult<StaticChargeRates> {
            let mut q = self.0.lock().unwrap_or_else(|e| e.into_inner());
            let next = if q.len() > 1 {
                q.remove(0)
            } else {
                q[0].clone()
            };
            next.map_err(ApiError::Blockchain)
        }
    }

    fn worker(
        script: Vec<Result<StaticChargeRates, String>>,
        initial: StaticChargeRates,
    ) -> (ChargeRatesWorker, Arc<RefreshingChargeRates>) {
        let rates = Arc::new(RefreshingChargeRates::new(initial));
        let w = ChargeRatesWorker::new(Arc::new(FakeSource(Mutex::new(script))), rates.clone(), 60);
        (w, rates)
    }

    /// A rate the operator changed on chain must reach the live schedule.
    #[tokio::test]
    async fn a_refresh_publishes_the_new_schedule() {
        let (w, rates) = worker(vec![Ok(RAISED)], LIVE);
        assert_eq!(rates.wheeling_rate_per_kwh(), 100_000);

        w.refresh_once().await.expect("read succeeds");

        assert_eq!(rates.wheeling_rate_per_kwh(), 250_000);
        assert_eq!(rates.snapshot(), RAISED);
    }

    /// The regression that makes polling safe: a failed read must not zero the
    /// schedule, because zero wheeling means `min_settleable_price_per_kwh` returns
    /// a floor of 0 and the submit edges stop refusing unsettleable asks.
    #[tokio::test]
    async fn a_failed_refresh_keeps_the_last_known_rates() {
        let (w, rates) = worker(vec![Err("connection refused".into())], LIVE);

        w.refresh_once().await.expect_err("read fails");

        assert_eq!(
            rates.snapshot(),
            LIVE,
            "a blip must not disable the price floor"
        );
    }

    /// The other half of the boot-time gap: a service that started with ZERO
    /// (on-chain read unavailable) must heal on the first successful poll rather
    /// than run unpriced until someone restarts it.
    #[tokio::test]
    async fn a_zero_boot_read_heals_on_the_first_success() {
        let (w, rates) = worker(
            vec![Err("node is behind".into()), Ok(LIVE)],
            StaticChargeRates::ZERO,
        );
        assert_eq!(rates.snapshot(), StaticChargeRates::ZERO);

        w.refresh_once().await.expect_err("first poll fails");
        assert_eq!(rates.snapshot(), StaticChargeRates::ZERO);

        w.refresh_once().await.expect("second poll succeeds");
        assert_eq!(rates.snapshot(), LIVE, "healed without a restart");
    }

    /// Republishing identical rates is the common case and must be a no-op the log
    /// stays quiet about — the change warning is only useful if it means something.
    #[tokio::test]
    async fn an_unchanged_refresh_leaves_the_schedule_alone() {
        let (w, rates) = worker(vec![Ok(LIVE)], LIVE);
        w.refresh_once().await.expect("read succeeds");
        assert_eq!(rates.snapshot(), LIVE);
    }
}
