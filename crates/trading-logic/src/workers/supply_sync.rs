use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use trading_core::traits::{BlockchainGateway, TraitResult};

/// Cap on the backoff delay applied after repeated sync failures.
const MAX_BACKOFF: Duration = Duration::from_mins(30);

/// Worker for periodically syncing the on-chain total supply cache.
/// This fulfills the "Deferred Supply Synchronization" architecture.
pub struct SupplySyncWorker {
    blockchain: Arc<dyn BlockchainGateway>,
    interval: Duration,
}

impl SupplySyncWorker {
    pub fn new(blockchain: Arc<dyn BlockchainGateway>, interval_secs: u64) -> Self {
        Self {
            blockchain,
            interval: Duration::from_secs(interval_secs),
        }
    }

    pub async fn run(&self) {
        info!(
            "🚀 Starting SupplySyncWorker loop (interval: {:?})",
            self.interval
        );

        // Track consecutive failures so a persistent misconfiguration (e.g. no
        // authority keypair loaded) degrades to a single warning plus exponential
        // backoff, instead of emitting one error per tick forever.
        let mut consecutive_failures: u32 = 0;

        loop {
            match self.sync_supply().await {
                Ok(()) => {
                    if consecutive_failures > 0 {
                        info!(
                            "✅ SupplySyncWorker recovered after {} failed attempt(s)",
                            consecutive_failures
                        );
                    }
                    consecutive_failures = 0;
                    sleep(self.interval).await;
                }
                Err(e) => {
                    consecutive_failures += 1;
                    // Log the first failure at error, then suppress to debug to
                    // avoid log spam while backing off.
                    if consecutive_failures == 1 {
                        error!(
                            "❌ SupplySyncWorker failed: {} — backing off, further failures logged at debug",
                            e
                        );
                    } else {
                        debug!("SupplySyncWorker failure #{}: {}", consecutive_failures, e);
                    }

                    // Exponential backoff: interval * 2^(n-1), capped.
                    let backoff = backoff_delay(self.interval, consecutive_failures);

                    if consecutive_failures == 1 || backoff >= MAX_BACKOFF {
                        warn!(
                            "SupplySyncWorker backing off {:?} after {} consecutive failure(s)",
                            backoff, consecutive_failures
                        );
                    }
                    sleep(backoff).await;
                }
            }
        }
    }

    async fn sync_supply(&self) -> TraitResult<()> {
        info!("🔄 Triggering periodic total supply synchronization...");
        match self.blockchain.sync_total_supply().await {
            Ok(sig) => {
                info!("✅ Total supply synced. Signature: {}", sig);
                Ok(())
            }
            Err(e) => {
                error!("❌ Failed to sync total supply: {}", e);
                Err(e)
            }
        }
    }
}

/// Backoff for the `n`th consecutive failure: `interval * 2^(n-1)`, capped at
/// [`MAX_BACKOFF`]. The shift exponent is clamped (and `checked_mul` guards
/// against `Duration` overflow) so a long outage saturates at the cap instead of
/// wrapping or panicking.
fn backoff_delay(interval: Duration, consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(16);
    interval
        .checked_mul(1u32 << shift)
        .unwrap_or(MAX_BACKOFF)
        .min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_waits_one_interval() {
        let i = Duration::from_secs(60);
        assert_eq!(backoff_delay(i, 1), i, "2^0 = 1x interval");
    }

    #[test]
    fn backoff_doubles_per_failure() {
        let i = Duration::from_secs(10);
        assert_eq!(backoff_delay(i, 2), Duration::from_secs(20)); // 2^1
        assert_eq!(backoff_delay(i, 3), Duration::from_secs(40)); // 2^2
        assert_eq!(backoff_delay(i, 4), Duration::from_secs(80)); // 2^3
    }

    #[test]
    fn backoff_is_capped_at_max() {
        let i = Duration::from_secs(60);
        // 2^16 * 60s ≫ 30min cap.
        assert_eq!(backoff_delay(i, 50), MAX_BACKOFF);
        assert_eq!(backoff_delay(i, 17), MAX_BACKOFF);
    }

    #[test]
    fn backoff_handles_overflow_to_cap() {
        // A huge interval whose doubling overflows Duration must saturate, not panic.
        let huge = Duration::from_secs(u64::MAX / 2);
        assert_eq!(backoff_delay(huge, 10), MAX_BACKOFF);
    }
}
