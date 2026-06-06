use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use trading_core::traits::{BlockchainGateway, TraitResult};

/// Cap on the backoff delay applied after repeated sync failures.
const MAX_BACKOFF: Duration = Duration::from_secs(30 * 60);

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
                        debug!(
                            "SupplySyncWorker failure #{}: {}",
                            consecutive_failures, e
                        );
                    }

                    // Exponential backoff: interval * 2^(n-1), capped.
                    let backoff = self
                        .interval
                        .checked_mul(1u32 << (consecutive_failures - 1).min(16))
                        .unwrap_or(MAX_BACKOFF)
                        .min(MAX_BACKOFF);

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
