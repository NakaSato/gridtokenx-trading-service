use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};
use trading_core::traits::{BlockchainGateway, TraitResult};

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

        let mut ticker = interval(self.interval);

        loop {
            ticker.tick().await;

            if let Err(e) = self.sync_supply().await {
                error!("❌ SupplySyncWorker failed: {}", e);
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
