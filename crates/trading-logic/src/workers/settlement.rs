use crate::settlement::SettlementService;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

pub struct SettlementWorker {
    service: Arc<SettlementService>,
    interval: Duration,
    batch_limit: i64,
}

impl SettlementWorker {
    #[must_use]
    pub fn new(service: Arc<SettlementService>, interval: Duration, batch_limit: i64) -> Self {
        Self {
            service,
            interval,
            batch_limit,
        }
    }

    pub async fn run(self) {
        info!(
            "🚀 Starting SettlementWorker loop (interval: {:?}, limit: {})",
            self.interval, self.batch_limit
        );
        let mut ticker = interval(self.interval);

        loop {
            ticker.tick().await;

            // Recover settlements orphaned in `processing` (crash / partial
            // failure between claim and finalize) before claiming new work.
            match self.service.reclaim_stale_settlements().await {
                Ok(n) if n > 0 => info!("Reclaimed {} stale processing settlement(s)", n),
                Ok(_) => {}
                Err(e) => error!("Error reclaiming stale settlements: {}", e),
            }

            match self
                .service
                .process_pending_settlements(self.batch_limit)
                .await
            {
                Ok(count) => {
                    if count > 0 {
                        info!("Successfully processed {} pending settlements", count);
                    }
                }
                Err(e) => {
                    error!("Error processing pending settlements: {}", e);
                }
            }
        }
    }
}
