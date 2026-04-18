use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, Interval};
use tracing::{info, error};
use crate::settlement::SettlementService;

pub struct SettlementWorker {
    service: Arc<SettlementService>,
    interval: Duration,
    batch_limit: i64,
}

impl SettlementWorker {
    pub fn new(service: Arc<SettlementService>, interval: Duration, batch_limit: i64) -> Self {
        Self { service, interval, batch_limit }
    }

    pub async fn run(self) {
        info!("🚀 Starting SettlementWorker loop (interval: {:?}, limit: {})", self.interval, self.batch_limit);
        let mut ticker = interval(self.interval);

        loop {
            ticker.tick().await;
            
            match self.service.process_pending_settlements(self.batch_limit).await {
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
