use crate::matcher_service::MatcherService;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

pub struct MatcherWorker {
    service: Arc<MatcherService>,
    interval: Duration,
}

impl MatcherWorker {
    pub fn new(service: Arc<MatcherService>, interval: Duration) -> Self {
        Self { service, interval }
    }

    pub async fn run(self) {
        info!(
            "🚀 Starting MatcherWorker loop (interval: {:?})",
            self.interval
        );
        let mut ticker = interval(self.interval);

        loop {
            ticker.tick().await;

            match self.service.run_matching_cycle().await {
                Ok(count) => {
                    if count > 0 {
                        info!(
                            "Successfully processed matching cycle with {} matches",
                            count
                        );
                    }
                }
                Err(e) => {
                    error!("Error in matching cycle: {}", e);
                }
            }
        }
    }
}
