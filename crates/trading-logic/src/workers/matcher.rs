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

            let cycle_started = std::time::Instant::now();
            match self.service.run_matching_cycle().await {
                Ok(count) => {
                    trading_infra::metrics::record_matching_cycle_result(
                        cycle_started.elapsed().as_secs_f64() * 1000.0,
                        count as u64,
                    );
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
