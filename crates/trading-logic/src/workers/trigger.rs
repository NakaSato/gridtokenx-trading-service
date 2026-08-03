use crate::trigger_evaluator::TriggerEvaluator;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

/// Periodically drives [`TriggerEvaluator::run_cycle`] to fire price alerts
/// against the current market price.
pub struct TriggerEvaluatorWorker {
    service: Arc<TriggerEvaluator>,
    interval: Duration,
}

impl TriggerEvaluatorWorker {
    #[must_use]
    pub fn new(service: Arc<TriggerEvaluator>, interval: Duration) -> Self {
        Self { service, interval }
    }

    pub async fn run(self) {
        info!(
            "🔔 Starting TriggerEvaluatorWorker loop (interval: {:?})",
            self.interval
        );
        let mut ticker = interval(self.interval);
        loop {
            ticker.tick().await;
            if let Err(e) = self.service.run_cycle().await {
                error!("Error in trigger evaluator cycle: {}", e);
            }
        }
    }
}
