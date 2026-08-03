use crate::recurring_evaluator::RecurringEvaluator;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

/// Periodically drives [`RecurringEvaluator::run_cycle`] to place due recurring
/// orders.
pub struct RecurringEvaluatorWorker {
    service: Arc<RecurringEvaluator>,
    interval: Duration,
}

impl RecurringEvaluatorWorker {
    #[must_use]
    pub fn new(service: Arc<RecurringEvaluator>, interval: Duration) -> Self {
        Self { service, interval }
    }

    pub async fn run(self) {
        info!(
            "🔁 Starting RecurringEvaluatorWorker loop (interval: {:?})",
            self.interval
        );
        let mut ticker = interval(self.interval);
        loop {
            ticker.tick().await;
            if let Err(e) = self.service.run_cycle().await {
                error!("Error in recurring evaluator cycle: {}", e);
            }
        }
    }
}
