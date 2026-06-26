use crate::clearing::ClearingService;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};

/// Background worker for the uniform-price (interval) trading mechanism. On each
/// tick it asks the [`ClearingService`] to clear and close every epoch whose
/// 15-minute window has elapsed. Polling (rather than firing exactly on the
/// boundary) keeps it robust to restarts and clock drift: a due epoch is picked
/// up on the next tick regardless of when the process came up.
pub struct ClearingWorker {
    service: Arc<ClearingService>,
    interval: Duration,
}

impl ClearingWorker {
    pub fn new(service: Arc<ClearingService>, interval: Duration) -> Self {
        Self { service, interval }
    }

    pub async fn run(self) {
        info!(
            "🚀 Starting ClearingWorker loop (interval: {:?})",
            self.interval
        );
        let mut ticker = interval(self.interval);

        loop {
            ticker.tick().await;

            match self.service.clear_due_epochs().await {
                Ok(summaries) if !summaries.is_empty() => {
                    let matches: usize = summaries.iter().map(|s| s.matches).sum();
                    info!(
                        "Cleared {} epoch(s) with {} total match(es)",
                        summaries.len(),
                        matches
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    error!("Error clearing due epochs: {}", e);
                }
            }
        }
    }
}
