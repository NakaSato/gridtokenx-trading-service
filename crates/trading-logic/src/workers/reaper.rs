use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info};
use trading_core::traits::OrderRepository;

/// Background worker that reaps expired orders: marks every open order past its
/// `expires_at` as `Expired` (segment-agnostic). Decoupled from the matcher so
/// it runs in every deployment role — an `api`-only node with no matcher still
/// reaps. The reaper uses the telemetry (NTP) clock, the same clock `expires_at`
/// was written with, so the expiry comparison is clock-consistent.
pub struct ReaperWorker {
    order_repo: Arc<dyn OrderRepository>,
    interval: Duration,
}

impl ReaperWorker {
    pub fn new(order_repo: Arc<dyn OrderRepository>, interval: Duration) -> Self {
        Self {
            order_repo,
            interval,
        }
    }

    pub async fn run(self) {
        info!("🚀 Starting ReaperWorker loop (interval: {:?})", self.interval);
        let mut ticker = interval(self.interval);

        loop {
            ticker.tick().await;

            match self
                .order_repo
                .expire_stale_orders(gridtokenx_telemetry::time::now())
                .await
            {
                Ok(reaped) if !reaped.is_empty() => {
                    info!("Reaped {} expired order(s)", reaped.len());
                }
                Ok(_) => {}
                Err(e) => {
                    error!("Error reaping expired orders: {}", e);
                }
            }
        }
    }
}
