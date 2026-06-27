use std::sync::Arc;
use std::time::Duration;
use metrics::{counter, gauge};
use tokio::time::interval;
use tracing::{error, info};
use trading_core::traits::OrderRepository;

/// After this many consecutive failed reap ticks, expiry is effectively stalled
/// (the DB-side queries no longer filter expired orders, so they keep matching).
/// Escalate the log to make the stall unmissable; the metric alerts before this.
const STALL_ESCALATION_THRESHOLD: u32 = 3;

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
        // Tracks consecutive failures so a persistent DB fault — which silently
        // stops all expiry, since the get_active queries no longer filter
        // expired rows — surfaces as both an escalating log and a gauge.
        let mut consecutive_failures: u32 = 0;

        loop {
            ticker.tick().await;

            // Bound the DB call: a hung-but-not-erroring connection would otherwise
            // block this loop forever and silently stop all expiry (the supervisor
            // can't see a stuck-alive task). On timeout we treat it as a failure and
            // tick again rather than wait indefinitely. Cap at 2x the interval so a
            // merely-slow tick isn't killed.
            let deadline = self.interval.saturating_mul(2);
            let outcome = tokio::time::timeout(
                deadline,
                self.order_repo
                    .expire_stale_orders(gridtokenx_telemetry::time::now()),
            )
            .await;

            // Collapse (timeout, db-error, ok) into a single Result for one
            // success/failure handling path.
            let result = match outcome {
                Ok(inner) => inner.map_err(|e| e.to_string()),
                Err(_elapsed) => Err(format!("expire_stale_orders timed out after {deadline:?}")),
            };

            match result {
                Ok(reaped) => {
                    consecutive_failures = 0;
                    gauge!("trading_reaper_consecutive_failures").set(0.0);
                    if !reaped.is_empty() {
                        counter!("trading_reaper_orders_expired_total")
                            .increment(reaped.len() as u64);
                        info!("Reaped {} expired order(s)", reaped.len());
                    }
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    counter!("trading_reaper_failures_total").increment(1);
                    gauge!("trading_reaper_consecutive_failures")
                        .set(f64::from(consecutive_failures));
                    // Expiry is now the ONLY mechanism that drops expired orders
                    // from the active set, so a sustained failure means expired
                    // orders keep matching. Escalate once it's clearly stuck.
                    if consecutive_failures >= STALL_ESCALATION_THRESHOLD {
                        error!(
                            consecutive_failures,
                            "Order expiry STALLED: reaping has failed {consecutive_failures} times in a row — expired orders remain matchable: {e}"
                        );
                    } else {
                        error!("Error reaping expired orders: {e}");
                    }
                }
            }
        }
    }
}
