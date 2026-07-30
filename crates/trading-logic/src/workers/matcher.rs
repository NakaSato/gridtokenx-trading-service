use crate::matcher_service::MatcherService;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{error, info};

/// Drives [`MatcherService::run_matching_cycle`].
///
/// Two things can start a cycle:
///
/// 1. **An order arriving** — submit paths call `MatcherService::request_cycle`,
///    which wakes this loop immediately. This is what makes matching realtime:
///    a crossing pair matches ~`debounce` after the second order lands, not up
///    to `interval` later.
/// 2. **The fallback tick** (`interval`) — covers what arrivals don't: orders
///    inserted by another replica, and any future insert path that forgets to
///    wake the matcher. (The in-process order sources — REST, gRPC and
///    `RecurringEvaluator` — all wake it, so the tick is a safety net, not the
///    mechanism.)
///
/// Expiry deliberately does NOT wake the matcher, and needs no tick of its own:
/// reaping only ever *removes* liquidity, and removing an order cannot create a
/// crossing pair. Nor does a match wait on the reaper — the engine skips any
/// expired order it is handed (`FastOrder::is_expired`,
/// `trading-engine/src/engine.rs:438` for buys, `:412` for sells), so an
/// expired-but-not-yet-reaped order is already unmatchable.
///
/// Either way the first `interval` tick fires immediately, so one cycle always
/// runs at boot — the book may already be crossable from before the restart.
///
/// `debounce` both batches an arrival burst into one cycle and floors the gap
/// between cycles; a cycle re-reads the entire active book, so the floor is what
/// keeps a high order rate from turning into one full book scan per order.
pub struct MatcherWorker {
    service: Arc<MatcherService>,
    interval: Duration,
    /// `None` = polling only: never await the wake-up channel (`MATCHER_REALTIME=false`).
    debounce: Option<Duration>,
}

impl MatcherWorker {
    /// Polling-only worker — kept for tests and for callers that want the
    /// pre-realtime behaviour; production builds via [`Self::realtime`].
    pub fn new(service: Arc<MatcherService>, interval: Duration) -> Self {
        Self {
            service,
            interval,
            debounce: None,
        }
    }

    /// Event-driven worker: cycles on order arrival (coalesced over `debounce`)
    /// with `interval` as the fallback tick.
    pub fn realtime(service: Arc<MatcherService>, interval: Duration, debounce: Duration) -> Self {
        Self {
            service,
            interval,
            debounce: Some(debounce),
        }
    }

    pub async fn run(self) {
        if let Some(d) = self.debounce {
            info!(
                "🚀 Starting MatcherWorker (realtime: wake on order arrival, debounce {:?}, fallback tick {:?})",
                d, self.interval
            );
        } else {
            info!(
                "🚀 Starting MatcherWorker loop (polling only, interval: {:?})",
                self.interval
            );
        }

        let mut ticker = interval(self.interval);
        // A cycle can outlast the tick (DB reads + persist). Default `Burst`
        // behaviour would then fire the backlog of missed ticks back-to-back
        // with no delay; `Delay` re-bases the schedule so a slow cycle degrades
        // to "run again promptly", not "run N times immediately".
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            match self.debounce {
                Some(debounce) => {
                    tokio::select! {
                        // Biased so a pending arrival is served before the tick
                        // when both are ready: the tick would run the same
                        // cycle, then consume the wake-up permit for a second,
                        // redundant scan of an unchanged book.
                        biased;
                        () = self.service.cycle_requested() => {
                            // Sleep before matching so a burst of arrivals
                            // collapses into one cycle. Because this sleep sits
                            // after the previous cycle finished, it is also the
                            // cycle-rate floor: consecutive cycles are always
                            // >= `debounce` apart, no separate bookkeeping.
                            //
                            // Arrivals during the cycle (or during this sleep)
                            // leave a permit behind, so they get their own
                            // follow-up cycle. That can mean one extra no-match
                            // scan at the tail of a burst — cheap, and the
                            // alternative (dropping the permit) risks losing a
                            // late arrival's wake-up entirely.
                            if !debounce.is_zero() {
                                tokio::time::sleep(debounce).await;
                            }
                        }
                        _ = ticker.tick() => {}
                    }
                }
                None => {
                    ticker.tick().await;
                }
            }

            let cycle_started = Instant::now();
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
