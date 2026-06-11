//! Price-alert trigger service.
//!
//! Each cycle it derives a single "current market price" from the live order
//! book, scans every `active` alert, and for each whose condition the price now
//! satisfies it records the firing (`mark_triggered`) and publishes a
//! `PriceAlertTriggered` event. The event flows through the outbox to the
//! notification service, which delivers it to the alert's owner. This service
//! sends no notifications directly.

use std::sync::Arc;

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{info, warn};

use trading_core::events::{Event, PriceAlertTriggeredPayload};
use trading_core::traits::{OrderRepository, PriceAlertRepository, TraitResult};
use trading_core::types::{AlertCondition, OrderSide};

/// Half-width of the band (as a fraction of target) within which a `Crosses`
/// alert is considered to have crossed its target. The service is stateless —
/// it has no previous price to detect an actual crossing — so a tight band
/// around the target approximates it.
const CROSS_BAND: Decimal = dec!(0.001); // 0.1%

/// Async orchestrator that fires price alerts against the current market price.
pub struct TriggerEvaluator {
    alert_repo: Arc<dyn PriceAlertRepository>,
    order_repo: Arc<dyn OrderRepository>,
}

impl TriggerEvaluator {
    pub fn new(
        alert_repo: Arc<dyn PriceAlertRepository>,
        order_repo: Arc<dyn OrderRepository>,
    ) -> Self {
        Self {
            alert_repo,
            order_repo,
        }
    }

    /// Fire every active alert the current market price now satisfies. Returns
    /// the number fired. No price (empty book) → no-op. One bad alert never
    /// aborts the cycle.
    pub async fn run_cycle(&self) -> TraitResult<usize> {
        let Some(price) = self.current_price().await? else {
            return Ok(0);
        };

        let alerts = self.alert_repo.get_active_alerts().await?;
        let mut fired = 0usize;
        for alert in alerts {
            if !condition_met(alert.condition, price, alert.target_price) {
                continue;
            }

            // Firing record + PriceAlertTriggered event committed in one
            // transaction so the notification cannot be lost relative to the
            // state change.
            let event = Event::PriceAlertTriggered(PriceAlertTriggeredPayload {
                alert_id: alert.id,
                user_id: alert.user_id,
                target_price: alert.target_price,
                triggered_price: price,
                condition: alert.condition.to_string(),
                triggered_at: Utc::now(),
            });
            if let Err(e) = self
                .alert_repo
                .mark_triggered_with_event(alert.id, price, &event)
                .await
            {
                warn!(alert_id = %alert.id, error = %e, "Failed to mark price alert triggered; skipping");
                continue;
            }
            fired += 1;
        }

        if fired > 0 {
            info!("Trigger evaluator fired {} price alert(s) at price {}", fired, price);
        }
        Ok(fired)
    }

    /// Current market price = midpoint of best bid (highest buy) and best ask
    /// (lowest sell) on the live book. Falls back to whichever side exists when
    /// only one does; `None` when the book is empty.
    async fn current_price(&self) -> TraitResult<Option<Decimal>> {
        let book = self.order_repo.get_all_active_orders().await?;
        let mut best_bid: Option<Decimal> = None;
        let mut best_ask: Option<Decimal> = None;
        for e in &book {
            match e.side {
                OrderSide::Buy => {
                    best_bid = Some(best_bid.map_or(e.price_per_kwh, |b| b.max(e.price_per_kwh)));
                }
                OrderSide::Sell => {
                    best_ask = Some(best_ask.map_or(e.price_per_kwh, |a| a.min(e.price_per_kwh)));
                }
            }
        }
        Ok(match (best_bid, best_ask) {
            (Some(b), Some(a)) => Some((b + a) / dec!(2)),
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        })
    }
}

/// Whether `price` satisfies `condition` relative to `target`.
fn condition_met(condition: AlertCondition, price: Decimal, target: Decimal) -> bool {
    match condition {
        AlertCondition::Above => price >= target,
        AlertCondition::Below => price <= target,
        AlertCondition::Crosses => (price - target).abs() <= target.abs() * CROSS_BAND,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn above_fires_at_or_over_target() {
        assert!(condition_met(AlertCondition::Above, dec!(0.30), dec!(0.25)));
        assert!(condition_met(AlertCondition::Above, dec!(0.25), dec!(0.25)));
        assert!(!condition_met(AlertCondition::Above, dec!(0.20), dec!(0.25)));
    }

    #[test]
    fn below_fires_at_or_under_target() {
        assert!(condition_met(AlertCondition::Below, dec!(0.20), dec!(0.25)));
        assert!(condition_met(AlertCondition::Below, dec!(0.25), dec!(0.25)));
        assert!(!condition_met(AlertCondition::Below, dec!(0.30), dec!(0.25)));
    }

    #[test]
    fn crosses_fires_within_band() {
        // 0.1% of 0.25 = 0.00025
        assert!(condition_met(AlertCondition::Crosses, dec!(0.2502), dec!(0.25)));
        assert!(!condition_met(AlertCondition::Crosses, dec!(0.26), dec!(0.25)));
    }
}
