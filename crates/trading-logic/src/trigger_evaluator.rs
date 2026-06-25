//! Price-alert trigger service.
//!
//! Each cycle it derives a single "current market price" from the live order
//! book, scans every `active` alert, and for each whose condition the price now
//! satisfies it records the firing (`mark_triggered`) and publishes a
//! `PriceAlertTriggered` event. The event flows through the outbox to the
//! notification service, which delivers it to the alert's owner. This service
//! sends no notifications directly.

use std::sync::Arc;

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
                triggered_at: gridtokenx_telemetry::time::now(),
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
    #![allow(clippy::unwrap_used)] // unwrap is idiomatic in tests
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::sync::Mutex;
    use trading_core::models::{NewPriceAlert, OrderBookEntry, PriceAlert, TradingOrder};
    use trading_core::types::{AlertStatus, OrderStatus, TimeInForce};
    use uuid::Uuid;

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

    fn book_entry(side: OrderSide, price: Decimal) -> OrderBookEntry {
        OrderBookEntry {
            order_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            side,
            energy_amount: dec!(10),
            original_amount: dec!(10),
            price_per_kwh: price,
            created_at: Utc::now(),
            zone_id: None,
            session_token: None,
            signature: None,
            payload_bytes: None,
            time_in_force: TimeInForce::Gtc,
        }
    }

    fn alert(condition: AlertCondition, target: Decimal) -> PriceAlert {
        PriceAlert {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            target_price: target,
            condition,
            status: AlertStatus::Active,
            triggered_at: None,
            triggered_price: None,
            repeat: false,
            note: None,
            created_at: Utc::now(),
            updated_at: None,
        }
    }

    #[derive(Default)]
    struct MockAlertRepo {
        active: Mutex<Vec<PriceAlert>>,
        triggered: Mutex<Vec<(Uuid, Decimal)>>,
    }

    #[async_trait]
    impl PriceAlertRepository for MockAlertRepo {
        async fn get_active_alerts(&self) -> TraitResult<Vec<PriceAlert>> {
            Ok(self.active.lock().unwrap().clone())
        }
        async fn mark_triggered_with_event(&self, id: Uuid, price: Decimal, _event: &Event) -> TraitResult<()> {
            self.triggered.lock().unwrap().push((id, price));
            Ok(())
        }
        async fn create_price_alert(&self, _input: NewPriceAlert) -> TraitResult<PriceAlert> {
            unimplemented!()
        }
        async fn list_price_alerts_for_user(&self, _user_id: Uuid) -> TraitResult<Vec<PriceAlert>> {
            unimplemented!()
        }
        async fn delete_price_alert(&self, _id: Uuid, _user_id: Uuid) -> TraitResult<bool> {
            unimplemented!()
        }
        async fn mark_triggered(&self, _id: Uuid, _price: Decimal) -> TraitResult<()> {
            unimplemented!()
        }
    }

    struct MockOrderRepo {
        book: Vec<OrderBookEntry>,
    }

    #[async_trait]
    impl OrderRepository for MockOrderRepo {
        async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>> {
            Ok(self.book.clone())
        }
        async fn insert_order(&self, _order: &TradingOrder) -> TraitResult<()> { unimplemented!() }
        async fn insert_order_with_event(&self, _order: &TradingOrder, _event: &Event) -> TraitResult<()> { unimplemented!() }
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> { unimplemented!() }
        async fn get_order(&self, _id: Uuid) -> TraitResult<Option<TradingOrder>> { unimplemented!() }
        async fn get_orders_by_user(&self, _u: Uuid, _l: i64, _o: i64) -> TraitResult<Vec<TradingOrder>> { unimplemented!() }
        async fn get_active_orders_by_zone(&self, _z: i32) -> TraitResult<Vec<OrderBookEntry>> { unimplemented!() }
        async fn update_order_status(&self, _id: Uuid, _s: OrderStatus) -> TraitResult<()> { unimplemented!() }
        async fn update_filled_amount(&self, _id: Uuid, _f: Decimal, _s: OrderStatus) -> TraitResult<()> { unimplemented!() }
        async fn update_filled_amount_with_event(&self, _id: Uuid, _f: Decimal, _s: OrderStatus, _e: &Event) -> TraitResult<()> { unimplemented!() }
        async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> { unimplemented!() }
        async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> { unimplemented!() }
        async fn cancel_order(&self, _id: Uuid, _u: Uuid) -> TraitResult<bool> { unimplemented!() }
        async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> { unimplemented!() }
    }

    /// Current price = midpoint of best bid (0.20) and best ask (0.30) = 0.25; an
    /// `Above 0.24` alert fires and is recorded with the triggering price.
    #[tokio::test]
    async fn run_cycle_fires_alert_on_midpoint_price() {
        let a = alert(AlertCondition::Above, dec!(0.24));
        let alert_id = a.id;
        let alerts = Arc::new(MockAlertRepo::default());
        alerts.active.lock().unwrap().push(a);
        let orders = Arc::new(MockOrderRepo {
            book: vec![book_entry(OrderSide::Buy, dec!(0.20)), book_entry(OrderSide::Sell, dec!(0.30))],
        });

        let evaluator = TriggerEvaluator::new(alerts.clone(), orders);
        let fired = evaluator.run_cycle().await.unwrap();

        assert_eq!(fired, 1);
        assert_eq!(alerts.triggered.lock().unwrap().as_slice(), &[(alert_id, dec!(0.25))]);
    }

    /// An alert whose condition the current price does not satisfy is not fired.
    #[tokio::test]
    async fn run_cycle_skips_unmet_alert() {
        let alerts = Arc::new(MockAlertRepo::default());
        alerts.active.lock().unwrap().push(alert(AlertCondition::Above, dec!(0.40))); // mid 0.25 < 0.40
        let orders = Arc::new(MockOrderRepo {
            book: vec![book_entry(OrderSide::Buy, dec!(0.20)), book_entry(OrderSide::Sell, dec!(0.30))],
        });

        let evaluator = TriggerEvaluator::new(alerts.clone(), orders);
        let fired = evaluator.run_cycle().await.unwrap();

        assert_eq!(fired, 0);
        assert!(alerts.triggered.lock().unwrap().is_empty());
    }

    /// Empty order book → no current price → no alerts fired (no-op).
    #[tokio::test]
    async fn run_cycle_noop_on_empty_book() {
        let alerts = Arc::new(MockAlertRepo::default());
        alerts.active.lock().unwrap().push(alert(AlertCondition::Above, dec!(0.01)));
        let orders = Arc::new(MockOrderRepo { book: vec![] });

        let evaluator = TriggerEvaluator::new(alerts.clone(), orders);
        let fired = evaluator.run_cycle().await.unwrap();

        assert_eq!(fired, 0);
        assert!(alerts.triggered.lock().unwrap().is_empty());
    }
}
