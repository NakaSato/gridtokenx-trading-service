//! Recurring-order execution service.
//!
//! Periodically materialises *due* recurring orders into real `trading_orders`
//! rows, taking the same path as the REST create handler
//! (`insert_order_with_event` + `OrderCreated` outbox event), then advances each
//! rule's `next_execution_at` and execution counter. The matcher worker picks
//! the placed orders up on its next cycle — this service never matches directly.

use std::sync::Arc;

use rust_decimal::Decimal;
use tracing::{info, warn};
use uuid::Uuid;

use trading_core::events::{Event, OrderCreatedPayload};
use trading_core::models::{RecurringOrder, TradingOrder};
use trading_core::recurring::next_execution_at;
use trading_core::traits::{OrderRepository, RecurringOrderRepository, TraitResult};
use trading_core::types::{OrderSide, OrderStatus, OrderType, TimeInForce};

/// Async orchestrator that turns due recurring orders into placed orders.
pub struct RecurringEvaluator {
    recurring_repo: Arc<dyn RecurringOrderRepository>,
    order_repo: Arc<dyn OrderRepository>,
}

impl RecurringEvaluator {
    pub fn new(
        recurring_repo: Arc<dyn RecurringOrderRepository>,
        order_repo: Arc<dyn OrderRepository>,
    ) -> Self {
        Self {
            recurring_repo,
            order_repo,
        }
    }

    /// Place orders for every recurring rule whose `next_execution_at` has
    /// passed, then reschedule it. Returns the number of orders placed. One bad
    /// rule never aborts the cycle — it is logged and skipped.
    pub async fn run_cycle(&self) -> TraitResult<usize> {
        let now = gridtokenx_telemetry::time::now();
        let due = self.recurring_repo.get_due_recurring_orders(now).await?;
        if due.is_empty() {
            return Ok(0);
        }

        // Resolve the active epoch once for the whole batch — every placed order
        // needs it so the matcher's settlement/order_matches FK to market_epochs
        // is satisfied (mirrors the REST create path).
        let epoch_id = self.order_repo.get_or_create_active_epoch().await?;

        let mut placed = 0usize;
        for rule in due {
            // A limit order needs a price. Buys cap at `max_price_per_kwh`,
            // sells floor at `min_price_per_kwh`. A rule missing its side's
            // bound can't be priced — skip without advancing so a later config
            // fix lets it run.
            let price = match rule.side {
                OrderSide::Buy => rule.max_price_per_kwh,
                OrderSide::Sell => rule.min_price_per_kwh,
            };
            let Some(price) = price else {
                warn!(
                    recurring_id = %rule.id,
                    side = %rule.side,
                    "Skipping recurring order: no price bound for its side"
                );
                continue;
            };

            if let Err(e) = self.place_order(&rule, price, epoch_id).await {
                warn!(recurring_id = %rule.id, error = %e, "Failed to place recurring order; will retry next cycle");
                continue;
            }

            let next = next_execution_at(now, rule.interval_type, rule.interval_value);
            if let Err(e) = self
                .recurring_repo
                .update_after_execution(rule.id, next, rule.total_executions + 1)
                .await
            {
                // Order is already placed; failing to advance means it could
                // re-fire next cycle. Log loudly rather than silently double-place.
                warn!(recurring_id = %rule.id, error = %e, "Placed recurring order but failed to advance schedule");
                continue;
            }
            placed += 1;
        }

        if placed > 0 {
            info!("Recurring evaluator placed {} order(s)", placed);
        }
        Ok(placed)
    }

    async fn place_order(
        &self,
        rule: &RecurringOrder,
        price: Decimal,
        epoch_id: Uuid,
    ) -> TraitResult<()> {
        let order = TradingOrder {
            id: Uuid::new_v4(),
            user_id: rule.user_id,
            order_type: OrderType::Limit,
            side: rule.side,
            energy_amount: rule.energy_amount,
            price_per_kwh: price,
            filled_amount: Decimal::ZERO,
            status: OrderStatus::Pending,
            expires_at: Some(gridtokenx_telemetry::time::now() + chrono::Duration::hours(24)),
            created_at: Some(gridtokenx_telemetry::time::now()),
            filled_at: None,
            epoch_id: Some(epoch_id),
            zone_id: None,
            meter_id: None,
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: None,
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
            time_in_force: TimeInForce::Gtc,
        };

        let event = Event::OrderCreated(OrderCreatedPayload {
            id: order.id,
            user_id: order.user_id,
            order_type: order.order_type.to_string(),
            side: order.side.to_string(),
            energy_amount: order.energy_amount,
            price_per_kwh: order.price_per_kwh,
            status: order.status.to_string(),
            zone_id: order.zone_id,
            created_at: order.created_at,
        });

        // Atomic order + outbox event, same as the REST create path.
        self.order_repo.insert_order_with_event(&order, &event).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // unwrap is idiomatic in tests
    use super::*;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use std::sync::Mutex;
    use trading_core::models::{NewRecurringOrder, OrderBookEntry, TradingOrder};
    use trading_core::types::{IntervalType, OrderStatus, RecurringStatus};

    fn due_rule(side: OrderSide, max_price: Option<Decimal>, min_price: Option<Decimal>) -> RecurringOrder {
        RecurringOrder {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            side,
            energy_amount: dec!(10),
            max_price_per_kwh: max_price,
            min_price_per_kwh: min_price,
            interval_type: IntervalType::Daily,
            interval_value: 1,
            next_execution_at: Utc::now() - chrono::Duration::hours(1), // due
            last_executed_at: None,
            status: RecurringStatus::Active,
            total_executions: 2,
            max_executions: None,
            name: None,
            description: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[derive(Default)]
    struct MockRecurringRepo {
        due: Mutex<Vec<RecurringOrder>>,
        advanced: Mutex<Vec<(Uuid, i32)>>, // (id, total_executions) from update_after_execution
    }

    #[async_trait]
    impl RecurringOrderRepository for MockRecurringRepo {
        async fn get_due_recurring_orders(&self, _now: DateTime<Utc>) -> TraitResult<Vec<RecurringOrder>> {
            Ok(std::mem::take(&mut self.due.lock().unwrap()))
        }
        async fn update_after_execution(&self, id: Uuid, _next: DateTime<Utc>, total: i32) -> TraitResult<()> {
            self.advanced.lock().unwrap().push((id, total));
            Ok(())
        }
        async fn create_recurring_order(&self, _input: NewRecurringOrder) -> TraitResult<RecurringOrder> {
            unimplemented!()
        }
        async fn list_recurring_orders_for_user(&self, _user_id: Uuid) -> TraitResult<Vec<RecurringOrder>> {
            unimplemented!()
        }
        async fn get_recurring_order(&self, _id: Uuid, _user_id: Uuid) -> TraitResult<Option<RecurringOrder>> {
            unimplemented!()
        }
        async fn delete_recurring_order(&self, _id: Uuid, _user_id: Uuid) -> TraitResult<bool> {
            unimplemented!()
        }
        async fn set_recurring_status(&self, _id: Uuid, _user_id: Uuid, _status: RecurringStatus) -> TraitResult<bool> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct MockOrderRepo {
        placed: Mutex<Vec<TradingOrder>>,
    }

    #[async_trait]
    impl OrderRepository for MockOrderRepo {
        async fn get_or_create_active_epoch(&self) -> TraitResult<Uuid> {
            Ok(Uuid::new_v4())
        }
        async fn insert_order_with_event(&self, order: &TradingOrder, _event: &Event) -> TraitResult<()> {
            self.placed.lock().unwrap().push(order.clone());
            Ok(())
        }
        async fn insert_order(&self, _order: &TradingOrder) -> TraitResult<()> {
            unimplemented!()
        }
        async fn get_order(&self, _id: Uuid) -> TraitResult<Option<TradingOrder>> {
            unimplemented!()
        }
        async fn get_orders_by_user(&self, _user_id: Uuid, _limit: i64, _offset: i64) -> TraitResult<Vec<TradingOrder>> {
            unimplemented!()
        }
        async fn get_active_orders_by_zone(&self, _zone_id: i32) -> TraitResult<Vec<OrderBookEntry>> {
            unimplemented!()
        }
        async fn get_all_active_orders(&self) -> TraitResult<Vec<OrderBookEntry>> {
            unimplemented!()
        }
        async fn update_order_status(&self, _id: Uuid, _status: OrderStatus) -> TraitResult<()> {
            unimplemented!()
        }
        async fn update_filled_amount(&self, _id: Uuid, _filled: Decimal, _status: OrderStatus) -> TraitResult<()> {
            unimplemented!()
        }
        async fn update_filled_amount_with_event(&self, _id: Uuid, _filled: Decimal, _status: OrderStatus, _event: &Event) -> TraitResult<()> {
            unimplemented!()
        }
        async fn get_active_buy_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            unimplemented!()
        }
        async fn get_active_sell_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            unimplemented!()
        }
        async fn cancel_order(&self, _id: Uuid, _user_id: Uuid) -> TraitResult<bool> {
            unimplemented!()
        }
        async fn bootstrap_active_orders(&self) -> TraitResult<Vec<TradingOrder>> {
            unimplemented!()
        }
    }

    /// A due Buy rule is materialized into a placed order priced at its
    /// `max_price_per_kwh`, and its schedule is advanced (total_executions + 1).
    #[tokio::test]
    async fn run_cycle_places_due_buy_order_and_reschedules() {
        let rule = due_rule(OrderSide::Buy, Some(dec!(4.25)), None);
        let rule_id = rule.id;
        let recurring = Arc::new(MockRecurringRepo::default());
        recurring.due.lock().unwrap().push(rule);
        let orders = Arc::new(MockOrderRepo::default());

        let evaluator = RecurringEvaluator::new(recurring.clone(), orders.clone());
        let placed = evaluator.run_cycle().await.unwrap();

        assert_eq!(placed, 1);
        let placed_orders = orders.placed.lock().unwrap();
        assert_eq!(placed_orders.len(), 1);
        assert_eq!(placed_orders[0].side, OrderSide::Buy);
        assert_eq!(placed_orders[0].price_per_kwh, dec!(4.25), "buy priced at max bound");
        // Schedule advanced: total_executions 2 -> 3.
        assert_eq!(recurring.advanced.lock().unwrap().as_slice(), &[(rule_id, 3)]);
    }

    /// A due Buy rule with no `max_price_per_kwh` cannot be priced — it is skipped
    /// without placing an order or advancing its schedule (so a later config fix
    /// lets it run).
    #[tokio::test]
    async fn run_cycle_skips_rule_missing_price_bound() {
        let rule = due_rule(OrderSide::Buy, None, None);
        let recurring = Arc::new(MockRecurringRepo::default());
        recurring.due.lock().unwrap().push(rule);
        let orders = Arc::new(MockOrderRepo::default());

        let evaluator = RecurringEvaluator::new(recurring.clone(), orders.clone());
        let placed = evaluator.run_cycle().await.unwrap();

        assert_eq!(placed, 0);
        assert!(orders.placed.lock().unwrap().is_empty(), "no order placed");
        assert!(recurring.advanced.lock().unwrap().is_empty(), "schedule not advanced");
    }
}
