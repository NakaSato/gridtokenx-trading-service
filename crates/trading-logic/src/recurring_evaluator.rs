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
