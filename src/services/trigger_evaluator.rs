use std::sync::Arc;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use tracing::{info, error, debug};
use uuid::Uuid;

use crate::domain::trading::models::{TriggerType, TradingOrderDb};
use crate::infra::db::schema::types::{OrderSide, OrderStatus, OrderType};
use crate::domain::trading::engine::OrderMatchingEngine;

pub struct TriggerEvaluator {
    db: PgPool,
    matching_engine: Arc<OrderMatchingEngine>,
}

impl TriggerEvaluator {
    pub fn new(db: PgPool, matching_engine: Arc<OrderMatchingEngine>) -> Self {
        Self { db, matching_engine }
    }

    pub async fn process_triggers(&self) -> anyhow::Result<usize> {
        // 1. Fetch pending conditional orders
        let pending_orders = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            SELECT * FROM conditional_orders 
            WHERE trigger_status = 'pending' 
            AND (expires_at IS NULL OR expires_at > NOW())
            "#
        )
        .fetch_all(&self.db)
        .await?;

        if pending_orders.is_empty() {
            return Ok(0);
        }

        let mut triggered_count = 0;

        // 2. Fetch latest market price(s)
        // For simplicity, we'll get the latest match price globally
        let latest_price: Option<Decimal> = sqlx::query(
            "SELECT match_price FROM order_matches ORDER BY match_time DESC LIMIT 1"
        )
        .map(|row: sqlx::postgres::PgRow| row.get("match_price"))
        .fetch_optional(&self.db)
        .await?;

        let market_price = match latest_price {
            Some(price) => price,
            None => {
                debug!("No market price available for trigger evaluation yet");
                return Ok(0);
            }
        };

        for order in pending_orders {
            // Update last_peak_price for Trailing Stops
            let current_peak = order.last_peak_price.unwrap_or(market_price);
            let updated_peak = if order.side == OrderSide::Sell {
                // For a sell stop, we want to track the highest price (peak)
                if market_price > current_peak {
                    Some(market_price)
                } else {
                    Some(current_peak)
                }
            } else {
                // For a buy stop, we want to track the lowest price (trough)
                if market_price < current_peak {
                    Some(market_price)
                } else {
                    Some(current_peak)
                }
            };

            // Atomically update peak in DB if it changed
            if updated_peak != order.last_peak_price {
                if let Err(e) = sqlx::query("UPDATE conditional_orders SET last_peak_price = $1, updated_at = NOW() WHERE id = $2")
                    .bind(updated_peak)
                    .bind(order.id)
                    .execute(&self.db)
                    .await {
                        error!("Failed to update trailing peak for order {}: {}", order.id, e);
                    }
            }

            // Create a local copy with updated peak for evaluation
            let mut eval_order = order.clone();
            eval_order.last_peak_price = updated_peak;

            if self.should_trigger(&eval_order, market_price) {
                if let Err(e) = self.execute_trigger(eval_order).await {
                    error!("Failed to execute trigger for order {}: {}", triggered_count, e);
                } else {
                    triggered_count += 1;
                }
            }
        }

        // 3. Handle expired conditional orders
        let expired_result = sqlx::query(
            "UPDATE conditional_orders SET trigger_status = 'expired', updated_at = NOW() 
             WHERE trigger_status = 'pending' AND expires_at <= NOW()"
        )
        .execute(&self.db)
        .await?;
        
        if expired_result.rows_affected() > 0 {
            info!("Marked {} conditional orders as expired", expired_result.rows_affected());
        }

        Ok(triggered_count)
    }

    fn should_trigger(&self, order: &TradingOrderDb, market_price: Decimal) -> bool {
        let trigger_price = order.trigger_price;

        match order.trigger_type {
            Some(TriggerType::StopLoss) => {
                let trigger = trigger_price.unwrap_or(Decimal::ZERO);
                if order.side == OrderSide::Sell {
                    market_price <= trigger
                } else {
                    market_price >= trigger
                }
            }
            Some(TriggerType::TakeProfit) => {
                let trigger = trigger_price.unwrap_or(Decimal::ZERO);
                if order.side == OrderSide::Sell {
                    market_price >= trigger
                } else {
                    market_price <= trigger
                }
            }
            Some(TriggerType::TrailingStop) => {
                let peak = order.last_peak_price.unwrap_or(market_price);
                let offset = order.trailing_offset.unwrap_or(Decimal::ZERO);
                
                if order.side == OrderSide::Sell {
                    // Sell if price drops by 'offset' from the peak
                    market_price <= (peak - offset)
                } else {
                    // Buy if price rises by 'offset' from the trough
                    market_price >= (peak + offset)
                }
            }
            None => false,
        }
    }

    async fn execute_trigger(&self, cond_order: TradingOrderDb) -> anyhow::Result<()> {
        info!("🎯 Triggering conditional order: {}", cond_order.id);

        let mut tx = self.db.begin().await?;

        // 1. Update conditional order status
        sqlx::query(
            "UPDATE conditional_orders SET trigger_status = 'triggered', triggered_at = NOW(), updated_at = NOW() WHERE id = $1"
        )
        .bind(cond_order.id)
        .execute(&mut *tx)
        .await?;

        // 2. Insert into trading_orders (promote to real order)
        // If limit_price was set, it's a Limit order, otherwise it's a Market order
        let order_type = if cond_order.limit_price.is_some() {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let price = cond_order.limit_price.unwrap_or(Decimal::ZERO);
        let order_id = Uuid::new_v4();

        let new_order = sqlx::query_as::<_, TradingOrderDb>(
            r#"
            INSERT INTO trading_orders (
                id, user_id, side, order_type, energy_amount, price_per_kwh,
                filled_amount, status, created_at, session_token, zone_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, $10)
            RETURNING *
            "#,
        )
        .bind(order_id)
        .bind(cond_order.user_id)
        .bind(cond_order.side)
        .bind(order_type)
        .bind(cond_order.energy_amount)
        .bind(price)
        .bind(Decimal::ZERO)
        .bind(OrderStatus::Active)
        .bind(cond_order.session_token)
        .bind(cond_order.zone_id)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        // 3. Notify Matching Engine
        self.matching_engine.notify_new_order(new_order.zone_id, Some(new_order)).await;

        info!("Successfully promoted conditional order {} to trading order {}", cond_order.id, order_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_should_trigger_stop_loss() {
        let mut order = TradingOrderDb {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            order_type: OrderType::Market,
            side: OrderSide::Sell,
            trigger_type: Some(TriggerType::StopLoss),
            trigger_price: Some(dec!(100)),
            energy_amount: dec!(10),
            price_per_kwh: dec!(0),
            limit_price: None,
            status: OrderStatus::Active,
            time_in_force: crate::infra::db::schema::types::TimeInForce::Gtc,
            filled_amount: None,
            expires_at: None,
            created_at: None,
            filled_at: None,
            epoch_id: None,
            zone_id: None,
            meter_id: None,
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: None,
            trigger_status: None,
            trailing_offset: None,
            triggered_at: None,
            last_peak_price: None,
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
        };

        // Sell StopLoss: triggers if market_price <= trigger_price
        let evaluator = TriggerEvaluator { db: PgPool::connect_lazy("postgres://localhost/fake").unwrap(), matching_engine: Arc::new(OrderMatchingEngine::new(PgPool::connect_lazy("postgres://localhost/fake").unwrap())) };
        
        assert!(evaluator.should_trigger(&order, dec!(90)));
        assert!(evaluator.should_trigger(&order, dec!(100)));
        assert!(!evaluator.should_trigger(&order, dec!(110)));

        // Buy StopLoss: triggers if market_price >= trigger_price
        order.side = OrderSide::Buy;
        assert!(evaluator.should_trigger(&order, dec!(110)));
        assert!(evaluator.should_trigger(&order, dec!(100)));
        assert!(!evaluator.should_trigger(&order, dec!(90)));
    }

    #[tokio::test]
    async fn test_should_trigger_trailing_stop() {
        let mut order = TradingOrderDb {
            id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            order_type: OrderType::Market,
            side: OrderSide::Sell,
            trigger_type: Some(TriggerType::TrailingStop),
            trailing_offset: Some(dec!(5)),
            last_peak_price: Some(dec!(100)),
            energy_amount: dec!(10),
            price_per_kwh: dec!(0),
            limit_price: None,
            status: OrderStatus::Active,
            time_in_force: crate::infra::db::schema::types::TimeInForce::Gtc,
            trigger_price: None,
            filled_amount: None,
            expires_at: None,
            created_at: None,
            filled_at: None,
            epoch_id: None,
            zone_id: None,
            meter_id: None,
            refund_tx_signature: None,
            order_pda: None,
            order_index: None,
            session_token: None,
            trigger_status: None,
            triggered_at: None,
            blockchain_status: None,
            blockchain_tx_hash: None,
            blockchain_error: None,
            retry_count: 0,
        };

        let evaluator = TriggerEvaluator { db: PgPool::connect_lazy("postgres://localhost/fake").unwrap(), matching_engine: Arc::new(OrderMatchingEngine::new(PgPool::connect_lazy("postgres://localhost/fake").unwrap())) };

        // Sell TrailingStop: triggers if market_price <= peak - offset (100 - 5 = 95)
        assert!(evaluator.should_trigger(&order, dec!(90)));
        assert!(evaluator.should_trigger(&order, dec!(95)));
        assert!(!evaluator.should_trigger(&order, dec!(96)));
        assert!(!evaluator.should_trigger(&order, dec!(100)));

        // Buy TrailingStop: triggers if market_price >= trough + offset (100 + 5 = 105)
        order.side = OrderSide::Buy;
        assert!(evaluator.should_trigger(&order, dec!(110)));
        assert!(evaluator.should_trigger(&order, dec!(105)));
        assert!(!evaluator.should_trigger(&order, dec!(104)));
    }
}
