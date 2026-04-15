use std::sync::Arc;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use tracing::{info, error, debug};
use uuid::Uuid;

use crate::domain::trading::models::{RecurringStatus, IntervalType};
use crate::infra::db::schema::types::{OrderSide, OrderStatus, OrderType};
use crate::domain::trading::engine::OrderMatchingEngine;

pub struct RecurringEvaluator {
    db: PgPool,
    matching_engine: Arc<OrderMatchingEngine>,
}

impl RecurringEvaluator {
    pub fn new(db: PgPool, matching_engine: Arc<OrderMatchingEngine>) -> Self {
        Self { db, matching_engine }
    }

    pub async fn process_recurring_orders(&self) -> anyhow::Result<usize> {
        // 1. Fetch active recurring orders due for execution
        // We use a small optimization here: only fetch if next_execution_at is in the past
        let due_orders = sqlx::query(
            r#"
            SELECT * FROM recurring_orders 
            WHERE status = 'active' 
            AND next_execution_at <= NOW()
            AND (max_executions IS NULL OR total_executions < max_executions)
            "#
        )
        .fetch_all(&self.db)
        .await?;

        if due_orders.is_empty() {
            return Ok(0);
        }

        let mut executed_count = 0;

        for row in due_orders {
            let order_id: Uuid = row.get("id");
            if let Err(e) = self.execute_recurring_order(row).await {
                error!("Failed to execute recurring order {}: {}", order_id, e);
            } else {
                executed_count += 1;
            }
        }

        Ok(executed_count)
    }

    pub async fn process_recurring_orders_with_metrics(&self) -> anyhow::Result<usize> {
        let start = std::time::Instant::now();
        let res = self.process_recurring_orders().await;
        let duration = start.elapsed().as_secs_f64() * 1000.0;
        
        match res {
            Ok(count) => {
                // Fetch total active for "evaluated" count metric
                let active_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM recurring_orders WHERE status = 'active'")
                    .fetch_one(&self.db)
                    .await
                    .unwrap_or(0);

                crate::metrics::record_dca_evaluation(duration, active_count as u64, count as u64);
                Ok(count)
            }
            Err(e) => {
                error!("❌ Recurring evaluator cycle failed: {}", e);
                Err(e)
            }
        }
    }

    async fn execute_recurring_order(&self, row: sqlx::postgres::PgRow) -> anyhow::Result<()> {
        let id: Uuid = row.get("id");
        let user_id: Uuid = row.get("user_id");
        let side: OrderSide = row.get("side");
        let amount: Decimal = row.get("energy_amount");
        let interval_type: IntervalType = row.get("interval_type");
        let interval_value: i32 = row.get("interval_value");
        let total_executions: i32 = row.get("total_executions");
        let max_executions: Option<i32> = row.get("max_executions");
        let session_token: Option<String> = row.get("session_token");
        
        let max_price: Option<Decimal> = row.get("max_price_per_kwh");
        let min_price: Option<Decimal> = row.get("min_price_per_kwh");

        info!("🎯 Executing recurring order: {} (Execution #{})", id, total_executions + 1);

        // Optional: Check price constraints before execution
        if let Some(limit_price) = if side == OrderSide::Buy { max_price } else { min_price } {
            let latest_price: Option<Decimal> = sqlx::query(
                "SELECT match_price FROM order_matches ORDER BY match_time DESC LIMIT 1"
            )
            .map(|row: sqlx::postgres::PgRow| row.get("match_price"))
            .fetch_optional(&self.db)
            .await?;

            if let Some(market_price) = latest_price {
                if side == OrderSide::Buy && market_price > limit_price {
                    debug!("Skipping recurring buy order {}: market price {} > max price {}", id, market_price, limit_price);
                    self.record_skipped(id, format!("Price too high: {} > {}", market_price, limit_price)).await?;
                    self.update_next_execution(id, interval_type, interval_value).await?;
                    return Ok(());
                }
                if side == OrderSide::Sell && market_price < limit_price {
                    debug!("Skipping recurring sell order {}: market price {} < min price {}", id, market_price, limit_price);
                    self.record_skipped(id, format!("Price too low: {} < {}", market_price, limit_price)).await?;
                    self.update_next_execution(id, interval_type, interval_value).await?;
                    return Ok(());
                }
            }
        }

        let mut tx = self.db.begin().await?;

        // 1. Create the live trading order
        let trading_order_id = Uuid::new_v4();
        let order_status = OrderStatus::Active;
        let order_type = OrderType::Market; // DCA usually executes at market or "best available"

        let new_order = sqlx::query_as::<_, crate::domain::trading::models::TradingOrderDb>(
            r#"
            INSERT INTO trading_orders (
                id, user_id, side, order_type, energy_amount, price_per_kwh,
                filled_amount, status, created_at, session_token, epoch_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), $9, (SELECT id FROM epochs WHERE status = 'active' LIMIT 1))
            RETURNING *
            "#,
        )
        .bind(trading_order_id)
        .bind(user_id)
        .bind(side)
        .bind(order_type)
        .bind(amount)
        .bind(Decimal::ZERO) // Market price determined at matching
        .bind(Decimal::ZERO)
        .bind(order_status)
        .bind(session_token)
        .fetch_one(&mut *tx)
        .await?;

        // 2. Record the execution
        sqlx::query(
            r#"
            INSERT INTO recurring_order_executions (
                recurring_order_id, trading_order_id, status, energy_amount, price_per_kwh
            )
            VALUES ($1, $2, 'success', $3, $4)
            "#
        )
        .bind(id)
        .bind(trading_order_id)
        .bind(amount)
        .bind(Decimal::ZERO)
        .execute(&mut *tx)
        .await?;

        // 3. Update the recurring order state
        let new_total = total_executions + 1;
        let next_status = if let Some(max) = max_executions {
            if new_total >= max { RecurringStatus::Completed } else { RecurringStatus::Active }
        } else {
            RecurringStatus::Active
        };

        let next_execution = self.calculate_next_execution_time(interval_type, interval_value);

        sqlx::query(
            r#"
            UPDATE recurring_orders 
            SET total_executions = $1, 
                last_executed_at = NOW(), 
                next_execution_at = $2,
                status = $3,
                updated_at = NOW()
            WHERE id = $4
            "#
        )
        .bind(new_total)
        .bind(next_execution)
        .bind(next_status)
        .bind(id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        // 4. Notify Matching Engine
        self.matching_engine.notify_new_order(new_order.zone_id, Some(new_order)).await;

        info!("Successfully executed recurring order {} -> trading order {}", id, trading_order_id);
        Ok(())
    }

    async fn record_skipped(&self, id: Uuid, reason: String) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO recurring_order_executions (recurring_order_id, status, error_message) VALUES ($1, 'skipped', $2)"
        )
        .bind(id)
        .bind(reason)
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn update_next_execution(&self, id: Uuid, interval_type: IntervalType, interval_value: i32) -> anyhow::Result<()> {
        let next_execution = self.calculate_next_execution_time(interval_type, interval_value);
        sqlx::query("UPDATE recurring_orders SET next_execution_at = $1, updated_at = NOW() WHERE id = $2")
            .bind(next_execution)
            .bind(id)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    fn calculate_next_execution_time(&self, interval_type: IntervalType, interval_value: i32) -> DateTime<Utc> {
        let now = Utc::now();
        match interval_type {
            IntervalType::Hourly => now + chrono::Duration::hours(interval_value as i64),
            IntervalType::Daily => now + chrono::Duration::days(interval_value as i64),
            IntervalType::Weekly => now + chrono::Duration::weeks(interval_value as i64),
            IntervalType::Monthly => now + chrono::Duration::days(30 * interval_value as i64),
        }
    }
}
