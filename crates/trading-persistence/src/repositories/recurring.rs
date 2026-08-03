//! Recurring order repository implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use trading_core::models::RecurringOrder;
use trading_core::traits::{RecurringOrderRepository, TraitResult};
use trading_core::types::{IntervalType, OrderSide, RecurringStatus};

#[derive(Debug, Clone, FromRow)]
pub struct RecurringOrderDb {
    pub id: Uuid,
    pub user_id: Uuid,
    pub side: OrderSide,
    pub energy_amount: Decimal,
    pub max_price_per_kwh: Option<Decimal>,
    pub min_price_per_kwh: Option<Decimal>,
    pub interval_type: IntervalType,
    pub interval_value: i32,
    pub next_execution_at: DateTime<Utc>,
    pub last_executed_at: Option<DateTime<Utc>>,
    pub status: RecurringStatus,
    pub total_executions: i32,
    pub max_executions: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<RecurringOrderDb> for RecurringOrder {
    fn from(db: RecurringOrderDb) -> Self {
        Self {
            id: db.id,
            user_id: db.user_id,
            side: db.side,
            energy_amount: db.energy_amount,
            max_price_per_kwh: db.max_price_per_kwh,
            min_price_per_kwh: db.min_price_per_kwh,
            interval_type: db.interval_type,
            interval_value: db.interval_value,
            next_execution_at: db.next_execution_at,
            last_executed_at: db.last_executed_at,
            status: db.status,
            total_executions: db.total_executions,
            max_executions: db.max_executions,
            name: db.name,
            description: db.description,
            created_at: db.created_at,
            updated_at: db.updated_at,
        }
    }
}

pub struct PostgresRecurringOrderRepository {
    pool: PgPool,
}

impl PostgresRecurringOrderRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RecurringOrderRepository for PostgresRecurringOrderRepository {
    async fn get_due_recurring_orders(
        &self,
        now: DateTime<Utc>,
    ) -> TraitResult<Vec<RecurringOrder>> {
        let orders = sqlx::query_as::<_, RecurringOrderDb>(
            "SELECT * FROM recurring_orders WHERE status = 'active' AND next_execution_at <= $1",
        )
        .bind(now)
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn update_after_execution(
        &self,
        id: Uuid,
        next_execution: DateTime<Utc>,
        total_executions: i32,
    ) -> TraitResult<()> {
        sqlx::query(
            r"
            UPDATE recurring_orders 
            SET next_execution_at = $1, 
                total_executions = $2, 
                last_executed_at = NOW(),
                updated_at = NOW(),
                status = CASE WHEN max_executions IS NOT NULL AND $2 >= max_executions THEN 'completed' ELSE status END
            WHERE id = $3
            "
        )
        .bind(next_execution)
        .bind(total_executions)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn create_recurring_order(
        &self,
        input: trading_core::models::NewRecurringOrder,
    ) -> TraitResult<RecurringOrder> {
        let row = sqlx::query_as::<_, RecurringOrderDb>(
            r"
            INSERT INTO recurring_orders
                (user_id, side, energy_amount, max_price_per_kwh, min_price_per_kwh,
                 interval_type, interval_value, next_execution_at, max_executions, name, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING *
            ",
        )
        .bind(input.user_id)
        .bind(input.side)
        .bind(input.energy_amount)
        .bind(input.max_price_per_kwh)
        .bind(input.min_price_per_kwh)
        .bind(input.interval_type)
        .bind(input.interval_value)
        .bind(input.next_execution_at)
        .bind(input.max_executions)
        .bind(input.name)
        .bind(input.description)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }

    async fn list_recurring_orders_for_user(
        &self,
        user_id: Uuid,
    ) -> TraitResult<Vec<RecurringOrder>> {
        let orders = sqlx::query_as::<_, RecurringOrderDb>(
            "SELECT * FROM recurring_orders WHERE user_id = $1 ORDER BY created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn get_recurring_order(
        &self,
        id: Uuid,
        user_id: Uuid,
    ) -> TraitResult<Option<RecurringOrder>> {
        let order = sqlx::query_as::<_, RecurringOrderDb>(
            "SELECT * FROM recurring_orders WHERE id = $1 AND user_id = $2",
        )
        .bind(id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(order.map(Into::into))
    }

    async fn delete_recurring_order(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool> {
        let result = sqlx::query("DELETE FROM recurring_orders WHERE id = $1 AND user_id = $2")
            .bind(id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn set_recurring_status(
        &self,
        id: Uuid,
        user_id: Uuid,
        status: RecurringStatus,
    ) -> TraitResult<bool> {
        let result = sqlx::query(
            "UPDATE recurring_orders SET status = $3, updated_at = NOW() WHERE id = $1 AND user_id = $2",
        )
        .bind(id)
        .bind(user_id)
        .bind(status)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }
}
