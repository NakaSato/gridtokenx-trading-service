//! Recurring order repository implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, FromRow};
use uuid::Uuid;

use trading_core::models::RecurringOrder;
use trading_core::traits::{RecurringOrderRepository, TraitResult};
use trading_core::types::{OrderSide, IntervalType, RecurringStatus};

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
            "SELECT * FROM recurring_orders WHERE status = 'active' AND next_execution_at <= $1"
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
            r#"
            UPDATE recurring_orders 
            SET next_execution_at = $1, 
                total_executions = $2, 
                last_executed_at = NOW(),
                updated_at = NOW(),
                status = CASE WHEN max_executions IS NOT NULL AND $2 >= max_executions THEN 'completed' ELSE status END
            WHERE id = $3
            "#
        )
        .bind(next_execution)
        .bind(total_executions)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
