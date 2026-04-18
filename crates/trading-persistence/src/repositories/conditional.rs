//! Conditional order repository implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, FromRow};
use uuid::Uuid;

use trading_core::models::ConditionalOrder;
use trading_core::traits::{ConditionalOrderRepository, TraitResult};
use trading_core::types::{OrderSide, TriggerStatus, TriggerType};

#[derive(Debug, Clone, FromRow)]
pub struct ConditionalOrderDb {
    pub id: Uuid,
    pub user_id: Uuid,
    pub side: OrderSide,
    pub energy_amount: Decimal,
    pub trigger_price: Decimal,
    pub trigger_type: TriggerType,
    pub trigger_status: TriggerStatus,
    pub limit_price: Option<Decimal>,
    pub trailing_offset: Option<Decimal>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub triggered_at: Option<DateTime<Utc>>,
    pub last_peak_price: Option<Decimal>,
}

impl From<ConditionalOrderDb> for ConditionalOrder {
    fn from(db: ConditionalOrderDb) -> Self {
        Self {
            id: db.id,
            user_id: db.user_id,
            side: db.side,
            energy_amount: db.energy_amount,
            trigger_price: db.trigger_price,
            trigger_type: db.trigger_type,
            trigger_status: db.trigger_status,
            limit_price: db.limit_price,
            trailing_offset: db.trailing_offset,
            expires_at: db.expires_at,
            created_at: db.created_at,
            triggered_at: db.triggered_at,
            last_peak_price: db.last_peak_price,
        }
    }
}

pub struct PostgresConditionalOrderRepository {
    pool: PgPool,
}

impl PostgresConditionalOrderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ConditionalOrderRepository for PostgresConditionalOrderRepository {
    async fn get_pending_conditional_orders(&self) -> TraitResult<Vec<ConditionalOrder>> {
        let orders = sqlx::query_as::<_, ConditionalOrderDb>(
            "SELECT * FROM trading_orders WHERE trigger_status = 'pending'"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(orders.into_iter().map(Into::into).collect())
    }

    async fn update_trigger_status(
        &self,
        id: Uuid,
        status: &str,
        triggered_at: Option<DateTime<Utc>>,
    ) -> TraitResult<()> {
        sqlx::query(
            "UPDATE trading_orders SET trigger_status = $1, triggered_at = $2, updated_at = NOW() WHERE id = $3"
        )
        .bind(status)
        .bind(triggered_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_peak_price(
        &self,
        id: Uuid,
        peak_price: Decimal,
    ) -> TraitResult<()> {
        sqlx::query(
            "UPDATE trading_orders SET last_peak_price = $1, updated_at = NOW() WHERE id = $2"
        )
        .bind(peak_price)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
