//! Price alert repository implementation (table `price_alerts`).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use trading_core::models::{NewPriceAlert, PriceAlert};
use trading_core::traits::{PriceAlertRepository, TraitResult};
use trading_core::types::{AlertCondition, AlertStatus};

#[derive(Debug, Clone, FromRow)]
pub struct PriceAlertDb {
    pub id: Uuid,
    pub user_id: Uuid,
    pub target_price: Decimal,
    pub condition: AlertCondition,
    pub status: AlertStatus,
    pub triggered_at: Option<DateTime<Utc>>,
    pub triggered_price: Option<Decimal>,
    pub repeat: bool,
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl From<PriceAlertDb> for PriceAlert {
    fn from(db: PriceAlertDb) -> Self {
        Self {
            id: db.id,
            user_id: db.user_id,
            target_price: db.target_price,
            condition: db.condition,
            status: db.status,
            triggered_at: db.triggered_at,
            triggered_price: db.triggered_price,
            repeat: db.repeat,
            note: db.note,
            created_at: db.created_at,
            updated_at: db.updated_at,
        }
    }
}

pub struct PostgresPriceAlertRepository {
    pool: PgPool,
}

impl PostgresPriceAlertRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PriceAlertRepository for PostgresPriceAlertRepository {
    async fn create_price_alert(&self, input: NewPriceAlert) -> TraitResult<PriceAlert> {
        let row = sqlx::query_as::<_, PriceAlertDb>(
            r#"INSERT INTO price_alerts (user_id, target_price, condition, note)
               VALUES ($1, $2, $3, $4)
               RETURNING *"#,
        )
        .bind(input.user_id)
        .bind(input.target_price)
        .bind(input.condition)
        .bind(input.note)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.into())
    }

    async fn list_price_alerts_for_user(&self, user_id: Uuid) -> TraitResult<Vec<PriceAlert>> {
        let rows = sqlx::query_as::<_, PriceAlertDb>(
            "SELECT * FROM price_alerts WHERE user_id = $1 ORDER BY created_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn delete_price_alert(&self, id: Uuid, user_id: Uuid) -> TraitResult<bool> {
        let result = sqlx::query("DELETE FROM price_alerts WHERE id = $1 AND user_id = $2")
            .bind(id)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn get_active_alerts(&self) -> TraitResult<Vec<PriceAlert>> {
        let rows = sqlx::query_as::<_, PriceAlertDb>(
            "SELECT * FROM price_alerts WHERE status = 'active' ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn mark_triggered(&self, id: Uuid, triggered_price: Decimal) -> TraitResult<()> {
        // One-shot alerts move to `triggered`; repeating alerts stay `active`
        // so a later cycle can fire them again. `triggered_at`/`triggered_price`
        // always record the most recent firing.
        sqlx::query(
            r#"UPDATE price_alerts
               SET triggered_at = NOW(),
                   triggered_price = $2,
                   status = CASE WHEN repeat THEN status ELSE 'triggered'::alert_status END,
                   updated_at = NOW()
               WHERE id = $1"#,
        )
        .bind(id)
        .bind(triggered_price)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
