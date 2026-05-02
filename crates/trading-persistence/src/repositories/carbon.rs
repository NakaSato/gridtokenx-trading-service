//! Carbon credits repository implementation.

use async_trait::async_trait;
use sqlx::{PgPool, FromRow};
use uuid::Uuid;
use rust_decimal::Decimal;

use trading_core::models::{CarbonCredit, CarbonTransaction};
use trading_core::traits::{CarbonRepository, TraitResult};
use trading_core::types::CarbonStatus;

#[derive(Debug, Clone, FromRow)]
struct CarbonCreditDb {
    id: Uuid,
    user_id: Uuid,
    amount: Decimal,
    source: String,
    status: CarbonStatus,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl From<CarbonCreditDb> for CarbonCredit {
    fn from(db: CarbonCreditDb) -> Self {
        Self {
            id: db.id,
            user_id: db.user_id,
            amount: db.amount,
            source: db.source,
            status: db.status,
            created_at: db.created_at,
        }
    }
}

pub struct PostgresCarbonRepository {
    pool: PgPool,
}

impl PostgresCarbonRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CarbonRepository for PostgresCarbonRepository {
    async fn get_balance(&self, user_id: Uuid) -> TraitResult<Decimal> {
        let row: (Option<Decimal>,) = sqlx::query_as(
            "SELECT SUM(amount) FROM carbon_credits WHERE user_id = $1 AND status = 'active'"
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.0.unwrap_or(Decimal::ZERO))
    }

    async fn get_history(&self, user_id: Uuid) -> TraitResult<Vec<CarbonCredit>> {
        let history = sqlx::query_as::<_, CarbonCreditDb>(
            "SELECT * FROM carbon_credits WHERE user_id = $1 ORDER BY created_at DESC"
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(history.into_iter().map(Into::into).collect())
    }

    async fn get_transactions(&self, _user_id: Uuid) -> TraitResult<Vec<CarbonTransaction>> {
        // Implement real query here
        Ok(vec![])
    }

    async fn insert_transaction(&self, tx: &CarbonTransaction) -> TraitResult<()> {
        sqlx::query(
            r#"
            INSERT INTO carbon_transactions (
                id, from_user_id, to_user_id, amount, price_per_credit, status
            ) VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(tx.id)
        .bind(tx.from_user_id)
        .bind(tx.to_user_id)
        .bind(tx.amount)
        .bind(tx.price_per_credit)
        .bind(tx.status)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
