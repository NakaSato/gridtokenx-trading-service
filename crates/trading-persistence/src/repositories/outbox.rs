//! Outbox repository implementation.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

use trading_core::events::Event;
use trading_core::models::{OutboxEvent, OutboxStatus};
use trading_core::traits::{OutboxRepository, TraitResult};

pub struct PostgresOutboxRepository {
    pool: PgPool,
}

impl PostgresOutboxRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OutboxRepository for PostgresOutboxRepository {
    async fn insert_event(&self, event: &Event) -> TraitResult<()> {
        let event_type = match event {
            Event::OrderMatched(_) => "OrderMatched",
            Event::SettlementRequested(_) => "SettlementRequested",
            Event::OrderUpdate { .. } => "OrderUpdate",
            Event::PeakPriceUpdate { .. } => "PeakPriceUpdate",
            Event::TriggerExecution { .. } => "TriggerExecution",
            Event::OrderCreated(_) => "OrderCreated",
            Event::ErcIssued(_) => "ErcIssued",
            Event::SettlementProcessed(_) => "SettlementProcessed",
            Event::UserRegistered(_) => "UserRegistered",
            Event::UserOnboarded(_) => "UserOnboarded",
            Event::UserWalletLinked(_) => "UserWalletLinked",
            Event::OracleReading(_) => "OracleReading",
            Event::VppDispatched(_) => "VppDispatched",
        };

        let payload = serde_json::to_value(event).map_err(|e| {
            trading_core::error::ApiError::Internal(format!("Failed to serialize event: {}", e))
        })?;

        sqlx::query(
            "INSERT INTO outbox_events (event_type, payload) VALUES ($1, $2)"
        )
        .bind(event_type)
        .bind(payload)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_pending_events(&self, limit: i32) -> TraitResult<Vec<OutboxEvent>> {
        let rows = sqlx::query_as::<_, OutboxEventDb>(
            "SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT $1"
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn mark_processed(&self, id: Uuid) -> TraitResult<()> {
        sqlx::query(
            "UPDATE outbox_events SET status = 'PROCESSED', last_attempt_at = NOW() WHERE id = $1"
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn mark_failed(&self, id: Uuid, _error: &str) -> TraitResult<()> {
        // In a real system, we might want to store the error message in a separate column
        sqlx::query(
            "UPDATE outbox_events SET status = 'FAILED', attempts = attempts + 1, last_attempt_at = NOW() WHERE id = $1"
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn cleanup_processed(&self, before: DateTime<Utc>) -> TraitResult<u64> {
        let result = sqlx::query(
            "DELETE FROM outbox_events WHERE status = 'PROCESSED' AND created_at < $1"
        )
        .bind(before)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}

#[derive(FromRow)]
struct OutboxEventDb {
    id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    status: String,
    attempts: i32,
    last_attempt_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl From<OutboxEventDb> for OutboxEvent {
    fn from(db: OutboxEventDb) -> Self {
        Self {
            id: db.id,
            event_type: db.event_type,
            payload: db.payload,
            status: match db.status.as_str() {
                "PENDING" | "pending" => OutboxStatus::Pending,
                "PROCESSED" | "processed" => OutboxStatus::Processed,
                _ => OutboxStatus::Failed,
            },
            attempts: db.attempts,
            last_attempt_at: db.last_attempt_at,
            created_at: db.created_at,
        }
    }
}
