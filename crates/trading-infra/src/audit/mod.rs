use async_trait::async_trait;
use ipnetwork::IpNetwork;
use sqlx::PgPool;
use uuid::Uuid;

pub use trading_core::traits::AuditLog;

pub mod types;
pub mod worker;
pub use types::{AuditEvent, AuditEventRecord};

/// Audit logger service for the Trading microservice
#[derive(Debug, Clone)]
pub struct AuditLogger {
    db: PgPool,
    job_sender: tokio::sync::mpsc::Sender<AuditEvent>,
}

impl AuditLogger {
    /// Create a new audit logger
    pub fn new(db: PgPool, job_sender: tokio::sync::mpsc::Sender<AuditEvent>) -> Self {
        Self { db, job_sender }
    }

    /// Log an audit event to the database
    pub async fn log(&self, event: AuditEvent) -> anyhow::Result<()> {
        let event_type = event.event_type();
        let user_id = event.user_id();
        let ip_address_str = event.ip_address().map(|s| s.to_string());
        let ip_address = ip_address_str
            .as_deref()
            .and_then(|s| s.parse::<IpNetwork>().ok());
        let event_data = match serde_json::to_value(&event) {
            Ok(data) => data,
            Err(e) => {
                tracing::error!("Failed to serialize audit event: {}. Event: {:?}", e, event);
                serde_json::json!({
                    "error": "serialization_failed",
                    "event_type": event_type,
                    "raw_debug": format!("{:?}", event),
                    "message": e.to_string()
                })
            }
        };
        let created_at = gridtokenx_telemetry::time::now();

        // Use user_activities table (unified schema)
        sqlx::query(
            r#"
            INSERT INTO user_activities (activity_type, user_id, ip_address, metadata, created_at)
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(event_type)
        .bind(user_id)
        .bind(ip_address)
        .bind(event_data)
        .bind(created_at)
        .execute(&self.db)
        .await?;

        // Log to application logs as well
        tracing::info!(
            event_type = event_type,
            user_id = ?user_id,
            ip = ?ip_address,
            "Audit event logged by Trading Service"
        );

        Ok(())
    }

    /// Log batch of audit events to the database (High performance)
    pub async fn log_batch(&self, events: &[AuditEvent]) -> anyhow::Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut activity_types = Vec::with_capacity(events.len());
        let mut user_ids = Vec::with_capacity(events.len());
        let mut ip_addresses = Vec::with_capacity(events.len());
        let mut metadata_list = Vec::with_capacity(events.len());
        let mut created_at_list = Vec::with_capacity(events.len());

        let now = gridtokenx_telemetry::time::now();

        for event in events {
            let event_type = event.event_type();
            let user_id = event.user_id();
            let ip_address = event.ip_address().and_then(|s| s.parse::<IpNetwork>().ok());

            let event_data = match serde_json::to_value(&event) {
                Ok(data) => data,
                Err(_) => {
                    serde_json::json!({ "error": "serialization_failed", "type": event_type })
                }
            };

            activity_types.push(event_type.to_string());
            user_ids.push(user_id);
            ip_addresses.push(ip_address);
            metadata_list.push(event_data);
            created_at_list.push(now);
        }

        sqlx::query(
            r#"
            INSERT INTO user_activities (activity_type, user_id, ip_address, metadata, created_at)
            SELECT * FROM UNNEST($1::text[], $2::uuid[], $3::inet[], $4::jsonb[], $5::timestamptz[])
            "#,
        )
        .bind(&activity_types)
        .bind(&user_ids)
        .bind(&ip_addresses)
        .bind(&metadata_list)
        .bind(&created_at_list)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Log event without awaiting (fire-and-forget inside the worker queue)
    pub fn log_async(&self, event: AuditEvent) {
        let sender = self.job_sender.clone();
        if let Err(e) = sender.try_send(event) {
            tracing::error!(error = %e, "Trading Service: Failed to queue audit event");
        }
    }

    /// Query recent events for a user
    pub async fn get_user_events(
        &self,
        user_id: Uuid,
        limit: i64,
    ) -> anyhow::Result<Vec<AuditEventRecord>> {
        let records = sqlx::query_as::<_, AuditEventRecord>(
            r#"
            SELECT id, activity_type as event_type, user_id, ip_address, metadata as event_data, created_at
            FROM user_activities
            WHERE user_id = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.db)
        .await?;

        Ok(records)
    }
}

#[async_trait]
impl AuditLog for AuditLogger {
    async fn log_action(
        &self,
        user_id: Uuid,
        action: &str,
        details: &str,
    ) -> trading_core::traits::TraitResult<()> {
        tracing::info!(user_id = ?user_id, action = action, details = details, "Audit action logged");
        Ok(())
    }
}
