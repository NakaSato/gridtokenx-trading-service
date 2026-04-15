use anyhow::Result;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tracing::info;

use crate::core::config::EventProcessorConfig;
use crate::services::webhook::WebhookService;

pub mod types;
pub use types::*;

#[derive(Clone)]
pub struct EventProcessorService {
    db: Arc<PgPool>,
    config: EventProcessorConfig,
    retry_count: Arc<AtomicU64>,
    replay_status: Arc<Mutex<Option<ReplayStatus>>>,
    webhook_service: WebhookService,
}

impl EventProcessorService {
    /// Create new event processor service (Stunted version for Gateway)
    pub fn new(
        db: Arc<PgPool>,
        _rpc_url: String,
        config: EventProcessorConfig,
        _energy_token_mint: String,
    ) -> Self {
        let webhook_service =
            WebhookService::new(config.webhook_url.clone(), config.webhook_secret.clone());

        Self {
            db,
            config,
            retry_count: Arc::new(AtomicU64::new(0)),
            replay_status: Arc::new(Mutex::new(None)),
            webhook_service,
        }
    }

    /// Start the event processor service (Disabled in Gateway)
    pub async fn start(&self) {
        info!("Event processor service is now handled by Microservices. Gateway listener is inactive.");
    }

    /// Get processing statistics
    pub async fn get_stats(&self) -> Result<EventProcessorStats> {
        Ok(EventProcessorStats {
            total_events: 0,
            confirmed_readings: 0,
            pending_confirmations: 0,
            total_retries: self.retry_count.load(Ordering::Relaxed),
        })
    }

    pub async fn replay_events(&self, _start_slot: u64, _end_slot: Option<u64>) -> Result<String> {
        anyhow::bail!("Event replaying is no longer supported in Gateway. Please use the Trading Service.")
    }

    pub fn get_replay_status(&self) -> Option<ReplayStatus> {
        None
    }
}
