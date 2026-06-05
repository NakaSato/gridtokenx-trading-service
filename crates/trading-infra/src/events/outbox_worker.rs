//! Background worker for processing the transactional outbox.

use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};
use trading_core::traits::{EventPublisher, OutboxRepository};

pub struct OutboxWorker {
    repository: Arc<dyn OutboxRepository>,
    publisher: Arc<dyn EventPublisher>,
    batch_size: i32,
    interval: Duration,
}

impl OutboxWorker {
    pub fn new(repository: Arc<dyn OutboxRepository>, publisher: Arc<dyn EventPublisher>) -> Self {
        Self {
            repository,
            publisher,
            batch_size: 100,
            interval: Duration::from_millis(500),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Starting OutboxWorker...");
        loop {
            match self.process_batch().await {
                Ok(count) => {
                    if count == 0 {
                        sleep(self.interval).await;
                    }
                }
                Err(e) => {
                    error!("Error in OutboxWorker: {}", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn process_batch(&self) -> anyhow::Result<usize> {
        let events = self.repository.get_pending_events(self.batch_size).await?;
        let count = events.len();

        for outbox_event in events {
            let event: trading_core::events::Event = serde_json::from_value(outbox_event.payload)?;

            match self.publisher.publish(event).await {
                Ok(_) => {
                    self.repository.mark_processed(outbox_event.id).await?;
                }
                Err(e) => {
                    warn!("Failed to publish outbox event {}: {}", outbox_event.id, e);
                    self.repository
                        .mark_failed(outbox_event.id, &e.to_string())
                        .await?;
                }
            }
        }

        Ok(count)
    }
}
