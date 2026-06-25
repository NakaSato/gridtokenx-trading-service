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

    pub(crate) async fn process_batch(&self) -> anyhow::Result<usize> {
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)] // unwrap is idiomatic in tests
    use super::*;
    use async_trait::async_trait;
    use std::pin::Pin;
    use std::sync::Mutex;
    use trading_core::error::ApiError;
    use trading_core::events::{Event, SettlementProcessedPayload};
    use trading_core::models::{OutboxEvent, OutboxStatus};
    use trading_core::traits::TraitResult;
    use uuid::Uuid;

    fn sample_event() -> Event {
        Event::SettlementProcessed(SettlementProcessedPayload {
            settlement_id: Uuid::new_v4(),
            tx_signature: "sig123".to_string(),
            status: "completed".to_string(),
            timestamp: chrono::Utc::now(),
        })
    }

    fn pending_row(event: &Event) -> OutboxEvent {
        OutboxEvent {
            id: Uuid::new_v4(),
            event_type: "SettlementProcessed".to_string(),
            payload: serde_json::to_value(event).unwrap(),
            status: OutboxStatus::Pending,
            attempts: 0,
            last_attempt_at: None,
            created_at: chrono::Utc::now(),
        }
    }

    #[derive(Default)]
    struct MockOutboxRepo {
        pending: Mutex<Vec<OutboxEvent>>,
        processed: Mutex<Vec<Uuid>>,
        failed: Mutex<Vec<(Uuid, String)>>,
    }

    #[async_trait]
    impl OutboxRepository for MockOutboxRepo {
        async fn insert_event(&self, _event: &Event) -> TraitResult<()> {
            Ok(())
        }
        async fn get_pending_events(&self, _limit: i32) -> TraitResult<Vec<OutboxEvent>> {
            // Drain so a second call returns nothing (mirrors mark_processed removing rows).
            Ok(std::mem::take(&mut self.pending.lock().unwrap()))
        }
        async fn mark_processed(&self, id: Uuid) -> TraitResult<()> {
            self.processed.lock().unwrap().push(id);
            Ok(())
        }
        async fn mark_failed(&self, id: Uuid, error: &str) -> TraitResult<()> {
            self.failed.lock().unwrap().push((id, error.to_string()));
            Ok(())
        }
        async fn cleanup_processed(
            &self,
            _before: chrono::DateTime<chrono::Utc>,
        ) -> TraitResult<u64> {
            Ok(0)
        }
    }

    struct MockPublisher {
        fail: bool,
        published: Mutex<Vec<Event>>,
    }

    #[async_trait]
    impl EventPublisher for MockPublisher {
        async fn publish(&self, event: Event) -> TraitResult<()> {
            if self.fail {
                return Err(ApiError::Internal("publish boom".to_string()));
            }
            self.published.lock().unwrap().push(event);
            Ok(())
        }
        async fn publish_to_topic(&self, _topic: &str, _event: Event) -> TraitResult<()> {
            Ok(())
        }
        async fn create_consumer_group(&self, _group_name: &str) -> TraitResult<()> {
            Ok(())
        }
        async fn consume_events(
            &self,
            _group_name: &str,
            _consumer_name: &str,
            _handler: Arc<
                dyn Fn(
                        Event,
                    )
                        -> Pin<Box<dyn std::future::Future<Output = TraitResult<()>> + Send>>
                    + Send
                    + Sync,
            >,
        ) -> TraitResult<()> {
            Ok(())
        }
    }

    /// A pending outbox row is deserialized, published to the EventBus, and
    /// marked processed — the transactional-outbox fan-out happy path.
    #[tokio::test]
    async fn process_batch_publishes_and_marks_processed() {
        let event = sample_event();
        let row = pending_row(&event);
        let row_id = row.id;

        let repo = Arc::new(MockOutboxRepo::default());
        repo.pending.lock().unwrap().push(row);
        let publisher = Arc::new(MockPublisher {
            fail: false,
            published: Mutex::new(Vec::new()),
        });

        let worker = OutboxWorker::new(repo.clone(), publisher.clone());
        let count = worker.process_batch().await.unwrap();

        assert_eq!(count, 1);
        assert_eq!(publisher.published.lock().unwrap().len(), 1, "event published to bus");
        assert_eq!(repo.processed.lock().unwrap().as_slice(), &[row_id], "row marked processed");
        assert!(repo.failed.lock().unwrap().is_empty(), "not marked failed");
    }

    /// When publishing to the EventBus fails, the row is marked failed (for retry)
    /// and NOT marked processed — so the event is never silently dropped.
    #[tokio::test]
    async fn process_batch_marks_failed_on_publish_error() {
        let event = sample_event();
        let row = pending_row(&event);
        let row_id = row.id;

        let repo = Arc::new(MockOutboxRepo::default());
        repo.pending.lock().unwrap().push(row);
        let publisher = Arc::new(MockPublisher {
            fail: true,
            published: Mutex::new(Vec::new()),
        });

        let worker = OutboxWorker::new(repo.clone(), publisher.clone());
        let count = worker.process_batch().await.unwrap();

        assert_eq!(count, 1);
        assert!(publisher.published.lock().unwrap().is_empty(), "nothing published");
        assert!(repo.processed.lock().unwrap().is_empty(), "must NOT mark processed");
        assert_eq!(repo.failed.lock().unwrap().len(), 1, "row marked failed for retry");
        assert_eq!(repo.failed.lock().unwrap()[0].0, row_id);
    }
}
