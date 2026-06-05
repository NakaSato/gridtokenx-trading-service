//! Outbox publisher implementation.

use async_trait::async_trait;
use std::sync::Arc;
use trading_core::events::Event;
use trading_core::traits::{EventPublisher, OutboxRepository, TraitResult};

/// Publisher that writes events to the outbox table instead of directly to the message bus.
pub struct OutboxPublisher {
    repository: Arc<dyn OutboxRepository>,
}

impl OutboxPublisher {
    pub fn new(repository: Arc<dyn OutboxRepository>) -> Self {
        Self { repository }
    }
}

#[async_trait]
impl EventPublisher for OutboxPublisher {
    async fn publish(&self, event: Event) -> TraitResult<()> {
        self.repository.insert_event(&event).await
    }

    async fn publish_to_topic(&self, _topic: &str, event: Event) -> TraitResult<()> {
        // Topic is usually determined by the outbox worker based on the event type.
        self.repository.insert_event(&event).await
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
                    -> std::pin::Pin<Box<dyn std::future::Future<Output = TraitResult<()>> + Send>>
                + Send
                + Sync,
        >,
    ) -> TraitResult<()> {
        Err(trading_core::error::ApiError::Internal(
            "OutboxPublisher does not support consumption".to_string(),
        ))
    }
}
