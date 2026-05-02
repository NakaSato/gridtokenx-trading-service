use anyhow::Result;
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use std::sync::Arc;
use tracing::{error, info};

use trading_core::events::Event;
use trading_core::traits::{EventPublisher, TraitResult};

/// Kafka topic configuration for trading events
#[derive(Debug, Clone)]
pub struct KafkaTopics {
    pub orders_created: String,
    pub orders_matched: String,
    pub orders_updated: String,
    pub settlements: String,
    pub triggers: String,
    pub participants: String,
    pub telemetry: String,
}

impl KafkaTopics {
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            orders_created: format!("{}.orders.created", prefix),
            orders_matched: format!("{}.orders.matched", prefix),
            orders_updated: format!("{}.orders.updated", prefix),
            settlements: format!("{}.settlements", prefix),
            triggers: format!("{}.triggers", prefix),
            participants: format!("{}.participants", prefix),
            telemetry: format!("{}.telemetry", prefix),
        }
    }
}

impl Default for KafkaTopics {
    fn default() -> Self {
        Self::with_prefix("trading")
    }
}

/// Kafka-backed event bus for high-throughput event sourcing.
/// Replaces Redis Streams for trading events, providing:
/// - Topic-per-event-type partitioning for independent consumer scaling
/// - Zone-based partitioning for order locality
/// - Epoch-based partitioning for settlement batching
#[derive(Clone)]
pub struct KafkaEventBus {
    producer: FutureProducer,
    pub topics: KafkaTopics,
    pub bootstrap_servers: String,
}

impl std::fmt::Debug for KafkaEventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaEventBus")
            .field("topics", &self.topics)
            .finish()
    }
}

impl KafkaEventBus {
    pub async fn new(bootstrap_servers: &str, topic_prefix: Option<&str>) -> Result<Self> {
        info!(
            "Initializing Kafka EventBus (brokers: {})",
            bootstrap_servers
        );

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576") // 1GB buffer
            .set("batch.num.messages", "1000")
            .set("linger.ms", "5") // Batch for 5ms before sending
            .set("compression.type", "lz4")
            .set("acks", "1") // Leader ack only for low latency
            .set("retries", "3")
            .set("retry.backoff.ms", "100")
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka producer: {}", e))?;

        let topics = match topic_prefix {
            Some(prefix) => KafkaTopics::with_prefix(prefix),
            None => KafkaTopics::default(),
        };

        info!("✅ Kafka EventBus initialized with topics: {:?}", topics);

        Ok(Self { 
            producer, 
            topics,
            bootstrap_servers: bootstrap_servers.to_string(),
        })
    }

    /// Publish an event to the appropriate Kafka topic.
    /// Events are routed by type and partitioned by zone/epoch for locality.
    pub async fn publish(&self, event: &Event) -> Result<String> {
        let (topic, key) = self.route_event(event);
        let payload = serde_json::to_vec(event)?;

        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(&payload);

        match self
            .producer
            .send(record, Duration::from_secs(5))
            .await
        {
            Ok((partition, offset)) => {
                let id = format!("{}:{}:{}", topic, partition, offset);
                info!(
                    "Event published to Kafka topic {} (partition: {}, offset: {})",
                    topic, partition, offset
                );
                Ok(id)
            }
            Err((e, _)) => {
                error!("Failed to publish event to Kafka topic {}: {}", topic, e);
                Err(anyhow::anyhow!("Kafka produce failed: {}", e))
            }
        }
    }

    /// Batch publish multiple events (amortizes serialization overhead).
    /// Events are still routed individually to their correct topics.
    pub async fn publish_batch(&self, events: &[Event]) -> Result<usize> {
        let mut published = 0;

        for event in events {
            match self.publish(event).await {
                Ok(_) => published += 1,
                Err(e) => {
                    error!("Batch publish error (continuing): {}", e);
                }
            }
        }

        Ok(published)
    }

    /// Route an event to its topic and partition key
    fn route_event<'a>(&'a self, event: &Event) -> (&'a str, String) {
        match event {
            Event::OrderCreated(order) => (
                &self.topics.orders_created,
                order.zone_id.unwrap_or(0).to_string(), // Partition by zone
            ),
            Event::OrderMatched(payload) => (
                &self.topics.orders_matched,
                payload.epoch_id.to_string(), // Partition by epoch
            ),
            Event::OrderUpdate { id, .. } => (
                &self.topics.orders_updated,
                id.to_string(), // Partition by order ID
            ),
            Event::SettlementRequested(settlement) => (
                &self.topics.settlements,
                settlement.epoch_id.to_string(), // Partition by epoch
            ),
            Event::SettlementProcessed(payload) => (
                &self.topics.settlements,
                payload.settlement_id.to_string(),
            ),
            Event::PeakPriceUpdate { id, .. } => (
                &self.topics.orders_updated,
                id.to_string(),
            ),
            Event::TriggerExecution { id, .. } => (
                &self.topics.triggers,
                id.to_string(),
            ),
            Event::ErcIssued(payload) => (
                &self.topics.settlements,
                payload.user_id.to_string(),
            ),
            Event::UserRegistered(payload) => (
                &self.topics.orders_created, // Or dedicated identity topic
                payload.user_id.to_string(),
            ),
            Event::UserOnboarded(payload) => (
                &self.topics.participants,
                payload.user_id.to_string(),
            ),
            Event::UserWalletLinked(payload) => (
                &self.topics.participants,
                payload.user_id.to_string(),
            ),
            Event::OracleReading(payload) => (
                &self.topics.telemetry,
                payload.meter_id.clone(),
            ),
        }
    }
}

#[async_trait]
impl EventPublisher for KafkaEventBus {
    async fn publish(&self, event: Event) -> TraitResult<()> {
        self.publish_internal(&event).await.map(|_| ()).map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))
    }

    async fn publish_to_topic(&self, topic: &str, event: Event) -> TraitResult<()> {
        let payload = serde_json::to_vec(&event).map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))?;
        let record = FutureRecord::to(topic)
            .key("") // No specific key for direct topic publish
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map(|_| ())
            .map_err(|(e, _)| trading_core::error::ApiError::Internal(e.to_string()))
    }

    async fn create_consumer_group(&self, _group_name: &str) -> TraitResult<()> {
        // Kafka consumer groups are created automatically on first consume
        Ok(())
    }

    async fn consume_events(
        &self,
        group_name: &str,
        _consumer_name: &str,
        handler: Arc<dyn Fn(Event) -> std::pin::Pin<Box<dyn std::future::Future<Output = TraitResult<()>> + Send>> + Send + Sync>,
    ) -> TraitResult<()> {
        let consumer_topics = vec![
            self.topics.orders_created.clone(),
            self.topics.orders_matched.clone(),
            self.topics.orders_updated.clone(),
            self.topics.settlements.clone(),
            self.topics.triggers.clone(),
            self.topics.participants.clone(),
            self.topics.telemetry.clone(),
        ];

        let consumer = crate::events::kafka_consumer::KafkaConsumer::new(
            &self.bootstrap_servers,
            consumer_topics,
            Some(group_name.to_string()),
        ).map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))?;

        consumer.stream(move |event| {
            let h = handler.clone();
            async move {
                h(event).await.map_err(|e| anyhow::anyhow!(e.to_string()))
            }
        }).await.map_err(|e| trading_core::error::ApiError::Internal(e.to_string()))
    }
}

impl KafkaEventBus {
    /// Internal publish method that returns anyhow::Result and uses a reference to Event.
    pub async fn publish_internal(&self, event: &Event) -> Result<String> {
        let (topic, key) = self.route_event(event);
        let payload = serde_json::to_vec(event)?;

        let record = FutureRecord::to(topic)
            .key(&key)
            .payload(&payload);

        match self
            .producer
            .send(record, Duration::from_secs(5))
            .await
        {
            Ok((partition, offset)) => {
                let id = format!("{}:{}:{}", topic, partition, offset);
                info!(
                    "Event published to Kafka topic {} (partition: {}, offset: {})",
                    topic, partition, offset
                );
                Ok(id)
            }
            Err((e, _)) => {
                error!("Failed to publish event to Kafka topic {}: {}", topic, e);
                Err(anyhow::anyhow!("Kafka produce failed: {}", e))
            }
        }
    }
}
