pub mod kafka;
pub mod kafka_consumer;

use trading_core::events::Event;
use anyhow::Result;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, RedisResult};
use tracing::{error, info};

pub use kafka::KafkaEventBus;

/// Unified event bus that delegates to either Redis Streams or Kafka
/// based on configuration. Controlled by `KAFKA_EVENTS_ENABLED` env var.
#[derive(Clone)]
pub enum EventBus {
    Redis(RedisEventBus),
    Kafka(KafkaEventBus),
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redis(r) => f.debug_tuple("EventBus::Redis").field(r).finish(),
            Self::Kafka(k) => f.debug_tuple("EventBus::Kafka").field(k).finish(),
        }
    }
}

impl EventBus {
    /// Create the appropriate event bus based on configuration.
    /// If `kafka_enabled` is true, connects to Kafka; otherwise falls back to Redis Streams.
    pub async fn new(
        redis_url: &str,
        kafka_enabled: bool,
        kafka_brokers: Option<&str>,
        kafka_topic_prefix: Option<&str>,
    ) -> Result<Self> {
        if kafka_enabled {
            if let Some(brokers) = kafka_brokers {
                match KafkaEventBus::new(brokers, kafka_topic_prefix).await {
                    Ok(kafka_bus) => {
                        info!("✅ EventBus initialized with Kafka backend");
                        return Ok(Self::Kafka(kafka_bus));
                    }
                    Err(e) => {
                        error!(
                            "Failed to initialize Kafka EventBus, falling back to Redis: {}",
                            e
                        );
                    }
                }
            } else {
                error!("KAFKA_EVENTS_ENABLED=true but no KAFKA_BOOTSTRAP_SERVERS set, falling back to Redis");
            }
        }

        // Default: Redis Streams
        let redis_bus = RedisEventBus::new(redis_url).await?;
        info!("✅ EventBus initialized with Redis Streams backend");
        Ok(Self::Redis(redis_bus))
    }

    /// Legacy constructor for backward compatibility (always Redis)
    pub async fn new_redis(redis_url: &str) -> Result<Self> {
        let redis_bus = RedisEventBus::new(redis_url).await?;
        Ok(Self::Redis(redis_bus))
    }

    pub async fn publish(&self, event: &Event) -> Result<String> {
        match self {
            Self::Redis(bus) => bus.publish(event).await,
            Self::Kafka(bus) => bus.publish(event).await,
        }
    }

    pub async fn publish_batch(&self, events: &[Event]) -> Result<usize> {
        match self {
            Self::Redis(bus) => bus.publish_batch(events).await,
            Self::Kafka(bus) => bus.publish_batch(events).await,
        }
    }

    pub async fn create_consumer_group(&self, group_name: &str) -> Result<()> {
        match self {
            Self::Redis(bus) => bus.create_consumer_group(group_name).await,
            Self::Kafka(_) => {
                // Kafka consumer groups are created automatically on first consume
                info!(
                    "Kafka consumer group '{}' will be auto-created on first consume",
                    group_name
                );
                Ok(())
            }
        }
    }

    pub async fn consume_events<F, Fut>(
        &self,
        group_name: &str,
        consumer_name: &str,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        match self {
            Self::Redis(bus) => bus.consume_events(group_name, consumer_name, handler).await,
            Self::Kafka(bus) => {
                // For real-time matching, we use the Orders and Updates topics
                let consumer_topics = vec![
                    bus.topics.orders_created.clone(),
                    bus.topics.orders_updated.clone(),
                ];
                
                let consumer = crate::events::kafka_consumer::KafkaConsumer::new(
                    &bus.bootstrap_servers,
                    consumer_topics,
                    Some(group_name.to_string()),
                )?;

                consumer.stream(handler).await
            }
        }
    }
}

/// Redis Streams-backed event bus (original implementation, now behind the enum).
#[derive(Clone)]
pub struct RedisEventBus {
    connection_manager: ConnectionManager,
    stream_name: String,
}

impl std::fmt::Debug for RedisEventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEventBus")
            .field("stream_name", &self.stream_name)
            .finish()
    }
}

impl RedisEventBus {
    pub async fn new(redis_url: &str) -> Result<Self> {
        info!("Initializing Redis EventBus service");

        let client = Client::open(redis_url)?;
        let connection_manager = ConnectionManager::new(client).await?;

        let stream_name = std::env::var("EVENT_STREAM_NAME")
            .unwrap_or_else(|_| "gridtokenx:events:v1".to_string());

        Ok(Self {
            connection_manager,
            stream_name,
        })
    }

    pub async fn publish(&self, event: &Event) -> Result<String> {
        let serialized = serde_json::to_string(event)?;
        let mut conn = self.connection_manager.clone();

        // XADD stream * event <json>
        let result: RedisResult<String> = conn
            .xadd(&self.stream_name, "*", &[("event", serialized)])
            .await;

        match result {
            Ok(id) => {
                info!(
                    "Event published to stream {}: {:?} (ID: {})",
                    self.stream_name, event, id
                );
                Ok(id)
            }
            Err(e) => {
                error!(
                    "Failed to publish event to stream {}: {}",
                    self.stream_name, e
                );
                Err(anyhow::anyhow!("Redis XADD failed: {}", e))
            }
        }
    }

    /// Batch publish events to Redis Streams (sequential XADD)
    pub async fn publish_batch(&self, events: &[Event]) -> Result<usize> {
        let mut count = 0;
        for event in events {
            if self.publish(event).await.is_ok() {
                count += 1;
            }
        }
        Ok(count)
    }

    pub async fn create_consumer_group(&self, group_name: &str) -> Result<()> {
        let mut conn = self.connection_manager.clone();
        let _: RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.stream_name)
            .arg(group_name)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;
        Ok(())
    }

    pub async fn consume_events<F, Fut>(
        &self,
        group_name: &str,
        consumer_name: &str,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        use redis::streams::{StreamReadOptions, StreamReadReply};

        info!(
            "Starting event consumption from {} (Group: {}, Consumer: {})",
            self.stream_name, group_name, consumer_name
        );

        let mut conn = self.connection_manager.clone();
        let opts = StreamReadOptions::default()
            .group(group_name, consumer_name)
            .block(5000)
            .count(10);

        loop {
            let read_result: RedisResult<StreamReadReply> = conn
                .xread_options(&[&self.stream_name], &[">"], &opts)
                .await;

            match read_result {
                Ok(reply) => {
                    for stream in reply.keys {
                        for entry in stream.ids {
                            if let Some(event_json) = entry.map.get("event") {
                                if let Ok(event_str) = redis::from_redis_value::<String>(event_json)
                                {
                                    if let Ok(event) = serde_json::from_str::<Event>(&event_str) {
                                        info!("🔍 Processing event from stream: {:?}", event);
                                        if let Err(e) = handler(event).await {
                                            error!("Error handling event {}: {}", entry.id, e);
                                        } else {
                                            // ACK the message
                                            let _: RedisResult<()> = conn
                                                .xack(&self.stream_name, group_name, &[&entry.id])
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    if !e.is_timeout() {
                        error!("Redis xread error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}
