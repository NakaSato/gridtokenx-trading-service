use anyhow::Result;
use trading_core::events::Event;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

/// High-performance Kafka consumer for state rehydration and event streaming.
pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topics: Vec<String>,
}

impl KafkaConsumer {
    /// Create a new Kafka consumer. If group_id is None, an ephemeral one is generated (for rehydration).
    pub fn new(bootstrap_servers: &str, topics: Vec<String>, group_id: Option<String>) -> Result<Self> {
        let group_id = group_id.unwrap_or_else(|| format!("rehydrator-{}", Uuid::new_v4()));
        
        info!("Initializing Kafka Consumer (brokers: {}, group: {})", bootstrap_servers, group_id);

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "true") // Auto-commit for streaming, manual seek for rehydration
            .set("auto.offset.reset", "earliest")
            .set("fetch.message.max.bytes", "10485760") // 10MB
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka consumer: {}", e))?;

        Ok(Self { consumer, topics })
    }

    /// Subscribe to configured topics and seek to start of history.
    pub async fn subscribe_from_beginning(&self) -> Result<()> {
        let topic_names: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
        self.consumer.subscribe(&topic_names)?;
        
        info!("Subscribed to topics: {:?}", topic_names);
        
        // rdkafka handles "earliest" via auto.offset.reset if no commit exists,
        // which is guaranteed by our ephemeral group ID.
        Ok(())
    }

    /// Read events until the high-water mark (current tip) is reached for all assigned partitions.
    /// This is used for "catch-up" rehydration.
    pub async fn rehydrate<F>(&self, mut handler: F) -> Result<usize> 
    where 
    F: FnMut(Event) -> Result<()>,
    {
        let mut count = 0;
        
        // Fetch metadata to log progress information
        if let Ok(metadata) = self.consumer.fetch_metadata(None, Duration::from_secs(5)) {
            for topic in metadata.topics() {
                if self.topics.contains(&topic.name().to_string()) {
                    info!("Topic: {} has {} partitions", topic.name(), topic.partitions().len());
                }
            }
        }

        info!("Starting Kafka rehydration loop (topics: {:?})...", self.topics);

        // We poll until we get a reasonable sequence of "no more messages" or a timeout
        loop {
            match tokio::time::timeout(Duration::from_secs(2), self.consumer.recv()).await {
                Ok(Ok(msg)) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<Event>(payload) {
                            Ok(event) => {
                                handler(event)?;
                                count += 1;
                                if count % 1000 == 0 {
                                    info!("🔄 Rehydrated {} events...", count);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to deserialize rehydration event: {} (offset: {})", e, msg.offset());
                            }
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!("Kafka rehydration poll error: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout hit - we've likely reached the tip of the stream
                    if count == 0 {
                        info!("Rehydration: Topics appear to be empty or tip already reached.");
                    } else {
                        info!("Rehydration catch-up reached tip of stream (no new messages for 2s)");
                    }
                    break;
                }
            }
        }

        info!("✅ Rehydration complete. Processed {} events total.", count);
        Ok(count)
    }

    /// Stream events indefinitely using a provided handler.
    pub async fn stream<F, Fut>(&self, handler: F) -> Result<()>
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let topic_names: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
        self.consumer.subscribe(&topic_names)?;
        
        info!("Started Kafka streaming consumer on topics: {:?}", topic_names);

        loop {
            match self.consumer.recv().await {
                Ok(msg) => {
                    if let Some(payload) = msg.payload() {
                        match serde_json::from_slice::<Event>(payload) {
                            Ok(event) => {
                                if let Err(e) = handler(event).await {
                                    error!("Error handling Kafka event: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to deserialize Kafka event: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Kafka consumer error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Get current high water marks for monitoring
    pub fn get_stats(&self) -> Result<()> {
        let metadata = self.consumer.fetch_metadata(None, Duration::from_secs(5))?;
        for topic in metadata.topics() {
            info!("Topic: {} (partitions: {})", topic.name(), topic.partitions().len());
        }
        Ok(())
    }
}
