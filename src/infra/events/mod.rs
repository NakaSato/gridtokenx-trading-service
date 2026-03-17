use anyhow::Result;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client, RedisResult};
use tracing::{debug, error, info};
use crate::domain::events::Event;

#[derive(Clone)]
pub struct EventBus {
    connection_manager: ConnectionManager,
    stream_name: String,
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("stream_name", &self.stream_name)
            .finish()
    }
}

impl EventBus {
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
        let result: RedisResult<String> = conn.xadd(
            &self.stream_name,
            "*",
            &[("event", serialized)]
        ).await;

        match result {
            Ok(id) => {
                debug!("Event published to stream {}: {:?} (ID: {})", self.stream_name, event, id);
                Ok(id)
            }
            Err(e) => {
                error!("Failed to publish event to stream {}: {}", self.stream_name, e);
                Err(anyhow::anyhow!("Redis XADD failed: {}", e))
            }
        }
    }
}
