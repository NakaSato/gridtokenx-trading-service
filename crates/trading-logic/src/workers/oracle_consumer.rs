use std::sync::Arc;
use tracing::info;
use trading_core::traits::{EventPublisher, SettlementRepository};
use trading_core::events::Event;
use anyhow::Result;

use crate::settlement::SettlementService;

/// Worker that consumes telemetry from the Oracle Bridge via Redis Streams/Kafka
pub struct OracleConsumer {
    event_bus: Arc<dyn EventPublisher>,
    settlement_service: Arc<SettlementService>,
    consumer_group: String,
    consumer_name: String,
}

impl OracleConsumer {
    pub fn new(
        event_bus: Arc<dyn EventPublisher>,
        settlement_service: Arc<SettlementService>,
        consumer_group: String,
        consumer_name: String,
    ) -> Self {
        Self {
            event_bus,
            settlement_service,
            consumer_group,
            consumer_name,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!(
            "Starting Oracle Consumer worker (Group: {}, Consumer: {})",
            self.consumer_group, self.consumer_name
        );

        // Ensure consumer group exists
        if let Err(e) = self.event_bus.create_consumer_group(&self.consumer_group).await {
            info!("Note: Consumer group creation might have failed or already exists: {}", e);
        }

        // Define the handler
        let settlement_service = self.settlement_service.clone();
        let handler = Arc::new(move |event: Event| {
            let settlement_service = settlement_service.clone();
            Box::pin(async move {
                if let Event::OracleReading(payload) = event {
                    info!("🔍 Processing Oracle Reading for meter: {}", payload.meter_id);
                    
                    // Trigger settlement logic
                    if let Err(e) = settlement_service.process_reading(payload).await {
                        tracing::error!("Failed to process oracle reading: {}", e);
                    }
                }
                Ok(())
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = trading_core::traits::TraitResult<()>> + Send>>
        });

        self.event_bus.consume_events(
            &self.consumer_group,
            &self.consumer_name,
            handler
        ).await.map_err(|e| anyhow::anyhow!("Consumption loop failed: {}", e))?;

        Ok(())
    }
}
